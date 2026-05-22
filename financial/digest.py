"""Orchestrates the daily cashflow digest.

Flow: pull QBO + Jobber + sheet → compute → render → send → save snapshot.
"""
import os
import json
import logging
from datetime import date, datetime

from financial.config import (
    email_settings,
    anomaly_settings,
    qbo_accounts,
    vendors as vendors_cfg,
)
from financial.jobber_finance import fetch_open_invoices, fetch_time_entries_since, current_pay_week_start
from financial.overhead_sheet import fetch_upcoming_bills
from financial.compute import (
    compute_ar,
    compute_vendor_balances,
    compute_payroll_accrual,
    compute_position,
)
from financial.email_render import render_email
from financial.gmail_send import send_email
from financial import sheets_overrides

logger = logging.getLogger(__name__)

LAST_SNAPSHOT_FILE = "last_financial_snapshot.json"


def _load_last_snapshot():
    if os.path.exists(LAST_SNAPSHOT_FILE):
        try:
            with open(LAST_SNAPSHOT_FILE) as f:
                return json.load(f)
        except Exception:
            return None
    return None


def _save_snapshot(snapshot):
    saveable = {
        "date": snapshot["date"].isoformat(),
        "position": snapshot["position"],
        "ar_total": snapshot["ar"]["total_outstanding"],
        "ar_buckets": [{"label": b["label"], "total": b["total"], "count": b["count"]} for b in snapshot["ar"]["buckets"]],
    }
    with open(LAST_SNAPSHOT_FILE, "w") as f:
        json.dump(saveable, f, indent=2)


def _compute_flags(snapshot, previous):
    flags = []
    cfg = anomaly_settings()
    pos = snapshot["position"]

    # Stale account balances — flag any that haven't been refreshed in >3 days.
    # Only flags accounts with meaningful balances ($10+), so $0 placeholders
    # and trivial leftovers (QB Checking at $1.39) don't spam the warnings.
    qbo = snapshot["qbo"]
    stale = []
    for a in (qbo.get("cash_accounts") or []) + (qbo.get("cc_accounts") or []):
        if abs(a.get("balance") or 0) < 10:
            continue
        as_of = a.get("as_of")
        if not as_of:
            continue
        try:
            age = (snapshot["date"] - date.fromisoformat(str(as_of))).days
            if age > 3:
                stale.append(f"{a['name']} ({age}d old)")
        except (ValueError, TypeError):
            continue
    if stale:
        flags.append(
            "Account balances haven't been refreshed in >3 days: "
            + ", ".join(stale[:5])
            + (f" + {len(stale)-5} more" if len(stale) > 5 else "")
            + ". Update via the 'Balance Overrides' Sheet tab or financial_config.yaml."
        )

    # Day-over-day cash drop
    if previous and previous.get("position"):
        prev_cash = previous["position"].get("cash") or 0
        cur_cash = pos.get("cash") or 0
        if prev_cash > 0:
            drop_pct = (prev_cash - cur_cash) / prev_cash * 100
            threshold = cfg.get("cash_drop_pct_alert", 25)
            if drop_pct >= threshold:
                flags.append(
                    f"Cash dropped {drop_pct:.0f}% since last snapshot "
                    f"(${prev_cash:,.2f} → ${cur_cash:,.2f})"
                )

    # Missing wage rates
    missing = snapshot["payroll"].get("missing_rates") or []
    if missing:
        flags.append(
            f"Wage rate missing for {len(missing)} user(s): {', '.join(missing)}. "
            f"Set in financial_config.yaml under payroll.rate_overrides."
        )

    # Bills due today
    today_bills = [b for b in snapshot["upcoming_bills"] if b.get("days_until_due") == 0]
    if today_bills:
        total = sum(b["amount"] or 0 for b in today_bills)
        names = ", ".join(b["expense"] for b in today_bills)
        flags.append(f"${total:,.2f} due today: {names}")

    # Skipped overhead rows
    skipped = [b for b in snapshot["upcoming_bills"] if b.get("skip_reason")]
    if skipped:
        flags.append(
            f"{len(skipped)} overhead row(s) couldn't be parsed — check the sheet "
            f"or fix the row(s)."
        )

    # Big AR concentration
    ar_total = snapshot["ar"]["total_outstanding"]
    if ar_total > 0 and snapshot["ar"]["top_overdue"]:
        top = snapshot["ar"]["top_overdue"][0]
        share = top["outstanding"] / ar_total * 100
        if share >= 40:
            flags.append(
                f"One client ({top['client_name']}) is {share:.0f}% of all outstanding AR "
                f"(${top['outstanding']:,.2f})."
            )

    return flags


def _build_qbo_summary():
    """Build the qbo_summary dict from the YAML account list + the Balance
    Overrides Google Sheet tab. Sheet entries (id-keyed) override YAML.
    No QBO API calls — all values are manual.
    """
    yaml_accounts = qbo_accounts()

    try:
        sheet_overrides = sheets_overrides.read_balance_overrides(
            seed_accounts=[
                {"id": str(a.get("id", "")), "name": a.get("name", "")}
                for a in yaml_accounts
            ]
        )
    except Exception as e:
        logger.warning(f"Balance override sheet read failed: {e}")
        sheet_overrides = {}

    cash_accounts = []
    cc_accounts = []
    cash_total = 0.0
    cc_total = 0.0

    for a in yaml_accounts:
        acc_id = str(a.get("id", ""))
        sheet = sheet_overrides.get(acc_id)
        if sheet:
            balance = round(float(sheet["balance"]), 2)
            as_of = sheet.get("as_of") or a.get("as_of")
            is_override = True
        else:
            balance = round(float(a.get("balance") or 0), 2)
            as_of = a.get("as_of")
            is_override = False

        entry = {
            "id": acc_id,
            "name": a.get("name", ""),
            "type": a.get("type", "Bank"),
            "subtype": "",
            "balance": balance,
            "currency": "USD",
            "is_override": is_override,
            "override_as_of": as_of if is_override else None,
            "override_note": (sheet.get("note", "") if sheet else ""),
            "as_of": as_of,
        }
        if entry["type"] == "Bank":
            cash_total += balance
            cash_accounts.append(entry)
        elif entry["type"] == "Credit Card":
            cc_total += balance
            cc_accounts.append(entry)

    logger.info(
        f"QBO: {len(yaml_accounts)} accounts from config, "
        f"{len(sheet_overrides)} overridden by sheet"
    )
    return {
        "cash_total": round(cash_total, 2),
        "cc_total": round(cc_total, 2),
        "cash_accounts": cash_accounts,
        "cc_accounts": cc_accounts,
        "mode": "config",
        "books_through": None,
        "sheet_overrides_active": len(sheet_overrides),
    }


def build_snapshot(today=None):
    today = today or date.today()
    logger.info(f"=== Cashflow snapshot starting for {today.isoformat()} ===")

    # Track sources that failed so the email can shout about it
    unavailable = []

    qbo_summary = _build_qbo_summary()

    invoices = fetch_open_invoices()
    if invoices is None:
        unavailable.append("Jobber AR")
        invoices = []
        logger.error("Jobber AR fetch failed (None). Marked unavailable.")
    ar = compute_ar(invoices)

    # Vendor balances — read sheet overrides, seed with current config vendors
    try:
        vendor_sheet = sheets_overrides.read_vendor_overrides(
            seed_vendors=[{"name": v["name"]} for v in vendors_cfg()]
        )
    except Exception as e:
        logger.warning(f"Vendor override sheet read failed: {e}")
        vendor_sheet = {}
    vendors = compute_vendor_balances(today, sheet_overrides=vendor_sheet)

    # Payroll — fetch entries once, detect failure, then compute
    week_start = current_pay_week_start(today)
    entries_start_dt = datetime.combine(week_start, datetime.min.time())
    time_entries = fetch_time_entries_since(entries_start_dt)
    if time_entries is None:
        unavailable.append("Jobber Time Entries")
        logger.error("Jobber time entries fetch failed. Payroll marked unavailable.")
        payroll = {
            "week_start": week_start.isoformat(),
            "as_of": today.isoformat(),
            "total_hours": 0.0,
            "total_accrual": 0.0,
            "breakdown": [],
            "missing_rates": [],
            "unavailable": True,
        }
    else:
        payroll = compute_payroll_accrual(today, entries=time_entries)

    upcoming_bills = fetch_upcoming_bills(today)
    position = compute_position(qbo_summary, ar, vendors, payroll, upcoming_bills)

    snapshot = {
        "date": today,
        "position": position,
        "qbo": qbo_summary,
        "ar": ar,
        "vendors": vendors,
        "payroll": payroll,
        "upcoming_bills": upcoming_bills,
        "unavailable_sources": unavailable,
    }

    previous = _load_last_snapshot()
    snapshot["flags"] = _compute_flags(snapshot, previous)

    return snapshot


def run_digest(today=None, dry_run=False):
    """Build the snapshot, render the email, send it, and save state.

    If dry_run=True, returns (subject, html, snapshot) without sending.
    """
    snapshot = build_snapshot(today)
    subject, html = render_email(snapshot)

    if dry_run:
        logger.info("Dry run: not sending email.")
        return {"status": "dry_run", "subject": subject, "html": html, "snapshot": snapshot}

    # Support both legacy `recipient` (single string) and `recipients` (list).
    es = email_settings()
    recipients = es.get("recipients") or ([es["recipient"]] if es.get("recipient") else [])
    if not recipients:
        return {"status": "error", "message": "No recipients configured"}
    # Gmail accepts a comma-separated To: header for multiple recipients.
    to_header = ", ".join(recipients)
    result = send_email(to_header, subject, html)

    if result:
        _save_snapshot(snapshot)
        return {"status": "ok", "subject": subject, "to": to_header, "message_id": result.get("id")}
    return {"status": "error", "message": "Gmail send failed (see logs)"}
