"""Orchestrates the daily cashflow digest.

Flow: pull QBO + Jobber + sheet → compute → render → send → save snapshot.
"""
import os
import json
import logging
from datetime import date

from financial.config import email_settings, anomaly_settings, qbo_settings
from financial.qbo import fetch_account_balances, summarize_balances
from financial.jobber_finance import fetch_open_invoices
from financial.overhead_sheet import fetch_upcoming_bills
from financial.compute import (
    compute_ar,
    compute_vendor_balances,
    compute_payroll_accrual,
    compute_position,
)
from financial.email_render import render_email
from financial.gmail_send import send_email

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

    # Stale or missing manual QBO data
    qbo = snapshot["qbo"]
    if qbo.get("mode") == "manual":
        as_of = qbo.get("as_of")
        if not as_of:
            flags.append("Cash/CC balances are in manual mode and the as_of date is blank. "
                         "Update financial_config.yaml under qbo.manual when you check balances.")
        else:
            try:
                as_of_date = date.fromisoformat(str(as_of))
                age = (snapshot["date"] - as_of_date).days
                if age > 3:
                    flags.append(f"Manual cash/CC balances are {age} days old "
                                 f"(as_of {as_of}). Refresh them in financial_config.yaml.")
            except (ValueError, TypeError):
                flags.append(f"qbo.manual.as_of ({as_of}) isn't a valid YYYY-MM-DD date.")

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


def _manual_qbo_summary(cfg):
    """Build a qbo_summary dict from config (when qbo.mode == 'manual')."""
    manual = cfg.get("manual") or {}
    cash_accounts = [
        {"name": a["name"], "balance": round(float(a.get("balance") or 0), 2),
         "type": "Bank", "subtype": "", "currency": "USD"}
        for a in manual.get("cash_accounts", [])
    ]
    cc_accounts = [
        {"name": a["name"], "balance": round(float(a.get("balance") or 0), 2),
         "type": "Credit Card", "subtype": "", "currency": "USD"}
        for a in manual.get("cc_accounts", [])
    ]
    return {
        "cash_total": round(sum(a["balance"] for a in cash_accounts), 2),
        "cc_total": round(sum(a["balance"] for a in cc_accounts), 2),
        "cash_accounts": cash_accounts,
        "cc_accounts": cc_accounts,
        "as_of": manual.get("as_of"),
        "mode": "manual",
    }


def build_snapshot(today=None):
    today = today or date.today()
    logger.info(f"=== Cashflow snapshot starting for {today.isoformat()} ===")

    qbo_cfg = qbo_settings()
    if qbo_cfg.get("mode") == "manual":
        qbo_summary = _manual_qbo_summary(qbo_cfg)
        logger.info(f"QBO: manual mode, as_of={qbo_summary.get('as_of')}")
    else:
        accounts = fetch_account_balances()
        qbo_summary = summarize_balances(accounts)
        qbo_summary["mode"] = "api"

    invoices = fetch_open_invoices()
    ar = compute_ar(invoices)

    vendors = compute_vendor_balances(today)
    payroll = compute_payroll_accrual(today)
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

    recipient = email_settings()["recipient"]
    result = send_email(recipient, subject, html)

    if result:
        _save_snapshot(snapshot)
        return {"status": "ok", "subject": subject, "to": recipient, "message_id": result.get("id")}
    return {"status": "error", "message": "Gmail send failed (see logs)"}
