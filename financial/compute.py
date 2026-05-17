"""Aggregation layer.

Takes raw data from QBO, Jobber, and the overhead sheet, and produces a
single 'position' dict that the email renderer consumes.
"""
import logging
from datetime import date, datetime, timedelta

from financial.config import vendors as vendors_cfg, payroll_settings, aging_buckets
from financial.jobber_finance import (
    fetch_open_invoices,
    fetch_expenses_since,
    expenses_for_vendor,
    fetch_time_entries_since,
    current_pay_week_start,
)

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# AR
# ---------------------------------------------------------------------------

def compute_ar(invoices=None):
    invoices = invoices if invoices is not None else fetch_open_invoices()
    buckets = aging_buckets()
    bucketed = {b["label"]: {"label": b["label"], "count": 0, "total": 0.0} for b in buckets}
    total = 0.0

    for inv in invoices:
        dpd = inv.get("days_past_due")
        if dpd is None:
            # Treat unknown due dates as Current
            target_label = buckets[0]["label"]
        else:
            target_label = None
            for b in buckets:
                if b["days_min"] <= dpd <= b["days_max"]:
                    target_label = b["label"]
                    break
            if target_label is None:
                target_label = buckets[-1]["label"]
        bucketed[target_label]["count"] += 1
        bucketed[target_label]["total"] = round(bucketed[target_label]["total"] + inv["outstanding"], 2)
        total += inv["outstanding"]

    # Top 5 overdue clients by outstanding amount (days_past_due > 0)
    overdue = [i for i in invoices if (i.get("days_past_due") or 0) > 0]
    top_overdue = sorted(overdue, key=lambda i: i["outstanding"], reverse=True)[:5]

    return {
        "total_outstanding": round(total, 2),
        "buckets": list(bucketed.values()),
        "invoices": invoices,
        "top_overdue": top_overdue,
    }


# ---------------------------------------------------------------------------
# Vendor balances
# ---------------------------------------------------------------------------

def compute_vendor_balances(today=None, sheet_overrides=None):
    """Compute vendor balances using an anchor + accumulator model.

    For each vendor:
      anchor_balance  = sheet override (if set) OR config manual_balance
      anchor_date     = sheet override 'as of' OR config manual_balance_as_of
      accumulator     = sum of Jobber Expenses tagged with the vendor name
                        and dated strictly after anchor_date
      balance         = anchor_balance + accumulator (when auto: true)

    When Alex pays a vendor and wants to reset the accumulator, he updates
    Balance + As Of in the Vendor Balances Google Sheet (sheet wins). The
    new anchor date causes the accumulator to restart from there.
    """
    today = today or date.today()
    cfg = vendors_cfg()
    sheet_overrides = sheet_overrides or {}
    sheet_lookup = {k.strip().lower(): v for k, v in sheet_overrides.items()}

    def _resolve_anchor(v):
        """Return (anchor_balance, anchor_date_str, mode)."""
        ov = sheet_lookup.get(v["name"].strip().lower())
        if ov:
            return (round(float(ov["balance"]), 2), ov.get("as_of") or None, "sheet")
        mb = v.get("manual_balance")
        if mb is not None:
            return (round(float(mb), 2), v.get("manual_balance_as_of") or None, "manual")
        return (0.0, None, "manual")

    def _parse_date(s):
        try:
            return date.fromisoformat(str(s)) if s else None
        except (ValueError, TypeError):
            return None

    # Compute earliest anchor date across all auto vendors so one fetch
    # covers everyone. Per-vendor filtering happens in-memory below.
    auto_vendors = [v for v in cfg if v.get("auto")]
    earliest_anchor = None
    for v in auto_vendors:
        _, anchor_str, _ = _resolve_anchor(v)
        d = _parse_date(anchor_str)
        if d and (earliest_anchor is None or d < earliest_anchor):
            earliest_anchor = d

    expenses = None
    if auto_vendors:
        fetch_from = earliest_anchor or today.replace(day=1)
        expenses = fetch_expenses_since(fetch_from)

    results = []
    grand_total = 0.0
    for v in cfg:
        anchor_balance, anchor_date_str, mode = _resolve_anchor(v)
        entry = {
            "name": v["name"],
            "mode": mode,
            "balance": anchor_balance,
            "as_of": anchor_date_str,
            "source_note": "",
            "expense_count": 0,
            "anchor_balance": anchor_balance,
            "accumulated": 0.0,
        }

        if v.get("auto"):
            anchor_date = _parse_date(anchor_date_str) or (earliest_anchor or today.replace(day=1))
            anchor_iso = anchor_date.isoformat()
            # Filter expenses: matches vendor patterns AND dated AFTER anchor.
            matched = expenses_for_vendor(expenses or [], v.get("parse_patterns", []))
            matched_after = [
                e for e in matched
                if e.get("date") and e["date"] > anchor_iso
            ]
            accumulated = round(sum(e["total"] for e in matched_after), 2)
            entry["mode"] = "auto"
            entry["accumulated"] = accumulated
            entry["expense_count"] = len(matched_after)
            entry["balance"] = round(anchor_balance + accumulated, 2)
            entry["as_of"] = today.isoformat()
            entry["source_note"] = (
                f"${anchor_balance:,.2f} as of {anchor_date_str or 'fetch start'} "
                f"+ {len(matched_after)} tagged expense(s) totalling ${accumulated:,.2f}"
            )
        else:
            entry["source_note"] = (
                "manual entry in financial_config.yaml"
                if mode == "manual" else "from 'Vendor Balances' sheet"
            )
            if v.get("manual_balance") is None and mode == "manual":
                entry["source_note"] += " (not yet set — appears as $0)"

        results.append(entry)
        grand_total += entry["balance"]

    return {
        "vendors": results,
        "total": round(grand_total, 2),
    }


# ---------------------------------------------------------------------------
# Payroll accrual
# ---------------------------------------------------------------------------

def compute_payroll_accrual(today=None, entries=None):
    """Hours worked since Monday 00:00 of current pay week × labourRate.

    Each time entry carries its own labourRate (from Jobber), so the accrual
    is sum(duration * labourRate) per entry. Per-person breakdown computes
    a weighted-average effective rate when one user worked at multiple rates
    during the week.

    rate_overrides in financial_config.yaml force a specific rate for a
    given user name when Jobber's labourRate is missing or wrong.

    entries: optional pre-fetched list (lets the caller decide failure
    handling and avoids a redundant API call).
    """
    today = today or date.today()
    week_start = current_pay_week_start(today)
    start_dt = datetime.combine(week_start, datetime.min.time())

    rate_overrides = payroll_settings().get("rate_overrides") or {}
    if entries is None:
        entries = fetch_time_entries_since(start_dt) or []

    by_user = {}
    for e in entries:
        uid = e.get("user_id")
        name = e.get("user_name") or "Unknown"
        hours = e["duration_seconds"] / 3600.0
        rate = float(rate_overrides[name]) if name in rate_overrides else float(e.get("labour_rate") or 0)

        bucket = by_user.setdefault(uid or name, {
            "name": name,
            "hours": 0.0,
            "estimated_pay": 0.0,
            "rate_source": "override" if name in rate_overrides else ("entry" if rate else "missing"),
        })
        bucket["hours"] += hours
        bucket["estimated_pay"] += hours * rate

    # Compute effective rate per user (weighted avg)
    total_accrual = 0.0
    total_hours = 0.0
    for u in by_user.values():
        u["hours"] = round(u["hours"], 2)
        u["estimated_pay"] = round(u["estimated_pay"], 2)
        u["rate"] = round(u["estimated_pay"] / u["hours"], 2) if u["hours"] else 0.0
        total_accrual += u["estimated_pay"]
        total_hours += u["hours"]

    breakdown = sorted(by_user.values(), key=lambda u: u["estimated_pay"], reverse=True)

    return {
        "week_start": week_start.isoformat(),
        "as_of": today.isoformat(),
        "total_hours": round(total_hours, 2),
        "total_accrual": round(total_accrual, 2),
        "breakdown": breakdown,
        "missing_rates": [u["name"] for u in breakdown if u["rate"] == 0.0],
    }


# ---------------------------------------------------------------------------
# Full position
# ---------------------------------------------------------------------------

def compute_position(qbo_summary, ar_data, vendor_data, payroll_data, upcoming_bills):
    """Roll everything up into the headline numbers.

    Two views:
      - Liquid right now (no AR, no future bills): cash - CC - vendor - payroll
      - Total position (includes AR receivable): liquid + AR
      - 30-day forecast: total - upcoming_bills(30) - estimated next-week payroll accrual

    AR is shown but NOT subtracted; CC + vendor + payroll are all liabilities.
    """
    cash = qbo_summary.get("cash_total", 0.0)
    cc_debt = qbo_summary.get("cc_total", 0.0)
    vendor_total = vendor_data.get("total", 0.0)
    payroll_accrued = payroll_data.get("total_accrual", 0.0)
    ar_total = ar_data.get("total_outstanding", 0.0)

    liquid_now = round(cash - cc_debt - vendor_total - payroll_accrued, 2)
    total_position = round(liquid_now + ar_total, 2)

    # 30-day forecast
    bills_30 = sum((b["amount"] or 0) for b in upcoming_bills if b.get("days_until_due") is not None and 0 <= b["days_until_due"] <= 30)
    forecast_30 = round(total_position - bills_30, 2)

    return {
        "cash": cash,
        "cc_debt": cc_debt,
        "vendor_debt": vendor_total,
        "payroll_accrued": payroll_accrued,
        "ar_outstanding": ar_total,
        "liquid_now": liquid_now,
        "total_position": total_position,
        "bills_next_30": round(bills_30, 2),
        "forecast_30": forecast_30,
    }
