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
    """Compute vendor balances. sheet_overrides (from sheets_overrides.read_vendor_overrides)
    takes precedence over the YAML config when present.
    """
    today = today or date.today()
    cfg = vendors_cfg()
    sheet_overrides = sheet_overrides or {}
    # Build case-insensitive lookup for sheet overrides
    sheet_lookup = {k.strip().lower(): v for k, v in sheet_overrides.items()}

    # Only fetch expenses if any vendor is in auto mode
    expenses = None
    auto_needed = any(v.get("auto") for v in cfg)
    if auto_needed:
        mtd_start = today.replace(day=1)
        expenses = fetch_expenses_since(mtd_start)

    results = []
    grand_total = 0.0
    for v in cfg:
        entry = {
            "name": v["name"],
            "mode": "auto" if v.get("auto") else "manual",
            "balance": 0.0,
            "as_of": None,
            "source_note": "",
            "expense_count": 0,
        }

        # Sheet override wins over both auto and manual
        override = sheet_lookup.get(v["name"].strip().lower())
        if override:
            entry["balance"] = round(override["balance"], 2)
            entry["as_of"] = override.get("as_of") or None
            entry["mode"] = "sheet"
            entry["source_note"] = "from 'Vendor Balances' sheet"
        elif v.get("auto"):
            matches = expenses_for_vendor(expenses or [], v.get("parse_patterns", []))
            entry["balance"] = round(sum(e["total"] for e in matches), 2)
            entry["expense_count"] = len(matches)
            entry["as_of"] = today.isoformat()
            entry["source_note"] = f"sum of {len(matches)} Jobber expense(s) MTD"
        else:
            mb = v.get("manual_balance")
            entry["balance"] = round(float(mb), 2) if mb is not None else 0.0
            entry["as_of"] = v.get("manual_balance_as_of")
            entry["source_note"] = "manual entry in financial_config.yaml"
            if mb is None:
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
