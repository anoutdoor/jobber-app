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
        logger.info(
            f"Vendor balances: fetched {len(expenses or [])} expenses since "
            f"{fetch_from.isoformat()} for {len(auto_vendors)} auto vendor(s)"
        )
        # Log the first few so we can see what's in the bucket
        for e in (expenses or [])[:5]:
            logger.info(
                f"  expense sample: date={e.get('date')!r} "
                f"title={e.get('title')!r} desc={e.get('description')!r} "
                f"total={e.get('total')}"
            )

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
            # Filter expenses: matches vendor patterns AND dated on or after anchor.
            # Inclusive so that resetting the anchor today (after a payment, say)
            # immediately catches today's new expenses too.
            matched = expenses_for_vendor(expenses or [], v.get("parse_patterns", []))
            matched_from = [
                e for e in matched
                if e.get("date") and e["date"] >= anchor_iso
            ]
            logger.info(
                f"Vendor {v['name']!r}: patterns={v.get('parse_patterns')} "
                f"anchor={anchor_iso} -> {len(matched)} matched substring, "
                f"{len(matched_from)} survived date filter"
            )
            accumulated = round(sum(e["total"] for e in matched_from), 2)
            entry["mode"] = "auto"
            entry["accumulated"] = accumulated
            entry["expense_count"] = len(matched_from)
            entry["balance"] = round(anchor_balance + accumulated, 2)
            entry["as_of"] = today.isoformat()
            entry["source_note"] = (
                f"${anchor_balance:,.2f} as of {anchor_date_str or 'fetch start'} "
                f"+ {len(matched_from)} tagged expense(s) totalling ${accumulated:,.2f}"
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
    excluded = {str(x).strip().lower() for x in (payroll_settings().get("exclude_names") or []) if str(x).strip()}
    if entries is None:
        entries = fetch_time_entries_since(start_dt) or []

    by_user = {}
    for e in entries:
        uid = e.get("user_id")
        name = e.get("user_name") or "Unknown"
        # Skip anyone on the exclusion list (matched on any word of their name).
        if excluded and (excluded & set(name.lower().split())):
            continue
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
    total_gross = 0.0
    total_hours = 0.0
    for u in by_user.values():
        u["hours"] = round(u["hours"], 2)
        u["estimated_pay"] = round(u["estimated_pay"], 2)
        u["rate"] = round(u["estimated_pay"] / u["hours"], 2) if u["hours"] else 0.0
        total_gross += u["estimated_pay"]
        total_hours += u["hours"]

    # Filter to users who actually worked. Zero-hour entries come from
    # clock-ins that are still ticking (finalDuration=0 until clock-out)
    # or short/test entries; they're noise here and they trigger spurious
    # "missing wage rate" warnings when included.
    breakdown = [u for u in by_user.values() if u["hours"] > 0]
    breakdown.sort(key=lambda u: u["estimated_pay"], reverse=True)

    # Tax burden (employer-side FICA, Medicare, UI, workers' comp, etc.).
    # Configurable in financial_config.yaml under payroll.tax_burden_pct.
    tax_burden_pct = float(payroll_settings().get("tax_burden_pct") or 0)
    tax_burden = round(total_gross * tax_burden_pct / 100.0, 2)
    total_accrual = round(total_gross + tax_burden, 2)

    return {
        "week_start": week_start.isoformat(),
        "as_of": today.isoformat(),
        "total_hours": round(total_hours, 2),
        "gross_pay": round(total_gross, 2),
        "tax_burden_pct": tax_burden_pct,
        "tax_burden": tax_burden,
        "total_accrual": total_accrual,
        "breakdown": breakdown,
        "missing_rates": [u["name"] for u in breakdown if u["rate"] == 0.0],
    }


# ---------------------------------------------------------------------------
# Full position
# ---------------------------------------------------------------------------

def compute_position(
    qbo_summary,
    ar_data,
    vendor_data,
    payroll_data,
    upcoming_bills,
    *,
    cc_rewards_amount=0.0,
    customer_deposits_total=0.0,
    uncleared_checks=0.0,
    owner_pay=0.0,
    today=None,
    due_by_jul1_date=None,
):
    """Full position with two-bucket free-cash views.

    NET POSITION (headline) zeros out every debt + liability and pulls in
    AR + CC rewards as offsetting assets. This is your true economic
    position if everything cleared and got paid right now.

    Two free-cash views:
      - Free cash THIS WEEK: liquid - obligations through end of this week
      - Free cash BY JULY 1: liquid - obligations through 2026-07-01 (incl. vendor AP)

    Obligations are NOT double-counted between the two windows; the July 1
    window is a superset of this week's.
    """
    from datetime import timedelta as _td

    today = today or date.today()
    # End of this week = next Sunday (or today if it's already Sunday)
    days_to_sunday = (6 - today.weekday()) % 7
    end_of_week = today + _td(days=days_to_sunday)
    jul1 = due_by_jul1_date or date(today.year, 7, 1)

    cash = float(qbo_summary.get("cash_total", 0.0) or 0)
    cc_debt = float(qbo_summary.get("cc_total", 0.0) or 0)
    vendor_total = float(vendor_data.get("total", 0.0) or 0)
    payroll_accrued = float(payroll_data.get("total_accrual", 0.0) or 0)
    ar_total = float(ar_data.get("total_outstanding", 0.0) or 0)

    cc_rewards_amount = float(cc_rewards_amount or 0)
    customer_deposits_total = float(customer_deposits_total or 0)
    uncleared_checks = float(uncleared_checks or 0)
    owner_pay = float(owner_pay or 0)

    # Bills classified by window
    def _in_window(b, cutoff):
        nd = b.get("next_due_date")
        if nd is None or b.get("amount") is None:
            return False
        return today <= nd <= cutoff

    bills_this_week = [b for b in upcoming_bills if _in_window(b, end_of_week)]
    bills_by_jul1 = [b for b in upcoming_bills if _in_window(b, jul1)]
    bills_this_week_total = round(sum(b["amount"] for b in bills_this_week), 2)
    bills_by_jul1_total = round(sum(b["amount"] for b in bills_by_jul1), 2)

    # This week obligations: bills + uncleared checks + payroll + owner pay
    this_week_obligations = round(
        bills_this_week_total + uncleared_checks + payroll_accrued + owner_pay, 2
    )
    # By July 1: this week's stuff + remaining bills through 7/1 + vendor AP
    by_jul1_obligations = round(
        bills_by_jul1_total + uncleared_checks + payroll_accrued + owner_pay + vendor_total, 2
    )

    free_cash_this_week = round(cash - this_week_obligations, 2)
    free_cash_by_jul1 = round(cash - by_jul1_obligations, 2)

    # Net position: full economic value
    net_position = round(
        cash + ar_total + cc_rewards_amount
        - cc_debt - vendor_total - payroll_accrued - owner_pay
        - customer_deposits_total - uncleared_checks,
        2,
    )

    return {
        # Assets
        "cash": round(cash, 2),
        "cc_rewards_available": round(cc_rewards_amount, 2),
        "ar_outstanding": round(ar_total, 2),
        # Liabilities
        "cc_debt": round(cc_debt, 2),
        "vendor_debt": round(vendor_total, 2),
        "payroll_accrued": round(payroll_accrued, 2),
        "owner_pay": round(owner_pay, 2),
        "customer_deposits": round(customer_deposits_total, 2),
        "uncleared_checks": round(uncleared_checks, 2),
        # Windows
        "this_week_end": end_of_week.isoformat(),
        "by_jul1_date": jul1.isoformat(),
        "bills_this_week_total": bills_this_week_total,
        "bills_by_jul1_total": bills_by_jul1_total,
        "this_week_obligations_total": this_week_obligations,
        "by_jul1_obligations_total": by_jul1_obligations,
        "free_cash_this_week": free_cash_this_week,
        "free_cash_by_jul1": free_cash_by_jul1,
        # Headline
        "net_position": net_position,
        # Backwards-compat shims for anomaly flag code that reads these names
        "liquid_now": round(cash - cc_debt - vendor_total - payroll_accrued, 2),
        "total_position": net_position,
    }
