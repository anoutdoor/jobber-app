"""Reads the recurring overhead bills from financial_config.yaml and turns
them into a list of upcoming bills with concrete next-due dates.

(Used to read from a Google Sheet; switched to in-config storage since the
list is stable month-to-month and easier to maintain in YAML than via the
Sheets API.)
"""
import logging
import calendar
from datetime import date

from financial.config import overhead_bills

logger = logging.getLogger(__name__)


def _next_occurrence(day_of_month, today=None):
    """The next date on or after today where day-of-month equals day_of_month.

    Clamps to end-of-month if day_of_month > days in that month (e.g. 31 in Feb).
    """
    today = today or date.today()
    for offset in (0, 1, 2):
        year = today.year
        month = today.month + offset
        while month > 12:
            month -= 12
            year += 1
        last_day = calendar.monthrange(year, month)[1]
        clamped_day = min(day_of_month, last_day)
        candidate = date(year, month, clamped_day)
        if candidate >= today:
            return candidate
    return None


def fetch_upcoming_bills(today=None):
    """Returns list of bill dicts:
        {expense, amount, account, billing_day, category, fixed_variable,
         notes, next_due_date, days_until_due, skip_reason}
    sorted by next_due_date. Rows that can't be scheduled (missing amount or
    billing_day) get next_due_date=None and a skip_reason explaining why.
    """
    today = today or date.today()
    raw = overhead_bills()

    bills = []
    for r in raw:
        expense = (r.get("expense") or "").strip()
        if not expense:
            continue

        amount = r.get("amount")
        billing_day = r.get("billing_day")

        bill = {
            "expense": expense,
            "amount": float(amount) if amount is not None else None,
            "account": (r.get("account") or "").strip(),
            "billing_day": billing_day,
            "category": (r.get("category") or "").strip(),
            "fixed_variable": (r.get("fixed_variable") or "Fixed").strip(),
            "notes": (r.get("notes") or "").strip(),
            "next_due_date": None,
            "days_until_due": None,
            "skip_reason": None,
        }

        if amount is None and billing_day is None:
            bill["skip_reason"] = "amount and billing date both unset"
        elif amount is None:
            bill["skip_reason"] = "amount unset"
        elif billing_day is None:
            bill["skip_reason"] = "billing date unset/irregular"
        else:
            nd = _next_occurrence(int(billing_day), today)
            if nd:
                bill["next_due_date"] = nd
                bill["days_until_due"] = (nd - today).days

        bills.append(bill)

    def sort_key(b):
        if b["next_due_date"] is None:
            return (1, 99999)
        return (0, b["days_until_due"])
    bills.sort(key=sort_key)

    logger.info(
        f"Overhead: loaded {len(bills)} bills from config, "
        f"{sum(1 for b in bills if b['skip_reason'])} skipped"
    )
    return bills


def bills_in_window(bills, days):
    """Subset of bills with days_until_due in [0, days]."""
    return [b for b in bills if b["days_until_due"] is not None and 0 <= b["days_until_due"] <= days]
