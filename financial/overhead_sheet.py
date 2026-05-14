"""Reads the recurring overhead sheet (rent, insurance, subscriptions, etc.)
and turns it into a list of upcoming bills with concrete next-due dates.

Reuses get_sheets_client() from jobber_sync so OAuth is shared.
"""
import re
import logging
import calendar
from datetime import date

from jobber_sync import get_sheets_client
from financial.config import overhead_settings

logger = logging.getLogger(__name__)


def _parse_amount(raw):
    """Parse a currency-ish cell value to float. Returns None for blank/N/A."""
    if raw is None:
        return None
    s = str(raw).strip()
    if not s or s.upper() == "N/A":
        return None
    # Strip $ , spaces
    cleaned = re.sub(r"[^\d.\-]", "", s)
    if not cleaned:
        return None
    try:
        return round(float(cleaned), 2)
    except ValueError:
        return None


_DAY_RE = re.compile(r"^\s*(\d{1,2})(?:st|nd|rd|th)?\s*$", re.IGNORECASE)


def _parse_day_of_month(raw):
    """Parse '1st', '22nd', '16', etc. to int 1-31. Returns None if unparseable."""
    if raw is None:
        return None
    s = str(raw).strip()
    if not s:
        return None
    m = _DAY_RE.match(s)
    if not m:
        return None
    day = int(m.group(1))
    if 1 <= day <= 31:
        return day
    return None


def _next_occurrence(day_of_month, today=None):
    """The next date on or after today where day-of-month equals day_of_month.

    Clamps to end-of-month if day_of_month > days in that month (e.g. 31 in Feb).
    """
    today = today or date.today()

    # Try this month first
    for candidate_month_offset in (0, 1, 2):
        year = today.year
        month = today.month + candidate_month_offset
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
    """Returns list of dicts:
        {expense, amount, account, billing_day, category, fixed_variable,
         notes, next_due_date, days_until_due}
    sorted by next_due_date. Rows with unparseable amount OR unparseable
    billing day are returned with next_due_date=None and a 'skip_reason'.
    """
    today = today or date.today()
    settings = overhead_settings()
    sheet_id = settings["sheet_id"]
    tab_name = settings["tab_name"]

    if not sheet_id:
        logger.warning("Overhead: no sheet id in env (FINANCIAL_OVERHEAD_SHEET_ID)")
        return []

    try:
        gc = get_sheets_client()
        sheet = gc.open_by_key(sheet_id).worksheet(tab_name)
    except Exception as e:
        logger.error(f"Overhead sheet open failed: {e}")
        return []

    all_values = sheet.get_all_values()
    if len(all_values) < 2:
        return []

    headers = [h.strip() for h in all_values[0]]
    col = {h: i for i, h in enumerate(headers)}

    required_cols = ["Monthly Expense", "Amount", "Billing Date"]
    missing = [c for c in required_cols if c not in col]
    if missing:
        logger.error(f"Overhead: missing required columns {missing}. Got: {headers}")
        return []

    def get(row, key, default=""):
        idx = col.get(key)
        if idx is None or idx >= len(row):
            return default
        return row[idx]

    bills = []
    for row in all_values[1:]:
        expense = get(row, "Monthly Expense").strip()
        if not expense:
            continue

        amount = _parse_amount(get(row, "Amount"))
        billing_day = _parse_day_of_month(get(row, "Billing Date"))
        notes = get(row, "Notes").strip()

        bill = {
            "expense": expense,
            "amount": amount,
            "account": get(row, "Account").strip(),
            "billing_day": billing_day,
            "category": get(row, "Category").strip(),
            "fixed_variable": get(row, "Fixed/Variable").strip(),
            "notes": notes,
            "next_due_date": None,
            "days_until_due": None,
            "skip_reason": None,
        }

        if amount is None and billing_day is None:
            bill["skip_reason"] = "missing amount and billing date"
        elif amount is None:
            bill["skip_reason"] = "missing/unparseable amount"
        elif billing_day is None:
            bill["skip_reason"] = "missing/unparseable billing date"
        else:
            nd = _next_occurrence(billing_day, today)
            if nd:
                bill["next_due_date"] = nd
                bill["days_until_due"] = (nd - today).days

        bills.append(bill)

    # Sort by next_due_date, skipped rows at the end
    def sort_key(b):
        if b["next_due_date"] is None:
            return (1, 99999)
        return (0, b["days_until_due"])
    bills.sort(key=sort_key)

    logger.info(f"Overhead: parsed {len(bills)} bills, "
                f"{sum(1 for b in bills if b['skip_reason'])} skipped")
    return bills


def bills_in_window(bills, days):
    """Subset of bills with days_until_due in [0, days]."""
    return [b for b in bills if b["days_until_due"] is not None and 0 <= b["days_until_due"] <= days]
