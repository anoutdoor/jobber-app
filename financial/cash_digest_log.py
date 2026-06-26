"""Tiny durable log of the daily cash-digest headline numbers so the email can
show day-over-day movement. One row per send in a _cashdigest Sheet tab. Best
effort: any failure just means no trend arrow that day.
"""
import logging
import os

from gspread.exceptions import WorksheetNotFound

from jobber_sync import get_sheets_client

logger = logging.getLogger(__name__)
_TAB = "_cashdigest"
_HEADER = ["date", "net_position", "free_cash", "free_cash_month", "total_liquid"]


def _ws():
    sid = os.getenv("GOOGLE_SHEET_ID")
    if not sid:
        raise RuntimeError("GOOGLE_SHEET_ID not set")
    gc = get_sheets_client()
    sh = gc.open_by_key(sid)
    try:
        return sh.worksheet(_TAB)
    except WorksheetNotFound:
        ws = sh.add_worksheet(title=_TAB, rows=400, cols=6)
        ws.update("A1:E1", [_HEADER])
        try:
            ws.hide()
        except Exception:
            pass
        return ws


def last_entry(before_date=None):
    """Most recent logged row, optionally strictly before an ISO date."""
    try:
        rows = _ws().get_all_values()
    except Exception:
        logger.exception("cash_digest_log read failed")
        return None
    found = None
    for r in rows[1:]:
        if not r or not r[0] or r[0] == "date":
            continue
        if before_date and r[0] >= before_date:
            continue
        found = r  # appended chronologically, keep the last match
    if not found:
        return None

    def f(i):
        try:
            return float(found[i])
        except (IndexError, ValueError):
            return None

    return {"date": found[0], "net_position": f(1), "free_cash": f(2),
            "free_cash_month": f(3), "total_liquid": f(4)}


def log_today(today_iso, net_position, free_cash, free_cash_month, total_liquid):
    """Append today's row, or overwrite it if already logged today. Best effort."""
    try:
        ws = _ws()
        rows = ws.get_all_values()
        target = next((i + 1 for i, r in enumerate(rows) if r and r[0] == today_iso), None)
        vals = [today_iso, round(net_position, 2), round(free_cash, 2),
                round(free_cash_month, 2), round(total_liquid, 2)]
        if target:
            ws.update(f"A{target}:E{target}", [vals])
        else:
            ws.append_row(vals, value_input_option="RAW")
    except Exception:
        logger.exception("cash_digest_log write failed")
