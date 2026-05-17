"""User-editable balance + vendor overrides via Google Sheets tabs.

Instead of editing financial_config.yaml and shipping a deploy to refresh
bank balances or vendor totals, Alex updates them in two sheet tabs:

    "Balance Overrides" — Account ID | Account Name | Balance | As Of | Notes
    "Vendor Balances"   — Vendor | Balance | As Of | Notes

The digest reads these at runtime; non-empty cells beat the YAML config.
Tabs are auto-created with headers (and seeded with the current list on
first creation) so they appear in the sheet automatically.
"""
import os
import logging

from gspread.exceptions import WorksheetNotFound

from jobber_sync import get_sheets_client

logger = logging.getLogger(__name__)

BALANCE_TAB = "Balance Overrides"
BALANCE_HEADERS = ["Account ID", "Account Name", "Balance", "As Of", "Notes"]

VENDOR_TAB = "Vendor Balances"
VENDOR_HEADERS = ["Vendor", "Balance", "As Of", "Notes"]


def _sheet():
    sid = os.getenv("GOOGLE_SHEET_ID")
    if not sid:
        raise RuntimeError("GOOGLE_SHEET_ID not set")
    gc = get_sheets_client()
    return gc.open_by_key(sid)


def _ensure_tab(sh, title, headers, seed_rows=None):
    try:
        return sh.worksheet(title)
    except WorksheetNotFound:
        ws = sh.add_worksheet(title=title, rows=max(20, len(seed_rows or []) + 10), cols=10)
        ws.update("A1", [headers])
        if seed_rows:
            ws.append_rows(seed_rows)
        logger.info(f"Created sheet tab '{title}' with {len(seed_rows or [])} seed row(s).")
        return ws


def _parse_money(raw):
    if raw is None or raw == "":
        return None
    s = str(raw).strip()
    if not s or s.upper() == "N/A":
        return None
    cleaned = s.replace(",", "").replace("$", "").strip()
    if not cleaned:
        return None
    try:
        return float(cleaned)
    except ValueError:
        return None


def read_balance_overrides(seed_accounts=None):
    """Returns dict {account_id_str: {balance, as_of, note}}.

    seed_accounts: list of {id, name} dicts to seed the tab on first creation.
    Empty Balance column = no override.
    """
    try:
        sh = _sheet()
    except Exception as e:
        logger.warning(f"Balance overrides: can't open sheet ({e})")
        return {}

    seed_rows = None
    if seed_accounts:
        seed_rows = [[str(a.get("id", "")), a.get("name", ""), "", "", ""]
                     for a in seed_accounts]

    try:
        ws = _ensure_tab(sh, BALANCE_TAB, BALANCE_HEADERS, seed_rows=seed_rows)
        rows = ws.get_all_records()
    except Exception as e:
        logger.warning(f"Balance overrides: can't read tab ({e})")
        return {}

    out = {}
    for row in rows:
        acc_id = str(row.get("Account ID", "")).strip()
        balance = _parse_money(row.get("Balance"))
        if not acc_id or balance is None:
            continue
        out[acc_id] = {
            "balance": balance,
            "as_of": str(row.get("As Of", "") or "").strip(),
            "note": str(row.get("Notes", "") or "").strip(),
        }
    return out


def read_vendor_overrides(seed_vendors=None):
    """Returns dict {vendor_name: {balance, as_of, note}}.

    Vendor name match is case-insensitive at consumer side.
    """
    try:
        sh = _sheet()
    except Exception as e:
        logger.warning(f"Vendor overrides: can't open sheet ({e})")
        return {}

    seed_rows = None
    if seed_vendors:
        seed_rows = [[v.get("name", ""), "", "", ""] for v in seed_vendors]

    try:
        ws = _ensure_tab(sh, VENDOR_TAB, VENDOR_HEADERS, seed_rows=seed_rows)
        rows = ws.get_all_records()
    except Exception as e:
        logger.warning(f"Vendor overrides: can't read tab ({e})")
        return {}

    out = {}
    for row in rows:
        name = str(row.get("Vendor", "")).strip()
        balance = _parse_money(row.get("Balance"))
        if not name or balance is None:
            continue
        out[name] = {
            "balance": balance,
            "as_of": str(row.get("As Of", "") or "").strip(),
            "note": str(row.get("Notes", "") or "").strip(),
        }
    return out
