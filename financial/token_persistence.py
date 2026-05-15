"""Persistent OAuth token storage backed by a private worksheet in the
job-costing Google Sheet.

Railway's filesystem is ephemeral (wiped on every redeploy), and using env
vars to seed tokens breaks when providers rotate refresh tokens (Jobber
does this on each refresh). Writing tokens to a Sheets row instead means
they survive deploys and the rotation-aware refresh-and-save pattern works
correctly — every refresh writes the fresh tokens back to the same row.

Google's own OAuth tokens can't live here (chicken-egg: we need Sheets
auth to read them), so those stay in token.json / GOOGLE_TOKEN env var.
Jobber and QBO tokens, however, just need *some* persistent store that
Google authentication can reach.

Schema (single worksheet "_tokens"):
    provider | access_token | refresh_token | realm_id | updated_at
    "jobber" | "..."        | "..."         | ""       | "2026-..."
    "qbo"    | "..."        | "..."         | "..."    | "2026-..."
"""
import os
import logging
from datetime import datetime, timezone

import gspread
from gspread.exceptions import WorksheetNotFound

from jobber_sync import get_sheets_client

logger = logging.getLogger(__name__)

TOKEN_TAB = "_tokens"
HEADERS = ["provider", "access_token", "refresh_token", "realm_id", "updated_at"]


def _sheet_id():
    return os.getenv("GOOGLE_SHEET_ID")


def _ws():
    sid = _sheet_id()
    if not sid:
        raise RuntimeError("GOOGLE_SHEET_ID not set; can't persist tokens.")
    gc = get_sheets_client()
    sh = gc.open_by_key(sid)
    try:
        return sh.worksheet(TOKEN_TAB)
    except WorksheetNotFound:
        ws = sh.add_worksheet(title=TOKEN_TAB, rows=10, cols=10)
        ws.update("A1", [HEADERS])
        # Hide the tab from casual viewing (not foolproof but signals "system data")
        try:
            ws.hide()
        except Exception:
            pass
        return ws


def read_tokens(provider):
    """Return {access_token, refresh_token, realm_id} for the provider, or {}."""
    try:
        ws = _ws()
    except Exception as e:
        logger.warning(f"Token store: can't open sheet ({e})")
        return {}

    try:
        rows = ws.get_all_records()
    except Exception as e:
        logger.warning(f"Token store: can't read sheet rows ({e})")
        return {}

    for row in rows:
        if str(row.get("provider", "")).lower() == provider.lower():
            out = {
                "access_token": row.get("access_token") or "",
                "refresh_token": row.get("refresh_token") or "",
            }
            if row.get("realm_id"):
                out["realm_id"] = row["realm_id"]
            return out
    return {}


def write_tokens(provider, tokens):
    """Write tokens for the provider. Creates the row if missing, updates if present."""
    try:
        ws = _ws()
    except Exception as e:
        logger.error(f"Token store: can't open sheet to write ({e})")
        return False

    try:
        rows = ws.get_all_records()
    except Exception as e:
        logger.error(f"Token store: can't read sheet rows before write ({e})")
        return False

    target_row_index = None
    for i, row in enumerate(rows, start=2):  # +1 for header, +1 for 1-indexed
        if str(row.get("provider", "")).lower() == provider.lower():
            target_row_index = i
            break

    payload = [
        provider,
        tokens.get("access_token", ""),
        tokens.get("refresh_token", ""),
        tokens.get("realm_id", ""),
        datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC"),
    ]

    try:
        if target_row_index:
            ws.update(f"A{target_row_index}:E{target_row_index}", [payload])
        else:
            ws.append_row(payload)
        logger.info(f"Token store: wrote {provider} tokens to Sheets row.")
        return True
    except Exception as e:
        logger.error(f"Token store: write failed ({e})")
        return False
