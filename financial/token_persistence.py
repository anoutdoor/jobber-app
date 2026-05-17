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
    """Return {access_token, refresh_token, realm_id} for the provider, or {}.

    Retries once on transient Sheets failures (Google API hiccups at the
    exact moment of the 6am cron could otherwise blank out all three data
    sources at once because they all chain through this).
    """
    import time as _time

    last_err = None
    for attempt in (1, 2):
        try:
            ws = _ws()
            rows = ws.get_all_records()
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
        except Exception as e:
            last_err = e
            if attempt == 1:
                logger.warning(f"Token store ({provider}): read attempt 1 failed ({e}); retrying in 2s.")
                _time.sleep(2)
            else:
                logger.error(f"Token store ({provider}): read failed both attempts ({e}).")
    return {}


def write_tokens(provider, tokens):
    """Write tokens for the provider with retries. Creates the row if missing,
    updates if present. Returns True on success. Loud failure logging so we
    notice when Sheets drifts behind the file's freshest tokens — that's the
    root cause of the rotating-refresh-token failures.
    """
    import time as _time

    payload = [
        provider,
        tokens.get("access_token", ""),
        tokens.get("refresh_token", ""),
        tokens.get("realm_id", ""),
        datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC"),
    ]

    last_err = None
    for attempt in (1, 2, 3):
        try:
            ws = _ws()
            rows = ws.get_all_records()
            target_row_index = None
            for i, row in enumerate(rows, start=2):  # +1 header, +1 1-indexed
                if str(row.get("provider", "")).lower() == provider.lower():
                    target_row_index = i
                    break

            if target_row_index:
                ws.update(f"A{target_row_index}:E{target_row_index}", [payload])
            else:
                ws.append_row(payload)
            logger.info(f"Token store: wrote {provider} tokens to Sheets row (attempt {attempt}).")
            return True
        except Exception as e:
            last_err = e
            logger.warning(f"Token store ({provider}): write attempt {attempt} failed ({e}).")
            if attempt < 3:
                _time.sleep(2 ** attempt)  # 2s, 4s

    logger.error(
        f"Token store ({provider}): write FAILED after 3 attempts ({last_err}). "
        f"Sheets is now BEHIND the local file — next Railway deploy will "
        f"likely fall back to stale tokens. Re-auth via /login or /qbo/login."
    )
    return False
