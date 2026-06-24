"""Cross-device storage for the cash position tracker's state.

Railway's filesystem is ephemeral, so the state lives in a private worksheet in
the job-costing Google Sheet (same durable store as the OAuth tokens). The JSON
is gzipped + base64-encoded and chunked across column A so it survives growth
(snapshots) past the per-cell 50k character limit. Last write wins.
"""
import base64
import gzip
import json
import logging
import os

from gspread.exceptions import WorksheetNotFound

from jobber_sync import get_sheets_client

logger = logging.getLogger(__name__)

_TAB = "_cashstate"
_CHUNK = 40000


def _ws():
    sid = os.getenv("GOOGLE_SHEET_ID")
    if not sid:
        raise RuntimeError("GOOGLE_SHEET_ID not set; can't persist cash state.")
    gc = get_sheets_client()
    sh = gc.open_by_key(sid)
    try:
        return sh.worksheet(_TAB)
    except WorksheetNotFound:
        ws = sh.add_worksheet(title=_TAB, rows=100, cols=2)
        try:
            ws.hide()
        except Exception:
            pass
        return ws


def _encode(state):
    raw = json.dumps(state, separators=(",", ":")).encode("utf-8")
    return base64.b64encode(gzip.compress(raw)).decode("ascii")


def _decode(blob):
    return json.loads(gzip.decompress(base64.b64decode(blob)).decode("utf-8"))


def load_cash_state():
    """Return the stored state dict, or None if nothing saved yet."""
    ws = _ws()
    col = ws.col_values(1)
    blob = "".join(c for c in col if c)
    if not blob:
        return None
    try:
        return _decode(blob)
    except Exception:
        logger.exception("cash_state decode failed")
        return None


def save_cash_state(state):
    """Overwrite the stored state. Returns True on success."""
    blob = _encode(state)
    chunks = [blob[i:i + _CHUNK] for i in range(0, len(blob), _CHUNK)] or [""]
    ws = _ws()
    existing = len(ws.col_values(1))
    ws.update(f"A1:A{len(chunks)}", [[c] for c in chunks])
    if existing > len(chunks):
        ws.batch_clear([f"A{len(chunks) + 1}:A{existing}"])
    return True
