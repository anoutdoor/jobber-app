"""Hourly at-risk quote flagger.

Scores quotes that entered "Awaiting Response" since the last run using the
logistic model fit on 1,818 labeled quotes (quote-analytics project): win
probability from log(total) + repeat client + discounted, plus the sharper
"quote as % of home value" lens via a live Cook County Assessor parcel
lookup. Quotes with predicted win odds under 40% or priced above 0.5% of the
home's market value get flagged with an internal note on the quote in Jobber
(write-back gated behind JOBBER_RISK_WRITE=1; default is log-only).

High-water mark on the quote sent timestamp lives in a hidden Sheet tab
("_riskstate") so it survives redeploys, same pattern as new_homeowners.py.
"""
import json
import logging
import math
import os
from datetime import datetime, timedelta, timezone

import requests
from gspread.exceptions import WorksheetNotFound

from jobber_sync import graphql_request, get_sheets_client
from financial.token_persistence import read_tokens

logger = logging.getLogger(__name__)

MODEL_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)), "risk_model.json")
COOK_BASE = "https://datacatalog.cookcountyil.gov/resource"
STATE_TAB = "_riskstate"
FIRST_RUN_LOOKBACK_DAYS = 14

# Flag thresholds (from quote-analytics REPORT.md)
WIN_PROB_FLOOR = 0.40
PCT_VALUE_CEILING = 0.005

# A client counts as "repeat" if their Jobber record predates the quote by
# more than this many days (proxy used in the quote-analytics training data).
REPEAT_CLIENT_MIN_DAYS = 30

# Street-suffix / directional tokens for the fuzzy parcel fallback
# (mirrors quote-analytics/fuzzy_match.py).
SUFFIXES = {"LN", "ST", "AVE", "DR", "RD", "CT", "PL", "BLVD", "TER", "TRL",
            "PKWY", "CIR", "WAY", "AV", "HWY"}
DIRS = {"N", "S", "E", "W"}

# Jobber client addresses spell words out ("South Owen Street"); the assessor
# abbreviates ("S OWEN ST"). Normalize to the assessor's convention.
STREET_ABBREV = {
    "NORTH": "N", "SOUTH": "S", "EAST": "E", "WEST": "W",
    "STREET": "ST", "AVENUE": "AVE", "DRIVE": "DR", "ROAD": "RD",
    "COURT": "CT", "PLACE": "PL", "LANE": "LN", "BOULEVARD": "BLVD",
    "TERRACE": "TER", "TRAIL": "TRL", "PARKWAY": "PKWY", "CIRCLE": "CIR",
    "HIGHWAY": "HWY", "CRESCENT": "CRES",
}

# NOTE: the quotes(filter: { sentAt: ... }) range filter returns zero rows on
# API version 2026-03-10 even for a full-year window (verified 2026-07-11), so
# we filter on status server-side and cut on sentAt client-side. The
# awaiting_response set is small (~35 quotes), so this stays cheap.
QUOTES_QUERY = """
query RiskQuotes($cursor: String) {
  quotes(
    first: 25,
    after: $cursor,
    filter: { status: awaiting_response }
  ) {
    nodes {
      id
      quoteNumber
      quoteStatus
      createdAt
      sentAt
      transitionedAt
      client { id name createdAt }
      property {
        address { street city province postalCode }
      }
      amounts { total discountAmount }
    }
    pageInfo { hasNextPage endCursor }
    totalCount
  }
}
"""

QUOTE_NOTE_MUTATION = """
mutation FlagQuote($quoteId: EncodedId!, $message: String!) {
  quoteCreateNote(quoteId: $quoteId, input: { message: $message }) {
    quoteNote { id createdAt }
    userErrors { message path }
  }
}
"""


# ---------------------------------------------------------------------------
# High-water state in a hidden Sheet tab (same pattern as new_homeowners.py)
# ---------------------------------------------------------------------------

def _state_ws():
    sid = os.getenv("GOOGLE_SHEET_ID")
    if not sid:
        raise RuntimeError("GOOGLE_SHEET_ID not set; can't persist risk state.")
    sh = get_sheets_client().open_by_key(sid)
    try:
        return sh.worksheet(STATE_TAB)
    except WorksheetNotFound:
        ws = sh.add_worksheet(title=STATE_TAB, rows=5, cols=2)
        try:
            ws.hide()
        except Exception:
            pass
        return ws


def _load_state():
    # Read without _state_ws() so a dry run doesn't create the tab.
    try:
        sid = os.getenv("GOOGLE_SHEET_ID")
        if not sid:
            return {}
        raw = get_sheets_client().open_by_key(sid).worksheet(STATE_TAB).acell("A1").value
        return json.loads(raw) if raw else {}
    except WorksheetNotFound:
        return {}
    except Exception as e:
        logger.warning(f"QuoteRisk: state read failed ({e}); using empty state.")
        return {}


def _save_state(state):
    try:
        _state_ws().update("A1", [[json.dumps(state)]])
    except Exception as e:
        logger.error(f"QuoteRisk: state write failed ({e}); next run may rescore quotes.")


# ---------------------------------------------------------------------------
# Model
# ---------------------------------------------------------------------------

def _load_model():
    with open(MODEL_PATH) as f:
        return json.load(f)


def _win_probability(model, total, repeat_client, discounted):
    """Logistic win probability from the fitted coefficients."""
    c = model["coefficients"]
    z = (c["intercept"]
         + c["log_total"] * math.log1p(total)
         + c["repeat_client"] * (1 if repeat_client else 0)
         + c["discounted"] * (1 if discounted else 0))
    return 1.0 / (1.0 + math.exp(-z))


def _win_rate_above_ceiling(model):
    """Weighted historical win rate for quotes priced above the pct-of-value
    ceiling (the 'quotes above 0.5% close at X%' number in the note)."""
    bands = [b for b in model["pct_value_bands"] if b["max_pct"] > PCT_VALUE_CEILING]
    n = sum(b.get("n", 0) for b in bands)
    if not n:
        return None
    return sum(b["win_rate"] * b.get("n", 0) for b in bands) / n


# ---------------------------------------------------------------------------
# Cook County parcel lookup: address -> PIN -> assessed value -> market value
# ---------------------------------------------------------------------------

def _soql(dataset, where, select="*", limit=100):
    r = requests.get(f"{COOK_BASE}/{dataset}.json",
                     params={"$where": where, "$select": select, "$limit": limit},
                     timeout=30)
    r.raise_for_status()
    return r.json()


def _core_street(street):
    """'1037 S PLUM TREE LN' -> ('1037', 'PLUM TREE') for the fuzzy fallback."""
    toks = street.split()
    if not toks or not toks[0][:1].isdigit():
        return None, None
    num = toks[0]
    rest = [t for t in toks[1:] if t not in DIRS]
    if rest and rest[-1] in SUFFIXES:
        rest = rest[:-1]
    return num, " ".join(rest)


def _find_pin(street, zip5):
    """Match a street address + zip to a PIN in the assessor address dataset.

    Exact match on prop_address_full first; if that misses (the assessor keeps
    directionals like '991 W CEDAR LN' that client records often drop), retry
    on house number + core street name, accepting only an unambiguous hit.
    """
    toks = (street or "").upper().replace(".", "").replace(",", "").split()
    street = " ".join(STREET_ABBREV.get(t, t) for t in toks)
    if not street or not zip5:
        return None

    esc = street.replace("'", "''")
    rows = _soql("3723-97qp",
                 f"prop_address_full = '{esc}' "
                 f"AND prop_address_zipcode_1 like '{zip5}%' AND year >= '2023'",
                 select="pin,year,prop_address_full")
    if rows:
        return max(rows, key=lambda r: r.get("year", ""))["pin"]

    num, core = _core_street(street)
    if not core:
        return None
    core_esc = core.replace("'", "''")
    rows = _soql("3723-97qp",
                 f"prop_address_full like '{num} %{core_esc}%' "
                 f"AND prop_address_zipcode_1 like '{zip5}%' AND year >= '2023'",
                 select="pin,year,prop_address_full")
    pins = {r["pin"] for r in rows}
    return pins.pop() if len(pins) == 1 else None


def _market_value(pin):
    """Latest assessed total for the PIN x 10 (Cook County assesses residential
    property at 10% of market value). Returns None if no dollar figure."""
    rows = _soql("uzyt-m557", f"pin = '{pin}' AND year >= '2022'",
                 select="pin,year,certified_tot,board_tot,mailed_tot")
    best = None
    for row in rows:
        assessed = next((row[f] for f in ("certified_tot", "board_tot", "mailed_tot")
                         if row.get(f)), None)
        if assessed is None:
            continue
        if best is None or row.get("year", "") > best[0]:
            best = (row.get("year", ""), float(assessed))
    return best[1] * 10 if best else None


def lookup_market_value(street, zip5):
    """Full address -> market value chain. Returns None on any miss."""
    pin = _find_pin(street, zip5)
    if not pin:
        return None
    return _market_value(pin)


# ---------------------------------------------------------------------------
# Jobber fetch + write-back
# ---------------------------------------------------------------------------

def _fetch_new_awaiting_quotes(access, since_iso):
    """Quotes currently awaiting response that were sent after since_iso.

    Pages through the (small) awaiting_response set and applies the sentAt
    cutoff client-side; see the note above QUOTES_QUERY.
    """
    quotes, cursor = [], None
    for _ in range(40):  # runaway guard; hourly runs should be 1-2 pages
        d = graphql_request(QUOTES_QUERY, {"cursor": cursor},
                            access_token=access, _retry=False)
        block = ((d or {}).get("data") or {}).get("quotes")
        if not block:
            raise RuntimeError(f"quotes query failed: {json.dumps(d)[:400]}")
        quotes.extend(block.get("nodes") or [])
        pi = block.get("pageInfo") or {}
        if not pi.get("hasNextPage"):
            break
        cursor = pi.get("endCursor")
    return [q for q in quotes
            if (q.get("sentAt") or q.get("transitionedAt") or "") > since_iso]


def flag_quote_in_jobber(quote_id, note_text, access=None):
    """Create an internal note on the quote via quoteCreateNote.

    Gated: only executes when JOBBER_RISK_WRITE == "1". Otherwise logs the
    would-be note and returns {"skipped": True} so the scoring pipeline can
    run in production without touching Jobber until we flip the switch.
    """
    if os.getenv("JOBBER_RISK_WRITE") != "1":
        logger.info(f"QuoteRisk: write-back disabled; would note quote {quote_id}: {note_text}")
        return {"skipped": True}

    if access is None:
        access = (read_tokens("jobber") or {}).get("access_token")
    d = graphql_request(QUOTE_NOTE_MUTATION,
                        {"quoteId": quote_id, "message": note_text},
                        access_token=access, _retry=False)
    payload = ((d or {}).get("data") or {}).get("quoteCreateNote") or {}
    errors = payload.get("userErrors") or []
    if errors or not payload.get("quoteNote"):
        logger.error(f"QuoteRisk: quoteCreateNote failed for {quote_id}: "
                     f"{errors or json.dumps(d)[:400]}")
        return {"skipped": False, "ok": False, "errors": errors}
    logger.info(f"QuoteRisk: note created on quote {quote_id}.")
    return {"skipped": False, "ok": True, "note_id": payload["quoteNote"]["id"]}


# ---------------------------------------------------------------------------
# Scoring
# ---------------------------------------------------------------------------

def _compose_note(win_prob, total, pct_of_value, band_rate):
    """Internal note text. All numbers computed, no canned figures."""
    parts = [f"Heads up: this quote is high risk (est. win odds ~{win_prob * 100:.0f}%)."]
    if pct_of_value is not None:
        parts.append(
            f"${total:,.0f} is {pct_of_value * 100:.1f}% of this home's value"
        )
        if pct_of_value > PCT_VALUE_CEILING and band_rate is not None:
            parts[-1] += (f"; quotes above {PCT_VALUE_CEILING * 100:.1f}% of home "
                          f"value historically close at {band_rate * 100:.0f}%.")
        else:
            parts[-1] += "."
    parts.append("Consider phasing the work or a tighter follow-up.")
    return " ".join(parts)


def _score_quote(model, q):
    """Score one quote node. Returns the result row dict."""
    total = float((q.get("amounts") or {}).get("total") or 0)
    discounted = float((q.get("amounts") or {}).get("discountAmount") or 0) > 0

    # Repeat client: their Jobber record predates the quote by 30+ days.
    repeat = False
    client = q.get("client") or {}
    try:
        c_created = datetime.fromisoformat(client["createdAt"].replace("Z", "+00:00"))
        q_created = datetime.fromisoformat(q["createdAt"].replace("Z", "+00:00"))
        repeat = (q_created - c_created) > timedelta(days=REPEAT_CLIENT_MIN_DAYS)
    except (KeyError, TypeError, ValueError):
        pass

    win_prob = _win_probability(model, total, repeat, discounted)

    # Parcel lookup (Cook County only; misses just mean no pct-of-value signal)
    addr = (q.get("property") or {}).get("address") or {}
    pct_of_value = None
    try:
        mv = lookup_market_value(addr.get("street"), (addr.get("postalCode") or "")[:5])
        if mv and total > 0:
            pct_of_value = total / mv
    except requests.RequestException as e:
        logger.warning(f"QuoteRisk: parcel lookup failed for quote "
                       f"{q.get('quoteNumber')} ({e}); scoring on model only.")

    at_risk = win_prob < WIN_PROB_FLOOR or (
        pct_of_value is not None and pct_of_value > PCT_VALUE_CEILING)

    note_text = None
    if at_risk:
        band_rate = _win_rate_above_ceiling(model) if pct_of_value else None
        note_text = _compose_note(win_prob, total, pct_of_value, band_rate)

    return {
        "quote_id": q["id"],
        "quote_number": q.get("quoteNumber"),
        "client": client.get("name"),
        "total": total,
        "win_prob": round(win_prob, 3),
        "pct_of_value": round(pct_of_value, 5) if pct_of_value is not None else None,
        "at_risk": at_risk,
        "note_text": note_text,
    }


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

def run_hourly(dry_run=True):
    """Score quotes sent since the last high-water mark; flag the at-risk ones.

    dry_run=True scores and returns results without writing notes or
    advancing the high-water mark. The scheduler calls run_hourly(dry_run=False).
    """
    access = (read_tokens("jobber") or {}).get("access_token")
    if not access:
        logger.error("QuoteRisk: no Jobber access token in Sheets _tokens tab.")
        return {"status": "error", "message": "no access token"}

    state = _load_state()
    since = state.get("last_sent_at")
    if not since:
        since = (datetime.now(timezone.utc)
                 - timedelta(days=FIRST_RUN_LOOKBACK_DAYS)).strftime("%Y-%m-%dT%H:%M:%SZ")
    run_started = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")

    try:
        quotes = _fetch_new_awaiting_quotes(access, since)
    except RuntimeError as e:
        logger.error(f"QuoteRisk: {e}")
        return {"status": "error", "message": str(e)}

    model = _load_model()
    results, flagged, errors = [], 0, 0
    for q in quotes:
        try:
            row = _score_quote(model, q)
        except Exception as e:
            errors += 1
            logger.error(f"QuoteRisk: scoring failed for quote "
                         f"{q.get('quoteNumber')} ({e}); skipping.")
            continue
        if row["at_risk"] and not dry_run:
            flag_quote_in_jobber(row["quote_id"], row["note_text"], access=access)
            flagged += 1
        results.append(row)

    if not dry_run:
        # Advance the mark to when this run started: the sentAt filter is
        # exclusive-after, so anything sent mid-run lands in the next hour.
        _save_state({"last_sent_at": run_started})

    logger.info(f"QuoteRisk: scored {len(results)} quotes since {since}, "
                f"{sum(r['at_risk'] for r in results)} at risk, "
                f"{flagged} flagged, {errors} errors, dry_run={dry_run}.")
    return {
        "status": "ok",
        "since": since,
        "scored": len(results),
        "at_risk": sum(r["at_risk"] for r in results),
        "flagged": flagged,
        "errors": errors,
        "dry_run": dry_run,
        "results": results,
    }
