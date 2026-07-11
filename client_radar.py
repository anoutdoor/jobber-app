"""Client-moving radar + sold-property history match.

Two jobs, one module:

1. Churn/lead radar: cross-reference recent Cook County sales (as produced by
   new_homeowners.fetch_new_homeowners) against our CURRENT Jobber client
   property addresses. A hit means a client's house just sold: churn warning
   on the client, warm lead on the buyer.

2. "We know this property" tagging: a set of normalized address keys built
   from BOTH the historical quote-analytics CSV (1.8k quotes, 2024-2026) and
   the live Jobber client list, so the weekly new-homeowner digest can tag
   leads at properties we've quoted or serviced before.

Address normalization has to bridge two styles: the assessor's
"991 W CEDAR LN" and the Census/Jobber "991 Cedar Lane". normalize_addr()
canonicalizes case/suffix/directional-words; addr_match_keys() additionally
emits a variant with the directional letter after the house number stripped,
and matching happens on the union of keys from both sides.

Jobber access is READ-ONLY (clients + property addresses, paginated). The
pull is cached in a hidden Google Sheet tab (survives Railway redeploys)
and refreshed weekly.
"""
import base64
import gzip
import json
import logging
import os
import re
import time
from datetime import date, datetime

from gspread.exceptions import WorksheetNotFound

from jobber_sync import get_sheets_client, graphql_request
from financial.token_persistence import read_tokens

logger = logging.getLogger(__name__)

CACHE_TAB = "_clientaddr"
CACHE_MAX_AGE_DAYS = 7
_CHUNK = 40000  # per-cell char budget, same pattern as financial/cash_state.py

QUOTES_CSV = os.getenv(
    "QUOTES_ENRICHED_CSV",
    "/Users/anoutdoorservices/Desktop/AN/quote-analytics/data/quotes_enriched.csv",
)

# ---------------------------------------------------------------------------
# Address normalization
# ---------------------------------------------------------------------------

_SUFFIXES = {
    "STREET": "ST", "AVENUE": "AVE", "AV": "AVE", "AVE.": "AVE",
    "LANE": "LN", "DRIVE": "DR", "DRV": "DR", "ROAD": "RD",
    "COURT": "CT", "CRT": "CT", "PLACE": "PL", "BOULEVARD": "BLVD",
    "BLV": "BLVD", "TERRACE": "TER", "TERR": "TER", "CIRCLE": "CIR",
    "PARKWAY": "PKWY", "PKY": "PKWY", "TRAIL": "TRL", "HIGHWAY": "HWY",
    "SQUARE": "SQ",
}
# Canonical suffix abbreviations, for locating the suffix inside a street
# string (e.g. "10 S WILLE ST 603" has a condo unit after the suffix).
_SUFFIX_ABBRS = set(s.lower() for s in _SUFFIXES.values()) | {"way"}
# Directional words -> single letters, applied anywhere in the string so
# "201 South Owen Street" and "201 S OWEN ST" normalize identically.
_DIR_WORDS = {
    "NORTH": "N", "SOUTH": "S", "EAST": "E", "WEST": "W",
    "NORTHEAST": "NE", "NORTHWEST": "NW", "SOUTHEAST": "SE", "SOUTHWEST": "SW",
}
_DIR_LETTERS = {"N", "S", "E", "W", "NE", "NW", "SE", "SW"}
# Unit designators: everything from this token onward is dropped ("APT 2B").
_UNIT_TOKENS = {"APT", "UNIT", "STE", "SUITE", "BLDG", "FL", "FLOOR", "#"}


def _zip5(zip5):
    """Normalize any zip representation ('60056', '60056.0', '60056-1234',
    60056.0) to a 5-digit string."""
    digits = re.sub(r"\D", "", str(zip5 or ""))
    return digits[:5]


def normalize_addr(street, zip5):
    """Canonical (street_lower, zip5) key for an address.

    Uppercases, strips punctuation, expands directional words to letters
    (SOUTH -> S), abbreviates street-suffix words wherever they appear
    (LANE -> LN; applied to every token so it's symmetric between the
    assessor's abbreviated style and Jobber's spelled-out style), drops
    designated unit/apartment tails (APT 2B, #603), then lowercases.
    Does NOT strip the directional itself; that's addr_match_keys' job.
    """
    s = (street or "").upper().strip()
    s = re.sub(r"[^\w#\s]", " ", s)
    toks = []
    for t in s.split():
        if t in _UNIT_TOKENS or t.startswith("#"):
            break  # drop unit designator and everything after it
        t = _DIR_WORDS.get(t, t)
        toks.append(_SUFFIXES.get(t, t))
    return (" ".join(toks).lower(), _zip5(zip5))


def _street_variants(street_lower):
    """All normalized street-string variants an address should match under:
    with/without the directional after the house number, and with/without a
    bare trailing unit (assessor writes condos as '10 S WILLE ST 603')."""
    variants = {street_lower}
    toks = street_lower.split()
    # strip a bare unit tail: digit-bearing tokens after the street suffix
    for i in range(len(toks) - 1, -1, -1):
        if toks[i] in _SUFFIX_ABBRS:
            tail = toks[i + 1:]
            if tail and all(re.search(r"\d", t) for t in tail):
                variants.add(" ".join(toks[:i + 1]))
            break
    # strip the directional letter(s) right after the house number
    for v in list(variants):
        vt = v.split()
        if (len(vt) >= 3 and re.fullmatch(r"\d+\w?", vt[0])
                and vt[1].upper() in _DIR_LETTERS):
            variants.add(" ".join([vt[0]] + vt[2:]))
    return variants


def addr_match_keys(street, zip5):
    """Set of normalized keys this address should match under: the exact
    normalized key plus variants with the post-house-number directional
    stripped (assessor '991 W CEDAR LN' vs Census '991 CEDAR LN') and any
    bare condo-unit tail stripped. Match two addresses by intersecting
    their key sets."""
    key = normalize_addr(street, zip5)
    return {(v, key[1]) for v in _street_variants(key[0])}


# ---------------------------------------------------------------------------
# Sheet cache (hidden tab, gzip+base64 chunked down column A)
# ---------------------------------------------------------------------------

def _cache_ws():
    sid = os.getenv("GOOGLE_SHEET_ID")
    if not sid:
        raise RuntimeError("GOOGLE_SHEET_ID not set; can't cache client addresses.")
    sh = get_sheets_client().open_by_key(sid)
    try:
        return sh.worksheet(CACHE_TAB)
    except WorksheetNotFound:
        ws = sh.add_worksheet(title=CACHE_TAB, rows=100, cols=2)
        try:
            ws.hide()
        except Exception:
            pass
        return ws


def _cache_load():
    try:
        col = _cache_ws().col_values(1)
    except Exception as e:
        logger.warning(f"client_radar: cache read failed ({e}).")
        return None
    blob = "".join(c for c in col if c)
    if not blob:
        return None
    try:
        return json.loads(gzip.decompress(base64.b64decode(blob)).decode("utf-8"))
    except Exception:
        logger.exception("client_radar: cache decode failed; will refetch.")
        return None


def _cache_save(payload):
    try:
        raw = json.dumps(payload, separators=(",", ":")).encode("utf-8")
        blob = base64.b64encode(gzip.compress(raw)).decode("ascii")
        chunks = [blob[i:i + _CHUNK] for i in range(0, len(blob), _CHUNK)] or [""]
        ws = _cache_ws()
        existing = len(ws.col_values(1))
        ws.update(f"A1:A{len(chunks)}", [[c] for c in chunks])
        if existing > len(chunks):
            ws.batch_clear([f"A{len(chunks) + 1}:A{existing}"])
    except Exception as e:
        logger.error(f"client_radar: cache write failed ({e}); next run refetches.")


# ---------------------------------------------------------------------------
# Jobber pull: every client's property addresses (READ-ONLY)
# ---------------------------------------------------------------------------

_PROPERTIES_QUERY = """
query ClientRadarProperties($cursor: String) {
  properties(first: 100, after: $cursor) {
    nodes {
      address { street city postalCode }
      client { id name isArchived isLead }
    }
    pageInfo { hasNextPage endCursor }
    totalCount
  }
}
"""


def _fetch_client_rows():
    """Paginate every Jobber property (with its client), return list of
    [street, zip5, client_name, client_id, is_active_ish] rows.
    Raises RuntimeError on a hard failure so callers can fall back to cache."""
    access = read_tokens("jobber").get("access_token")
    if not access:
        raise RuntimeError("client_radar: no Jobber access token available.")

    rows = []
    cursor = None
    pages = 0
    while True:
        pages += 1
        if pages > 60:  # safety cap: 60 * 100 = 6000 properties, far past scale
            logger.warning("client_radar: hit page cap pulling properties.")
            break
        body = graphql_request(_PROPERTIES_QUERY, {"cursor": cursor},
                               access_token=access, _retry=False)
        # graphql_request with _retry=False does NOT retry THROTTLED itself,
        # and returns None on transient network/SSL errors. One manual retry
        # after a pause covers both (Jobber restores 500 cost/sec).
        throttled = bool(body) and body.get("errors") and not body.get("data") and any(
            (e.get("extensions") or {}).get("code") == "THROTTLED"
            for e in body["errors"])
        if body is None or throttled:
            reason = "THROTTLED" if throttled else "network error"
            logger.warning(f"client_radar: {reason} on page {pages}; "
                           "sleeping 5s and retrying once.")
            time.sleep(5)
            body = graphql_request(_PROPERTIES_QUERY, {"cursor": cursor},
                                   access_token=access, _retry=False)
        cdata = ((body or {}).get("data") or {}).get("properties")
        if not cdata:
            raise RuntimeError(
                f"client_radar: properties page {pages} failed: "
                f"{json.dumps((body or {}).get('errors', 'no response'))[:300]}")

        for n in cdata.get("nodes", []):
            c = n.get("client") or {}
            active = not (c.get("isArchived") or c.get("isLead"))
            a = n.get("address") or {}
            street = (a.get("street") or "").strip()
            z = _zip5(a.get("postalCode"))
            if street and z and c.get("id"):
                rows.append([street, z, c.get("name", ""), c.get("id"), active])

        page = cdata.get("pageInfo") or {}
        if not page.get("hasNextPage"):
            break
        cursor = page.get("endCursor")
        time.sleep(0.5)  # respect Jobber's cost-based rate limit

    logger.info(f"client_radar: pulled {len(rows)} property addresses "
                f"across {pages} pages.")
    return rows


def get_client_addresses(force_refresh=False):
    """Dict mapping normalized (street_lower, zip5) -> client info:
    {"client_name", "client_id", "is_active_ish"}.

    Pulls all client property addresses from Jobber (read-only, paginated),
    cached in the hidden '_clientaddr' Sheet tab for CACHE_MAX_AGE_DAYS.
    Active (non-archived, non-lead) clients win key collisions."""
    cached = None if force_refresh else _cache_load()
    rows = None
    if cached:
        try:
            age = (date.today()
                   - datetime.strptime(cached["cached_at"], "%Y-%m-%d").date()).days
            if age <= CACHE_MAX_AGE_DAYS:
                rows = cached["rows"]
        except Exception:
            rows = None

    if rows is None:
        try:
            rows = _fetch_client_rows()
            _cache_save({"cached_at": date.today().isoformat(), "rows": rows})
        except Exception as e:
            if cached and cached.get("rows"):
                logger.error(f"client_radar: Jobber pull failed ({e}); "
                             f"using stale cache from {cached.get('cached_at')}.")
                rows = cached["rows"]
            else:
                raise

    out = {}
    for street, z, name, cid, active in rows:
        info = {"client_name": name, "client_id": cid, "is_active_ish": bool(active)}
        for key in addr_match_keys(street, z):
            # Prefer active clients when two clients share a normalized key.
            if key in out and out[key]["is_active_ish"] and not active:
                continue
            out[key] = info
    return out


# ---------------------------------------------------------------------------
# Radar: which current clients' houses just sold
# ---------------------------------------------------------------------------

def find_client_moves(sales, client_index=None):
    """Match county sale records against our client property addresses.

    sales: list of dicts with at least address + zip (accepts the lead dicts
    new_homeowners.fetch_new_homeowners returns: address/zip/sale_date/
    price or sale_price/buyer or buyer_name).
    Returns [{"client_name", "address", "sale_date", "sale_price",
    "buyer_name"}], deduped per client+address."""
    if client_index is None:
        client_index = get_client_addresses()

    hits, seen = [], set()
    for s in sales:
        street = s.get("address") or s.get("street") or ""
        z = s.get("zip") or s.get("zip5") or ""
        info = None
        for key in addr_match_keys(street, z):
            info = client_index.get(key)
            if info:
                break
        if not info:
            continue
        dedupe = (info["client_id"], normalize_addr(street, z))
        if dedupe in seen:
            continue
        seen.add(dedupe)
        hits.append({
            "client_name": info["client_name"],
            "address": street,
            "sale_date": (s.get("sale_date") or "")[:10],
            "sale_price": float(s.get("sale_price") or s.get("price") or 0),
            "buyer_name": (s.get("buyer_name") or s.get("buyer") or "").title(),
        })
    return hits


# ---------------------------------------------------------------------------
# Sold-property history match: "we know this property"
# ---------------------------------------------------------------------------

def _quote_history_keys():
    """Normalized match keys from the historical quote CSV (best-effort:
    an empty set if the CSV isn't present, e.g. on Railway)."""
    keys = set()
    try:
        import csv as _csv
        with open(QUOTES_CSV, newline="", encoding="utf-8") as f:
            for row in _csv.DictReader(f):
                street, z = row.get("street", ""), _zip5(row.get("zip"))
                if street and z:
                    keys |= addr_match_keys(street, z)
    except FileNotFoundError:
        logger.warning(f"client_radar: quote CSV not found at {QUOTES_CSV}; "
                       "known-property set will be Jobber-only.")
    except Exception:
        logger.exception("client_radar: quote CSV parse failed.")
    return keys


def known_property_keys(client_index=None):
    """Set of normalized (street_lower, zip5) keys for every property we
    'know': historical quotes (won or lost) plus current Jobber client
    properties. Tag a new-homeowner lead as a known property when
    addr_match_keys(lead_address, lead_zip) & known_property_keys()."""
    if client_index is None:
        client_index = get_client_addresses()
    return _quote_history_keys() | set(client_index.keys())
