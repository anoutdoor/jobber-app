"""Marketing ROI dashboard engine.

Pulls every Jobber client with its lead source (the built-in `leadSource` field
Alex fills in on each new client) plus that client's invoiced revenue, reads
marketing spend from a Google Sheet tab, and computes per-channel performance:
new clients, cost per lead, cost per client, revenue attributed, and ROI.

Data model
----------
- Client attribution comes straight from Jobber's `Client.leadSource` (falls back
  to `sourceAttribution.displayLeadSource` / `sourceText` when leadSource is blank).
- Revenue per client = sum of that client's invoice totals (what we've billed them).
  A client is "won" once it has converted out of lead status (`isLead == false`).
- Spend is logged by hand (door hangers, the monthly SEO fee, FB boosts) into the
  `_marketing_spend` tab so it survives Railway redeploys and stays phone-editable.
- A client is counted in the period it was WON (its createdAt), and its lifetime
  revenue is attributed there, so ROI answers "what did clients from this source
  bring in" rather than "what work happened this month".

The Jobber client pull is cached to a local JSON file (rebuildable on a cold
start, refreshed nightly by the scheduler). Spend lives only in Sheets.
"""
import os
import json
import logging
from datetime import datetime, date, timezone

from gspread.exceptions import WorksheetNotFound

from jobber_sync import (
    graphql_request,
    get_sheets_client,
    utc_iso_to_central_date,
    CENTRAL_TZ,
)

logger = logging.getLogger(__name__)

CACHE_FILE = "marketing_cache.json"
SPEND_TAB = "_marketing_spend"
SPEND_HEADERS = [
    "id", "channel", "kind", "amount", "date", "end_date",
    "quantity", "neighborhood", "note", "created_at",
]

# The fixed list of channels the dashboard reports on. Google/Meta Ads sit here
# as placeholders until those campaigns actually start. "Unattributed" is shown
# separately as a data-quality signal (clients with no lead source set).
CANONICAL_SOURCES = [
    "SEO", "Door Hanger", "Facebook Group", "Referral", "Google Ads", "Meta Ads", "Other",
]
UNATTRIBUTED = "Unattributed"

# Ordered keyword rules, first match wins. Paid-ads rules come BEFORE the organic
# ones so "google ads" -> Google Ads (not SEO) and "facebook ad" -> Meta Ads
# (not Facebook Group). Everything Alex might type into Jobber's lead source
# should land in exactly one canonical bucket; tweak freely as real values show up.
SOURCE_RULES = [
    (("google ad", "adwords", "ppc", "sem", "paid search", "google ppc"), "Google Ads"),
    (("facebook ad", "fb ad", "meta ad", "instagram ad", "insta ad", "ig ad", "boosted"), "Meta Ads"),
    (("door hanger", "doorhanger", "door-hanger", "hanger", "flyer", "door knock", "door-knock"), "Door Hanger"),
    (("facebook", "fb group", "fb ", " fb", "meta group", "neighborhood group", "community group"), "Facebook Group"),
    (("referral", "referred", "refer", "word of mouth", "neighbor", "friend", "family", "recommend", "repeat"), "Referral"),
    (("seo", "search", "organic", "google", "website", "web search", "online", "bing", "yelp", "gmb", "maps"), "SEO"),
]


def normalize_source(raw):
    """Map a free-text Jobber lead source onto one canonical channel."""
    s = (raw or "").strip().lower()
    if not s:
        return UNATTRIBUTED
    for keys, canon in SOURCE_RULES:
        if any(k in s for k in keys):
            return canon
    return "Other"


# ---------------------------------------------------------------------------
# Jobber pull: clients + lead source + invoiced revenue
# ---------------------------------------------------------------------------
CLIENTS_QUERY = """
query MarketingClients($cursor: String) {
  clients(first: 50, after: $cursor) {
    nodes {
      id
      name
      createdAt
      isLead
      leadSource
      sourceAttribution { displayLeadSource sourceText }
      invoices(first: 50) {
        nodes { amounts { total } }
        totalCount
        pageInfo { hasNextPage }
      }
    }
    pageInfo { hasNextPage endCursor }
    totalCount
  }
}
"""


def _raw_source(node):
    """Best available source string for a client: the manual leadSource first,
    then Jobber's source attribution display/text."""
    if node.get("leadSource"):
        return node["leadSource"]
    sa = node.get("sourceAttribution") or {}
    return sa.get("displayLeadSource") or sa.get("sourceText") or ""


def fetch_clients():
    """Paginate all clients and return a flat list of attribution rows. Returns
    None on a hard auth/network failure so callers can fall back to cache."""
    out = []
    cursor = None
    pages = 0
    while True:
        pages += 1
        if pages > 50:  # safety: ~2500 clients, far past our scale
            logger.warning("marketing: hit page cap pulling clients.")
            break
        data = graphql_request(CLIENTS_QUERY, {"cursor": cursor})
        if not data:
            return None if not out else out
        cdata = (data.get("data") or {}).get("clients")
        if not cdata:
            logger.error(f"marketing: unexpected clients response: {json.dumps(data)[:400]}")
            return None if not out else out

        for n in cdata.get("nodes", []):
            inv = n.get("invoices") or {}
            revenue = sum(
                float(((node.get("amounts") or {}).get("total")) or 0)
                for node in (inv.get("nodes") or [])
            )
            if (inv.get("pageInfo") or {}).get("hasNextPage"):
                logger.info(f"marketing: client {n.get('name')} has >50 invoices; revenue capped.")
            created_date = utc_iso_to_central_date(n.get("createdAt"))
            raw = _raw_source(n)
            out.append({
                "id": n.get("id"),
                "name": n.get("name", ""),
                "created_date": created_date,           # YYYY-MM-DD (Central)
                "created_month": created_date[:7],       # YYYY-MM
                "is_lead": bool(n.get("isLead")),
                "raw_source": raw,
                "source": normalize_source(raw),
                "revenue": round(revenue, 2),
                "invoice_count": inv.get("totalCount", 0),
            })

        pi = cdata.get("pageInfo", {})
        if not pi.get("hasNextPage"):
            break
        cursor = pi.get("endCursor")

    logger.info(f"marketing: pulled {len(out)} clients across {pages} page(s).")
    return out


# ---------------------------------------------------------------------------
# Local cache (rebuildable; refreshed nightly + on ?refresh=1)
# ---------------------------------------------------------------------------

def _save_cache(clients):
    payload = {
        "generated_at": datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC"),
        "clients": clients,
    }
    try:
        with open(CACHE_FILE, "w") as f:
            json.dump(payload, f)
    except Exception as e:
        logger.warning(f"marketing: cache write failed ({e}).")
    return payload


def _load_cache():
    if os.path.exists(CACHE_FILE):
        try:
            with open(CACHE_FILE) as f:
                return json.load(f)
        except Exception as e:
            logger.warning(f"marketing: cache read failed ({e}).")
    return None


def get_clients(force=False):
    """Return (clients, generated_at). Uses cache unless force=True or no cache."""
    if not force:
        cached = _load_cache()
        if cached and cached.get("clients") is not None:
            return cached["clients"], cached.get("generated_at")
    clients = fetch_clients()
    if clients is None:
        # Live pull failed — fall back to whatever cache we have.
        cached = _load_cache()
        if cached:
            logger.warning("marketing: live pull failed; serving stale cache.")
            return cached.get("clients", []), cached.get("generated_at")
        return [], None
    payload = _save_cache(clients)
    return clients, payload["generated_at"]


def refresh():
    """Force a live rebuild of the cache (used by the nightly scheduler job)."""
    clients = fetch_clients()
    if clients is None:
        logger.error("marketing: scheduled refresh failed (no client data).")
        return {"status": "error", "message": "Jobber pull failed."}
    _save_cache(clients)
    return {"status": "ok", "clients": len(clients)}


# ---------------------------------------------------------------------------
# Spend storage — a Google Sheet tab (durable across Railway redeploys)
# ---------------------------------------------------------------------------

def _spend_ws():
    sid = os.getenv("GOOGLE_SHEET_ID")
    if not sid:
        raise RuntimeError("GOOGLE_SHEET_ID not set; can't store marketing spend.")
    gc = get_sheets_client()
    sh = gc.open_by_key(sid)
    try:
        return sh.worksheet(SPEND_TAB)
    except WorksheetNotFound:
        ws = sh.add_worksheet(title=SPEND_TAB, rows=200, cols=len(SPEND_HEADERS))
        ws.update("A1", [SPEND_HEADERS])
        return ws


def read_spend():
    """All spend entries as dicts. Empty list on failure (dashboard still works)."""
    try:
        ws = _spend_ws()
        return ws.get_all_records()
    except Exception as e:
        logger.warning(f"marketing: spend read failed ({e}).")
        return []


def add_spend(entry):
    """Append a spend entry. `entry` keys mirror SPEND_HEADERS (id/created_at auto)."""
    ws = _spend_ws()
    now = datetime.now(timezone.utc)
    entry_id = now.strftime("%Y%m%d%H%M%S")
    row = [
        entry_id,
        entry.get("channel", ""),
        entry.get("kind", "one_time"),
        entry.get("amount", ""),
        entry.get("date", ""),
        entry.get("end_date", ""),
        entry.get("quantity", ""),
        entry.get("neighborhood", ""),
        entry.get("note", ""),
        now.strftime("%Y-%m-%d %H:%M UTC"),
    ]
    ws.append_row(row, value_input_option="USER_ENTERED")
    return entry_id


def delete_spend(entry_id):
    """Remove a spend entry by id. Returns True if a row was deleted."""
    ws = _spend_ws()
    values = ws.get_all_values()
    for idx, r in enumerate(values[1:], start=2):  # skip header, 1-indexed
        if r and str(r[0]) == str(entry_id):
            ws.delete_rows(idx)
            return True
    return False


# ---------------------------------------------------------------------------
# Date / range helpers
# ---------------------------------------------------------------------------

def _today():
    return datetime.now(CENTRAL_TZ).date()


def _parse_date(s):
    try:
        return date.fromisoformat(str(s)[:10])
    except (ValueError, TypeError):
        return None


RANGE_LABELS = {
    "30d": "Last 30 days",
    "90d": "Last 90 days",
    "ytd": "This year",
    "all": "All time",
}


def _range_bounds(key):
    today = _today()
    if key == "30d":
        from datetime import timedelta
        return today - timedelta(days=30), today
    if key == "90d":
        from datetime import timedelta
        return today - timedelta(days=90), today
    if key == "all":
        return date(2000, 1, 1), today
    # default: ytd
    return date(today.year, 1, 1), today


def _months_between(lo, hi):
    """Inclusive count of calendar months touched by [lo, hi]."""
    if lo > hi:
        return 0
    return (hi.year - lo.year) * 12 + (hi.month - lo.month) + 1


def _spend_in_range(entry, start, end, today):
    """Dollar spend an entry contributes within [start, end]. One-time spends
    count if their date lands in range; monthly (recurring) spends count one
    multiple of `amount` per calendar month of overlap."""
    try:
        amt = float(entry.get("amount") or 0)
    except (ValueError, TypeError):
        return 0.0
    kind = (entry.get("kind") or "one_time").strip().lower()
    if kind == "monthly":
        s = _parse_date(entry.get("date")) or start
        e = _parse_date(entry.get("end_date")) or today
        lo, hi = max(s, start), min(e, end)
        return amt * _months_between(lo, hi)
    d = _parse_date(entry.get("date"))
    if d and start <= d <= end:
        return amt
    return 0.0


# ---------------------------------------------------------------------------
# Compute the dashboard payload
# ---------------------------------------------------------------------------

def _empty_channel(name):
    return {
        "channel": name, "clients": 0, "won": 0, "open_leads": 0,
        "spend": 0.0, "revenue": 0.0, "quantity": 0,
        "cost_per_lead": None, "cost_per_client": None, "roi": None, "romi": None,
    }


def _finalize(ch):
    ch["spend"] = round(ch["spend"], 2)
    ch["revenue"] = round(ch["revenue"], 2)
    if ch["spend"] > 0:
        ch["cost_per_lead"] = round(ch["spend"] / ch["clients"], 2) if ch["clients"] else None
        ch["cost_per_client"] = round(ch["spend"] / ch["won"], 2) if ch["won"] else None
        ch["roi"] = round(ch["revenue"] / ch["spend"], 2)
        ch["romi"] = round((ch["revenue"] - ch["spend"]) / ch["spend"] * 100, 1)
    return ch


def compute(range_key="ytd"):
    clients, generated_at = get_clients(force=False)
    spend_entries = read_spend()
    today = _today()
    start, end = _range_bounds(range_key if range_key in RANGE_LABELS else "ytd")

    # --- Per-channel aggregation over clients won in range ---
    channels = {name: _empty_channel(name) for name in CANONICAL_SOURCES}
    channels[UNATTRIBUTED] = _empty_channel(UNATTRIBUTED)

    unattributed_names = []
    for c in clients:
        d = _parse_date(c.get("created_date"))
        if not d or not (start <= d <= end):
            continue
        src = c.get("source") or UNATTRIBUTED
        ch = channels.setdefault(src, _empty_channel(src))
        ch["clients"] += 1
        if c.get("is_lead"):
            ch["open_leads"] += 1
        else:
            ch["won"] += 1
        ch["revenue"] += float(c.get("revenue") or 0)
        if src == UNATTRIBUTED:
            unattributed_names.append(c.get("name", ""))

    # --- Spend per channel in range ---
    for e in spend_entries:
        src = normalize_source(e.get("channel"))
        # Spend channels are entered from the canonical list, but normalize so a
        # hand-typed value still lands somewhere sensible.
        ch = channels.setdefault(src, _empty_channel(src))
        ch["spend"] += _spend_in_range(e, start, end, today)
        try:
            ch["quantity"] += int(float(e.get("quantity") or 0)) if _spend_in_range(e, start, end, today) else 0
        except (ValueError, TypeError):
            pass

    # Order: canonical list first, then Unattributed, then any stragglers.
    ordered = [channels[n] for n in CANONICAL_SOURCES]
    ordered += [channels[n] for n in channels if n not in CANONICAL_SOURCES and n != UNATTRIBUTED]
    ordered.append(channels[UNATTRIBUTED])
    for ch in ordered:
        _finalize(ch)

    # --- Totals (exclude Unattributed spend/revenue? No — include everything
    # real. Unattributed has no spend but may have revenue.) ---
    totals = _empty_channel("Total")
    for ch in ordered:
        totals["clients"] += ch["clients"]
        totals["won"] += ch["won"]
        totals["open_leads"] += ch["open_leads"]
        totals["spend"] += ch["spend"]
        totals["revenue"] += ch["revenue"]
        totals["quantity"] += ch["quantity"]
    _finalize(totals)

    # --- Monthly trend: new clients per channel, Jan this year -> current month ---
    trend = _build_trend(clients, today)

    # --- Data quality ---
    in_range = [c for c in clients if (_parse_date(c.get("created_date")) and start <= _parse_date(c["created_date"]) <= end)]
    with_source = sum(1 for c in in_range if c.get("source") != UNATTRIBUTED)
    total_in_range = len(in_range)

    return {
        "generated_at": generated_at,
        "range": {
            "key": range_key if range_key in RANGE_LABELS else "ytd",
            "label": RANGE_LABELS.get(range_key, RANGE_LABELS["ytd"]),
            "start": start.isoformat(),
            "end": end.isoformat(),
        },
        "totals": totals,
        "channels": [ch for ch in ordered if ch["clients"] or ch["spend"] or ch["channel"] in CANONICAL_SOURCES],
        "trend": trend,
        "spend_entries": _sorted_spend(spend_entries),
        "data_quality": {
            "total_clients": total_in_range,
            "with_source": with_source,
            "without_source": total_in_range - with_source,
            "pct_attributed": round(with_source / total_in_range * 100, 1) if total_in_range else 100.0,
            "unattributed_names": unattributed_names[:25],
        },
    }


def summary_block():
    """Compact headline numbers for the Command Center tile. Cache-only: never
    triggers a live Jobber pull, so it can't slow down or break the hub's
    /api/summary. Returns None if there's no cached client data yet."""
    cached = _load_cache()
    if not cached or cached.get("clients") is None:
        return None
    clients = cached["clients"]
    today = _today()
    start = date(today.year, 1, 1)
    in_range = [
        c for c in clients
        if (_parse_date(c.get("created_date")) and _parse_date(c["created_date"]) >= start)
    ]
    new_clients = len(in_range)
    with_source = sum(1 for c in in_range if c.get("source") != UNATTRIBUTED)
    pct = round(with_source / new_clients * 100) if new_clients else 100
    roi = None
    try:
        spend = sum(_spend_in_range(e, start, today, today) for e in read_spend())
        revenue = sum(float(c.get("revenue") or 0) for c in in_range)
        roi = round(revenue / spend, 1) if spend > 0 else None
    except Exception:
        logger.warning("marketing summary_block: spend/ROI unavailable.")
    return {"new_clients_ytd": new_clients, "roi": roi, "pct_attributed": pct}


def _sorted_spend(entries):
    def k(e):
        return str(e.get("date") or "")
    return sorted(entries, key=k, reverse=True)


def _build_trend(clients, today):
    """New-client counts per channel by month, Jan(this year) .. current month."""
    months = [f"{today.year}-{m:02d}" for m in range(1, today.month + 1)]
    series = {name: [0] * len(months) for name in CANONICAL_SOURCES}
    series[UNATTRIBUTED] = [0] * len(months)
    idx = {m: i for i, m in enumerate(months)}
    for c in clients:
        m = c.get("created_month")
        if m in idx:
            src = c.get("source") or UNATTRIBUTED
            series.setdefault(src, [0] * len(months))
            series[src][idx[m]] += 1
    # Drop all-zero series to keep the chart legible (but always keep canonical).
    keep = {name: vals for name, vals in series.items()
            if name in CANONICAL_SOURCES or any(vals)}
    return {"months": months, "series": keep}
