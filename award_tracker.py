"""Weekly municipal bid AWARD tracker.

bid_scanner.py catches bid OPPORTUNITIES; this module catches the other end
of the pipeline: who actually WON green-industry contracts in the NW suburbs
and at what price. That is real market pricing for municipal work.

Sources (researched 2026-07-11):
  - CivicClerk agenda APIs for Arlington Heights and Palatine. Both villages
    publish board/council agenda packets through CivicClerk, whose public
    portal is backed by an unauthenticated JSON API
    (https://{site}.api.civicclerk.com/v1). Agenda items carry the full staff
    report text, which names the winning contractor and the dollar amount
    ("award a contract to X in the amount of $Y"). This is where awards are
    actually announced; villages rarely publish award notices in the paper.
  - Daily Herald bid-notices RSS, opportunistically. The rare published
    "notice of award / bid results" would land in the same feed the bid
    scanner reads; the RSS carries the full notice text so it costs one
    request to check.

Ruled out:
  - Daily Herald keyword search (the ?keyword= param is ignored server-side).
  - Palatine Bids.aspx "Show Closed/Awarded" view (three bids total in the
    module; detail pages show a bare status, no winner or price).
  - vah.com bid page (opportunities only, no results tab).
  - Mount Prospect / Des Plaines sites (WAF-blocked, per bid_scanner).

Anything scraped is untrusted DATA: we only pattern-match text, never act on
instructions found in a page or API payload.

New awards are appended to the visible "Bid Awards" Sheet tab so the pricing
history accumulates somewhere browsable, and deduped via the hidden
"_awardstate" tab (same pattern as bid_scanner's _bidstate).
"""
import hashlib
import json
import logging
import os
import re
from datetime import date, datetime, timedelta

import requests
from gspread.exceptions import WorksheetNotFound

from jobber_sync import get_sheets_client
from bid_scanner import GREEN_RE, HEADERS, _get, _muni

logger = logging.getLogger(__name__)

STATE_TAB = "_awardstate"
AWARDS_TAB = "Bid Awards"
AWARDS_HEADER = ["Date Found", "Agency", "Contract", "Winner", "Amount", "Source URL"]
SEEN_CAP = 2000  # bound the stored id list
LOOKBACK_DAYS = 35  # weekly run; overlap so a missed week still gets caught

# CivicClerk-published villages: api subdomain, display name, and which
# meeting bodies to scan (regex on categoryName).
CIVICCLERK_SITES = [
    ("arlingtonheightsil", "Village of Arlington Heights",
     re.compile(r"Village Board|Committee of the Whole", re.I)),
    ("palatineil", "Village of Palatine",
     re.compile(r"Village Council", re.I)),
    ("mountprospectil", "Village of Mount Prospect",
     re.compile(r"Village Board", re.I)),
    ("desplainesil", "City of Des Plaines",
     re.compile(r"City Council", re.I)),
    ("wheelingil", "Village of Wheeling",
     re.compile(r"Village Board", re.I)),
    ("buffalogroveil", "Village of Buffalo Grove",
     re.compile(r"Village Board", re.I)),
    ("prospectheightsil", "City of Prospect Heights",
     re.compile(r"City Council", re.I)),
    ("hoffmanestatesil", "Village of Hoffman Estates",
     re.compile(r"Village Board", re.I)),
]

# Award/contract language required in the agenda item TITLE. Titles are
# terse; descriptions are full of boilerplate, so gating on the title keeps
# precision high.
AWARD_TITLE_RE = re.compile(
    r"\b(?:award\w*|lowest responsible bidder|accept\w* (?:of )?(?:the )?(?:low )?bid|"
    r"bid results|bid tabulation|"
    r"(?:approv\w+|authoriz\w+|ratif\w+|execut\w+)\b[^.]{0,80}?\b(?:contract|agreement)|"
    r"contract (?:extension|renewal|award))\b",
    re.I,
)

# Award-announcement phrasing for legal notices. Deliberately does NOT match
# the "reserves the right to ... award" boilerplate found in bid invitations.
AWARD_NOTICE_RE = re.compile(
    r"\b(?:notice of award|bid results|bid tabulation|resolution awarding|"
    r"contract (?:was|has been|is hereby) awarded)\b",
    re.I,
)

AMOUNT_RE = re.compile(r"\$\s?\d{1,3}(?:,\d{3})*(?:\.\d{1,2})?|\$\s?\d{4,9}(?:\.\d{1,2})?")
# "in the amount of $X" / "not to exceed $X" style, preferred over any stray
# dollar figure (budgets, per-unit prices) elsewhere in the text.
AMOUNT_CTX_RE = re.compile(
    r"(?:amount of|not[- ]to[- ]exceed|not exceed|total (?:cost|amount|price) of|"
    r"in an amount up to|sum of|bid (?:price |amount )?of)\s*"
    r"(\$\s?\d{1,3}(?:,\d{3})*(?:\.\d{1,2})?)", re.I)
# "... contract/agreement/bid ... to/with <Company>" - company name runs until
# a stop word or punctuation.
WINNER_RE = re.compile(
    r"\b(?:contract|agreement|bid|proposal|purchase order|work)\b[^.;]{0,120}?"
    r"\b(?:to|with|from)\s+"
    r"([A-Z][A-Za-z0-9&.,'’\- ]{2,60}?(?:,? (?:Inc|LLC|L\.L\.C|Ltd|Co|Corp|Corporation|Company)\.?)?)"
    r"(?=\s+(?:of|in|for|at|located|per|the|dba)\b|[.,;:)]|\s*$)")
# Suffix-anchored fallback: any "Name, Inc./LLC/..." mention.
COMPANY_RE = re.compile(
    r"\b([A-Z][A-Za-z0-9&.'’\- ]{2,50}?,? (?:Inc|LLC|L\.L\.C|Ltd|Corp|Corporation|Company)\.?)(?=[\s.,;)]|$)")


def _clean(html_text):
    """Strip tags/entities and collapse whitespace in scraped text."""
    t = re.sub(r"<[^>]+>", " ", html_text or "")
    t = re.sub(r"&amp;", "&", t)
    t = re.sub(r"&[a-z#0-9]+;", " ", t)
    return re.sub(r"\s+", " ", t).strip()


def _extract_amount(text):
    """Best-guess contract dollar amount from a text blob, or None."""
    m = AMOUNT_CTX_RE.search(text or "")
    if m:
        return m.group(1).replace(" ", "")
    amts = AMOUNT_RE.findall(text or "")
    if not amts:
        return None
    # No context phrase: take the largest figure (contract totals dwarf the
    # per-unit prices and fee lines that also show up).
    def _val(a):
        try:
            return float(a.replace("$", "").replace(",", "").replace(" ", ""))
        except ValueError:
            return 0.0
    return max(amts, key=_val).replace(" ", "")


# Things WINNER_RE can grab that are not contractors: municipal bodies and
# street names / street ranges ("Arlington Heights Rd to Weber Dr").
NOT_A_WINNER_RE = re.compile(
    r"^(?:The |Village|City|Town|County|Staff|Board)\b"
    r"|\b(?:Rd|Road|Dr|Drive|Ave|Avenue|St|Street|Ln|Lane|Blvd|Boulevard|Ct|Court|Pkwy|Parkway|Hwy|Route)\.?$"
    r"|\s\bto\s")


def _tidy_name(name):
    """Drop leading ALL-CAPS junk (bid-tabulation headers like
    'BIDDER TOTAL BID Builders Paving, LLC') and trim punctuation."""
    name = re.sub(r"^(?:[A-Z][A-Z&.]+\s+)+(?=[A-Z][a-z])", "", name.strip(" ,.;"))
    return name.strip(" ,.;")[:80]


def _extract_winner(text):
    """Best-guess winning-contractor name from a text blob, or None."""
    m = WINNER_RE.search(text or "")
    if m:
        name = _tidy_name(m.group(1))
        if name and not NOT_A_WINNER_RE.search(name):
            return name
    m = COMPANY_RE.search(text or "")
    if m:
        return _tidy_name(m.group(1)) or None
    return None


# --------------------------------------------------------------------------
# Source: CivicClerk agenda items (Arlington Heights + Palatine)
# --------------------------------------------------------------------------
def _cc_events(site, since, until, max_pages=25):
    """Published meetings for a CivicClerk site in [since, until). The API
    caps pages at ~15 rows regardless of $top, so paginate with $skip."""
    out, skip = [], 0
    for _ in range(max_pages):
        r = requests.get(
            f"https://{site}.api.civicclerk.com/v1/Events",
            params={
                "$filter": (f"eventDate gt {since:%Y-%m-%d}T00:00:00Z "
                            f"and eventDate lt {until:%Y-%m-%d}T23:59:59Z"),
                "$orderby": "eventDate desc",
                "$top": "100",
                "$skip": str(skip),
            },
            headers=HEADERS, timeout=30)
        r.raise_for_status()
        page = r.json().get("value", [])
        out.extend(page)
        if not page:
            break
        skip += len(page)
    return out


def _cc_items(site, agenda_id):
    """Flattened agenda items (with nested childItems) for one meeting."""
    r = requests.get(f"https://{site}.api.civicclerk.com/v1/Meetings/{agenda_id}",
                     headers=HEADERS, timeout=60)
    r.raise_for_status()

    def flat(items):
        for it in items:
            yield it
            yield from flat(it.get("childItems") or [])
    return list(flat(r.json().get("items") or []))


def scan_civicclerk():
    """Green-industry contract/award agenda items from village board and
    council meetings. Item titles carry the contract; the attached staff
    report (agendaObjectItemDescription) usually names the winner and the
    dollar amount."""
    out = []
    until = date.today()
    since = until - timedelta(days=LOOKBACK_DAYS)
    for site, agency, body_re in CIVICCLERK_SITES:
        try:
            events = [e for e in _cc_events(site, since, until)
                      if body_re.search(e.get("categoryName") or "")
                      and e.get("agendaId")]
        except (requests.RequestException, ValueError) as e:
            logger.error(f"Awards/CivicClerk {site}: events fetch failed: {e}")
            continue
        for ev in events:
            try:
                items = _cc_items(site, ev["agendaId"])
            except (requests.RequestException, ValueError) as e:
                logger.error(f"Awards/CivicClerk {site}: meeting {ev['agendaId']} failed: {e}")
                continue
            meeting_date = (ev.get("eventDate") or "")[:10]
            for it in items:
                title = _clean(it.get("agendaObjectItemName") or "")
                if len(title) < 20 or not AWARD_TITLE_RE.search(title):
                    continue
                desc = _clean(it.get("agendaObjectItemDescription") or "")
                blob = f"{title} {desc}"
                if not GREEN_RE.search(blob):
                    continue
                # Key on the normalized title, not the CivicClerk item id: the
                # same contract shows up at Committee of the Whole and again on
                # the board consent agenda under different item ids, and one
                # row per contract is what we want.
                item_id = hashlib.md5(f"{site}:{title.lower()}".encode()).hexdigest()[:16]
                out.append({
                    "agency": agency,
                    "contract_title": (f"{title} ({ev.get('categoryName', '')} {meeting_date})")[:200],
                    "winner": _extract_winner(blob),
                    "amount": _extract_amount(blob),
                    "source": f"CivicClerk agenda ({agency.split(' of ')[-1]})",
                    "url": f"https://{site}.portal.civicclerk.com/event/{ev['id']}/overview",
                    "found_date_placeholder": date.today().isoformat(),
                    "id": f"cc:{site}:{item_id}",
                })
        logger.info(f"Awards/CivicClerk {site}: {len(events)} meetings scanned.")
    return out


# --------------------------------------------------------------------------
# Source: Daily Herald bid-notices RSS (published award notices, rare)
# --------------------------------------------------------------------------
DH_RSS = "https://marketplace.dailyherald.com/il/bid-notices/rss.xml"


def scan_daily_herald_awards():
    """Published notice-of-award / bid-results legal notices. Uncommon (the
    villages announce awards at board meetings instead), but the RSS carries
    full notice text so checking is one cheap request."""
    out = []
    try:
        r = _get(DH_RSS)
        r.raise_for_status()
    except requests.RequestException as e:
        logger.error(f"Awards/DailyHerald: RSS fetch failed: {e}")
        return out
    for item in re.findall(r"<item>(.*?)</item>", r.text, re.S):
        def _tag(name):
            m = re.search(rf"<{name}>(?:<!\[CDATA\[)?(.*?)(?:\]\]>)?</{name}>", item, re.S)
            return _clean(m.group(1)) if m else ""
        title, desc, link = _tag("title"), _tag("description"), _tag("link")
        blob = f"{title} {desc}"
        if not (AWARD_NOTICE_RE.search(blob) and GREEN_RE.search(blob) and link):
            continue
        out.append({
            "agency": _muni(blob) or "(see notice)",
            "contract_title": title[:200] or "(award notice)",
            "winner": _extract_winner(blob),
            "amount": _extract_amount(blob),
            "source": "Daily Herald legal notice",
            "url": link,
            "found_date_placeholder": date.today().isoformat(),
            "id": "dhaward:" + link.rstrip("/").split("/")[-1],
        })
    logger.info(f"Awards/DailyHerald: {len(out)} award notices.")
    return out


# --------------------------------------------------------------------------
# Orchestration
# --------------------------------------------------------------------------
def scan_awards():
    """All sources, error-isolated; deduped by id within the run."""
    found = []
    for fn in (scan_civicclerk, scan_daily_herald_awards):
        try:
            found.extend(fn())
        except Exception as e:  # one dead source must not kill the run
            logger.error(f"Awards: source {fn.__name__} crashed: {e}")
    by_id = {}
    for a in found:
        by_id.setdefault(a["id"], a)
    return list(by_id.values())


def _sheet():
    sid = os.getenv("GOOGLE_SHEET_ID")
    if not sid:
        raise RuntimeError("GOOGLE_SHEET_ID not set; can't persist award state.")
    return get_sheets_client().open_by_key(sid)


def _state_ws(sh):
    try:
        return sh.worksheet(STATE_TAB)
    except WorksheetNotFound:
        ws = sh.add_worksheet(title=STATE_TAB, rows=5, cols=2)
        try:
            ws.hide()
        except Exception:
            pass
        return ws


def _load_seen(sh):
    try:
        raw = _state_ws(sh).acell("A1").value
        return set(json.loads(raw)) if raw else set()
    except Exception as e:
        logger.warning(f"Awards: seen-state read failed ({e}); treating all as new.")
        return set()


def _save_seen(sh, seen):
    try:
        trimmed = list(seen)[-SEEN_CAP:]
        _state_ws(sh).update("A1", [[json.dumps(trimmed)]])
    except Exception as e:
        logger.error(f"Awards: seen-state write failed ({e}); may repeat next run.")


def _awards_ws(sh):
    """Visible 'Bid Awards' tab; created with a header row when missing."""
    try:
        ws = sh.worksheet(AWARDS_TAB)
    except WorksheetNotFound:
        ws = sh.add_worksheet(title=AWARDS_TAB, rows=200, cols=len(AWARDS_HEADER))
        ws.update("A1", [AWARDS_HEADER])
        try:
            ws.format("A1:F1", {"textFormat": {"bold": True}})
        except Exception:
            pass
    return ws


def run_weekly(dry_run=True):
    """Scan all sources, dedupe against the hidden _awardstate tab, and
    append the NEW awards to the visible 'Bid Awards' tab (skipped when
    dry_run). Returns {"new": [...], "total_seen": n} for the caller (the
    daily digest folds new awards into its email itself)."""
    awards = scan_awards()
    sh = _sheet()
    seen = _load_seen(sh)
    new = [a for a in awards if a["id"] not in seen]
    new.sort(key=lambda a: (a["agency"], a["contract_title"]))
    logger.info(f"Awards: {len(awards)} matched, {len(new)} new.")

    if new and not dry_run:
        today = datetime.now().strftime("%Y-%m-%d")
        rows = [[today, a["agency"], a["contract_title"], a["winner"] or "",
                 a["amount"] or "", a["url"]] for a in new]
        _awards_ws(sh).append_rows(rows, value_input_option="USER_ENTERED")
        _save_seen(sh, seen | {a["id"] for a in awards})

    return {"new": new, "total_seen": len(seen | {a["id"] for a in awards})}
