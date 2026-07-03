"""Daily municipal bid-opportunity scanner.

Scrapes public bid/RFP listings, keeps only green-industry work (landscape,
grounds, mowing, snow, tree, drainage, etc.), dedupes against a hidden Sheet
tab, and emails a daily digest of NEW matches.

Sources (chosen with Alex 2026-07):
  - Daily Herald legal/bid notices  -> the paper of record for the NW
    suburbs; Illinois law requires municipalities to publish bid notices in
    a local paper, so this one source catches every village, including the
    ones whose own sites block scrapers (Mount Prospect, Des Plaines).
  - Village of Arlington Heights (vah.com) -> static Revize table.
  - Village of Palatine (palatine.il.us) -> CivicPlus Bids.aspx.

Anything scraped is treated as untrusted DATA: we only pattern-match text,
never act on instructions found in a page.

Manual fire/preview: /bids-now?dry=1
"""
import json
import logging
import os
import re
import time

import requests

from jobber_sync import get_sheets_client
from financial.gmail_send import send_email
from gspread.exceptions import WorksheetNotFound

logger = logging.getLogger(__name__)

UA = ("Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 "
      "(KHTML, like Gecko) Chrome/120.0 Safari/537.36")
HEADERS = {"User-Agent": UA, "Accept": "text/html,application/xhtml+xml",
           "Accept-Language": "en-US,en;q=0.9"}
STATE_TAB = "_bidstate"
DEFAULT_TO = "an.lawnservices1@gmail.com"
SEEN_CAP = 1500  # bound the stored id list

# Green-industry relevance filter. Stems (\w* suffix) for word families,
# whole words guarded by \b to avoid e.g. "tree" inside "street".
GREEN_RE = re.compile(
    r"\b(?:land ?scap\w*|lawn\w*|turf\w*|mow\w*|mulch\w*|sod|seed(?:ing|s)?|"
    r"tree|trees|shrub\w*|planting\w*|plant material|irrigation|sprinkler\w*|"
    r"fertiliz\w*|herbicide\w*|pesticide\w*|weed\w*|brush|vegetation|"
    r"grounds ?keep\w*|grounds maintenance|grounds management|landscape maintenance|"
    r"snow\w*|salt\w*|plow\w*|de-?ic\w*|ice control|parkway\w*|"
    r"right[- ]of[- ]way|arbor\w*|forestry|urban forest\w*|horticultur\w*|"
    r"prairie|naturaliz\w*|native (?:plant|seed|landscap)\w*|restoration|"
    r"erosion|drainage|grading|excavat\w*|retaining wall\w*|paver\w*|"
    r"hardscap\w*|leaf removal|mowing)\b",
    re.I,
)

# Core service-area municipalities (surfaced first). Others still included
# under "wider area."
SERVICE_MUNIS = {
    "mount prospect", "mt prospect", "arlington heights", "palatine",
    "rolling meadows", "prospect heights", "des plaines", "elk grove",
    "wheeling", "buffalo grove", "park ridge", "glenview", "northbrook",
    "hoffman estates", "schaumburg", "inverness", "mount prospect",
}


def _matched_kw(text):
    m = GREEN_RE.search(text or "")
    return m.group(0) if m else None


def _muni(text):
    m = re.search(r"\b((?:village|city|town) of [A-Z][A-Za-z.\- ]{2,30})", text or "")
    if m:
        return re.sub(r"\s+", " ", m.group(1)).strip().rstrip(".")
    return None


def _in_service_area(text):
    t = (text or "").lower()
    return any(muni in t for muni in SERVICE_MUNIS)


def _get(url, **kw):
    return requests.get(url, headers=HEADERS, timeout=30, **kw)


# --------------------------------------------------------------------------
# State (hidden Sheet tab, single JSON cell)
# --------------------------------------------------------------------------
def _state_ws():
    sid = os.getenv("GOOGLE_SHEET_ID")
    if not sid:
        raise RuntimeError("GOOGLE_SHEET_ID not set; can't persist bid state.")
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


def _load_seen():
    try:
        raw = _state_ws().acell("A1").value
        return set(json.loads(raw)) if raw else set()
    except Exception as e:
        logger.warning(f"Bids: seen-state read failed ({e}); treating all as new.")
        return set()


def _save_seen(seen):
    try:
        trimmed = list(seen)[-SEEN_CAP:]
        _state_ws().update("A1", [[json.dumps(trimmed)]])
    except Exception as e:
        logger.error(f"Bids: seen-state write failed ({e}); may repeat next run.")


# --------------------------------------------------------------------------
# Source: Daily Herald bid notices
# --------------------------------------------------------------------------
DH_BASE = "https://marketplace.dailyherald.com"


def scan_daily_herald(fetch_detail=True, detail_cap=30):
    """Listing is server-rendered (title + slug + link). For notices whose
    title/slug don't already match, fetch the detail page once and match its
    og:description (the marketplace embeds the notice text there)."""
    out = []
    try:
        r = _get(f"{DH_BASE}/il/bid-notices/search")
        r.raise_for_status()
    except requests.RequestException as e:
        logger.error(f"Bids/DailyHerald: listing fetch failed: {e}")
        return out

    items = re.findall(r'href="(/il/bid-notices/[a-z0-9-]+/[A-Za-z0-9]+)"[^>]*>(.*?)</a>',
                       r.text, re.S)
    seen_urls, detail_fetches = set(), 0
    for path, raw_title in items:
        if path in seen_urls:
            continue
        seen_urls.add(path)
        title = re.sub(r"<[^>]+>", " ", raw_title)
        title = re.sub(r"\s+", " ", title).replace("Featured -->", "").strip()
        slug = path.split("/")[-2].replace("-", " ")
        if not title:
            title = slug.title()
        haystack = f"{title} {slug}"

        kw = _matched_kw(haystack)
        detail_text = ""
        if not kw and fetch_detail and detail_fetches < detail_cap:
            detail_fetches += 1
            try:
                d = _get(f"{DH_BASE}{path}")
                mm = re.search(
                    r'<meta[^>]+(?:name|property)="(?:description|og:description)"'
                    r'[^>]+content="([^"]+)"', d.text)
                detail_text = mm.group(1) if mm else ""
                kw = _matched_kw(f"{haystack} {detail_text}")
                time.sleep(0.4)
            except requests.RequestException:
                pass
        if not kw:
            continue

        muni = _muni(title) or _muni(detail_text)
        out.append({
            "source": "Daily Herald",
            "title": title[:120] or "(bid notice)",
            "agency": muni or "(see notice)",
            "due": "",
            "url": f"{DH_BASE}{path}",
            "kw": kw,
            "in_area": _in_service_area(f"{title} {slug} {detail_text}"),
            "id": "dh:" + path.split("/")[-1],
        })
    logger.info(f"Bids/DailyHerald: {len(out)} green matches ({detail_fetches} detail fetches).")
    return out


# --------------------------------------------------------------------------
# Source: Village of Arlington Heights (vah.com, Revize)
# --------------------------------------------------------------------------
VAH = "https://www.vah.com"


def scan_arlington_heights():
    out = []
    try:
        r = _get(f"{VAH}/government/departments/finance/bid_opportunities.php")
        r.raise_for_status()
    except requests.RequestException as e:
        logger.error(f"Bids/ArlingtonHts: fetch failed: {e}")
        return out

    for row in re.findall(r"<tr[^>]*>(.*?)</tr>", r.text, re.S):
        link = re.search(r'href="([^"]*bid_detail[^"]*)"', row)
        if not link:
            continue
        cells = [re.sub(r"\s+", " ", re.sub(r"<[^>]+>", " ", c)).strip()
                 for c in re.findall(r"<td[^>]*>(.*?)</td>", row, re.S)]
        blob = " ".join(cells)
        # subject follows the bid number, e.g. "... Bid # 260014 BACKYARD DRAINAGE"
        sm = re.search(r"Bid #\s*\d+\s+(.*)$", blob)
        subject = (sm.group(1).strip() if sm else blob)[:120]
        due = ""
        dm = re.search(r"([A-Z][a-z]+ \d{1,2}, \d{4}[^A-Za-z]*\d{0,2}:?\d{0,2}\s*[AP]?M?)", blob)
        if dm:
            due = dm.group(1).strip()
        kw = _matched_kw(subject)
        if not kw:
            continue
        href = link.group(1)
        url = href if href.startswith("http") else f"{VAH}/{href.lstrip('/')}"
        out.append({
            "source": "Arlington Heights",
            "title": subject or "(bid)",
            "agency": "Village of Arlington Heights",
            "due": due,
            "url": url,
            "kw": kw,
            "in_area": True,
            "id": "vah:" + (re.search(r"(\d{6})", blob).group(1) if re.search(r"\d{6}", blob) else href),
        })
    logger.info(f"Bids/ArlingtonHts: {len(out)} green matches.")
    return out


# --------------------------------------------------------------------------
# Source: Village of Palatine (palatine.il.us, CivicPlus)
# --------------------------------------------------------------------------
PAL = "https://www.palatine.il.us"


def scan_palatine():
    out = []
    try:
        r = _get(f"{PAL}/Bids.aspx")
        r.raise_for_status()
    except requests.RequestException as e:
        logger.error(f"Bids/Palatine: fetch failed: {e}")
        return out

    for bid_id, title in re.findall(r'href="[Bb]ids\.aspx\?bidID=(\d+)"[^>]*>([^<]+)</a>', r.text):
        title = re.sub(r"\s+", " ", title).strip()
        kw = _matched_kw(title)
        if not kw:
            continue
        out.append({
            "source": "Palatine",
            "title": title[:120],
            "agency": "Village of Palatine",
            "due": "",
            "url": f"{PAL}/Bids.aspx?bidID={bid_id}",
            "kw": kw,
            "in_area": True,
            "id": f"pal:{bid_id}",
        })
    logger.info(f"Bids/Palatine: {len(out)} green matches.")
    return out


# --------------------------------------------------------------------------
# Orchestration
# --------------------------------------------------------------------------
def _render_email(new_matches):
    core = [m for m in new_matches if m["in_area"]]
    wider = [m for m in new_matches if not m["in_area"]]

    def rows(ms):
        parts = []
        for m in ms:
            due = f"<span style='color:#676D89'>Due {m['due']}</span>" if m["due"] else ""
            parts.append(
                f"<tr>"
                f"<td style='padding:7px 12px 7px 0;vertical-align:top'>"
                f"<a href='{m['url']}' style='color:#232850;font-weight:600;text-decoration:none'>{m['title']}</a>"
                f"<div style='color:#9A7A1E;font-size:12px;margin-top:2px'>matched: {m['kw']}</div></td>"
                f"<td style='padding:7px 12px 7px 0;vertical-align:top'>{m['agency']}</td>"
                f"<td style='padding:7px 12px 7px 0;vertical-align:top'>{m['source']}</td>"
                f"<td style='padding:7px 0;vertical-align:top;white-space:nowrap'>{due}</td>"
                f"</tr>")
        return "".join(parts)

    def section(label, ms):
        if not ms:
            return ""
        return (f"<h3 style='margin:20px 0 6px;color:#232850'>{label} ({len(ms)})</h3>"
                f"<table style='border-collapse:collapse;font-size:14px;width:100%'>"
                f"<tr style='text-align:left;color:#676D89;font-size:11px;text-transform:uppercase'>"
                f"<th style='padding:0 12px 6px 0'>Opportunity</th><th style='padding:0 12px 6px 0'>Agency</th>"
                f"<th style='padding:0 12px 6px 0'>Source</th><th style='padding:0 0 6px'>Due</th></tr>"
                f"{rows(ms)}</table>")

    return f"""
    <div style="font-family:-apple-system,Segoe UI,Roboto,sans-serif;color:#232850">
      <h2 style="margin:0 0 4px">New landscaping / grounds bid opportunities</h2>
      <p style="color:#676D89;margin:0 0 8px">{len(new_matches)} new green-industry bid notice(s)
      since yesterday, filtered from the Daily Herald plus the Arlington Heights and Palatine village sites.</p>
      {section("In your service area", core)}
      {section("Wider suburban area", wider)}
      <p style="color:#9AA0B4;font-size:12px;margin-top:22px">Mount Prospect and Des Plaines block automated
      reads; their notices are caught via the Daily Herald. Titles/links are pulled straight from the sources,
      always confirm details on the agency page before bidding.</p>
    </div>"""


def run_daily(dry_run=False):
    matches = []
    for fn in (scan_daily_herald, scan_arlington_heights, scan_palatine):
        try:
            matches.extend(fn())
        except Exception as e:  # one bad source shouldn't kill the digest
            logger.error(f"Bids: source {fn.__name__} crashed: {e}")

    # dedupe within this run by id
    by_id = {}
    for m in matches:
        by_id.setdefault(m["id"], m)
    matches = list(by_id.values())

    seen = _load_seen()
    new_matches = [m for m in matches if m["id"] not in seen]
    new_matches.sort(key=lambda m: (not m["in_area"], m["source"], m["title"]))

    if not new_matches:
        logger.info(f"Bids: {len(matches)} green matches, none new; no email.")
        return {"status": "ok", "matches": len(matches), "new": 0, "sent": False}

    html = _render_email(new_matches)
    if dry_run:
        return {"status": "ok", "matches": len(matches), "new": len(new_matches),
                "sent": False, "html": html}

    to = os.getenv("BID_SCANNER_TO", DEFAULT_TO)
    subject = f"{len(new_matches)} new landscaping bid opportunit" + ("y" if len(new_matches) == 1 else "ies")
    result = send_email(to, subject, html)
    if result:
        _save_seen(seen | {m["id"] for m in matches})
        logger.info(f"Bids: emailed {len(new_matches)} new matches to {to}.")
        return {"status": "ok", "matches": len(matches), "new": len(new_matches),
                "sent": True, "to": to}
    return {"status": "error", "message": "send_email failed (see log)"}
