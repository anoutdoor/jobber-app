"""Weekly new-homeowner lead digest.

Pulls recent residential sales from the Cook County Assessor's open data
(dataset wvhk-k5uv), joins property addresses (3723-97qp) and zip/lat-lon
(pabr-t5kh), filters to our service-area zips, and emails a ranked list.
New owners are the best cold prospects in the quote-analytics data: they
spend on landscaping in year one and repeat clients close 16 points higher
ever after.

The assessor data lags recording by ~4-6 weeks, so "this week's" list is
sales that newly APPEARED in the dataset since the last run (high-water
mark on sale_date, stored in a hidden Sheet tab so it survives redeploys).
"""
import json
import logging
import math
import os
from datetime import datetime, timedelta

import requests
from gspread.exceptions import WorksheetNotFound

from jobber_sync import get_sheets_client
from financial.gmail_send import send_email

logger = logging.getLogger(__name__)

BASE = "https://datacatalog.cookcountyil.gov/resource"
SERVICE_ZIPS = {
    "60056", "60004", "60005", "60067", "60074", "60016", "60018", "60008",
    "60070", "60007", "60025", "60068", "60062", "60090", "60053",
}
SHOP = (42.0664, -87.9373)  # Mount Prospect base
STATE_TAB = "_leadstate"
DEFAULT_TO = "an.lawnservices1@gmail.com"


def _soql(dataset, params):
    r = requests.get(f"{BASE}/{dataset}.json", params=params, timeout=120)
    r.raise_for_status()
    return r.json()


def _state_ws():
    sid = os.getenv("GOOGLE_SHEET_ID")
    if not sid:
        raise RuntimeError("GOOGLE_SHEET_ID not set; can't persist lead state.")
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
    try:
        raw = _state_ws().acell("A1").value
        return json.loads(raw) if raw else {}
    except Exception as e:
        logger.warning(f"Homeowners: state read failed ({e}); using empty state.")
        return {}


def _save_state(state):
    try:
        _state_ws().update("A1", [[json.dumps(state)]])
    except Exception as e:
        logger.error(f"Homeowners: state write failed ({e}); next run may repeat rows.")


def _haversine(lat, lon):
    dlat, dlon = math.radians(lat - SHOP[0]), math.radians(lon - SHOP[1])
    a = (math.sin(dlat / 2) ** 2
         + math.cos(math.radians(SHOP[0])) * math.cos(math.radians(lat)) * math.sin(dlon / 2) ** 2)
    return 3958.8 * 2 * math.asin(math.sqrt(a))


def _batched(items, n=100):
    for i in range(0, len(items), n):
        yield items[i:i + n]


def fetch_new_homeowners(since_sale_date):
    """Residential arm's-length sales with sale_date > since, joined to
    address + zip + distance, filtered to service-area zips."""
    sales = _soql("wvhk-k5uv", {
        "$where": (f"sale_date > '{since_sale_date}' AND sale_price > 100000 "
                   "AND class like '2%' AND is_multisale = false "
                   "AND sale_filter_deed_type = false AND sale_filter_less_than_10k = false "
                   "AND sale_filter_same_sale_within_365 = false"),
        "$select": "pin,sale_date,sale_price,buyer_name",
        "$limit": 50000,
    })
    if not sales:
        return [], since_sale_date
    max_seen = max(s["sale_date"] for s in sales)
    pins = sorted({s["pin"] for s in sales})

    # zip + coordinates first (cheap columns), keep only service-area pins
    meta = {}
    for batch in _batched(pins):
        q = ", ".join(f"'{p}'" for p in batch)
        for row in _soql("pabr-t5kh", {"$where": f"pin in ({q})",
                                       "$select": "pin,zip_code,lat,lon,township_name",
                                       "$limit": len(batch) * 2}):
            z = (row.get("zip_code") or "")[:5]
            if z in SERVICE_ZIPS:
                meta[row["pin"]] = row

    # addresses only for the pins that survived the territory filter
    addr = {}
    keep = sorted(meta)
    for batch in _batched(keep):
        q = ", ".join(f"'{p}'" for p in batch)
        for row in _soql("3723-97qp", {"$where": f"pin in ({q}) AND year >= '2023'",
                                       "$select": "pin,year,prop_address_full,prop_address_city_name",
                                       "$limit": len(batch) * 6}):
            prev = addr.get(row["pin"])
            if not prev or row.get("year", "") > prev.get("year", ""):
                addr[row["pin"]] = row

    leads = []
    for s in sales:
        m = meta.get(s["pin"])
        if not m:
            continue
        a = addr.get(s["pin"], {})
        try:
            miles = _haversine(float(m["lat"]), float(m["lon"]))
        except (TypeError, ValueError, KeyError):
            miles = None
        leads.append({
            "address": a.get("prop_address_full", "(address pending)"),
            "city": (a.get("prop_address_city_name") or m.get("township_name") or "").title(),
            "zip": (m.get("zip_code") or "")[:5],
            "sale_date": s["sale_date"][:10],
            "price": float(s.get("sale_price") or 0),
            "buyer": (s.get("buyer_name") or "").title(),
            "miles": miles,
        })
    leads.sort(key=lambda x: (x["miles"] is None, x["miles"] or 0))
    return leads, max_seen


def _render_email(leads, since):
    parts = []
    for x in leads:
        dist = "" if x["miles"] is None else f"{x['miles']:.1f} mi"
        parts.append(
            f"<tr><td style='padding:5px 10px 5px 0'>{x['address']}</td>"
            f"<td style='padding:5px 10px 5px 0'>{x['city']}</td>"
            f"<td style='padding:5px 10px 5px 0'>{x['buyer']}</td>"
            f"<td style='padding:5px 10px 5px 0;text-align:right'>${x['price']:,.0f}</td>"
            f"<td style='padding:5px 10px 5px 0'>{x['sale_date']}</td>"
            f"<td style='padding:5px 0;text-align:right'>{dist}</td></tr>"
        )
    rows = "".join(parts)
    return f"""
    <div style="font-family:-apple-system,Segoe UI,Roboto,sans-serif;color:#232850">
      <h2 style="margin:0 0 4px">New homeowners in the service area</h2>
      <p style="color:#676D89;margin:0 0 16px">{len(leads)} closings newly recorded since {since[:10]}
      (county data lags ~5 weeks). Sorted nearest-first from the shop.
      New owners spend on landscaping in year one; land them small, they repeat at 58%.</p>
      <table style="border-collapse:collapse;font-size:14px">
        <tr style="text-align:left;color:#676D89;font-size:12px;text-transform:uppercase">
          <th style="padding:0 10px 6px 0">Address</th><th style="padding:0 10px 6px 0">City</th>
          <th style="padding:0 10px 6px 0">Buyer</th><th style="padding:0 10px 6px 0">Sale price</th>
          <th style="padding:0 10px 6px 0">Sale date</th><th style="padding:0 0 6px">Dist</th>
        </tr>
        {rows}
      </table>
    </div>"""


def run_weekly(dry_run=False):
    state = _load_state()
    since = state.get("last_sale_date")
    if not since:
        since = (datetime.utcnow() - timedelta(days=49)).strftime("%Y-%m-%d")
    try:
        leads, max_seen = fetch_new_homeowners(since)
    except requests.RequestException as e:
        logger.error(f"Homeowners: county data pull failed: {e}")
        return {"status": "error", "message": str(e)}

    if not leads:
        logger.info("Homeowners: no new sales since last run; no email sent.")
        return {"status": "ok", "leads": 0, "sent": False}

    html = _render_email(leads, since)
    if dry_run:
        return {"status": "ok", "leads": len(leads), "sent": False, "html": html}

    to = os.getenv("NEW_HOMEOWNER_TO", DEFAULT_TO)
    subject = f"New homeowners: {len(leads)} in the service area"
    result = send_email(to, subject, html)
    if result:
        _save_state({"last_sale_date": max_seen})
        logger.info(f"Homeowners: sent {len(leads)} leads to {to}.")
        return {"status": "ok", "leads": len(leads), "sent": True, "to": to}
    return {"status": "error", "message": "send_email failed (see log)"}
