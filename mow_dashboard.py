"""Mow dashboard data layer.

Turns the structures gathered by mow_time_export.gather_mow_data() into a flat
JSON payload the dashboard template renders, and caches it to disk so the page
loads instantly. The daily mow export refreshes this cache from the same Jobber
pull that writes the sheet, so the dashboard and the sheet never disagree.

No new dependencies: reuses the existing Jobber + Google auth in mow_time_export.
"""
import os
import json
import statistics
import collections
from datetime import datetime

try:
    from zoneinfo import ZoneInfo
    CENTRAL = ZoneInfo("America/Chicago")
except Exception:
    import pytz
    CENTRAL = pytz.timezone("America/Chicago")

BASE = os.path.dirname(os.path.abspath(__file__))
CACHE_PATH = os.path.join(BASE, "mow_dashboard_cache.json")

DOW = ["Mon", "Tue", "Wed", "Thu", "Fri", "Sat", "Sun"]
MON_ABBR = ["Jan", "Feb", "Mar", "Apr", "May", "Jun",
            "Jul", "Aug", "Sep", "Oct", "Nov", "Dec"]


# ---------------------------------------------------------------------------
# Payload assembly
# ---------------------------------------------------------------------------
def build_payload(g):
    """g is the dict returned by mow_time_export.gather_mow_data()."""
    detail = g["detail"]
    props = g["props"]                       # typical/target/best/slack/day per property
    day_rollup = g["day_rollup"]             # per day-of-week route totals (minutes)
    day_agg = g["day_agg"]                    # per calendar day (cleaned mow_min)
    prop_info = g["prop_info"]                # job# -> client/street/city
    clean_prop_visits = g["clean_prop_visits"]  # job# -> [clean visit minutes]
    excl_count = g["excl_count"]              # job# -> # outlier visits excluded
    n_excluded = g["n_excluded"]

    # --- per-property stats keyed by (client, street) so we can join to props ---
    stats_by_key = {}
    total_min = 0.0
    total_mows = 0
    for jn, mins in clean_prop_visits.items():
        if not mins:
            continue
        info = prop_info.get(jn, {})
        total = sum(mins)
        total_min += total
        total_mows += len(mins)
        key = (info.get("client", ""), info.get("street") or f"Job #{jn}")
        stats_by_key[key] = {
            "mows": len(mins),
            "avg": round(total / len(mins), 1),
            "fastest": round(min(mins), 1),
            "slowest": round(max(mins), 1),
            "totalHours": round(total / 60.0, 2),
            "flagged": excl_count.get(jn, 0),
        }

    # --- houses: standards (typical/target/best/slack) joined with the stats ---
    houses = []
    for p in props:
        st = stats_by_key.get((p["client"], p["prop"]), {})
        houses.append({
            "client": p["client"],
            "property": p["prop"],
            "day": p["day"],
            "mows": p["n"],
            "typical": round(p["typical"], 1),
            "target": round(p["target"], 1),
            "best": round(p["best"], 1),
            "slack": round(p["slack"], 1),
            "avg": st.get("avg", round(p["typical"], 1)),
            "fastest": st.get("fastest", round(p["best"], 1)),
            "slowest": st.get("slowest", round(p["typical"], 1)),
            "totalHours": st.get("totalHours", 0),
            "flagged": st.get("flagged", 0),
        })
    houses.sort(key=lambda h: -h["slack"])

    # --- per day-of-week capacity (Mon..Sun in order) ---
    day_cap = []
    for d in DOW:
        if d not in day_rollup:
            continue
        x = day_rollup[d]
        day_cap.append({
            "day": d,
            "props": x["n"],
            "typicalH": round(x["typ"] / 60.0, 1),
            "targetH": round(x["tgt"] / 60.0, 1),
            "slackH": round((x["typ"] - x["tgt"]) / 60.0, 1),
        })

    # --- per calendar day (uses cleaned mow minutes) ---
    daily = []
    for date_key in sorted(day_agg):
        a = day_agg[date_key]
        first = a.get("first")
        fmow = a.get("first_mow")
        yard = round((fmow - first).total_seconds() / 60) if (fmow and first) else 0
        daily.append({
            "date": date_key,
            "day": DOW[first.weekday()] if first else "",
            "props": a["prop_count"],
            "mowH": round(a["mow_min"] / 60.0, 2),
            "travelH": round(a["travel_min"] / 60.0, 2),
            "yardMin": yard,
        })

    # --- weekly rollup (group calendar days by their Monday) ---
    weekly = _weekly_rollup(daily)

    # --- single-day drilldown: sum segments per (date, property), full season ---
    typ_by_key = {(h["client"], h["property"]): h["typical"] for h in houses}
    seg_group = {}
    for r in detail:
        if r["minutes"] == "":
            continue
        k = (r["date"], r["property"])
        grp = seg_group.setdefault(k, {"min": 0.0, "client": "", "day": "",
                                       "ckey": None, "forgot": False})
        grp["min"] += r["minutes"]
        grp["client"] = r["client"]
        grp["day"] = r["dow"]
        grp["ckey"] = (r["client"], r["property"])
        if r.get("flag_kind") == "long":
            grp["forgot"] = True
    seg_by_date = collections.defaultdict(list)
    for (date_key, prop), grp in seg_group.items():
        typ = typ_by_key.get(grp["ckey"], 0)
        seg_by_date[date_key].append({
            "property": prop,
            "client": grp["client"],
            "day": grp["day"],
            "min": round(grp["min"], 1),
            "typical": round(typ, 1) if typ else 0,
            "delta": round(grp["min"] - typ, 1) if typ else None,
            "forgot": grp["forgot"],
        })
    for d in seg_by_date:
        seg_by_date[d].sort(key=lambda x: -x["min"])
    seg_dates = sorted(seg_by_date.keys())

    # --- headline meta ---
    typicals = [h["typical"] for h in houses if h["typical"] > 0]
    total_mow_h = sum(a["mow_min"] for a in day_agg.values()) / 60.0
    total_travel_h = sum(a["travel_min"] for a in day_agg.values()) / 60.0
    grand = total_mow_h + total_travel_h
    weekly_slack_h = sum((x["typ"] - x["tgt"]) for x in day_rollup.values()) / 60.0

    meta = {
        "seasonStart": daily[0]["date"] if daily else "",
        "seasonEnd": daily[-1]["date"] if daily else "",
        "generated": datetime.now(CENTRAL).strftime("%b %-d, %Y %-I:%M %p CT"),
        "mowDays": len(day_agg),
        "totalMows": total_mows,
        "houseCount": len(houses),
        "totalMowHours": round(total_mow_h, 1),
        "totalTravelHours": round(total_travel_h, 1),
        "travelPct": round(100 * total_travel_h / grand, 1) if grand else 0,
        "medianTypical": round(statistics.median(typicals), 1) if typicals else 0,
        "meanMin": round(total_min / total_mows, 1) if total_mows else 0,
        "weeklySlackH": round(weekly_slack_h, 1),
        "totalFlagged": n_excluded,
        "segStart": seg_dates[0] if seg_dates else "",
        "segEnd": seg_dates[-1] if seg_dates else "",
    }

    return {
        "meta": meta,
        "dayCapacity": day_cap,
        "houses": houses,
        "weekly": weekly,
        "daily": daily,
        "segByDate": dict(seg_by_date),
        "segDates": seg_dates,
    }


def _weekly_rollup(daily):
    """Group the per-day rows by their Monday into weekly totals."""
    from datetime import date, timedelta
    wk = collections.OrderedDict()
    for r in daily:
        d = date.fromisoformat(r["date"])
        mon = d - timedelta(days=d.weekday())
        g = wk.setdefault(mon, {"mowDays": 0, "mowH": 0.0, "travelH": 0.0,
                                "props": 0, "yard": []})
        g["mowDays"] += 1
        g["mowH"] += r["mowH"]
        g["travelH"] += r["travelH"]
        g["props"] += r["props"]
        if r["yardMin"]:
            g["yard"].append(r["yardMin"])
    out = []
    for mon, g in wk.items():
        mt = g["mowH"] + g["travelH"]
        out.append({
            "weekStart": mon.isoformat(),
            "label": f"{MON_ABBR[mon.month-1]} {mon.day}",
            "mowDays": g["mowDays"],
            "mowH": round(g["mowH"], 1),
            "travelH": round(g["travelH"], 1),
            "props": g["props"],
            "travelPct": round(100 * g["travelH"] / mt, 1) if mt else 0,
            "yardMin": round(statistics.mean(g["yard"]), 0) if g["yard"] else 0,
            "minPerProp": round(60 * g["mowH"] / g["props"], 1) if g["props"] else 0,
        })
    return out


# ---------------------------------------------------------------------------
# Cache
# ---------------------------------------------------------------------------
def save_cache(payload):
    with open(CACHE_PATH, "w") as f:
        json.dump(payload, f)
    return payload


def load_cache():
    if not os.path.exists(CACHE_PATH):
        return None
    with open(CACHE_PATH) as f:
        return json.load(f)


def refresh():
    """Live pull from Jobber + rebuild the payload + cache it. Used on cold
    start (no cache yet) or a manual ?refresh=1. The daily export refreshes the
    cache itself via build_payload, so this is the fallback path."""
    from mow_time_export import gather_mow_data
    payload = build_payload(gather_mow_data())
    save_cache(payload)
    return payload


def get_payload(force=False):
    if not force:
        cached = load_cache()
        if cached:
            return cached
    return refresh()
