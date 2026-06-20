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
CONFIG_PATH = os.path.join(BASE, "financial_config.yaml")

DOW = ["Mon", "Tue", "Wed", "Thu", "Fri", "Sat", "Sun"]
MON_ABBR = ["Jan", "Feb", "Mar", "Apr", "May", "Jun",
            "Jul", "Aug", "Sep", "Oct", "Nov", "Dec"]

# Fallbacks if financial_config.yaml is missing the mow_costing block.
COST_DEFAULTS = {
    "crew_rates": {"Gonzalo": 28, "Jonathan": 23},
    "labor_burden_pct": 15,
    "crew_daily_overhead": 252,
    "overhead_method": "by_mow_time",
}


def load_cost_model():
    """Read mow_costing from financial_config.yaml, falling back to defaults.
    Returns a dict with the loaded crew rate, burden multiplier, daily overhead,
    and method."""
    cfg = dict(COST_DEFAULTS)
    try:
        import yaml
        with open(CONFIG_PATH) as f:
            mc = (yaml.safe_load(f) or {}).get("mow_costing") or {}
        cfg.update({k: v for k, v in mc.items() if v is not None})
    except Exception:
        pass
    rates = cfg.get("crew_rates") or COST_DEFAULTS["crew_rates"]
    crew_hourly = float(sum(rates.values()))
    burden = 1 + float(cfg.get("labor_burden_pct", 15)) / 100.0
    return {
        "crew_hourly": crew_hourly,                 # sum of crew wages, $/hr
        "loaded_hourly": crew_hourly * burden,      # wages x burden, $/hr
        "burden_pct": float(cfg.get("labor_burden_pct", 15)),
        "daily_overhead": float(cfg.get("crew_daily_overhead", 252)),
        "method": cfg.get("overhead_method", "by_mow_time"),
        "rates": rates,
    }


# ---------------------------------------------------------------------------
# P&L per mow visit
#   labor    = mow hours x (sum of crew wages) x (1 + burden)
#   overhead = crew daily overhead, split across that day's mows by mow time
#   revenue  = the job's per-visit price (job.total, VISIT_BASED billing)
#   profit   = revenue - labor - overhead
# Excludes the same short/long outlier visits the summaries exclude. Visits
# with no price (price = None) are costed but left out of revenue/profit/margin.
# ---------------------------------------------------------------------------
def compute_pnl(detail, prop_info, cost):
    excluded = {(r["job_number"], r["date"]) for r in detail
                if r.get("flag_kind") in ("short", "long")}
    visit_min = collections.defaultdict(float)   # (jn, date) -> minutes
    for r in detail:
        if r["minutes"] == "":
            continue
        key = (r["job_number"], r["date"])
        if key in excluded:
            continue
        visit_min[key] += r["minutes"]

    day_min = collections.defaultdict(float)
    day_stops = collections.defaultdict(int)
    for (jn, d), m in visit_min.items():
        day_min[d] += m
        day_stops[d] += 1

    loaded = cost["loaded_hourly"]
    ovh = cost["daily_overhead"]
    method = cost["method"]

    def overhead_for(jn, d, m):
        if method == "even_per_stop":
            return ovh / day_stops[d] if day_stops[d] else 0.0
        if method == "flat_hourly":
            return (m / 60.0) * (ovh / 8.0)   # standard ~8h crew day
        return ovh * (m / day_min[d]) if day_min[d] else 0.0   # by_mow_time

    house = collections.defaultdict(lambda: {
        "n": 0, "priced_n": 0, "minutes": 0.0, "revenue": 0.0,
        "labor": 0.0, "overhead": 0.0})
    day = collections.defaultdict(lambda: {
        "revenue": 0.0, "labor": 0.0, "overhead": 0.0, "priced": 0})
    visit_profits = []
    season = {"revenue": 0.0, "labor": 0.0, "overhead": 0.0, "priced": 0}

    for (jn, d), m in visit_min.items():
        hrs = m / 60.0
        labor = hrs * loaded
        overhead = overhead_for(jn, d, m)
        info = prop_info.get(jn, {})
        price = info.get("price")
        key = (info.get("client", ""), info.get("street") or f"Job #{jn}")

        h = house[key]
        h["n"] += 1
        h["minutes"] += m
        h["labor"] += labor
        h["overhead"] += overhead
        dd = day[d]
        dd["labor"] += labor
        dd["overhead"] += overhead
        season["labor"] += labor
        season["overhead"] += overhead

        if price is not None:
            profit = price - labor - overhead
            h["priced_n"] += 1
            h["revenue"] += price
            dd["revenue"] += price
            dd["priced"] += 1
            season["revenue"] += price
            season["priced"] += 1
            visit_profits.append(profit)

    # finalize per-house aggregates
    house_pnl = {}
    for key, h in house.items():
        hrs = h["minutes"] / 60.0
        cost_total = h["labor"] + h["overhead"]
        priced = h["priced_n"]
        rev = h["revenue"]
        profit = rev - cost_total if priced else None
        house_pnl[key] = {
            "price": round(rev / priced, 2) if priced else None,
            "avgCost": round(cost_total / h["n"], 2) if h["n"] else 0,
            "avgLabor": round(h["labor"] / h["n"], 2) if h["n"] else 0,
            "avgOverhead": round(h["overhead"] / h["n"], 2) if h["n"] else 0,
            "avgProfit": round(profit / priced, 2) if priced else None,
            "marginPct": round(100 * profit / rev, 1) if priced and rev else None,
            "profitPerHr": round(profit / hrs, 2) if priced and hrs else None,
            "totalProfit": round(profit, 2) if priced else None,
            "totalRevenue": round(rev, 2),
            "pricedN": priced,
        }

    day_pnl = {}
    for d, x in day.items():
        c = x["labor"] + x["overhead"]
        p = x["revenue"] - c if x["priced"] else None
        day_pnl[d] = {
            "revenue": round(x["revenue"], 2),
            "labor": round(x["labor"], 2),
            "overhead": round(x["overhead"], 2),
            "cost": round(c, 2),
            "profit": round(p, 2) if p is not None else None,
        }

    s_cost = season["labor"] + season["overhead"]
    s_profit = season["revenue"] - s_cost
    season_out = {
        "revenue": round(season["revenue"], 2),
        "labor": round(season["labor"], 2),
        "overhead": round(season["overhead"], 2),
        "cost": round(s_cost, 2),
        "profit": round(s_profit, 2),
        "marginPct": round(100 * s_profit / season["revenue"], 1) if season["revenue"] else 0,
        "pricedVisits": season["priced"],
        "medianProfit": round(statistics.median(visit_profits), 2) if visit_profits else 0,
    }
    return {"house": house_pnl, "day": day_pnl, "season": season_out}


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

    # --- P&L per visit/house/day (revenue from Jobber price, cost from config) ---
    cost = load_cost_model()
    pnl = compute_pnl(detail, prop_info, cost)
    house_pnl = pnl["house"]   # (client, street) -> aggregated P&L

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

    # --- houses: standards (typical/target/best/slack) joined with stats + P&L ---
    houses = []
    for p in props:
        key = (p["client"], p["prop"])
        st = stats_by_key.get(key, {})
        pl = house_pnl.get(key, {})
        # "typical profit" = profit on a normal-length mow: price minus labor at
        # the typical pace minus this house's average overhead per visit.
        typ_hrs = p["typical"] / 60.0
        price = pl.get("price")
        typ_profit = None
        if price is not None:
            typ_labor = typ_hrs * cost["loaded_hourly"]
            typ_profit = round(price - typ_labor - (pl.get("avgOverhead") or 0), 2)
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
            # P&L
            "price": price,
            "cost": pl.get("avgCost"),
            "labor": pl.get("avgLabor"),
            "overhead": pl.get("avgOverhead"),
            "profit": pl.get("avgProfit"),
            "typProfit": typ_profit,
            "margin": pl.get("marginPct"),
            "profitPerHr": pl.get("profitPerHr"),
            "totalProfit": pl.get("totalProfit"),
            "slackCost": round((p["slack"] / 60.0) * cost["loaded_hourly"], 2),
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
        dp = pnl["day"].get(date_key, {})
        daily.append({
            "date": date_key,
            "day": DOW[first.weekday()] if first else "",
            "props": a["prop_count"],
            "mowH": round(a["mow_min"] / 60.0, 2),
            "travelH": round(a["travel_min"] / 60.0, 2),
            "yardMin": yard,
            "revenue": dp.get("revenue", 0),
            "cost": dp.get("cost", 0),
            "profit": dp.get("profit", 0),
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
    n_weeks = len(weekly) or 1
    season = pnl["season"]

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
        # --- P&L headline ---
        "revenue": season["revenue"],
        "laborCost": season["labor"],
        "overheadCost": season["overhead"],
        "totalCost": season["cost"],
        "profit": season["profit"],
        "marginPct": season["marginPct"],
        "medianProfit": season["medianProfit"],
        "profitPerWeek": round(season["profit"] / n_weeks, 0),
        "revenuePerWeek": round(season["revenue"] / n_weeks, 0),
        "slackCostPerWeek": round(weekly_slack_h * cost["loaded_hourly"], 0),
        "pricedVisits": season["pricedVisits"],
        "crewHourly": cost["crew_hourly"],
        "loadedHourly": round(cost["loaded_hourly"], 2),
        "burdenPct": cost["burden_pct"],
        "dailyOverhead": cost["daily_overhead"],
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
                                "props": 0, "yard": [], "revenue": 0.0,
                                "cost": 0.0, "profit": 0.0})
        g["mowDays"] += 1
        g["mowH"] += r["mowH"]
        g["travelH"] += r["travelH"]
        g["props"] += r["props"]
        g["revenue"] += r.get("revenue", 0) or 0
        g["cost"] += r.get("cost", 0) or 0
        g["profit"] += r.get("profit", 0) or 0
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
            "revenue": round(g["revenue"], 0),
            "cost": round(g["cost"], 0),
            "profit": round(g["profit"], 0),
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
