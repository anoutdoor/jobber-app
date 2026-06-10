"""Mow-time export: pull Gonzalo's Jobber clock-in/out data and write it to the
job-costing Google Sheet as a set of mow-time tabs.

Gonzalo clocks in/out of every mow, so his timesheet entries are a clean record
of how long each lawn takes. Each entry tied to a job/client is a mow; entries
with no job (label "General") are travel/transition time between stops.

Tabs written (created if missing, fully overwritten each run):
  - "Mow Detail"     : one row per clock segment (one mow), with a Flag column +
                       colored rows for likely data errors to review in Jobber
                       (red = too long / forgot to clock out, amber = too short
                       vs that property's norm, yellow = tiny sub-5-min clock tap)
  - "Mow by Property": per-property rollup (a client with two properties splits
                       into two rows; visits, avg/fastest/slowest minutes)
  - "Mow by Day"     : per-day rollup (properties mowed, mow vs travel time, span)
  - "Mow Travel"     : season totals + per-day mow-vs-travel-time breakdown
  - "Mow Standards"  : per-property "should take" target (avg of 3 fastest clean
                       mows) vs typical, + per-day route totals and the slack

Travel time is the between-mow time only: General (no-job) time before the
first mow (yard/arrival) and after the last mow (drive back) is excluded.

The Detail tab shows every mow (flagged ones highlighted). The summary tabs
(Property / Day / Travel totals) EXCLUDE the short/long flagged visits so the
averages and totals aren't skewed by clock errors; tiny "blip" segments stay
because their visit total is already correct.

Auth reuses the same stores the production app uses, so no separate setup:
  - Google token from token.json (refreshed in place if expired — safe; Google
    re-auth does not affect Railway)
  - Jobber token READ from the Sheet's hidden "_tokens" tab. This is read-only:
    we never refresh the Jobber token here. Railway runs the app as a long-lived
    process that caches the token in memory and refreshes it hourly; rotating it
    locally would break that sync until redeploy. On a 401 we fail loudly and
    wait for Railway's next sync.

Run locally with system Python (only needs `requests`; timezones via stdlib):
    python3 mow_time_export.py
"""
import os
import json
import time
import statistics
from collections import defaultdict, Counter
from datetime import datetime, timezone

import requests

BASE = os.path.dirname(os.path.abspath(__file__))

# Prefer stdlib zoneinfo (no dependency); fall back to pytz when the OS lacks a
# timezone database (e.g. some Railway/container images), since pytz is already
# a project dependency and ships its own tz data. astimezone() works with either.
try:
    from zoneinfo import ZoneInfo
    CENTRAL = ZoneInfo("America/Chicago")
except Exception:
    import pytz
    CENTRAL = pytz.timezone("America/Chicago")

GRAPHQL_URL = "https://api.getjobber.com/api/graphql"
GRAPHQL_VERSION = "2026-03-10"

# Gonzalo Cardenas — the mow-crew foreman who clocks each mow.
GONZALO_USER_ID = "Z2lkOi8vSm9iYmVyL1VzZXIvMzg1ODYzNw=="
# His first clock-in of the 2026 season (everything in Jobber is from here on).
SEASON_START = "2026-04-01T00:00:00Z"

TOKEN_TAB = "_tokens"
DETAIL_TAB = "Mow Detail"
PROPERTY_TAB = "Mow by Property"
DAY_TAB = "Mow by Day"
TRAVEL_TAB = "Mow Travel"
STANDARDS_TAB = "Mow Standards"
# Earlier builds wrote a per-client tab; it's replaced by the per-property tab.
STALE_TABS = ["Mow by Client"]

DOW_ORDER = {"Mon": 0, "Tue": 1, "Wed": 2, "Thu": 3, "Fri": 4, "Sat": 5, "Sun": 6}


# ---------------------------------------------------------------------------
# Config / secrets
# ---------------------------------------------------------------------------
def load_env():
    env = {}
    path = os.path.join(BASE, ".env")
    if os.path.exists(path):  # Railway injects real env vars; no .env file there
        with open(path) as f:
            for line in f:
                s = line.strip()
                if s and not s.startswith("#") and "=" in s:
                    k, v = s.split("=", 1)
                    env[k] = v.strip()
    return env


ENV = load_env()
# Real environment variables (Railway) win over the local .env file.
SHEET_ID = os.getenv("GOOGLE_SHEET_ID") or ENV.get("GOOGLE_SHEET_ID")


# ---------------------------------------------------------------------------
# Google auth — token.json locally, GOOGLE_TOKEN env var on Railway (same JSON
# shape). Both are authorized-user creds; we just refresh to a fresh access
# token. Google refresh tokens don't rotate on refresh, so on Railway (where we
# can't rewrite an env var) skipping the write-back is safe.
# ---------------------------------------------------------------------------
def google_access_token():
    env_blob = os.getenv("GOOGLE_TOKEN")
    persist_path = None
    if env_blob:
        g = json.loads(env_blob)
    else:
        persist_path = os.path.join(BASE, "token.json")
        g = json.load(open(persist_path))
    resp = requests.post(g["token_uri"], data={
        "grant_type": "refresh_token",
        "refresh_token": g["refresh_token"],
        "client_id": g["client_id"],
        "client_secret": g["client_secret"],
    })
    resp.raise_for_status()
    data = resp.json()
    access = data["access_token"]
    if persist_path:  # local only — keep token.json current
        g["token"] = access
        if data.get("refresh_token"):
            g["refresh_token"] = data["refresh_token"]
        with open(persist_path, "w") as f:
            json.dump(g, f)
    return access


# ---------------------------------------------------------------------------
# Sheets REST helpers (no gspread dependency)
# ---------------------------------------------------------------------------
SHEETS_API = "https://sheets.googleapis.com/v4/spreadsheets"


def sheets_get(gacc, suffix):
    r = requests.get(f"{SHEETS_API}/{SHEET_ID}{suffix}",
                     headers={"Authorization": f"Bearer {gacc}"})
    r.raise_for_status()
    return r.json()


def sheets_post(gacc, suffix, body):
    r = requests.post(f"{SHEETS_API}/{SHEET_ID}{suffix}",
                      headers={"Authorization": f"Bearer {gacc}",
                               "Content-Type": "application/json"},
                      json=body)
    r.raise_for_status()
    return r.json()


def read_token_row(gacc):
    vals = sheets_get(gacc, f"/values/{TOKEN_TAB}!A1:E20")["values"]
    hdr = vals[0]
    for row in vals[1:]:
        rec = dict(zip(hdr, row))
        if rec.get("provider", "").lower() == "jobber":
            return rec
    raise RuntimeError("No jobber row in _tokens tab.")


# ---------------------------------------------------------------------------
# Jobber GraphQL — READ ONLY. Do NOT refresh the Jobber token locally.
# Railway runs the app as a long-lived process whose hourly sync caches the
# token in memory; rotating it here (a refresh) would break that sync until the
# next redeploy. We only READ the current access token from the Sheet's _tokens
# tab (Railway keeps it fresh). On a 401 we fail loudly instead of refreshing.
# ---------------------------------------------------------------------------
class JobberClient:
    def __init__(self, gacc):
        self.gacc = gacc
        self.access = read_token_row(gacc)["access_token"]

    def gql(self, query, variables=None, _retry=True):
        r = requests.post(GRAPHQL_URL,
                          json={"query": query, "variables": variables or {}},
                          headers={"Authorization": f"Bearer {self.access}",
                                   "Content-Type": "application/json",
                                   "X-JOBBER-GRAPHQL-VERSION": GRAPHQL_VERSION},
                          timeout=30)
        if r.status_code == 401:
            raise RuntimeError(
                "Jobber returned 401 — the access token in the Sheet's _tokens "
                "tab is expired. Do NOT refresh it here (that breaks Railway's "
                "in-memory sync). Wait for Railway's next hourly sync to refresh "
                "it, then re-run."
            )
        r.raise_for_status()
        body = r.json()
        errors = body.get("errors") or []
        if errors and not body.get("data"):
            throttled = any((e.get("extensions") or {}).get("code") == "THROTTLED"
                            for e in errors)
            if throttled and _retry:
                print("[throttled, sleeping 3s]")
                time.sleep(3)
                return self.gql(query, variables, _retry=False)
            raise RuntimeError(f"GraphQL errors: {json.dumps(errors)[:500]}")
        return body


ENTRIES_QUERY = """
query($f: TimeSheetEntriesFilterAttributes, $cursor: String) {
  timeSheetEntries(first: 100, filter: $f, after: $cursor,
                   sort: [{field: START_AT, direction: ASCENDING}]) {
    nodes {
      startAt
      endAt
      finalDuration
      ticking
      label
      client { name }
      job {
        jobNumber
        title
        property { address { street city } }
      }
    }
    pageInfo { hasNextPage endCursor }
  }
}
"""


def fetch_gonzalo_entries(client):
    out = []
    cursor = None
    f = {"assignedTo": GONZALO_USER_ID, "startAt": {"after": SEASON_START}}
    page = 0
    while True:
        body = client.gql(ENTRIES_QUERY, {"f": f, "cursor": cursor})
        conn = body["data"]["timeSheetEntries"]
        out.extend(conn["nodes"])
        page += 1
        print(f"[page {page}: +{len(conn['nodes'])} (total {len(out)})]")
        if not conn["pageInfo"]["hasNextPage"]:
            break
        cursor = conn["pageInfo"]["endCursor"]
        time.sleep(0.3)
    return out


# ---------------------------------------------------------------------------
# Processing
# ---------------------------------------------------------------------------
def to_central(iso_str):
    if not iso_str:
        return None
    s = iso_str.strip()
    if s.endswith("Z"):
        s = s[:-1] + "+00:00"
    dt = datetime.fromisoformat(s)
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(CENTRAL)


def entry_minutes(e):
    """Minutes for an entry: prefer Jobber's finalDuration, fall back to span."""
    dur = e.get("finalDuration")
    if dur:
        return dur / 60.0
    start = to_central(e.get("startAt"))
    end = to_central(e.get("endAt"))
    if start and end:
        return (end - start).total_seconds() / 60.0
    return None  # ticking / open entry


def fmt_time(dt):
    if not dt:
        return ""
    # 12-hour, strip leading zero (e.g. "7:03 AM")
    return dt.strftime("%-I:%M %p")


DOW = ["Mon", "Tue", "Wed", "Thu", "Fri", "Sat", "Sun"]


def _overlap_minutes(seg_start, seg_end, win_start, win_end):
    """Minutes of [seg_start, seg_end] that fall inside [win_start, win_end]."""
    if not (seg_start and seg_end and win_start and win_end):
        return 0.0
    lo = max(seg_start, win_start)
    hi = min(seg_end, win_end)
    secs = (hi - lo).total_seconds()
    return secs / 60.0 if secs > 0 else 0.0


def process(entries):
    """Return (detail, prop_visits, prop_info, day_agg) for building tabs.

    A 'mow' is an entry tied to a job whose title contains "mow" (the weekly
    mow jobs). A property = one mow job (a client with two properties has two
    job numbers, so they split out). Non-job entries (label "General") are
    travel time, but only the General time BETWEEN the first mow's start and
    the last mow's end counts: morning yard time (he arrives early) and the
    drive back to the yard after the last mow are excluded. Other job-linked
    work (cleanups, etc.) is excluded from the mow tabs but still counts toward
    the workday span (First In / Last Out).
    """
    # Pass 1: bucket every entry into its day as a mow or a General segment.
    days = defaultdict(lambda: {"mows": [], "generals": [],
                                "first": None, "last": None})
    prop_info = {}  # job_number -> {"client", "street", "city"}

    for e in entries:
        start = to_central(e.get("startAt"))
        end = to_central(e.get("endAt"))
        if not start:
            continue
        mins = entry_minutes(e)
        date_key = start.date().isoformat()
        d = days[date_key]
        # Workday bookends span ALL entries (mows, travel, cleanups).
        if d["first"] is None or start < d["first"]:
            d["first"] = start
        if end and (d["last"] is None or end > d["last"]):
            d["last"] = end

        job = e.get("job") or {}
        title = job.get("title") or ""
        is_mow = bool(job) and "mow" in title.lower()

        if is_mow:
            jn = str(job.get("jobNumber") or "")
            client = (e.get("client") or {}).get("name") or ""
            addr = (job.get("property") or {}).get("address") or {}
            street = addr.get("street") or ""
            city = addr.get("city") or ""
            if jn and jn not in prop_info:
                prop_info[jn] = {"client": client, "street": street, "city": city}
            d["mows"].append({"start": start, "end": end, "mins": mins,
                              "client": client, "job_number": jn,
                              "title": title, "street": street})
        elif not job:
            d["generals"].append({"start": start, "end": end})

    # Detail rows: one per mow clock segment, chronological.
    detail = []
    for date_key in sorted(days):
        for m in sorted(days[date_key]["mows"], key=lambda x: x["start"]):
            mins = m["mins"]
            detail.append({
                "date": date_key,
                "dow": DOW[m["start"].weekday()],
                "client": m["client"],
                "property": m["street"] or f"Job #{m['job_number']}",
                "job_number": m["job_number"],
                "job_title": m["title"],
                "clock_in": fmt_time(m["start"]),
                "clock_out": fmt_time(m["end"]),
                "minutes": round(mins, 1) if mins is not None else "",
                "hours": round(mins / 60.0, 2) if mins is not None else "",
            })

    # Per-day aggregates with travel windowed to [first mow start, last mow end].
    day_agg = {}
    for date_key, d in days.items():
        mows = d["mows"]
        if not mows:
            continue  # pure cleanup / non-mow day
        mow_min = sum(m["mins"] for m in mows if m["mins"] is not None)
        starts = [m["start"] for m in mows if m["start"]]
        ends = [m["end"] for m in mows if m["end"]]
        win_start = min(starts) if starts else None
        win_end = max(ends) if ends else None
        travel_min = sum(_overlap_minutes(g["start"], g["end"], win_start, win_end)
                         for g in d["generals"])
        day_agg[date_key] = {
            "mow_min": mow_min,
            "travel_min": travel_min,
            "prop_count": len({m["job_number"] for m in mows}),
            "first": d["first"],
            "first_mow": win_start,  # start of the first mow = end of morning yard time
            "last": d["last"],
        }

    # Per-property visits: a visit = (job_number, date) with segments summed, so
    # one property mowed twice in a day (split clock) counts as a single visit.
    visit_min = defaultdict(float)
    for date_key in days:
        for m in days[date_key]["mows"]:
            if m["mins"] is not None:
                visit_min[(m["job_number"], date_key)] += m["mins"]
    prop_visits = defaultdict(list)  # job_number -> [minutes per visit]
    for (jn, _date), mn in visit_min.items():
        prop_visits[jn].append(mn)

    return detail, prop_visits, prop_info, day_agg


# ---------------------------------------------------------------------------
# Data-quality flags (for manual review / cleanup in Jobber)
# ---------------------------------------------------------------------------
def compute_flags(detail):
    """Tag each detail row with a flag + kind for manual review. Compares each
    mow VISIT (job#, date) to that property's own typical time, and catches tiny
    clock taps. Kinds (drive the row color):
      - "long"  : visit far above the property's norm (likely forgot to clock out)
      - "short" : visit far below the property's norm (missing/under-logged time)
      - "blip"  : a sub-5-min segment on an otherwise-normal visit (clock tap)
    """
    visit_total = defaultdict(float)
    for r in detail:
        if r["minutes"] != "":
            visit_total[(r["job_number"], r["date"])] += r["minutes"]
    prop_totals = defaultdict(list)
    for (jn, _d), t in visit_total.items():
        prop_totals[jn].append(t)
    prop_med = {jn: statistics.median(v) for jn, v in prop_totals.items()}
    prop_n = {jn: len(v) for jn, v in prop_totals.items()}

    for r in detail:
        r["flag"] = ""
        r["flag_kind"] = ""
        m = r["minutes"]
        if m == "":
            r["flag"], r["flag_kind"] = "Open entry (never clocked out)", "long"
            continue
        jn = r["job_number"]
        v = visit_total[(jn, r["date"])]
        med = prop_med.get(jn)
        n = prop_n.get(jn, 0)
        reasons, kind = [], ""
        if n >= 3 and med:  # property has a reliable baseline
            if v > 2.0 * med and v > med + 20:
                reasons.append(f"Long: {v:g} min vs ~{med:g} usual (forgot to clock out?)")
                kind = "long"
            elif v < 0.5 * med and v < med - 10:
                reasons.append(f"Short: {v:g} min vs ~{med:g} usual")
                kind = "short"
        else:               # sparse property — fall back to absolute sanity
            if v > 90:
                reasons.append(f"Long: {v:g} min (one-off property, forgot to clock out?)")
                kind = "long"
            elif v < 8 and m < 8:
                reasons.append(f"Short: {v:g} min (one-off property)")
                kind = "short"
        if m < 5:
            reasons.append(f"Tiny segment: {m:g} min")
            kind = kind or "blip"
        r["flag"], r["flag_kind"] = "; ".join(reasons), kind


def apply_exclusions(detail, day_agg):
    """Drop short/long flagged VISITS from the summary aggregations (the Detail
    tab still shows them, highlighted). Tiny 'blip' segments stay — their visit
    total is already correct. Mutates day_agg's mow_min/prop_count to clean
    values and returns (clean_prop_visits, excluded_per_property, n_excluded).
    """
    excluded = {(r["job_number"], r["date"]) for r in detail
                if r["flag_kind"] in ("short", "long")}

    visit_total = defaultdict(float)
    for r in detail:
        if r["minutes"] == "":
            continue
        key = (r["job_number"], r["date"])
        if key in excluded:
            continue
        visit_total[key] += r["minutes"]

    prop_visits = defaultdict(list)
    day_mow = defaultdict(float)
    day_props = defaultdict(set)
    for (jn, d), t in visit_total.items():
        prop_visits[jn].append(t)
        day_mow[d] += t
        day_props[d].add(jn)

    # Rewrite the per-day mow figures to exclude outliers; travel/span untouched.
    for d, agg in day_agg.items():
        agg["mow_min"] = day_mow.get(d, 0.0)
        agg["prop_count"] = len(day_props.get(d, set()))

    excl_count = defaultdict(int)
    for (jn, _d) in excluded:
        excl_count[jn] += 1
    return prop_visits, excl_count, len(excluded)


# ---------------------------------------------------------------------------
# Tab building
# ---------------------------------------------------------------------------
def build_detail(detail):
    header = ["Date", "Day", "Client", "Property", "Job #", "Job Title",
              "Clock In", "Clock Out", "Minutes", "Hours", "Flag (review)"]
    rows = [header]
    for r in detail:
        rows.append([r["date"], r["dow"], r["client"], r["property"],
                     r["job_number"], r["job_title"], r["clock_in"],
                     r["clock_out"], r["minutes"], r["hours"], r.get("flag", "")])
    return rows


def build_by_property(prop_visits, prop_info, excl_count=None):
    # One row per property (mow job), so multi-property clients split out.
    # Stats use clean visits only; "# Flagged" shows how many were left out.
    excl_count = excl_count or {}
    header = ["Client", "Property", "# Mows", "Avg Min", "Fastest Min",
              "Slowest Min", "Total Hours", "# Flagged (excl.)"]
    rows = [header]
    data = []
    for jn, mins in prop_visits.items():
        if not mins:
            continue
        info = prop_info.get(jn, {})
        total = sum(mins)
        data.append([
            info.get("client", ""),
            info.get("street") or f"Job #{jn}",
            len(mins),
            round(total / len(mins), 1),
            round(min(mins), 1),
            round(max(mins), 1),
            round(total / 60.0, 2),
            excl_count.get(jn, 0),
        ])
    data.sort(key=lambda x: x[6], reverse=True)  # most total hours first
    rows.extend(data)
    return rows


def compute_standards(detail):
    """Per-property speed standards. 'Target' = a property's *proven good pace*
    (average of its 3 fastest clean mows), which beats the median because the
    median bakes in truck-sitting and dawdle. Uses clean visits only (short/long
    flagged visits excluded). Returns (props, day_rollup)."""
    excluded = {(r["job_number"], r["date"]) for r in detail
                if r["flag_kind"] in ("short", "long")}
    visit_total = defaultdict(float)
    visit_day = {}
    meta = {}
    for r in detail:
        meta.setdefault(r["job_number"], (r["client"], r["property"]))
        if r["minutes"] == "":
            continue
        key = (r["job_number"], r["date"])
        if key in excluded:
            continue
        visit_total[key] += r["minutes"]
        visit_day[key] = r["dow"]

    prop_times = defaultdict(list)
    prop_days = defaultdict(Counter)
    for (jn, date), t in visit_total.items():
        prop_times[jn].append(t)
        prop_days[jn][visit_day[(jn, date)]] += 1

    props = []
    for jn, ts in prop_times.items():
        if not ts:
            continue
        s = sorted(ts)
        target = statistics.mean(s[:3]) if len(s) >= 3 else statistics.mean(s)
        med = statistics.median(ts)
        client, prop = meta.get(jn, ("", ""))
        usual = prop_days[jn].most_common(1)[0][0] if prop_days[jn] else ""
        props.append({"client": client, "prop": prop, "n": len(ts),
                      "typical": med, "target": target, "best": min(ts),
                      "slack": med - target, "day": usual})

    day_rollup = defaultdict(lambda: {"n": 0, "typ": 0.0, "tgt": 0.0})
    for p in props:
        d = day_rollup[p["day"]]
        d["n"] += 1
        d["typ"] += p["typical"]
        d["tgt"] += p["target"]
    return props, day_rollup


def build_standards(props, day_rollup):
    """Returns (rows, bold_row_indices). Two stacked sections: per-day route
    totals, then per-property targets sorted by usual day then biggest slack."""
    rows, bold = [], []
    bold.append(len(rows))
    rows.append(["ROUTE TOTALS (mow time only — travel & yard are separate)",
                 "", "", "", "", "", "", ""])
    bold.append(len(rows))
    rows.append(["Usual Day", "# Props", "Typical (h)", "Target (h)",
                 "Slack (h)", "", "", ""])
    tot_n = tot_typ = tot_tgt = 0
    for d in ["Mon", "Tue", "Wed", "Thu", "Fri", "Sat", "Sun"]:
        if d not in day_rollup:
            continue
        x = day_rollup[d]
        tot_n += x["n"]; tot_typ += x["typ"]; tot_tgt += x["tgt"]
        rows.append([d, x["n"], round(x["typ"] / 60, 1), round(x["tgt"] / 60, 1),
                     round((x["typ"] - x["tgt"]) / 60, 1), "", "", ""])
    rows.append(["All days", tot_n, round(tot_typ / 60, 1), round(tot_tgt / 60, 1),
                 round((tot_typ - tot_tgt) / 60, 1), "", "", ""])

    rows.append(["", "", "", "", "", "", "", ""])
    bold.append(len(rows))
    rows.append(["PER PROPERTY — Target = average of the 3 fastest clean mows "
                 "(a pace they've already proven)", "", "", "", "", "", "", ""])
    bold.append(len(rows))
    rows.append(["Usual Day", "Client", "Property", "# Mows", "Typical Min",
                 "Target Min", "Best Min", "Slack Min"])
    for p in sorted(props, key=lambda p: (DOW_ORDER.get(p["day"], 9), -p["slack"])):
        rows.append([p["day"], p["client"], p["prop"], p["n"],
                     round(p["typical"], 1), round(p["target"], 1),
                     round(p["best"], 1), round(p["slack"], 1)])
    return rows, bold


def build_by_day(day_agg):
    # Yard Time = clock-in to first mow (loading, fueling, drive to first stop).
    # Read it with Clock In + First Mow: a long yard time with an early clock-in
    # = clocked in early / sat; with a normal clock-in but late first mow = long
    # load/drive (routing).
    header = ["Date", "Day", "Properties Mowed", "Mow Time (h)", "Travel (h)",
              "Clock In", "First Mow", "Yard Time (min)", "Last Out"]
    rows = [header]
    for date_key in sorted(day_agg):
        d = day_agg[date_key]
        first = d["first"]
        fmow = d["first_mow"]
        yard = round((fmow - first).total_seconds() / 60) if (fmow and first) else ""
        rows.append([
            date_key,
            DOW[first.weekday()] if first else "",
            d["prop_count"],
            round(d["mow_min"] / 60.0, 2),
            round(d["travel_min"] / 60.0, 2),
            fmt_time(first),
            fmt_time(fmow),
            yard,
            fmt_time(d["last"]),
        ])
    return rows


def build_travel(day_agg):
    # Travel is the between-mow time only (yard time and the drive back are
    # excluded). Season totals first, then a per-day breakdown.
    total_mow = sum(d["mow_min"] for d in day_agg.values()) / 60.0
    total_travel = sum(d["travel_min"] for d in day_agg.values()) / 60.0
    grand = total_mow + total_travel
    travel_pct = (total_travel / grand * 100) if grand else 0

    rows = [["SEASON TOTALS (mow days, outliers excluded)", "", "", "", ""]]
    rows.append(["Total Mow Time (h)", round(total_mow, 1), "", "", ""])
    rows.append(["Total Travel Time (h)", round(total_travel, 1), "", "", ""])
    rows.append(["Mow + Travel (h)", round(grand, 1), "", "", ""])
    rows.append(["Travel % of mow+travel time", round(travel_pct, 1), "", "", ""])
    rows.append(["", "", "", "", ""])
    rows.append(["Date", "Day", "Mow Time (h)", "Travel (h)", "Travel %"])
    for date_key in sorted(day_agg):
        d = day_agg[date_key]
        first = d["first"]
        mow_h = d["mow_min"] / 60.0
        travel_h = d["travel_min"] / 60.0
        day_total = mow_h + travel_h
        pct = (travel_h / day_total * 100) if day_total else 0
        rows.append([
            date_key,
            DOW[first.weekday()] if first else "",
            round(mow_h, 2),
            round(travel_h, 2),
            round(pct, 1),
        ])
    return rows


# ---------------------------------------------------------------------------
# Sheets write
# ---------------------------------------------------------------------------
def get_sheet_ids(gacc):
    meta = sheets_get(gacc, "?fields=sheets.properties(sheetId,title)")
    return {s["properties"]["title"]: s["properties"]["sheetId"]
            for s in meta["sheets"]}


def delete_tab(gacc, title, existing):
    if title not in existing:
        return
    sheets_post(gacc, ":batchUpdate",
                {"requests": [{"deleteSheet": {"sheetId": existing[title]}}]})
    del existing[title]
    print(f"[deleted stale tab '{title}']")


def ensure_tab(gacc, title, existing):
    if title in existing:
        return existing[title]
    resp = sheets_post(gacc, ":batchUpdate",
                       {"requests": [{"addSheet": {"properties": {"title": title}}}]})
    sid = resp["replies"][0]["addSheet"]["properties"]["sheetId"]
    existing[title] = sid
    print(f"[created tab '{title}']")
    return sid


def clear_and_write(gacc, title, rows):
    # Clear a generous range, then write from A1.
    requests.post(f"{SHEETS_API}/{SHEET_ID}/values/{title}!A1:Z10000:clear",
                  headers={"Authorization": f"Bearer {gacc}"}).raise_for_status()
    r = requests.put(f"{SHEETS_API}/{SHEET_ID}/values/{title}!A1",
                     headers={"Authorization": f"Bearer {gacc}",
                              "Content-Type": "application/json"},
                     params={"valueInputOption": "RAW"},
                     json={"values": rows})
    r.raise_for_status()
    print(f"[wrote {len(rows)} rows to '{title}']")


FLAG_COLORS = {
    "long":  {"red": 0.96, "green": 0.80, "blue": 0.80},  # red   — overstated time
    "short": {"red": 1.00, "green": 0.90, "blue": 0.66},  # amber — under-logged
    "blip":  {"red": 1.00, "green": 0.97, "blue": 0.75},  # yellow — clock tap
}


def highlight_detail(gacc, sid, detail, ncols=11):
    """Color each flagged row by its kind; reset everything else to white so a
    row fixed in Jobber clears its highlight on the next run."""
    n = len(detail)
    white = {"red": 1, "green": 1, "blue": 1}
    reqs = [{"repeatCell": {
        "range": {"sheetId": sid, "startRowIndex": 1, "endRowIndex": n + 1,
                  "startColumnIndex": 0, "endColumnIndex": ncols},
        "cell": {"userEnteredFormat": {"backgroundColor": white}},
        "fields": "userEnteredFormat.backgroundColor",
    }}]
    counts = defaultdict(int)
    for i, r in enumerate(detail):
        kind = r.get("flag_kind")
        if not kind:
            continue
        counts[kind] += 1
        reqs.append({"repeatCell": {
            "range": {"sheetId": sid, "startRowIndex": i + 1, "endRowIndex": i + 2,
                      "startColumnIndex": 0, "endColumnIndex": ncols},
            "cell": {"userEnteredFormat": {"backgroundColor": FLAG_COLORS[kind]}},
            "fields": "userEnteredFormat.backgroundColor",
        }})
    sheets_post(gacc, ":batchUpdate", {"requests": reqs})
    return counts


def bold_rows(gacc, sid, indices, ncols=8):
    """Bold a set of (0-indexed) rows — for tabs with multiple header rows."""
    reqs = [{"repeatCell": {
        "range": {"sheetId": sid, "startRowIndex": i, "endRowIndex": i + 1,
                  "startColumnIndex": 0, "endColumnIndex": ncols},
        "cell": {"userEnteredFormat": {"textFormat": {"bold": True}}},
        "fields": "userEnteredFormat.textFormat.bold",
    }} for i in indices]
    sheets_post(gacc, ":batchUpdate", {"requests": reqs})


def format_header(gacc, sid, freeze_rows=1, bold_row=0):
    """Bold a header row and freeze the top row for readability."""
    sheets_post(gacc, ":batchUpdate", {"requests": [
        {"repeatCell": {
            "range": {"sheetId": sid, "startRowIndex": bold_row,
                      "endRowIndex": bold_row + 1},
            "cell": {"userEnteredFormat": {"textFormat": {"bold": True}}},
            "fields": "userEnteredFormat.textFormat.bold",
        }},
        {"updateSheetProperties": {
            "properties": {"sheetId": sid,
                           "gridProperties": {"frozenRowCount": freeze_rows}},
            "fields": "gridProperties.frozenRowCount",
        }},
    ]})


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------
def main():
    gacc = google_access_token()
    print("[google auth OK]")
    client = JobberClient(gacc)

    entries = fetch_gonzalo_entries(client)
    print(f"[fetched {len(entries)} total Gonzalo timesheet entries]")

    detail, prop_visits, prop_info, day_agg = process(entries)
    compute_flags(detail)
    clean_prop_visits, excl_count, n_excluded = apply_exclusions(detail, day_agg)
    std_rows, std_bold = build_standards(*compute_standards(detail))
    print(f"[{len(detail)} mow segments, {len(prop_visits)} properties, "
          f"{len(day_agg)} mow days | {n_excluded} outlier visits excluded "
          f"from summaries]")

    tabs = {
        DETAIL_TAB: build_detail(detail),
        PROPERTY_TAB: build_by_property(clean_prop_visits, prop_info, excl_count),
        DAY_TAB: build_by_day(day_agg),
        TRAVEL_TAB: build_travel(day_agg),
        STANDARDS_TAB: std_rows,
    }

    existing = get_sheet_ids(gacc)
    for stale in STALE_TABS:
        delete_tab(gacc, stale, existing)
    flag_counts = {}
    for title, rows in tabs.items():
        sid = ensure_tab(gacc, title, existing)
        clear_and_write(gacc, title, rows)
        # Travel tab has its header on row 7 (after the totals block).
        if title == TRAVEL_TAB:
            format_header(gacc, sid, freeze_rows=0, bold_row=6)
        elif title == STANDARDS_TAB:
            bold_rows(gacc, sid, std_bold)
        else:
            format_header(gacc, sid, freeze_rows=1, bold_row=0)
        if title == DETAIL_TAB:
            flag_counts = highlight_detail(gacc, sid, detail)
            flagged = sum(flag_counts.values())
            print(f"[flagged {flagged} rows for review: "
                  f"{flag_counts.get('long',0)} long, {flag_counts.get('short',0)} short, "
                  f"{flag_counts.get('blip',0)} tiny-segment]")

    print("\nDone. Tabs written:", ", ".join(tabs.keys()))
    return {
        "status": "ok",
        "mows": len(detail),
        "properties": len(clean_prop_visits),
        "mow_days": len(day_agg),
        "flagged": sum(flag_counts.values()),
        "outliers_excluded": n_excluded,
        "tabs": list(tabs.keys()),
    }


if __name__ == "__main__":
    main()
