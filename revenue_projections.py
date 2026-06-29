"""Revenue projections engine.

Answers "what is my revenue going to be?" by combining two layers:

1. COMMITTED  — every scheduled visit on the Jobber calendar from today through
   the end of the year, valued at real Jobber prices. Because the entire mowing
   season is already scheduled out, this captures essentially all recurring
   revenue as hard, dated dollars. This is the source of truth, not a guess.

2. UPSIDE     — a modeled estimate of one-off work (cleanups, mulch, small
   installs) that you reliably book but that isn't on the calendar yet. Built
   from your recent run-rate of one-off revenue per week and laid on top of the
   committed band only where future weeks are still light. Clearly labeled as
   modeled, never mixed into the committed number.

Valuation rules (validated against the live Jobber schema):
  - RECURRING visit  -> job.total is the PER-VISIT price (the weekly mow price).
                        Every future visit counts that amount, on its own date.
  - ONE_OFF  job     -> job.total is the WHOLE-JOB price. It counts ONCE, on the
                        job's completion date (its last scheduled visit), so a
                        multi-visit cleanup isn't multiplied across its visits.

Pulls visits via the top-level `visits` connection with a startAt date filter
(one efficient query, paginated), reusing the auto-refreshing graphql_request
client. Results cache to projections_cache.json so the page loads instantly and
only re-pulls on demand or on the nightly scheduler.
"""
import os
import json
import logging
from collections import defaultdict
from datetime import datetime, date, timedelta

from jobber_sync import graphql_request, CENTRAL_TZ

logger = logging.getLogger(__name__)

CACHE_FILE = "projections_cache.json"

# How many weeks of recent one-off revenue to average for the upside baseline.
UPSIDE_LOOKBACK_WEEKS = 8

# One-off jobs at or above this price are treated as big installs (hardscapes,
# design/build). They count as committed revenue on their scheduled date, but are
# EXCLUDED from the upside run-rate: you don't book a $50k install every week, so
# extrapolating them would wildly overstate the forecast. The upside band models
# only routine small extras (cleanups, mulch, small repairs) below this line.
SMALL_ONEOFF_MAX = 2000.0

# Full-year forecast (top-down, from QuickBooks P&L history). The committed band
# above is bottom-up from the Jobber schedule; this is the statistical layer the
# whole-year revenue number comes from. Source is the same monthly P&L the
# financials dashboard reads.
PNL_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)),
                        "financial_dashboard", "pnl.json")

# Which scenario is the headline "expected" forecast. The page shows all three as
# a low/expected/high band; this just picks the number we lead with.
#   conservative -> back half matches last year, no growth (factor 1.0)
#   moderate     -> growth eases to half the current YTD pace
#   aggressive   -> current YTD year-over-year pace holds all year
FORECAST_EXPECTED_CASE = "moderate"

# Top-level visits in a date window, each with its parent job's type + price.
# first:100 keeps pagination cheap; the whole-year pull is ~20 pages.
VISITS_QUERY = """
query ProjectionVisits($from: ISO8601DateTime!, $to: ISO8601DateTime!, $cursor: String) {
  visits(filter: { startAt: { after: $from, before: $to } }, first: 100, after: $cursor) {
    nodes {
      id
      startAt
      job {
        id
        jobType
        total
        client { name }
      }
    }
    pageInfo { hasNextPage endCursor }
  }
}
"""


# ---------------------------------------------------------------------------
# Date helpers (Central time — the schedule is read the way Alex reads it)
# ---------------------------------------------------------------------------

def _today_central():
    return datetime.now(CENTRAL_TZ).date()


def _central_date(iso_str):
    """Jobber UTC ISO8601 -> America/Chicago date object."""
    if not iso_str:
        return None
    s = iso_str.strip()
    if s.endswith("Z"):
        s = s[:-1] + "+00:00"
    try:
        dt = datetime.fromisoformat(s)
    except ValueError:
        return None
    if dt.tzinfo is None:
        import pytz
        dt = pytz.utc.localize(dt)
    return dt.astimezone(CENTRAL_TZ).date()


def _week_start(d):
    """Monday of the week containing date d."""
    return d - timedelta(days=d.weekday())


def _week_label(d):
    """Short label for a week starting Monday d, e.g. 'Jun 29'."""
    return d.strftime("%b %-d")


def _month_label(d):
    return d.strftime("%b %Y")


# ---------------------------------------------------------------------------
# Data pull
# ---------------------------------------------------------------------------

def fetch_visits(start_iso, end_iso, access_token=None):
    """Paginate the top-level visits connection over [start_iso, end_iso).
    Returns a flat list of {visit_id, date, job_id, job_type, total, client}."""
    out = []
    cursor = None
    pages = 0
    while True:
        body = graphql_request(
            VISITS_QUERY,
            variables={"from": start_iso, "to": end_iso, "cursor": cursor},
            access_token=access_token,
        )
        if not body or not body.get("data"):
            logger.error("projections: visits pull returned no data on page %s", pages)
            break
        conn = body["data"]["visits"]
        for node in conn.get("nodes", []):
            job = node.get("job") or {}
            d = _central_date(node.get("startAt"))
            if d is None:
                continue
            out.append({
                "visit_id": node.get("id"),
                "date": d,
                "job_id": job.get("id"),
                "job_type": job.get("jobType"),
                "total": float(job.get("total") or 0.0),
                "client": (job.get("client") or {}).get("name", ""),
            })
        page_info = conn.get("pageInfo") or {}
        pages += 1
        if page_info.get("hasNextPage") and pages < 60:
            cursor = page_info.get("endCursor")
        else:
            break
    logger.info("projections: pulled %s visits across %s pages", len(out), pages)
    return out


# ---------------------------------------------------------------------------
# Projection math
# ---------------------------------------------------------------------------

def _committed_events(visits, today, horizon_end):
    """Turn raw visits into dated revenue events in [today, horizon_end].

    Recurring: one event per future visit at job.total.
    One-off:   one event per job at job.total, dated to the job's completion
               (its last scheduled visit), counted only if completion is in window.
    Returns list of {date, amount, kind, client} where kind is 'recurring'/'oneoff'.
    """
    events = []

    # One-off jobs: collapse to a single completion event per job.
    oneoff_last = {}  # job_id -> {date, total, client}
    for v in visits:
        if v["job_type"] == "ONE_OFF":
            jid = v["job_id"]
            cur = oneoff_last.get(jid)
            if cur is None or v["date"] > cur["date"]:
                oneoff_last[jid] = {"date": v["date"], "total": v["total"], "client": v["client"]}
        elif v["job_type"] == "RECURRING":
            if today <= v["date"] <= horizon_end and v["total"] > 0:
                events.append({"date": v["date"], "amount": v["total"],
                               "kind": "recurring", "client": v["client"]})

    for jid, info in oneoff_last.items():
        if today <= info["date"] <= horizon_end and info["total"] > 0:
            events.append({"date": info["date"], "amount": info["total"],
                           "kind": "oneoff", "client": info["client"]})

    return events


def _oneoff_weekly_baseline(visits, today):
    """Average one-off revenue per week over the trailing lookback window.

    Uses one-off completion events that fell in the recent past, so the upside
    band reflects how much unscheduled small work you've actually been booking.
    """
    window_start = today - timedelta(weeks=UPSIDE_LOOKBACK_WEEKS)
    # Completion event per one-off job, same rule as committed.
    oneoff_last = {}
    for v in visits:
        if v["job_type"] != "ONE_OFF":
            continue
        jid = v["job_id"]
        cur = oneoff_last.get(jid)
        if cur is None or v["date"] > cur["date"]:
            oneoff_last[jid] = {"date": v["date"], "total": v["total"]}
    # Small extras only — big installs are committed-but-not-extrapolated.
    recent = [i["total"] for i in oneoff_last.values()
              if window_start <= i["date"] < today and 0 < i["total"] < SMALL_ONEOFF_MAX]
    total = sum(recent)
    return total / float(UPSIDE_LOOKBACK_WEEKS) if UPSIDE_LOOKBACK_WEEKS else 0.0


def _weekly_buckets(events, baseline_weekly, today, horizon_end):
    """Canonical weekly series with committed + upside bands. Keys are the
    Monday-start ISO date of each week, so the payload is JSON safe.

    Upside tops a week's booked small-extra revenue up to the recent run-rate,
    so a week already loaded with small work shows little upside and a quiet week
    shows the modeled run-rate you typically still book. The run-rate is SCALED by
    how busy the crew is that week (its recurring revenue vs a typical peak week),
    so the modeled band follows the real operating season: full at peak, tapering
    through the shoulders, ~zero in deep winter, instead of projecting spring
    cleanups flat into December.
    """
    import statistics as _stats
    recurring = defaultdict(float)
    oneoff = defaultdict(float)
    small_oneoff = defaultdict(float)  # only the extras the run-rate competes with
    order = []

    d = _week_start(today)
    seen = set()
    while d <= horizon_end:
        k = d.isoformat()
        recurring[k] = 0.0
        oneoff[k] = 0.0
        small_oneoff[k] = 0.0
        order.append((k, d))
        seen.add(k)
        d += timedelta(days=7)

    for e in events:
        sd = _week_start(e["date"])
        k = sd.isoformat()
        if k not in seen:
            recurring[k] = 0.0; oneoff[k] = 0.0; small_oneoff[k] = 0.0
            order.append((k, sd)); seen.add(k)
        if e["kind"] == "recurring":
            recurring[k] += e["amount"]
        else:
            oneoff[k] += e["amount"]
            if e["amount"] < SMALL_ONEOFF_MAX:
                small_oneoff[k] += e["amount"]

    order.sort(key=lambda t: t[1])

    # A "typical" active week = median recurring across weeks the crew is out.
    # Used to scale the upside run-rate down through the shoulder season.
    active_rec = [recurring[k] for k, _ in order if recurring[k] > 0]
    typical_rec = _stats.median(active_rec) if active_rec else 0.0

    rows = []
    for k, sd in order:
        if typical_rec > 0:
            activity = min(1.0, recurring[k] / typical_rec)
        else:
            activity = 1.0 if recurring[k] > 0 else 0.0
        upside = max(0.0, baseline_weekly * activity - small_oneoff[k])
        rows.append({
            "key": k,
            "label": _week_label(sd),
            "recurring": round(recurring[k], 2),
            "oneoff": round(oneoff[k], 2),
            "committed": round(recurring[k] + oneoff[k], 2),
            "upside": round(upside, 2),
            "projected": round(recurring[k] + oneoff[k] + upside, 2),
        })
    return rows


def _aggregate_monthly(weekly_rows):
    """Roll the canonical weekly series up to months so the two views reconcile
    exactly. A week is attributed to the month of its Monday start date."""
    months = defaultdict(lambda: {"recurring": 0.0, "oneoff": 0.0, "upside": 0.0})
    order = []
    for r in weekly_rows:
        sd = date.fromisoformat(r["key"])
        mk = date(sd.year, sd.month, 1)
        k = mk.isoformat()
        if k not in months:
            order.append((k, mk))
        months[k]["recurring"] += r["recurring"]
        months[k]["oneoff"] += r["oneoff"]
        months[k]["upside"] += r["upside"]

    order.sort(key=lambda t: t[1])
    rows = []
    for k, mk in order:
        m = months[k]
        committed = m["recurring"] + m["oneoff"]
        rows.append({
            "key": k,
            "label": _month_label(mk),
            "recurring": round(m["recurring"], 2),
            "oneoff": round(m["oneoff"], 2),
            "committed": round(committed, 2),
            "upside": round(m["upside"], 2),
            "projected": round(committed + m["upside"], 2),
        })
    return rows


# ---------------------------------------------------------------------------
# Full-year forecast from P&L history
# ---------------------------------------------------------------------------

def _load_pnl_revenue():
    """Return {('YYYY-MM'): revenue} for complete months from the P&L export,
    plus the set of keys flagged incomplete. Missing file -> empty."""
    try:
        with open(PNL_PATH) as f:
            doc = json.load(f)
    except Exception:
        logger.warning("projections: P&L file not readable at %s", PNL_PATH)
        return {}, set()
    rev, incomplete = {}, set()
    for m in doc.get("months", []):
        k = m.get("key")
        if not k:
            continue
        rev[k] = float(m.get("revenue") or 0.0)
        if m.get("incomplete"):
            incomplete.add(k)
    return rev, incomplete


def build_forecast(today):
    """Top-down full-year revenue forecast from QuickBooks monthly history.

    Method: anchor on this year's actual revenue to date, then project each
    remaining month from the prior full year's same-month revenue scaled by a
    growth factor. Three scenarios bracket how much of the current
    year-over-year pace holds through the back half:
      low  (conservative) factor 1.0   - prior year repeats, growth stalls
      mid  (moderate)      factor 1+(g-1)/2 - growth eases to half the YTD pace
      high (aggressive)    factor g     - current YTD YoY pace holds

    Returns None if there isn't enough history (need a prior-year comparable).
    """
    rev, incomplete = _load_pnl_revenue()
    if not rev:
        return None

    cur = today.year
    prior = cur - 1

    # Actual months this year = present, this year, not flagged incomplete.
    actual_keys = sorted(k for k in rev
                         if k.startswith(f"{cur}-") and k not in incomplete)
    if not actual_keys:
        return None
    last_actual_month = int(actual_keys[-1].split("-")[1])

    ytd_actual = sum(rev[k] for k in actual_keys)
    # Same span last year, for the growth comparison.
    prior_ytd = sum(rev.get(f"{prior}-{int(k.split('-')[1]):02d}", 0.0) for k in actual_keys)
    prior_total = sum(rev.get(f"{prior}-{m:02d}", 0.0) for m in range(1, 13))

    if prior_ytd <= 0:
        return None  # no comparable prior year -> can't ground a growth rate
    growth = ytd_actual / prior_ytd

    factors = {
        "low": 1.0,
        "mid": 1.0 + (growth - 1.0) * 0.5,
        "high": growth,
    }

    months = []
    totals = {"low": ytd_actual, "mid": ytd_actual, "high": ytd_actual}
    for m in range(1, 13):
        mk = f"{cur}-{m:02d}"
        label = date(cur, m, 1).strftime("%b")
        if m <= last_actual_month and mk in rev:
            v = round(rev[mk], 2)
            months.append({"month": m, "label": label, "is_actual": True,
                           "actual": v, "low": v, "mid": v, "high": v})
        else:
            base = rev.get(f"{prior}-{m:02d}", 0.0)
            row = {"month": m, "label": label, "is_actual": False, "actual": None}
            for s in ("low", "mid", "high"):
                fv = round(base * factors[s], 2)
                row[s] = fv
                totals[s] += fv
            months.append(row)

    full_year = {s: round(totals[s], 2) for s in totals}
    full_year["expected"] = full_year[{"conservative": "low", "moderate": "mid",
                                        "aggressive": "high"}[FORECAST_EXPECTED_CASE]]

    return {
        "current_year": cur,
        "prior_year": prior,
        "expected_case": FORECAST_EXPECTED_CASE,
        "last_actual_label": date(cur, last_actual_month, 1).strftime("%b"),
        "ytd_actual": round(ytd_actual, 2),
        "prior_ytd": round(prior_ytd, 2),
        "prior_year_total": round(prior_total, 2),
        "growth_pct": round((growth - 1.0) * 100, 1),
        "full_year": full_year,
        "months": months,
    }


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

def compute(access_token=None, force=False):
    """Build the full projection payload, using cache unless force=True."""
    if not force:
        cached = _load_cache()
        if cached:
            return cached

    today = _today_central()
    horizon_end = date(today.year, 12, 31)
    # Pull a trailing window too, so the upside baseline has recent actuals.
    pull_start = today - timedelta(weeks=UPSIDE_LOOKBACK_WEEKS)

    start_iso = datetime(pull_start.year, pull_start.month, pull_start.day).strftime("%Y-%m-%dT00:00:00Z")
    end_iso = datetime(horizon_end.year, horizon_end.month, horizon_end.day).strftime("%Y-%m-%dT23:59:59Z")

    visits = fetch_visits(start_iso, end_iso, access_token=access_token)

    events = _committed_events(visits, today, horizon_end)
    baseline_weekly = _oneoff_weekly_baseline(visits, today)

    weekly = _weekly_buckets(events, baseline_weekly, today, horizon_end)
    monthly = _aggregate_monthly(weekly)

    committed_recurring = round(sum(e["amount"] for e in events if e["kind"] == "recurring"), 2)
    committed_oneoff = round(sum(e["amount"] for e in events if e["kind"] == "oneoff"), 2)
    committed_total = round(committed_recurring + committed_oneoff, 2)
    upside_total = round(sum(r["upside"] for r in weekly), 2)
    weeks_remaining = max(1, len(weekly))

    payload = {
        "last_updated": datetime.now(CENTRAL_TZ).strftime("%b %-d, %-I:%M %p CT"),
        "today": today.isoformat(),
        "horizon_end": horizon_end.isoformat(),
        "weeks_remaining": len(weekly),
        "baseline_weekly_oneoff": round(baseline_weekly, 2),
        "summary": {
            "committed_total": committed_total,
            "committed_recurring": committed_recurring,
            "committed_oneoff": committed_oneoff,
            "upside_total": upside_total,
            "projected_total": round(committed_total + upside_total, 2),
            "avg_committed_per_week": round(committed_total / weeks_remaining, 2),
        },
        "weekly": weekly,
        "monthly": monthly,
        "forecast": build_forecast(today),
    }
    _save_cache(payload)
    return payload


def get_payload(force=False, access_token=None):
    return compute(access_token=access_token, force=force)


# ---------------------------------------------------------------------------
# Cache
# ---------------------------------------------------------------------------

def _save_cache(payload):
    try:
        with open(CACHE_FILE, "w") as f:
            json.dump(payload, f)
    except Exception:
        logger.exception("projections: cache write failed")


def _load_cache():
    if os.path.exists(CACHE_FILE):
        try:
            with open(CACHE_FILE) as f:
                return json.load(f)
        except Exception:
            logger.exception("projections: cache read failed")
    return None
