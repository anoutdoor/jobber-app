from datetime import date, timedelta
from collections import defaultdict

from jobber_sync import get_sheets_client, SHEET_ID


def safe_float(val, default=0.0):
    try:
        f = float(val)
        return f if f == f else default  # guard against NaN
    except (ValueError, TypeError):
        return default


def get_sheet_jobs():
    try:
        gc = get_sheets_client()
        ws = gc.open_by_key(SHEET_ID).worksheet("Jobs")
        return ws.get_all_records()
    except Exception as e:
        return []


def parse_jobs(raw):
    parsed = []
    for job in raw:
        close_str = str(job.get("Close Date", ""))[:10]
        if not close_str or len(close_str) < 10:
            continue
        try:
            close_date = date.fromisoformat(close_str)
        except ValueError:
            continue

        def pending_or_float(key):
            v = str(job.get(key, "")).strip()
            return None if v in ("Pending", "") else safe_float(v)

        parsed.append({
            "job_number":       str(job.get("Job #", "")),
            "job_title":        str(job.get("Job Title", "")),
            "client":           str(job.get("Client", "")),
            "crew":             str(job.get("Crew", "Unknown")),
            "close_date":       close_date,
            "close_date_str":   close_str,
            "visit_days":       safe_float(job.get("Visit Days", 0)),
            "revenue":          safe_float(job.get("Invoice Total ($)", 0)),
            "labor_cost":       safe_float(job.get("Labor Cost ($)", 0)),
            "materials_cost":   safe_float(job.get("Materials Cost ($)", 0)),
            "total_job_cost":   pending_or_float("Total Job Cost ($)"),
            "gross_profit":     safe_float(job.get("Gross Profit ($)", 0)),
            "gross_margin_pct": pending_or_float("Gross Margin %"),
            "net_margin_pct":   pending_or_float("Net Margin %"),
            "gross_margin_flag": str(job.get("Gross Margin Flag", "")),
            "net_margin_flag":   str(job.get("Net Margin Flag", "")),
            "rev_per_visit_day": safe_float(job.get("Revenue / Visit Day ($)", 0)),
        })

    parsed.sort(key=lambda x: x["close_date"], reverse=True)
    return parsed


def month_key(d):
    return d.strftime("%Y-%m")


def compute_dashboard():
    raw = get_sheet_jobs()
    if not raw:
        return None

    jobs = parse_jobs(raw)
    if not jobs:
        return None

    today = date.today()
    cur_month = month_key(today)

    month_jobs = [j for j in jobs if month_key(j["close_date"]) == cur_month]

    # ── Summary cards ────────────────────────────────────────────────────────
    month_revenue   = round(sum(j["revenue"] for j in month_jobs), 2)
    month_job_count = len(month_jobs)

    gm_vals = [j["gross_margin_pct"] for j in month_jobs if j["gross_margin_pct"] is not None]
    nm_vals = [j["net_margin_pct"]   for j in month_jobs if j["net_margin_pct"]   is not None]
    avg_gross_margin = round(sum(gm_vals) / len(gm_vals), 1) if gm_vals else None
    avg_net_margin   = round(sum(nm_vals) / len(nm_vals), 1) if nm_vals else None

    # ── Crew leaderboard ─────────────────────────────────────────────────────
    CREWS = ["Ernesto", "Arturo", "Gonzalo", "Other"]
    crew_stats = {}
    for crew in CREWS:
        cj = [j for j in month_jobs if j["crew"] == crew]
        cgm = [j["gross_margin_pct"] for j in cj if j["gross_margin_pct"] is not None]
        rpd = [j["rev_per_visit_day"] for j in cj if j["rev_per_visit_day"]]
        crew_stats[crew] = {
            "jobs":             len(cj),
            "revenue":          round(sum(j["revenue"] for j in cj), 2),
            "avg_gross_margin": round(sum(cgm) / len(cgm), 1) if cgm else None,
            "avg_rev_per_day":  round(sum(rpd) / len(rpd), 2) if rpd else None,
        }

    # ── Chart: gross margin by crew ──────────────────────────────────────────
    crew_margin_chart = {
        "labels": CREWS,
        "data":   [crew_stats[c]["avg_gross_margin"] or 0 for c in CREWS],
        "colors": [
            "#22c55e" if (crew_stats[c]["avg_gross_margin"] or 0) >= 45 else "#ef4444"
            for c in CREWS
        ],
    }

    # ── Chart: monthly revenue last 6 months ─────────────────────────────────
    rev_labels, rev_data = [], []
    for i in range(5, -1, -1):
        m = today.month - i
        y = today.year
        while m <= 0:
            m += 12
            y -= 1
        mk = f"{y}-{m:02d}"
        label = date(y, m, 1).strftime("%b %Y")
        total = round(sum(j["revenue"] for j in jobs if month_key(j["close_date"]) == mk), 2)
        rev_labels.append(label)
        rev_data.append(total)

    monthly_revenue_chart = {"labels": rev_labels, "data": rev_data}

    # ── Chart: jobs per week last 8 weeks ────────────────────────────────────
    week_labels, week_counts = [], []
    week_start_base = today - timedelta(days=today.weekday())
    for i in range(7, -1, -1):
        ws = week_start_base - timedelta(weeks=i)
        we = ws + timedelta(days=6)
        count = sum(1 for j in jobs if ws <= j["close_date"] <= we)
        week_labels.append(ws.strftime("%b %d"))
        week_counts.append(count)

    weekly_jobs_chart = {"labels": week_labels, "data": week_counts}

    return {
        "month_label":          today.strftime("%B %Y"),
        "month_revenue":        month_revenue,
        "month_job_count":      month_job_count,
        "avg_gross_margin":     avg_gross_margin,
        "avg_net_margin":       avg_net_margin,
        "crew_stats":           crew_stats,
        "crews":                CREWS,
        "crew_margin_chart":    crew_margin_chart,
        "monthly_revenue_chart": monthly_revenue_chart,
        "weekly_jobs_chart":    weekly_jobs_chart,
        "recent_jobs":          jobs[:30],
        "last_updated":         today.strftime("%B %d, %Y"),
    }
