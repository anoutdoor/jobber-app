import os
import json
import logging
from datetime import datetime

import requests
from dotenv import load_dotenv
import gspread
from google.oauth2.credentials import Credentials
from google.auth.transport.requests import Request
from google_auth_oauthlib.flow import InstalledAppFlow

load_dotenv()

logger = logging.getLogger(__name__)

GRAPHQL_URL = "https://api.getjobber.com/api/graphql"
GRAPHQL_VERSION = "2026-03-10"
TOKEN_URL = "https://api.getjobber.com/api/oauth/token"

TOKEN_STORE_FILE = "token_store.json"
SYNCED_JOBS_FILE = "synced_jobs.json"
LAST_SYNC_FILE = "last_sync.txt"
GOOGLE_TOKEN_FILE = "token.json"

GOOGLE_SCOPES = ["https://www.googleapis.com/auth/spreadsheets"]
GOOGLE_CLIENT_SECRETS_FILE = os.getenv("GOOGLE_CLIENT_SECRETS_FILE", "client_secrets.json")
SHEET_ID = os.getenv("GOOGLE_SHEET_ID")

# ---------------------------------------------------------------------------
# Crew config — priority order matters (first match wins)
# ---------------------------------------------------------------------------
CREW_CONFIG = [
    {"name": "Ernesto Cardenas", "crew_label": "Ernesto", "daily_overhead": 346},
    {"name": "Arturo L Marin",   "crew_label": "Arturo",  "daily_overhead": 295},
    {"name": "Gonzalo Feroz",    "crew_label": "Gonzalo", "daily_overhead": 252},
]

# Crews that split daily overhead proportionally across multiple jobs per day
PROPORTIONAL_OVERHEAD_CREWS = {"Arturo", "Gonzalo"}

# ---------------------------------------------------------------------------
# GraphQL query — labor cost pulled directly from Jobber
# ---------------------------------------------------------------------------
JOBS_QUERY = """
query GetClosedJobs($cursor: String) {
  jobs(filter: { status: requires_invoicing }, first: 10, after: $cursor) {
    nodes {
      id
      title
      jobNumber
      completedAt
      jobType
      client {
        name
      }
      property {
        address {
          street
          city
          province
          postalCode
        }
      }
      jobCosting {
        labourCost
        labourDuration
        expenseCost
        totalRevenue
      }
      visits(first: 10) {
        nodes {
          startAt
        }
      }
      timeSheetEntries(first: 10) {
        nodes {
          user {
            name {
              full
            }
          }
        }
      }
    }
    pageInfo {
      hasNextPage
      endCursor
    }
  }
}
"""

# ---------------------------------------------------------------------------
# Jobber token management
# ---------------------------------------------------------------------------

def load_tokens():
    if os.path.exists(TOKEN_STORE_FILE):
        with open(TOKEN_STORE_FILE) as f:
            return json.load(f)
    return {}


def save_tokens(access_token, refresh_token):
    with open(TOKEN_STORE_FILE, "w") as f:
        json.dump({"access_token": access_token, "refresh_token": refresh_token}, f)


def refresh_access_token():
    tokens = load_tokens()
    refresh_token = tokens.get("refresh_token")
    if not refresh_token:
        logger.error("No refresh token — user must re-authenticate at /login.")
        return None

    resp = requests.post(
        TOKEN_URL,
        data={
            "grant_type": "refresh_token",
            "refresh_token": refresh_token,
            "client_id": os.getenv("JOBBER_CLIENT_ID"),
            "client_secret": os.getenv("JOBBER_CLIENT_SECRET"),
        },
        headers={"Content-Type": "application/x-www-form-urlencoded"},
    )

    if not resp.ok:
        logger.error(f"Token refresh failed: {resp.text}")
        return None

    data = resp.json()
    new_access = data.get("access_token")
    new_refresh = data.get("refresh_token", refresh_token)
    save_tokens(new_access, new_refresh)
    logger.info("Jobber access token refreshed successfully.")
    return new_access


# ---------------------------------------------------------------------------
# GraphQL client with auto-refresh on 401
# ---------------------------------------------------------------------------

def graphql_request(query, variables=None, access_token=None, _retry=True):
    if not access_token:
        access_token = load_tokens().get("access_token")

    resp = requests.post(
        GRAPHQL_URL,
        json={"query": query, "variables": variables or {}},
        headers={
            "Authorization": f"Bearer {access_token}",
            "Content-Type": "application/json",
            "X-JOBBER-GRAPHQL-VERSION": GRAPHQL_VERSION,
        },
    )

    if resp.status_code == 401 and _retry:
        logger.info("Token expired — attempting refresh.")
        new_token = refresh_access_token()
        if new_token:
            return graphql_request(query, variables, new_token, _retry=False)
        return None

    if not resp.ok:
        logger.error(f"GraphQL HTTP error {resp.status_code}: {resp.text}")
        return None

    return resp.json()


def fetch_all_closed_jobs():
    jobs = []
    cursor = None

    while True:
        data = graphql_request(JOBS_QUERY, {"cursor": cursor})
        if not data:
            break

        jobs_data = (data.get("data") or {}).get("jobs")
        if not jobs_data:
            logger.error(f"Unexpected GraphQL response shape: {json.dumps(data)[:500]}")
            break

        nodes = jobs_data.get("nodes", [])
        jobs.extend(nodes)
        logger.info(f"Fetched page of {len(nodes)} jobs (total so far: {len(jobs)})")

        page_info = jobs_data.get("pageInfo", {})
        if not page_info.get("hasNextPage"):
            break
        cursor = page_info.get("endCursor")

    return jobs


# ---------------------------------------------------------------------------
# Costing logic
# ---------------------------------------------------------------------------

def resolve_crew(assigned_names):
    for config in CREW_CONFIG:
        if config["name"] in assigned_names:
            return config["crew_label"], config["daily_overhead"]
    return "Other", 0


def count_visit_days(visits):
    dates = set()
    for visit in visits:
        start = visit.get("startAt", "")
        if start:
            dates.add(start[:10])  # YYYY-MM-DD
    return len(dates), sorted(dates)


def format_address(addr):
    if not addr:
        return ""
    return ", ".join(
        p for p in [
            addr.get("street", ""),
            addr.get("city", ""),
            addr.get("province", ""),
            addr.get("postalCode", ""),
        ] if p
    )


def cost_job(job):
    # Crew determined from who clocked time on the job
    timesheet_nodes = (job.get("timeSheetEntries") or {}).get("nodes", [])
    worked_names = list({((n.get("user") or {}).get("name") or {}).get("full", "") for n in timesheet_nodes if ((n.get("user") or {}).get("name") or {}).get("full")})

    crew_label, daily_overhead_rate = resolve_crew(worked_names)
    team_count = len(worked_names) or 1

    visit_nodes = (job.get("visits") or {}).get("nodes", [])
    visit_day_count, visit_dates = count_visit_days(visit_nodes)

    # All cost and revenue figures come from Jobber's built-in jobCosting object
    jc = job.get("jobCosting") or {}
    labor_cost = round(float(jc.get("labourCost") or 0), 2)
    labour_duration_seconds = float(jc.get("labourDuration") or 0)
    labor_hours = round(labour_duration_seconds / 3600, 2)
    materials_cost = round(float(jc.get("expenseCost") or 0), 2)
    invoice_total = round(float(jc.get("totalRevenue") or 0), 2)

    # Gross profit is overhead-independent — always calculated immediately
    gross_profit = round(invoice_total - labor_cost - materials_cost, 2)
    gross_margin_pct = round(gross_profit / invoice_total * 100, 2) if invoice_total else 0.0
    gross_margin_flag = "FLAG: BELOW 45%" if gross_margin_pct < 45 else ""
    rev_per_day = round(invoice_total / visit_day_count, 2) if visit_day_count else 0.0

    base = {
        "job_id": job.get("id", ""),
        "job_number": str(job.get("jobNumber", "")),
        "job_title": job.get("title", ""),
        "client_name": (job.get("client") or {}).get("name", ""),
        "property_address": format_address((job.get("property") or {}).get("address")),
        "close_date": (job.get("completedAt") or "")[:10],
        "crew": crew_label,
        "team_members": ", ".join(worked_names),
        "team_count": team_count,
        "visit_dates": ", ".join(visit_dates),
        "visit_days": visit_day_count,
        "labor_hours": labor_hours,
        "labor_cost": labor_cost,
        "materials_cost": materials_cost,
        "invoice_total": invoice_total,
        "gross_profit": gross_profit,
        "gross_margin_pct": gross_margin_pct,
        "gross_margin_flag": gross_margin_flag,
        "rev_per_visit_day": rev_per_day,
        "synced_at": datetime.utcnow().strftime("%Y-%m-%d %H:%M UTC"),
    }

    if crew_label in PROPORTIONAL_OVERHEAD_CREWS:
        # Pass 1: defer overhead-dependent fields until 8pm reconciliation
        base.update({
            "daily_overhead_rate": "Pending",
            "total_overhead": "Pending",
            "total_job_cost": "Pending",
            "net_profit": "Pending",
            "net_margin_pct": "Pending",
            "net_margin_flag": "Pending",
        })
    else:
        # Ernesto: full daily overhead applied immediately
        total_overhead = round(visit_day_count * daily_overhead_rate, 2)
        total_job_cost = round(total_overhead + labor_cost + materials_cost, 2)
        net_profit = round(invoice_total - total_job_cost, 2)
        net_margin_pct = round(net_profit / invoice_total * 100, 2) if invoice_total else 0.0
        base.update({
            "daily_overhead_rate": daily_overhead_rate,
            "total_overhead": total_overhead,
            "total_job_cost": total_job_cost,
            "net_profit": net_profit,
            "net_margin_pct": net_margin_pct,
            "net_margin_flag": "FLAG: BELOW 15%" if net_margin_pct < 15 else "",
        })

    return base


# ---------------------------------------------------------------------------
# Google Sheets — OAuth user credentials (token saved to token.json)
# ---------------------------------------------------------------------------

def get_google_credentials():
    creds = None

    if os.path.exists(GOOGLE_TOKEN_FILE):
        creds = Credentials.from_authorized_user_file(GOOGLE_TOKEN_FILE, GOOGLE_SCOPES)

    if not creds or not creds.valid:
        if creds and creds.expired and creds.refresh_token:
            creds.refresh(Request())
            logger.info("Google credentials refreshed.")
        else:
            if not os.path.exists(GOOGLE_CLIENT_SECRETS_FILE):
                raise FileNotFoundError(
                    f"Google client secrets file not found: '{GOOGLE_CLIENT_SECRETS_FILE}'. "
                    "Download it from Google Cloud Console (OAuth 2.0 Desktop app credentials) "
                    "and place it in the project folder."
                )
            flow = InstalledAppFlow.from_client_secrets_file(
                GOOGLE_CLIENT_SECRETS_FILE, GOOGLE_SCOPES
            )
            creds = flow.run_local_server(port=0)
            logger.info("Google authorization completed.")

        with open(GOOGLE_TOKEN_FILE, "w") as f:
            f.write(creds.to_json())
        logger.info(f"Google credentials saved to {GOOGLE_TOKEN_FILE}.")

    return creds


def get_sheets_client():
    creds = get_google_credentials()
    return gspread.authorize(creds)


JOBS_HEADERS = [
    "Job ID", "Job #", "Job Title", "Client", "Property Address",
    "Close Date", "Crew", "Team Members", "Team Count", "Visit Dates", "Visit Days",
    "Daily Overhead Rate ($)", "Total Overhead ($)", "Labor Hours",
    "Labor Cost ($)", "Materials Cost ($)", "Total Job Cost ($)",
    "Invoice Total ($)", "Gross Profit ($)", "Gross Margin %",
    "Gross Margin Flag", "Net Profit ($)", "Net Margin %",
    "Net Margin Flag", "Revenue / Visit Day ($)", "Synced At",
]

CREW_OVERHEAD_HEADERS = ["Crew", "Lead Name", "Daily Overhead Rate ($)"]
CREW_OVERHEAD_DATA = [
    ["Ernesto", "Ernesto Cardenas", 346],
    ["Arturo",  "Arturo L Marin",   295],
    ["Gonzalo", "Gonzalo Feroz",    252],
]


def ensure_sheets(spreadsheet):
    existing = {ws.title for ws in spreadsheet.worksheets()}

    if "Jobs" not in existing:
        ws = spreadsheet.add_worksheet(title="Jobs", rows=2000, cols=30)
        ws.update("A1", [JOBS_HEADERS])
        logger.info("Created 'Jobs' tab.")
    else:
        ws = spreadsheet.worksheet("Jobs")
        if ws.row_values(1) != JOBS_HEADERS:
            ws.update("A1", [JOBS_HEADERS])
            logger.info("Updated 'Jobs' tab headers.")

    if "Crew Overhead" not in existing:
        ws = spreadsheet.add_worksheet(title="Crew Overhead", rows=10, cols=5)
        ws.append_row(CREW_OVERHEAD_HEADERS)
        for row in CREW_OVERHEAD_DATA:
            ws.append_row(row)
        logger.info("Created 'Crew Overhead' tab.")


def row_from_costed(c):
    return [
        c["job_id"], c["job_number"], c["job_title"], c["client_name"],
        c["property_address"], c["close_date"], c["crew"], c["team_members"],
        c["team_count"], c["visit_dates"], c["visit_days"],
        c["daily_overhead_rate"], c["total_overhead"], c["labor_hours"],
        c["labor_cost"], c["materials_cost"], c["total_job_cost"],
        c["invoice_total"], c["gross_profit"], c["gross_margin_pct"],
        c["gross_margin_flag"], c["net_profit"], c["net_margin_pct"],
        c["net_margin_flag"], c["rev_per_visit_day"], c["synced_at"],
    ]


# ---------------------------------------------------------------------------
# Sync state helpers
# ---------------------------------------------------------------------------

def load_synced_ids():
    if os.path.exists(SYNCED_JOBS_FILE):
        with open(SYNCED_JOBS_FILE) as f:
            return set(json.load(f))
    return set()


def save_synced_ids(ids):
    with open(SYNCED_JOBS_FILE, "w") as f:
        json.dump(list(ids), f)


def write_last_sync(status="ok", count=0, message=""):
    with open(LAST_SYNC_FILE, "w") as f:
        json.dump({
            "timestamp": datetime.utcnow().strftime("%Y-%m-%d %H:%M UTC"),
            "status": status,
            "jobs_synced": count,
            "message": message,
        }, f)


def read_last_sync():
    if os.path.exists(LAST_SYNC_FILE):
        with open(LAST_SYNC_FILE) as f:
            return json.load(f)
    return None


# ---------------------------------------------------------------------------
# Daily overhead reconciliation (Pass 2 — runs at 8pm)
# ---------------------------------------------------------------------------

def _col_to_letter(n):
    """Convert 1-indexed column number to A1 letter notation."""
    result = ""
    while n:
        n, rem = divmod(n - 1, 26)
        result = chr(65 + rem) + result
    return result


def reconcile_daily_overhead():
    from collections import defaultdict
    logger.info("=== Daily overhead reconciliation starting ===")

    if not SHEET_ID:
        return {"status": "error", "message": "GOOGLE_SHEET_ID not set"}

    try:
        gc = get_sheets_client()
        spreadsheet = gc.open_by_key(SHEET_ID)
        jobs_ws = spreadsheet.worksheet("Jobs")
    except Exception as e:
        logger.error(f"Sheets error in reconciliation: {e}")
        return {"status": "error", "message": str(e)}

    all_values = jobs_ws.get_all_values()
    if len(all_values) < 2:
        return {"status": "ok", "reconciled": 0, "message": "No data rows"}

    headers = all_values[0]
    col = {h: i for i, h in enumerate(headers)}

    required = [
        "Total Overhead ($)", "Crew", "Close Date", "Labor Hours", "Team Count",
        "Daily Overhead Rate ($)", "Total Job Cost ($)", "Labor Cost ($)",
        "Materials Cost ($)", "Invoice Total ($)", "Net Profit ($)",
        "Net Margin %", "Net Margin Flag",
    ]
    missing = [f for f in required if f not in col]
    if missing:
        msg = f"Missing sheet columns: {missing}"
        logger.error(msg)
        return {"status": "error", "message": msg}

    # Collect rows where overhead is still Pending
    pending = []
    for row_idx, row in enumerate(all_values[1:], start=2):
        overhead_val = row[col["Total Overhead ($)"]] if len(row) > col["Total Overhead ($)"] else ""
        if overhead_val == "Pending":
            pending.append((row_idx, row))

    if not pending:
        logger.info("No pending rows to reconcile.")
        return {"status": "ok", "reconciled": 0, "message": "No pending rows"}

    # Group by (crew, close_date)
    groups = defaultdict(list)
    for row_idx, row in pending:
        crew = row[col["Crew"]] if len(row) > col["Crew"] else ""
        close_date = (row[col["Close Date"]] if len(row) > col["Close Date"] else "")[:10]
        groups[(crew, close_date)].append((row_idx, row))

    batch_updates = []
    reconciled = 0

    for (crew, close_date), group_rows in groups.items():
        daily_rate = next(
            (c["daily_overhead"] for c in CREW_CONFIG if c["crew_label"] == crew), 0
        )

        # job duration = labor_hours / team_count (calendar hours the crew was on site)
        durations = []
        for _, row in group_rows:
            labor_h = float(row[col["Labor Hours"]] or 0) if len(row) > col["Labor Hours"] else 0
            t_count = int(row[col["Team Count"]] or 1) if len(row) > col["Team Count"] else 1
            durations.append(labor_h / t_count if t_count else labor_h)

        total_dur = sum(durations)

        for i, (row_idx, row) in enumerate(group_rows):
            proportion = durations[i] / total_dur if total_dur else 1.0 / len(group_rows)
            overhead = round(daily_rate * proportion, 2)

            labor_cost    = float(row[col["Labor Cost ($)"]]    or 0) if len(row) > col["Labor Cost ($)"]    else 0
            materials_cost = float(row[col["Materials Cost ($)"]] or 0) if len(row) > col["Materials Cost ($)"] else 0
            invoice_total  = float(row[col["Invoice Total ($)"]]  or 0) if len(row) > col["Invoice Total ($)"]  else 0

            total_job_cost = round(overhead + labor_cost + materials_cost, 2)
            net_profit     = round(invoice_total - total_job_cost, 2)
            net_margin_pct = round(net_profit / invoice_total * 100, 2) if invoice_total else 0.0
            net_margin_flag = "FLAG: BELOW 15%" if net_margin_pct < 15 else ""

            for field, value in [
                ("Daily Overhead Rate ($)", daily_rate),
                ("Total Overhead ($)",      overhead),
                ("Total Job Cost ($)",      total_job_cost),
                ("Net Profit ($)",          net_profit),
                ("Net Margin %",            net_margin_pct),
                ("Net Margin Flag",         net_margin_flag),
            ]:
                col_letter = _col_to_letter(col[field] + 1)
                batch_updates.append({
                    "range": f"{col_letter}{row_idx}",
                    "values": [[value]],
                })

            reconciled += 1

    if batch_updates:
        jobs_ws.batch_update(batch_updates)

    logger.info(f"=== Reconciliation complete: {reconciled} rows updated ===")
    return {"status": "ok", "reconciled": reconciled}


# ---------------------------------------------------------------------------
# Main sync entry point
# ---------------------------------------------------------------------------

def run_sync():
    logger.info("=== Jobber → Sheets sync starting ===")

    tokens = load_tokens()
    if not tokens.get("access_token"):
        msg = "No access token. Visit /login to authenticate first."
        logger.warning(msg)
        write_last_sync("error", 0, msg)
        return {"status": "error", "message": msg}

    if not SHEET_ID:
        msg = "GOOGLE_SHEET_ID is not set in .env."
        logger.error(msg)
        write_last_sync("error", 0, msg)
        return {"status": "error", "message": msg}

    all_jobs = fetch_all_closed_jobs()
    logger.info(f"Total closed jobs from Jobber: {len(all_jobs)}")

    jobs = [j for j in all_jobs if (j.get("jobType") or "").upper() != "RECURRING"]
    logger.info(f"After filtering recurring jobs: {len(jobs)}")

    synced_ids = load_synced_ids()
    new_jobs = [j for j in jobs if j.get("id") not in synced_ids]
    logger.info(f"New jobs to write: {len(new_jobs)}")

    if not new_jobs:
        write_last_sync("ok", 0, "No new jobs.")
        return {"status": "ok", "synced": 0, "message": "No new jobs to sync."}

    try:
        gc = get_sheets_client()
        spreadsheet = gc.open_by_key(SHEET_ID)
        ensure_sheets(spreadsheet)
        jobs_ws = spreadsheet.worksheet("Jobs")
    except Exception as e:
        msg = f"Google Sheets error: {e}"
        logger.error(msg)
        write_last_sync("error", 0, msg)
        return {"status": "error", "message": msg}

    rows = []
    errors = []
    for job in new_jobs:
        try:
            rows.append((job.get("id"), row_from_costed(cost_job(job))))
        except Exception as e:
            msg = f"Job {job.get('jobNumber')}: {e}"
            logger.error(msg)
            errors.append(msg)

    written = 0
    if rows:
        try:
            jobs_ws.append_rows([r for _, r in rows], value_input_option="RAW")
            for job_id, _ in rows:
                synced_ids.add(job_id)
            written = len(rows)
        except Exception as e:
            msg = f"Batch write failed: {e}"
            logger.error(msg)
            errors.append(msg)

    save_synced_ids(synced_ids)
    write_last_sync("ok", written)
    logger.info(f"=== Sync complete: {written} jobs written ===")
    result = {"status": "ok", "synced": written}
    if errors:
        result["errors"] = errors
    return result
