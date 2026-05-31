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

GOOGLE_SCOPES = [
    "https://www.googleapis.com/auth/spreadsheets",
    "https://www.googleapis.com/auth/gmail.send",
]
GOOGLE_CLIENT_SECRETS_FILE = os.getenv("GOOGLE_CLIENT_SECRETS_FILE", "client_secrets.json")
SHEET_ID = os.getenv("GOOGLE_SHEET_ID")

# ---------------------------------------------------------------------------
# Crew config — lead-based assignment, priority order matters (first match wins)
# A job's crew = whichever LEAD clocked into it (from timeSheetEntries names).
# Lead names MUST match Jobber's user name.full EXACTLY — verify against the
# Jobs sheet's Team Members column if jobs land in "Other".
# ---------------------------------------------------------------------------
CREW_LEADS = [
    {"name": "Ernesto Cardenas",         "crew_label": "Ernesto"},
    {"name": "Jovanni Garduno Martinez", "crew_label": "Jovani"},
    {"name": "Gonzalo Cardenas",         "crew_label": "Mow"},
    # Jorge is LAST on purpose: he flexes to drive for Jovani, so when both
    # clocked in, the job is Jovani's.
    {"name": "Jorge Armenta",            "crew_label": "Jorge"},
]

# Install crews shown on the job-costing dashboard (Mow gets its own later)
INSTALL_CREWS = {"Ernesto", "Jovani", "Jorge"}

# Fresh-start cutoff: ignore everything completed before this date. The sheet
# was wiped of older/duplicated rows on 2026-05-31; this keeps old jobs from
# syncing back in. Format: YYYY-MM-DD.
SYNC_START_DATE = "2026-05-25"

# ---------------------------------------------------------------------------
# GraphQL query — labor cost pulled directly from Jobber
# ---------------------------------------------------------------------------
JOBS_QUERY = """
query GetClosedJobs($cursor: String) {
  jobs(filter: { status: __STATUS__ }, first: 10, after: $cursor) {
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
      customFields {
        ... on CustomFieldNumeric { label valueNumeric }
        ... on CustomFieldText { label valueText }
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

# In-memory token cache. Without this, every graphql_request triggers a fresh
# Sheets read for tokens, which burns through Google Sheets' 60-reads-per-minute
# per-user quota in ~11 pages of pagination and blows up the whole digest.
_token_cache = None


def _load_tokens_uncached():
    """Cold load: file > Sheets > env."""
    if os.path.exists(TOKEN_STORE_FILE):
        with open(TOKEN_STORE_FILE) as f:
            return json.load(f)
    try:
        from financial.token_persistence import read_tokens as _read_sheets
        sheets_tokens = _read_sheets("jobber")
        if sheets_tokens.get("access_token"):
            return sheets_tokens
    except Exception as e:
        logger.warning(f"Jobber: Sheets token read failed ({e}); falling back to env.")

    env_blob = os.getenv("JOBBER_TOKEN_STORE")
    if env_blob:
        try:
            parsed = json.loads(env_blob)
            if isinstance(parsed, dict) and parsed.get("access_token"):
                return parsed
            logger.error("JOBBER_TOKEN_STORE is not a JSON object with access_token.")
        except json.JSONDecodeError:
            logger.error("JOBBER_TOKEN_STORE env var is not valid JSON.")
    return {}


def load_tokens():
    global _token_cache
    if _token_cache is not None:
        return _token_cache
    _token_cache = _load_tokens_uncached()
    return _token_cache


def save_tokens(access_token, refresh_token):
    global _token_cache
    payload = {"access_token": access_token, "refresh_token": refresh_token}
    # Update cache FIRST so any concurrent reader sees the fresh tokens.
    _token_cache = payload
    # Write to local file (synchronous, fast).
    with open(TOKEN_STORE_FILE, "w") as f:
        json.dump(payload, f)
    # Persist to Sheets so it survives the next Railway redeploy.
    try:
        from financial.token_persistence import write_tokens as _write_sheets
        _write_sheets("jobber", payload)
    except Exception as e:
        logger.error(f"Jobber: Sheets token write failed ({e}); only local file updated.")


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

    if not access_token:
        logger.error("graphql_request: no access_token available before request.")
        return None

    try:
        resp = requests.post(
            GRAPHQL_URL,
            json={"query": query, "variables": variables or {}},
            headers={
                "Authorization": f"Bearer {access_token}",
                "Content-Type": "application/json",
                "X-JOBBER-GRAPHQL-VERSION": GRAPHQL_VERSION,
            },
            timeout=30,
        )
    except requests.exceptions.RequestException as e:
        # Network error, timeout, etc. — surface so we don't silently return None.
        logger.error(f"graphql_request: network error: {e}")
        return None

    if resp.status_code == 401 and _retry:
        logger.info("Token expired — attempting refresh.")
        new_token = refresh_access_token()
        if new_token:
            return graphql_request(query, variables, new_token, _retry=False)
        logger.error("graphql_request: refresh returned no token after 401.")
        return None

    if not resp.ok:
        # Log the FULL response body so we can see exactly what Jobber returned
        # on HTTP errors. Previously this was 'resp.text' but Python truncates
        # very long messages in logs — explicit slicing makes the limit obvious.
        logger.error(
            f"graphql_request: HTTP {resp.status_code} from Jobber. "
            f"Variables: {json.dumps(variables or {})[:300]}. "
            f"Response body (first 2000 chars): {resp.text[:2000]}"
        )
        return None

    body = resp.json()
    # Jobber returns HTTP 200 with errors-only body for several conditions.
    # Detect throttling specifically and retry once after a brief sleep — most
    # bursts clear within a couple seconds (restore rate 500 cost/sec).
    errors = body.get("errors") or []
    if errors and not body.get("data"):
        is_throttled = any(
            (e.get("extensions") or {}).get("code") == "THROTTLED"
            for e in errors
        )
        if is_throttled and _retry:
            logger.warning(
                f"graphql_request: Jobber THROTTLED. "
                f"Sleeping 3s then retrying once. Variables: {json.dumps(variables or {})[:200]}"
            )
            import time as _time
            _time.sleep(3)
            return graphql_request(query, variables, access_token, _retry=False)
        logger.error(
            f"graphql_request: 200 but errors-only body. "
            f"Errors: {json.dumps(errors)[:500]}"
        )
    return body


# Statuses that represent a finished job. requires_invoicing catches jobs that
# are done but not yet invoiced; archived catches jobs that have been invoiced
# and closed out. Without archived, a job leaves requires_invoicing the moment
# it's invoiced, so the sync could never pull a post-invoice edit to its total.
CLOSED_JOB_STATUSES = ["requires_invoicing", "archived"]


def fetch_all_closed_jobs():
    by_id = {}

    for status in CLOSED_JOB_STATUSES:
        query = JOBS_QUERY.replace("__STATUS__", status)
        cursor = None
        while True:
            data = graphql_request(query, {"cursor": cursor})
            if not data:
                break

            jobs_data = (data.get("data") or {}).get("jobs")
            if not jobs_data:
                logger.error(f"Unexpected GraphQL response shape ({status}): {json.dumps(data)[:500]}")
                break

            nodes = jobs_data.get("nodes", [])
            for node in nodes:
                by_id[node.get("id")] = node
            logger.info(f"[{status}] page of {len(nodes)} jobs (unique so far: {len(by_id)})")

            page_info = jobs_data.get("pageInfo", {})
            if not page_info.get("hasNextPage"):
                break
            cursor = page_info.get("endCursor")

    return list(by_id.values())


# ---------------------------------------------------------------------------
# Costing logic
# ---------------------------------------------------------------------------

def resolve_crew(worked_names):
    # Jobber's name.full sometimes carries trailing whitespace (e.g.
    # "Gonzalo Cardenas "), so compare against a stripped set.
    stripped = {n.strip() for n in worked_names}
    for lead in CREW_LEADS:
        if lead["name"] in stripped:
            return lead["crew_label"]
    return "Other"


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

    crew_label = resolve_crew(worked_names)
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

    # Estimated hours from custom fields (it's a union type, not a connection)
    estimated_hours = ""
    for cf in (job.get("customFields") or []):
        if cf.get("label", "").lower().strip() == "estimated hours":
            val = cf.get("valueNumeric") or cf.get("valueText") or ""
            if val == 0.0:
                estimated_hours = ""
            elif isinstance(val, float) and val == int(val):
                estimated_hours = str(int(val))
            else:
                estimated_hours = str(val)
            break

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
        "estimated_hours": estimated_hours,
        "synced_at": datetime.utcnow().strftime("%Y-%m-%d %H:%M UTC"),
    }

    # Overhead is no longer allocated per job. Net is reported month-level only
    # (see dashboard.py MONTHLY_OVERHEAD). These columns stay blank to preserve
    # the sheet layout without implying any per-job net.
    base.update({
        "daily_overhead_rate": "",
        "total_overhead": "",
        "total_job_cost": "",
        "net_profit": "",
        "net_margin_pct": "",
        "net_margin_flag": "",
    })

    return base


# ---------------------------------------------------------------------------
# Google Sheets — OAuth user credentials (token saved to token.json)
# ---------------------------------------------------------------------------

def _load_token_from_env_or_file():
    """Load Google OAuth token from GOOGLE_TOKEN env var or token.json file."""
    env_token = os.getenv("GOOGLE_TOKEN")
    if env_token:
        return Credentials.from_authorized_user_info(json.loads(env_token), GOOGLE_SCOPES)
    if os.path.exists(GOOGLE_TOKEN_FILE):
        return Credentials.from_authorized_user_file(GOOGLE_TOKEN_FILE, GOOGLE_SCOPES)
    return None


def _save_token(creds):
    """Save refreshed token back to env-aware storage or local file."""
    token_json = creds.to_json()
    # In production (env var mode), log a reminder — Railway env vars must be updated manually
    if os.getenv("GOOGLE_TOKEN"):
        logger.info("Google token refreshed. Update GOOGLE_TOKEN env var in Railway if refresh token changed.")
    with open(GOOGLE_TOKEN_FILE, "w") as f:
        f.write(token_json)
    logger.info(f"Google credentials saved to {GOOGLE_TOKEN_FILE}.")


def get_google_credentials():
    creds = _load_token_from_env_or_file()

    if not creds or not creds.valid:
        if creds and creds.expired and creds.refresh_token:
            creds.refresh(Request())
            logger.info("Google credentials refreshed.")
            _save_token(creds)
        else:
            # Fresh OAuth flow — only works locally (opens a browser)
            env_secrets = os.getenv("GOOGLE_CLIENT_SECRETS")
            if env_secrets:
                import tempfile
                with tempfile.NamedTemporaryFile(mode="w", suffix=".json", delete=False) as tmp:
                    tmp.write(env_secrets)
                    tmp_path = tmp.name
                flow = InstalledAppFlow.from_client_secrets_file(tmp_path, GOOGLE_SCOPES)
                os.unlink(tmp_path)
            elif os.path.exists(GOOGLE_CLIENT_SECRETS_FILE):
                flow = InstalledAppFlow.from_client_secrets_file(
                    GOOGLE_CLIENT_SECRETS_FILE, GOOGLE_SCOPES
                )
            else:
                raise FileNotFoundError(
                    "No Google credentials found. Set GOOGLE_CLIENT_SECRETS env var "
                    "or place client_secrets.json in the project folder."
                )
            creds = flow.run_local_server(port=0)
            logger.info("Google authorization completed.")
            _save_token(creds)

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
    "Net Margin Flag", "Revenue / Visit Day ($)", "Estimated Hours", "Synced At",
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
        c["net_margin_flag"], c["rev_per_visit_day"], c["estimated_hours"],
        c["synced_at"],
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
    # Per-job overhead allocation was removed — net is reported month-level only
    # (dashboard.py MONTHLY_OVERHEAD). This is now a safe no-op so the existing
    # /reconcile-now route and the daily scheduler call don't error.
    logger.info("Overhead reconciliation is a no-op (per-job overhead removed; net is month-level).")
    return {"status": "ok", "reconciled": 0}


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

    jobs = [j for j in jobs if (j.get("completedAt") or "")[:10] >= SYNC_START_DATE]
    logger.info(f"After fresh-start cutoff ({SYNC_START_DATE}): {len(jobs)}")

    # Connect to the sheet first so we can diff against what's already there.
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

    # Map existing Job ID -> sheet row number so we can overwrite a job in place
    # when its Jobber numbers change, instead of only ever appending new jobs.
    # Row 1 is the header, so data rows are numbered from 2.
    try:
        existing_values = jobs_ws.get_all_values()
        id_to_row = {
            r[0]: idx
            for idx, r in enumerate(existing_values[1:], start=2)
            if r and r[0]
        }
    except Exception as e:
        msg = f"Failed to read existing rows: {e}"
        logger.error(msg)
        write_last_sync("error", 0, msg)
        return {"status": "error", "message": msg}

    last_col = _col_to_letter(len(JOBS_HEADERS))

    appends = []
    updates = []  # batch_update payload: {"range", "values"}
    errors = []
    for job in jobs:
        try:
            row = row_from_costed(cost_job(job))
        except Exception as e:
            msg = f"Job {job.get('jobNumber')}: {e}"
            logger.error(msg)
            errors.append(msg)
            continue

        existing_row = id_to_row.get(job.get("id"))
        if existing_row:
            updates.append({
                "range": f"A{existing_row}:{last_col}{existing_row}",
                "values": [row],
            })
        else:
            appends.append(row)

    written = 0
    if appends:
        try:
            jobs_ws.append_rows(appends, value_input_option="RAW")
            written = len(appends)
        except Exception as e:
            msg = f"Batch append failed: {e}"
            logger.error(msg)
            errors.append(msg)

    updated = 0
    if updates:
        try:
            jobs_ws.batch_update(updates, value_input_option="RAW")
            updated = len(updates)
        except Exception as e:
            msg = f"Batch update failed: {e}"
            logger.error(msg)
            errors.append(msg)

    # synced_jobs.json is no longer the dedup gate (the sheet is the source of
    # truth now), but keep it current so nothing downstream reads stale state.
    synced_ids = load_synced_ids()
    synced_ids.update(j.get("id") for j in jobs)
    save_synced_ids(synced_ids)

    write_last_sync("ok", written + updated)
    logger.info(f"=== Sync complete: {written} added, {updated} updated ===")
    result = {"status": "ok", "synced": written, "updated": updated}
    if errors:
        result["errors"] = errors
    return result
