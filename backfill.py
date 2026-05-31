import logging

from jobber_sync import (
    graphql_request, cost_job, row_from_costed,
    get_sheets_client, ensure_sheets, load_synced_ids, save_synced_ids,
    SHEET_ID,
)

logger = logging.getLogger(__name__)

BACKFILL_START_DATE = "2026-01-01"


# ---------------------------------------------------------------------------
# Query builder — same fields as the regular sync query
# ---------------------------------------------------------------------------

def _jobs_query(status):
    return f"""
query BackfillJobs($cursor: String) {{
  jobs(filter: {{ status: {status} }}, first: 10, after: $cursor) {{
    nodes {{
      id
      title
      jobNumber
      completedAt
      jobType
      client {{
        name
      }}
      property {{
        address {{
          street
          city
          province
          postalCode
        }}
      }}
      jobCosting {{
        labourCost
        labourDuration
        expenseCost
        totalRevenue
      }}
      visits(first: 10) {{
        nodes {{
          startAt
        }}
      }}
      customFields {{
        ... on CustomFieldNumeric {{ label valueNumeric }}
        ... on CustomFieldText {{ label valueText }}
      }}
      timeSheetEntries(first: 10) {{
        nodes {{
          user {{
            name {{
              full
            }}
          }}
        }}
      }}
    }}
    pageInfo {{
      hasNextPage
      endCursor
    }}
  }}
}}
"""


def _fetch_by_status(status):
    query = _jobs_query(status)
    jobs = []
    cursor = None

    while True:
        data = graphql_request(query, {"cursor": cursor})
        if not data:
            logger.error(f"No response fetching status={status}")
            break

        jobs_data = (data.get("data") or {}).get("jobs")
        if not jobs_data:
            logger.error(f"Unexpected response for status={status}: {str(data)[:300]}")
            break

        nodes = jobs_data.get("nodes", [])
        jobs.extend(nodes)
        logger.info(f"[{status}] page fetched: {len(nodes)} jobs (running total: {len(jobs)})")

        page_info = jobs_data.get("pageInfo", {})
        if not page_info.get("hasNextPage"):
            break
        cursor = page_info.get("endCursor")

    return jobs


# ---------------------------------------------------------------------------
# Read existing Job IDs from the sheet (source of truth for deduplication)
# ---------------------------------------------------------------------------

def _get_existing_ids(jobs_ws):
    try:
        all_values = jobs_ws.get_all_values()
        if len(all_values) < 2:
            return set()
        headers = all_values[0]
        if "Job ID" not in headers:
            return set()
        id_col = headers.index("Job ID")
        return {row[id_col] for row in all_values[1:] if len(row) > id_col and row[id_col]}
    except Exception as e:
        logger.error(f"Failed to read existing IDs: {e}")
        return set()


# ---------------------------------------------------------------------------
# Main backfill entry point
# ---------------------------------------------------------------------------

def run_backfill():
    logger.info("=== Backfill starting ===")

    if not SHEET_ID:
        return {"status": "error", "message": "GOOGLE_SHEET_ID not set in .env"}

    # 1. Fetch both statuses
    logger.info("Fetching requires_invoicing jobs...")
    jobs_ri = _fetch_by_status("requires_invoicing")

    logger.info("Fetching archived jobs...")
    jobs_ar = _fetch_by_status("archived")

    # Deduplicate across the two status queries
    all_raw = {j["id"]: j for j in jobs_ri + jobs_ar}
    logger.info(f"Total unique jobs fetched: {len(all_raw)}")

    # 2. Date filter + skip recurring
    filtered = []
    for job in all_raw.values():
        completed = (job.get("completedAt") or "")[:10]
        if not completed or completed < BACKFILL_START_DATE:
            continue
        if (job.get("jobType") or "").upper() == "RECURRING":
            continue
        filtered.append(job)

    logger.info(f"After date filter (>= {BACKFILL_START_DATE}) and recurring skip: {len(filtered)}")

    # 3. Connect to sheet
    try:
        gc = get_sheets_client()
        spreadsheet = gc.open_by_key(SHEET_ID)
        ensure_sheets(spreadsheet)
        jobs_ws = spreadsheet.worksheet("Jobs")
    except Exception as e:
        return {"status": "error", "message": f"Google Sheets error: {e}"}

    # 4. Deduplicate against sheet (not synced_jobs.json)
    existing_ids = _get_existing_ids(jobs_ws)
    logger.info(f"Job IDs already in sheet: {len(existing_ids)}")

    new_jobs = [j for j in filtered if j.get("id") not in existing_ids]
    logger.info(f"New jobs to import: {len(new_jobs)}")

    if not new_jobs:
        return {
            "status": "ok",
            "imported": 0,
            "message": "Sheet is already up to date — no new jobs to import.",
        }

    # 5. Sort chronologically so rows land in order
    new_jobs.sort(key=lambda j: j.get("completedAt") or "")

    # 6. Cost all jobs
    costed = []
    for job in new_jobs:
        try:
            costed.append(cost_job(job))
        except Exception as e:
            logger.error(f"Failed to cost job {job.get('jobNumber')}: {e}")

    # 7. Write all rows in a single batch request to avoid Sheets rate limits
    rows = [row_from_costed(c) for c in costed]
    errors = []

    try:
        jobs_ws.append_rows(rows, value_input_option="RAW")
        logger.info(f"Batch wrote {len(rows)} rows to sheet.")
    except Exception as e:
        logger.error(f"Batch write failed: {e}")
        return {"status": "error", "message": f"Batch write failed: {e}"}

    written = len(rows)
    synced_ids = load_synced_ids()
    for c in costed:
        synced_ids.add(c["job_id"])
    save_synced_ids(synced_ids)

    summary = {
        "status":                    "ok",
        "fetched_requires_invoicing": len(jobs_ri),
        "fetched_archived":           len(jobs_ar),
        "after_date_filter":          len(filtered),
        "already_in_sheet":           len(existing_ids),
        "new_jobs_found":             len(new_jobs),
        "imported":                   written,
    }
    if errors:
        summary["errors"] = errors

    logger.info(f"=== Backfill complete: {written} jobs imported ===")
    return summary
