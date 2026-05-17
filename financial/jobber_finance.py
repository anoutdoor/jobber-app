"""Jobber GraphQL queries for the cashflow digest.

Reuses graphql_request() and refresh_access_token() from the parent jobber_sync
module, so token storage and auto-refresh on 401 are shared.

Queries here pull data NOT covered by the job-costing sync:
  - AR: open invoices with outstanding balances
  - Expenses: for vendor account balance auto-parsing (v2, when auto=true)
  - Time entries + user wages: for payroll accrual
"""
import json
import logging
from datetime import datetime, date, timedelta

# Reuse the existing Jobber GraphQL client
from jobber_sync import graphql_request

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# AR — open invoices with outstanding balances
# ---------------------------------------------------------------------------

INVOICES_QUERY = """
query OpenInvoices($cursor: String) {
  invoices(first: 50, after: $cursor) {
    nodes {
      id
      invoiceNumber
      subject
      issuedDate
      dueDate
      invoiceStatus
      amounts {
        total
        subtotal
        depositAmount
        discountAmount
        paymentsTotal
      }
      client {
        id
        name
        emails { address primary }
        phones { number primary }
      }
    }
    pageInfo {
      hasNextPage
      endCursor
    }
  }
}
"""

# Statuses that should be excluded from outstanding AR
_AR_EXCLUDE_STATUSES = {"paid", "draft", "bad_debt", "archived"}


def fetch_open_invoices():
    """Return a list of open invoice dicts with computed outstanding balance.

    Returns None if the underlying GraphQL call fails before returning any
    data (auth failure, schema error, network error). The caller can use
    None to detect "data source unavailable" vs an empty result set.

    Each invoice dict has keys:
        id, number, subject, client_name, client_id, client_email, client_phone,
        issued_date, due_date, status, total, paid, outstanding, days_past_due
    """
    today = date.today()
    invoices = []
    cursor = None
    got_first_response = False

    while True:
        data = graphql_request(INVOICES_QUERY, {"cursor": cursor})
        if not data:
            logger.error("AR: graphql_request returned None")
            if not got_first_response:
                return None
            break

        if data.get("errors"):
            logger.error(f"AR query errors: {json.dumps(data['errors'])[:500]}")
            if not got_first_response:
                return None
            break

        inv_data = (data.get("data") or {}).get("invoices")
        if not inv_data:
            logger.error(f"AR: unexpected response shape: {json.dumps(data)[:500]}")
            if not got_first_response:
                return None
            break

        got_first_response = True

        for node in inv_data.get("nodes", []):
            status = (node.get("invoiceStatus") or "").lower()
            if status in _AR_EXCLUDE_STATUSES:
                continue
            amounts = node.get("amounts") or {}
            total = float(amounts.get("total") or 0)
            payments = float(amounts.get("paymentsTotal") or 0)
            # Jobber tracks the deposit collected separately from regular payments,
            # so paymentsTotal does NOT include it. Without subtracting depositAmount
            # too, AR is overstated by the deposit amount for any invoice where a
            # deposit was collected.
            deposit = float(amounts.get("depositAmount") or 0)
            paid = payments + deposit
            outstanding = round(total - paid, 2)
            if outstanding <= 0:
                continue

            due_date_str = node.get("dueDate") or ""
            try:
                due_date = datetime.strptime(due_date_str[:10], "%Y-%m-%d").date()
                days_past_due = (today - due_date).days
            except (ValueError, TypeError):
                due_date = None
                days_past_due = None

            client = node.get("client") or {}
            emails = client.get("emails") or []
            phones = client.get("phones") or []
            primary_email = next((e["address"] for e in emails if e.get("primary")), emails[0]["address"] if emails else "")
            primary_phone = next((p["number"] for p in phones if p.get("primary")), phones[0]["number"] if phones else "")

            invoices.append({
                "id": node.get("id"),
                "number": node.get("invoiceNumber"),
                "subject": node.get("subject", ""),
                "client_id": client.get("id"),
                "client_name": client.get("name", ""),
                "client_email": primary_email,
                "client_phone": primary_phone,
                "issued_date": (node.get("issuedDate") or "")[:10],
                "due_date": due_date_str[:10] if due_date_str else "",
                "status": node.get("invoiceStatus"),
                "total": round(total, 2),
                "payments": round(payments, 2),
                "deposit": round(deposit, 2),
                "paid": round(paid, 2),
                "outstanding": outstanding,
                "days_past_due": days_past_due,
            })

        page_info = inv_data.get("pageInfo", {})
        if not page_info.get("hasNextPage"):
            break
        cursor = page_info.get("endCursor")

    logger.info(f"AR: fetched {len(invoices)} open invoices")
    return invoices


# ---------------------------------------------------------------------------
# Expenses — for vendor balance auto-parsing (used when vendor.auto = true)
# ---------------------------------------------------------------------------

EXPENSES_QUERY = """
query Expenses($cursor: String, $from: ISO8601DateTime!) {
  expenses(
    filter: { date: { after: $from } }
    first: 50
    after: $cursor
  ) {
    nodes {
      id
      description
      title
      total
      date
      createdAt
      paidBy
    }
    pageInfo {
      hasNextPage
      endCursor
    }
  }
}
"""


def fetch_expenses_since(start_date):
    """Return all expenses with `date` on or after start_date (date object).

    Uses Jobber's server-side ExpenseFilterAttributes.date range filter.

    Jobber's `after` is strictly greater-than. Expenses with a date-only
    value are stored as midnight UTC of that day, so `after: 2026-05-17T00:00:00Z`
    would EXCLUDE same-day expenses. To include same-day expenses dated on
    start_date itself, pass `start_date - 1 day` as the API filter and let
    the caller (compute_vendor_balances) filter inclusively on the day.
    """
    from datetime import timedelta as _td
    expenses = []
    cursor = None
    api_from = start_date - _td(days=1)
    start_dt_iso = f"{api_from.isoformat()}T00:00:00Z"

    while True:
        data = graphql_request(EXPENSES_QUERY, {"cursor": cursor, "from": start_dt_iso})
        if not data:
            logger.error("Expenses: graphql_request returned None")
            break
        if data.get("errors"):
            logger.error(f"Expenses query errors: {json.dumps(data['errors'])[:500]}")
            break

        exp_data = (data.get("data") or {}).get("expenses")
        if not exp_data:
            logger.error(f"Expenses: unexpected shape: {json.dumps(data)[:500]}")
            break

        for node in exp_data.get("nodes", []):
            expenses.append({
                "id": node.get("id"),
                "description": (node.get("description") or "").strip(),
                "title": (node.get("title") or "").strip(),
                "total": float(node.get("total") or 0),
                "date": (node.get("date") or "")[:10],
                "created_at": (node.get("createdAt") or "")[:10],
                "paid_by": node.get("paidBy", ""),
            })

        page_info = exp_data.get("pageInfo", {})
        if not page_info.get("hasNextPage"):
            break
        cursor = page_info.get("endCursor")

    logger.info(f"Expenses: fetched {len(expenses)} since {start_dt_iso}")
    return expenses


def expenses_for_vendor(expenses, parse_patterns):
    """Filter expenses whose description or title contains any of the (lowercased) substrings."""
    patterns = [p.lower() for p in parse_patterns]
    matches = []
    for e in expenses:
        haystack = (e["description"] + " " + e["title"]).lower()
        if any(p in haystack for p in patterns):
            matches.append(e)
    return matches


# ---------------------------------------------------------------------------
# Time entries + user wages — for payroll accrual
# ---------------------------------------------------------------------------
#
# Pay week: Monday through Sunday. Paid the following Thursday.
# At any point in the week, the accrual = sum of (user_hours * user_rate)
# for hours worked since the most recent completed pay-week boundary.
#
# "Most recent completed pay-week boundary" = the Sunday of the prior pay
# period (last Sunday, inclusive). Hours worked Monday onward = unpaid.

TIME_ENTRIES_QUERY = """
query TimeEntries($cursor: String, $from: ISO8601DateTime!) {
  timeSheetEntries(
    filter: { startAt: { after: $from } }
    first: 25
    after: $cursor
  ) {
    nodes {
      id
      startAt
      endAt
      finalDuration
      labourRate
      user {
        id
        name { full }
      }
    }
    pageInfo {
      hasNextPage
      endCursor
    }
  }
}
"""

# Hard cap on pagination depth so a misbehaving cursor can't spin forever.
# At 25 entries/page × 40 pages = 1000 entries, plenty for a week of work.
_MAX_TIME_ENTRY_PAGES = 40

USERS_QUERY = """
query AllUsers($cursor: String) {
  users(first: 50, after: $cursor) {
    nodes {
      id
      name { full }
    }
    pageInfo {
      hasNextPage
      endCursor
    }
  }
}
"""


def current_pay_week_start(today=None):
    """The Monday that begins the currently-in-progress pay week.

    If today is Mon-Sun, return this week's Monday. After Sunday at midnight
    we roll to a new week.
    """
    today = today or date.today()
    # weekday: Mon=0 ... Sun=6
    return today - timedelta(days=today.weekday())


def fetch_users():
    """Return dict { user_id: {name} }. Used to backfill names if needed."""
    out = {}
    cursor = None
    while True:
        data = graphql_request(USERS_QUERY, {"cursor": cursor})
        if not data:
            logger.error("Users: graphql_request returned None")
            break
        if data.get("errors"):
            logger.error(f"Users query errors: {json.dumps(data['errors'])[:500]}")
            break

        u_data = (data.get("data") or {}).get("users")
        if not u_data:
            break

        for node in u_data.get("nodes", []):
            out[node["id"]] = {
                "name": ((node.get("name") or {}).get("full") or ""),
            }

        page_info = u_data.get("pageInfo", {})
        if not page_info.get("hasNextPage"):
            break
        cursor = page_info.get("endCursor")

    return out


def fetch_time_entries_since(start_dt):
    """Return time entries with startAt on or after start_dt (datetime).

    Returns None on auth failure / API error (vs [] for no rows in window).
    Uses Jobber's server-side TimeSheetEntriesFilterAttributes.startAt
    range filter. Each entry carries its own labourRate. Capped at
    _MAX_TIME_ENTRY_PAGES to prevent runaway pagination.
    """
    entries = []
    cursor = None
    got_first_response = False
    page_num = 0
    # Filter expects ISO8601DateTime
    from_iso = start_dt.strftime("%Y-%m-%dT%H:%M:%SZ")

    while True:
        page_num += 1
        if page_num > _MAX_TIME_ENTRY_PAGES:
            logger.error(
                f"Time entries: hit {_MAX_TIME_ENTRY_PAGES}-page cap with "
                f"{len(entries)} entries gathered. Last cursor: {(cursor or '')[:50]}. "
                f"Data may be incomplete past this point."
            )
            break

        logger.info(
            f"Time entries: requesting page {page_num} "
            f"(cursor={(cursor or 'null')[:30]}, from={from_iso})"
        )
        data = graphql_request(TIME_ENTRIES_QUERY, {"cursor": cursor, "from": from_iso})
        if not data:
            logger.error(
                f"Time entries: page {page_num} graphql_request returned None. "
                f"Already collected {len(entries)} entries before this. "
                f"See preceding graphql_request log line for HTTP error detail."
            )
            if not got_first_response:
                return None
            break
        if data.get("errors"):
            logger.error(
                f"Time entries page {page_num} errors: "
                f"{json.dumps(data['errors'])[:500]}"
            )
            if not got_first_response:
                return None
            break

        t_data = (data.get("data") or {}).get("timeSheetEntries")
        if not t_data:
            logger.error(
                f"Time entries: page {page_num} t_data is missing/null. "
                f"Full response (first 1000 chars): {json.dumps(data)[:1000]}"
            )
            if not got_first_response:
                return None
            break

        got_first_response = True

        page_node_count = 0
        for node in t_data.get("nodes", []):
            user = node.get("user") or {}
            entries.append({
                "id": node.get("id"),
                "user_id": user.get("id"),
                "user_name": ((user.get("name") or {}).get("full") or ""),
                "start_at": node.get("startAt"),
                "end_at": node.get("endAt"),
                "duration_seconds": float(node.get("finalDuration") or 0),
                "labour_rate": float(node.get("labourRate") or 0),
            })
            page_node_count += 1

        page_info = t_data.get("pageInfo", {})
        logger.info(
            f"Time entries page {page_num}: got {page_node_count} entries "
            f"(running total {len(entries)}), "
            f"hasNextPage={page_info.get('hasNextPage')}, "
            f"endCursor={(page_info.get('endCursor') or '')[:30]}"
        )
        if not page_info.get("hasNextPage"):
            break
        cursor = page_info.get("endCursor")

    logger.info(f"Time entries: fetched {len(entries)} total since {from_iso} ({page_num} page(s))")
    return entries


# ---------------------------------------------------------------------------
# Debug helpers — raw query dumps for troubleshooting
# ---------------------------------------------------------------------------

def debug_run_query(query, variables=None):
    """Run a query once (no pagination) and return the raw GraphQL response.
    Useful for figuring out which field names / filter shapes Jobber accepts.
    """
    data = graphql_request(query, variables or {})
    return data


_DEBUG_INVOICES_RAW = """
query DebugInvoices {
  invoices(first: 5) {
    nodes { id invoiceNumber invoiceStatus amounts { total paymentsTotal } dueDate }
  }
}
"""

_DEBUG_USERS_RAW = """
query DebugUsers {
  users(first: 5) {
    nodes { id name { full } }
  }
}
"""

_DEBUG_TIMESHEETS_RAW = """
query DebugTimesheets {
  timeSheetEntries(first: 5) {
    nodes { id startAt endAt finalDuration labourRate ticking user { id name { full } } }
  }
}
"""

_DEBUG_EXPENSES_RAW = """
query DebugExpenses {
  expenses(first: 5) {
    nodes { id title description total date createdAt }
  }
}
"""

# Mirrors fetch_expenses_since query shape so we can see if a recent test
# expense is returned by the actual filter we use in production.
_DEBUG_EXPENSES_FILTERED = """
query DebugExpensesFiltered($from: ISO8601DateTime!) {
  expenses(filter: { date: { after: $from } }, first: 20) {
    nodes { id title description total date createdAt }
  }
}
"""

# Mirrors the production TIME_ENTRIES_QUERY shape exactly (filter + pagination)
# so we can see whether the filter itself is the breaking change vs the
# unfiltered debug query.
_DEBUG_TIMESHEETS_FILTERED = """
query DebugTSFiltered($from: ISO8601DateTime!) {
  timeSheetEntries(filter: { startAt: { after: $from } }, first: 5) {
    nodes {
      id startAt endAt finalDuration labourRate
      user { id name { full } }
    }
    pageInfo { hasNextPage endCursor }
  }
}
"""

# Schema introspection — ask Jobber for the actual fields on each type, plus
# the argument shapes for the top-level queries AND the input fields of the
# filter types. Stops the guessing game for filter syntax.
_INTROSPECT_QUERY = """
query Introspect {
  Expense: __type(name: "Expense") {
    fields { name type { name kind ofType { name kind } } }
  }
  TimeSheetEntry: __type(name: "TimeSheetEntry") {
    fields { name type { name kind ofType { name kind } } }
  }
  User: __type(name: "User") {
    fields { name type { name kind ofType { name kind } } }
  }
  Invoice: __type(name: "Invoice") {
    fields { name type { name kind ofType { name kind } } }
  }
  InvoiceAmounts: __type(name: "InvoiceAmounts") {
    fields { name type { name kind ofType { name kind } } }
  }
  Query: __type(name: "Query") {
    fields {
      name
      args { name type { name kind ofType { name kind } } }
    }
  }
  TimeSheetFilter: __type(name: "TimeSheetEntriesFilterAttributes") {
    inputFields { name type { name kind ofType { name kind ofType { name kind } } } }
  }
  ExpenseFilter: __type(name: "ExpenseFilterAttributes") {
    inputFields { name type { name kind ofType { name kind ofType { name kind } } } }
  }
  InvoiceFilter: __type(name: "InvoiceFilterAttributes") {
    inputFields { name type { name kind ofType { name kind ofType { name kind } } } }
  }
}
"""


def debug_all():
    """Run every Jobber query the financial module needs and return a dict
    that's safe to dump as JSON. Lets us see which queries work and which
    error out with field-name issues.
    """
    # Use the start of the current pay week for the filtered timesheets probe
    from datetime import timedelta as _td
    today = date.today()
    week_start = today - _td(days=today.weekday())
    from_iso = week_start.strftime("%Y-%m-%dT00:00:00Z")

    # Mirror production: padded 1 day back so same-day expenses are included
    expenses_from = (today - _td(days=1)).strftime("%Y-%m-%dT00:00:00Z")

    return {
        "invoices_query": debug_run_query(_DEBUG_INVOICES_RAW),
        "users_query": debug_run_query(_DEBUG_USERS_RAW),
        "timesheets_query": debug_run_query(_DEBUG_TIMESHEETS_RAW),
        "timesheets_filtered_query": {
            "_filter_from": from_iso,
            "response": debug_run_query(_DEBUG_TIMESHEETS_FILTERED, {"from": from_iso}),
        },
        "expenses_query": debug_run_query(_DEBUG_EXPENSES_RAW),
        "expenses_filtered_query": {
            "_filter_from": expenses_from,
            "response": debug_run_query(_DEBUG_EXPENSES_FILTERED, {"from": expenses_from}),
        },
        "introspect": debug_run_query(_INTROSPECT_QUERY),
    }


def debug_timesheet_filters():
    """Try several plausible filter shapes for timeSheetEntries and report
    which ones work. The one that returns rows (or at least no errors) is
    the right syntax for Iso8601DateTimeRangeInput.
    """
    from datetime import datetime as _dt, timedelta as _td
    # 7 days ago, ISO8601 with Z
    cutoff = (_dt.utcnow() - _td(days=7)).strftime("%Y-%m-%dT%H:%M:%SZ")

    shapes = {
        "from": '{ from: "%s" }' % cutoff,
        "after": '{ after: "%s" }' % cutoff,
        "gte": '{ gte: "%s" }' % cutoff,
        "gt": '{ gt: "%s" }' % cutoff,
        "start": '{ start: "%s" }' % cutoff,
        "since": '{ since: "%s" }' % cutoff,
    }

    results = {}
    for label, inner in shapes.items():
        q = """
        query DebugTSFilter {
          timeSheetEntries(filter: { startAt: %s }, first: 3) {
            nodes { id startAt user { name { full } } }
          }
        }
        """ % inner
        resp = debug_run_query(q)
        if not resp:
            results[label] = {"status": "no response"}
            continue
        if resp.get("errors"):
            results[label] = {
                "status": "error",
                "message": (resp["errors"][0].get("message") or "")[:200],
            }
        else:
            nodes = (((resp.get("data") or {}).get("timeSheetEntries") or {}).get("nodes")) or []
            results[label] = {"status": "ok", "row_count": len(nodes),
                              "first_startAt": nodes[0]["startAt"] if nodes else None}
    return {"cutoff_used": cutoff, "tried": results}


def debug_field_names():
    """Compact view of Introspect: field names per type, query args, and
    INPUT fields for the filter types so we can see how to filter."""
    data = debug_run_query(_INTROSPECT_QUERY)
    if not data or not data.get("data"):
        return {"error": "introspection failed", "raw": data}

    d = data["data"]
    out = {}
    for type_name in ("Expense", "TimeSheetEntry", "User"):
        t = d.get(type_name) or {}
        out[type_name + "_fields"] = sorted(f["name"] for f in (t.get("fields") or []))

    query_type = d.get("Query") or {}
    targets = {"expenses", "timeSheetEntries", "users", "invoices"}
    query_args = {}
    for f in (query_type.get("fields") or []):
        if f["name"] in targets:
            query_args[f["name"]] = [
                {"name": a["name"], "type": (a["type"] or {}).get("name") or (a["type"] or {}).get("kind")}
                for a in (f.get("args") or [])
            ]
    out["Query_args"] = query_args

    # Filter input types — show the input fields we can pass under filter:{...}
    def shape_type(t):
        if not t:
            return None
        name = t.get("name")
        if name:
            return name
        of = t.get("ofType")
        return f"{t.get('kind')}<{shape_type(of)}>" if of else t.get("kind")

    for label in ("TimeSheetFilter", "ExpenseFilter", "InvoiceFilter"):
        t = d.get(label) or {}
        out[label + "_inputs"] = [
            {"name": f["name"], "type": shape_type(f.get("type"))}
            for f in (t.get("inputFields") or [])
        ]

    return out
