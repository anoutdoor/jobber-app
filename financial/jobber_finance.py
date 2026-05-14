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

    Each invoice dict has keys:
        id, number, subject, client_name, client_id, client_email, client_phone,
        issued_date, due_date, status, total, paid, outstanding, days_past_due
    """
    today = date.today()
    invoices = []
    cursor = None

    while True:
        data = graphql_request(INVOICES_QUERY, {"cursor": cursor})
        if not data:
            logger.error("AR: graphql_request returned None")
            break

        if data.get("errors"):
            logger.error(f"AR query errors: {json.dumps(data['errors'])[:500]}")
            break

        inv_data = (data.get("data") or {}).get("invoices")
        if not inv_data:
            logger.error(f"AR: unexpected response shape: {json.dumps(data)[:500]}")
            break

        for node in inv_data.get("nodes", []):
            status = (node.get("invoiceStatus") or "").lower()
            if status in _AR_EXCLUDE_STATUSES:
                continue
            amounts = node.get("amounts") or {}
            total = float(amounts.get("total") or 0)
            paid = float(amounts.get("paymentsTotal") or 0)
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
query Expenses($cursor: String) {
  expenses(first: 50, after: $cursor) {
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

    Filter is applied in Python since we don't yet know the exact filter shape
    on ExpenseFilterAttributes. Once the introspection confirms it, this can
    move server-side. For now we paginate all and skip older rows; if we hit
    a page that's entirely older than start_date we stop early.
    """
    expenses = []
    cursor = None
    start_iso = start_date.isoformat()
    consecutive_old_pages = 0

    while True:
        data = graphql_request(EXPENSES_QUERY, {"cursor": cursor})
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

        page_nodes = exp_data.get("nodes", [])
        page_kept = 0
        for node in page_nodes:
            exp_date = (node.get("date") or "")[:10]
            if exp_date and exp_date < start_iso:
                continue
            expenses.append({
                "id": node.get("id"),
                "description": (node.get("description") or "").strip(),
                "title": (node.get("title") or "").strip(),
                "total": float(node.get("total") or 0),
                "date": exp_date,
                "created_at": (node.get("createdAt") or "")[:10],
                "paid_by": node.get("paidBy", ""),
            })
            page_kept += 1

        # If a whole page returned no in-window rows AND results look chronological,
        # bail out after two consecutive empty pages (defensive against unknown sort order).
        if page_kept == 0 and page_nodes:
            consecutive_old_pages += 1
            if consecutive_old_pages >= 2:
                break
        else:
            consecutive_old_pages = 0

        page_info = exp_data.get("pageInfo", {})
        if not page_info.get("hasNextPage"):
            break
        cursor = page_info.get("endCursor")

    logger.info(f"Expenses: kept {len(expenses)} on/after {start_iso}")
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
query TimeEntries($cursor: String) {
  timeSheetEntries(first: 50, after: $cursor) {
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
    """Return time entries with startAt on or after start_dt.

    Each entry carries its own labourRate, so we use that directly for the
    payroll accrual rather than looking up a per-user rate.

    Filter is applied in Python for now (TimeSheetEntriesFilterAttributes
    shape still being introspected). Early-exits when we see two consecutive
    pages of all-older entries.
    """
    entries = []
    cursor = None
    start_iso = start_dt.isoformat()
    consecutive_old_pages = 0

    while True:
        data = graphql_request(TIME_ENTRIES_QUERY, {"cursor": cursor})
        if not data:
            break
        if data.get("errors"):
            logger.error(f"Time entries errors: {json.dumps(data['errors'])[:500]}")
            break

        t_data = (data.get("data") or {}).get("timeSheetEntries")
        if not t_data:
            break

        page_nodes = t_data.get("nodes", [])
        page_kept = 0
        for node in page_nodes:
            start_at = node.get("startAt") or ""
            if start_at and start_at < start_iso:
                continue
            user = node.get("user") or {}
            entries.append({
                "id": node.get("id"),
                "user_id": user.get("id"),
                "user_name": ((user.get("name") or {}).get("full") or ""),
                "start_at": start_at,
                "end_at": node.get("endAt"),
                "duration_seconds": float(node.get("finalDuration") or 0),
                "labour_rate": float(node.get("labourRate") or 0),
            })
            page_kept += 1

        if page_kept == 0 and page_nodes:
            consecutive_old_pages += 1
            if consecutive_old_pages >= 2:
                break
        else:
            consecutive_old_pages = 0

        page_info = t_data.get("pageInfo", {})
        if not page_info.get("hasNextPage"):
            break
        cursor = page_info.get("endCursor")

    logger.info(f"Time entries: kept {len(entries)} on/after {start_iso}")
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
    nodes { id startAt endAt duration user { id name { full } } }
  }
}
"""

_DEBUG_EXPENSES_RAW = """
query DebugExpenses {
  expenses(first: 5) {
    nodes { id title description total }
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
    return {
        "invoices_query": debug_run_query(_DEBUG_INVOICES_RAW),
        "users_query": debug_run_query(_DEBUG_USERS_RAW),
        "timesheets_query": debug_run_query(_DEBUG_TIMESHEETS_RAW),
        "expenses_query": debug_run_query(_DEBUG_EXPENSES_RAW),
        "introspect": debug_run_query(_INTROSPECT_QUERY),
    }


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
