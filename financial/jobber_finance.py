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
  invoices(
    filter: { invoiceStatus: [awaiting_payment, past_due, partial] }
    first: 50
    after: $cursor
  ) {
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
query Expenses($cursor: String, $startDate: ISO8601Date!) {
  expenses(
    filter: { enteredAt: { after: $startDate } }
    first: 50
    after: $cursor
  ) {
    nodes {
      id
      description
      title
      total
      enteredAt
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
    """Return all expenses entered in Jobber since start_date (date object)."""
    expenses = []
    cursor = None
    start_str = start_date.isoformat()

    while True:
        data = graphql_request(EXPENSES_QUERY, {"cursor": cursor, "startDate": start_str})
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
                "entered_at": (node.get("enteredAt") or "")[:10],
                "paid_by": node.get("paidBy", ""),
            })

        page_info = exp_data.get("pageInfo", {})
        if not page_info.get("hasNextPage"):
            break
        cursor = page_info.get("endCursor")

    logger.info(f"Expenses: fetched {len(expenses)} since {start_str}")
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
query TimeEntries($cursor: String, $startDate: ISO8601DateTime!) {
  timeSheetEntries(
    filter: { startAt: { after: $startDate } }
    first: 50
    after: $cursor
  ) {
    nodes {
      id
      startAt
      endAt
      duration
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
      hourlyRate
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


def fetch_user_rates():
    """Return dict { user_id: hourly_rate (float) }. Missing rates default to 0."""
    rates = {}
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
            rates[node["id"]] = {
                "name": ((node.get("name") or {}).get("full") or ""),
                "rate": float(node.get("hourlyRate") or 0),
            }

        page_info = u_data.get("pageInfo", {})
        if not page_info.get("hasNextPage"):
            break
        cursor = page_info.get("endCursor")

    return rates


def fetch_time_entries_since(start_dt):
    """Return all time entries since start_dt (datetime). duration is in seconds."""
    entries = []
    cursor = None
    start_str = start_dt.isoformat()

    while True:
        data = graphql_request(TIME_ENTRIES_QUERY, {"cursor": cursor, "startDate": start_str})
        if not data:
            break
        if data.get("errors"):
            logger.error(f"Time entries errors: {json.dumps(data['errors'])[:500]}")
            break

        t_data = (data.get("data") or {}).get("timeSheetEntries")
        if not t_data:
            break

        for node in t_data.get("nodes", []):
            user = node.get("user") or {}
            entries.append({
                "id": node.get("id"),
                "user_id": user.get("id"),
                "user_name": ((user.get("name") or {}).get("full") or ""),
                "start_at": node.get("startAt"),
                "end_at": node.get("endAt"),
                "duration_seconds": float(node.get("duration") or 0),
            })

        page_info = t_data.get("pageInfo", {})
        if not page_info.get("hasNextPage"):
            break
        cursor = page_info.get("endCursor")

    logger.info(f"Time entries: fetched {len(entries)} since {start_str}")
    return entries
