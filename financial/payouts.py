"""Upcoming Jobber payouts: money customers have paid through Jobber Payments
that is settling to the bank but hasn't landed yet (status PENDING or
IN_TRANSIT). Amounts come back in cents and are converted to dollars."""
import logging

from jobber_sync import graphql_request

logger = logging.getLogger(__name__)

_QUERY = """
query Payouts($status: PayoutStatus!, $cursor: String) {
  payoutRecords(filter: { status: $status }, first: 50, after: $cursor) {
    nodes {
      id
      identifier
      status
      grossAmount
      feeAmount
      netAmount
      currency
      arrivalDate
    }
    pageInfo { hasNextPage endCursor }
    totalCount
  }
}
"""

_UPCOMING = ["PENDING", "IN_TRANSIT"]
_MAX_PAGES = 10


def _cents(v):
    try:
        return round(int(v) / 100.0, 2)
    except (TypeError, ValueError):
        return 0.0


def fetch_upcoming_payouts():
    """PENDING + IN_TRANSIT payouts. Returns a dict with total_net, count, and a
    per-payout breakdown sorted by arrival date. Returns None if the very first
    call comes back with no data (auth failure)."""
    payouts = []
    errors = []
    first_call = True
    got_any_data = False

    for status in _UPCOMING:
        cursor = None
        pages = 0
        while True:
            resp = graphql_request(_QUERY, {"status": status, "cursor": cursor}) or {}
            if first_call and not resp.get("data"):
                return None
            first_call = False
            if resp.get("data"):
                got_any_data = True
            if resp.get("errors"):
                errs = resp["errors"]
                errors.extend(errs if isinstance(errs, list) else [errs])
            conn = (resp.get("data") or {}).get("payoutRecords") or {}
            for n in conn.get("nodes") or []:
                payouts.append({
                    "id": n.get("id"),
                    "identifier": n.get("identifier"),
                    "status": n.get("status"),
                    "grossAmount": _cents(n.get("grossAmount")),
                    "feeAmount": _cents(n.get("feeAmount")),
                    "netAmount": _cents(n.get("netAmount")),
                    "currency": n.get("currency") or "USD",
                    "arrivalDate": n.get("arrivalDate"),
                })
            pi = conn.get("pageInfo") or {}
            cursor = pi.get("endCursor")
            pages += 1
            if not pi.get("hasNextPage") or pages >= _MAX_PAGES:
                break

    if not got_any_data:
        return None

    payouts.sort(key=lambda p: p.get("arrivalDate") or "")
    return {
        "total_net": round(sum(p["netAmount"] for p in payouts), 2),
        "total_gross": round(sum(p["grossAmount"] for p in payouts), 2),
        "count": len(payouts),
        "payouts": payouts,
        "errors": [str(e)[:200] for e in errors][:5],
    }
