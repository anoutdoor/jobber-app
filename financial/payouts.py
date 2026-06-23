"""Upcoming Jobber payouts: money collected via Jobber Payments (card/ACH) that
hasn't been paid out to the bank yet. A payment is "upcoming" when it has a
payoutEstimatedDate but no payout yet. Grouped by estimated payout date; the net
(amount minus fee) matches Jobber's "Upcoming payouts" widget.

Note: finalized payoutRecords only ever carry PAID status (Jobber doesn't create
a payout record until it actually deposits), so upcoming amounts have to come
from the unpaid payments, not from payoutRecords.
"""
import logging
from datetime import date, timedelta

from jobber_sync import graphql_request

logger = logging.getLogger(__name__)

_QUERY = """
query Upcoming($after: ISO8601DateTime!, $cursor: String) {
  paymentRecords(filter: { entryDate: { after: $after } }, first: 100, after: $cursor) {
    nodes {
      __typename
      ... on JobberPaymentsCreditCardPaymentRecord { amount feeAmount payoutEstimatedDate payout { id } }
      ... on JobberPaymentsACHPaymentRecord { amount feeAmount payoutEstimatedDate payout { id } }
    }
    pageInfo { hasNextPage endCursor }
  }
}
"""

_LOOKBACK_DAYS = 90
_MAX_PAGES = 15


def _f(v):
    try:
        return float(v)
    except (TypeError, ValueError):
        return 0.0


def fetch_upcoming_payouts(today=None):
    """Upcoming payouts grouped by estimated payout date. Returns a dict with
    total_net, the per-date breakdown, and counts, or None on auth failure."""
    today = today or date.today()
    after = (today - timedelta(days=_LOOKBACK_DAYS)).isoformat() + "T00:00:00Z"

    by_date = {}
    payment_count = 0
    got_data = False
    first_call = True
    cursor = None
    pages = 0

    while True:
        resp = graphql_request(_QUERY, {"after": after, "cursor": cursor}) or {}
        if first_call and not resp.get("data"):
            return None
        first_call = False
        if resp.get("data"):
            got_data = True
        conn = (resp.get("data") or {}).get("paymentRecords") or {}
        for n in conn.get("nodes") or []:
            ped = n.get("payoutEstimatedDate")
            if ped and not n.get("payout"):
                by_date[ped] = by_date.get(ped, 0.0) + (_f(n.get("amount")) - _f(n.get("feeAmount")))
                payment_count += 1
        pi = conn.get("pageInfo") or {}
        cursor = pi.get("endCursor")
        pages += 1
        if not pi.get("hasNextPage") or pages >= _MAX_PAGES:
            break

    if not got_data:
        return None

    payouts = [
        {"arrivalDate": d, "netAmount": round(amt, 2), "status": "Upcoming"}
        for d, amt in sorted(by_date.items())
    ]
    return {
        "total_net": round(sum(p["netAmount"] for p in payouts), 2),
        "count": len(payouts),
        "payment_count": payment_count,
        "payouts": payouts,
    }
