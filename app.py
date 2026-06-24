import os
import secrets
import logging

import requests
from dotenv import load_dotenv

from flask import Flask, redirect, request, session, url_for, jsonify, render_template, send_from_directory
from werkzeug.middleware.proxy_fix import ProxyFix
from jobber_sync import save_tokens, run_sync, read_last_sync, reconcile_daily_overhead
from dashboard import compute_dashboard
from backfill import run_backfill
from scheduler import start_scheduler, stop_scheduler
from financial import qbo as financial_qbo
from financial import google_auth as financial_google_auth
from financial.digest import run_digest
from financial.pnl_reminder import send_pnl_reminder
from mow_time_export import main as run_mow_export
import marketing

load_dotenv()
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s")
logger = logging.getLogger(__name__)

app = Flask(__name__)
app.secret_key = os.getenv("FLASK_SECRET_KEY", secrets.token_hex(32))

# Railway terminates SSL at the proxy and forwards as HTTP, so Flask sees http://
# unless we trust the X-Forwarded-* headers. Without this, request.url_root returns
# http://... in production, which breaks OAuth redirect_uri matching.
app.wsgi_app = ProxyFix(app.wsgi_app, x_proto=1, x_host=1, x_for=1)

CLIENT_ID = os.getenv("JOBBER_CLIENT_ID")
CLIENT_SECRET = os.getenv("JOBBER_CLIENT_SECRET")
REDIRECT_URI = os.getenv("JOBBER_REDIRECT_URI", "http://127.0.0.1:8080/callback")

AUTHORIZE_URL = "https://api.getjobber.com/api/oauth/authorize"
TOKEN_URL = "https://api.getjobber.com/api/oauth/token"
GRAPHQL_URL = "https://api.getjobber.com/api/graphql"

CLIENTS_QUERY = """
query {
  clients(first: 50) {
    nodes {
      id
      name
      emails { address primary }
      phones { number primary }
      billingAddress { street city province postalCode country }
      createdAt
    }
    pageInfo { hasNextPage endCursor }
    totalCount
  }
}
"""


# ---------------------------------------------------------------------------
# Routes
# ---------------------------------------------------------------------------

@app.route("/")
def index():
    connected = "access_token" in session
    last = read_last_sync()

    if connected:
        sync_info = ""
        if last:
            flag = "error" in last.get("status", "")
            color = "red" if flag else "green"
            sync_info = (
                f"<p style='color:{color}'>Last sync: {last['timestamp']} — "
                f"{last.get('jobs_synced', 0)} jobs written"
                + (f" — {last.get('message','')}" if last.get("message") else "")
                + "</p>"
            )

        qbo_connected = bool(financial_qbo.load_tokens().get("access_token"))
        qbo_section = (
            "<p style='color:green'>QBO: connected</p>" if qbo_connected
            else "<p><a href='/qbo/login'><strong>Connect QuickBooks Online</strong></a></p>"
        )

        return (
            "<h2>Jobber Job Costing — Connected</h2>"
            + sync_info
            + qbo_section
            + "<p><a href='/google/login'>Re-authorize Google (Sheets + Gmail Send)</a></p>"
            + "<p><a href='/dashboard'><strong>→ Open Dashboard</strong></a></p>"
            + "<p><a href='/mow-dashboard'><strong>→ Open Mowing Numbers Dashboard</strong></a></p>"
            + "<p><a href='/financials'><strong>→ Open Financial Dashboard (P&amp;L)</strong></a></p>"
            + "<p><a href='/marketing'><strong>→ Open Marketing ROI Dashboard</strong></a></p>"
            + "<p><a href='/sync-now'>Run Sync Now</a></p>"
            + "<p><a href='/backfill'>Run Historical Backfill (Jan 1 2026 – Today)</a></p>"
            + "<p><a href='/reconcile-now'>Run Overhead Reconciliation Now</a></p>"
            + "<p><a href='/digest-now?dry=1'>Preview Cashflow Digest (no email)</a></p>"
            + "<p><a href='/pnl-reminder-now?dry=1'>Preview Monthly P&amp;L Reminder (no email)</a></p>"
            + "<p><a href='/digest-now'>Send Cashflow Digest Now</a></p>"
            + "<p><a href='/qbo/test'>Test QBO Balance Pull</a></p>"
            + "<p><a href='/logout'>Logout</a></p>"
        )

    return (
        "<h2>Jobber Job Costing</h2>"
        "<p><a href='/login'>Connect to Jobber</a></p>"
    )


@app.route("/login")
def login():
    state = secrets.token_urlsafe(16)
    session["oauth_state"] = state

    params = {
        "client_id": CLIENT_ID,
        "redirect_uri": REDIRECT_URI,
        "response_type": "code",
        "state": state,
    }
    auth_url = requests.Request("GET", AUTHORIZE_URL, params=params).prepare().url
    return redirect(auth_url)


@app.route("/callback")
def callback():
    error = request.args.get("error")
    if error:
        return jsonify({"error": error, "description": request.args.get("error_description")}), 400

    code = request.args.get("code")
    state = request.args.get("state")

    if not code:
        return jsonify({"error": "Missing authorization code"}), 400

    if state != session.get("oauth_state"):
        return jsonify({"error": "State mismatch — possible CSRF attack"}), 400

    session.pop("oauth_state", None)

    token_resp = requests.post(
        TOKEN_URL,
        data={
            "grant_type": "authorization_code",
            "code": code,
            "redirect_uri": REDIRECT_URI,
            "client_id": CLIENT_ID,
            "client_secret": CLIENT_SECRET,
        },
        headers={"Content-Type": "application/x-www-form-urlencoded"},
    )

    if not token_resp.ok:
        return jsonify({"error": "Token exchange failed", "details": token_resp.text}), 400

    token_data = token_resp.json()
    access_token = token_data.get("access_token")
    refresh_token = token_data.get("refresh_token")

    # Store in session for web routes
    session["access_token"] = access_token
    session["refresh_token"] = refresh_token

    # Persist to file so the background scheduler can use them
    save_tokens(access_token, refresh_token)

    return redirect(url_for("index"))



@app.route("/sync-now")
def sync_now():
    if "access_token" not in session:
        return redirect(url_for("login"))
    result = run_sync()
    return jsonify(result)


@app.route("/reconcile-now")
def reconcile_now():
    if "access_token" not in session:
        return redirect(url_for("login"))
    result = reconcile_daily_overhead()
    return jsonify(result)


@app.route("/mow-export-now")
def mow_export_now():
    if "access_token" not in session:
        return redirect(url_for("login"))
    result = run_mow_export()
    return jsonify(result)


@app.route("/digest-now")
def digest_now():
    if "access_token" not in session:
        return redirect(url_for("login"))
    dry = request.args.get("dry") == "1"
    result = run_digest(dry_run=dry)
    if dry and "html" in result:
        return result["html"]
    return jsonify({k: v for k, v in result.items() if k != "html"})


@app.route("/pnl-reminder-now")
def pnl_reminder_now():
    """Manually fire (or preview) the monthly P&L update reminder. Add ?dry=1 to
    render the email without sending. The real send runs automatically on the
    15th at 8am CT via the scheduler."""
    if "access_token" not in session:
        return redirect(url_for("login"))
    dry = request.args.get("dry") == "1"
    result = send_pnl_reminder(dry_run=dry)
    if dry and "html" in result:
        return result["html"]
    return jsonify(result)


# Origins allowed to call /payroll-owed cross-origin (the cash position tracker).
_PAYROLL_CORS_ORIGINS = {
    "https://cash-position-tracker-production.up.railway.app",
    "http://localhost:5173",
    "http://localhost:5174",
    "http://localhost:5175",
}


def _payroll_cors(resp):
    """Add CORS headers when the request comes from an allowed origin. The
    endpoint is key-gated and uses no cookies, so we only echo the origin back
    and permit the X-Api-Key header."""
    origin = request.headers.get("Origin", "")
    if origin in _PAYROLL_CORS_ORIGINS:
        resp.headers["Access-Control-Allow-Origin"] = origin
        resp.headers["Vary"] = "Origin"
        resp.headers["Access-Control-Allow-Headers"] = "X-Api-Key, Content-Type"
        resp.headers["Access-Control-Allow-Methods"] = "GET, POST, PUT, OPTIONS"
        resp.headers["Access-Control-Max-Age"] = "86400"
    return resp


@app.route("/payroll-owed", methods=["GET", "OPTIONS"])
def payroll_owed():
    """Read-only payroll accrual for the cash position tracker. Returns the same
    numbers as the daily digest's payroll block, gated by a shared key
    (PAYROLL_API_KEY) so it can be called cross-origin without a login session.
    Jobber tokens stay server-side; only the computed dollar figures cross the
    wire."""
    if request.method == "OPTIONS":
        return _payroll_cors(app.make_response(("", 204)))

    # Strip whitespace so a stray space/newline pasted into the Railway var or
    # the client doesn't cause a silent mismatch.
    expected = (os.getenv("PAYROLL_API_KEY") or "").strip()
    provided = (request.headers.get("X-Api-Key", "") or "").strip()
    if not expected or not secrets.compare_digest(provided, expected):
        resp = jsonify({"error": "unauthorized"})
        resp.status_code = 401
        return _payroll_cors(resp)

    from datetime import datetime
    from financial.compute import compute_payroll_accrual
    from financial.jobber_finance import current_pay_week_start, fetch_time_entries_since

    week_start = current_pay_week_start()
    start_dt = datetime.combine(week_start, datetime.min.time())
    entries = fetch_time_entries_since(start_dt)
    if entries is None:
        resp = jsonify({
            "error": "jobber_auth",
            "message": "Jobber token missing or expired. Reconnect jobber-app at /login.",
        })
        resp.status_code = 502
        return _payroll_cors(resp)

    payroll = compute_payroll_accrual(entries=entries)
    resp = jsonify({
        "ok": True,
        "as_of": payroll["as_of"],
        "week_start": payroll["week_start"],
        "total_hours": payroll["total_hours"],
        "gross_pay": payroll["gross_pay"],
        "tax_burden_pct": payroll["tax_burden_pct"],
        "tax_burden": payroll["tax_burden"],
        "total_accrual": payroll["total_accrual"],
        "breakdown": payroll["breakdown"],
        "missing_rates": payroll["missing_rates"],
    })
    return _payroll_cors(resp)


@app.route("/receivables", methods=["GET", "OPTIONS"])
def receivables():
    """Read-only accounts receivable for the cash position tracker. Outstanding
    Jobber invoices aggregated per client. Same shared key (PAYROLL_API_KEY) and
    CORS as /payroll-owed; Jobber tokens stay server-side."""
    if request.method == "OPTIONS":
        return _payroll_cors(app.make_response(("", 204)))

    expected = (os.getenv("PAYROLL_API_KEY") or "").strip()
    provided = (request.headers.get("X-Api-Key", "") or "").strip()
    if not expected or not secrets.compare_digest(provided, expected):
        resp = jsonify({"error": "unauthorized"})
        resp.status_code = 401
        return _payroll_cors(resp)

    from datetime import date
    from financial.compute import compute_ar
    from financial.jobber_finance import fetch_open_invoices

    invoices = fetch_open_invoices()
    if invoices is None:
        resp = jsonify({
            "error": "jobber_auth",
            "message": "Jobber token missing or expired. Reconnect jobber-app at /login.",
        })
        resp.status_code = 502
        return _payroll_cors(resp)

    ar = compute_ar(invoices)

    # Aggregate outstanding per client across all open invoices (not just the
    # top overdue) so the cash tracker can show the full receivables list.
    by_client = {}
    for inv in ar.get("invoices", []):
        name = inv.get("client_name") or "Unknown"
        b = by_client.setdefault(name, {"name": name, "amount": 0.0, "invoices": 0, "max_days_past_due": 0})
        b["amount"] += inv.get("outstanding", 0) or 0
        b["invoices"] += 1
        b["max_days_past_due"] = max(b["max_days_past_due"], inv.get("days_past_due", 0) or 0)
    breakdown = sorted(by_client.values(), key=lambda c: c["amount"], reverse=True)
    for b in breakdown:
        b["amount"] = round(b["amount"], 2)

    resp = jsonify({
        "ok": True,
        "as_of": date.today().isoformat(),
        "total_outstanding": ar["total_outstanding"],
        "invoice_count": len(ar.get("invoices", [])),
        "breakdown": breakdown,
        "buckets": ar.get("buckets", []),
    })
    return _payroll_cors(resp)


def _api_key_ok():
    expected = (os.getenv("PAYROLL_API_KEY") or "").strip()
    provided = (request.headers.get("X-Api-Key", "") or "").strip()
    return bool(expected) and secrets.compare_digest(provided, expected)


@app.route("/bank-connect", methods=["POST", "OPTIONS"])
def bank_connect():
    """Claim a SimpleFIN setup token (sent once from the cash tracker), store the
    durable access URL server-side, and return the current account balances.
    Key-gated and CORS-locked like the other cash-tracker endpoints."""
    if request.method == "OPTIONS":
        return _payroll_cors(app.make_response(("", 204)))
    if not _api_key_ok():
        resp = jsonify({"error": "unauthorized"})
        resp.status_code = 401
        return _payroll_cors(resp)

    data = request.get_json(silent=True) or {}
    token = (data.get("setup_token") or "").strip()
    if not token:
        resp = jsonify({"error": "missing_token", "message": "No setup token provided."})
        resp.status_code = 400
        return _payroll_cors(resp)

    from financial.bank import claim_setup_token, store_access_url, fetch_accounts
    try:
        access_url = claim_setup_token(token)
        store_access_url(access_url)
        accounts, errors = fetch_accounts(access_url)
    except Exception as e:
        logger.exception("bank-connect failed")
        resp = jsonify({"error": "connect_failed", "message": str(e)[:200]})
        resp.status_code = 502
        return _payroll_cors(resp)

    return _payroll_cors(jsonify({"ok": True, "accounts": accounts, "errors": errors}))


@app.route("/bank-balances", methods=["GET", "OPTIONS"])
def bank_balances():
    """Read-only account balances. Prefers a live Plaid connection; falls back
    to the SimpleFIN cache if that is what is connected."""
    if request.method == "OPTIONS":
        return _payroll_cors(app.make_response(("", 204)))
    if not _api_key_ok():
        resp = jsonify({"error": "unauthorized"})
        resp.status_code = 401
        return _payroll_cors(resp)

    # Live Plaid first.
    try:
        from financial.plaid_client import fetch_plaid_balances
        plaid_accounts = fetch_plaid_balances()
        if plaid_accounts is not None:
            return _payroll_cors(jsonify({"ok": True, "source": "plaid", "accounts": plaid_accounts}))
    except Exception as e:
        logger.exception("plaid balances failed")
        resp = jsonify({"error": "fetch_failed", "message": str(e)[:200]})
        resp.status_code = 502
        return _payroll_cors(resp)

    # SimpleFIN fallback.
    from financial.bank import load_access_url, fetch_accounts
    access_url = load_access_url()
    if not access_url:
        resp = jsonify({"error": "not_connected", "message": "No bank connection yet."})
        resp.status_code = 409
        return _payroll_cors(resp)
    try:
        accounts, errors = fetch_accounts(access_url)
    except Exception as e:
        logger.exception("bank-balances failed")
        resp = jsonify({"error": "fetch_failed", "message": str(e)[:200]})
        resp.status_code = 502
        return _payroll_cors(resp)

    return _payroll_cors(jsonify({"ok": True, "source": "simplefin", "accounts": accounts, "errors": errors}))


@app.route("/plaid-link-token", methods=["POST", "OPTIONS"])
def plaid_link_token():
    """Create a Plaid Hosted Link session for the cash tracker to open."""
    if request.method == "OPTIONS":
        return _payroll_cors(app.make_response(("", 204)))
    if not _api_key_ok():
        resp = jsonify({"error": "unauthorized"})
        resp.status_code = 401
        return _payroll_cors(resp)
    from financial.plaid_client import create_hosted_link
    try:
        data = create_hosted_link()
    except Exception as e:
        logger.exception("plaid-link-token failed")
        resp = jsonify({"error": "link_token_failed", "message": str(e)[:200]})
        resp.status_code = 502
        return _payroll_cors(resp)
    return _payroll_cors(jsonify({"ok": True, **data}))


@app.route("/plaid-complete", methods=["POST", "OPTIONS"])
def plaid_complete():
    """Poll a Hosted Link session; once the user finishes, exchange and store the
    token and return live balances. Returns {pending: true} until then."""
    if request.method == "OPTIONS":
        return _payroll_cors(app.make_response(("", 204)))
    if not _api_key_ok():
        resp = jsonify({"error": "unauthorized"})
        resp.status_code = 401
        return _payroll_cors(resp)
    data = request.get_json(silent=True) or {}
    link_token = (data.get("link_token") or "").strip()
    if not link_token:
        resp = jsonify({"error": "missing_link_token"})
        resp.status_code = 400
        return _payroll_cors(resp)
    from financial.plaid_client import complete_link
    try:
        status, accounts = complete_link(link_token)
    except Exception as e:
        logger.exception("plaid-complete failed")
        resp = jsonify({"error": "complete_failed", "message": str(e)[:200]})
        resp.status_code = 502
        return _payroll_cors(resp)
    if status == "pending":
        return _payroll_cors(jsonify({"ok": True, "pending": True}))
    return _payroll_cors(jsonify({"ok": True, "pending": False, "source": "plaid", "accounts": accounts}))


@app.route("/api/summary", methods=["GET", "OPTIONS"])
def api_summary():
    """Read-only headline numbers for the master Command Center hub. Key-gated
    and CORS-locked like the other cross-app endpoints. Returns a job-costing
    and a mowing block; either is null if its source is unavailable, so the hub
    degrades gracefully rather than erroring."""
    if request.method == "OPTIONS":
        return _payroll_cors(app.make_response(("", 204)))
    if not _api_key_ok():
        resp = jsonify({"error": "unauthorized"})
        resp.status_code = 401
        return _payroll_cors(resp)

    job_costing = None
    try:
        d = compute_dashboard()
        if d:
            job_costing = {
                "month_label": d.get("month_label"),
                "revenue_mtd": d.get("month_revenue"),
                "net_margin_pct": d.get("avg_net_margin"),
                "job_count": d.get("month_job_count"),
            }
    except Exception:
        logger.exception("api/summary job_costing failed")

    mowing = None
    try:
        # Read the cached mow payload only; never trigger a live rebuild here.
        # refresh() does a slow Jobber pull and would block this status endpoint
        # (and take job costing down with it) on a cold container. The cache is
        # kept warm by the daily mow export and the /mow-dashboard route.
        from mow_dashboard import load_cache
        meta = (load_cache() or {}).get("meta", {})
        if meta:
            mowing = {
                "profit_per_week": meta.get("profitPerWeek"),
                "revenue_per_week": meta.get("revenuePerWeek"),
                "house_count": meta.get("houseCount"),
                "total_mows": meta.get("totalMows"),
            }
    except Exception:
        logger.exception("api/summary mowing failed")

    marketing_block = None
    try:
        # Cache-only — never triggers a live Jobber pull (keeps this endpoint fast
        # and matches the mowing block's read-only contract above).
        marketing_block = marketing.summary_block()
    except Exception:
        logger.exception("api/summary marketing failed")

    return _payroll_cors(jsonify({
        "ok": True, "job_costing": job_costing, "mowing": mowing, "marketing": marketing_block,
    }))


@app.route("/payouts", methods=["GET", "OPTIONS"])
def payouts():
    """Read-only upcoming Jobber payouts (PENDING + IN_TRANSIT) for the cash
    position tracker. Money collected through Jobber Payments that is settling
    to the bank but hasn't landed yet."""
    if request.method == "OPTIONS":
        return _payroll_cors(app.make_response(("", 204)))
    if not _api_key_ok():
        resp = jsonify({"error": "unauthorized"})
        resp.status_code = 401
        return _payroll_cors(resp)
    from financial.payouts import fetch_upcoming_payouts
    try:
        data = fetch_upcoming_payouts()
    except Exception as e:
        logger.exception("payouts failed")
        resp = jsonify({"error": "fetch_failed", "message": str(e)[:200]})
        resp.status_code = 502
        return _payroll_cors(resp)
    if data is None:
        resp = jsonify({"error": "jobber_auth", "message": "Jobber token missing or expired."})
        resp.status_code = 502
        return _payroll_cors(resp)
    return _payroll_cors(jsonify({"ok": True, **data}))


@app.route("/cash-state", methods=["GET", "PUT", "OPTIONS"])
def cash_state():
    """Cross-device store for the cash tracker's full state. GET returns the
    saved state (null if none); PUT overwrites it. Key-gated and CORS-locked."""
    if request.method == "OPTIONS":
        return _payroll_cors(app.make_response(("", 204)))
    if not _api_key_ok():
        resp = jsonify({"error": "unauthorized"})
        resp.status_code = 401
        return _payroll_cors(resp)

    from financial.cash_state import load_cash_state, save_cash_state
    if request.method == "GET":
        try:
            state = load_cash_state()
        except Exception as e:
            logger.exception("cash-state load failed")
            resp = jsonify({"error": "load_failed", "message": str(e)[:200]})
            resp.status_code = 502
            return _payroll_cors(resp)
        return _payroll_cors(jsonify({"ok": True, "state": state}))

    body = request.get_json(silent=True) or {}
    state = body.get("state")
    if not isinstance(state, dict):
        resp = jsonify({"error": "bad_state", "message": "Missing or invalid state object."})
        resp.status_code = 400
        return _payroll_cors(resp)
    try:
        save_cash_state(state)
    except Exception as e:
        logger.exception("cash-state save failed")
        resp = jsonify({"error": "save_failed", "message": str(e)[:200]})
        resp.status_code = 502
        return _payroll_cors(resp)
    return _payroll_cors(jsonify({"ok": True}))


@app.route("/lurvey-debug", methods=["GET"])
def lurvey_debug():
    """Temporary: log into Lurvey's and report dollar amounts + keyword context
    so we can locate the account balance on the page. Key-gated."""
    if not _api_key_ok():
        resp = jsonify({"error": "unauthorized"})
        resp.status_code = 401
        return resp
    from financial.lurvey import fetch_lurvey_debug
    try:
        return jsonify(fetch_lurvey_debug())
    except Exception as e:
        logger.exception("lurvey-debug failed")
        return jsonify({"error": "lurvey_failed", "message": str(e)[:300]})


@app.route("/financial-debug")
def financial_debug():
    """Dump raw responses from each Jobber query so we can see which ones
    work and which fail with schema errors."""
    if "access_token" not in session:
        return redirect(url_for("login"))
    from financial.jobber_finance import debug_all
    try:
        return jsonify(debug_all())
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route("/financial-schema")
def financial_schema():
    """Compact list of field names per Jobber type + arg names per top-level
    query field. Easier to skim than /financial-debug's full introspection."""
    if "access_token" not in session:
        return redirect(url_for("login"))
    from financial.jobber_finance import debug_field_names
    try:
        return jsonify(debug_field_names())
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route("/financial-ts-filter-probe")
def financial_ts_filter_probe():
    """Probe several plausible filter shapes for timeSheetEntries.startAt to
    discover which one Iso8601DateTimeRangeInput actually expects."""
    if "access_token" not in session:
        return redirect(url_for("login"))
    from financial.jobber_finance import debug_timesheet_filters
    try:
        return jsonify(debug_timesheet_filters())
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route("/backfill")
def backfill():
    if "access_token" not in session:
        return redirect(url_for("login"))
    result = run_backfill()
    return jsonify(result)



@app.route("/outstanding-quotes")
def outstanding_quotes():
    if "access_token" not in session:
        return redirect(url_for("login"))
    from jobber_sync import graphql_request
    from jobber_sync import graphql_request as gql
    import csv, io
    from flask import Response

    # Paginate quotes and filter client-side. We tried server-side filter
    # earlier but the enum value we guessed (awaiting_response) didn't match
    # what Jobber's QuoteStatusTypeEnum expects, so we punt to Python-side.
    query = """
    query OutstandingQuotes($cursor: String) {
      quotes(first: 50, after: $cursor) {
        nodes {
          quoteNumber title quoteStatus sentAt
          amounts { total }
          client {
            name
            phones { number primary }
            emails { address primary }
          }
          property { address { street city province postalCode } }
        }
        pageInfo { hasNextPage endCursor }
      }
    }
    """

    all_quotes = []
    statuses_seen = {}
    pages = 0
    page_limit = int(request.args.get("max_pages", "30"))
    cursor = None
    last_err = None
    while True:
        pages += 1
        if pages > page_limit:
            break
        data = gql(query, {"cursor": cursor}, access_token=session.get("access_token"))
        if not data:
            last_err = "graphql_request returned None (check Railway logs for HTTP error body)"
            break
        # Check errors first — Jobber returns 200 with errors-only body for
        # schema mismatches, throttle, etc. and we want to see WHAT it said.
        if data.get("errors"):
            last_err = data["errors"]
            if not data.get("data"):
                break  # no data at all, just errors
            # else partial data; keep going with what's there
        qdata = (data.get("data") or {}).get("quotes")
        if not qdata:
            last_err = last_err or "data.quotes missing"
            break
        nodes = qdata.get("nodes", [])
        for n in nodes:
            s = (n.get("quoteStatus") or "").lower()
            statuses_seen[s] = statuses_seen.get(s, 0) + 1
            if s == "awaiting_response":
                all_quotes.append(n)
        pi = qdata.get("pageInfo", {})
        if not pi.get("hasNextPage"):
            break
        cursor = pi.get("endCursor")

    # Diagnostic header for debugging: shows statuses Jobber actually returned
    # across the scanned quotes. Visible in JSON output below.
    diagnostics = {
        "pages_scanned": pages,
        "total_quotes_scanned": sum(statuses_seen.values()),
        "statuses_seen": statuses_seen,
        "last_error": last_err,
    }

    fmt = request.args.get("format", "json")

    from collections import OrderedDict
    clients = OrderedDict()
    for q in all_quotes:
        client = q.get("client") or {}
        name = client.get("name", "")
        emails = client.get("emails") or []
        phones = client.get("phones") or []
        primary_email = next((e["address"] for e in emails if e.get("primary")), emails[0]["address"] if emails else "")
        primary_phone = next((p["number"] for p in phones if p.get("primary")), phones[0]["number"] if phones else "")
        addr = ((q.get("property") or {}).get("address") or {})
        address = ", ".join(p for p in [addr.get("street",""), addr.get("city",""), addr.get("province",""), addr.get("postalCode","")] if p)
        line_items = (q.get("lineItems") or {}).get("nodes", [])
        li_names = [li.get("name", "") for li in line_items if li.get("name")]

        if name not in clients:
            clients[name] = {
                "Client": name,
                "Email": primary_email,
                "Phone": primary_phone,
                "Property": address,
                "Quotes": [],
                "Line Items": [],
                "Total Outstanding": 0,
            }

        clients[name]["Quotes"].append(q.get("quoteNumber", ""))
        clients[name]["Line Items"].extend(li_names)
        clients[name]["Total Outstanding"] += q.get("amounts", {}).get("total", 0)

    rows = []
    for c in clients.values():
        rows.append({
            "Client": c["Client"],
            "Email": c["Email"],
            "Phone": c["Phone"],
            "Property": c["Property"],
            "Quote #s": ", ".join(c["Quotes"]),
            "# of Quotes": len(c["Quotes"]),
            "Total Outstanding ($)": round(c["Total Outstanding"], 2),
            "Line Items": ", ".join(dict.fromkeys(c["Line Items"])),  # unique, preserves order
        })

    if fmt == "csv":
        if not rows:
            return "No outstanding quotes found.", 200
        si = io.StringIO()
        writer = csv.DictWriter(si, fieldnames=rows[0].keys())
        writer.writeheader()
        writer.writerows(rows)
        return Response(si.getvalue(), mimetype="text/csv",
                        headers={"Content-Disposition": "attachment; filename=outstanding_quotes.csv"})

    return jsonify({
        "total_clients": len(rows),
        "clients": rows,
        "diagnostics": diagnostics,
    })


@app.route("/dashboard")
def dashboard():
    try:
        data = compute_dashboard()
    except Exception as e:
        data = None
    return render_template("dashboard.html", data=data)


@app.route("/mow-dashboard")
def mow_dashboard():
    """Mowing-numbers dashboard. Reads the cache the daily mow export writes;
    on a cold start (no cache) or ?refresh=1 it does a live read-only Jobber
    pull. No secrets on this page, so it matches /dashboard (no auth gate)."""
    import json as _json
    from mow_dashboard import get_payload
    force = request.args.get("refresh") == "1"
    try:
        payload = get_payload(force=force)
    except Exception as e:
        return (
            "<h2>Mowing dashboard</h2>"
            "<p>Couldn't build the numbers right now.</p>"
            f"<pre style='color:#a5281b'>{e}</pre>"
            "<p>If the Jobber token just expired, wait for the next hourly sync "
            "and <a href='/mow-dashboard?refresh=1'>try again</a>.</p>"
        ), 500
    return render_template("mow_dashboard.html",
                           data_json=_json.dumps(payload), meta=payload["meta"])


# --- Financial (P&L) dashboard ------------------------------------------------
# Pre-built React/Recharts app (built in the Maintenance Program repo with
# `npm run build:jobber`, base = /financials/). Its files live in
# financial_dashboard/. Unlike the mow dashboard this shows full P&L numbers,
# so it is gated behind the Jobber login session.
FIN_DASH_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "financial_dashboard")


@app.route("/financials")
def financials():
    if "access_token" not in session:
        return redirect(url_for("login"))
    # Inject the AI-CFO chat widget into the built dashboard at serve time. This
    # keeps the chat working without rebuilding the React bundle (the build can't
    # run locally while the project sits on iCloud-synced storage).
    from financial.cfo import CHAT_WIDGET_HTML
    with open(os.path.join(FIN_DASH_DIR, "index.html"), encoding="utf-8") as f:
        html = f.read()
    return html.replace("</body>", CHAT_WIDGET_HTML + "</body>")


@app.route("/financials/ask", methods=["POST"])
def financials_ask():
    """AI CFO endpoint. The dashboard's chat panel POSTs {question, history};
    Claude answers grounded in pnl.json. Gated on the Jobber session (JSON 401
    rather than a redirect, since this is called via fetch)."""
    if "access_token" not in session:
        return jsonify({"error": "Session expired. Refresh the page and log in again."}), 401
    body = request.get_json(silent=True) or {}
    question = (body.get("question") or "").strip()
    if not question:
        return jsonify({"error": "Ask a question first."}), 400
    from financial.cfo import answer_question
    result = answer_question(question, history=body.get("history"))
    return jsonify(result)


@app.route("/financials/<path:asset>")
def financials_assets(asset):
    # Serves JS/CSS and pnl.json. Gated too, because pnl.json holds the numbers.
    if "access_token" not in session:
        return redirect(url_for("login"))
    return send_from_directory(FIN_DASH_DIR, asset)


@app.route("/clients")
def clients():
    access_token = session.get("access_token")
    if not access_token:
        return redirect(url_for("login"))

    resp = requests.post(
        GRAPHQL_URL,
        json={"query": CLIENTS_QUERY},
        headers={
            "Authorization": f"Bearer {access_token}",
            "Content-Type": "application/json",
            "X-JOBBER-GRAPHQL-VERSION": "2026-03-10",
        },
    )

    if resp.status_code == 401:
        session.clear()
        return redirect(url_for("login"))

    if not resp.ok:
        return jsonify({"error": "GraphQL request failed", "details": resp.text}), resp.status_code

    data = resp.json()
    if "errors" in data:
        return jsonify({"errors": data["errors"]}), 400

    return jsonify(data.get("data", {}))


# ---------------------------------------------------------------------------
# Marketing ROI dashboard
# ---------------------------------------------------------------------------

@app.route("/marketing")
def marketing_dashboard():
    """Marketing ROI dashboard. Lead source per client comes live from Jobber;
    spend is logged on /marketing/spend. Gated behind the Jobber session because
    it shows revenue (matches /financials). ?range=30d|90d|ytd|all selects the
    window; ?refresh=1 forces a live Jobber re-pull before computing."""
    if "access_token" not in session:
        return redirect(url_for("login"))
    import json as _json
    range_key = request.args.get("range", "ytd")
    if request.args.get("refresh") == "1":
        try:
            marketing.get_clients(force=True)
        except Exception:
            logger.exception("marketing: forced refresh failed")
    try:
        data = marketing.compute(range_key)
    except Exception as e:
        logger.exception("marketing: compute failed")
        return (
            "<h2>Marketing dashboard</h2>"
            "<p>Couldn't build the numbers right now.</p>"
            f"<pre style='color:#a5281b'>{e}</pre>"
            "<p>If the Jobber token just expired, reconnect at <a href='/login'>/login</a> "
            "and <a href='/marketing?refresh=1'>try again</a>.</p>"
        ), 500
    return render_template("marketing_dashboard.html", data=data,
                           trend_json=_json.dumps(data["trend"]))


@app.route("/marketing/spend", methods=["GET", "POST"])
def marketing_spend():
    """Log marketing spend (door hangers, the monthly SEO fee, FB boosts). Stored
    in the _marketing_spend Sheet tab so it survives Railway redeploys and stays
    editable from a phone."""
    if "access_token" not in session:
        return redirect(url_for("login"))
    msg = ""
    if request.method == "POST":
        entry = {
            "channel": (request.form.get("channel") or "").strip(),
            "kind": (request.form.get("kind") or "one_time").strip(),
            "amount": (request.form.get("amount") or "").strip(),
            "date": (request.form.get("date") or "").strip(),
            "end_date": (request.form.get("end_date") or "").strip(),
            "quantity": (request.form.get("quantity") or "").strip(),
            "neighborhood": (request.form.get("neighborhood") or "").strip(),
            "note": (request.form.get("note") or "").strip(),
        }
        if not entry["channel"] or not entry["amount"]:
            msg = "Channel and amount are required."
        else:
            try:
                marketing.add_spend(entry)
                return redirect(url_for("marketing_spend"))
            except Exception as e:
                logger.exception("marketing: add_spend failed")
                msg = f"Could not save: {e}"
    try:
        entries = sorted(marketing.read_spend(), key=lambda e: str(e.get("date") or ""), reverse=True)
    except Exception:
        logger.exception("marketing: read_spend failed")
        entries = []
    return render_template("marketing_spend.html", entries=entries,
                           channels=marketing.CANONICAL_SOURCES, msg=msg)


@app.route("/marketing/spend/delete", methods=["POST"])
def marketing_spend_delete():
    if "access_token" not in session:
        return redirect(url_for("login"))
    eid = (request.form.get("id") or "").strip()
    if eid:
        try:
            marketing.delete_spend(eid)
        except Exception:
            logger.exception("marketing: delete_spend failed")
    return redirect(url_for("marketing_spend"))


@app.route("/logout")
def logout():
    session.clear()
    return redirect(url_for("index"))


# ---------------------------------------------------------------------------
# Legal pages (for Intuit app review: EULA + Privacy Policy)
# ---------------------------------------------------------------------------

_LEGAL_STYLE = (
    "max-width:760px;margin:40px auto;padding:24px;"
    "font-family:Helvetica,Arial,sans-serif;color:#222;line-height:1.5"
)


@app.route("/eula")
def eula():
    return f"""
    <!doctype html><html><body style='{_LEGAL_STYLE}'>
    <h1>End-User License Agreement</h1>
    <p><strong>A&amp;N Cashflow</strong> ("the App") is an internal financial reporting tool
    operated by A&amp;N Outdoor Services LLC ("A&amp;N", "we", "our"). Last updated:
    May 13, 2026.</p>

    <h2>1. License Grant</h2>
    <p>This App is licensed for internal use by A&amp;N Outdoor Services LLC and its
    authorized personnel only. It is not licensed for distribution, resale, or use
    by any third party. By connecting your QuickBooks Online account, you grant the
    App permission to read account-level financial data on your behalf.</p>

    <h2>2. Permitted Use</h2>
    <p>The App is provided solely to enable A&amp;N to view consolidated cashflow
    information drawn from its own QuickBooks Online company, Jobber account, and
    related operational systems. Use of the App for any other purpose is prohibited.</p>

    <h2>3. No Warranties</h2>
    <p>The App is provided "as is" without warranties of any kind, express or implied.
    A&amp;N makes no warranty that the App will be error-free or that the financial
    information it displays is suitable for accounting, tax, or compliance purposes.
    Authoritative financial records remain in QuickBooks Online and Jobber.</p>

    <h2>4. Limitation of Liability</h2>
    <p>In no event shall A&amp;N be liable for any indirect, incidental, or
    consequential damages arising out of the use of, or inability to use, the App.</p>

    <h2>5. Termination</h2>
    <p>You may terminate this agreement at any time by disconnecting the App from
    your QuickBooks Online account or revoking access in your Intuit account
    settings.</p>

    <h2>6. Governing Law</h2>
    <p>This agreement is governed by the laws of the State of Illinois, United States.</p>

    <h2>7. Contact</h2>
    <p>Questions: <a href='mailto:alex@anoutdoorservices.com'>alex@anoutdoorservices.com</a></p>
    </body></html>
    """


@app.route("/privacy")
def privacy():
    return f"""
    <!doctype html><html><body style='{_LEGAL_STYLE}'>
    <h1>Privacy Policy</h1>
    <p><strong>A&amp;N Cashflow</strong> is operated by A&amp;N Outdoor Services LLC.
    Last updated: May 13, 2026.</p>

    <h2>1. Who We Are</h2>
    <p>A&amp;N Outdoor Services LLC, a landscaping and outdoor services company based
    in Arlington Heights, Illinois, United States.</p>

    <h2>2. What Data We Access</h2>
    <p>When connected to QuickBooks Online, the App reads the following data from
    your QuickBooks company:</p>
    <ul>
      <li>Account names, types, and current balances for Bank and Credit Card accounts</li>
    </ul>
    <p>The App does not access, store, or transmit transaction-level data, customer
    information, vendor information, employee data, or payroll data from QuickBooks
    Online.</p>

    <h2>3. Why We Access It</h2>
    <p>This data is used solely to compute and email an internal daily cashflow
    summary to authorized A&amp;N personnel.</p>

    <h2>4. Who Sees the Data</h2>
    <p>The App is for internal use by A&amp;N Outdoor Services LLC. Data is not
    shared with, sold to, or disclosed to any third party. The App does not have
    public users.</p>

    <h2>5. Where Data Is Stored</h2>
    <p>OAuth tokens are stored in encrypted environment variables on Railway, our
    hosting provider. Daily snapshot data is stored on Railway in the App's own
    storage. We do not maintain a separate user database.</p>

    <h2>6. Data Retention</h2>
    <p>OAuth tokens are retained for as long as the App is connected to your
    QuickBooks Online company. Upon disconnection, tokens are deleted and no
    further data is accessed. Past daily snapshots may be retained in encrypted
    form for historical comparison.</p>

    <h2>7. Your Rights</h2>
    <p>You may disconnect the App at any time via your Intuit account settings or
    by visiting our disconnect URL. Disconnection immediately revokes the App's
    access.</p>

    <h2>8. Security</h2>
    <p>All connections to QuickBooks Online use HTTPS and OAuth 2.0. Tokens are
    never logged, displayed, or transmitted outside of authenticated API calls
    to Intuit.</p>

    <h2>9. Changes to This Policy</h2>
    <p>If this policy materially changes, we will update the date at the top of
    this page.</p>

    <h2>10. Contact</h2>
    <p>Questions: <a href='mailto:alex@anoutdoorservices.com'>alex@anoutdoorservices.com</a></p>
    </body></html>
    """


# ---------------------------------------------------------------------------
# Startup / teardown
# ---------------------------------------------------------------------------

financial_qbo.register_routes(app)
financial_google_auth.register_routes(app)


if __name__ == "__main__":
    if not CLIENT_ID or not CLIENT_SECRET:
        print("\nWARNING: JOBBER_CLIENT_ID and JOBBER_CLIENT_SECRET are not set.\n")

    # Avoid double-start from Flask's reloader spawning a second process
    if os.environ.get("WERKZEUG_RUN_MAIN") == "true" or not app.debug:
        start_scheduler(run_sync, reconcile_daily_overhead, digest_fn=run_digest,
                        mow_export_fn=run_mow_export,
                        pnl_reminder_fn=send_pnl_reminder,
                        marketing_refresh_fn=marketing.refresh)

    import atexit
    atexit.register(stop_scheduler)

    app.run(host='0.0.0.0', port=int(os.environ.get('PORT', 8080)))
