import os
import secrets
import logging

import requests
from dotenv import load_dotenv

from flask import Flask, redirect, request, session, url_for, jsonify, render_template
from werkzeug.middleware.proxy_fix import ProxyFix
from jobber_sync import save_tokens, run_sync, read_last_sync, reconcile_daily_overhead
from dashboard import compute_dashboard
from backfill import run_backfill
from scheduler import start_scheduler, stop_scheduler
from financial import qbo as financial_qbo
from financial import google_auth as financial_google_auth
from financial.digest import run_digest

load_dotenv()
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s")

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
            + "<p><a href='/sync-now'>Run Sync Now</a></p>"
            + "<p><a href='/backfill'>Run Historical Backfill (Jan 1 2026 – Today)</a></p>"
            + "<p><a href='/reconcile-now'>Run Overhead Reconciliation Now</a></p>"
            + "<p><a href='/digest-now?dry=1'>Preview Cashflow Digest (no email)</a></p>"
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


@app.route("/digest-now")
def digest_now():
    if "access_token" not in session:
        return redirect(url_for("login"))
    dry = request.args.get("dry") == "1"
    result = run_digest(dry_run=dry)
    if dry and "html" in result:
        return result["html"]
    return jsonify({k: v for k, v in result.items() if k != "html"})


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
        if not data or not data.get("data"):
            last_err = "no response"
            break
        if data.get("errors"):
            last_err = data["errors"]
            break
        qdata = data["data"].get("quotes", {})
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
        start_scheduler(run_sync, reconcile_daily_overhead, digest_fn=run_digest)

    import atexit
    atexit.register(stop_scheduler)

    app.run(host='0.0.0.0', port=int(os.environ.get('PORT', 8080)))
