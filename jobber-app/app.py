import os
import secrets
import logging

import requests
from dotenv import load_dotenv

from flask import Flask, redirect, request, session, url_for, jsonify, render_template
from jobber_sync import save_tokens, run_sync, read_last_sync, reconcile_daily_overhead
from dashboard import compute_dashboard
from backfill import run_backfill
from scheduler import start_scheduler, stop_scheduler

load_dotenv()
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s")

app = Flask(__name__)
app.secret_key = os.getenv("FLASK_SECRET_KEY", secrets.token_hex(32))

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

        return (
            "<h2>Jobber Job Costing — Connected</h2>"
            + sync_info
            + "<p><a href='/dashboard'><strong>→ Open Dashboard</strong></a></p>"
            + "<p><a href='/sync-now'>Run Sync Now</a></p>"
            + "<p><a href='/backfill'>Run Historical Backfill (Jan 1 2026 – Today)</a></p>"
            + "<p><a href='/reconcile-now'>Run Overhead Reconciliation Now</a></p>"
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

    query = """
    query OutstandingQuotes($cursor: String) {
      quotes(first: 10, after: $cursor) {
        nodes {
          quoteNumber title quoteStatus sentAt
          amounts { total }
          client {
            name
            phones { number primary }
            emails { address primary }
          }
          property { address { street city province postalCode } }
          lineItems { nodes { name description quantity unitPrice totalPrice } }
        }
        pageInfo { hasNextPage endCursor }
      }
    }
    """

    all_quotes = []
    cursor = None
    while True:
        data = gql(query, {"cursor": cursor}, access_token=session.get("access_token"))
        if not data or not data.get("data"):
            break
        qdata = data["data"].get("quotes", {})
        nodes = qdata.get("nodes", [])
        for n in nodes:
            if n.get("quoteStatus") == "awaiting_response":
                all_quotes.append(n)
        pi = qdata.get("pageInfo", {})
        if not pi.get("hasNextPage"):
            break
        cursor = pi.get("endCursor")

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

    return jsonify({"total_clients": len(rows), "clients": rows})
    return jsonify(data)


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
# Startup / teardown
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    if not CLIENT_ID or not CLIENT_SECRET:
        print("\nWARNING: JOBBER_CLIENT_ID and JOBBER_CLIENT_SECRET are not set.\n")

    # Avoid double-start from Flask's reloader spawning a second process
    if os.environ.get("WERKZEUG_RUN_MAIN") == "true" or not app.debug:
        start_scheduler(run_sync, reconcile_daily_overhead)

    import atexit
    atexit.register(stop_scheduler)

    app.run(host='0.0.0.0', port=int(os.environ.get('PORT', 8080)))
