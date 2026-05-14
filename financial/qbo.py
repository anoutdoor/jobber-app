"""QuickBooks Online OAuth + Account balance queries.

Mirrors the Jobber auth pattern in jobber_sync.py:
  - Tokens persisted to qbo_token_store.json (gitignored)
  - Auto-refresh on 401
  - Flask routes for initial OAuth handshake exposed via register_routes()

Required .env vars:
  QBO_CLIENT_ID
  QBO_CLIENT_SECRET
  QBO_REDIRECT_URI       (e.g. http://127.0.0.1:8080/qbo/callback)
  QBO_ENVIRONMENT        ("production" or "sandbox", default production)
"""
import os
import json
import logging
import secrets as secrets_mod
from urllib.parse import urlencode

import requests
from flask import redirect, request, session, jsonify, url_for

logger = logging.getLogger(__name__)

QBO_AUTH_URL = "https://appcenter.intuit.com/connect/oauth2"
QBO_TOKEN_URL = "https://oauth.platform.intuit.com/oauth2/v1/tokens/bearer"
QBO_SCOPES = "com.intuit.quickbooks.accounting"

TOKEN_STORE_FILE = "qbo_token_store.json"


def _api_base():
    env = (os.getenv("QBO_ENVIRONMENT") or "production").lower()
    if env == "sandbox":
        return "https://sandbox-quickbooks.api.intuit.com"
    return "https://quickbooks.api.intuit.com"


# ---------------------------------------------------------------------------
# Token store
# ---------------------------------------------------------------------------

def load_tokens():
    if os.path.exists(TOKEN_STORE_FILE):
        with open(TOKEN_STORE_FILE) as f:
            return json.load(f)
    # Allow env-based override for Railway: QBO_TOKEN_STORE as JSON blob
    env_blob = os.getenv("QBO_TOKEN_STORE")
    if env_blob:
        return json.loads(env_blob)
    return {}


def save_tokens(access_token, refresh_token, realm_id):
    payload = {
        "access_token": access_token,
        "refresh_token": refresh_token,
        "realm_id": realm_id,
    }
    with open(TOKEN_STORE_FILE, "w") as f:
        json.dump(payload, f)
    if os.getenv("QBO_TOKEN_STORE"):
        logger.info("QBO token refreshed. Update QBO_TOKEN_STORE env var in Railway.")


# ---------------------------------------------------------------------------
# OAuth flow
# ---------------------------------------------------------------------------

def auth_url():
    state = secrets_mod.token_urlsafe(16)
    session["qbo_oauth_state"] = state
    params = {
        "client_id": os.getenv("QBO_CLIENT_ID"),
        "redirect_uri": os.getenv("QBO_REDIRECT_URI"),
        "response_type": "code",
        "scope": QBO_SCOPES,
        "state": state,
    }
    return f"{QBO_AUTH_URL}?{urlencode(params)}"


def exchange_code(code, realm_id):
    resp = requests.post(
        QBO_TOKEN_URL,
        data={
            "grant_type": "authorization_code",
            "code": code,
            "redirect_uri": os.getenv("QBO_REDIRECT_URI"),
        },
        auth=(os.getenv("QBO_CLIENT_ID"), os.getenv("QBO_CLIENT_SECRET")),
        headers={"Accept": "application/json"},
    )
    resp.raise_for_status()
    data = resp.json()
    save_tokens(data["access_token"], data["refresh_token"], realm_id)
    return data


def refresh_access_token():
    tokens = load_tokens()
    refresh_token = tokens.get("refresh_token")
    if not refresh_token:
        logger.error("QBO: no refresh token — re-authenticate at /qbo/login")
        return None

    resp = requests.post(
        QBO_TOKEN_URL,
        data={"grant_type": "refresh_token", "refresh_token": refresh_token},
        auth=(os.getenv("QBO_CLIENT_ID"), os.getenv("QBO_CLIENT_SECRET")),
        headers={"Accept": "application/json"},
    )
    if not resp.ok:
        logger.error(f"QBO token refresh failed: {resp.text}")
        return None

    data = resp.json()
    new_access = data.get("access_token")
    new_refresh = data.get("refresh_token", refresh_token)
    save_tokens(new_access, new_refresh, tokens.get("realm_id"))
    logger.info("QBO access token refreshed.")
    return new_access


# ---------------------------------------------------------------------------
# API calls
# ---------------------------------------------------------------------------

def _api_get(path, access_token=None, _retry=True):
    tokens = load_tokens()
    if not access_token:
        access_token = tokens.get("access_token")
    realm_id = tokens.get("realm_id")
    if not realm_id:
        raise RuntimeError("QBO: no realm_id stored. Re-authenticate at /qbo/login")

    url = f"{_api_base()}/v3/company/{realm_id}{path}"
    resp = requests.get(
        url,
        headers={
            "Authorization": f"Bearer {access_token}",
            "Accept": "application/json",
        },
    )

    if resp.status_code == 401 and _retry:
        logger.info("QBO 401 — refreshing token.")
        new_token = refresh_access_token()
        if new_token:
            return _api_get(path, new_token, _retry=False)
        return None

    if not resp.ok:
        logger.error(f"QBO HTTP {resp.status_code}: {resp.text[:500]}")
        return None

    return resp.json()


def fetch_account_balances():
    """Return list of dicts:
        {name, type, subtype, current_balance, currency}
    for all active Bank and Credit Card accounts.

    Bank balances are positive (cash on hand).
    Credit Card balances are stored positive in QBO but represent debt.
    """
    # QBO SQL-like query language
    query = (
        "SELECT Id, Name, AccountType, AccountSubType, CurrentBalance, CurrencyRef "
        "FROM Account "
        "WHERE Active = true AND AccountType IN ('Bank','Credit Card') "
        "MAXRESULTS 100"
    )
    path = f"/query?query={requests.utils.quote(query)}&minorversion=70"
    data = _api_get(path)
    if not data:
        return []

    accounts = ((data.get("QueryResponse") or {}).get("Account") or [])
    out = []
    for a in accounts:
        out.append({
            "id": a.get("Id"),
            "name": a.get("Name"),
            "type": a.get("AccountType"),
            "subtype": a.get("AccountSubType"),
            "balance": round(float(a.get("CurrentBalance") or 0), 2),
            "currency": (a.get("CurrencyRef") or {}).get("value", "USD"),
        })
    return out


def summarize_balances(accounts):
    """Split balances into cash (Bank) and credit card debt."""
    cash_total = 0.0
    cc_total = 0.0
    cash_accounts = []
    cc_accounts = []
    for a in accounts:
        if a["type"] == "Bank":
            cash_total += a["balance"]
            cash_accounts.append(a)
        elif a["type"] == "Credit Card":
            cc_total += a["balance"]
            cc_accounts.append(a)
    return {
        "cash_total": round(cash_total, 2),
        "cc_total": round(cc_total, 2),
        "cash_accounts": cash_accounts,
        "cc_accounts": cc_accounts,
    }


# ---------------------------------------------------------------------------
# Flask routes — register on the main app
# ---------------------------------------------------------------------------

def register_routes(app):
    @app.route("/qbo/login")
    def qbo_login():
        return redirect(auth_url())

    @app.route("/qbo/callback")
    def qbo_callback():
        error = request.args.get("error")
        if error:
            return jsonify({"error": error, "description": request.args.get("error_description")}), 400

        code = request.args.get("code")
        state = request.args.get("state")
        realm_id = request.args.get("realmId")

        if not code or not realm_id:
            return jsonify({"error": "Missing code or realmId"}), 400
        if state != session.get("qbo_oauth_state"):
            return jsonify({"error": "State mismatch — possible CSRF"}), 400
        session.pop("qbo_oauth_state", None)

        try:
            exchange_code(code, realm_id)
        except Exception as e:
            return jsonify({"error": "QBO token exchange failed", "details": str(e)}), 400

        return redirect(url_for("index"))

    @app.route("/qbo/test")
    def qbo_test():
        accounts = fetch_account_balances()
        return jsonify({"accounts": accounts, "summary": summarize_balances(accounts)})

    @app.route("/qbo/disconnect")
    def qbo_disconnect():
        """Intuit requires a disconnect URL. This revokes the stored tokens and
        clears local state. The user is responsible for also revoking access in
        their Intuit account settings.
        """
        import os as _os
        if _os.path.exists(TOKEN_STORE_FILE):
            try:
                _os.remove(TOKEN_STORE_FILE)
            except OSError:
                pass
        return (
            "<html><body style='font-family:Helvetica,Arial;max-width:600px;margin:40px auto;padding:24px'>"
            "<h1>QuickBooks disconnected</h1>"
            "<p>The A&amp;N Cashflow App has been disconnected. Local OAuth tokens "
            "were removed. To complete the disconnection, also revoke access in your "
            "<a href='https://accounts.intuit.com/app/account-manager/security'>Intuit "
            "account settings</a>.</p>"
            "<p><a href='/'>Return to home</a></p>"
            "</body></html>"
        )
