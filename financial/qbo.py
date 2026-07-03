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

# In-memory cache — see jobber_sync._token_cache for rationale.
_token_cache = None


def _load_tokens_uncached():
    if os.path.exists(TOKEN_STORE_FILE):
        with open(TOKEN_STORE_FILE) as f:
            return json.load(f)
    try:
        from financial.token_persistence import read_tokens as _read_sheets
        sheets_tokens = _read_sheets("qbo")
        if sheets_tokens.get("access_token"):
            return sheets_tokens
    except Exception as e:
        logger.warning(f"QBO: Sheets token read failed ({e}); falling back to env.")
    env_blob = os.getenv("QBO_TOKEN_STORE")
    if env_blob:
        try:
            parsed = json.loads(env_blob)
            if isinstance(parsed, dict) and parsed.get("access_token"):
                return parsed
            logger.error("QBO_TOKEN_STORE is not a JSON object with access_token.")
        except json.JSONDecodeError:
            logger.error("QBO_TOKEN_STORE env var is not valid JSON.")
    return {}


def load_tokens():
    global _token_cache
    if _token_cache is not None:
        return _token_cache
    _token_cache = _load_tokens_uncached()
    return _token_cache


def save_tokens(access_token, refresh_token, realm_id):
    global _token_cache
    payload = {
        "access_token": access_token,
        "refresh_token": refresh_token,
        "realm_id": realm_id,
    }
    _token_cache = payload  # update cache first so concurrent readers see fresh
    with open(TOKEN_STORE_FILE, "w") as f:
        json.dump(payload, f)
    try:
        from financial.token_persistence import write_tokens as _write_sheets
        _write_sheets("qbo", payload)
    except Exception as e:
        logger.error(f"QBO: Sheets token write failed ({e}); only local file updated.")


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


_RECENT_TXN_QUERY = (
    "SELECT TxnDate FROM Purchase "
    "ORDER BY TxnDate DESC MAXRESULTS 1"
)


def fetch_most_recent_txn_date():
    """Return the date string of the most recent Purchase transaction posted
    to QBO. Useful as a 'books current through' freshness signal — if it's
    days old, the books are lagging behind the bank feed.
    """
    path = f"/query?query={requests.utils.quote(_RECENT_TXN_QUERY)}&minorversion=70"
    data = _api_get(path)
    if not data:
        return None
    purchases = ((data.get("QueryResponse") or {}).get("Purchase") or [])
    if not purchases:
        return None
    return purchases[0].get("TxnDate")


# ---------------------------------------------------------------------------
# Profit & Loss report (monthly revenue) — powers the revenue-projection forecast
# ---------------------------------------------------------------------------

def _pnl_basis():
    """Accounting basis for the P&L report. Accrual matches revenue to when the
    work was done/invoiced (how the Jobber committed layer counts it), so it's
    the default for a forward revenue forecast. Override with QBO_PNL_BASIS."""
    basis = (os.getenv("QBO_PNL_BASIS") or "Accrual").strip().capitalize()
    return "Cash" if basis == "Cash" else "Accrual"


def fetch_pnl_report(start_date, end_date, accounting_method=None):
    """Raw QBO ProfitAndLoss report summarized by month over [start_date, end_date]
    (both 'YYYY-MM-DD'). Returns the parsed report dict, or None on auth/API failure."""
    params = {
        "start_date": start_date,
        "end_date": end_date,
        "summarize_column_by": "Month",
        "accounting_method": accounting_method or _pnl_basis(),
        "minorversion": "70",
    }
    return _api_get(f"/reports/ProfitAndLoss?{urlencode(params)}")


def _num(value):
    """QBO money cell -> float. Blank/None/garbage -> 0.0."""
    if value is None:
        return 0.0
    s = str(value).replace(",", "").replace("$", "").strip()
    if not s:
        return 0.0
    try:
        return float(s)
    except ValueError:
        return 0.0


def _month_key_from_column(col):
    """Map a report column to 'YYYY-MM', or None for non-month columns (the
    leading account column and the trailing 'Total' column have no StartDate)."""
    for md in (col.get("MetaData") or []):
        if md.get("Name") == "StartDate" and md.get("Value"):
            return md["Value"][:7]
    title = (col.get("ColTitle") or "").strip()
    if title:
        try:
            from datetime import datetime as _dt
            return _dt.strptime(title, "%b %Y").strftime("%Y-%m")
        except ValueError:
            return None
    return None


def _income_summary_row(report):
    """The 'Total Income' summary ColData for the report's Income section, index-
    aligned with the columns. None if no Income section is present."""
    rows = ((report.get("Rows") or {}).get("Row")) or []
    for r in rows:
        header0 = (((r.get("Header") or {}).get("ColData") or [{}])[0]
                   .get("value") or "").strip().lower()
        if r.get("group") == "Income" or header0 == "income":
            summ = (r.get("Summary") or {}).get("ColData")
            if summ:
                return summ
    return None


def fetch_monthly_revenue(start_date, end_date, accounting_method=None):
    """Live monthly Total Income from QuickBooks as {'YYYY-MM': revenue}.

    Pulls the ProfitAndLoss report summarized by month and reads the Income
    section's 'Total Income' line per month column. Returns None on auth/API
    failure or if the report has no recognizable Income section, so callers can
    fall back to another revenue source.
    """
    report = fetch_pnl_report(start_date, end_date, accounting_method)
    if not report:
        return None
    cols = ((report.get("Columns") or {}).get("Column")) or []
    idx_to_month = {}
    for i, col in enumerate(cols):
        mk = _month_key_from_column(col)
        if mk:
            idx_to_month[i] = mk
    summary = _income_summary_row(report)
    if summary is None:
        logger.error("QBO P&L: no Income section found in report")
        return None
    out = {}
    for i, mk in idx_to_month.items():
        if i < len(summary):
            out[mk] = round(_num(summary[i].get("value")), 2)
    return out


def fetch_account_balances():
    """Return list of dicts:
        {id, name, type, subtype, balance, currency}
    for all active Bank and Credit Card accounts.

    Bank balances: positive = cash on hand.
    Credit Card balances: QBO returns these as negative when there's debt owed
    (e.g. Capital One Spark balance of -$11,890 means you owe $11,890).
    The sign is normalized in summarize_balances() so cc_total is a positive
    number representing total debt.
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
        return None  # signal auth/API failure (distinct from empty result)

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


def summarize_balances(accounts, excluded_ids=None, overrides=None):
    """Split balances into cash (Bank) and credit card debt.

    excluded_ids: optional iterable of QBO account IDs to skip (e.g. personal
    cards mixed in with business accounts in QBO).
    overrides: optional dict {account_id_str: {balance, as_of, note}} to pin
    real bank-balance values per account (works around the API only exposing
    posted/ledger balances). Override 'balance' is in human sign convention:
    positive for Bank cash, positive for CC debt.

    Output:
      cash_total = sum of bank balances (positive = cash on hand)
      cc_total   = total credit card debt as a POSITIVE number.
                   QBO returns CC debt as negative balances, so we flip the
                   sign: a -$11,890 balance becomes +$11,890 of debt.
    """
    excluded = set(str(x) for x in (excluded_ids or []))
    overrides = overrides or {}
    cash_total = 0.0
    cc_total = 0.0
    cash_accounts = []
    cc_accounts = []
    for a in accounts:
        acc_id = str(a.get("id"))
        if acc_id in excluded:
            continue

        override = overrides.get(acc_id)
        is_override = False
        if override and "balance" in override:
            balance = round(float(override["balance"]), 2)
            account = {
                **a,
                "balance": balance,
                "is_override": True,
                "override_as_of": override.get("as_of"),
                "override_note": override.get("note", ""),
            }
            is_override = True
        else:
            account = a

        if a["type"] == "Bank":
            if not is_override:
                account = {**a, "is_override": False}
            cash_total += account["balance"]
            cash_accounts.append(account)
        elif a["type"] == "Credit Card":
            if not is_override:
                # Flip sign so debt is positive
                flipped = round(-a["balance"], 2)
                account = {**a, "balance": flipped, "is_override": False}
            cc_total += account["balance"]
            cc_accounts.append(account)
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
        from financial.config import qbo_settings as _qbo_cfg
        tokens = load_tokens()
        if not tokens.get("access_token") or not tokens.get("realm_id"):
            return jsonify({
                "error": "QBO not authorized",
                "message": "No QBO tokens found. Visit /qbo/login to authorize. "
                           "Note: Railway redeploys wipe the local token file. "
                           "Set QBO_TOKEN_STORE env var to persist across deploys.",
            }), 400
        try:
            api_cfg = (_qbo_cfg().get("api") or {})
            excluded = api_cfg.get("excluded_account_ids") or []
            overrides = api_cfg.get("balance_overrides") or {}
            accounts = fetch_account_balances()
            return jsonify({
                "accounts": accounts,
                "summary": summarize_balances(
                    accounts, excluded_ids=excluded, overrides=overrides
                ),
                "excluded_account_ids": excluded,
                "balance_overrides_active": list(overrides.keys()),
            })
        except Exception as e:
            logger.exception("QBO test failed")
            return jsonify({"error": "QBO test failed", "details": str(e)}), 500

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
