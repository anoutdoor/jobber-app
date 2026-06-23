"""Plaid live bank balances for the cash position tracker.

Uses Plaid Hosted Link so Plaid hosts the whole connect flow (including OAuth
banks like Chase) on their own page; we just open the hosted URL and poll for
the result. Balances come from /accounts/balance/get, which hits the bank in
real time. Access token is stored in the Sheets vault (provider "plaid").

Env: PLAID_CLIENT_ID, PLAID_SECRET, PLAID_ENV (production | sandbox).
"""
import logging
import os

import requests

from financial.token_persistence import read_tokens, write_tokens

logger = logging.getLogger(__name__)

_PROVIDER = "plaid"


def _base():
    env = (os.getenv("PLAID_ENV") or "production").strip().lower()
    return "https://sandbox.plaid.com" if env == "sandbox" else "https://production.plaid.com"


def _post(path, payload):
    cid = (os.getenv("PLAID_CLIENT_ID") or "").strip()
    sec = (os.getenv("PLAID_SECRET") or "").strip()
    if not cid or not sec:
        raise RuntimeError("Plaid keys not set (PLAID_CLIENT_ID / PLAID_SECRET).")
    r = requests.post(_base() + path, json={"client_id": cid, "secret": sec, **payload}, timeout=30)
    if not r.ok:
        try:
            e = r.json()
            raise RuntimeError(e.get("error_message") or e.get("error_code") or f"HTTP {r.status_code}")
        except ValueError:
            raise RuntimeError(f"HTTP {r.status_code}")
    return r.json()


def load_plaid_token():
    return (read_tokens(_PROVIDER) or {}).get("access_token") or ""


def store_plaid_token(access_token, item_id=""):
    write_tokens(_PROVIDER, {"access_token": access_token, "item_id": item_id})


def create_hosted_link():
    """Create a Hosted Link session. Returns {link_token, hosted_link_url}."""
    res = _post(
        "/link/token/create",
        {
            "user": {"client_user_id": "anos-cash"},
            "client_name": "A&N Cash Position",
            "products": ["balance"],
            "country_codes": ["US"],
            "language": "en",
            "hosted_link": {},
        },
    )
    return {"link_token": res.get("link_token"), "hosted_link_url": res.get("hosted_link_url")}


def _extract_public_token(res):
    results = res.get("results") or {}
    for a in results.get("item_add_results") or []:
        if a.get("public_token"):
            return a["public_token"]
    # Defensive fallbacks for shape variation.
    for s in res.get("link_sessions") or []:
        if s.get("public_token"):
            return s["public_token"]
        inner = s.get("results") or {}
        for a in inner.get("item_add_results") or []:
            if a.get("public_token"):
                return a["public_token"]
    return None


def _to_float(v):
    try:
        return float(v)
    except (TypeError, ValueError):
        return 0.0


def _norm_account(a):
    bal = a.get("balances") or {}
    typ = a.get("type")
    current = _to_float(bal.get("current"))
    mask = a.get("mask")
    name = a.get("name") or a.get("official_name") or "Account"
    if mask:
        name = f"{name} ••{mask}"
    if typ == "credit":
        balance = -abs(current)  # show owed as negative, matches the rest of the UI
        bucket = "credit"
    elif typ == "depository":
        balance = current
        bucket = "liquid"
    else:
        balance = current
        bucket = "ignore"
    return {
        "id": a.get("account_id") or "",
        "org": a.get("official_name") or (a.get("subtype") or "").title(),
        "name": name,
        "currency": bal.get("iso_currency_code") or "USD",
        "balance": balance,
        "available": balance,
        "balanceDate": 0,
        "bucket": bucket,
        "type": typ,
        "subtype": a.get("subtype"),
    }


def fetch_plaid_balances():
    """Live balances from the stored Plaid connection. Returns list of accounts."""
    token = load_plaid_token()
    if not token:
        return None
    data = _post("/accounts/balance/get", {"access_token": token})
    return [_norm_account(a) for a in (data.get("accounts") or [])]


def complete_link(link_token):
    """Poll a Hosted Link session. Returns (status, accounts):
      'pending'   -> user has not finished yet
      'connected' -> exchanged + stored, accounts returned
    """
    res = _post("/link/token/get", {"link_token": link_token})
    public_token = _extract_public_token(res)
    if not public_token:
        return "pending", []
    exchanged = _post("/item/public_token/exchange", {"public_token": public_token})
    access_token = exchanged.get("access_token")
    store_plaid_token(access_token, exchanged.get("item_id", ""))
    accounts = fetch_plaid_balances() or []
    return "connected", accounts
