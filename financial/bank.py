"""SimpleFIN bank-balance pull for the cash position tracker.

Turns a one-time SimpleFIN setup token into a durable access URL, then reads
account balances (read-only). The access URL is stored in the same Sheets
_tokens vault as the other provider tokens so it survives Railway redeploys.

Flow:
  setup token (base64)  -> decode -> claim URL
  POST claim URL        -> access URL (embeds https://user:pass@host/path)
  GET access URL/accounts?balances-only=1 -> balances
"""
import base64
import logging

import requests

from financial.token_persistence import read_tokens, write_tokens

logger = logging.getLogger(__name__)

_PROVIDER = "simplefin"


def claim_setup_token(setup_token):
    """Exchange a one-time setup token for a durable access URL."""
    claim_url = base64.b64decode((setup_token or "").strip()).decode("utf-8").strip()
    if not claim_url.lower().startswith("https://"):
        raise ValueError("Setup token did not decode to an https claim URL.")
    resp = requests.post(claim_url, timeout=30)
    resp.raise_for_status()
    access_url = resp.text.strip()
    if not access_url.lower().startswith("https://"):
        raise ValueError("Claim did not return a valid access URL.")
    return access_url


def store_access_url(access_url):
    write_tokens(_PROVIDER, {"access_token": access_url})


def load_access_url():
    return (read_tokens(_PROVIDER) or {}).get("access_token") or ""


def _to_float(v):
    try:
        return float(v)
    except (TypeError, ValueError):
        return 0.0


def _norm_account(a):
    org = a.get("org") or {}
    avail = a.get("available-balance")
    return {
        "id": a.get("id") or "",
        "org": org.get("name") or org.get("domain") or "",
        "name": a.get("name") or "",
        "currency": a.get("currency") or "USD",
        "balance": _to_float(a.get("balance")),
        "available": _to_float(avail if avail is not None else a.get("balance")),
        "balanceDate": a.get("balance-date") or 0,
    }


def fetch_accounts(access_url):
    """GET balances for every linked account. Returns (accounts, errors)."""
    url = access_url.rstrip("/") + "/accounts?balances-only=1"
    resp = requests.get(url, timeout=30)
    resp.raise_for_status()
    data = resp.json() if resp.content else {}
    accounts = [_norm_account(a) for a in (data.get("accounts") or [])]
    return accounts, (data.get("errors") or [])
