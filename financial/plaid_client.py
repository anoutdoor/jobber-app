"""Plaid live bank balances for the cash position tracker.

Uses Plaid Hosted Link so Plaid hosts the whole connect flow (including OAuth
banks like Chase) on their own page; we just open the hosted URL and poll for
the result. Balances come from /accounts/balance/get, which hits the bank in
real time. Access token is stored in the Sheets vault (provider "plaid").

Env: PLAID_CLIENT_ID, PLAID_SECRET, PLAID_ENV (production | sandbox).
"""
import json
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


def load_plaid_items():
    """Stored connections, one per bank, as {access_token, institution_id}.
    Tolerates legacy shapes (a JSON list of bare tokens, or a single token)."""
    raw = (read_tokens(_PROVIDER) or {}).get("access_token") or ""
    if not raw:
        return []
    try:
        v = json.loads(raw)
    except (ValueError, TypeError):
        return [{"access_token": raw, "institution_id": ""}]
    if isinstance(v, str) and v:
        return [{"access_token": v, "institution_id": ""}]
    out = []
    if isinstance(v, list):
        for x in v:
            if isinstance(x, dict) and x.get("access_token"):
                out.append({"access_token": x["access_token"], "institution_id": x.get("institution_id") or ""})
            elif isinstance(x, str) and x:
                out.append({"access_token": x, "institution_id": ""})
    return out


def store_plaid_items(items):
    write_tokens(_PROVIDER, {"access_token": json.dumps(items)})


def _institution_id(access_token):
    try:
        r = _post("/item/get", {"access_token": access_token})
        return ((r.get("item") or {}).get("institution_id")) or ""
    except Exception:
        logger.exception("plaid /item/get failed")
        return ""


def create_hosted_link():
    """Create a Hosted Link session. Returns {link_token, hosted_link_url}."""
    res = _post(
        "/link/token/create",
        {
            "user": {"client_user_id": "anos-cash"},
            "client_name": "A&N Cash Position",
            # Balance is auto-included with any product; "transactions" is the
            # most universally supported anchor. We only ever read live balances.
            "products": ["transactions"],
            "country_codes": ["US"],
            "language": "en",
            "hosted_link": {},
        },
    )
    return {"link_token": res.get("link_token"), "hosted_link_url": res.get("hosted_link_url")}


def _extract_public_tokens(res):
    """Every public token in a finished Hosted Link session (one per bank)."""
    tokens = []
    results = res.get("results") or {}
    for a in results.get("item_add_results") or []:
        if a.get("public_token"):
            tokens.append(a["public_token"])
    # Defensive fallbacks for shape variation.
    for s in res.get("link_sessions") or []:
        if s.get("public_token"):
            tokens.append(s["public_token"])
        inner = s.get("results") or {}
        for a in inner.get("item_add_results") or []:
            if a.get("public_token"):
                tokens.append(a["public_token"])
    return list(dict.fromkeys(tokens))


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
    """Live balances across every connected bank. None if nothing is connected."""
    items = load_plaid_items()
    if not items:
        return None
    accounts = []
    for it in items:
        try:
            data = _post("/accounts/balance/get", {"access_token": it["access_token"]})
            accounts.extend(_norm_account(a) for a in (data.get("accounts") or []))
        except Exception:
            logger.exception("plaid balance fetch failed for one item; skipping it")
    return accounts


def complete_link(link_token):
    """Poll a Hosted Link session; when finished, exchange the public token(s)
    and MERGE the bank(s) into the stored set, one entry per institution (newest
    wins). Connecting banks across separate sessions accumulates instead of
    replacing, and reconnecting a bank refreshes it rather than duplicating.
    Returns (status, accounts).
    """
    res = _post("/link/token/get", {"link_token": link_token})
    public_tokens = _extract_public_tokens(res)
    if not public_tokens:
        return "pending", []

    items = load_plaid_items()
    # Backfill institution ids on legacy entries so dedup is reliable.
    for it in items:
        if not it.get("institution_id") and it.get("access_token"):
            it["institution_id"] = _institution_id(it["access_token"])

    by_inst = {it["institution_id"]: it for it in items if it.get("institution_id")}
    no_inst = [it for it in items if not it.get("institution_id")]

    for pt in public_tokens:
        try:
            exchanged = _post("/item/public_token/exchange", {"public_token": pt})
            at = exchanged.get("access_token")
            if not at:
                continue
            inst = _institution_id(at)
            entry = {"access_token": at, "institution_id": inst}
            if inst:
                by_inst[inst] = entry
            else:
                no_inst.append(entry)
        except Exception:
            logger.exception("plaid exchange failed for one public_token; skipping")

    store_plaid_items(list(by_inst.values()) + no_inst)
    accounts = fetch_plaid_balances() or []
    return "connected", accounts
