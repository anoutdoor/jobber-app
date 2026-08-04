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
    """Stored connections as {access_token, fingerprint}. The fingerprint
    (institution + account masks) distinguishes different logins at the SAME
    bank, while a reconnected login keeps the same fingerprint and refreshes.
    Tolerates legacy shapes (bare token list, single token, old institution_id)."""
    raw = (read_tokens(_PROVIDER) or {}).get("access_token") or ""
    if not raw:
        return []
    try:
        v = json.loads(raw)
    except (ValueError, TypeError):
        return [{"access_token": raw, "fingerprint": ""}]
    if isinstance(v, str) and v:
        return [{"access_token": v, "fingerprint": ""}]
    out = []
    if isinstance(v, list):
        for x in v:
            if isinstance(x, dict) and x.get("access_token"):
                out.append({"access_token": x["access_token"], "fingerprint": x.get("fingerprint") or ""})
            elif isinstance(x, str) and x:
                out.append({"access_token": x, "fingerprint": ""})
    return out


def store_plaid_items(items):
    write_tokens(_PROVIDER, {"access_token": json.dumps(items)})


def _fingerprint_for(access_token):
    """Identity of a connection: institution + its sorted account masks. Two
    different logins at one bank differ (different masks); the same login
    reconnected matches, so it refreshes instead of duplicating."""
    try:
        data = _post("/accounts/balance/get", {"access_token": access_token})
    except Exception:
        logger.exception("plaid fingerprint fetch failed")
        return ""
    inst = ((data.get("item") or {}).get("institution_id")) or ""
    masks = sorted((a.get("mask") or a.get("account_id") or "") for a in (data.get("accounts") or []))
    return inst + "|" + ",".join(masks)


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


def _norm_transaction(t, accounts_by_id):
    """Flatten a Plaid transaction to what the fuel tracker needs: identity,
    which account/card, when, where, and how much. Plaid amounts are positive
    for money OUT, which is what a spend tracker wants, so pass through as-is."""
    acct = accounts_by_id.get(t.get("account_id")) or {}
    pfc = t.get("personal_finance_category") or {}
    loc = t.get("location") or {}
    return {
        "id": t.get("transaction_id") or "",
        "accountId": t.get("account_id") or "",
        "accountMask": acct.get("mask") or "",
        "accountName": acct.get("name") or acct.get("official_name") or "",
        "accountType": acct.get("type") or "",
        "accountSubtype": acct.get("subtype") or "",
        "date": t.get("date") or "",
        "authorizedDate": t.get("authorized_date") or "",
        "datetime": t.get("datetime") or "",
        "name": t.get("name") or "",
        "merchant": t.get("merchant_name") or "",
        "amount": _to_float(t.get("amount")),
        "currency": t.get("iso_currency_code") or "USD",
        "pending": bool(t.get("pending")),
        "accountOwner": t.get("account_owner") or "",
        "channel": t.get("payment_channel") or "",
        "categoryPrimary": pfc.get("primary") or "",
        "categoryDetailed": pfc.get("detailed") or "",
        "lat": loc.get("lat"),
        "lon": loc.get("lon"),
        "address": loc.get("address") or "",
        "city": loc.get("city") or "",
    }


def fetch_plaid_transactions(days=35):
    """Transactions across every connected bank for the last `days` days,
    newest first. Stateless date-range pull (/transactions/get) so the caller
    can upsert idempotently by transaction id; no cursor to persist. None if
    nothing is connected."""
    from datetime import date, timedelta

    items = load_plaid_items()
    if not items:
        return None
    days = max(1, min(int(days or 35), 90))
    start = (date.today() - timedelta(days=days)).isoformat()
    end = date.today().isoformat()
    out = []
    for it in items:
        try:
            offset = 0
            while True:
                data = _post(
                    "/transactions/get",
                    {
                        "access_token": it["access_token"],
                        "start_date": start,
                        "end_date": end,
                        "options": {"count": 500, "offset": offset, "include_personal_finance_category": True},
                    },
                )
                accounts_by_id = {a.get("account_id"): a for a in (data.get("accounts") or [])}
                txns = data.get("transactions") or []
                out.extend(_norm_transaction(t, accounts_by_id) for t in txns)
                offset += len(txns)
                if not txns or offset >= int(data.get("total_transactions") or 0):
                    break
        except Exception:
            logger.exception("plaid transactions fetch failed for one item; skipping it")
    out.sort(key=lambda t: (t["date"], t["id"]), reverse=True)
    return out


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
    # Backfill fingerprints on legacy entries so dedup is reliable.
    for it in items:
        if not it.get("fingerprint") and it.get("access_token"):
            it["fingerprint"] = _fingerprint_for(it["access_token"])

    by_fp = {it["fingerprint"]: it for it in items if it.get("fingerprint")}
    no_fp = [it for it in items if not it.get("fingerprint")]

    for pt in public_tokens:
        try:
            exchanged = _post("/item/public_token/exchange", {"public_token": pt})
            at = exchanged.get("access_token")
            if not at:
                continue
            fp = _fingerprint_for(at)
            entry = {"access_token": at, "fingerprint": fp}
            if fp:
                by_fp[fp] = entry
            else:
                no_fp.append(entry)
        except Exception:
            logger.exception("plaid exchange failed for one public_token; skipping")

    store_plaid_items(list(by_fp.values()) + no_fp)
    accounts = fetch_plaid_balances() or []
    return "connected", accounts
