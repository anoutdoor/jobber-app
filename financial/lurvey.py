"""Lurvey's AP balance. lurveys.com is WooCommerce with no API, but the account
page embeds a Modern Retail B2B widget whose account-summary data comes from a
plain JSON GET:

    {host}/data.php?hash=&provider=b2b_widget_account&channel=&contact_email=&contact_email_hash=

We log into Lurvey (WooCommerce nonce + form post) with LURVEY_USER/LURVEY_PASS,
read the widget config (host + hashes) fresh from the page so it self-heals if
those rotate, then call the account-summary endpoint and parse the balance.
"""
import logging
import os
import re

import requests

logger = logging.getLogger(__name__)

_BASE = "https://lurveys.com"
_ACCOUNT = _BASE + "/my-account/"
_UA = "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123 Safari/537.36"


def _login_session():
    user = (os.getenv("LURVEY_USER") or "").strip()
    pw = os.getenv("LURVEY_PASS") or ""
    if not user or not pw:
        raise RuntimeError("LURVEY_USER / LURVEY_PASS not set.")
    s = requests.Session()
    s.headers.update({"User-Agent": _UA})
    page = s.get(_ACCOUNT, timeout=30).text
    m = re.search(r'name="woocommerce-login-nonce"[^>]*value="([^"]+)"', page)
    s.post(
        _ACCOUNT,
        data={
            "username": user,
            "password": pw,
            "woocommerce-login-nonce": m.group(1) if m else "",
            "_wp_http_referer": "/my-account/",
            "login": "Log in",
        },
        timeout=30,
    )
    return s


def _widget_config(html):
    """Pull the Modern Retail widget config (host + hashes) from the page."""
    def g(key):
        m = re.search(r"\b" + re.escape(key) + r"\s*:\s*'([^']*)'", html)
        return m.group(1) if m else ""

    return {
        "host": g("host"),
        "hash": g("hash"),
        "channel": g("channel"),
        "contact_email": g("contact_email"),
        "contact_email_hash": g("contact_email_hash"),
    }


def _num(v):
    try:
        return round(float(v), 2)
    except (TypeError, ValueError):
        return 0.0


def fetch_lurvey_balance():
    """Returns Lurvey AP balance + credit info, or None on auth failure."""
    s = _login_session()
    html = s.get(_ACCOUNT, timeout=30).text
    cfg = _widget_config(html)
    if not cfg["host"] or not cfg["contact_email_hash"]:
        return None  # login failed or config not found
    r = requests.get(
        cfg["host"].rstrip("/") + "/data.php",
        params={
            "hash": cfg["hash"],
            "provider": "b2b_widget_account",
            "channel": cfg["channel"],
            "contact_email": cfg["contact_email"],
            "contact_email_hash": cfg["contact_email_hash"],
        },
        headers={"Origin": _BASE, "Referer": _ACCOUNT, "User-Agent": _UA},
        timeout=30,
    )
    r.raise_for_status()
    data = r.json()
    return {
        "account_id": data.get("id"),
        "balance": _num(data.get("balance")),
        "credit_limit": _num(data.get("credit_limit")),
        "credit_available": _num(data.get("credit_available")),
    }
