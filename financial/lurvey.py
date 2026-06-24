"""Scrape the Lurvey's account balance from lurveys.com (WooCommerce, no API).

Logs in with credentials stored in env vars (LURVEY_USER / LURVEY_PASS) using
the WooCommerce my-account login form (fetch the nonce, post the form), then
reads the account page. The exact balance location isn't known yet, so
fetch_lurvey_debug() returns dollar amounts + keyword context to locate it;
fetch_lurvey_balance() parses it once the selector is confirmed.
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
    nonce = m.group(1) if m else ""
    s.post(
        _ACCOUNT,
        data={
            "username": user,
            "password": pw,
            "woocommerce-login-nonce": nonce,
            "_wp_http_referer": "/my-account/",
            "login": "Log in",
        },
        timeout=30,
    )
    return s


def _text(html):
    t = re.sub(r"<(script|style)[^>]*>.*?</\1>", " ", html, flags=re.I | re.S)
    t = re.sub(r"<[^>]+>", " ", t)
    return re.sub(r"\s+", " ", t).strip()


def fetch_lurvey_debug():
    """Logged-in account page, summarized so we can find where the balance is."""
    s = _login_session()
    html = s.get(_ACCOUNT, timeout=30).text
    logged_in = ("customer-logout" in html) or ("/my-account/orders" in html) or ("log out" in html.lower())

    # The my-account navigation links (reveal sub-pages like statement/invoices).
    menu, seen = [], set()
    for m in re.finditer(r'href="([^"]*?/my-account/[^"]*)"[^>]*>\s*([^<]{0,60})', html):
        href = m.group(1)
        txt = re.sub(r"\s+", " ", m.group(2)).strip()
        if href not in seen:
            seen.add(href)
            menu.append({"text": txt or "(no text)", "href": href})

    text = _text(html)
    money = []
    for m in re.finditer(r"\$\s?[\d,]+(?:\.\d{1,2})?|\b[\d,]+\.\d{2}\b", text):
        money.append(text[max(0, m.start() - 50):m.end() + 20].strip())
    keywords = []
    for m in re.finditer(r"(balance|credit|owe|amount due|outstanding|statement|invoice|ledger|account funds|past due|terms)", text, re.I):
        keywords.append(text[max(0, m.start() - 25):m.end() + 70].strip())
    return {
        "logged_in": logged_in,
        "account_menu": menu[:30],
        "money": money[:25],
        "keyword_context": keywords[:25],
        "page_chars": len(html),
    }
