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


def _money(text, n=20):
    out = []
    for m in re.finditer(r"\$\s?[\d,]+(?:\.\d{1,2})?|\b[\d,]+\.\d{2}\b", text):
        out.append(text[max(0, m.start() - 45):m.end() + 25].strip())
    return out[:n]


def fetch_lurvey_debug():
    """Probe the dashboard + orders page to find where the balance lives."""
    s = _login_session()
    dash = s.get(_ACCOUNT, timeout=30).text
    logged_in = ("customer-logout" in dash) or ("/my-account/orders" in dash) or ("log out" in dash.lower())

    # AJAX / REST hints (the balance may be fetched client-side).
    ajax = sorted(set(re.findall(r'(admin-ajax\.php|/wp-json/[a-z0-9/_-]+)', dash, re.I)))[:15]
    # Embedded scripts mentioning balance/account/credit.
    scripts = []
    for m in re.finditer(r"(balance|account_balance|store_credit|amount_due|open_balance)", dash, re.I):
        scripts.append(dash[max(0, m.start() - 40):m.end() + 80])
    scripts = scripts[:10]

    # Dump the JS config block around the credit/balance labels so we can find
    # the ajax action, nonce, and url the page uses to load the numbers.
    cfg = ""
    mk = re.search(r"Credit Limit", dash)
    if mk:
        cfg = dash[max(0, mk.start() - 1400):mk.end() + 1400]
    # Also surface any nonce-looking and action-looking fields near it.
    nonces = re.findall(r"(nonce|action|ajax_url|ajaxurl|security)\s*[:=]\s*['\"]?([\w\-/.]+)", cfg)

    out = {
        "logged_in": logged_in,
        "dash_money": _money(_text(dash)),
        "ajax_hints": ajax,
        "balance_script_hits": scripts,
        "config_block": cfg,
        "config_fields": nonces[:30],
    }

    # Orders page: unpaid order totals could be the AP balance.
    try:
        orders_html = s.get(_BASE + "/my-account/orders/", timeout=30).text
        otext = _text(orders_html)
        out["orders_money"] = _money(otext)
        out["orders_statuses"] = sorted(set(re.findall(r"\b(Pending(?: payment)?|Processing|On hold|Completed|Cancelled|Refunded|Failed|Open|Unpaid|Awaiting)\b", otext, re.I)))[:12]
        # quick row context around "Total"/"View"
        rows = []
        for m in re.finditer(r"(Order|#\d{3,})[^$]{0,120}\$\s?[\d,]+(?:\.\d{2})?", otext):
            rows.append(m.group(0).strip()[:160])
        out["orders_rows"] = rows[:12]
    except Exception as e:
        out["orders_error"] = str(e)[:150]

    return out
