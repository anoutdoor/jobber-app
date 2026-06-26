"""Daily cash-position email, built from the cash tracker's synced state.

At send time it re-pulls the live auto numbers (bank, payroll, A/R, payouts,
Lurvey), merges them onto the manual entries saved in the _cashstate tab,
recomputes the position with the same math the app uses, and emails it.
"""
import copy
import logging
from datetime import date, datetime

import pytz

from financial.cash_state import load_cash_state
from financial.cash_position import compute_cash_position, fixed_overhead_due, _num
from financial.config import email_settings
from financial.gmail_send import send_email

logger = logging.getLogger(__name__)
CT = pytz.timezone("America/Chicago")


def _refresh_autos(state):
    """Re-pull each enabled auto source onto a copy of the saved state. Each
    source is independent: a failure leaves that source's last-saved value in
    place and is reported in `errors`."""
    errors = {}
    today = date.today()

    bs = state.get("bankSync") or {}
    if bs.get("connected"):
        try:
            from financial.plaid_client import fetch_plaid_balances
            accounts = fetch_plaid_balances()
            if accounts:
                bs["accounts"] = accounts
                bs["fetchedAt"] = datetime.now(CT).isoformat()
                state["bankSync"] = bs
            else:
                errors["bank"] = "no live balances (using last saved)"
        except Exception as e:
            logger.exception("cash digest: bank refresh failed")
            errors["bank"] = str(e)[:120]

    pa = state.get("payrollAuto") or {}
    if pa.get("enabled") and pa.get("source") == "jobber":
        try:
            from financial.compute import compute_payroll_accrual
            p = compute_payroll_accrual(today)
            j = pa.get("jobber") or {}
            j.update({
                "total": p.get("total_accrual", 0),
                "grossPay": p.get("gross_pay", 0),
                "taxBurden": p.get("tax_burden", 0),
                "weekStart": p.get("week_start", ""),
                "asOf": p.get("as_of", ""),
                "breakdown": p.get("breakdown", []),
                "missingRates": p.get("missing_rates", []),
            })
            pa["jobber"] = j
            state["payrollAuto"] = pa
        except Exception as e:
            logger.exception("cash digest: payroll refresh failed")
            errors["payroll"] = str(e)[:120]

    ar = state.get("ar") or {}
    auto = ar.get("auto") or {}
    if auto.get("enabled"):
        try:
            from financial.compute import compute_ar
            from financial.jobber_finance import fetch_open_invoices
            invoices = fetch_open_invoices()
            if invoices is None:
                errors["ar"] = "Jobber auth (using last saved)"
            else:
                a = compute_ar(invoices)
                j = auto.get("jobber") or {}
                j["total"] = a.get("total_outstanding", 0)
                j["invoiceCount"] = len(a.get("invoices", []))
                j["asOf"] = today.isoformat()
                auto["jobber"] = j
                ar["auto"] = auto
                state["ar"] = ar
        except Exception as e:
            logger.exception("cash digest: AR refresh failed")
            errors["ar"] = str(e)[:120]

    ip = state.get("incomingPayouts") or {}
    if ip.get("enabled"):
        try:
            from financial.payouts import fetch_upcoming_payouts
            po = fetch_upcoming_payouts()
            if po:
                ip["total"] = po.get("total_net", 0)
                ip["count"] = po.get("count", 0)
                ip["payouts"] = po.get("payouts", [])
                state["incomingPayouts"] = ip
        except Exception as e:
            logger.exception("cash digest: payouts refresh failed")
            errors["payouts"] = str(e)[:120]

    la = state.get("lurveyAuto") or {}
    if la.get("enabled"):
        try:
            from financial.lurvey import fetch_lurvey_balance
            lb = fetch_lurvey_balance()
            if lb:
                la["balance"] = lb.get("balance", 0)
                la["creditLimit"] = lb.get("credit_limit", 0)
                la["creditAvailable"] = lb.get("credit_available", 0)
                la["accountId"] = lb.get("account_id", "")
                state["lurveyAuto"] = la
            else:
                errors["lurvey"] = "login/config failed (using last saved)"
        except Exception as e:
            logger.exception("cash digest: lurvey refresh failed")
            errors["lurvey"] = str(e)[:120]

    return state, errors


# --- rendering ---------------------------------------------------------------

def _money(v, cents=False):
    v = _num(v)
    neg = v < 0
    amt = abs(v)
    body = format(amt, ",.2f") if cents else format(round(amt), ",")
    return ("-$" if neg else "$") + body


_STATUS = {
    "green": {"bg": "#ecfdf5", "border": "#a7f3d0", "dot": "#059669", "text": "#065f46", "label": "Looking good"},
    "yellow": {"bg": "#fffbeb", "border": "#fde68a", "dot": "#d97706", "text": "#92400e", "label": "A bit tight"},
    "red": {"bg": "#fef2f2", "border": "#fecaca", "dot": "#dc2626", "text": "#991b1b", "label": "Careful"},
}


def _status_thresholds():
    try:
        from financial.config import load_config
        cs = (load_config() or {}).get("cash_status") or {}
    except Exception:
        cs = {}
    return float(cs.get("green_min", 10000)), float(cs.get("yellow_min", 0))


def _status_for(pos):
    green_min, yellow_min = _status_thresholds()
    month = pos["freeCashMonth"]
    if month < yellow_min:
        return "red"
    if month < green_min:
        return "yellow"
    return "green"


def _trend(cur, prev):
    if prev is None:
        return ""
    d = cur - prev
    if abs(d) < 1:
        return ' &nbsp;<span style="color:#94a3b8;font-size:13px;font-weight:400;">no change from yesterday</span>'
    if d > 0:
        return f' &nbsp;<span style="color:#0f766e;font-size:13px;font-weight:400;">&#9650; {_money(d)} from yesterday</span>'
    return f' &nbsp;<span style="color:#b91c1c;font-size:13px;font-weight:400;">&#9660; {_money(abs(d))} from yesterday</span>'


def _verdict_sentence(pos, status):
    safe = pos["freeCash"]
    month = pos["freeCashMonth"]
    lead = {
        "green": "You're in good shape.",
        "yellow": "Doing okay, but keep an eye on it.",
        "red": "Heads up, money is tight.",
    }[status]
    s = f"{lead} About <b>{_money(safe)}</b> is safe to spend right now, after setting aside the bills coming up. "
    if month >= safe:
        s += f"More money is landing than going out this month, so your cushion grows to about <b>{_money(month)}</b> by month-end."
    elif month >= 0:
        s += f"After this month's bills, you're left with about <b>{_money(month)}</b>."
    else:
        s += f"This month's bills run you about <b>{_money(abs(month))}</b> short, so hold off on big spending."
    return s


def render_cash_email(pos, overhead_due, state, today, refresh_errors, prev=None):
    fetched = datetime.now(CT).strftime("%b %-d, %-I:%M %p")
    date_str = today.strftime("%A, %b %-d, %Y")
    status = _status_for(pos)
    sc = _STATUS[status]
    money_owed_to_you = pos["ar"]["total"] + pos["incomingPayouts"]

    p = []
    p.append('<div style="font-family:-apple-system,Segoe UI,Roboto,Arial,sans-serif;max-width:620px;margin:0 auto;color:#0f172a;">')
    p.append(
        f'<div style="color:#64748b;font-size:13px;margin-bottom:12px;">A&amp;N cash check &nbsp;&middot;&nbsp; {date_str}</div>'
    )

    # Status banner + plain-English verdict
    p.append(
        f'<div style="background:{sc["bg"]};border:1px solid {sc["border"]};border-radius:12px;padding:14px 16px;">'
        f'<div style="font-size:14px;font-weight:700;color:{sc["text"]};margin-bottom:4px;">'
        f'<span style="display:inline-block;width:10px;height:10px;border-radius:50%;background:{sc["dot"]};margin-right:7px;"></span>'
        f'{sc["label"]}</div>'
        f'<div style="font-size:14px;line-height:1.5;color:#1f2937;">{_verdict_sentence(pos, status)}</div>'
        f'</div>'
    )

    # Hero: safe to spend now + cushion by month-end
    prev_free = prev.get("free_cash") if prev else None
    free_color = "#0f766e" if pos["freeCash"] >= 0 else "#b91c1c"
    month_color = "#0f766e" if pos["freeCashMonth"] >= 0 else "#b91c1c"
    p.append('<div style="margin:16px 0 4px;">')
    p.append(
        f'<div style="font-size:13px;color:#64748b;">Safe to spend right now</div>'
        f'<div style="font-size:34px;font-weight:700;color:{free_color};line-height:1.1;">'
        f'{_money(pos["freeCash"])}{_trend(pos["freeCash"], prev_free)}</div>'
    )
    p.append(
        f'<div style="font-size:13px;color:#64748b;margin-top:10px;">Cushion by month-end '
        f'(after this month&#39;s bills and money coming in)</div>'
        f'<div style="font-size:20px;font-weight:700;color:{month_color};">{_money(pos["freeCashMonth"])}</div>'
    )
    p.append('</div>')

    # The quick picture: have + owed to you - coming up = worth
    p.append('<h3 style="font-size:13px;text-transform:uppercase;letter-spacing:.04em;color:#475569;margin:22px 0 6px;">The quick picture</h3>')
    p.append('<table cellpadding="0" cellspacing="0" style="width:100%;border-collapse:collapse;font-size:14px;">')
    p.append(_row("Cash you have right now", _money(pos["totalLiquid"])))
    p.append(_row("Plus money owed to you (customers + payouts)", "+ " + _money(money_owed_to_you)))
    p.append(_row("Minus bills &amp; debts coming up", "&#8722; " + _money(pos["totalOwed"])))
    p.append(_row("<b>What the business is worth in cash</b>", "<b>" + _money(pos["netPosition"]) + "</b>", top=True))
    p.append('</table>')

    # What you owe soon (plain labels)
    label_map = {"Vendor AP": "Suppliers you owe", "Materials / POs": "Materials &amp; open orders",
                 "Owner pay": "Owner pay", "Payroll": "Payroll (crew wages)", "Taxes": "Taxes"}
    ob_rows = [(label_map.get(c["label"], c["label"]), _money(c["total"])) for c in pos["obligationCategories"] if c["total"]]
    ob_rows.append(("<b>Total bills coming up</b>", "<b>" + _money(pos["totalObligations"]) + "</b>"))
    p.append(_section("What you owe soon", ob_rows))

    # Money coming in
    in_rows = [("Customers owe you", _money(pos["ar"]["total"]))]
    if pos["incomingPayouts"]:
        in_rows.append((f'Jobber payouts landing soon ({pos["payoutCount"]})', _money(pos["incomingPayouts"])))
    p.append(_section("Money coming in", in_rows))

    # Payroll detail
    ap = pos.get("autoPayroll")
    if ap and ap.get("source") == "jobber":
        crew = len(ap.get("breakdown") or [])
        ws = ap.get("weekStart") or ""
        detail = f"This week so far ({crew} on the crew)" + (f", since {ws}" if ws else "")
        p.append(_section("Payroll owed", [(detail, _money(ap["total"]))]))

    # Lurvey
    lv = pos.get("lurvey")
    if lv:
        p.append(_section("Lurvey account", [
            ("You owe them", _money(lv["balance"])),
            ("Credit you have left", _money(lv["creditAvailable"])),
        ]))

    # Fixed bills still due this month
    od = overhead_due or {}
    if od.get("totalDue") or od.get("items"):
        ov_rows = [(f'{it["description"]} (the {it["day"]}th)', _money(it["amount"]))
                   for it in (od.get("items") or [])[:8]]
        ov_rows.append(("<b>Total still due this month</b>", "<b>" + _money(od.get("totalDue", 0)) + "</b>"))
        p.append(_section("Fixed bills still due this month", ov_rows))

    if refresh_errors:
        notes = "; ".join(f"{k}: {v}" for k, v in refresh_errors.items())
        p.append(
            f'<div style="margin-top:18px;padding:10px 12px;background:#fef3c7;border:1px solid #fde68a;'
            f'border-radius:8px;font-size:12px;color:#92400e;">Could not refresh live: {notes}. Showing last saved for those.</div>'
        )

    p.append(
        f'<div style="margin-top:22px;color:#94a3b8;font-size:11px;">Numbers from your cash tracker, '
        f'live pulls refreshed {fetched} CT. Open the tracker to adjust anything entered by hand.</div>'
    )
    p.append('</div>')

    es = email_settings()
    prefix = es.get("subject_prefix") or "A&N Cash"
    light = {"green": "OK", "yellow": "tight", "red": "careful"}[status]
    subject = f'{prefix} {today.strftime("%b %-d")} ({light}): {_money(pos["freeCash"])} safe to spend'
    return subject, "".join(p)


def _row(label, value, top=False):
    border = "border-top:2px solid #e2e8f0;" if top else "border-bottom:1px solid #f1f5f9;"
    return (
        f'<tr><td style="padding:7px 0;{border}color:#334155;">{label}</td>'
        f'<td style="padding:7px 0;{border}text-align:right;font-variant-numeric:tabular-nums;color:#0f172a;">{value}</td></tr>'
    )


def _section(title, rows):
    out = [f'<h3 style="font-size:13px;text-transform:uppercase;letter-spacing:.04em;color:#475569;margin:22px 0 6px;">{title}</h3>']
    out.append('<table cellpadding="0" cellspacing="0" style="width:100%;border-collapse:collapse;font-size:14px;">')
    for label, value in rows:
        out.append(_row(label, value))
    out.append('</table>')
    return "".join(out)


def run_cash_digest(today=None, dry_run=False):
    """Build the cash-position email from the synced tracker state and send it.
    dry_run=True returns the rendered HTML without sending."""
    today = today or date.today()
    cash = load_cash_state()
    if not cash or not isinstance(cash, dict) or not cash.get("current"):
        msg = "No cash tracker state saved yet (open the tracker once so it syncs)."
        logger.warning("cash digest: %s", msg)
        return {"status": "error", "message": msg}

    state = copy.deepcopy(cash["current"])
    state, refresh_errors = _refresh_autos(state)

    pos = compute_cash_position(state, today)
    overhead_due = fixed_overhead_due(state.get("overhead"), today)

    # Day-over-day movement (best effort).
    prev = None
    try:
        from financial.cash_digest_log import last_entry
        prev = last_entry(before_date=today.isoformat())
    except Exception:
        logger.exception("cash digest: prior lookup failed")

    subject, html = render_cash_email(pos, overhead_due, state, today, refresh_errors, prev=prev)

    if dry_run:
        return {"status": "dry_run", "subject": subject, "html": html, "errors": refresh_errors}

    es = email_settings()
    recipients = es.get("recipients") or ([es["recipient"]] if es.get("recipient") else [])
    if not recipients:
        return {"status": "error", "message": "No recipients configured"}
    to_header = ", ".join(recipients)
    result = send_email(to_header, subject, html)
    if result:
        try:
            from financial.cash_digest_log import log_today
            log_today(today.isoformat(), pos["netPosition"], pos["freeCash"],
                      pos["freeCashMonth"], pos["totalLiquid"])
        except Exception:
            logger.exception("cash digest: log write failed")
        return {"status": "ok", "subject": subject, "to": to_header, "message_id": result.get("id")}
    return {"status": "error", "message": "Gmail send failed (see logs)"}
