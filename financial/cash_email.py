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


def _card(label, value, accent):
    return (
        f'<td style="padding:10px 14px;background:#f8fafc;border:1px solid #e2e8f0;'
        f'border-radius:10px;" valign="top">'
        f'<div style="font-size:11px;letter-spacing:.04em;text-transform:uppercase;color:#64748b;">{label}</div>'
        f'<div style="font-size:22px;font-weight:700;color:{accent};margin-top:2px;">{value}</div>'
        f'</td>'
    )


def render_cash_email(pos, overhead_due, state, today, refresh_errors):
    pos_color = "#0f766e" if pos["netPosition"] >= 0 else "#b91c1c"
    free_color = "#0f766e" if pos["freeCash"] >= 0 else "#b91c1c"
    fetched = datetime.now(CT).strftime("%b %-d, %-I:%M %p")
    date_str = today.strftime("%A, %b %-d, %Y")

    parts = []
    parts.append(
        f'<div style="font-family:-apple-system,Segoe UI,Roboto,Arial,sans-serif;'
        f'max-width:620px;margin:0 auto;color:#0f172a;">'
    )
    parts.append(
        f'<h2 style="margin:0 0 2px;font-size:18px;">A&amp;N Cash Position</h2>'
        f'<div style="color:#64748b;font-size:13px;margin-bottom:16px;">{date_str}'
        f' &nbsp;&middot;&nbsp; live pulls refreshed {fetched} CT, manual entries as last saved</div>'
    )

    # Headline cards
    parts.append('<table cellspacing="8" cellpadding="0" style="border-collapse:separate;width:100%;"><tr>')
    parts.append(_card("Net Position", _money(pos["netPosition"]), pos_color))
    parts.append(_card("Free Cash (spendable now)", _money(pos["freeCash"]), free_color))
    parts.append('</tr><tr>')
    parts.append(_card("Total Liquid", _money(pos["totalLiquid"]), "#0f766e"))
    parts.append(_card("Total Owed", _money(pos["totalOwed"]), "#b91c1c"))
    parts.append('</tr></table>')

    # Horizon
    parts.append('<h3 style="font-size:13px;text-transform:uppercase;letter-spacing:.04em;color:#475569;margin:20px 0 6px;">Free cash by horizon</h3>')
    parts.append('<table cellspacing="8" cellpadding="0" style="border-collapse:separate;width:100%;"><tr>')
    for lbl, key in [("Now", "freeCashNow"), ("This week", "freeCashWeek"), ("This month", "freeCashMonth")]:
        c = "#0f766e" if pos[key] >= 0 else "#b91c1c"
        parts.append(_card(lbl, _money(pos[key]), c))
    parts.append('</tr></table>')

    # Inflows
    rows = [("Accounts receivable", _money(pos["ar"]["total"], cents=True))]
    if pos["incomingPayouts"]:
        rows.append((f'Incoming Jobber payouts ({pos["payoutCount"]})', _money(pos["incomingPayouts"], cents=True)))
    parts.append(_section("Money coming in", rows))

    # Obligations breakdown
    ob_rows = [(c["label"], _money(c["total"], cents=True)) for c in pos["obligationCategories"] if c["total"]]
    ob_rows.append(("<b>Total near-term obligations</b>", "<b>" + _money(pos["totalObligations"], cents=True) + "</b>"))
    parts.append(_section("What you owe (near-term)", ob_rows))

    # Payroll detail
    ap = pos.get("autoPayroll")
    if ap and ap.get("source") == "jobber":
        crew = len(ap.get("breakdown") or [])
        ws = ap.get("weekStart") or ""
        detail = f"This week ({crew} crew)" + (f", since {ws}" if ws else "")
        parts.append(_section("Payroll owed", [(detail, _money(ap["total"], cents=True))]))

    # Lurvey
    lv = pos.get("lurvey")
    if lv:
        parts.append(_section("Lurvey (vendor AP)", [
            ("Balance owed", _money(lv["balance"], cents=True)),
            ("Credit available", _money(lv["creditAvailable"], cents=True)),
        ]))

    # Fixed overhead still due
    od = overhead_due or {}
    if od.get("totalDue") or od.get("items"):
        ov_rows = [(f'{it["description"]} (the {it["day"]}th)', _money(it["amount"], cents=True))
                   for it in (od.get("items") or [])[:8]]
        ov_rows.append(("<b>Total still due this month</b>", "<b>" + _money(od.get("totalDue", 0), cents=True) + "</b>"))
        parts.append(_section("Fixed overhead still due this month", ov_rows))

    # Refresh problems
    if refresh_errors:
        notes = "; ".join(f"{k}: {v}" for k, v in refresh_errors.items())
        parts.append(
            f'<div style="margin-top:18px;padding:10px 12px;background:#fef3c7;border:1px solid #fde68a;'
            f'border-radius:8px;font-size:12px;color:#92400e;">Could not refresh live: {notes}.</div>'
        )

    parts.append(
        '<div style="margin-top:22px;color:#94a3b8;font-size:11px;">'
        'Generated from your cash tracker. Open it to adjust manual entries.</div>'
    )
    parts.append('</div>')

    es = email_settings()
    prefix = es.get("subject_prefix") or "A&N Cash Position"
    sign = "" if pos["netPosition"] >= 0 else "-"
    subject = f'{prefix} {today.strftime("%b %-d")}: Net {_money(pos["netPosition"])}, Free {_money(pos["freeCash"])}'
    return subject, "".join(parts)


def _section(title, rows):
    out = [f'<h3 style="font-size:13px;text-transform:uppercase;letter-spacing:.04em;color:#475569;margin:20px 0 6px;">{title}</h3>']
    out.append('<table cellpadding="0" cellspacing="0" style="width:100%;border-collapse:collapse;font-size:14px;">')
    for label, value in rows:
        out.append(
            f'<tr><td style="padding:6px 0;border-bottom:1px solid #f1f5f9;color:#334155;">{label}</td>'
            f'<td style="padding:6px 0;border-bottom:1px solid #f1f5f9;text-align:right;'
            f'font-variant-numeric:tabular-nums;color:#0f172a;">{value}</td></tr>'
        )
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
    subject, html = render_cash_email(pos, overhead_due, state, today, refresh_errors)

    if dry_run:
        return {"status": "dry_run", "subject": subject, "html": html, "errors": refresh_errors}

    es = email_settings()
    recipients = es.get("recipients") or ([es["recipient"]] if es.get("recipient") else [])
    if not recipients:
        return {"status": "error", "message": "No recipients configured"}
    to_header = ", ".join(recipients)
    result = send_email(to_header, subject, html)
    if result:
        return {"status": "ok", "subject": subject, "to": to_header, "message_id": result.get("id")}
    return {"status": "error", "message": "Gmail send failed (see logs)"}
