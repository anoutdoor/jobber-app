"""Render the daily cashflow snapshot as an HTML email body."""
from datetime import date
from html import escape


def _money(n):
    if n is None:
        return "—"
    try:
        n = float(n)
    except (TypeError, ValueError):
        return "—"
    sign = "-" if n < 0 else ""
    return f"{sign}${abs(n):,.2f}"


def _row(label, value, color=None):
    style = f"color:{color};" if color else ""
    return f"<tr><td style='padding:4px 8px'>{escape(label)}</td><td style='padding:4px 8px;text-align:right;font-variant-numeric:tabular-nums;{style}'>{value}</td></tr>"


def _section_header(title):
    return f"<h3 style='font-family:Helvetica,Arial,sans-serif;margin:20px 0 6px;color:#222;border-bottom:1px solid #ccc;padding-bottom:4px'>{escape(title)}</h3>"


def _color_for_position(n):
    if n is None:
        return "#222"
    return "#0a7d2a" if n >= 0 else "#a02020"


def render_email(snapshot, today=None):
    """Build (subject, html_body) from the digest snapshot dict.

    snapshot = {
        'date': date,
        'position': {...},
        'qbo': {...},
        'ar': {...},
        'vendors': {...},
        'payroll': {...},
        'upcoming_bills': [...],
        'flags': [...],
    }
    """
    today = today or snapshot.get("date") or date.today()
    pos = snapshot["position"]
    qbo = snapshot["qbo"]
    ar = snapshot["ar"]
    vendors = snapshot["vendors"]
    payroll = snapshot["payroll"]
    bills = snapshot["upcoming_bills"]
    flags = snapshot.get("flags") or []
    unavailable = snapshot.get("unavailable_sources") or []

    subject_unavail = " [DEGRADED]" if unavailable else ""
    subject = f"A&N Cashflow — {today.isoformat()} — Net {_money(pos['total_position'])}{subject_unavail}"

    # --- Headline ---
    headline = f"""
    <div style='font-family:Helvetica,Arial,sans-serif;background:#f5f5f5;padding:16px;border-radius:8px;margin-bottom:16px'>
      <div style='font-size:13px;color:#666;text-transform:uppercase;letter-spacing:1px'>Net position right now</div>
      <div style='font-size:34px;font-weight:600;color:{_color_for_position(pos['total_position'])};margin:6px 0'>{_money(pos['total_position'])}</div>
      <div style='font-size:13px;color:#444'>
        Liquid: <b>{_money(pos['liquid_now'])}</b>
        &nbsp;&middot;&nbsp; AR: <b>{_money(pos['ar_outstanding'])}</b>
        &nbsp;&middot;&nbsp; 30-day forecast: <b style='color:{_color_for_position(pos['forecast_30'])}'>{_money(pos['forecast_30'])}</b>
      </div>
    </div>
    """

    # --- Unavailable data warning (loud, top of email) ---
    unavail_html = ""
    if unavailable:
        items = "".join(f"<li>{escape(s)}</li>" for s in unavailable)
        unavail_html = f"""
        <div style='background:#ffe0e0;border:2px solid #a02020;padding:14px 18px;margin-bottom:16px;font-family:Helvetica,Arial,sans-serif;border-radius:6px'>
          <div style='font-size:16px;font-weight:700;color:#a02020;text-transform:uppercase;letter-spacing:1px'>⚠ Data source(s) unavailable</div>
          <ul style='margin:8px 0 4px 18px;padding:0;color:#222'>{items}</ul>
          <div style='font-size:12px;color:#555;margin-top:6px'>
            The numbers below for the affected source(s) are zeros, not real values.
            Likely cause: OAuth tokens expired/revoked. Visit the relevant /login route on the app
            to re-authorize.
          </div>
        </div>
        """

    # --- Flags ---
    flags_html = ""
    if flags:
        items = "".join(f"<li>{escape(f)}</li>" for f in flags)
        flags_html = f"""
        <div style='background:#fff4cc;border-left:4px solid #d9a400;padding:10px 14px;margin-bottom:16px;font-family:Helvetica,Arial,sans-serif'>
          <strong>Heads up</strong>
          <ul style='margin:6px 0 0 16px;padding:0'>{items}</ul>
        </div>
        """

    # --- Breakdown table ---
    breakdown_rows = [
        _row("Cash on hand (banks)", _money(pos['cash']), color="#0a7d2a"),
        _row("Credit card debt", "−" + _money(pos['cc_debt']), color="#a02020"),
        _row("Vendor accounts (Lurvey's, Des Plaines, Patriot)", "−" + _money(pos['vendor_debt']), color="#a02020"),
        _row("Payroll accrued (unpaid this week)", "−" + _money(pos['payroll_accrued']), color="#a02020"),
        _row("AR outstanding", "+" + _money(pos['ar_outstanding']), color="#0a7d2a"),
        f"<tr><td colspan='2' style='border-top:1px solid #ccc'></td></tr>",
        _row("Total position", _money(pos['total_position']), color=_color_for_position(pos['total_position'])),
    ]
    breakdown_html = (
        _section_header("Breakdown")
        + "<table style='width:100%;font-family:Helvetica,Arial,sans-serif;font-size:14px;border-collapse:collapse'>"
        + "".join(breakdown_rows)
        + "</table>"
    )

    # --- Cash detail ---
    def _acct_label(a):
        if a.get("is_override"):
            as_of = a.get("override_as_of") or ""
            tag = f" <span style='font-size:11px;color:#888'>(override{' as of ' + escape(str(as_of)) if as_of else ''})</span>"
            return escape(a["name"]) + tag
        return escape(a["name"])

    cash_rows = "".join(
        f"<tr><td style='padding:4px 8px'>{_acct_label(a)}</td>"
        f"<td style='padding:4px 8px;text-align:right;font-variant-numeric:tabular-nums'>{_money(a['balance'])}</td></tr>"
        for a in qbo.get("cash_accounts", [])
    ) or "<tr><td>No bank accounts found</td></tr>"
    cc_rows = "".join(
        f"<tr><td style='padding:4px 8px'>{_acct_label(a)}</td>"
        f"<td style='padding:4px 8px;text-align:right;font-variant-numeric:tabular-nums;color:#a02020'>{_money(a['balance'])}</td></tr>"
        for a in qbo.get("cc_accounts", [])
    ) or "<tr><td>No credit card accounts found</td></tr>"

    books_through = qbo.get("books_through")
    freshness_note = ""
    if books_through:
        freshness_note = (
            f"<div style='font-size:11px;color:#888;margin-top:4px'>"
            f"QBO books current through {escape(str(books_through))} "
            f"(based on most recent Purchase transaction). "
            f"Live bank balances may differ — see overrides if pinned.</div>"
        )

    cash_html = (
        _section_header("Bank & CC detail")
        + "<table style='width:100%;font-family:Helvetica,Arial,sans-serif;font-size:13px;border-collapse:collapse'>"
        + "<tr><td colspan='2' style='font-weight:600;padding:4px 8px;background:#eef9ee'>Bank accounts</td></tr>"
        + cash_rows
        + "<tr><td colspan='2' style='font-weight:600;padding:4px 8px;background:#fdeeee'>Credit cards (debt)</td></tr>"
        + cc_rows
        + "</table>"
        + freshness_note
    )

    # --- AR ---
    bucket_rows = "".join(
        _row(f"{b['label']} ({b['count']})", _money(b['total']))
        for b in ar.get("buckets", [])
    )
    ar_html = (
        _section_header(f"AR — {_money(ar['total_outstanding'])} outstanding")
        + "<table style='width:100%;font-family:Helvetica,Arial,sans-serif;font-size:13px;border-collapse:collapse'>"
        + bucket_rows
        + "</table>"
    )

    if ar.get("top_overdue"):
        overdue_rows = "".join(
            f"<tr>"
            f"<td style='padding:4px 8px'>{escape(i['client_name'])}</td>"
            f"<td style='padding:4px 8px;color:#666'>#{escape(str(i['number']))}</td>"
            f"<td style='padding:4px 8px;text-align:right;font-variant-numeric:tabular-nums'>{_money(i['outstanding'])}</td>"
            f"<td style='padding:4px 8px;text-align:right;color:#a02020'>{i['days_past_due']}d late</td>"
            f"</tr>"
            for i in ar["top_overdue"]
        )
        ar_html += (
            "<div style='font-family:Helvetica,Arial,sans-serif;font-size:13px;color:#555;margin-top:8px'>Top overdue:</div>"
            + "<table style='width:100%;font-family:Helvetica,Arial,sans-serif;font-size:13px;border-collapse:collapse'>"
            + overdue_rows
            + "</table>"
        )

    # --- Vendors ---
    vendor_rows = ""
    for v in vendors.get("vendors", []):
        mode = v.get("mode") or "manual"
        anchor = v.get("anchor_balance", 0.0)
        accumulated = v.get("accumulated", 0.0)
        count = v.get("expense_count", 0)

        if mode == "auto":
            # Show "starting $X + $Y from N expenses" so the breakdown is visible
            tag_text = (
                f"{_money(anchor)} starting + {_money(accumulated)} from "
                f"{count} tagged expense{'s' if count != 1 else ''}"
            )
        elif mode == "sheet":
            tag_text = "sheet override"
        else:
            tag_text = "manual"

        tag = f"<div style='font-size:11px;color:#888;margin-top:2px'>{escape(tag_text)}</div>"
        as_of = f"<div style='font-size:11px;color:#888'>as of {escape(str(v['as_of']))}</div>" if v.get("as_of") else ""
        vendor_rows += (
            f"<tr>"
            f"<td style='padding:4px 8px'>{escape(v['name'])}{tag}{as_of}</td>"
            f"<td style='padding:4px 8px;text-align:right;font-variant-numeric:tabular-nums;vertical-align:top'>{_money(v['balance'])}</td>"
            f"</tr>"
        )
    vendor_html = (
        _section_header(f"Vendor accounts — {_money(vendors.get('total', 0))} owed")
        + "<table style='width:100%;font-family:Helvetica,Arial,sans-serif;font-size:13px;border-collapse:collapse'>"
        + vendor_rows
        + "</table>"
    )

    # --- Payroll ---
    payroll_rows = "".join(
        f"<tr>"
        f"<td style='padding:4px 8px'>{escape(u['name'])}</td>"
        f"<td style='padding:4px 8px;text-align:right'>{u['hours']:.1f}h</td>"
        f"<td style='padding:4px 8px;text-align:right'>{_money(u['rate'])}/hr</td>"
        f"<td style='padding:4px 8px;text-align:right;font-variant-numeric:tabular-nums'>{_money(u['estimated_pay'])}</td>"
        f"</tr>"
        for u in payroll.get("breakdown", [])
    ) or "<tr><td>No time entries since Monday</td></tr>"
    missing = payroll.get("missing_rates") or []
    missing_note = (
        f"<div style='font-size:11px;color:#a02020;margin-top:6px'>Missing wage rates: {escape(', '.join(missing))}. "
        f"Set them in financial_config.yaml under payroll.rate_overrides.</div>"
        if missing else ""
    )
    # Gross + tax-burden breakdown beneath the per-user table
    tax_pct = payroll.get("tax_burden_pct") or 0
    if tax_pct:
        totals_block = (
            "<div style='font-family:Helvetica,Arial,sans-serif;font-size:13px;"
            "color:#444;margin-top:10px;text-align:right;line-height:1.6'>"
            f"Gross wages: <b>{_money(payroll.get('gross_pay', 0))}</b><br>"
            f"+ employer tax burden ({tax_pct:g}%): <b>{_money(payroll.get('tax_burden', 0))}</b><br>"
            f"<span style='font-size:14px'>Total payroll cost: "
            f"<b>{_money(payroll.get('total_accrual', 0))}</b></span>"
            "</div>"
        )
    else:
        totals_block = ""

    payroll_html = (
        _section_header(f"Payroll accrued — {_money(payroll.get('total_accrual', 0))} (week starting {escape(payroll.get('week_start', ''))})")
        + "<table style='width:100%;font-family:Helvetica,Arial,sans-serif;font-size:13px;border-collapse:collapse'>"
        + payroll_rows
        + "</table>"
        + totals_block
        + missing_note
    )

    # --- Upcoming bills ---
    def bill_table(window_label, window_days):
        in_window = [b for b in bills if b.get("days_until_due") is not None and 0 <= b["days_until_due"] <= window_days]
        if not in_window:
            return f"<div style='font-family:Helvetica,Arial,sans-serif;font-size:13px;color:#666'>Nothing due in next {window_days} days.</div>"
        rows = "".join(
            f"<tr>"
            f"<td style='padding:4px 8px'>{escape(b['expense'])}</td>"
            f"<td style='padding:4px 8px;color:#666'>{escape(b['account'])}</td>"
            f"<td style='padding:4px 8px;text-align:right;font-variant-numeric:tabular-nums'>{_money(b['amount'])}</td>"
            f"<td style='padding:4px 8px;text-align:right;color:#666'>{escape(str(b['next_due_date']))} ({b['days_until_due']}d)</td>"
            f"</tr>"
            for b in in_window
        )
        subtotal = sum(b['amount'] or 0 for b in in_window)
        rows += f"<tr><td colspan='4' style='border-top:1px solid #ccc'></td></tr>"
        rows += _row(f"Subtotal — next {window_days} days", _money(subtotal))
        return f"<table style='width:100%;font-family:Helvetica,Arial,sans-serif;font-size:13px;border-collapse:collapse'>{rows}</table>"

    bills_html = (
        _section_header("Upcoming fixed bills")
        + "<div style='font-family:Helvetica,Arial,sans-serif;font-size:13px;color:#555;margin-bottom:4px;font-weight:600'>Next 7 days</div>"
        + bill_table("7d", 7)
        + "<div style='font-family:Helvetica,Arial,sans-serif;font-size:13px;color:#555;margin:10px 0 4px;font-weight:600'>Next 14 days</div>"
        + bill_table("14d", 14)
        + "<div style='font-family:Helvetica,Arial,sans-serif;font-size:13px;color:#555;margin:10px 0 4px;font-weight:600'>Next 30 days</div>"
        + bill_table("30d", 30)
    )

    # Skipped bills warning
    skipped = [b for b in bills if b.get("skip_reason")]
    if skipped:
        items = "".join(f"<li>{escape(b['expense'])}: {escape(b['skip_reason'])}</li>" for b in skipped)
        bills_html += (
            f"<div style='font-size:11px;color:#a02020;margin-top:6px'>Couldn't include "
            f"{len(skipped)} row(s) from the overhead sheet:<ul style='margin:4px 0 0 16px;padding:0'>{items}</ul></div>"
        )

    body = f"""
    <!doctype html>
    <html><body style='background:#fff;max-width:680px;margin:0 auto;padding:20px;font-family:Helvetica,Arial,sans-serif;color:#222'>
      <div style='color:#888;font-size:12px;letter-spacing:1px;text-transform:uppercase'>A&amp;N Outdoor Services &middot; Daily Cashflow &middot; {today.isoformat()}</div>
      {unavail_html}
      {headline}
      {flags_html}
      {breakdown_html}
      {cash_html}
      {ar_html}
      {vendor_html}
      {payroll_html}
      {bills_html}
      <div style='margin-top:24px;font-size:11px;color:#999;border-top:1px solid #eee;padding-top:12px'>
        Generated automatically by jobber-app/financial. Adjust thresholds in financial_config.yaml.
      </div>
    </body></html>
    """
    return subject, body
