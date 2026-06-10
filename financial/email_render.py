"""Render the daily cashflow snapshot as an HTML email body.

Layout (top → bottom):
  1. Optional DEGRADED warning banner (when data sources are unavailable)
  2. Headline NET POSITION
  3. Optional heads-up flags
  4. Two obligation buckets with free-cash numbers
       a. Due this week
       b. Due by July 1
  5. Net position breakdown (asset + liability math)
  6. Liquid assets detail (bank accounts + CC rewards memo)
  7. AR detail
  8. Vendor accounts detail
  9. Customer deposits detail (if any)
  10. Payroll detail
  11. Upcoming fixed bills (next 30 days)
  12. Variable overhead reference (informational footer)
"""
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


def _acct_label(a):
    if a.get("is_override"):
        as_of = a.get("override_as_of") or ""
        tag = f" <span style='font-size:11px;color:#888'>(override{' as of ' + escape(str(as_of)) if as_of else ''})</span>"
        return escape(a["name"]) + tag
    return escape(a["name"])


def render_email(snapshot, today=None):
    """Build (subject, html_body) from the digest snapshot dict."""
    today = today or snapshot.get("date") or date.today()
    pos = snapshot["position"]
    qbo = snapshot["qbo"]
    ar = snapshot["ar"]
    vendors = snapshot["vendors"]
    payroll = snapshot["payroll"]
    bills = snapshot["upcoming_bills"]
    flags = snapshot.get("flags") or []
    unavailable = snapshot.get("unavailable_sources") or []
    cc_rewards_cfg = snapshot.get("cc_rewards") or {}
    deposits = snapshot.get("customer_deposits") or []
    var_overhead = snapshot.get("variable_overhead") or {}

    subject_unavail = " [DEGRADED]" if unavailable else ""
    subject = f"A&N Cashflow — {today.isoformat()} — Net {_money(pos['net_position'])}{subject_unavail}"

    # =================================================================
    # 1. Unavailable warning (loud, top of email)
    # =================================================================
    unavail_html = ""
    if unavailable:
        items = "".join(f"<li>{escape(s)}</li>" for s in unavailable)
        base_url = "https://jobber-app-production.up.railway.app"
        action_buttons = ""
        if any("Jobber" in s for s in unavailable):
            action_buttons += (
                f"<a href='{base_url}/login' "
                f"style='display:inline-block;background:#a02020;color:#fff;"
                f"padding:10px 18px;border-radius:6px;text-decoration:none;"
                f"font-weight:600;margin-right:8px'>Re-authorize Jobber</a>"
            )
        if any("QuickBooks" in s or "QBO" in s for s in unavailable):
            action_buttons += (
                f"<a href='{base_url}/qbo/login' "
                f"style='display:inline-block;background:#a02020;color:#fff;"
                f"padding:10px 18px;border-radius:6px;text-decoration:none;"
                f"font-weight:600;margin-right:8px'>Re-authorize QuickBooks</a>"
            )
        unavail_html = f"""
        <div style='background:#ffe0e0;border:2px solid #a02020;padding:14px 18px;margin-bottom:16px;font-family:Helvetica,Arial,sans-serif;border-radius:6px'>
          <div style='font-size:16px;font-weight:700;color:#a02020;text-transform:uppercase;letter-spacing:1px'>⚠ Data source(s) unavailable</div>
          <ul style='margin:8px 0 4px 18px;padding:0;color:#222'>{items}</ul>
          <div style='font-size:12px;color:#555;margin:6px 0 10px'>
            Numbers below for affected source(s) are zeros, not real values. One click fixes it:
          </div>
          <div>{action_buttons}</div>
        </div>
        """

    # =================================================================
    # 2. Headline: NET POSITION
    # =================================================================
    headline = f"""
    <div style='font-family:Helvetica,Arial,sans-serif;background:#f5f5f5;padding:16px;border-radius:8px;margin-bottom:16px'>
      <div style='font-size:13px;color:#666;text-transform:uppercase;letter-spacing:1px'>Net position (full economic value)</div>
      <div style='font-size:34px;font-weight:600;color:{_color_for_position(pos['net_position'])};margin:6px 0'>{_money(pos['net_position'])}</div>
      <div style='font-size:13px;color:#444'>
        Liquid cash: <b>{_money(pos['cash'])}</b>
        &nbsp;&middot;&nbsp; AR: <b>{_money(pos['ar_outstanding'])}</b>
        &nbsp;&middot;&nbsp; CC debt: <b style='color:#a02020'>{_money(pos['cc_debt'])}</b>
      </div>
    </div>
    """

    # =================================================================
    # 3. Flags (heads up)
    # =================================================================
    flags_html = ""
    if flags:
        items = "".join(f"<li>{escape(f)}</li>" for f in flags)
        flags_html = f"""
        <div style='background:#fff4cc;border-left:4px solid #d9a400;padding:10px 14px;margin-bottom:16px;font-family:Helvetica,Arial,sans-serif'>
          <strong>Heads up</strong>
          <ul style='margin:6px 0 0 16px;padding:0'>{items}</ul>
        </div>
        """

    # =================================================================
    # 4. Two bucket sections (Due This Week / Due By July 1)
    # =================================================================
    def _bucket_box(title, end_label, bills_in_bucket, line_items, obligations_total, free_cash):
        bill_rows = "".join(
            f"<tr>"
            f"<td style='padding:3px 8px;font-size:13px'>{escape(b['expense'])}</td>"
            f"<td style='padding:3px 8px;font-size:13px;text-align:right;font-variant-numeric:tabular-nums'>{_money(b['amount'])}</td>"
            f"<td style='padding:3px 8px;font-size:13px;text-align:right;color:#666'>{escape(str(b['next_due_date']))} ({b['days_until_due']}d)</td>"
            f"</tr>"
            for b in bills_in_bucket
        )
        if not bill_rows:
            bill_rows = "<tr><td colspan='3' style='padding:4px 8px;font-size:12px;color:#888'>No bills in this window.</td></tr>"

        extra_rows = "".join(
            f"<tr>"
            f"<td style='padding:3px 8px;font-size:13px;font-weight:600'>{escape(label)}</td>"
            f"<td style='padding:3px 8px;font-size:13px;text-align:right;font-variant-numeric:tabular-nums;color:#a02020'>{_money(amt)}</td>"
            f"<td></td>"
            f"</tr>"
            for (label, amt) in line_items if amt
        )

        color = _color_for_position(free_cash)
        return f"""
        <div style='background:#fafafa;border:1px solid #e0e0e0;border-radius:8px;padding:14px 16px;margin-bottom:14px;font-family:Helvetica,Arial,sans-serif'>
          <div style='font-size:14px;font-weight:700;color:#222;margin-bottom:2px'>{escape(title)}</div>
          <div style='font-size:11px;color:#888;margin-bottom:8px'>through {escape(end_label)}</div>
          <table style='width:100%;border-collapse:collapse'>
            <tr><td colspan='3' style='font-size:11px;color:#888;padding:2px 8px;text-transform:uppercase;letter-spacing:0.5px'>Fixed bills</td></tr>
            {bill_rows}
            <tr><td colspan='3' style='font-size:11px;color:#888;padding:6px 8px 2px;text-transform:uppercase;letter-spacing:0.5px'>Other obligations</td></tr>
            {extra_rows}
            <tr><td colspan='3' style='border-top:1px solid #ccc;padding:0'></td></tr>
            <tr>
              <td style='padding:4px 8px;font-size:13px;font-weight:600'>Total obligations</td>
              <td style='padding:4px 8px;font-size:13px;text-align:right;font-variant-numeric:tabular-nums;font-weight:600;color:#a02020'>{_money(obligations_total)}</td>
              <td></td>
            </tr>
            <tr>
              <td style='padding:6px 8px;font-size:14px;font-weight:700'>Free cash after</td>
              <td style='padding:6px 8px;font-size:16px;text-align:right;font-variant-numeric:tabular-nums;font-weight:700;color:{color}'>{_money(free_cash)}</td>
              <td></td>
            </tr>
          </table>
        </div>
        """

    # This week bucket
    this_week_bills = [b for b in bills if b.get("next_due_date") and b.get("days_until_due") is not None
                        and date.fromisoformat(pos["this_week_end"]) >= b["next_due_date"] >= today]
    week_extras = [
        ("Uncleared checks", pos.get("uncleared_checks") or 0),
        ("Payroll owed (incl. tax burden)", pos.get("payroll_accrued") or 0),
        ("Owner pay", pos.get("owner_pay") or 0),
    ]
    this_week_box = _bucket_box(
        "Due this week",
        f"end of week ({pos['this_week_end']})",
        this_week_bills,
        week_extras,
        pos["this_week_obligations_total"],
        pos["free_cash_this_week"],
    )

    # By July 1 bucket
    jul1_date_obj = date.fromisoformat(pos["by_jul1_date"])
    jul1_bills = [b for b in bills if b.get("next_due_date") and b.get("days_until_due") is not None
                    and jul1_date_obj >= b["next_due_date"] >= today]
    jul1_extras = [
        ("Uncleared checks", pos.get("uncleared_checks") or 0),
        ("Payroll owed (incl. tax burden)", pos.get("payroll_accrued") or 0),
        ("Owner pay", pos.get("owner_pay") or 0),
        ("Vendor accounts (Lurvey's / Des Plaines / Patriot / Brock)", pos.get("vendor_debt") or 0),
    ]
    jul1_box = _bucket_box(
        "Due by July 1",
        f"July 1 ({pos['by_jul1_date']})",
        jul1_bills,
        jul1_extras,
        pos["by_jul1_obligations_total"],
        pos["free_cash_by_jul1"],
    )

    buckets_html = (
        _section_header("Obligations by window")
        + this_week_box
        + jul1_box
    )

    # =================================================================
    # 5. Net Position Breakdown (full math)
    # =================================================================
    breakdown_rows = [
        f"<tr><td colspan='2' style='padding:4px 8px;font-weight:600;background:#eef9ee'>Assets</td></tr>",
        _row("Cash on hand (banks)", "+" + _money(pos['cash']), color="#0a7d2a"),
        _row("A/R outstanding (will collect)", "+" + _money(pos['ar_outstanding']), color="#0a7d2a"),
        _row("CC rewards available", "+" + _money(pos['cc_rewards_available']), color="#0a7d2a"),
        f"<tr><td colspan='2' style='padding:4px 8px;font-weight:600;background:#fdeeee'>Liabilities</td></tr>",
        _row("Credit card debt", "−" + _money(pos['cc_debt']), color="#a02020"),
        _row("Vendor accounts", "−" + _money(pos['vendor_debt']), color="#a02020"),
        _row("Payroll accrued (incl. tax burden)", "−" + _money(pos['payroll_accrued']), color="#a02020"),
        _row("Owner pay owed", "−" + _money(pos['owner_pay']), color="#a02020"),
        _row("Customer deposits held (liability)", "−" + _money(pos['customer_deposits']), color="#a02020"),
        _row("Uncleared checks", "−" + _money(pos['uncleared_checks']), color="#a02020"),
        f"<tr><td colspan='2' style='border-top:1px solid #ccc'></td></tr>",
        _row("Net position", _money(pos['net_position']), color=_color_for_position(pos['net_position'])),
    ]
    breakdown_html = (
        _section_header("Net position breakdown")
        + "<table style='width:100%;font-family:Helvetica,Arial,sans-serif;font-size:14px;border-collapse:collapse'>"
        + "".join(breakdown_rows)
        + "</table>"
    )

    # =================================================================
    # 6. Liquid assets detail (banks + CC rewards memo + CC detail)
    # =================================================================
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

    rewards_amt = cc_rewards_cfg.get("available", 0)
    rewards_memo = ""
    if rewards_amt:
        as_of_r = cc_rewards_cfg.get("as_of") or ""
        note_r = cc_rewards_cfg.get("note") or ""
        rewards_memo = (
            f"<div style='font-size:12px;color:#555;margin-top:6px;padding:6px 8px;background:#f7fbf7;border-left:3px solid #0a7d2a'>"
            f"<b>CC rewards available (memo):</b> {_money(rewards_amt)}"
            f"{(' — ' + escape(note_r)) if note_r else ''}"
            f"{(' (as of ' + escape(str(as_of_r)) + ')') if as_of_r else ''}"
            f"</div>"
        )

    cash_html = (
        _section_header("Liquid assets & CC detail")
        + "<table style='width:100%;font-family:Helvetica,Arial,sans-serif;font-size:13px;border-collapse:collapse'>"
        + "<tr><td colspan='2' style='font-weight:600;padding:4px 8px;background:#eef9ee'>Bank accounts</td></tr>"
        + cash_rows
        + "<tr><td colspan='2' style='font-weight:600;padding:4px 8px;background:#fdeeee'>Credit cards (debt)</td></tr>"
        + cc_rows
        + "</table>"
        + rewards_memo
    )

    # =================================================================
    # 7. AR
    # =================================================================
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

    # =================================================================
    # 8. Vendor accounts
    # =================================================================
    vendor_rows = ""
    for v in vendors.get("vendors", []):
        mode = v.get("mode") or "manual"
        anchor = v.get("anchor_balance", 0.0)
        accumulated = v.get("accumulated", 0.0)
        count = v.get("expense_count", 0)
        if mode == "auto":
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
        _section_header(f"Vendor accounts — {_money(vendors.get('total', 0))} owed (all due July 1)")
        + "<table style='width:100%;font-family:Helvetica,Arial,sans-serif;font-size:13px;border-collapse:collapse'>"
        + vendor_rows
        + "</table>"
    )

    # =================================================================
    # 9. Customer deposits (liability)
    # =================================================================
    deposits_html = ""
    if deposits:
        deposit_rows = "".join(
            f"<tr>"
            f"<td style='padding:4px 8px'>{escape(d.get('client', ''))}</td>"
            f"<td style='padding:4px 8px;text-align:right;font-variant-numeric:tabular-nums;color:#a02020'>{_money(d.get('amount', 0))}</td>"
            f"<td style='padding:4px 8px;color:#888;font-size:11px'>{escape(str(d.get('as_of', '')))}</td>"
            f"</tr>"
            for d in deposits
        )
        deposit_total = sum(float(d.get("amount") or 0) for d in deposits)
        deposits_html = (
            _section_header(f"Customer deposits held — {_money(deposit_total)} (liability)")
            + "<div style='font-size:12px;color:#666;margin-bottom:6px'>"
              "Cash sitting in our bank from customers ahead of work performed. "
              "Counted against net position because the obligation is to deliver service or refund."
            "</div>"
            + "<table style='width:100%;font-family:Helvetica,Arial,sans-serif;font-size:13px;border-collapse:collapse'>"
            + deposit_rows
            + "</table>"
        )

    # =================================================================
    # 10. Payroll detail
    # =================================================================
    payroll_rows = "".join(
        f"<tr>"
        f"<td style='padding:4px 8px'>{escape(u['name'])}</td>"
        f"<td style='padding:4px 8px;text-align:right'>{u['hours']:.1f}h</td>"
        f"<td style='padding:4px 8px;text-align:right'>{_money(u['rate'])}/hr</td>"
        f"<td style='padding:4px 8px;text-align:right;font-variant-numeric:tabular-nums'>{_money(u['estimated_pay'])}</td>"
        f"</tr>"
        for u in payroll.get("breakdown", [])
    ) or "<tr><td>No time entries in current accrual window</td></tr>"
    missing = payroll.get("missing_rates") or []
    missing_note = (
        f"<div style='font-size:11px;color:#a02020;margin-top:6px'>Missing wage rates: {escape(', '.join(missing))}. "
        f"Set them in financial_config.yaml under payroll.rate_overrides.</div>"
        if missing else ""
    )
    tax_pct = payroll.get("tax_burden_pct") or 0
    totals_block = ""
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
    payroll_html = (
        _section_header(f"Payroll accrued — {_money(payroll.get('total_accrual', 0))} (unpaid hours since {escape(payroll.get('week_start', ''))})")
        + "<table style='width:100%;font-family:Helvetica,Arial,sans-serif;font-size:13px;border-collapse:collapse'>"
        + payroll_rows
        + "</table>"
        + totals_block
        + missing_note
    )

    # =================================================================
    # 11. Upcoming bills detail (next 30 days)
    # =================================================================
    bills_30 = [b for b in bills if b.get("days_until_due") is not None and 0 <= b["days_until_due"] <= 30]
    if bills_30:
        rows = "".join(
            f"<tr>"
            f"<td style='padding:4px 8px'>{escape(b['expense'])}</td>"
            f"<td style='padding:4px 8px;color:#666'>{escape(b['account'])}</td>"
            f"<td style='padding:4px 8px;text-align:right;font-variant-numeric:tabular-nums'>{_money(b['amount'])}</td>"
            f"<td style='padding:4px 8px;text-align:right;color:#666'>{escape(str(b['next_due_date']))} ({b['days_until_due']}d)</td>"
            f"</tr>"
            for b in bills_30
        )
        subtotal = sum(b['amount'] or 0 for b in bills_30)
        rows += f"<tr><td colspan='4' style='border-top:1px solid #ccc'></td></tr>"
        rows += _row("Subtotal — next 30 days", _money(subtotal))
        bills_html = (
            _section_header("Upcoming fixed bills (next 30 days)")
            + f"<table style='width:100%;font-family:Helvetica,Arial,sans-serif;font-size:13px;border-collapse:collapse'>{rows}</table>"
        )
    else:
        bills_html = (
            _section_header("Upcoming fixed bills (next 30 days)")
            + "<div style='font-family:Helvetica,Arial,sans-serif;font-size:13px;color:#666'>Nothing due in next 30 days.</div>"
        )
    # Skipped (paid-ahead / unparseable) bills warning
    skipped = [b for b in bills if b.get("skip_reason") or b.get("paid_ahead_through")]
    if skipped:
        items = "".join(
            "<li>"
            + escape(b['expense'])
            + (
                f" — paid ahead through {escape(b['paid_ahead_through'])}"
                if b.get('paid_ahead_through') else
                f": {escape(b.get('skip_reason', ''))}"
            )
            + "</li>"
            for b in skipped
        )
        bills_html += (
            f"<div style='font-size:11px;color:#666;margin-top:6px'>"
            f"Note ({len(skipped)} row(s)):<ul style='margin:4px 0 0 16px;padding:0'>{items}</ul></div>"
        )

    # =================================================================
    # 12. Variable overhead reference (footer)
    # =================================================================
    var_html = ""
    if var_overhead:
        salaries = var_overhead.get("salaries") or {}
        salary_rows = "".join(
            f"<tr><td style='padding:2px 8px;color:#666;font-size:12px'>&nbsp;&nbsp;{escape(name)}</td>"
            f"<td style='padding:2px 8px;text-align:right;font-size:12px;color:#666;font-variant-numeric:tabular-nums'>{_money(amt)}</td></tr>"
            for name, amt in salaries.items()
        )
        fixed_total = 8056.42  # from your spec; could compute from bills if desired
        salary_total = sum(float(v or 0) for v in salaries.values())
        variable_total = (
            salary_total
            + float(var_overhead.get("fuel") or 0)
            + float(var_overhead.get("maintenance") or 0)
            + float(var_overhead.get("physical_marketing") or 0)
            + float(var_overhead.get("other") or 0)
        )
        breakeven = float(var_overhead.get("monthly_breakeven_target") or 0)
        var_html = (
            _section_header("Monthly overhead reference (informational)")
            + "<table style='width:100%;font-family:Helvetica,Arial,sans-serif;font-size:13px;border-collapse:collapse'>"
            + _row("Fixed overhead (sum of recurring bills above)", _money(fixed_total))
            + _row("Variable: Fuel", _money(var_overhead.get("fuel")))
            + "<tr><td colspan='2' style='padding:2px 8px;font-size:12px;color:#666'>Variable: Salaries</td></tr>"
            + salary_rows
            + _row("Variable: Maintenance", _money(var_overhead.get("maintenance")))
            + _row("Variable: Physical marketing", _money(var_overhead.get("physical_marketing")))
            + _row("Variable: Other", _money(var_overhead.get("other")))
            + f"<tr><td colspan='2' style='border-top:1px solid #ccc'></td></tr>"
            + _row("Total monthly overhead", _money(fixed_total + variable_total))
            + _row("Monthly revenue breakeven target", _money(breakeven))
            + "</table>"
        )

    # =================================================================
    # Final body
    # =================================================================
    body = f"""
    <!doctype html>
    <html><body style='background:#fff;max-width:680px;margin:0 auto;padding:20px;font-family:Helvetica,Arial,sans-serif;color:#222'>
      <div style='color:#888;font-size:12px;letter-spacing:1px;text-transform:uppercase'>A&amp;N Outdoor Services &middot; Daily Cashflow &middot; {today.isoformat()}</div>
      {unavail_html}
      {headline}
      {flags_html}
      {buckets_html}
      {breakdown_html}
      {cash_html}
      {ar_html}
      {vendor_html}
      {deposits_html}
      {payroll_html}
      {bills_html}
      {var_html}
      <div style='margin-top:24px;font-size:11px;color:#999;border-top:1px solid #eee;padding-top:12px'>
        Generated automatically by jobber-app/financial. Edit financial_config.yaml to adjust.
      </div>
    </body></html>
    """
    return subject, body
