"""Monthly reminder to load the previous month's P&L into the financial dashboard.

Scheduled for the 15th of each month (see scheduler.py). Reuses the same Gmail
send pipe as the daily cashflow digest, so no extra auth is needed.

Recipient defaults to alex@anoutdoorservices.com; override with PNL_REMINDER_TO.
The dashboard link is built from APP_BASE_URL (e.g. https://<app>.up.railway.app);
if that env var is unset the email still sends, just without a clickable button.
"""
import logging
import os
from datetime import datetime

import pytz

from financial.gmail_send import send_email

logger = logging.getLogger(__name__)
CT = pytz.timezone("America/Chicago")

REMINDER_TO = os.getenv("PNL_REMINDER_TO", "alex@anoutdoorservices.com")


def _previous_month_label(now=None):
    """Return the prior calendar month as e.g. 'June 2026'."""
    now = now or datetime.now(CT)
    prev_month = now.month - 1 or 12
    prev_year = now.year if now.month > 1 else now.year - 1
    return datetime(prev_year, prev_month, 1).strftime("%B %Y")


def _build_html(month_label, link):
    button = (
        f'<p style="margin:22px 0">'
        f'<a href="{link}" style="background:#4f46e5;color:#fff;text-decoration:none;'
        f'padding:11px 20px;border-radius:6px;font-size:14px;display:inline-block">'
        f'Open the dashboard</a></p>'
        if link else
        '<p style="margin:22px 0;font-size:14px">Open <strong>jobber-app</strong> and '
        'click <strong>Open Financial Dashboard (P&amp;L)</strong> on the home page.</p>'
    )
    return f"""\
<div style="font-family:-apple-system,Segoe UI,Roboto,Arial,sans-serif;max-width:560px;margin:0 auto;color:#1f2937">
  <h2 style="margin:0 0 8px;color:#3730a3">Monthly P&amp;L reminder</h2>
  <p style="margin:0 0 16px;font-size:15px">It's the 15th. Time to load
     <strong>{month_label}</strong> into the financial dashboard so the numbers stay current.</p>
  <ol style="font-size:14px;line-height:1.7;padding-left:20px;margin:0">
    <li>In QuickBooks, export the Profit &amp; Loss for the full range (cash basis, months as columns).</li>
    <li>Drop the file into <code>pnl_history/</code> in the Maintenance Program project, replacing the old export.</li>
    <li>Run <code>npm run parse</code> to rebuild the data.</li>
    <li>Run <code>npm run build:jobber</code>, copy <code>dist/</code> into
        <code>jobber-app/financial_dashboard/</code>, then commit and push.
        <br><span style="color:#6b7280">(Numbers-only change? Just copy the new
        <code>public/pnl.json</code> over and push, no rebuild needed.)</span></li>
  </ol>
  {button}
  <p style="font-size:12px;color:#6b7280;margin-top:24px">
     Sent automatically on the 15th by jobber-app. To change the timing or turn this off, just ask Claude.</p>
</div>"""


def send_pnl_reminder(dry_run=False):
    """Email the monthly P&L update reminder. With dry_run=True, returns the
    rendered email without sending (used by the /pnl-reminder-now?dry=1 route)."""
    month_label = _previous_month_label()
    base = os.getenv("APP_BASE_URL", "").rstrip("/")
    link = f"{base}/financials" if base else ""
    subject = f"Reminder: add {month_label} P&L to the dashboard"
    html = _build_html(month_label, link)

    if dry_run:
        return {"status": "preview", "to": REMINDER_TO, "subject": subject, "html": html}

    result = send_email(REMINDER_TO, subject, html)
    if result:
        return {"status": "ok", "to": REMINDER_TO, "subject": subject,
                "message_id": result.get("id")}
    return {"status": "error", "message": "Gmail send failed (see logs)"}
