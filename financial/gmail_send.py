"""Send the daily cashflow digest via the Gmail API.

Reuses the existing Google OAuth credentials from jobber_sync.get_google_credentials().
The 'gmail.send' scope was added to GOOGLE_SCOPES in jobber_sync.py — on the next
re-auth those scopes are granted together (one OAuth consent, both APIs).
"""
import base64
import logging
from email.mime.text import MIMEText

from googleapiclient.discovery import build

from jobber_sync import get_google_credentials

logger = logging.getLogger(__name__)


def _build_message(to_address, subject, html_body, from_address=None):
    msg = MIMEText(html_body, "html")
    msg["To"] = to_address
    msg["Subject"] = subject
    if from_address:
        msg["From"] = from_address
    raw = base64.urlsafe_b64encode(msg.as_bytes()).decode()
    return {"raw": raw}


def send_email(to_address, subject, html_body, from_address=None):
    """Send an HTML email via the authenticated user's Gmail account.

    Returns the Gmail API response dict, or None on failure.
    """
    creds = get_google_credentials()
    service = build("gmail", "v1", credentials=creds, cache_discovery=False)
    message = _build_message(to_address, subject, html_body, from_address)
    try:
        result = service.users().messages().send(userId="me", body=message).execute()
        logger.info(f"Gmail send ok: id={result.get('id')} to={to_address}")
        return result
    except Exception as e:
        logger.error(f"Gmail send failed: {e}")
        return None
