"""Web-based Google OAuth flow for the financial digest.

The existing jobber_sync.get_google_credentials uses InstalledAppFlow which
requires a local browser pop-up and doesn't work on Railway. This module
adds proper web OAuth routes (/google/login + /google/callback) using
google_auth_oauthlib.Flow so re-authorization is one click in production.

Reuses the GOOGLE_CLIENT_SECRETS / GOOGLE_CLIENT_SECRETS_FILE pattern from
jobber_sync, and writes the resulting token back to the same token.json /
GOOGLE_TOKEN locations.
"""
import os
import json
import logging
import tempfile

from flask import redirect, request, session, jsonify, url_for
from google_auth_oauthlib.flow import Flow

from jobber_sync import GOOGLE_SCOPES, GOOGLE_TOKEN_FILE, GOOGLE_CLIENT_SECRETS_FILE, _save_token

logger = logging.getLogger(__name__)


def _client_secrets_path():
    """Return a path to a client_secrets JSON file. Writes a temp file if
    only GOOGLE_CLIENT_SECRETS env var is available."""
    env_secrets = os.getenv("GOOGLE_CLIENT_SECRETS")
    if env_secrets:
        tmp = tempfile.NamedTemporaryFile(mode="w", suffix=".json", delete=False)
        tmp.write(env_secrets)
        tmp.close()
        return tmp.name
    if os.path.exists(GOOGLE_CLIENT_SECRETS_FILE):
        return GOOGLE_CLIENT_SECRETS_FILE
    raise FileNotFoundError(
        "No Google client secrets. Set GOOGLE_CLIENT_SECRETS env var "
        "or place client_secrets.json in the project folder."
    )


def _redirect_uri():
    """The /google/callback URL for this deployment. Uses request.url_root
    so it works for both local dev and Railway production without config."""
    return request.url_root.rstrip("/") + "/google/callback"


def register_routes(app):
    @app.route("/google/login")
    def google_login():
        secrets_path = _client_secrets_path()
        flow = Flow.from_client_secrets_file(
            secrets_path,
            scopes=GOOGLE_SCOPES,
            redirect_uri=_redirect_uri(),
        )
        auth_url, state = flow.authorization_url(
            access_type="offline",
            include_granted_scopes="true",
            prompt="consent",   # force consent so refresh_token is returned
        )
        session["google_oauth_state"] = state
        return redirect(auth_url)

    @app.route("/google/callback")
    def google_callback():
        state = session.pop("google_oauth_state", None)
        if not state:
            return jsonify({"error": "Missing OAuth state in session"}), 400

        try:
            secrets_path = _client_secrets_path()
            flow = Flow.from_client_secrets_file(
                secrets_path,
                scopes=GOOGLE_SCOPES,
                redirect_uri=_redirect_uri(),
                state=state,
            )
            flow.fetch_token(authorization_response=request.url)
        except Exception as e:
            logger.exception("Google OAuth callback failed")
            return jsonify({"error": "Google OAuth failed", "details": str(e)}), 400

        creds = flow.credentials
        _save_token(creds)

        token_json = creds.to_json()
        # Log the blob so it can be pasted into GOOGLE_TOKEN env var for Railway
        # persistence across redeploys.
        logger.info(
            "Google auth complete. To persist across Railway redeploys, set "
            "GOOGLE_TOKEN env var to: %s", token_json
        )
        return (
            "<html><body style='font-family:Helvetica,Arial;max-width:600px;"
            "margin:40px auto;padding:24px'>"
            "<h1>Google authorization complete</h1>"
            "<p>Tokens saved. The cashflow digest can now send email via Gmail.</p>"
            "<p style='font-size:13px;color:#666'>For Railway persistence "
            "across redeploys, copy the GOOGLE_TOKEN JSON blob from the "
            "Railway logs and update the env var.</p>"
            "<p><a href='/'>Return to home</a></p>"
            "</body></html>"
        )
