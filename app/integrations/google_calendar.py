from __future__ import annotations

import os
from pathlib import Path
from typing import Any

from google.auth.transport.requests import Request
from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import InstalledAppFlow
from googleapiclient.discovery import build

SCOPES = ["https://www.googleapis.com/auth/calendar.events"]


def _credentials_file() -> Path:
    configured = os.environ.get("GOOGLE_CALENDAR_CREDENTIALS_FILE", "").strip()
    if configured:
        return Path(configured)
    return Path("google_oauth_client_secret.json")


def _token_file() -> Path:
    configured = os.environ.get("GOOGLE_CALENDAR_TOKEN_FILE", "").strip()
    if configured:
        return Path(configured)
    return Path(".secrets/google_calendar_token.json")


def _use_console_auth() -> bool:
    return os.environ.get("GOOGLE_CALENDAR_USE_CONSOLE_AUTH", "false").lower() in {"1", "true", "yes"}


def _load_credentials() -> Credentials:
    cred_path = _credentials_file()
    token_path = _token_file()
    token_path.parent.mkdir(parents=True, exist_ok=True)

    creds: Credentials | None = None
    if token_path.exists():
        creds = Credentials.from_authorized_user_file(str(token_path), SCOPES)

    if not creds or not creds.valid:
        if creds and creds.expired and creds.refresh_token:
            creds.refresh(Request())
        else:
            if not cred_path.exists():
                raise RuntimeError(
                    "Google Calendar is not configured. Missing OAuth client file. "
                    "Set GOOGLE_CALENDAR_CREDENTIALS_FILE to your OAuth client JSON."
                )
            flow = InstalledAppFlow.from_client_secrets_file(str(cred_path), SCOPES)
            if _use_console_auth():
                creds = flow.run_console()
            else:
                creds = flow.run_local_server(port=0)
        token_path.write_text(creds.to_json(), encoding="utf-8")

    return creds


def _calendar_service():
    creds = _load_credentials()
    return build("calendar", "v3", credentials=creds, cache_discovery=False)


def create_calendar_event(
    *,
    summary: str,
    start_datetime: str,
    end_datetime: str,
    timezone: str,
    description: str | None = None,
    location: str | None = None,
    attendees: list[str] | None = None,
    calendar_id: str = "primary",
    send_updates: str = "none",
) -> dict[str, Any]:
    if not summary.strip():
        raise RuntimeError("Event summary is required.")
    if not start_datetime.strip() or not end_datetime.strip():
        raise RuntimeError("Both start_datetime and end_datetime are required.")
    if send_updates not in {"all", "externalOnly", "none"}:
        send_updates = "none"

    body: dict[str, Any] = {
        "summary": summary.strip(),
        "start": {"dateTime": start_datetime.strip(), "timeZone": timezone.strip()},
        "end": {"dateTime": end_datetime.strip(), "timeZone": timezone.strip()},
    }
    if description:
        body["description"] = description.strip()
    if location:
        body["location"] = location.strip()
    if attendees:
        body["attendees"] = [{"email": a} for a in attendees if a and "@" in a]

    service = _calendar_service()
    event = (
        service.events()
        .insert(
            calendarId=calendar_id.strip() or "primary",
            body=body,
            sendUpdates=send_updates,
        )
        .execute()
    )
    return {
        "id": event.get("id"),
        "htmlLink": event.get("htmlLink"),
        "status": event.get("status"),
        "summary": event.get("summary"),
        "start": (event.get("start") or {}).get("dateTime") or (event.get("start") or {}).get("date"),
        "end": (event.get("end") or {}).get("dateTime") or (event.get("end") or {}).get("date"),
        "calendarId": calendar_id,
    }

