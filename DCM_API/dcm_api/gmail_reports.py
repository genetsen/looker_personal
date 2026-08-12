from __future__ import annotations

import base64
import os
import pickle
import time
from pathlib import Path
from typing import Any

from google.auth.transport.requests import Request
from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import InstalledAppFlow
from googleapiclient.discovery import build

from dcm_api.google_api_auth import try_get_adc_credentials


GMAIL_SCOPES = ["https://www.googleapis.com/auth/gmail.readonly"]
RUNNER_GMAIL_CLIENT_PATH = Path(
    "/Users/eugenetsenter/Docs/R_Studio_Projects/DCM_authorization/dcm_cred_20240916.json"
)
GMAIL_TOKEN_JSON_PATH = Path.home() / ".cache" / "gmail_token.json"
GMAIL_TOKEN_PICKLE_PATH = Path.home() / ".cache" / "gmail_token.pickle"


def _gmail_client_secrets_path() -> Path:
    override = os.getenv("GOOGLE_GMAIL_CLIENT_SECRETS")
    if override:
        return Path(override).expanduser()
    return RUNNER_GMAIL_CLIENT_PATH


def _gmail_token_json_path() -> Path:
    override = os.getenv("GOOGLE_GMAIL_TOKEN_FILE")
    if override:
        return Path(override).expanduser()
    return GMAIL_TOKEN_JSON_PATH


def _load_json_gmail_credentials() -> Credentials | None:
    token_path = _gmail_token_json_path()
    if not token_path.exists():
        return None

    creds = Credentials.from_authorized_user_file(token_path, GMAIL_SCOPES)
    if creds.expired and creds.refresh_token:
        creds.refresh(Request())

    if not creds.valid:
        return None
    return creds


def _save_json_gmail_credentials(creds: Credentials) -> None:
    token_path = _gmail_token_json_path()
    token_path.parent.mkdir(parents=True, exist_ok=True)
    token_path.write_text(creds.to_json(), encoding="utf-8")


def _oauth_gmail_credentials() -> Credentials | None:
    client_secrets = _gmail_client_secrets_path()
    if not client_secrets.exists():
        return None

    creds = _load_json_gmail_credentials()
    if creds is not None:
        _save_json_gmail_credentials(creds)
        return creds

    flow = InstalledAppFlow.from_client_secrets_file(client_secrets, GMAIL_SCOPES)
    creds = flow.run_local_server(port=0)
    _save_json_gmail_credentials(creds)
    return creds


def _pickle_gmail_credentials():
    if not GMAIL_TOKEN_PICKLE_PATH.exists():
        return None

    with GMAIL_TOKEN_PICKLE_PATH.open("rb") as token_file:
        creds = pickle.load(token_file)
    if creds.expired and creds.refresh_token:
        creds.refresh(Request())
    if not creds.valid:
        return None
    return creds


def build_gmail_service():
    creds = try_get_adc_credentials(GMAIL_SCOPES)
    if creds is not None:
        return build("gmail", "v1", credentials=creds)

    creds = _oauth_gmail_credentials()
    if creds is None:
        creds = _pickle_gmail_credentials()
    if creds is None:
        raise RuntimeError(
            "Gmail credentials are not valid. Refresh ADC with gmail.readonly, "
            f"approve the runner Gmail client at {_gmail_client_secrets_path()}, "
            f"or refresh {GMAIL_TOKEN_PICKLE_PATH}."
        )
    return build("gmail", "v1", credentials=creds)


def iter_parts(payload: dict[str, Any]):
    yield payload
    for part in payload.get("parts", []) or []:
        yield from iter_parts(part)


def find_report_attachment(
    subject_query: str,
    expected_filename_prefix: str,
    after_epoch_seconds: int,
    timeout_seconds: int,
    poll_seconds: int = 30,
) -> tuple[dict[str, Any], dict[str, Any]]:
    service = build_gmail_service()
    query = f'newer:{after_epoch_seconds} filename:csv "{subject_query}"'
    deadline = time.time() + timeout_seconds

    while True:
        response = (
            service.users()
            .messages()
            .list(userId="me", q=query, maxResults=10)
            .execute()
        )
        for item in response.get("messages", []) or []:
            message = (
                service.users()
                .messages()
                .get(userId="me", id=item["id"], format="full")
                .execute()
            )
            for part in iter_parts(message.get("payload", {})):
                filename = part.get("filename") or ""
                body = part.get("body", {})
                if expected_filename_prefix in filename and body.get("attachmentId"):
                    return message, part

        if time.time() > deadline:
            raise TimeoutError(
                "Timed out waiting for CM360 report attachment in Gmail. "
                f"Query: {query}"
            )
        time.sleep(poll_seconds)


def find_latest_report_attachment(subject_query: str, expected_filename_prefix: str) -> tuple[dict[str, Any], dict[str, Any]]:
    service = build_gmail_service()
    query = f'filename:csv "{subject_query}"'
    response = service.users().messages().list(userId="me", q=query, maxResults=20).execute()
    for item in response.get("messages", []) or []:
        message = (
            service.users()
            .messages()
            .get(userId="me", id=item["id"], format="full")
            .execute()
        )
        for part in iter_parts(message.get("payload", {})):
            filename = part.get("filename") or ""
            body = part.get("body", {})
            if expected_filename_prefix in filename and body.get("attachmentId"):
                return message, part
    raise FileNotFoundError(f"No Gmail CSV attachment found for query: {query}")


def download_attachment(message: dict[str, Any], part: dict[str, Any], output_dir: Path) -> Path:
    service = build_gmail_service()
    body = part["body"]
    attachment = (
        service.users()
        .messages()
        .attachments()
        .get(userId="me", messageId=message["id"], id=body["attachmentId"])
        .execute()
    )
    data = base64.urlsafe_b64decode(attachment["data"].encode("utf-8"))
    output_dir.mkdir(parents=True, exist_ok=True)
    output_path = output_dir / part["filename"]
    output_path.write_bytes(data)
    return output_path
