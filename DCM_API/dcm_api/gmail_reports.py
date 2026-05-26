from __future__ import annotations

import base64
import pickle
import time
from pathlib import Path
from typing import Any

from google.auth.transport.requests import Request
from googleapiclient.discovery import build


GMAIL_TOKEN_PATH = Path.home() / ".cache" / "gmail_token.pickle"


def build_gmail_service():
    with GMAIL_TOKEN_PATH.open("rb") as token_file:
        creds = pickle.load(token_file)
    if creds.expired and creds.refresh_token:
        creds.refresh(Request())
    if not creds.valid:
        raise RuntimeError("Gmail credentials are not valid. Refresh ~/.cache/gmail_token.pickle.")
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
