from __future__ import annotations

import os
from pathlib import Path

from dotenv import load_dotenv
from google.auth.transport.requests import Request
from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import InstalledAppFlow
from googleapiclient.discovery import build

from dcm_api.google_api_auth import try_get_adc_credentials


SCOPES = ["https://www.googleapis.com/auth/dfareporting"]
DEFAULT_API_VERSION = "v5"


def project_root() -> Path:
    return Path(__file__).resolve().parents[1]


def load_settings() -> dict[str, Path | str]:
    load_dotenv(project_root() / ".env")

    client_secrets = Path(
        os.getenv("GOOGLE_CLIENT_SECRETS", "secrets/client_secrets.json")
    )
    token_file = Path(os.getenv("GOOGLE_TOKEN_FILE", "secrets/token.json"))
    api_version = os.getenv("CAMPAIGN_MANAGER_API_VERSION", DEFAULT_API_VERSION)

    if not client_secrets.is_absolute():
        client_secrets = project_root() / client_secrets
    if not token_file.is_absolute():
        token_file = project_root() / token_file

    return {
        "client_secrets": client_secrets,
        "token_file": token_file,
        "api_version": api_version,
    }


def get_credentials() -> Credentials:
    settings = load_settings()
    client_secrets = settings["client_secrets"]
    token_file = settings["token_file"]

    creds = try_get_adc_credentials(SCOPES)
    if creds is not None:
        return creds

    creds = None
    if token_file.exists():
        creds = Credentials.from_authorized_user_file(token_file, SCOPES)

    if creds and creds.expired and creds.refresh_token:
        creds.refresh(Request())

    if not creds or not creds.valid:
        if not client_secrets.exists():
            raise FileNotFoundError(
                "Missing Google OAuth client secrets file. Expected it at "
                f"{client_secrets}."
            )

        flow = InstalledAppFlow.from_client_secrets_file(client_secrets, SCOPES)
        creds = flow.run_local_server(port=0)

    token_file.parent.mkdir(parents=True, exist_ok=True)
    token_file.write_text(creds.to_json(), encoding="utf-8")
    return creds


def build_service():
    settings = load_settings()
    creds = get_credentials()
    return build("dfareporting", settings["api_version"], credentials=creds)
