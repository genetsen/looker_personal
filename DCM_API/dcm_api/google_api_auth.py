"""Shared Google API credential helpers for DCM_API.

This module centralizes the repo's preferred Google auth path:
Application Default Credentials (ADC) from gcloud. Callers can still
layer a temporary legacy fallback on top during a no-break migration.
"""

from __future__ import annotations

from collections.abc import Sequence

import google.auth
from google.auth.exceptions import DefaultCredentialsError, RefreshError
from google.auth.transport.requests import Request
from google.oauth2.credentials import Credentials


def try_get_adc_credentials(scopes: Sequence[str]) -> Credentials | None:
    """Return refreshed ADC credentials when available for the requested scopes."""

    try:
        creds, _project = google.auth.default(scopes=list(scopes))
    except DefaultCredentialsError:
        return None

    try:
        creds.refresh(Request())
    except RefreshError:
        return None

    has_scopes = getattr(creds, "has_scopes", None)
    if callable(has_scopes) and not has_scopes(list(scopes)):
        return None

    return creds
