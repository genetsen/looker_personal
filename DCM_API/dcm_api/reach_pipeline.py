from __future__ import annotations

import io
import json
import time
from dataclasses import asdict, dataclass
from datetime import date
from pathlib import Path
from typing import Any

from googleapiclient.errors import HttpError
from googleapiclient.http import MediaIoBaseDownload

from dcm_api.client import build_service, project_root


@dataclass(frozen=True)
class ReachPullConfig:
    profile_id: str
    advertiser_id: str
    advertiser_name: str
    start_date: date
    end_date: date
    output_dir: Path
    owner_email: str | None = None
    bigquery_table: str | None = None
    report_name_prefix: str = "Ritual QTD Digital Reach by Site and Date"
    metric_name: str = "uniqueReachImpressionReach"
    dimensions: tuple[str, ...] = ("date", "site", "siteId")

    @property
    def day_count(self) -> int:
        return (self.end_date - self.start_date).days + 1

    @property
    def as_of_date(self) -> str:
        return self.end_date.isoformat()

    @property
    def report_name(self) -> str:
        return f"{self.report_name_prefix} - {self.quarter_label} - {self.as_of_date}"

    @property
    def quarter_label(self) -> str:
        quarter = ((self.end_date.month - 1) // 3) + 1
        return f"{self.end_date.year}Q{quarter}"


def parse_date(value: str) -> date:
    return date.fromisoformat(value)


def default_output_dir() -> Path:
    return project_root() / "data" / "ritual_qtd_digital_reach" / "raw"


def build_report_body(config: ReachPullConfig) -> dict[str, Any]:
    return build_report_body_for_dates(
        config,
        {
            "startDate": config.start_date.isoformat(),
            "endDate": config.end_date.isoformat(),
        },
    )


def build_report_body_for_dates(config: ReachPullConfig, date_range: dict[str, str]) -> dict[str, Any]:
    body = {
        "name": config.report_name,
        "fileName": f"ritual_qtd_digital_reach_{config.quarter_label}_{config.as_of_date}",
        "type": "REACH",
        "format": "CSV",
        "reachCriteria": {
            "dateRange": date_range,
            "dimensions": [{"name": name} for name in config.dimensions],
            "reachByFrequencyMetricNames": [config.metric_name],
            "dimensionFilters": [
                {
                    "dimensionName": "advertiser",
                    "id": config.advertiser_id,
                    "value": config.advertiser_name,
                }
            ],
        },
    }
    if config.owner_email:
        body["delivery"] = {
            "emailOwner": True,
            "emailOwnerDeliveryType": "ATTACHMENT",
            "message": (
                f"Automatic Ritual QTD digital reach pull for {config.quarter_label} "
                f"through {config.as_of_date}."
            ),
            "recipients": [
                {
                    "email": config.owner_email,
                    "deliveryType": "ATTACHMENT",
                }
            ],
        }
    return body


def build_scheduled_report_body(config: ReachPullConfig, start_date: date, expiration_date: date) -> dict[str, Any]:
    body = build_report_body_for_dates(config, {"relativeDateRange": "QUARTER_TO_DATE"})
    body["name"] = "Ritual QTD Digital Reach by Site and Date - Daily Scheduled"
    body["fileName"] = "ritual_qtd_digital_reach_daily"
    body["schedule"] = {
        "active": True,
        "repeats": "DAILY",
        "every": 1,
        "startDate": start_date.isoformat(),
        "expirationDate": expiration_date.isoformat(),
        "timezone": "America/New_York",
    }
    return body


def compatible_fields(config: ReachPullConfig) -> dict[str, Any]:
    service = build_service()
    return (
        service.reports()
        .compatibleFields()
        .query(profileId=config.profile_id, body=build_report_body(config))
        .execute()
    )


def create_report(config: ReachPullConfig) -> dict[str, Any]:
    service = build_service()
    return (
        service.reports()
        .insert(profileId=config.profile_id, body=build_report_body(config))
        .execute()
    )


def find_report_by_name(profile_id: str, report_name: str) -> dict[str, Any] | None:
    service = build_service()
    page_token = None
    while True:
        response = (
            service.reports()
            .list(profileId=profile_id, maxResults=10, pageToken=page_token)
            .execute()
        )
        for report in response.get("items", []) or []:
            if report.get("name") == report_name:
                return report
        page_token = response.get("nextPageToken")
        if not page_token:
            return None


def ensure_scheduled_report(config: ReachPullConfig, start_date: date, expiration_date: date) -> dict[str, Any]:
    service = build_service()
    body = build_scheduled_report_body(config, start_date, expiration_date)
    existing = find_report_by_name(config.profile_id, body["name"])
    if existing:
        return (
            service.reports()
            .update(profileId=config.profile_id, reportId=existing["id"], body={**existing, **body})
            .execute()
        )
    return service.reports().insert(profileId=config.profile_id, body=body).execute()


def run_report(profile_id: str, report_id: str) -> dict[str, Any]:
    service = build_service()
    return (
        service.reports()
        .run(profileId=profile_id, reportId=report_id, synchronous=True)
        .execute()
    )


def wait_for_file(profile_id: str, report_id: str, file_id: str, timeout_seconds: int) -> dict[str, Any]:
    service = build_service()
    deadline = time.time() + timeout_seconds

    while True:
        file_resource = (
            service.reports()
            .files()
            .get(profileId=profile_id, reportId=report_id, fileId=file_id)
            .execute()
        )
        status = file_resource.get("status")
        if status == "REPORT_AVAILABLE":
            return file_resource
        if status == "FAILED":
            raise RuntimeError(f"CM360 report failed: {json.dumps(file_resource, indent=2)}")
        if time.time() > deadline:
            raise TimeoutError(f"Timed out waiting for report file {file_id}. Last status: {status}")
        time.sleep(10)


def download_file(profile_id: str, report_id: str, file_id: str, output_path: Path) -> Path:
    service = build_service()
    request = (
        service.reports()
        .files()
        .get_media(profileId=profile_id, reportId=report_id, fileId=file_id)
    )

    output_path.parent.mkdir(parents=True, exist_ok=True)
    with output_path.open("wb") as file_handle:
        downloader = MediaIoBaseDownload(file_handle, request)
        done = False
        while not done:
            _status, done = downloader.next_chunk()
    return output_path


def save_metadata(
    config: ReachPullConfig,
    report: dict[str, Any],
    file_resource: dict[str, Any],
    output_path: Path,
) -> Path:
    metadata_path = output_path.with_suffix(".metadata.json")
    payload = {
        "config": {
            **asdict(config),
            "start_date": config.start_date.isoformat(),
            "end_date": config.end_date.isoformat(),
            "output_dir": str(config.output_dir),
            "dimensions": list(config.dimensions),
            "quarter_label": config.quarter_label,
        },
        "report": report,
        "file": file_resource,
        "output_path": str(output_path),
    }
    metadata_path.write_text(json.dumps(payload, indent=2), encoding="utf-8")
    return metadata_path


def handoff_report_file(config: ReachPullConfig, timeout_seconds: int) -> tuple[Path, dict[str, Any]]:
    report = create_report(config)
    report_id = report["id"]

    file_resource = run_report(config.profile_id, report_id)
    file_id = file_resource["id"]
    if file_resource.get("status") != "REPORT_AVAILABLE":
        file_resource = wait_for_file(config.profile_id, report_id, file_id, timeout_seconds)

    handoff_path = (
        config.output_dir
        / f"ritual_qtd_digital_reach_{config.quarter_label}_{config.as_of_date}.browser_handoff.json"
    )
    handoff_path.parent.mkdir(parents=True, exist_ok=True)
    payload = {
        "status": "REPORT_AVAILABLE",
        "quarter_label": config.quarter_label,
        "date_range": {
            "start_date": config.start_date.isoformat(),
            "end_date": config.end_date.isoformat(),
        },
        "profile_id": config.profile_id,
        "advertiser_id": config.advertiser_id,
        "advertiser_name": config.advertiser_name,
        "report_id": report_id,
        "file_id": file_id,
        "browser_url": file_resource.get("urls", {}).get("browserUrl"),
        "api_url": file_resource.get("urls", {}).get("apiUrl"),
        "file_resource": file_resource,
        "report": report,
    }
    handoff_path.write_text(json.dumps(payload, indent=2), encoding="utf-8")
    save_metadata(config, report, file_resource, handoff_path)
    return handoff_path, payload


def pull_reach(config: ReachPullConfig, timeout_seconds: int) -> tuple[Path, Path]:
    report = create_report(config)
    report_id = report["id"]

    file_resource = run_report(config.profile_id, report_id)
    file_id = file_resource["id"]
    if file_resource.get("status") != "REPORT_AVAILABLE":
        file_resource = wait_for_file(config.profile_id, report_id, file_id, timeout_seconds)

    csv_path = (
        config.output_dir
        / f"ritual_qtd_digital_reach_{config.quarter_label}_{config.as_of_date}.csv"
    )
    download_file(config.profile_id, report_id, file_id, csv_path)
    metadata_path = save_metadata(config, report, file_resource, csv_path)
    return csv_path, metadata_path


def explain_http_error(error: HttpError) -> str:
    message = str(error)
    if "insufficientPermissions" in message:
        return "Google auth is missing a required Campaign Manager reporting permission."
    if "notCompatible" in message or "incompatible" in message:
        return "CM360 rejected this dimension/metric combination as incompatible."
    if "invalid" in message:
        return f"CM360 rejected the report request: {message}"
    return message
