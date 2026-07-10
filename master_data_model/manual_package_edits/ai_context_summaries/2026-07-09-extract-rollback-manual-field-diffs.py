"""Extract field-level manual differences from the rollback workbook.

The rollback workbook has visible editor columns, hidden baseline columns, and
hidden manual-marker columns. This script emits one row per active manual marker
with the visible value, baseline value, and numeric delta when possible.
It reads only the local XLSX export and writes local CSV evidence.
"""

from __future__ import annotations

import csv
import math
import re
from datetime import date, datetime
from pathlib import Path
from typing import Any

import openpyxl


ROOT = Path(
    "/Users/eugenetsenter/Looker_clonedRepo/looker_personal/master_data_model/manual_package_edits"
)
OUT_DIR = ROOT / "ai_context_summaries"
XLSX_PATH = OUT_DIR / "2026-07-09-rollback-copy-export.xlsx"
OUT_PATH = OUT_DIR / "2026-07-09-rollback-copy-manual-field-diffs.csv"


FIELD_MAP = {
    "Flight Start Date": ("Flight Start Date", "Baseline Flight Start Date"),
    "Flight End Date": ("Flight End Date", "Baseline Flight End Date"),
    "Planned Spend": ("Planned Spend", "Baseline Planned Spend"),
    "Planned Impressions": ("Planned Impressions", "Baseline Planned Impressions"),
    "Spend": ("Spend", "Baseline Spend"),
    "Impressions": ("Impressions", "Baseline Impressions"),
    "Clicks": ("Clicks", "Baseline Clicks"),
    "Video Plays": ("Video Plays", "Baseline Video Plays"),
    "Video Completions": ("Video Completions", "Baseline Video Completions"),
    "Delivery Start Date": ("Delivery Override Start Date", "Baseline Delivery Start Date"),
    "Delivery End Date": ("Delivery Override End Date", "Baseline Delivery End Date"),
    "Advertiser": ("Advertiser", "Baseline Advertiser"),
    "Package Name": ("Package Name", "Baseline Package Name"),
    "Package Friendly Name": ("Package Friendly Name", "Baseline Package Friendly Name"),
}


def clean(value: Any) -> str:
    if value is None:
        return ""
    if isinstance(value, datetime):
        return value.isoformat(sep=" ")
    if isinstance(value, date):
        return value.isoformat()
    return str(value).strip()


def dedupe_headers(raw_headers: list[str]) -> list[str]:
    seen: dict[str, int] = {}
    out: list[str] = []
    for index, header in enumerate(raw_headers, start=1):
        name = header.strip() if header else f"blank_header_{index}"
        if name in seen:
            seen[name] += 1
            name = f"{name}__{seen[name]}"
        else:
            seen[name] = 1
        out.append(name)
    return out


def parse_number(value: str) -> float | None:
    if not value:
        return None
    cleaned = re.sub(r"[$,% ,]", "", value)
    try:
        number = float(cleaned)
    except ValueError:
        return None
    if math.isnan(number):
        return None
    return number


def is_social_or_amazon(row: dict[str, str]) -> bool:
    text = " ".join(
        row.get(name, "")
        for name in [
            "Package ID",
            "Site",
            "Package Friendly Name",
            "Package Name",
            "Campaign Name",
        ]
    ).lower()
    blocked_terms = [
        "social:",
        "facebook",
        "instagram",
        "tiktok",
        "reddit",
        "google_ads",
        "amazon ads",
        "amazon_ads",
        "amazonadvertising.com",
        "amaadv_",
    ]
    return any(term in text for term in blocked_terms)


def is_2026(row: dict[str, str]) -> bool:
    text = " ".join(
        row.get(name, "")
        for name in [
            "Flight Start Date",
            "Flight End Date",
            "Delivery Override Start Date",
            "Delivery Override End Date",
            "Package Friendly Name",
            "Package Name",
            "Campaign Name",
        ]
    )
    return "2026" in text or re.search(r"\b26\d{4}\|26\d{4}\b", text) is not None


def main() -> None:
    workbook = openpyxl.load_workbook(XLSX_PATH, data_only=True, read_only=False)
    sheet = workbook["Package Editor"]
    headers = dedupe_headers([clean(cell.value) for cell in sheet[4]])

    output_rows: list[dict[str, str]] = []
    for sheet_row, values in enumerate(
        sheet.iter_rows(min_row=5, max_row=sheet.max_row, values_only=True),
        start=5,
    ):
        row = {"sheet_row": str(sheet_row)}
        row.update({header: clean(value) for header, value in zip(headers, values)})

        package_id = row.get("Package ID", "").strip()
        if not package_id or is_social_or_amazon(row) or not is_2026(row):
            continue

        for field_name, (visible_col, baseline_col) in FIELD_MAP.items():
            marker_col = f"Manual Marker {field_name}"
            if row.get(marker_col, "").strip().lower() not in {"true", "yes", "1"}:
                continue

            visible_value = row.get(visible_col, "")
            baseline_value = row.get(baseline_col, "")
            visible_number = parse_number(visible_value)
            baseline_number = parse_number(baseline_value)
            numeric_delta = ""
            abs_numeric_delta = ""
            if visible_number is not None and baseline_number is not None:
                delta = visible_number - baseline_number
                numeric_delta = str(delta)
                abs_numeric_delta = str(abs(delta))

            output_rows.append(
                {
                    "sheet_row": row["sheet_row"],
                    "package_friendly_name": row.get("Package Friendly Name", ""),
                    "package_id": package_id,
                    "advertiser": row.get("Advertiser", ""),
                    "site": row.get("Site", ""),
                    "campaign_name": row.get("Campaign Name", ""),
                    "flight_start_date": row.get("Flight Start Date", ""),
                    "flight_end_date": row.get("Flight End Date", ""),
                    "changed_field": field_name,
                    "visible_value": visible_value,
                    "baseline_value": baseline_value,
                    "numeric_delta": numeric_delta,
                    "abs_numeric_delta": abs_numeric_delta,
                    "validation_status": row.get("Validation Status", ""),
                    "validation_reason": row.get("Validation Reason", ""),
                }
            )

    output_rows.sort(
        key=lambda item: float(item["abs_numeric_delta"] or "-1"),
        reverse=True,
    )

    fieldnames = [
        "sheet_row",
        "package_friendly_name",
        "package_id",
        "advertiser",
        "site",
        "campaign_name",
        "flight_start_date",
        "flight_end_date",
        "changed_field",
        "visible_value",
        "baseline_value",
        "numeric_delta",
        "abs_numeric_delta",
        "validation_status",
        "validation_reason",
    ]
    with OUT_PATH.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(output_rows)

    print(f"manual_field_diffs={len(output_rows)} output={OUT_PATH}")


if __name__ == "__main__":
    main()
