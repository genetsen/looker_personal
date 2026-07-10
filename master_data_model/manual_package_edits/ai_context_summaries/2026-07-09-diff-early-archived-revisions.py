"""Diff early archived Manual Data Editor revisions.

This downloads the archived workbook revisions that existed before the prior
June 26 cutoff and compares adjacent Package Editor cells. It writes local CSV
evidence only and does not edit Google Drive, Sheets, or BigQuery.
"""

from __future__ import annotations

import csv
import subprocess
import zipfile
from datetime import date, datetime
from pathlib import Path
from typing import Any

import openpyxl


ROOT = Path(
    "/Users/eugenetsenter/Looker_clonedRepo/looker_personal/master_data_model/manual_package_edits"
)
OUT_DIR = ROOT / "ai_context_summaries"
EXPORT_DIR = OUT_DIR / "revision_exports_xlsx"
REVISION_CSV = OUT_DIR / "2026-07-09-drive-manual-editor-workbook-revisions.csv"
ARCHIVED_FILE_ID = "1WerhrbBMggzCwIUCOsOCV33aHygV96jt1HgqiYcUHZo"
MAX_TIME = "2026-06-26T19:19:17.512Z"


def clean(value: Any) -> str:
    if value is None:
        return ""
    if isinstance(value, datetime):
        return value.isoformat(sep=" ")
    if isinstance(value, date):
        return value.isoformat()
    return str(value).strip()


def safe_filename_time(value: str) -> str:
    return value.replace("-", "").replace(":", "").replace(".", "").replace("Z", "Z")


def get_token() -> str:
    return subprocess.check_output(
        ["gcloud", "auth", "application-default", "print-access-token"],
        text=True,
    ).strip()


def download_revision(token: str, revision: dict[str, str]) -> Path:
    revision_id = revision["revision_id"]
    modified_time = revision["modified_time"]
    path = EXPORT_DIR / (
        f"older_archived__rev_{revision_id}__{safe_filename_time(modified_time)}__early.xlsx"
    )
    if path.exists() and zipfile.is_zipfile(path):
        print(f"using cached revision {revision_id}", flush=True)
        return path

    print(f"downloading revision {revision_id} {modified_time}", flush=True)
    url = (
        "https://docs.google.com/spreadsheets/export"
        f"?id={ARCHIVED_FILE_ID}&revision={revision_id}&exportFormat=xlsx"
    )
    subprocess.run(
        [
            "curl",
            "-sS",
            "--fail",
            "--show-error",
            "-L",
            "--location-trusted",
            "--connect-timeout",
            "15",
            "--max-time",
            "60",
            "--retry",
            "2",
            "-H",
            f"Authorization: Bearer {token}",
            "-o",
            str(path),
            url,
        ],
        check=True,
    )
    if not zipfile.is_zipfile(path):
        preview = path.read_text(errors="replace")[:500]
        raise RuntimeError(f"Downloaded revision {revision_id} is not XLSX: {preview}")
    return path


def read_package_editor(path: Path) -> tuple[list[str], list[list[str]]]:
    workbook = openpyxl.load_workbook(path, data_only=True, read_only=False)
    sheet = workbook["Package Editor"]
    rows = [
        [clean(cell) for cell in row]
        for row in sheet.iter_rows(min_row=1, max_row=sheet.max_row, values_only=True)
    ]
    headers = rows[3] if len(rows) >= 4 else []
    return headers, rows


def row_value(rows: list[list[str]], row_index: int, header_index: dict[str, int], name: str) -> str:
    col_index = header_index.get(name)
    if col_index is None or row_index >= len(rows) or col_index >= len(rows[row_index]):
        return ""
    return rows[row_index][col_index]


def cell_name(row_number: int, col_number: int) -> str:
    letters = ""
    col = col_number
    while col:
        col, rem = divmod(col - 1, 26)
        letters = chr(65 + rem) + letters
    return f"{letters}{row_number}"


def diff_pair(
    before_revision: dict[str, str],
    after_revision: dict[str, str],
    before_path: Path,
    after_path: Path,
) -> tuple[dict[str, str], list[dict[str, str]]]:
    before_headers, before_rows = read_package_editor(before_path)
    after_headers, after_rows = read_package_editor(after_path)
    header_count = max(len(before_headers), len(after_headers))
    max_rows = max(len(before_rows), len(after_rows))
    max_cols = max(
        max((len(row) for row in before_rows), default=0),
        max((len(row) for row in after_rows), default=0),
    )

    after_header_index = {header: index for index, header in enumerate(after_headers)}
    before_header_index = {header: index for index, header in enumerate(before_headers)}
    details: list[dict[str, str]] = []
    changed_rows: set[int] = set()
    changed_cols: set[int] = set()

    for row_index in range(max_rows):
        before_row = before_rows[row_index] if row_index < len(before_rows) else []
        after_row = after_rows[row_index] if row_index < len(after_rows) else []
        for col_index in range(max_cols):
            before_value = before_row[col_index] if col_index < len(before_row) else ""
            after_value = after_row[col_index] if col_index < len(after_row) else ""
            if before_value == after_value:
                continue
            sheet_row = row_index + 1
            sheet_col = col_index + 1
            changed_rows.add(sheet_row)
            changed_cols.add(sheet_col)
            header = ""
            if col_index < len(after_headers) and after_headers[col_index]:
                header = after_headers[col_index]
            elif col_index < len(before_headers):
                header = before_headers[col_index]
            package_friendly_name = row_value(after_rows, row_index, after_header_index, "Package Friendly Name") or row_value(
                before_rows, row_index, before_header_index, "Package Friendly Name"
            )
            package_id = row_value(after_rows, row_index, after_header_index, "Package ID") or row_value(
                before_rows, row_index, before_header_index, "Package ID"
            )
            advertiser = row_value(after_rows, row_index, after_header_index, "Advertiser") or row_value(
                before_rows, row_index, before_header_index, "Advertiser"
            )
            site = row_value(after_rows, row_index, after_header_index, "Site") or row_value(
                before_rows, row_index, before_header_index, "Site"
            )
            details.append(
                {
                    "before_revision": before_revision["revision_id"],
                    "after_revision": after_revision["revision_id"],
                    "after_modified_time": after_revision["modified_time"],
                    "cell": cell_name(sheet_row, sheet_col),
                    "sheet_row": str(sheet_row),
                    "sheet_col": str(sheet_col),
                    "header": header,
                    "package_friendly_name": package_friendly_name,
                    "package_id": package_id,
                    "advertiser": advertiser,
                    "site": site,
                    "before_value": before_value,
                    "after_value": after_value,
                }
            )

    changed_cells = len(details)
    if changed_cells <= 20 and len(changed_rows) <= 3:
        classification = "small-cell-edit"
    elif changed_cells <= 1000:
        classification = "medium-edit"
    elif changed_cells == 0:
        classification = "no-change"
    else:
        classification = "bulk-or-refresh"

    summary = {
        "before_revision": before_revision["revision_id"],
        "after_revision": after_revision["revision_id"],
        "before_modified_time": before_revision["modified_time"],
        "after_modified_time": after_revision["modified_time"],
        "changed_cells": str(changed_cells),
        "changed_rows": str(len(changed_rows)),
        "changed_cols": str(len(changed_cols)),
        "header_cols": str(header_count),
        "classification": classification,
    }
    return summary, details


def main() -> None:
    EXPORT_DIR.mkdir(parents=True, exist_ok=True)
    with REVISION_CSV.open(newline="", encoding="utf-8") as handle:
        revisions = [
            row
            for row in csv.DictReader(handle)
            if row["file_id"] == ARCHIVED_FILE_ID
            and row["modified_time"]
            and row["modified_time"] <= MAX_TIME
        ]
    revisions.sort(key=lambda row: row["modified_time"])
    if len(revisions) < 2:
        raise RuntimeError("Need at least two early revisions to diff.")

    token = get_token()
    paths = {row["revision_id"]: download_revision(token, row) for row in revisions}

    summaries: list[dict[str, str]] = []
    all_details: list[dict[str, str]] = []
    for before, after in zip(revisions, revisions[1:]):
        summary, details = diff_pair(before, after, paths[before["revision_id"]], paths[after["revision_id"]])
        summaries.append(summary)
        all_details.extend(details)

    summary_path = OUT_DIR / "2026-07-09-archived-early-revision-cell-diff-summary.csv"
    detail_path = OUT_DIR / "2026-07-09-archived-early-revision-cell-diff-details.csv"
    with summary_path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=list(summaries[0].keys()))
        writer.writeheader()
        writer.writerows(summaries)
    detail_fields = [
        "before_revision",
        "after_revision",
        "after_modified_time",
        "cell",
        "sheet_row",
        "sheet_col",
        "header",
        "package_friendly_name",
        "package_id",
        "advertiser",
        "site",
        "before_value",
        "after_value",
    ]
    with detail_path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=detail_fields)
        writer.writeheader()
        writer.writerows(all_details)

    print(f"early_revision_pairs={len(summaries)}")
    print(f"summary={summary_path}")
    print(f"details={detail_path}")


if __name__ == "__main__":
    main()
