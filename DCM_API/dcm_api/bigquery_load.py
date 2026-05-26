from __future__ import annotations

import csv
import re
from pathlib import Path

from google.cloud import bigquery


def clean_cm360_reach_csv(raw_csv_path: Path) -> Path:
    clean_path = raw_csv_path.with_name(raw_csv_path.stem + ".clean.csv")
    rows = list(csv.reader(raw_csv_path.open(newline="", encoding="utf-8-sig")))

    header_index = None
    for index, row in enumerate(rows):
        if row and row[0] == "Report Fields":
            header_index = index + 1
            break
    if header_index is None or header_index >= len(rows):
        raise ValueError(f"Could not find CM360 Report Fields header in {raw_csv_path}")

    header = [normalize_column_name(value) for value in rows[header_index]]
    data_rows = rows[header_index + 1 :]

    with clean_path.open("w", newline="", encoding="utf-8") as clean_file:
        writer = csv.writer(clean_file)
        writer.writerow(header)
        for row in data_rows:
            if not row or not row[0] or row[0].startswith("Grand Total"):
                continue
            clean_row = [None if value == "-" else value for value in row]
            writer.writerow(clean_row)
    return clean_path


def normalize_column_name(value: str) -> str:
    value = value.strip().lower()
    value = value.replace("(cm360)", "")
    value = value.replace("+", "plus")
    value = value.replace(":", "")
    value = re.sub(r"[^a-z0-9]+", "_", value)
    value = value.strip("_")
    value = re.sub(r"_+", "_", value)
    if value and value[0].isdigit():
        value = f"freq_{value}"
    return value or "unnamed"


def load_csv_to_bigquery(csv_path: Path, table_id: str) -> bigquery.job.LoadJob:
    clean_path = clean_cm360_reach_csv(csv_path)
    client = bigquery.Client(project=table_id.split(".")[0])
    job_config = bigquery.LoadJobConfig(
        source_format=bigquery.SourceFormat.CSV,
        skip_leading_rows=1,
        autodetect=True,
        write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
    )
    with clean_path.open("rb") as source_file:
        job = client.load_table_from_file(source_file, table_id, job_config=job_config)
    job.result()
    return job


def preview_csv(csv_path: Path, row_count: int = 5) -> list[dict[str, str]]:
    preview_path = clean_cm360_reach_csv(csv_path)
    with preview_path.open(newline="", encoding="utf-8-sig") as csv_file:
        reader = csv.DictReader(csv_file)
        return [row for _, row in zip(range(row_count), reader)]
