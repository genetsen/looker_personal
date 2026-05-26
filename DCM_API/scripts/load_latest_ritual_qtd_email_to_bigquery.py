from __future__ import annotations

from datetime import date
from pathlib import Path

from dcm_api.bigquery_load import load_csv_to_bigquery, preview_csv
from dcm_api.gmail_reports import download_attachment, find_latest_report_attachment
from scripts.pull_ritual_ytd_exclusive_reach import current_quarter_start, load_config


def main() -> None:
    config_path = Path(__file__).resolve().parents[1] / "config" / "ritual_qtd_digital_reach.json"
    config = load_config(config_path, current_quarter_start(), date.today())
    subject_query = "Ritual QTD Digital Reach by Site and Date - Daily Scheduled"
    expected_prefix = "ritual_qtd_digital_reach_daily"

    message, part = find_latest_report_attachment(subject_query, expected_prefix)
    csv_path = download_attachment(message, part, config.output_dir)
    print(f"Downloaded latest scheduled attachment: {csv_path}")
    print(f"Preview rows: {preview_csv(csv_path, row_count=2)}")

    if not config.bigquery_table:
        raise ValueError("Config must include bigquery_table.")
    load_job = load_csv_to_bigquery(csv_path, config.bigquery_table)
    print(f"Loaded BigQuery table: {config.bigquery_table}")
    print(f"BigQuery job: {load_job.job_id}")


if __name__ == "__main__":
    main()
