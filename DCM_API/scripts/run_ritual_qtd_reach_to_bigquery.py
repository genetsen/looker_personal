from __future__ import annotations

import argparse
import time
from datetime import date
from pathlib import Path

from dcm_api.bigquery_load import load_csv_to_bigquery, preview_csv
from dcm_api.gmail_reports import download_attachment, find_report_attachment
from dcm_api.reach_pipeline import create_report, run_report
from scripts.pull_ritual_ytd_exclusive_reach import current_quarter_start, load_config


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Run Ritual QTD digital reach in CM360, pick up the emailed CSV, and load it to BigQuery."
    )
    parser.add_argument("--start-date", default=current_quarter_start().isoformat())
    parser.add_argument("--end-date", default=date.today().isoformat())
    parser.add_argument("--email-timeout-seconds", type=int, default=900)
    parser.add_argument("--config", default=None)
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    config_path = (
        Path(args.config)
        if args.config
        else Path(__file__).resolve().parents[1] / "config" / "ritual_qtd_digital_reach.json"
    )
    config = load_config(
        config_path,
        date.fromisoformat(args.start_date),
        date.fromisoformat(args.end_date),
    )
    if not config.bigquery_table:
        raise ValueError("Config must include bigquery_table.")

    started_at = int(time.time())
    expected_prefix = f"ritual_qtd_digital_reach_{config.quarter_label}_{config.as_of_date}"

    print("Ritual QTD digital reach to BigQuery")
    print(f"- quarter: {config.quarter_label}")
    print(f"- date range: {config.start_date.isoformat()} to {config.end_date.isoformat()}")
    print(f"- BigQuery table: {config.bigquery_table}")
    print(f"- email attachment prefix: {expected_prefix}")

    report = create_report(config)
    file_resource = run_report(config.profile_id, report["id"])
    print(f"- CM360 report_id: {report['id']}")
    print(f"- CM360 file_id: {file_resource['id']}")
    print(f"- CM360 file status: {file_resource.get('status')}")

    message, part = find_report_attachment(
        subject_query=config.report_name,
        expected_filename_prefix=expected_prefix,
        after_epoch_seconds=started_at,
        timeout_seconds=args.email_timeout_seconds,
    )
    csv_path = download_attachment(message, part, config.output_dir)
    print(f"- downloaded attachment: {csv_path}")
    print(f"- preview rows: {preview_csv(csv_path, row_count=2)}")

    load_job = load_csv_to_bigquery(csv_path, config.bigquery_table)
    print(f"- loaded BigQuery job: {load_job.job_id}")
    print("- status: complete")


if __name__ == "__main__":
    main()
