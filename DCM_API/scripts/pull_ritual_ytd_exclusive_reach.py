from __future__ import annotations

import argparse
import json
from datetime import date
from pathlib import Path

from googleapiclient.errors import HttpError

from dcm_api.client import project_root
from dcm_api.reach_pipeline import (
    ReachPullConfig,
    compatible_fields,
    default_output_dir,
    explain_http_error,
    handoff_report_file,
    parse_date,
    pull_reach,
)


DEFAULT_CONFIG_PATH = project_root() / "config" / "ritual_qtd_digital_reach.json"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Pull Ritual QTD digital reach by date and site from Campaign Manager 360."
    )
    parser.add_argument("--config", default=str(DEFAULT_CONFIG_PATH))
    parser.add_argument("--start-date", default=current_quarter_start().isoformat())
    parser.add_argument("--end-date", default=date.today().isoformat())
    parser.add_argument("--dry-run-compatible", action="store_true")
    parser.add_argument("--browser-handoff", action="store_true")
    parser.add_argument("--timeout-seconds", type=int, default=900)
    return parser.parse_args()


def current_quarter_start() -> date:
    today = date.today()
    start_month = ((today.month - 1) // 3) * 3 + 1
    return date(today.year, start_month, 1)


def load_config(path: Path, start_date: date, end_date: date) -> ReachPullConfig:
    raw = json.loads(path.read_text(encoding="utf-8"))
    output_dir = Path(raw.get("output_dir") or default_output_dir())
    if not output_dir.is_absolute():
        output_dir = project_root() / output_dir

    return ReachPullConfig(
        profile_id=str(raw["profile_id"]),
        advertiser_id=str(raw["advertiser_id"]),
        advertiser_name=str(raw["advertiser_name"]),
        owner_email=raw.get("owner_email"),
        bigquery_table=raw.get("bigquery_table"),
        metric_name=str(raw["metric_name"]),
        dimensions=tuple(raw["dimensions"]),
        start_date=start_date,
        end_date=end_date,
        output_dir=output_dir,
    )


def main() -> None:
    args = parse_args()
    config = load_config(Path(args.config), parse_date(args.start_date), parse_date(args.end_date))

    print("Ritual QTD digital reach pull")
    print(f"- profile_id: {config.profile_id}")
    print(f"- advertiser: {config.advertiser_name} ({config.advertiser_id})")
    print(f"- quarter: {config.quarter_label}")
    print(f"- date range: {config.start_date.isoformat()} to {config.end_date.isoformat()}")
    print(f"- window: {config.day_count} days")
    print(f"- dimensions: {', '.join(config.dimensions)}")
    print(f"- metric: {config.metric_name}")

    if args.dry_run_compatible:
        fields = compatible_fields(config)
        print(json.dumps(fields, indent=2))
        return

    if args.browser_handoff:
        handoff_path, payload = handoff_report_file(config, args.timeout_seconds)
        print(f"Saved browser handoff: {handoff_path}")
        print(f"Open/export URL: {payload.get('browser_url')}")
        return

    try:
        csv_path, metadata_path = pull_reach(config, args.timeout_seconds)
    except HttpError as error:
        print(explain_http_error(error))
        raise

    print(f"Saved CSV: {csv_path}")
    print(f"Saved metadata: {metadata_path}")


if __name__ == "__main__":
    main()
