from __future__ import annotations

import argparse
from datetime import date
from pathlib import Path

from dcm_api.reach_pipeline import ensure_scheduled_report
from scripts.pull_ritual_ytd_exclusive_reach import current_quarter_start, load_config


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Create or update the daily scheduled CM360 Ritual QTD digital reach report."
    )
    parser.add_argument("--start-date", default=date.today().isoformat())
    parser.add_argument("--expiration-date", default=f"{date.today().year}-12-31")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    config_path = Path(__file__).resolve().parents[1] / "config" / "ritual_qtd_digital_reach.json"
    config = load_config(config_path, current_quarter_start(), date.today())
    report = ensure_scheduled_report(
        config,
        date.fromisoformat(args.start_date),
        date.fromisoformat(args.expiration_date),
    )
    print("Scheduled CM360 report is active")
    print(f"- report_id: {report['id']}")
    print(f"- name: {report['name']}")
    print(f"- fileName: {report['fileName']}")
    print(f"- schedule: {report.get('schedule')}")
    print(f"- delivery: {report.get('delivery')}")


if __name__ == "__main__":
    main()
