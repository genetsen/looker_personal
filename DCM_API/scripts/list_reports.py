from __future__ import annotations

import argparse

from googleapiclient.errors import HttpError

from dcm_api.client import build_service


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="List saved Campaign Manager 360 reports for one profile."
    )
    parser.add_argument("profile_id", help="Campaign Manager 360 profile ID")
    parser.add_argument("--max-results", type=int, default=25)
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    service = build_service()
    try:
        response = (
            service.reports()
            .list(profileId=args.profile_id, maxResults=args.max_results)
            .execute()
        )
    except HttpError as error:
        message = str(error)
        if "accessNotConfigured" in message:
            print(
                "Campaign Manager 360 API is not enabled for the current "
                "Google Cloud quota project."
            )
            print(
                "Enable dfareporting.googleapis.com in Google Cloud, then "
                "run this command again."
            )
            return
        if "insufficientPermissions" in message:
            print(
                "Google credentials were found, but they do not include the "
                "Campaign Manager 360 reporting scope."
            )
            print(
                "Refresh application-default auth with the dfareporting "
                "scope, then run this command again."
            )
            return
        raise

    reports = response.get("items", [])

    if not reports:
        print(f"No reports found for profile_id={args.profile_id}.")
        return

    print(f"Reports for profile_id={args.profile_id}:")
    for report in reports:
        report_id = report.get("id")
        report_name = report.get("name", "(unnamed report)")
        report_type = report.get("type", "(unknown type)")
        print(f"- report_id={report_id} | type={report_type} | {report_name}")


if __name__ == "__main__":
    main()
