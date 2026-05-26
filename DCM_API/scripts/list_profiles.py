from __future__ import annotations

from googleapiclient.errors import HttpError

from dcm_api.client import build_service


def main() -> None:
    service = build_service()
    try:
        response = service.userProfiles().list().execute()
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

    profiles = response.get("items", [])

    if not profiles:
        print("No Campaign Manager 360 profiles were returned for this account.")
        return

    print("Campaign Manager 360 profiles:")
    for profile in profiles:
        profile_id = profile.get("profileId")
        account_id = profile.get("accountId")
        account_name = profile.get("accountName", "(unnamed account)")
        user_name = profile.get("userName", "(unnamed user)")
        print(f"- profile_id={profile_id} | account_id={account_id} | {account_name} | {user_name}")


if __name__ == "__main__":
    main()
