# Campaign Manager 360 API Starter

This folder is a small Python starter for connecting to the Google Campaign
Manager 360 API directly, without a third-party connector.

It starts with two safe read-only commands:

- List the Campaign Manager profiles your Google account can access.
- List saved reports under one profile.

## What You Need

1. A Google Cloud project with the Campaign Manager 360 API enabled.
2. An OAuth desktop app credential downloaded from Google Cloud.
3. Campaign Manager 360 access for the Google account you will sign in with.

Save the downloaded OAuth JSON file here:

```text
secrets/client_secrets.json
```

That `secrets/` folder is ignored by Git so credentials do not get committed.

## Setup

This starter is meant to run with your existing Python environment:

```bash
/Users/eugenetsenter/virtenvi-2025/bin/python
```

Check whether the needed Google packages are already installed:

```bash
/Users/eugenetsenter/virtenvi-2025/bin/python -c "import googleapiclient, google_auth_oauthlib, google.auth; print('Google API packages: OK')"
```

If that command fails, install dependencies into that existing environment:

```bash
/Users/eugenetsenter/virtenvi-2025/bin/python -m pip install -r requirements.txt
```

The code first tries existing Google application-default credentials. If those
are not available, it falls back to a local OAuth file.

Optional local environment file:

```bash
cp .env.example .env
```

## Smoke Test: List Profiles

Run:

```bash
/Users/eugenetsenter/virtenvi-2025/bin/python -m scripts.list_profiles
```

If existing application-default credentials have the Campaign Manager scope, the
script prints profiles directly.

If you see `insufficient authentication scopes`, refresh the existing Google
application-default login with the Campaign Manager scope:

```bash
gcloud auth application-default login --scopes=https://www.googleapis.com/auth/cloud-platform,https://www.googleapis.com/auth/dfareporting
```

That does not require adding a project OAuth file. It refreshes the local
Google Cloud SDK application-default credential.

If application-default credentials are missing entirely, the script falls back
to browser OAuth using:

```text
secrets/client_secrets.json
```

After browser approval, the fallback token is saved at:

```text
secrets/token.json
```

The expected output is a list of Campaign Manager 360 profiles, including a
`profile_id`. You need that `profile_id` for most reporting API calls.

## List Saved Reports

After you have a profile ID:

```bash
/Users/eugenetsenter/virtenvi-2025/bin/python -m scripts.list_reports PROFILE_ID
```

Replace `PROFILE_ID` with the profile ID from the smoke test.

## Notes

- The default API version is `v5`, set in `.env.example`.
- The starter uses the `https://www.googleapis.com/auth/dfareporting` OAuth
  scope, which is enough for read/write reporting workflows.
- The current scripts only read metadata. They do not create, update, or delete
  Campaign Manager objects.

## Official Docs

- [Campaign Manager 360 API](https://developers.google.com/doubleclick-advertisers/rest/v5)
- [Campaign Manager 360 API overview](https://support.google.com/campaignmanager/answer/2835059)
- [reports.get](https://developers.google.com/doubleclick-advertisers/rest/v5/reports/get)
