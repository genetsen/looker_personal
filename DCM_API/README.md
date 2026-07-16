# Campaign Manager 360 API Starter

This folder is a small Python starter for connecting to the Google Campaign
Manager 360 API directly, without a third-party connector.

It starts with two safe read-only commands:

- List the Campaign Manager profiles your Google account can access.
- List saved reports under one profile.

## What You Need

1. A Google Cloud project with the Campaign Manager 360 API enabled.
2. Existing Google application-default credentials from `gcloud`, or an OAuth desktop app credential as a temporary fallback.
3. Campaign Manager 360 access for the Google account you will sign in with.

If you need the temporary fallback path, save the downloaded OAuth JSON file here:

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

The code now prefers one shared Google auth path inside this repo:

1. Try existing Google application-default credentials first.
2. Use local OAuth files only as a temporary fallback during migration or on machines without working ADC.

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

If application-default credentials are missing entirely, the CM360 client falls back
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

## Ritual Gmail Loader Auth

The Ritual email-to-BigQuery loader follows the same auth preference:

1. Try application-default credentials with Gmail read-only scope.
2. Fall back temporarily to the legacy shared Gmail pickle token at `~/.cache/gmail_token.pickle`.

To refresh ADC for both CM360 and Gmail on one machine, use a combined scope login:

```bash
gcloud auth application-default login --scopes=https://www.googleapis.com/auth/cloud-platform,https://www.googleapis.com/auth/dfareporting,https://www.googleapis.com/auth/gmail.readonly
```

This is the repo's preferred path going forward because multiple scripts can
share one Google auth store instead of keeping separate per-script token files.

## Notes

- The default API version is `v5`, set in `.env.example`.
- The starter uses the `https://www.googleapis.com/auth/dfareporting` OAuth
  scope, which is enough for read/write reporting workflows.
- The Ritual Gmail attachment loader uses `https://www.googleapis.com/auth/gmail.readonly`.
- The current scripts only read metadata. They do not create, update, or delete
  Campaign Manager objects.

## Official Docs

- [Campaign Manager 360 API](https://developers.google.com/doubleclick-advertisers/rest/v5)
- [Campaign Manager 360 API overview](https://support.google.com/campaignmanager/answer/2835059)
- [reports.get](https://developers.google.com/doubleclick-advertisers/rest/v5/reports/get)
