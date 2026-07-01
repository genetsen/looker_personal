# Clean and Load Reddit Ads CSV to BigQuery and Update Stg View

We are resuming work to load the daily Reddit Ads email-ingested CSV data into BigQuery and update the staging view.

## User Review Required

We are using a custom R script with `bigrquery` to perform robust parsing, cleanup, and loading.

> [!IMPORTANT]
> - We will **skip** the `CPC (USD)` column entirely as per your instruction.
> - We will **overwrite** the current table schema of `looker-studio-pro-452620.landing.reddit-ads-email`.
> - We will **map and load** the actual campaign, ad group, and ad IDs which are now available in the new raw CSV, and update the staging view `stg__olipop_reddit_crossplatform` to project these real IDs instead of casting `NULL AS STRING`.

## Proposed Changes

### 1. Data Loader Utility [NEW]
We will create a new temporary R utility `load_reddit_csv.r` in the `master_data_model/tmp/` directory to safely load the CSV.

#### [NEW] [load_reddit_csv.r](file:///Users/eugenetsenter/Looker_clonedRepo/looker_personal/master_data_model/tmp/load_reddit_csv.r)
- Reads the raw scheduled CSV `master_data_model/tmp/Reddit-Olipop-scheaduled (Copy) (2).csv`
- Drops the `CPC (USD)` column.
- Cleans parenthesis from remaining headers to match BigQuery requirements (e.g. `Amount Spent (USD)` -> `Amount Spent _USD_`).
- Parses dates and timestamps correctly to prevent BigQuery ingestion type-mismatch errors.
- Uploads the cleaned dataframe into `looker-studio-pro-452620.landing.reddit-ads-email` with overwrite enabled.

---

### 2. SQL Staging View [MODIFY]
We will update the staging view definition file to leverage the new schema and map actual IDs.

#### [MODIFY] [stg__olipop_reddit_crossplatform.sql](file:///Users/eugenetsenter/Looker_clonedRepo/looker_personal/sql/stg/stg__olipop_reddit_crossplatform.sql)
- Update `campaign_id` mapping to use `Campaign Id` (instead of casting `NULL`).
- Update `ad_group_id` mapping to use `AD_GROUP Id` (instead of casting `NULL`).
- Update `ad_id` mapping to use `Ad Id` (instead of casting `NULL`).
- Re-deploy the staging view using `bq query` / `bigrquery`.

## Verification Plan

### Automated / Execution Verifications
- Execute the R loader script and verify a successful upload.
- Verify table counts and row parity in BigQuery.
- Deploy the updated `stg__olipop_reddit_crossplatform` view and run a validation query to check schema and null-rates.
