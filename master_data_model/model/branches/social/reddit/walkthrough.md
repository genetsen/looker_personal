# Walkthrough - Reddit Ads Data Pipeline Integration

We have successfully executed the steps to clean and ingest daily Reddit Ads CSV data into the landing table, and updated the corresponding BigQuery staging view to map actual IDs.

## Accomplishments

### 1. Data Cleaning & Ingestion
- Created a robust, temporary R loader utility `master_data_model/tmp/load_reddit_csv.r` (kept for your inspection as requested).
- Read the raw scheduled CSV `master_data_model/tmp/Reddit-Olipop-scheaduled (Copy) (2).csv` (654 rows).
- **Skipped/dropped** the `CPC (USD)` / `CPC _USD_` column entirely.
- Used `janitor::clean_names()` to convert all column names into clean, lowercase snake_case headers (e.g. `Amount Spent (USD)` -> `amount_spent_usd`), completely avoiding any JSON space-parsing issues with `bigrquery`.
- Handled leading single quotes (`'`) from ID fields (`campaign_id`, `ad_group_id`, `ad_id`).
- Correctly parsed the `Date` column and formatted it as a BigQuery `DATE` type.
- Safely deleted the old table to overwrite its schema before reloading, then ingested all 654 rows.

---

### 2. Staging View Modification & Deployment
- Modified `/Users/eugenetsenter/Looker_clonedRepo/looker_personal/sql/stg/stg__olipop_reddit_crossplatform.sql` to map fields from the clean snake_case landing schema.
- **Mapped actual IDs** (`campaign_id`, `ad_group_id`, `ad_id`) directly instead of casting `NULL AS STRING`.
- **Mapped actual video views & watches** (`video_view` from `video_views`, `video_views_p_25` from `watches_at_25_percent`, and `video_views_p_100` from `watches_at_100_percent`) to match standard shared-social daily-ad grain schema.
- Re-deployed the staging view `repo_stg.stg__olipop_reddit_crossplatform` in BigQuery successfully.

---

## Ingestion & View Validation Results

- **Landing Table Row Count Check**:
  ```sql
  SELECT COUNT(*) as row_count FROM `looker-studio-pro-452620.landing.reddit-ads-email`
  -- Output: 654 rows (100% match)
  ```

- **Staging View Output Sample Verification**:
  ```sql
  SELECT source_relation, date_day, platform, campaign_id, campaign_name, clicks, impressions, spend, video_view, video_views_p_25, video_views_p_100
  FROM `looker-studio-pro-452620.repo_stg.stg__olipop_reddit_crossplatform` LIMIT 1
  ```
  | Field | Value |
  |---|---|
  | `source_relation` | `reddit-ads-email` |
  | `date_day` | `2026-04-17` |
  | `platform` | `reddit` |
  | `campaign_id` | `2472210695356180304` *(real ID mapped)* |
  | `campaign_name` | `reddit_olipop_gs_us_fiber_awareness_4.17.26_05.31.26` |
  | `clicks` | `48` |
  | `impressions` | `36369` |
  | `spend` | `241.29357` |
  | `video_view` | `12751.0` |
  | `video_views_p_25` | `8572.0` |
  | `video_views_p_100` | `1167.0` |
