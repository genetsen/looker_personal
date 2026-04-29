# Master Data Model

This project owns the generalized cross-client package/date view:

`looker-studio-pro-452620.master_stg.data_model`

It was split out of the ADIF workspace because the model intentionally removes ADIF-only filters and is meant to support broader analysis across clients.

## What The View Does

The SQL definition is in:

`create_master_stg_data_model.sql`

The view:

1. Builds a Prisma package universe for packages with `start_date >= 2025-01-01`.
2. Reads DCM, original FPD, updated FPD, and Prisma rows with `date >= 2025-01-01`.
3. Removes the ADIF-only advertiser, source-file, and account filters used by the ADIF production pipeline.
4. Combines original and updated FPD before final digital metrics are calculated.
5. Recalculates package rollups after final spend, impressions, and clicks are assigned.
6. Adds TV rows from the combined local/national TV estimate view with synthetic package and placement IDs.
7. Adds social rows from the cross-platform raw social table when compatible daily social grain is available.

## Source Tables And Views

Digital inputs:

- `looker-studio-pro-452620.20250327_data_model.prisma_expanded_full`
- `looker-studio-pro-452620.DCM.20250505_costModel_v5`
- `looker-studio-pro-452620.landing.fpd_data_ranged_shortcutsFolder`
- `looker-studio-pro-452620.landing.adif_updated_fpd_daily`

Social inputs:

- `looker-studio-pro-452620.repo_stg.stg__olipop__crossplatform_raw_tbl`
- `looker-studio-pro-452620.repo_int.crossplatform_pacing`

TV inputs:

- `looker-studio-pro-452620.landing.tv_combined`

Output:

- `looker-studio-pro-452620.master_stg.data_model`
- `looker-studio-pro-452620.master_stg.data_model_qa_tv_layer` - QA validation view used before the TV layer was promoted to production.
- `looker-studio-pro-452620.master_stg.ritual_data_model` - Ritual-only filtered view over the master data model.

## Client-Specific Views

Ritual:

- View: `looker-studio-pro-452620.master_stg.ritual_data_model`
- SQL definition: `create_ritual_data_model_view.sql`
- Filter: `advertiser_name = 'Ritual' OR advertiser_short_name = 'RTL'`
- Source view: `looker-studio-pro-452620.master_stg.data_model`

## Important Modeling Notes

- This view is separate from the ADIF scheduled refresh and does not replace `looker-studio-pro-452620.repo_stg.adif__mainDataTable_notebook_v2_test`.
- Digital rows keep package IDs from Prisma/DCM/FPD.
- Social rows use a synthetic package key: `social:<platform>:<campaign_id>:<ad_group_id>`, because social data does not naturally share Prisma package IDs.
- TV rows use synthetic package and placement keys because the TV estimate view does not naturally share Prisma package IDs.
- TV source fields are preserved in `tv_*` fields, including outlet, type, program, market, quarter, year, net impressions, net cost, total units, and data refresh date.
- Updated FPD is layered before `final_spend`, `final_impressions`, and package actual rollups are calculated.
- Original FPD and updated FPD are both preserved in separate `fpd_orig_*` and `fpd_updated_*` fields, then combined into `fpd_*` fields.

## Current Verification Snapshot

Master view verified after TV deployment on `2026-04-29`:

- Total rows: `137,540`
- Digital rows: `75,714`
- Social rows: `59,990`
- TV rows: `1,836`
- Distinct package keys: `1,931`
- Rows before `2025-01-01`: `0`
- Digital rows with package starts before `2025-01-01`: `0`
- Updated FPD-only rows: `2,162`
- Original FPD-only rows: `2,752`
- DCM rows: `20,990`
- Planned-only rows: `49,810`
- TV package keys: `95`
- TV date range: `2025-06-30` through `2026-12-21`
- TV net impressions: `1,197,091,930`
- TV net cost: `35,591,737.382500015`
- TV total units: `5,993`
- TV rows missing synthetic package IDs: `0`
- TV rows missing synthetic placement IDs: `0`

Ritual view verified after creation on `2026-04-29`:

- Total rows: `2,549`
- Non-Ritual rows: `0`
- Advertiser name: `Ritual`
- Advertiser short name: `RTL`
- Date range: `2025-01-01` through `2026-12-31`
- Planned-only rows: `2,461`
- DCM rows: `88`

TV layer QA candidate verified before production deployment on `2026-04-29`:

- QA view: `looker-studio-pro-452620.master_stg.data_model_qa_tv_layer`
- Total rows: `137,540`
- TV rows: `1,836`
- TV package keys: `95`
- TV date range: `2025-06-30` through `2026-12-21`
- TV net impressions: `1,197,091,930`
- TV net cost: `35,591,737.382500015`
- TV total units: `5,993`
- TV rows missing synthetic package IDs: `0`
- TV rows missing synthetic placement IDs: `0`
- Rows before `2025-01-01`: `0`

## Deploy Or Refresh The View

From the repo root:

```bash
bq query --project_id=looker-studio-pro-452620 --use_legacy_sql=false \
  < master_data_model/create_master_stg_data_model.sql
```

Refresh the Ritual-only view:

```bash
bq query --project_id=looker-studio-pro-452620 --use_legacy_sql=false \
  < master_data_model/create_ritual_data_model_view.sql
```

Dry-run validation:

```bash
bq query --project_id=looker-studio-pro-452620 --use_legacy_sql=false --dry_run \
  < master_data_model/create_master_stg_data_model.sql
```

## Quick QA Queries

Row mix:

```sql
SELECT
  row_type,
  row_data_source_primary,
  COUNT(*) AS row_count,
  MIN(date) AS min_date,
  MAX(date) AS max_date,
  COUNT(DISTINCT package_id_joined) AS package_count
FROM `looker-studio-pro-452620.master_stg.data_model`
GROUP BY row_type, row_data_source_primary
ORDER BY row_type, row_count DESC;
```

Filter gates:

```sql
SELECT
  COUNT(*) AS total_rows,
  COUNTIF(date < DATE '2025-01-01') AS rows_before_2025,
  MIN(date) AS min_date,
  MIN(IF(row_type = 'digital', package_start_date, NULL)) AS min_digital_package_start_date,
  COUNTIF(row_type = 'digital' AND package_start_date < DATE '2025-01-01') AS digital_rows_with_pre_2025_start,
  COUNTIF(row_type = 'digital' AND (fpd_updated_impressions IS NOT NULL OR fpd_updated_spend IS NOT NULL)) AS digital_rows_with_updated_fpd,
  COUNTIF(row_type = 'social') AS social_rows
FROM `looker-studio-pro-452620.master_stg.data_model`;
```
