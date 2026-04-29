# ADIF Main Pipeline Lineage One-Pager

This page is the shortest reliable explanation of how the live ADIF main table is built today.

## Current Production Target

- Final table: `looker-studio-pro-452620.repo_stg.adif__mainDataTable_notebook_v2_test`
- Warehouse-side verification date: `2026-04-10`
- Current row count at verification time: `16,701`
- Last observed table modification at verification time: `2026-04-10T10:03:13Z`

## The Pipeline in One Sentence

ADIF starts with digital delivery, planning data, and two first-party data branches, combines those into one digital base view, then the live BigQuery scheduled query adds social rows and pacing logic to produce the final main table.

## Full Lineage

```text
DCM.20250505_costModel_v5
    +
landing.fpd_data_ranged_shortcutsFolder
    +
20250327_data_model.prisma_expanded_full
    ->
repo_stg.adif__prisma_expanded_plus_dcm_view_v3_test
    +
landing.adif_updated_fpd_daily
    ->
repo_stg.adif__prisma_expanded_plus_dcm_updated_fpd_view
    +
repo_stg.stg__adif__social_crossplatform
    +
repo_int.crossplatform_pacing
    ->
repo_stg.adif__mainDataTable_notebook_v2_test
```

## What Each Step Does

### 1. Digital delivery branch

- Source: `looker-studio-pro-452620.DCM.20250505_costModel_v5`
- Meaning: actual ad-delivery numbers from DCM
- Main job: provides delivery metrics like impressions, spend, clicks, and video metrics at daily grain

### 2. Original first-party data branch

- Source: `looker-studio-pro-452620.landing.fpd_data_ranged_shortcutsFolder`
- ADIF filter:
  - `source_file` contains `De Beers`
  - or `source_file` starts with `FMUS | Partner Data Collection |`
- Main job: brings in partner-reported daily performance that can override DCM for matching package-and-date rows

### 3. Planning branch

- Source: `looker-studio-pro-452620.20250327_data_model.prisma_expanded_full`
- Main job: provides planned daily spend and planned daily impressions plus package metadata

### 4. Digital base view

- View: `looker-studio-pro-452620.repo_stg.adif__prisma_expanded_plus_dcm_view_v3_test`
- Main job:
  - FULL OUTER JOINs DCM, original FPD, and Prisma
  - keeps orphan rows instead of dropping them
  - uses original FPD ahead of DCM when both exist for the same row

### 5. Updated first-party data branch

- Source: `looker-studio-pro-452620.landing.adif_updated_fpd_daily`
- Current production stats at validation time:
  - rows: `2,162`
  - packages: `62`
  - date range: `2025-10-01` to `2026-01-15`
- Main job:
  - takes corrected package totals from a sheet
  - spreads them across each package's Prisma date range
  - joins them onto the digital base view

### 6. Updated digital base view

- View: `looker-studio-pro-452620.repo_stg.adif__prisma_expanded_plus_dcm_updated_fpd_view`
- Main job:
  - keeps the original digital base
  - adds updated FPD on top
  - recalculates final actuals with the combined FPD layer ahead of DCM

### 7. Social source branch

- View: `looker-studio-pro-452620.repo_stg.stg__adif__social_crossplatform`
- Main job:
  - filters the shared cross-platform source down to ADIF rows
  - normalizes platform naming
  - keeps row-level lineage fields for QA

### 8. Social pacing branch

- View: `looker-studio-pro-452620.repo_int.crossplatform_pacing`
- Main job:
  - provides social budget and pacing context used when social rows are added into the final table

## Effective Actuals Priority

For the digital side, the documented and still-live priority order is:

1. Updated FPD
2. Original FPD
3. DCM
4. Planned-only rows if no actuals exist

## What Is Still Live vs What Is Reference Only

- Live production target today:
  - `looker-studio-pro-452620.repo_stg.adif__mainDataTable_notebook_v2_test`
- Live active builder today:
  - BigQuery scheduled query `ADIF_FullDataRefresh_2604`
- Older reference table that still exists:
  - `looker-studio-pro-452620.repo_stg.adif__mainDataTable_notebook`
- Historical notebook copy:
  - `projects/social_layering/build__adif__prisma_expanded_plus_dcm_with_social_tbl.ipynb`
- Snapshot QA artifact:
  - `projects/social_layering/dashboard/adif__mainDataTable_notebook_v2_test_qa_dashboard.html`

## Validation Notes

- I verified the warehouse objects, schemas, and current row counts directly in BigQuery on `2026-04-10`.
- I could not re-check the scheduled-query service metadata from this environment because `bq` and `gcloud` are not installed here.
- Because of that, this page confirms the production data lineage from the warehouse side, while the transfer-config metadata remains based on the previously documented config ID and schedule.
