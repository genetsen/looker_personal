# Basis UTMs Utilities

Organized workspace for MassMutual Basis UTM extraction, validation, and supporting SQL.

## Folder Structure

- `essential/`: Actively used scripts and SQL for the current workflow.
- `archive/`: Legacy, exploratory, or scratch assets retained for reference.

## Essential Scripts

- `essential/util__basis__utm_pivot_longer_loop.r`
  - Batch pipeline that loads Basis trafficking sheets, pivots UTM creatives to long format, standardizes names, and writes to BigQuery.
- `essential/util_b_utm_validation.r`
  - Validation helper that queries BigQuery delivery data to inspect creative-name coverage and mappings.
- `essential/stg3_b_plus_utms_PnS.sql`
  - Main SQL join workflow that combines delivery keys and parsed UTM data.
- `essential/get_distinct_creative_names.sql`
  - Utility SQL for extracting distinct creative names and null-UTM diagnostics.
- `essential/load_basis_utms_unioned_0929_from_fy26_q1.sql`
  - Idempotent backfill script that inserts missing FY26 Q1 rows from `landing.basis_utms_pivoted_fy26_q1` into `landing.basis_utms_unioned-0929`.
- `essential/load_basis_utms_unioned_0929_from_fy26_q2_q3.sql`
  - Idempotent promotion script that inserts complete FY26 Q2/Q3 rows from `landing.basis_utms_pivoted_fy26_q2_q3` into `landing.basis_utms_unioned-0929`.

## Current FY26 Workbook Inputs

| Campaign period | Local workbook | Worksheet | Landing table |
|---|---|---|---|
| Q1 | `MassMutual_FY26_Q1_Traffic Sheet.xlsx` | `MASSMUTUAL004_updated 1.14.26` | `landing.basis_utms_pivoted_fy26_q1` |
| Q2/Q3 | `MASSMUTUAL005 - Creative Trafficking Sheet_Q3 7.7.xlsx` | `MASSMUTUAL005_Updated 7.7` | `landing.basis_utms_pivoted_fy26_q2_q3` |

Each campaign period is delivered as a separate file. The loader still requires the exact worksheet name inside that file and does not discover new files automatically. Embedded spaces and line breaks are removed from URL cells before upload.

## Pipeline-Critical SQL Dependencies

- `/Users/eugenetsenter/Looker_clonedRepo/looker_personal/util/basis_utms/essential/load_basis_utms_union.sql`
  - Unions all `landing.basis_utms_pivoted_*` tables (including FY26 Q1 and Q2/Q3) into `landing.basis_utms_unioned`.
- `/Users/eugenetsenter/Looker_clonedRepo/looker_personal/util/basis_utms/essential/stg__basis__utms.sql`
  - Parses UTM parameters from `landing.basis_utms_unioned` into the staging view used by downstream Basis+UTM joins.

## Live Compatibility Views

| View | Reads from | Notes |
|---|---|---|
| `looker-studio-pro-452620.20250327_data_model.basis_utms_stg` | `looker-studio-pro-452620.repo_stg.basis_utms` | Compatibility wrapper for older views expecting the 9-column UTM staging contract. |
| `looker-studio-pro-452620.utm_scrap.basis_utms_0519` | `looker-studio-pro-452620.repo_stg.basis_master2` and `looker-studio-pro-452620.20250327_data_model.basis_utms_stg` | Scratch/detail join that no longer reads Giant Spoon Basis tables. It can multiply delivery rows because multiple UTM rows can share the same `id`; do not use summed delivery metrics from this view without accounting for that join grain. |

## Basis Delivery Handoff

Basis delivery now starts from the Looker-owned external Google Sheets table [Basis Google Sheet external table](https://console.cloud.google.com/bigquery?project=looker-studio-pro-452620&p=looker-studio-pro-452620&d=repo_stg&t=basis_gsheet2&page=table), then `basis_update` merges recent latest-record rows into [Basis delivery master](https://console.cloud.google.com/bigquery?project=looker-studio-pro-452620&p=looker-studio-pro-452620&d=repo_stg&t=basis_master2&page=table). Combined DCM/Basis reporting reads that target through [Joined DCM and Basis view](https://console.cloud.google.com/bigquery?project=looker-studio-pro-452620&p=looker-studio-pro-452620&d=final_views&t=joined_dcmBasis&page=table).

For the broader DCM and master data model boundary, use [Basis, DCM, and master data model pipeline](/Users/eugenetsenter/Looker_clonedRepo/looker_personal/docs/BASIS_DCM_MASTER_DATA_MODEL_PIPELINE.md).

## Archive Contents

Archive includes older R script variants, ad hoc SQL scratchpads, and legacy notebook/diagram artifacts:

- `archive/util__basis__utm_pivot_longer.r`
- `archive/util__basis__utm_pivot_longer_clean.r`
- `archive/scrap.sql`
- `archive/testsAndScrap.sql`
- `archive/utm_validation_scrap.sql`
- `archive/union_basis_utms.ipynb`
- `archive/b_utms_diagram.md`

## Quick Run Commands

```bash
Rscript util/basis_utms/essential/util__basis__utm_pivot_longer_loop.r
Rscript util/basis_utms/essential/util_b_utm_validation.r
bq query --project_id=looker-studio-pro-452620 --use_legacy_sql=false \
  < util/basis_utms/essential/load_basis_utms_unioned_0929_from_fy26_q1.sql
bq query --project_id=looker-studio-pro-452620 --use_legacy_sql=false \
  < util/basis_utms/essential/load_basis_utms_unioned_0929_from_fy26_q2_q3.sql
```

Use project auth/environment defaults for BigQuery access before running these scripts.
