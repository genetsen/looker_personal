# Master Data Model

This project owns the generalized cross-client package/date view:

`looker-studio-pro-452620.master_stg.data_model`

It was split out of the ADIF workspace because the model intentionally removes ADIF-only filters and is meant to support broader analysis across clients.

Interactive orientation:

- [`docs/master-data-model-map.html`](docs/master-data-model-map.html) - clickable map of live inputs, branch logic, rollups, production outputs, and model-risk callouts.

## What The View Does

The SQL definition is in:

`create_master_stg_data_model.sql`

The reporting mart definition is in:

`create_master_stg_data_model_mart.sql`

The view:

1. Reads Prisma rows for packages with `start_date >= 2025-01-01`.
2. Reads DCM, original FPD, and updated FPD rows with `date >= 2025-01-01`.
3. Removes the ADIF-only advertiser, source-file, and account filters used by the ADIF production pipeline.
4. Combines original and updated FPD before final digital metrics are calculated.
5. Recalculates package rollups after final spend, impressions, and clicks are assigned.
6. Adds TV rows from the combined local/national TV estimate view with synthetic package and placement IDs.
7. Adds social rows from the cross-platform raw social table when compatible daily social grain is available.
8. Candidate logic also exposes all available row sources in `row_data_sources_available`, short issue labels in `row_data_issue_category`, and dashboard-friendly row callouts in `row_data_callouts`.

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
- `looker-studio-pro-452620.master_stg.data_model_mart` - Reporting mart over the master evidence layer. Applies reporting-only exclusions and recalculates package rollups after filtering.
- `looker-studio-pro-452620.master_stg.data_model_qa_tv_layer` - QA validation view used before the TV layer was promoted to production.
- `looker-studio-pro-452620.master_stg.data_model_qa_source_issues` - QA validation view for source visibility, issue labels, and non-Prisma DCM/FPD rows.
- `looker-studio-pro-452620.master_stg.data_model_mart_qa_source_issues` - QA validation mart built from the source-issue QA view.
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
- `row_data_source_primary` is a priority label. FPD wins over DCM when both are present.
- `row_data_sources_available` lists every detected source for the row, such as `prisma | prisma_daily | dcm | fpd_original`.
- `row_data_issue_category` uses compact labels such as `missing_prisma_package`, `missing_prisma_daily`, `actual_source_conflict`, `missing_actuals`, `missing_social_pacing`, and `missing_final_metrics`.
- `row_data_callouts` is null when no callout exists; otherwise it mirrors the compact row callouts so dashboard users can check one field.
- Candidate source-issue logic lets DCM and FPD rows enter even when their package ID is not present in Prisma. These rows stay visible in their raw `d_*` or `fpd_*` fields, but their `final_*` metrics remain null so they do not contribute to package actual rollups. When Prisma metadata is unavailable, the model falls back to DCM/FPD names, dates, supplier, and package identifiers where available.
- Candidate master logic labels low-signal DCM rows with `low_signal_dcm` when each row has fewer than `1,000` raw impressions, less than `$0.10` raw media cost, fewer than `5` raw clicks, and the package's filtered-row average is below `100` raw impressions per row.
- The reporting mart filters `low_signal_dcm` rows and recalculates package rollups after filtering. Keep the master model as the evidence layer; use the mart for reporting-ready totals.

## Verification Guidance

Row counts, source mix, package-key counts, and media totals fluctuate as source tables refresh. Do not treat those numbers as documentation state, and do not update this README just to chase count changes.

When validating a candidate or production view, run live QA and summarize the result in plain English:

- Confirm the date gates still behave as expected, such as no pre-`2025-01-01` output rows unless the model scope changes.
- Confirm required synthetic keys are populated for social and TV rows.
- Compare production and QA candidates live when reviewing a change.
- Confirm rows labeled `missing_prisma_package` have null `final_*` metrics and do not inflate `pkg_act_*` rollups.
- Confirm mart package rollups are recalculated after reporting-only filters such as `low_signal_dcm`.
- Keep durable findings in docs, such as field semantics, source lineage, filters, deployment commands, and known modeling risks.
- Keep fluctuating counts in the query result or handoff note for that run, not as permanent README values.

Recent stable QA context:

- `looker-studio-pro-452620.master_stg.data_model_qa_tv_layer` was the QA validation view used before the TV layer was promoted.
- `looker-studio-pro-452620.master_stg.data_model_qa_source_issues` is the QA validation view for source visibility, issue labels, and non-Prisma DCM/FPD rows.
- `looker-studio-pro-452620.master_stg.data_model_mart_qa_source_issues` is the QA mart that filters `low_signal_dcm` rows after the master evidence layer.
- The source-issue QA candidates passed dry run and production was not replaced.

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

Refresh the reporting mart after the master view is current:

```bash
bq query --project_id=looker-studio-pro-452620 --use_legacy_sql=false \
  < master_data_model/create_master_stg_data_model_mart.sql
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
  row_data_sources_available,
  row_data_issue_category,
  COUNT(*) AS row_count,
  MIN(date) AS min_date,
  MAX(date) AS max_date,
  COUNT(DISTINCT package_id_joined) AS package_count
FROM `looker-studio-pro-452620.master_stg.data_model`
GROUP BY row_type, row_data_source_primary, row_data_sources_available, row_data_issue_category
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
