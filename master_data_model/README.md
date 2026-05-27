# Master Data Model

This project owns the generalized cross-client package/date view:

`looker-studio-pro-452620.master_stg.data_model`

It was split out of the ADIF workspace because the model intentionally removes ADIF-only filters and is meant to support broader analysis across clients.

Interactive orientation:

- [`docs/master-data-model-map.html`](docs/master-data-model-map.html) - clickable map of live inputs, branch logic, rollups, production outputs, and model-risk callouts.

## Manual Package Edit Quick Reference

Use this when a dashboard value needs a direct manual correction.

- Detailed QA runbook: `manual_package_edits/QA_RUNBOOK.md`.
- Sheet: `Package Editor` in the manual updates Google Sheet.
- User action: filter to the package, edit the visible value directly, and look for markers: orange means changed from the current dashboard snapshot, purple means already using a validated manual update, and red means a started new row needs fixing.
- Best identifiers: `Package ID`, `Site`, and `Package Friendly Name`.
- Flight dates and package metadata: package-level corrections apply to the whole `Package ID`, across all delivery dates.
- Delivered metrics: can be edited for a full flight, one week, or one day by changing `Delivery Override Start Date` and `Delivery Override End Date`.
- Planned metrics: `Planned Spend` and `Planned Impressions` are full-flight only. Partial-range planned edits are blocked.
- New rows: require `Package ID`, `Site`, `Package Friendly Name`, flight dates, delivery override dates, at least one metric, and the visible required metadata fields.
- Refresh request: users can check the `Request refresh` control in the sheet to send Gene an email notification. It does not run the loader by itself.
- Loader: `manual_package_edits/load_manual_package_edits.R`, also registered in the universal script runner through `automation_hub/workloads/ops/master_manual_package_edits/load_master_manual_package_edits.R`.
- Backend evidence: valid edits land in `man_*` fields and take priority for final `_` fields with `COALESCE(man_value, normal_value)` behavior.
- QA path: visible Sheet row -> raw manual table -> daily manual table -> `master_stg.data_model` -> `master_stg.data_model_mart`.

## What The View Does

The SQL definition is in:

`create_master_stg_data_model.sql`

The reporting mart definition is in:

`create_master_stg_data_model_mart.sql`

The compatibility and creative-grain v2 definitions are in:

- `create_master_stg_data_model_v2.sql`
- `create_master_stg_data_model_mart_v2.sql`
- `create_ritual_data_model_view_v2.sql`
- `create_data_model_delivery_detail_v2.sql`
- `create_ritual_data_model_delivery_detail_v2.sql`
- `create_data_model_detail_master_v2_sample.sql`

The dashboard-like manual package edit path is in:

- `create_manual_package_edit_tables.sql`
- `manual_package_edits/load_manual_package_edits.R`
- `manual_package_edits/QA_RUNBOOK.md`

The view:

1. Reads Prisma rows for packages with `start_date >= 2025-01-01`.
2. Reads DCM, original FPD, and updated FPD rows with `date >= 2025-01-01`.
3. Removes the ADIF-only advertiser, source-file, and account filters used by the ADIF production pipeline.
4. Combines original and updated FPD before final digital metrics are calculated.
5. Recalculates package rollups after final spend, impressions, and clicks are assigned.
6. Adds TV rows from the combined local/national TV estimate view with synthetic package and placement IDs.
7. Adds social rows from the cross-platform raw social table when compatible daily social grain is available.
8. Candidate logic also exposes all available row sources in `row_data_sources_available`, short issue labels in `row_data_issue_category`, and dashboard-friendly row callouts in `row_data_callouts`.
9. Adds `_advertiser` as the canonical advertiser grouping field while preserving raw `_advertiser_name` and `_advertiser_short_name`.
10. Applies valid active manual package edits from `landing.master_data_model_manual_package_daily` and package-level metadata edits from `landing.master_data_model_manual_package_edits_raw` before package rollups are calculated.
11. Adds `initiative` from Prisma's source column `initative` at the package/date model grain.

## Source Tables And Views

Digital inputs:

- `looker-studio-pro-452620.20250327_data_model.prisma_expanded_full`
- `looker-studio-pro-452620.DCM.20250505_costModel_v5`
- `looker-studio-pro-452620.landing.fpd_data_ranged_shortcutsFolder`
- `looker-studio-pro-452620.landing.adif_updated_fpd_daily`

Social inputs:

- `looker-studio-pro-452620.repo_stg.stg__olipop__crossplatform_raw_tbl`
- `looker-studio-pro-452620.repo_int.crossplatform_pacing`

Apollo social QA candidate, not production:

- [APO Search Data Template](https://docs.google.com/spreadsheets/d/1fen46Ugxx12PYRzCDT88z8ENVcVl_MjlQhqkGDxZNbc/edit?gid=982708559#gid=982708559) supplies candidate Apollo delivery fields and creative metadata.
- [APO normalized staging QA](https://console.cloud.google.com/bigquery?project=looker-studio-pro-452620&p=looker-studio-pro-452620&d=repo_stg&t=stg__apo__search_data_template_daily_qa&page=table) retains source rows and flags cross-campaign ad-ID conflicts as pending source-owner review.
- [APO-primary cross-platform QA](https://console.cloud.google.com/bigquery?project=looker-studio-pro-452620&p=looker-studio-pro-452620&d=repo_stg&t=stg__crossplatform_apo_primary_qa&page=table) applies APO-first column precedence at candidate campaign/ad-group/ad grain, while retaining the current shared source as fallback.
- [APO-primary social reporting QA](https://console.cloud.google.com/bigquery?project=looker-studio-pro-452620&p=looker-studio-pro-452620&d=master_stg&t=data_model_social_apo_primary_qa&page=table) exposes candidate Apollo Paid Search and Online Video reporting labels plus creative and provenance fields.
- Production social staging and the production master model are unchanged while source-owner clarification and historical overlap impacts remain under review.

TV inputs:

- `looker-studio-pro-452620.landing.tv_combined`

Manual package edit inputs:

- `looker-studio-pro-452620.landing.master_data_model_manual_package_edits_raw`
- `looker-studio-pro-452620.landing.master_data_model_manual_package_daily`

Output:

- `looker-studio-pro-452620.master_stg.data_model` - Main package/date evidence layer, including `initiative`.
- `looker-studio-pro-452620.master_stg.data_model_v2` - Compatibility wrapper over `data_model` for existing v2 consumers.
- `looker-studio-pro-452620.master_stg.data_model_delivery_detail_v2` - Sibling v2 delivery-detail model. Preserves DCM and original FPD creative at the source/detail grain, with package budget fields marked as non-summable context.
- `looker-studio-pro-452620.master_stg.data_model_detail_master_v2_sample` - One-table comparison sample that stacks package/date rows with delivery-detail rows. This is useful for evaluating a single-table shape but is not the canonical reporting model because unfiltered sums can double-count package and delivery metrics.
- `looker-studio-pro-452620.master_stg.data_model_mart` - Reporting mart over the master evidence layer. Applies reporting-only exclusions, recalculates package rollups after filtering, and includes the consolidated `initiative` field.
- `looker-studio-pro-452620.master_stg.data_model_mart_v2` - Reporting mart over `data_model_v2`.
- `looker-studio-pro-452620.master_stg.data_model_qa_tv_layer` - QA validation view used before the TV layer was promoted to production.
- `looker-studio-pro-452620.master_stg.data_model_qa_source_issues` - QA validation view for source visibility, issue labels, and non-Prisma DCM/FPD rows.
- `looker-studio-pro-452620.master_stg.data_model_mart_qa_source_issues` - QA validation mart built from the source-issue QA view.
- `looker-studio-pro-452620.master_stg.ritual_data_model` - Ritual-only filtered view over the master data model.
- `looker-studio-pro-452620.master_stg.ritual_data_model_v2` - Ritual-only filtered view over `data_model_v2`.
- `looker-studio-pro-452620.master_stg.ritual_data_model_delivery_detail_v2` - Ritual-only filtered view over `data_model_delivery_detail_v2`.

## Client-Specific Views

Ritual:

- View: `looker-studio-pro-452620.master_stg.ritual_data_model`
- SQL definition: `create_ritual_data_model_view.sql`
- Filter: `advertiser_name = 'Ritual' OR advertiser_short_name = 'RTL'`
- Source view: `looker-studio-pro-452620.master_stg.data_model`

Ritual v2:

- View: `looker-studio-pro-452620.master_stg.ritual_data_model_v2`
- SQL definition: `create_ritual_data_model_view_v2.sql`
- Filter: `advertiser_name = 'Ritual' OR advertiser_short_name = 'RTL'`
- Source view: `looker-studio-pro-452620.master_stg.data_model_v2`

Ritual delivery detail v2:

- View: `looker-studio-pro-452620.master_stg.ritual_data_model_delivery_detail_v2`
- SQL definition: `create_ritual_data_model_delivery_detail_v2.sql`
- Filter: `_advertiser_name = 'Ritual' OR _advertiser_short_name = 'RTL'`
- Source view: `looker-studio-pro-452620.master_stg.data_model_delivery_detail_v2`

## Important Modeling Notes

- This view is separate from the ADIF scheduled refresh and does not replace `looker-studio-pro-452620.repo_stg.adif__mainDataTable_notebook_v2_test`.
- Digital rows keep package IDs from Prisma/DCM/FPD.
- Social rows use a synthetic package key: `social:<platform>:<campaign_id>:<ad_group_id>`, because social data does not naturally share Prisma package IDs.
- In the Apollo social QA candidate, `_Search_` campaign markers map to Paid Search and `_YT_` campaign markers map to Online Video; rows without either marker use the Sheet channel as fallback. Candidate identity includes campaign, ad group, and ad so cross-campaign ad-ID conflicts can be temporarily included as separately flagged `publish_pending_source_owner_review` rows pending source-owner clarification; exact duplicate records remain excluded.
- TV rows use synthetic package and placement keys because the TV estimate view does not naturally share Prisma package IDs.
- TV source fields are preserved in `tv_*` fields, including outlet, type, program, market, quarter, year, net impressions, net cost, total units, and data refresh date.
- Updated FPD is layered before `final_spend`, `final_impressions`, and package actual rollups are calculated.
- Original FPD and updated FPD are both preserved in separate `fpd_orig_*` and `fpd_updated_*` fields, then combined into `fpd_*` fields.
- Manual package edits are layered after the normal digital/social/TV rows and before planned backfills, row callouts, and package rollups. Non-null `man_daily_*` metric values win for their matching final `_` fields on the edited delivery dates, while package-level `man_*` metadata values override Prisma metadata for the whole package.
- The manual edit sheet uses a single `Package Editor` tab. It shows package lookup context and dashboard values in the same row; users edit the value cell directly, and the loader compares that cell to source-derived baselines plus the last run to decide whether to write or clear a backend manual value.
- The loader treats source values as the baseline for edit detection. Planned package totals come directly from PRISMA package totals, while delivered metric baselines are recalculated from the raw delivery fields instead of manual-affected final `_` fields. This prevents stale manual values from re-marking themselves as edits after the source data catches up.
- The visible editor starts with the fields a media buyer needs to recognize the row: `Package ID`, `Site`, and `Package Friendly Name`, followed by pale-yellow editable date and metric columns.
- The package lookup facets live in native Google Sheets slicers for Advertiser, Channel, Campaign, and Site. They filter the real editable package rows without Apps Script-driven dropdowns or hidden helper columns.
- The sheet has a `Request refresh` checkbox-style control. Checking it notifies Gene by email; if a Slack webhook script property exists, the same request can post to Slack. It is notification-only and does not run the loader or write to the warehouse by itself.
- `Flight Start Date` and `Flight End Date` are package-level fields. `Delivery Override Start Date` and `Delivery Override End Date` define the metric override window. To change delivery for one week while leaving other weeks as-is, users add or duplicate a row, set the delivery override dates to that week, and enter replacement delivered totals for that week only.
- Planned metrics display as full-flight totals and can only be edited on full-flight rows. Delivered actual metrics can use day, week, or full-flight replacement totals.
- To undo a manual value, set the visible cell back to the displayed baseline/source value, or leave it blank where blanks are allowed. If the normal source later catches up to a manual replacement, the next loader run clears the backend manual value and removes the formatting marker.
- Manual daily allocation preserves exact replacement totals after upload: count metrics distribute whole units across the selected dates, and spend metrics distribute by cents.
- New manual-only rows need enough metadata to enter the main model: `Package ID`, `Site`, `Package Friendly Name`, `Flight Start Date`, `Flight End Date`, `Delivery Override Start Date`, `Delivery Override End Date`, at least one edited metric, plus visible required metadata fields for Advertiser, Package Type, Channel, Campaign, Package Name, and GS Channel. Started new rows turn red when required fields are missing or dates are invalid.
- The editor UX is managed separately from the R loader by `manual_package_edits/setup_manual_package_editor_sheet.mjs`. That setup owns the instruction area, frozen header row, base table filter, table banding, borders, protected source/context columns, hidden baseline comparison columns, visual validation rules, native slicers, and pale-yellow editable date/metric columns.
- The R loader only refreshes data values and backend manual tables. It does not own sheet design, filters, widths, colors, or table formatting.
- Edited/manual-backed cells are marked only with Google Sheet formatting. The user-facing sheet does not expose replacement columns, delta columns, validation tabs, proof tabs, or `man_*` backend fields.
- The loader still proves each uploaded daily metric sums back to the edited total before it replaces the daily manual table.
- Manual-only packages can enter the model only from valid active manual rows with required metadata. They use `qa_row_type = 'manual'` and `qa_row_data_source_primary = 'manual_package_edits'`.
- Manual evidence is visible in `man_*` fields, including edit ID, reason, editor, source sheet URL, modified time, daily values, and total replacement values marked with `_doNotSum` where they are repeated across daily rows.
- `row_data_source_primary` is a priority label. FPD wins over DCM when both are present.
- `row_data_sources_available` lists every detected source for the row, such as `prisma | prisma_daily | dcm | fpd_original`.
- `row_data_issue_category` uses compact source/cause labels such as `missing_prisma_package`, `missing_prisma_daily`, `actual_source_conflict`, `missing_actuals`, `missing_social_pacing`, and `missing_final_metrics`.
- `row_data_callouts` is null when no callout exists; otherwise it keeps source/cause labels first and adds short metric-symptom labels only when they add new information. For example, `missing_actuals` stands alone for planned-only rows, while rows with planned and final spend but zero planned and final impressions are flagged as `spend_no_imps`.
- `_advertiser` is the canonical advertiser grouping field. It groups advertiser-name and short-code variants such as `RTL`/`Ritual`, Apollo variants, Olipop variants, MassMutual variants, and ADIF/Forevermark/De Beers variants while keeping raw advertiser fields unchanged.
- `_package_name_friendly` falls back to the full `_package_name` when the friendly name is blank, so package lookup rows do not show empty friendly labels when a usable full package name exists.
- Candidate source-issue logic lets DCM and FPD rows enter even when their package ID is not present in Prisma. These rows stay visible in their raw `d_*` or `fpd_*` fields, but their `final_*` metrics remain null so they do not contribute to package actual rollups. When Prisma metadata is unavailable, the model falls back to DCM/FPD names, dates, supplier, and package identifiers where available.
- Candidate master logic labels low-signal DCM rows with `low_signal_dcm` when each row has fewer than `1,000` raw impressions, less than `$0.10` raw media cost, fewer than `5` raw clicks, and the package's filtered-row average is below `100` raw impressions per row.
- The reporting mart filters `low_signal_dcm` rows and recalculates package rollups after filtering. Keep the master model as the evidence layer; use the mart for reporting-ready totals.
- `initiative` now lives directly in the main package/date `data_model`, sourced from Prisma's source column `initative`. The package/date `data_model_v2` view is a compatibility wrapper over `data_model`.
- Creative lives in `data_model_delivery_detail_v2`, where DCM rows are preserved at package/date/placement/ad/creative grain and original FPD rows are preserved at package/date/partner-placement/partner-creative grain.
- `creative_git_link` is preserved from original FPD detail rows when the partner data source supplies a Git-hosted creative image link.
- Package budget fields in `data_model_delivery_detail_v2` use `doNotSum` names because they are context copied onto lower-grain rows. Use package/date tables for budget totals.
- `data_model_detail_master_v2_sample` exists only to compare the single-table approach. Any query against it must filter `row_level` or `metric_grain` before summing metrics.
- The package/date v2 view is compatibility-only; detail/sample v2 views still carry the lower-grain creative/detail structures.

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

Create the empty manual-edit landing tables before deploying the manual-aware model:

```bash
bq query --project_id=looker-studio-pro-452620 --use_legacy_sql=false \
  < master_data_model/create_manual_package_edit_tables.sql
```

Load or refresh the Google Sheet manual-edit path:

```bash
Rscript /Users/eugenetsenter/Looker_clonedRepo/looker_personal/master_data_model/manual_package_edits/load_manual_package_edits.R
```

The production Manual Data Editor sheet is the loader default. Set `MASTER_MANUAL_EDIT_SHEET_ID` only when intentionally running against a different sheet copy.

Refresh the master view:

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

Create or refresh the v2 sibling views:

```bash
bq query --project_id=looker-studio-pro-452620 --use_legacy_sql=false \
  < master_data_model/create_master_stg_data_model_v2.sql

bq query --project_id=looker-studio-pro-452620 --use_legacy_sql=false \
  < master_data_model/create_data_model_delivery_detail_v2.sql

bq query --project_id=looker-studio-pro-452620 --use_legacy_sql=false \
  < master_data_model/create_master_stg_data_model_mart_v2.sql

bq query --project_id=looker-studio-pro-452620 --use_legacy_sql=false \
  < master_data_model/create_ritual_data_model_view_v2.sql

bq query --project_id=looker-studio-pro-452620 --use_legacy_sql=false \
  < master_data_model/create_ritual_data_model_delivery_detail_v2.sql

bq query --project_id=looker-studio-pro-452620 --use_legacy_sql=false \
  < master_data_model/create_data_model_detail_master_v2_sample.sql
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
