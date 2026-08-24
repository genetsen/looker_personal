---
pipeline: Manual Data Editor
source_type: user-entered package corrections
output: looker-studio-pro-452620.master_stg.data_model_v3
output_grain: lowest available source detail, with manual delivery overrides at package/date
source_tables:
  - looker-studio-pro-452620.landing.master_data_model_manual_package_edits_raw
  - looker-studio-pro-452620.landing.master_data_model_manual_package_daily
  - looker-studio-pro-452620.master_stg.data_model
refresh: manual loader, then master-model clustered and V3 refresh
loader_script: model/manual_editor/load_manual_package_edits.R
verified: 2026-08-24
verified_against:
  - manual_package_edits/README.md
  - model/manual_editor/load_manual_package_edits.R
  - model/final_model/create_master_stg_data_model_v3.sql
  - universal_cron_runner/automation_hub/workloads/ops/bq_trigger/run_master_data_model_clustered_advertiser_refresh.sh
reviewers: []
---

# Manual Data Editor

This guide is for the person who adds or publishes a manual package correction. It covers the normal Sheet workflow, the required refresh into [Master Data Model V3](https://console.cloud.google.com/bigquery?project=looker-studio-pro-452620&p=looker-studio-pro-452620&d=master_stg&t=data_model_v3&page=table), and the shortest useful checks. For investigation queries and full QA, use the [QA runbook](/Users/eugenetsenter/Looker_clonedRepo/looker_personal/master_data_model/manual_package_edits/QA_RUNBOOK.md).

## Contents

- [Terms](#terms)
- [Normal workflow: edit through V3](#normal-workflow-edit-through-v3)
- [What you may edit](#what-you-may-edit)
- [Date and row rules](#date-and-row-rules)
- [How the data moves](#how-the-data-moves)
- [Canonical files and protected surfaces](#canonical-files-and-protected-surfaces)
- [Known limits and maintenance](#known-limits-and-maintenance)
- [Troubleshooting](#troubleshooting)

## Terms

| Term | Meaning here |
|---|---|
| Manual evidence | The saved record showing which value a person intentionally replaced, when it was edited, and whether it passed validation. |
| V3 rebuild[^1] | Re-creating the stored V3 table from the current base model and source-detail tables. |
| Grain[^2] | What one row represents. Manual delivery rows are one package on one date; V3 can retain lower source detail such as placement or creative. |

## Normal workflow: edit through V3

The `Request refresh` control only sends a notification. It does **not** run either command below.

### 1. Make the edit

In the production Google Sheet:

1. Open the `Package Editor` tab.
2. Find the row using `Package ID`, `Site`, and `Package Friendly Name`.
3. Change only an editable field listed in [What you may edit](#what-you-may-edit).
4. Fix any red cells before publishing.

### 2. Publish the manual edit

Run the Manual Data Editor loader:

```bash
Rscript /Users/eugenetsenter/Looker_clonedRepo/looker_personal/master_data_model/manual_package_edits/load_manual_package_edits.R
```

This command writes the live manual raw, history, and daily tables and refreshes the Sheet values. It does not rebuild V3.

**Pass condition:** the output says `Completed manual package editor load.` and its final summary shows `blocked_package_rows` as `0` and `daily_total_proof_status` as `passed`.

If `blocked_package_rows` is above zero, use the row’s `Validation Reason` before continuing. Do not treat a completed R process by itself as proof that the edit published.

### 3. Refresh V3

After the loader passes, run the master-model refresh wrapper:

```bash
bash /Users/eugenetsenter/Docs/R_Studio_Projects/universal_cron_runner/automation_hub/workloads/ops/bq_trigger/run_master_data_model_clustered_advertiser_refresh.sh
```

This command performs two dependent refreshes in the required order:

1. Refreshes and verifies the clustered support table used by the model workflow.
2. Rebuilds [Master Data Model V3](https://console.cloud.google.com/bigquery?project=looker-studio-pro-452620&p=looker-studio-pro-452620&d=master_stg&t=data_model_v3&page=table) from the canonical V3 SQL.

**Pass condition:** the output ends with both of these messages:

```text
✓ Clustered advertiser table refresh verified.
✓ Master data model v3 refresh verified.
```

The wrapper also checks that V3 is a non-empty table clustered by advertiser, has no retired `package_plan` rows, has no duplicate visible keys, and has at most one row carrying summable planned metrics per package/date.

### 4. Confirm your package in V3

Replace `PACKAGE_ID_HERE`, then run this read-only query:

```sql
SELECT
  `_package_id`,
  MIN(`_date`) AS first_date,
  MAX(`_date`) AS last_date,
  COUNT(*) AS record_count,
  COUNTIF(qa_manual_edit_flag) AS manual_evidence_record_count,
  SUM(`_spend`) AS spend,
  SUM(`_impressions`) AS impressions,
  SUM(`_clicks`) AS clicks,
  STRING_AGG(DISTINCT qa_v3_source_detail_type, ', ' ORDER BY qa_v3_source_detail_type) AS v3_source_types
FROM `looker-studio-pro-452620.master_stg.data_model_v3`
WHERE `_package_id` = 'PACKAGE_ID_HERE'
GROUP BY `_package_id`;
```

**Pass condition:** the package is returned, its dates and edited totals match the intended correction, and `manual_evidence_record_count` is above zero. A delivered-metric correction should include `manual_package_daily` in `v3_source_types`.

For metadata-only or planned-value edits, use the [package trace query](/Users/eugenetsenter/Looker_clonedRepo/looker_personal/master_data_model/manual_package_edits/QA_RUNBOOK.md) because those changes do not necessarily create the same delivered-metric row type.

## What you may edit

| Edit | Editable fields | Where it applies |
|---|---|---|
| Delivered actuals | `Spend`, `Impressions`, `Clicks`, `Video Plays`, `Video Completions` | Only the delivery override dates on that row |
| Planned totals | `Planned Spend`, `Planned Impressions` | The package’s complete flight |
| Flight dates | `Flight Start Date`, `Flight End Date` | Every row for the package |
| Package metadata | `Advertiser`, `Package Type`, `Channel`, `Campaign`, `Initiative`, supplier fields, package names, `GS Channel` | Every row for the package |
| Benchmark | `Benchmark KPI`, `Benchmark Value` | Every row for the package |

Do not edit package identity cells on an existing source-backed row, headers, status fields, audit fields, hidden baselines, hidden manual markers, or helper columns.

### Color guide

| Color | Meaning | What to do |
|---|---|---|
| Orange | The visible value differs from the current source baseline. | Confirm the value is intentional, then run the loader. |
| Purple | A valid manual correction is already active. | Leave it unless you intend to correct or remove it. |
| Red | A started row contains missing or invalid required values. | Read `Validation Reason`, fix the highlighted cells, and rerun the loader. |

## Date and row rules

| Situation | Required setup |
|---|---|
| One-day delivered correction | Use the same delivery override start and end date. |
| One-week delivered correction | Add or duplicate a row, use that week as the delivery override range, and enter that week’s replacement totals. |
| Planned correction | Use a complete, valid flight start and end date. Planned edits cannot use a partial week or day range. |
| Metadata-only correction | Edit the metadata field on the existing package row; no metric edit is required. |
| New manual-only package | Add a row only when the package is absent, then provide package ID, site, friendly name, flight dates, delivery dates, at least one value, and the required reporting metadata. |
| Undo | Clear the edited value or restore the current source value, then rerun the loader. The preservation guard may stop the run when the user’s intent is not safely proven. |

Each date cell is parsed independently. ISO dates, U.S.-formatted dates, Google Sheets date serials, and R date serials can coexist in the column without forcing other rows into the same format.

Common blockers are a missing package ID, invalid dates, no actual changed value, duplicate active package/date/metric edits, a partial-range planned edit, or missing metadata on a new package.

## How the data moves

```text
Package Editor
    |
    | Manual Data Editor loader
    v
Raw manual snapshot + permanent history
    |
    v
Daily manual package/date rows
    |
    v
Compatibility base model and clustered support table
    |
    | Master-model refresh wrapper
    v
Master Data Model V3
```

### Active inputs

| Input | Purpose | Warning |
|---|---|---|
| Production `Package Editor` Sheet | Holds intentional user corrections and audit stamps | A visible value without accepted edit evidence is not automatically a manual override. |
| [Package lookup table](https://console.cloud.google.com/bigquery?project=looker-studio-pro-452620&p=looker-studio-pro-452620&d=master_stg&t=manual_package_editor_package_lookup&page=table) | Supplies the current source-backed package list | An old Sheet-only package does not become a normal source package. |
| PRISMA package data | Supplies planned totals, flight dates, and metadata baselines | Planned values are complete-flight totals, not daily editor values. |
| [Reporting mart](https://console.cloud.google.com/bigquery?project=looker-studio-pro-452620&p=looker-studio-pro-452620&d=master_stg&t=data_model_mart&page=table) | Supplies the visible current-value snapshot and delivered baselines | Final fields may already contain manual values, so the loader uses source-derived comparisons where needed. |

| Layer | One row represents | Responsibility |
|---|---|---|
| Raw manual table | One editor row | Stores the entered replacement, validation result, and audit evidence. |
| Manual history table | One accepted editor state at one publish time | Preserves recoverable history before the raw snapshot is replaced. |
| Daily manual table | One package on one date | Spreads valid delivered or planned replacements across the approved date range. |
| Compatibility base model | Usually one package on one date | Applies manual values to final `_` fields and retains `man_*` evidence. |
| V3 | Lowest available source detail | Keeps detailed source evidence while manual package/date delivery rows replace final actuals for edited dates. |

### Source boundaries

- PRISMA supplies planned totals, flight dates, and package metadata baselines.
- Delivery sources supply spend, impressions, clicks, and video baselines.
- The live Sheet supplies intentional corrections only; it is not an authority for unedited source rows.
- Prior accepted manual evidence is user-owned state. A refresh must not silently remove it.
- Final `_` fields are the reporting values. `man_*` and `qa_manual_*` fields explain whether manual evidence affected them.

## Canonical files and protected surfaces

| Surface | Role | Safety boundary |
|---|---|---|
| [Canonical loader](/Users/eugenetsenter/Looker_clonedRepo/looker_personal/master_data_model/model/manual_editor/load_manual_package_edits.R) | Implements edit detection, validation, warehouse writes, and Sheet refresh | The shorter script in this folder is only the supported launcher. |
| [Canonical V3 builder](/Users/eugenetsenter/Looker_clonedRepo/looker_personal/master_data_model/model/final_model/create_master_stg_data_model_v3.sql) | Rebuilds the stored V3 table | Run it through the verified refresh wrapper for normal operations. |
| [Master-model refresh wrapper](/Users/eugenetsenter/Docs/R_Studio_Projects/universal_cron_runner/automation_hub/workloads/ops/bq_trigger/run_master_data_model_clustered_advertiser_refresh.sh) | Refreshes the clustered dependency, rebuilds V3, and verifies both | This writes production BigQuery tables. Do not run it as a diagnostic probe. |
| [QA runbook](/Users/eugenetsenter/Looker_clonedRepo/looker_personal/master_data_model/manual_package_edits/QA_RUNBOOK.md) | Owns full trace queries and troubleshooting | Use it when the short checks in this guide disagree. |
| Live Sheet formatting | User-owned working interface | Do not run `setup_manual_package_editor_sheet.mjs` unless the user explicitly requests a full formatting rebuild. |

Routine data refreshes use the loader only. They must not rebuild the Sheet’s formatting. Before any future header, column-order, visibility, or conditional-formatting change, take a read-only formatting snapshot and verify the live rendered result.

## Known limits and maintenance

- The `Request refresh` control sends email and optional Slack notification only. Someone must still run both production commands.
- V3 is a stored table, so a successful loader does not make V3 current until the V3 refresh finishes.
- Global V3 checks do not prove one package’s value. Always run the package-specific check for the correction you published.
- Preserve the live Sheet’s user-created formatting. Use the loader for routine refreshes and the setup script only after explicit full-rebuild approval.
- Keep the loader, V3 builder, wrapper, and this guide aligned whenever the refresh order or pass messages change.

## Troubleshooting

| Symptom | First check | Healthy result |
|---|---|---|
| Loader reports blocked rows | `Validation Status` and `Validation Reason` in the Sheet | Every intended edit is `valid`; unintended rows are not active. |
| Loader passes, but V3 still shows the old value | Confirm the V3 refresh wrapper ran after the loader | Both wrapper verification messages are present. |
| V3 refresh fails before the rebuild | Read the scheduled transfer’s final state | The transfer must be `SUCCEEDED`; `PENDING` or `RUNNING` is not completion. |
| V3 refresh fails its final QA | Use the QA runbook before rerunning | Duplicate-key, planned-carrier, and retired-row checks all return zero. |
| Sheet refresh fails with a Google permission error | Confirm the configured account can access the Sheet and has Drive and Sheets scopes | The same account can read and update the production Sheet. |
| A prior valid edit may disappear | Stop and read the preservation-guard message | Restore the row or make an explicit newer edit; do not bypass the guard. |

### What does not prove the workflow finished

- Checking `Request refresh`
- A loader process that exits without the final proof summary
- A successful raw-table upload without a passing daily-total proof
- A successful V3 build query without the wrapper’s table and grain checks
- A global row count without checking the edited package and values

[^1]: Rebuild: replaces the stored table with a new result derived from the current sources; any required input not present at rebuild time will not survive automatically.
[^2]: Grain: the business meaning of one row, which determines which values may be safely summed or joined.
