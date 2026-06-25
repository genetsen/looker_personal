# Manual Package Editor QA Runbook

This runbook is for checking the Manual Data Editor when a dashboard value, sheet marker, loader run, or manual override looks wrong. It is intentionally operational: start at the quick map, then jump to the specific layer that needs proof.

## Quick Map

| Question | First Place To Check | Healthy Result | Next Hop |
|---|---|---|---|
| Did the user edit the right visible field? | Google Sheet `Package Editor` tab | The changed value is in a visible editable column, not a hidden helper column | Raw landing table |
| Did the loader recognize the edit? | `landing.master_data_model_manual_package_edits_raw` | Row is `is_active = TRUE` and `validation_status = 'valid'` | Daily landing table |
| Did a metric edit turn into daily rows? | `landing.master_data_model_manual_package_daily` | Daily rows exist for the edited package/date range and totals match the replacement | `master_stg.data_model` |
| Did a metadata edit apply package-wide? | `master_stg.data_model` | Rows for the package use the manual metadata even outside metric override dates | Reporting mart |
| Did the dashboard-ready output change? | `master_stg.data_model_mart` | Final `_` fields reflect manual values and `man_*` evidence is visible | Dashboard refresh |
| Did a row get blocked? | Google Sheet `Validation Status` / `Validation Reason` and raw landing table `validation_messages` | Message explains what must be fixed | Sheet row and instructions |
| Did a purple marker appear unexpectedly? | Sheet value, hidden baseline, hidden manual marker, raw table, daily table, model, mart | Marker is TRUE only when a valid backend manual value is still needed | Loader baseline logic |
| Did filters miss newly added rows? | Google Sheet basic filter and slicer ranges | Ranges cover the full current grid, not only the last populated row | Filter range repair script |
| Did `Edited Rows` filter miss edited rows? | Hidden `Edited Row Filter` helper and `Edited Rows` slicer column | Helper is TRUE for every active edited row; slicer points to that helper column | Loader filter-helper write |

## End-To-End Data Flow

| Step | Owner | Input | Output | QA Proof |
|---|---|---|---|---|
| 1. User finds package | Google Sheet slicers | Current package list from loader | Filtered visible rows | Slicers narrow the visible table without changing backend data |
| 2. User edits visible cell | Google Sheet `Package Editor` | Editable date, metric, or metadata cell | Changed visible value | Orange or purple marker behavior matches hidden marker/baseline state |
| 3. User requests refresh | Apps Script notification | `Request refresh` checkbox or menu item | Email, optional Slack message, status text | Email includes the full loader command; checkbox resets to unchecked |
| 4. Loader refreshes sheet values | `manual_package_edits/load_manual_package_edits.R` | Sheet values, prior raw table, reporting mart, PRISMA package totals | Refreshed visible values plus hidden manual-marker columns | Loader completes and prints status |
| 5. Loader writes raw edit rows | BigQuery raw landing table | All visible package rows | One row per package/editor row with current, replacement, validation, and metadata fields | Active valid rows match intentional edits only |
| 6. Loader writes daily metric rows | BigQuery daily landing table | Valid active metric edits | One row per package/date for edited metrics | Daily proof totals equal replacements |
| 7. Model applies manual data | `master_stg.data_model` | Normal model rows plus manual raw/daily rows | Manual-prioritized final fields and `man_*` evidence | Final fields use manual values where `man_*` exists |
| 8. Mart prepares reporting output | `master_stg.data_model_mart` | Manual-prioritized model | Dashboard-ready filtered output | Low-signal DCM rows are excluded and rollups recalculate |

## Script Inventory

| Script | What It Does | Live Effect | Run It When | Watchouts |
|---|---|---|---|---|
| [Main loader](/Users/eugenetsenter/Looker_clonedRepo/looker_personal/master_data_model/manual_package_edits/load_manual_package_edits.R) | Reads the sheet and warehouse, detects edits, validates rows, writes raw/daily landing tables, and refreshes visible values plus hidden markers | Writes BigQuery and sheet values | Normal manual editor refresh | Does not rebuild formatting |
| [Universal runner wrapper](/Users/eugenetsenter/Docs/R_Studio_Projects/universal_cron_runner/automation_hub/workloads/ops/master_manual_package_edits/load_master_manual_package_edits.R) | Runs the production manual editor loader from the universal-runner workflow | Writes BigQuery and sheet values | Running through the universal runner path | Sources the main loader |
| [Apps Script audit and request control](/Users/eugenetsenter/Looker_clonedRepo/looker_personal/master_data_model/manual_package_edits/apps_script/Code.js) | Stamps row-level manual-edit audit fields and sends the request-refresh email or optional Slack message | Writes audit cells and sends notification | Installed in the Google Sheet | Does not run the loader or write BigQuery |
| [Choice-logic tests](/Users/eugenetsenter/Looker_clonedRepo/looker_personal/master_data_model/manual_package_edits/tests/test_loader_choice_logic.R) | Tests edit detection rules for undo, corrections, stale source catch-up, blanks, zero overrides, text, and dates | No live writes | Before claiming loader choice logic is safe | Local regression test only |
| [Sheet UX rebuild](/Users/eugenetsenter/Looker_clonedRepo/looker_personal/master_data_model/manual_package_edits/setup_manual_package_editor_sheet.mjs) | Rebuilds sheet formatting, slicers, instructions, protections, hidden helpers, notes, widths, and colors | Rewrites sheet formatting | Only with explicit full-formatting-rebuild approval | Do not run during routine QA; live user formatting is source of truth |
| [Sheet geometry repair](/Users/eugenetsenter/Looker_clonedRepo/looker_personal/master_data_model/manual_package_edits/repair_manual_package_editor_filters.mjs) | Expands native filter and slicer ranges, points `Edited Rows` to `Edited Row Filter`, shows user-facing audit/status columns, and hides/protects backend columns | Filter/slicer ranges, column visibility, and the affected protected ranges only | After loader row-count or column-count/header changes | Loader resolves this helper from its own folder when sourced by the universal runner; if Node is installed outside common Mac paths, set `MASTER_MANUAL_EDIT_NODE` |
| [Manual table schema deploy](/Users/eugenetsenter/Looker_clonedRepo/looker_personal/master_data_model/create_manual_package_edit_tables.sql) | Creates or replaces the manual raw and daily landing table schemas | Writes BigQuery schemas | Schema deployment only | Not part of routine refresh |
| [Master model deploy](/Users/eugenetsenter/Looker_clonedRepo/looker_personal/master_data_model/create_master_stg_data_model.sql) | Applies manual rows into the master package/date model | Replaces BigQuery view if deployed | Model deployment only | Compare local SQL to live before deploy |
| [Reporting mart deploy](/Users/eugenetsenter/Looker_clonedRepo/looker_personal/master_data_model/create_master_stg_data_model_mart.sql) | Builds the dashboard-ready mart over the master model | Replaces BigQuery view if deployed | Mart deployment only | Filters `low_signal_dcm` rows and recalculates rollups |

## Table And View Inventory

| Object | Grain | Role | Key QA Fields |
|---|---|---|---|
| [PRISMA expanded full](https://console.cloud.google.com/bigquery?project=looker-studio-pro-452620&ws=!1m5!1m4!4m3!1slooker-studio-pro-452620!2s20250327_data_model!3sprisma_expanded_full) | PRISMA package/day | Source for planned values, flight dates, and package metadata | `package_id`, `start_date`, `end_date`, `planned_amount`, `planned_impressions`, `package_name` |
| [Reporting mart snapshot](https://console.cloud.google.com/bigquery?project=looker-studio-pro-452620&ws=!1m5!1m4!4m3!1slooker-studio-pro-452620!2smaster_stg!3sdata_model_mart) | Package/date reporting mart | Source for the visible current dashboard snapshot in the sheet | `_package_id`, `_spend`, `_impressions`, `_planned_spend`, `_planned_impressions`, `qa_row_data_issue_category` |
| [Raw manual edits](https://console.cloud.google.com/bigquery?project=looker-studio-pro-452620&ws=!1m5!1m4!4m3!1slooker-studio-pro-452620!2slanding!3smaster_data_model_manual_package_edits_raw) | One row per editor row | Raw manual edit record, validation status, replacement values, metadata overrides, current baselines | `is_active`, `validation_status`, `validation_messages`, `replacement_*`, `man_*`, `current_*`, `manual_edit_*` |
| [Daily manual rows](https://console.cloud.google.com/bigquery?project=looker-studio-pro-452620&ws=!1m5!1m4!4m3!1slooker-studio-pro-452620!2slanding!3smaster_data_model_manual_package_daily) | Package/date for active valid metric edits | Daily manual metric rows used by the model | `package_id`, `date`, `man_daily_*`, `man_total_*_doNotSum`, `manual_edit_*` |
| [Master evidence model](https://console.cloud.google.com/bigquery?project=looker-studio-pro-452620&ws=!1m5!1m4!4m3!1slooker-studio-pro-452620!2smaster_stg!3sdata_model) | Package/date evidence layer | Applies manual values before rollups and preserves `man_*` evidence | `_spend`, `_impressions`, `_planned_spend`, `_planned_impressions`, `man_daily_*`, `man_total_*`, `qa_manual_*` |
| [Dashboard reporting mart](https://console.cloud.google.com/bigquery?project=looker-studio-pro-452620&ws=!1m5!1m4!4m3!1slooker-studio-pro-452620!2smaster_stg!3sdata_model_mart) | Dashboard-ready package/date | Filters low-signal rows and recalculates reporting rollups | Final `_` fields, `man_*` evidence, `qa_manual_*`, rollup fields |

## Sheet Surface

| Area | Visible To User? | Editable? | Purpose |
|---|---:|---:|---|
| `Package ID`, `Site`, `Package Friendly Name` | Yes | Existing rows mostly locked; new rows can fill required IDs | Find the right package and identify new manual rows |
| `Flight Start Date`, `Flight End Date`, `Planned Spend`, `Planned Impressions` | Yes | Yes | Full-flight package plan corrections |
| `Spend`, `Impressions`, `Clicks`, `Video Plays`, `Video Completions` | Yes | Yes | Delivered metric corrections for the selected delivery override dates |
| `Delivery Override Start Date`, `Delivery Override End Date` | Yes | Yes | Date window for metric overrides only |
| `Advertiser`, `Package Type`, `Channel`, `Campaign`, `Initiative`, `Supplier Code`, `Supplier Name`, `Package Name`, `GS Channel` | Yes | Yes | Package-wide metadata corrections and required metadata for new rows |
| `Primary Row Data Source`, `Validation Status`, `Validation Reason` | Yes | No | Read-only row context and loader validation feedback |
| Baseline columns | Hidden | No | Source-derived comparison values used by formatting and loader |
| Manual marker columns | Hidden | No | TRUE/FALSE evidence for purple manual-marker formatting |
| `Request refresh` | Yes | Yes | Sends notification only; does not run loader |

## Baseline And Edit Detection Rules

The loader decides whether a visible cell is a real manual override by comparing it to source-derived baselines and the prior raw manual table.

| Field Family | Baseline Used | Why |
|---|---|---|
| Planned spend and planned impressions | PRISMA package totals | Planned values are full-flight package values and should not be distorted by mart row filters |
| Delivered spend, impressions, clicks, video plays, video completions | Recalculated raw delivery fields from the reporting mart rows | Avoids comparing against manual-affected final fields |
| Flight dates | Current source package flight dates | Flight dates are package-level fields |
| Metadata | Current source package metadata | Metadata overrides should apply package-wide |

| User Action | Loader Outcome |
|---|---|
| Cell is blank for a metric | Revert to current baseline; no manual override |
| Cell equals current source baseline | No manual override |
| Cell equals prior current value after source has changed | Treat as undo; no manual override |
| Cell equals prior replacement and still differs from current baseline | Keep the manual override active |
| Cell differs from current baseline and prior value | Capture as a new or corrected manual override |
| Source catches up to prior manual replacement | Clear stale manual evidence on next loader run |

## Validation Rules

| Rule | Blocks? | Message In Raw Table |
|---|---:|---|
| Missing package ID | Yes | `missing package ID` |
| Metric edit has missing or invalid delivery override dates | Yes | `invalid or missing delivery override date range` |
| Flight start/end is invalid | Yes | `invalid package flight date range` |
| Planned metric edited on a partial date range | Yes | `planned metrics can only be edited on the full flight date range` |
| New/manual-only row missing required metadata | Yes | `new package missing required metadata` |
| Duplicate active edit for same package, date, and metric | Yes | `duplicate active package/date metric: ...` |
| Row has no changed values | Inactive unless the row was otherwise marked active | `not edited` or `no changed values` |

Required metadata for a new/manual-only package:

- `Advertiser`
- `Campaign`
- `Package Type`
- `Package Name`
- `GS Channel`
- `Site` / supplier name
- `Channel`
- backend compatibility fields `channel_group` and `media_name`, which the loader can derive from visible values for existing editor rows

## How Manual Data Merges Into The Model

| Edit Type | Landing Layer | Merge Key | Final Behavior |
|---|---|---|---|
| Delivered metric correction | Daily manual table | `package_id` + `date` | Replaces only the edited metric on the edited dates |
| Planned metric correction | Daily manual table | `package_id` + `date` across the full flight | Replaces planned daily values and keeps package-level totals aligned |
| Flight date correction | Raw manual table metadata CTE | `package_id` | Applies to every model row for that package |
| Metadata correction | Raw manual table metadata CTE | `package_id` | Applies to every model row for that package, including dates outside metric override windows |
| Manual-only package | Daily manual table plus metadata from raw table | New `package_id` + date | Adds rows only when required metadata and metric/date fields are valid |

The model uses manual-priority behavior in the final fields. In plain English: if a valid `man_*` value exists for the field and grain, the final `_` field uses it; otherwise it uses the normal source value.

## Filters And Gotchas

| Filter Or Rule | Where It Happens | Why It Matters |
|---|---|---|
| `low_signal_dcm` label | `master_stg.data_model` column `qa_row_data_issue_category` | Marks tiny DCM-only noise rows |
| `low_signal_dcm` exclusion | `master_stg.data_model_mart` | Dashboard current values exclude those rows |
| PRISMA package scope | Loader planned-total lookup | Planned package totals exclude `package_type = 'Child'`, require `start_date >= 2025-01-01`, and require a non-null package ID |
| PRISMA package planned totals | Loader lookup | Planned package totals are not reduced by mart row filtering |
| Digital delivered baseline scope | Loader current-value lookup | Digital delivered metrics count rows with PRISMA evidence; TV and social use their own source fields |
| Active valid manual daily rows only | `manual_package_daily` CTE in `data_model` | Blocked and inactive rows do not reach final data |
| Active valid metadata rows only | `manual_package_metadata` CTE in `data_model` | Metadata overrides do not need metric edits, but blocked rows do not apply |
| Manual metric rows date-bound | Join on package/date | Metric corrections only affect the selected delivery override range |
| Metadata package-wide | Join on package only | Metadata corrections affect every date for the package |

Low-signal DCM threshold:

| Condition | Threshold |
|---|---:|
| No updated FPD spend or impressions | required |
| No original FPD spend or impressions | required |
| DCM delivery exists | required |
| DCM impressions | `< 1,000` |
| DCM media cost | `< 0.10` |
| DCM clicks | `< 5` |
| Package average DCM impressions across these rows | `< 100` |

## Normal Run Commands

Direct loader run:

```bash
Rscript /Users/eugenetsenter/Looker_clonedRepo/looker_personal/master_data_model/manual_package_edits/load_manual_package_edits.R
```

The production manual editor sheet is the loader default. Use `MASTER_MANUAL_EDIT_SHEET_ID=...` only when intentionally testing a different copy of the sheet.

Universal runner wrapper only:

```bash
Rscript /Users/eugenetsenter/Docs/R_Studio_Projects/universal_cron_runner/automation_hub/workloads/ops/master_manual_package_edits/load_master_manual_package_edits.R
```

Full universal runner:

```bash
Rscript /Users/eugenetsenter/Docs/R_Studio_Projects/universal_cron_runner/universal_script_runner.R
```

Choice-logic regression test:

```bash
Rscript manual_package_edits/tests/test_loader_choice_logic.R
```

R parse check:

```bash
Rscript -e 'invisible(parse("manual_package_edits/load_manual_package_edits.R")); cat("R parse OK\n")'
```

## QA Queries

### 1. Overall Manual Table Status

```sql
SELECT
  COUNT(*) AS raw_rows,
  COUNTIF(is_active) AS active_rows,
  COUNTIF(validation_status = 'valid') AS valid_rows,
  COUNTIF(validation_status = 'blocked') AS blocked_rows,
  COUNTIF(replacement_spend IS NOT NULL) AS spend_replacement_rows,
  COUNTIF(replacement_impressions IS NOT NULL) AS impression_replacement_rows,
  COUNTIF(man_channel IS NOT NULL OR man_campaign_name IS NOT NULL OR man_package_name IS NOT NULL) AS metadata_replacement_rows,
  MAX(loaded_at) AS latest_loaded_at
FROM `looker-studio-pro-452620.landing.master_data_model_manual_package_edits_raw`;
```

### 2. Blocked Rows

```sql
SELECT
  package_id,
  validation_status,
  validation_messages,
  man_start_date,
  man_end_date,
  replacement_spend,
  replacement_impressions,
  replacement_planned_spend,
  replacement_planned_impressions,
  loaded_at
FROM `looker-studio-pro-452620.landing.master_data_model_manual_package_edits_raw`
WHERE validation_status = 'blocked'
ORDER BY loaded_at DESC, package_id
LIMIT 50;
```

### 3. Package Trace From Raw To Mart

Replace `PACKAGE_ID_HERE` before running.

```sql
WITH raw AS (
  SELECT
    'raw' AS layer,
    package_id,
    CAST(NULL AS DATE) AS date,
    is_active,
    validation_status,
    replacement_spend,
    replacement_impressions,
    replacement_planned_spend,
    replacement_planned_impressions,
    man_channel,
    man_campaign_name,
    man_package_name,
    loaded_at
  FROM `looker-studio-pro-452620.landing.master_data_model_manual_package_edits_raw`
  WHERE package_id = 'PACKAGE_ID_HERE'
),
daily AS (
  SELECT
    'daily' AS layer,
    package_id,
    date,
    is_active,
    validation_status,
    man_total_spend_doNotSum AS replacement_spend,
    man_total_impressions_doNotSum AS replacement_impressions,
    man_total_planned_spend_doNotSum AS replacement_planned_spend,
    man_total_planned_impressions_doNotSum AS replacement_planned_impressions,
    CAST(NULL AS STRING) AS man_channel,
    CAST(NULL AS STRING) AS man_campaign_name,
    CAST(NULL AS STRING) AS man_package_name,
    loaded_at
  FROM `looker-studio-pro-452620.landing.master_data_model_manual_package_daily`
  WHERE package_id = 'PACKAGE_ID_HERE'
)
SELECT * FROM raw
UNION ALL
SELECT * FROM daily
ORDER BY layer, date;
```

### 4. Daily Totals Equal Replacement Totals

```sql
SELECT
  package_id,
  edit_id,
  MIN(date) AS first_date,
  MAX(date) AS last_date,
  SUM(COALESCE(man_daily_spend, 0)) AS daily_spend_sum,
  MAX(man_total_spend_doNotSum) AS replacement_spend,
  SUM(COALESCE(man_daily_impressions, 0)) AS daily_impressions_sum,
  MAX(man_total_impressions_doNotSum) AS replacement_impressions,
  SUM(COALESCE(man_daily_clicks, 0)) AS daily_clicks_sum,
  MAX(man_total_clicks_doNotSum) AS replacement_clicks
FROM `looker-studio-pro-452620.landing.master_data_model_manual_package_daily`
GROUP BY package_id, edit_id
ORDER BY package_id, edit_id;
```

### 5. Final Model Evidence For A Package

Replace `PACKAGE_ID_HERE` before running.

```sql
SELECT
  date,
  _package_id,
  _spend,
  _impressions,
  _planned_spend,
  _planned_impressions,
  _clicks,
  _video_plays,
  man_edit_id,
  qa_manual_edit_flag,
  qa_manual_edit_at,
  qa_manual_edit_by,
  qa_manual_edit_published_at,
  man_start_date,
  man_end_date,
  man_daily_spend,
  man_daily_impressions,
  man_daily_planned_spend,
  man_daily_planned_impressions,
  man_loaded_at
FROM `looker-studio-pro-452620.master_stg.data_model`
WHERE _package_id = 'PACKAGE_ID_HERE'
ORDER BY date
LIMIT 200;
```

### 6. Mart Low-Signal Filter Check

```sql
SELECT
  'data_model' AS object_name,
  COUNTIF(CONTAINS_SUBSTR(qa_row_data_issue_category, 'low_signal_dcm')) AS low_signal_rows
FROM `looker-studio-pro-452620.master_stg.data_model`
UNION ALL
SELECT
  'data_model_mart' AS object_name,
  COUNTIF(CONTAINS_SUBSTR(qa_row_data_issue_category, 'low_signal_dcm')) AS low_signal_rows
FROM `looker-studio-pro-452620.master_stg.data_model_mart`;
```

## Troubleshooting

| Symptom | Likely Cause | Check | Fix |
|---|---|---|---|
| Purple marker on a cell the user did not edit | Stale backend manual value or hidden marker still TRUE | Compare visible value, baseline column, manual marker column, raw table, daily table, model, and mart | Run loader after baseline fix; if still active, inspect raw row replacement/current values |
| User clicked request refresh but no dashboard change happened | Request control only sends notification | Apps Script status cell and email | Run loader or universal runner |
| User clicked request refresh but no email arrived | Installable trigger missing or Apps Script auth stale | `Manual Editor` menu, `getManualEditorAutomationStatus()` | Run `Authorize request button` from the sheet menu |
| Planned edit on one week is blocked | Planned metrics are full-flight only | Raw validation message | Use full flight for planned values; use partial dates only for delivered metrics |
| Metadata changed but dates outside override window did not change as expected | Model deploy drift or metadata raw row blocked | Raw table active valid metadata fields; model rows outside date range | Verify raw metadata CTE and model deployment |
| New row does not appear in reporting | Missing required metadata, invalid dates, or no metric | Raw validation message | Fill required visible metadata and date fields, then rerun loader |
| Daily totals do not equal replacement | Allocation bug or unexpected numeric parsing | Daily totals query | Stop before claiming success; inspect `allocate_daily_total()` and loader proof |
| Planned package total differs from mart row sum | Mart excludes low-signal rows | Compare PRISMA package total to mart sum | This is expected; planned baselines use PRISMA totals |

## What Not To Do

- Do not run `manual_package_edits/setup_manual_package_editor_sheet.mjs` against the live sheet during routine QA.
- Do not edit non-fee rows for tests unless explicitly approved.
- Do not treat the request-refresh checkbox as proof the data loaded.
- Do not use final manual-affected `_` fields as the baseline comparator for edit detection.
- Do not deploy local SQL over production unless local has first been compared to the live view definition.
