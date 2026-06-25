# FPD Loader (First Party Data)

R-based ETL pipelines that collect partner marketing data from Google Sheets, normalize heterogeneous column schemas, and expand date-ranged records into daily granularity for BigQuery. Part of the ADIF (Adidas Data & Insight Framework) infrastructure.

This directory contains two pipelines:

| Script | Purpose | BigQuery Target |
|---|---|---|
| `util_collect_fpd_shortcutsFolder.r` | Main 7-phase loader that discovers partner sheets, normalizes columns, expands to daily rows, and uploads | `landing.fpd_data_ranged_shortcutsFolder` |
| `manually_updated_data_loader.r` | Process manually corrected/updated FPD figures from a single sheet | `landing.manually_updated_fpd_daily` |

Legacy variants (`util_collect_fpd_v2.r` / `util_collect_fpd_v3.r`) still exist elsewhere in the broader repo, but they are not the primary entrypoint in this folder.

## Quick Start

If you want a fast orientation first, read `docs/PROJECT_OVERVIEW.md`.

```bash
# Install R packages (one-time)
Rscript -e 'install.packages(c("googledrive", "googlesheets4", "dplyr", "stringr", "readr", "lubridate", "janitor", "bigrquery", "tidyr"))'

# Run the main FPD collection pipeline (7 phases + upload)
Rscript util_collect_fpd_shortcutsFolder.r

# Run only Apollo sheets, then update only those sheets' rows in BigQuery
Rscript util_collect_fpd_shortcutsFolder.r --pattern="APO | Partner Data"

# Run the manually-updated data loader
Rscript manually_updated_data_loader.r
```

On first run, a browser window will open for Google OAuth. Subsequent runs use the cached token.

Default behavior now starts from Phase 1 with a fresh run. Reusing saved phase outputs is for debugging only.

## Safe Run Checklist (Beginner)

Use this quick checklist before and after every run:

1. Confirm you are in this folder:
   `pwd` should end with `FPD/FPD_loader`.
2. Confirm you are running the correct script:
   main flow = `Rscript util_collect_fpd_shortcutsFolder.r`
   manual corrections flow = `Rscript manually_updated_data_loader.r`
3. Confirm output folder is available:
   `output/` should exist for the main flow.
4. Run one script at a time:
   do not run both scripts at the same time.
5. After the run, check these files first:
   `output/phase7_daily_master_data.csv`
   `output/phase7_validation_table.csv`
6. If the validation file shows differences:
   review `output/phase6_filter_audit.csv` to see which rows were removed and why.
7. Before rerunning to debug:
   set `use_saved_phases <- TRUE` and pick one `current_phase` to rerun only the part you are working on.

## What Changed / How To Undo

- What changed:
  the main loader now supports shortcut-aware discovery, a one-run `--pattern` override, default per-sheet cache reuse for unchanged files, staged BigQuery sync that updates only the sheets included in the current run while auto-adding safe new columns in BigQuery, an in-pipeline APO `creative_git_link` refresh that runs during Phase 5 before BigQuery upload, a checkpoint-read safeguard that forces sparse rate columns like `ctr_vcr` back to numeric before later phases run, and character coercion for `partner_placement_name` so mixed text/date sheet cells can be combined safely.
- How to undo:
  if the shortcut-aware flow causes a bad result, restore the previous script version from Git and point daily runs back to the earlier loader entrypoint.

## How It Works

The pipeline runs 7 sequential phases. Each phase writes a checkpoint CSV to `output/`, so you can inspect intermediate results and re-run individual phases without starting from scratch.

```
Google Drive (partner sheets)
  Phase 1  ──>  Discover sheets
  Phase 2  ──>  Detect header rows
  Phase 3  ──>  Extract column names
  Phase 4  ──>  Build normalization mapping
  Phase 5  ──>  Ingest & combine all data
  Phase 6  ──>  Clean dates, extract IDs, filter
  Phase 6.1 ──>  Sheet-level summary with diagnostics
  Phase 7  ──>  Expand date ranges to daily rows
     │
     ▼
  BigQuery (landing.fpd_data_ranged_shortcutsFolder)
     │
     ▼
  APO creative GitHub link refresh (APO sheets only, inside Phase 5)
    updates the sheet before the final BigQuery write
```

### APO Creative Refresh Handoff

For APO sheets, the main loader now refreshes `creative_git_link` values during Phase 5 before the data is normalized and uploaded to BigQuery.

The in-pipeline APO refresh does this only for live-read APO sheets:

- reads `Final_img_path` or, when that is blank, the APO fallback column `Creative img PATH`, plus existing `creative_git_link` / tracking columns
- normalizes the local file path to the current machine
- uploads any missing creative files to `genetsen/apo-db-creat`
- writes `creative_git_link`, `creative_git_last_final_img_path`, and `creative_git_last_box_link` back into the sheet
- re-reads the updated sheet data and continues the normal Phase 5 -> Phase 7 -> BigQuery flow

This keeps BigQuery current in the same run instead of requiring a second sync after a separate creative-refresh script.

## Configuration

Edit the top of `util_collect_fpd_shortcutsFolder.r`:

| Variable | Purpose | Default |
|---|---|---|
| `gdrive_folder_id` | Google Drive folder ID containing partner sheets | `"1d--Bc554eBaRCr8blt1LnUYiOMHQe7jF"` |
| `pattern` | Sheet name pattern to match during discovery (can be overridden with `--pattern`) | `"| Partner Data"` |
| `output_dir` | Directory for checkpoint CSVs | `/Users/eugenetsenter/Looker_clonedRepo/looker_personal/FPD/FPD_loader/output` |
| `cache_dir` | Per-sheet cache directory used by the default cache-reuse mode | `/Users/eugenetsenter/Looker_clonedRepo/looker_personal/FPD/FPD_loader/output/sheet_cache` |
| `use_saved_phases` | When `TRUE`, load cached CSVs for phases other than `current_phase` | `FALSE` |
| `current_phase` | The phase to actively compute (1-7) | `1` |
| `known_kpi_metrics` | Metric columns to treat as numeric and split across days in Phase 7 | See below |
| `client_week_config` | Per-client week start day for filling missing `week` values in Phase 7 | `mass/oli/adif=Sun`, `apollo=Mon` |

### Optional CLI Flag (Main Script)

Use this when you want to test a different sheet-name match pattern or disable the default file cache without editing the script:

```bash
# Override only this run
Rscript util_collect_fpd_shortcutsFolder.r --pattern="| Partner Data"

# Example: process only Apollo partner sheets
Rscript util_collect_fpd_shortcutsFolder.r --pattern="APO | Partner Data"

# Example: force fresh Google Sheets reads for this run only
Rscript util_collect_fpd_shortcutsFolder.r --pattern="APO | Partner Data" --no-file-cache

# Show usage/help
Rscript util_collect_fpd_shortcutsFolder.r --help
```

If `--pattern` is not provided, the script uses the default value from the configuration block.
By default, the script stores per-sheet cache files in `output/sheet_cache/` and reuses them only when the Drive `last_modified_time` for that file matches the cached copy.
Cache files now include a schema version, and a modified-time or schema change resets the cached payload before writing a refreshed field. This prevents a fresh `header_row` or `raw_headers` write from making older `raw_data` look current.
Use `--no-file-cache` when you want to bypass cache reads while still rebuilding the cache files from fresh sheet reads during that run.

### KPI Metrics List

```r
known_kpi_metrics <- c(
  "spend", "impressions", "sends", "opens", "views", "completed_views",
  "engagements", "clicks", "pageviews", "conversions", "benchmark_metric", "benchmark"
)
```

If a new partner includes a metric column not in this list, add it here. Only columns in this list are coerced to numeric and divided during daily expansion.

Checkpoint CSV reads also force `ctr` and `ctr_vcr` back to numeric so sparse rate fields do not get re-read as logical blank columns before BigQuery upload.

## Inputs

### Google Sheets

The pipeline reads from a Google Drive folder containing spreadsheet files or spreadsheet shortcuts that match the `pattern` text. Shortcuts are automatically resolved to their target Google Sheet IDs before ingestion. Each sheet must have:

- A tab named **`data`**
- A header row with **5+ non-empty columns** (auto-detected, does not need to be row 1)
- Data in columns **A through Y**

Partner sheets can use different column names — the pipeline normalizes them (see Phase 4).

### Authentication

- **Google Drive/Sheets**: OAuth via `googledrive` and `googlesheets4`. First run opens a browser for consent; token is cached for subsequent runs.
- **BigQuery**: Uses `bigrquery` with project `looker-studio-pro-452620`.

## Outputs

All checkpoint CSVs are written to `output/`.

### `phase1_discovered_files.csv`

**Purpose**: Inventory of all partner sheets found in the Drive folder.

**Key columns**: `sheet_id`, `sheet_name`, `sheet_url`, `last_modified_time`, `last_modified_by`

**How to use**: Review to confirm expected partner sheets are present. Sheets with "ARCHIVE" in the name are automatically excluded.

---

### `phase2_header_detection.csv`

**Purpose**: Records where the data table starts in each sheet (which row is the header).

**Key columns**: `sheet_name`, `header_row`, `status`

**How to use**: Check `status` column — should be `"success"` for all active sheets. If a sheet shows `"no_header_found"` or `"empty_sheet"`, the sheet may have changed format or been emptied. The `header_row` number tells you exactly which row the pipeline treats as column headers.

---

### `phase3_raw_headers.csv`

**Purpose**: Catalog of every raw column name found across all sheets, with frequency counts.

**Key columns**: `column_name_raw`, `sheet_name`, `frequency_across_sheets`

**How to use**: Check this when a new partner sheet is added. High-frequency column names (appearing in many sheets) are reliably normalized. Low-frequency names may need new normalization rules in Phase 4.

---

### `phase4_normalization_mapping.csv`

**Purpose**: The complete mapping from raw column names to normalized canonical names, including which rule matched.

**Key columns**: `column_name_raw`, `normalized_name`, `rule_applied`, `frequency_across_sheets`

**How to use**: This is the reference for how column names are translated. Review `rule_applied` to understand why a column was mapped a certain way. Rules include `match_spend`, `match_impressions`, `drop_autogenerated`, `fallback_janitor`, etc. If a column is mapped incorrectly, adjust the regex rules in the Phase 4 section of the script (lines ~460-510).

---

### `phase5_combined_master_data.csv`

**Purpose**: Raw combined data from all sheets after column normalization and type coercion, before any date logic or filtering.

**Key columns**: All normalized data columns plus metadata (`source_file`, `source_url`, `last_modified_time`, `last_modified_by`, `partner_sheet`)

**How to use**: This is the "truth" dataset before cleaning. Use it to:
- Verify metric totals per partner (cross-reference with original sheets)
- Debug issues where data seems missing after Phase 6 filtering
- Compare against Phase 7 during validation

---

### `phase6_cleaned_master_data.csv`

**Purpose**: Cleaned data with standardized dates, extracted package IDs, and diagnostic columns showing which date source was used per row.

**Key columns** (in addition to Phase 5 columns):
- `start_date_final`, `end_date_final` — the canonical date range for each row
- `start_date_source`, `end_date_source` — which input column was used (e.g., `"week"`, `"start_date"`, `"prisma_start_date"`)
- `package_id` — extracted from `package_name` when not provided directly

**Rows removed by Phase 6**:
1. Rows where `source_file` contains "archive" (case-insensitive)
2. Rows where the sum of all numeric columns is 0 or NA
3. Rows where `package_name` is NA or blank

**How to use**: Check `start_date_source` and `end_date_source` to understand how dates were resolved. If a partner's data is using `prisma_start_date` as the source, it means no granular date column was available — the data will expand over the full Prisma planning range rather than weekly/monthly.

**Date coalesce precedence** (Prisma dates are last resort):
- `start_date_final`: `start_date` > `week` > `month` > `date` > `end_date` > `prisma_start_date` > `prisma_end_date`
- `end_date_final`: `end_date` > `week+6` > `month_end` > `start_date` > `date` > `prisma_end_date` > `prisma_start_date`

---

### `phase6_1_sheet_summary.csv`

**Purpose**: One-row-per-sheet summary with aggregated metrics and date source diagnostics.

**Key columns**:
- `partner_name`, `source_file`, `source_url`, `last_modified_time`, `last_modified_by`
- `row_count`, `package_count`
- `date_range_start`, `date_range_end`
- `start_date_sources` — which date columns are driving `start_date_final` for this sheet (e.g., `"week"` or `"start_date"`)
- `end_date_sources` — same for `end_date_final`
- `week_start_dates_used` — if `week` is a date source, lists the distinct week start dates (NA otherwise)
- `week_day_range` — if `week` is a date source, shows the day-of-week range (e.g., `"Sun-Sat"`, `"Mon-Sun"`)
- `spend_sum`, `impressions_sum`, `clicks_sum`, etc. — total KPI metrics per sheet

**How to use**: This is the primary diagnostic file. Check it to:
- Verify row counts and metric totals per partner
- See which date column drives each partner's data (are they weekly? monthly? have explicit ranges?)
- Confirm `week_day_range` makes sense for weekly partners (e.g., `"Sun-Sat"` means the partner reports weeks starting Sunday)
- Spot partners whose dates fall back to Prisma (indicates missing granular dates)

---

### `phase7_daily_master_data.csv`

**Purpose**: Final output — every row represents a single day. Date-ranged records are expanded, and metrics are divided evenly across the days in the range.

**Key columns** (in addition to Phase 6 columns):
- `date_final` — the specific date this row represents
- `data_update_datetime` — timestamp of when this pipeline run was executed
- All KPI metric columns now contain **daily values** (original value / number of days in range)
- `impressions` and `clicks` are rounded to whole-number integers in final output files

**How to use**: This is what gets uploaded to BigQuery. You can query by `date_final` for time-series analysis without worrying about mixed granularity. Cross-reference metric totals with Phase 5 via `phase7_validation_table.csv` to see any differences and the associated Phase 6 filter reasons.

---

### `phase6_filter_audit.csv`

**Purpose**: Filter-level audit from Phase 6 showing which rows were removed by each filter rule and the metric impact of those removals.

**Key columns**:
- `source_file` — partner sheet name
- `filter_reason` — filter code (`archive_source_file`, `numeric_row_metric_sum_zero_or_na`, `missing_package_name`)
- `removed_rows` — number of rows removed by that filter for that source
- `removed_spend`, `removed_impressions`, `removed_clicks`, etc. — summed KPI impact for removed rows

**How to use**: Use this file to explain why per-sheet totals can differ between Phase 5 and Phase 7. This is the source used to populate the `filter_reason` column in `phase7_validation_table.csv`.

---

### `phase7_validation_table.csv`

**Purpose**: Per-sheet validation table comparing KPI totals between Phase 5 and Phase 7.

**Key columns**:
- `source_file`
- `*_diff` columns (e.g., `spend_diff`, `impressions_diff`, `clicks_diff`)
- `mismatch_any` — whether any KPI diff exceeds tolerance
- `filter_reason` — aggregated reason text derived from `phase6_filter_audit.csv` (for example: `missing_package_name(rows=99, impressions=7280286)`)

**How to use**: This is the primary reconciliation output. It is saved to `output/` and printed at the end of the pipeline run so the mismatch table appears after upload/completion logs.

## Running Individual Phases

To iterate on a specific phase without re-running the full pipeline:

```r
# At the top of the script, set:
use_saved_phases <- TRUE
current_phase <- 4        # Only Phase 4 will recompute; others load from saved CSVs
```

Then run the script. All phases except `current_phase` will load their cached CSV from `output/`.

This is useful for:
- Debugging a specific phase
- Adjusting normalization rules (Phase 4) and re-running from there
- Re-running date logic (Phase 6) after fixing coalesce precedence

## BigQuery Target

| Field | Value |
|---|---|
| Project | `looker-studio-pro-452620` |
| Dataset | `landing` |
| Table | `fpd_data_ranged_shortcutsFolder` |
| Write mode | Staged partial sync (delete + reinsert only rows for sheets in the current run) |

This script uploads to a staging table first, checks the result, adds any missing BigQuery columns when the new column type is clear, and then replaces only the destination rows for the sheets included in that run.
If a brand-new column is completely blank in the current run, the script stops before changing production because it cannot infer a safe BigQuery type from empty data alone.
The optional downstream `source(".../util_process_updated_fpd.r")` call is currently commented out in this folder's script.

---

## Manually Updated Data Loader (`manually_updated_data_loader.r`)

A companion pipeline for processing **manually corrected or updated FPD figures**. While the main pipeline auto-discovers and ingests raw partner data from many sheets, this script reads from a single curated Google Sheet containing package-level totals that have been manually reviewed and adjusted.

### When to Use

Use this when:
- Partner data has been manually corrected after initial ingestion (e.g., spend adjustments, reconciliation)
- Updated impression or spend figures need to be loaded separately from the raw partner pipeline
- You need a clean, auditable path for manually-revised numbers

### How It Works

```
Google Sheet (manually updated FPD figures)
  Step 1  ──>  Read package-level records (package_id, impressions, spend)
  Step 2  ──>  Query Prisma in BigQuery for date ranges per package_id
  Step 3  ──>  Join packages with Prisma dates (left join)
  Step 4  ──>  Spread metrics evenly across Prisma date range → daily rows
  Step 5  ──>  Upload to BigQuery (landing.manually_updated_fpd_daily)
  Step 6  ──>  Print summary report
```

### Configuration

Edit the top of `manually_updated_data_loader.r`:

| Variable | Purpose | Default |
|---|---|---|
| `sheet_id` | Google Sheet with manually updated FPD data | `"1kUD8gVrHAAaZbULtFgDZl1hgGU-7Ut8fSdNJDgsZwfE"` |
| `target_gid` | Specific tab within the sheet | `"1894007924"` |
| `output_dir` | Directory for checkpoint CSVs | `/Users/eugenetsenter/Looker_clonedRepo/looker_personal/util/data_loaders/FPD_loader/MUD_output` |
| `bq_table` | BigQuery table name | `"manually_updated_fpd_daily"` |

### Input Google Sheet

The sheet must contain these columns:
- `package_id` — must match Prisma package IDs for date range lookup
- `supplier_name`, `initiative`, `package_name` — dimension columns
- `updated_FPD_IMPRESSIONS` — corrected impression totals (package-level)
- `updated_FPD_SPEND` — corrected spend totals (package-level)

Rows with no `package_id` or where both metrics are NA are filtered out.

### Date Ranges

Unlike the main pipeline (which derives dates from partner-provided columns), this script **always uses Prisma dates**. It queries `prisma_expanded_full` in BigQuery for `MIN(date)` / `MAX(date)` per `package_id`, filtered to `advertiser_name = 'Forevermark US'` and `package_type != 'Child'`.

Packages without a matching Prisma date range are skipped with a warning.

### Outputs (`MUD_output/`)

| File | Purpose |
|---|---|
| `updated_fpd_packages.csv` | Package-level data as read from the sheet (after filtering) |
| `prisma_package_dates.csv` | Date ranges retrieved from Prisma per package |
| `updated_fpd_with_dates.csv` | Joined result — packages with their Prisma date ranges |
| `updated_fpd_daily.csv` | Final daily rows (metrics ÷ days in range) |
| `manually_updated_fpd_package_summary.csv` | Summary by package sorted by spend |

### BigQuery Target

| Field | Value |
|---|---|
| Project | `looker-studio-pro-452620` |
| Dataset | `landing` |
| Table | `manually_updated_fpd_daily` |
| Write mode | `WRITE_TRUNCATE` (full replace each run) |

### Differences from Main Pipeline

| | `util_collect_fpd_shortcutsFolder.r` | `manually_updated_data_loader.r` |
|---|---|---|
| **Input** | Many partner sheets (auto-discovered from Drive) | One curated Google Sheet |
| **Column handling** | Auto-detects headers, normalizes column names | Fixed schema (`package_id`, `updated_FPD_IMPRESSIONS`, `updated_FPD_SPEND`) |
| **Date source** | Coalesced from partner-provided dates (week, start_date, etc.) | Always from Prisma (`prisma_expanded_full`) |
| **Metrics** | Many (spend, impressions, clicks, views, etc.) | Two (`updated_FPD_IMPRESSIONS`, `updated_FPD_SPEND`) |
| **Phases** | 7 phases with checkpoint/resume | 5 linear steps |
| **BQ table** | `landing.fpd_data_ranged_shortcutsFolder` | `landing.manually_updated_fpd_daily` |

---

## Troubleshooting

### Google Auth Fails

**Symptom**: `Error in gargle::token_fetch()` or browser doesn't open.

**Fix**: Delete the cached token and re-authenticate:
```r
googledrive::drive_deauth()
googlesheets4::gs4_deauth()
# Then re-run — a new browser auth window will appear
```

### "No header found" for a Sheet

**Symptom**: Phase 2 reports `no_header_found` for a partner sheet.

**Cause**: The sheet's header row has fewer than 5 non-empty columns in columns A:G, or the tab is not named `data`.

**Fix**: Open the sheet and verify:
1. The tab is named exactly `data` (case-sensitive)
2. The header row has at least 5 columns with text in columns A through G
3. There aren't blank rows above the header pushing it past row 500

### Metric Totals Don't Match Between Phase 5 and Phase 7

**Symptom**: Phase 7 validation prints `"Detected differences in per-sheet KPI totals"`.

**Cause**: Most often, row filtering differences explain this. In current logic, a KPI pre-filter already runs in Phase 5, and additional filters run in Phase 6 (archive rows, numeric-sum-zero rows, and missing package names). Any rows removed between those checkpoints can create per-sheet differences.

**Where to debug quickly**:
- `output/phase7_validation_table.csv` — shows per-sheet diffs and `filter_reason`
- `output/phase6_filter_audit.csv` — shows row/metric impact per filter reason

**Fix**: This is expected when rows are removed before expansion. The validation compares Phase 5 checkpoint totals to Phase 7 totals. To verify expansion math itself, compare Phase 6 totals with Phase 7 instead:
```r
phase6 <- readr::read_csv("output/phase6_cleaned_master_data.csv")
phase7 <- readr::read_csv("output/phase7_daily_master_data.csv")
sum(phase6$spend, na.rm = TRUE)
sum(phase7$spend, na.rm = TRUE)
# These should match
```

### A Column Is Mapped to the Wrong Normalized Name

**Symptom**: A partner's "Budget" column is not being treated as `spend`, or a column name collides with another.

**Fix**: Check `phase4_normalization_mapping.csv` to see which rule matched. Then edit the Phase 4 regex rules in the script (~lines 460-510). Rules are checked in priority order — add specific matches above general ones.

### Dates Falling Back to Prisma

**Symptom**: `phase6_1_sheet_summary.csv` shows `start_date_sources = "prisma_start_date"` for a partner.

**Cause**: That partner's sheet has no `start_date`, `week`, `month`, or `date` column — only Prisma planning dates.

**Impact**: The data expands over the entire Prisma range (could be months), dividing metrics across many days. This may undercount daily values compared to what the partner actually delivered per week.

**Fix**: Ask the partner to add a `week` or `date` column to their reporting sheet. Alternatively, check if the data has a date column with an unrecognized name — if so, add a normalization rule in Phase 4.

### BigQuery Upload Fails

**Symptom**: `"ERROR writing to BigQuery"` in console output.

**Possible causes**:
- Authentication expired — run `bigrquery::bq_auth()` to refresh
- Schema mismatch — the table schema changed (new columns or type changes). The `WRITE_TRUNCATE` mode recreates the table, but the preceding `bq_table_delete` might fail on permissions
- Network timeout — retry the run

## Possible Improvements

- **Expand CLI args coverage** — `pattern` is now overridable with `--pattern`, but `gdrive_folder_id`, `current_phase`, and `use_saved_phases` still require editing the script. These could be expanded with `commandArgs()` or an `optparse` setup.
- **Week-start config externalization** — week fill behavior is currently hardcoded in `client_week_config` (`mass/oli/adif=Sun`, `apollo=Mon`). Move this to a config file so new clients do not require script edits.
- **Parallel sheet processing** — Phases 2, 3, and 5 loop through sheets sequentially. Each iteration is an API call. `furrr::future_map()` or `parallel::mclapply()` could speed this up.
- **Drive-side change detection** — the BigQuery sync is now incremental for the sheets included in a run, but the pipeline still scans and processes every sheet matched by the current `pattern`. A later optimization could skip unchanged sheets earlier by comparing Drive modification timestamps before Phase 2.
- **Weighted daily expansion** — Phase 7 divides metrics evenly across days. Some metrics (e.g., TV ad spend) may follow day-of-week patterns. A weighted split (heavier on weekdays, lighter on weekends) could improve accuracy.
- **Unit tests** — no test suite exists. Key functions to test: date parsing (`parse_any_date`), column normalization (Phase 4 rules), and daily expansion math (Phase 7).
- **Config file** — move `known_kpi_metrics`, `gdrive_folder_id`, and other settings to a YAML or JSON config file so the script doesn't need to be edited directly.
- **TODO: Create a Codex skill for input standardization** — package the Excel-to-standard-input conversion workflow (header detection, weekly row extraction, canonical column mapping, and output validation) into a reusable skill for manual partner report ingestion.
