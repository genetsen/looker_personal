# FPD Loader Project Overview

This document is a practical map of this folder so you can quickly understand what to run, what each file does, and where outputs land.

## What This Folder Does

This folder contains two R data pipelines:

1. `util_collect_fpd_shortcutsFolder.r` (main pipeline)
2. `manually_updated_data_loader.r` (manual corrections pipeline)

Both pipelines read data from Google Sheets, transform it, and upload results to BigQuery.

## Folder Map

| Path | What it is |
|---|---|
| `util_collect_fpd_shortcutsFolder.r` | Main 7-phase ingestion and normalization pipeline |
| `manually_updated_data_loader.r` | Loader for manually corrected package-level FPD |
| `README.md` | Full operating guide, phase details, troubleshooting |
| `output/` | Main pipeline checkpoint files (`phase1...phase7`) |
| `docs/plans/` | Historical implementation plans and notes |

## Main Pipeline Flow (`util_collect_fpd_shortcutsFolder.r`)

The script runs these phases in order:

1. Discover matching sheets (including Drive shortcuts to sheets) in Google Drive
2. Detect header row in each sheet
3. Collect and profile raw column names
4. Build raw-to-normalized column mapping
5. Ingest all sheets into one combined table
6. Clean data and finalize date ranges
7. Expand ranged rows into daily rows

After Phase 7, it:

1. Builds validation output (`phase7_validation_table.csv`)
2. Uploads to BigQuery table `landing.fpd_data_ranged_shortcutsFolder`

## Manual Corrections Flow (`manually_updated_data_loader.r`)

This script is separate from the 7-phase flow:

1. Reads one curated Google Sheet tab
2. Pulls package date ranges from Prisma in BigQuery
3. Joins package totals to date ranges
4. Spreads totals across daily dates
5. Uploads to BigQuery table `landing.manually_updated_fpd_daily`

## Common Run Commands

```bash
# Main 7-phase pipeline
Rscript util_collect_fpd_shortcutsFolder.r

# Manual corrections pipeline
Rscript manually_updated_data_loader.r
```

## Output Files You Will Use Most

| File | Why it matters |
|---|---|
| `output/phase1_discovered_files.csv` | Confirms which sheets were discovered |
| `output/phase4_normalization_mapping.csv` | Shows how raw columns were mapped |
| `output/phase6_1_sheet_summary.csv` | Sheet-level quality and metric summary |
| `output/phase6_filter_audit.csv` | Shows what filters removed and why |
| `output/phase7_daily_master_data.csv` | Final daily output uploaded to BigQuery |
| `output/phase7_validation_table.csv` | Reconciliation between Phase 5 and Phase 7 |

## Important Configuration Knobs

In `util_collect_fpd_shortcutsFolder.r`, these settings control most behavior:

- `gdrive_folder_id`
- `pattern`
- `output_dir`
- `use_saved_phases`
- `current_phase`
- `known_kpi_metrics`
- `client_week_config`

In `manually_updated_data_loader.r`, these are most important:

- `sheet_id`
- `target_gid`
- `output_dir`
- `bq_project`, `bq_dataset`, `bq_table`

## Practical Tip

When debugging one phase, set:

```r
use_saved_phases <- TRUE
current_phase <- <phase_number>
```

This lets you rerun only one phase and reuse saved checkpoints from the others.
