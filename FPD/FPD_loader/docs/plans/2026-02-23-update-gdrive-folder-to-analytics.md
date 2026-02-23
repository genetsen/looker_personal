# Update FPD Loader to Ingest from Analytics Department Folder

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Update `util_collect_fpd_shortcutsFolder.r` to discover and ingest FPD partner sheets from the Analytics department folder (`1d--Bc554eBaRCr8blt1LnUYiOMHQe7jF`) instead of the current shortcuts folder (`1cMkgbplZ8sPIsDHluIuIEOmbam-BhZtY`), writing checkpoints to the local `output/` directory and uploading to a **new** BigQuery table so the older pipeline's data is preserved.

**Architecture:** Three configuration changes: (1) swap the Google Drive folder ID in Phase 1 discovery, (2) fix the `output_dir` path to point at the local `FPD/FPD_loader/output` directory, and (3) change the BigQuery output table name so this pipeline doesn't overwrite data from the older `util_collect_fpd_v3.r` script. The pipeline's 7-phase structure, column normalization, date coalescing, and daily expansion all remain unchanged.

**Tech Stack:** R, googledrive, googlesheets4, bigrquery

---

### Task 1: Update Google Drive folder and output directory configuration

**Files:**
- Modify: `util_collect_fpd_shortcutsFolder.r:28-33`

**Step 1: Update the config comment and folder ID (lines 28-31)**

Change lines 28-31 from:

```r
  # fpd shortcuts folder:
  # https://drive.google.com/drive/folders/1cMkgbplZ8sPIsDHluIuIEOmbam-BhZtY?usp=drive_link

gdrive_folder_id <- "1cMkgbplZ8sPIsDHluIuIEOmbam-BhZtY"
```

To:

```r
  # Analytics department folder (recursive scan, filtered by pattern):
  # https://drive.google.com/drive/folders/1d--Bc554eBaRCr8blt1LnUYiOMHQe7jF

gdrive_folder_id <- "1d--Bc554eBaRCr8blt1LnUYiOMHQe7jF"
```

**Step 2: Update output_dir to local path (line 33)**

Change line 33 from:

```r
output_dir <- "/Users/eugenetsenter/Looker_clonedRepo/looker_personal/util/data_loaders/FPD_loader/output"
```

To:

```r
output_dir <- "/Users/eugenetsenter/Looker_clonedRepo/looker_personal/FPD/FPD_loader/output"
```

**Step 3: Verify both edits**

Run: `grep -n "gdrive_folder_id\|output_dir\|1d--Bc554eBaRCr8blt1LnUYiOMHQe7jF\|1cMkgbplZ8sPIsDHluIuIEOmbam\|util/data_loaders" util_collect_fpd_shortcutsFolder.r`

Expected:
- Line 29: new URL with `1d--Bc554eBaRCr8blt1LnUYiOMHQe7jF`
- Line 31: `gdrive_folder_id <- "1d--Bc554eBaRCr8blt1LnUYiOMHQe7jF"`
- Line 33: `output_dir <- ".../FPD/FPD_loader/output"`
- No remaining references to old folder ID `1cMkgbplZ8sPIsDHluIuIEOmbam-BhZtY`
- No remaining references to `util/data_loaders/FPD_loader/output`

---

### Task 2: Change BigQuery output table to avoid overwriting

**Context:** Both `util_collect_fpd_v3.r` and `util_collect_fpd_shortcutsFolder.r` currently write to the same table (`landing.fpd_data_ranged`) with `WRITE_TRUNCATE`. Running either one destroys the other's output. This task gives the shortcutsFolder variant its own table.

**Files:**
- Modify: `util_collect_fpd_shortcutsFolder.r:1474` (delete old table reference)
- Modify: `util_collect_fpd_shortcutsFolder.r:1504` (write_to_bq call)

**Step 1: Update the table deletion reference (line 1474)**

Change:

```r
    bq_table <- bq_table(project = "looker-studio-pro-452620", dataset = "landing", table = "fpd_data_ranged")
```

To:

```r
    bq_table <- bq_table(project = "looker-studio-pro-452620", dataset = "landing", table = "adif_fpd_data_ranged")
```

**Step 2: Update the write_to_bq call (line 1504)**

Change:

```r
  write_to_bq(phase7_df, "landing", "fpd_data_ranged")
```

To:

```r
  write_to_bq(phase7_df, "landing", "adif_fpd_data_ranged")
```

**Step 3: Verify no stale references remain**

Run: `grep -n "fpd_data_ranged" util_collect_fpd_shortcutsFolder.r`

Expected: Only references to `adif_fpd_data_ranged` — no bare `fpd_data_ranged`.

**Step 4: Commit all config changes together**

```bash
git add util_collect_fpd_shortcutsFolder.r
git commit -m "feat: switch to Analytics folder, local output, new BQ table

- Point gdrive_folder_id at Analytics department folder (1d--Bc5...)
  instead of shortcuts folder (1cMkgbp...)
- Fix output_dir to FPD/FPD_loader/output (was util/data_loaders/...)
- Write to landing.adif_fpd_data_ranged instead of landing.fpd_data_ranged
  to avoid overwriting data from the older v3 pipeline"
```

---

### Task 3: Smoke-test Phase 1 discovery with new folder

**Purpose:** Verify the new folder ID resolves correctly, the pattern filter returns the expected sheets, and checkpoints land in the correct output directory.

**Step 1: Configure for Phase 1 only**

Temporarily set in the script:
```r
use_saved_phases <- TRUE
current_phase <- 1
```

**Step 2: Run the script**

Run: `Rscript util_collect_fpd_shortcutsFolder.r`

**Step 3: Inspect Phase 1 output at the NEW path**

Open `/Users/eugenetsenter/Looker_clonedRepo/looker_personal/FPD/FPD_loader/output/phase1_discovered_files.csv` and verify:
- The file exists at the correct location (not in `util/data_loaders/...`)
- The expected "De Beers | Partner Data" sheets are discovered
- No unexpected sheets snuck past the pattern filter
- Sheet count is reasonable (compare with previous Phase 1 output — was ~20 sheets before)

**Expected output:** Similar set of sheets as before, possibly with additional partner data sheets that were not shortcutted. All should match the `"De Beers | Partner Data"` pattern and none should contain "ARCHIVE" in the name.

---

### Task 4: Run full pipeline and validate

**Purpose:** Ensure the complete 7-phase pipeline succeeds end-to-end with the new folder, local output directory, and new BigQuery table.

**Step 1: Reset to full pipeline mode**

Set in script:
```r
use_saved_phases <- FALSE
current_phase <- 1
```

**Step 2: Run full pipeline**

Run: `Rscript util_collect_fpd_shortcutsFolder.r`

**Step 3: Check Phase 7 validation**

- Inspect console output for `"✓ All per-sheet KPI metric totals match between Phase 5 and Phase 7"`
- Check `output/phase7_validation_table.csv` — no mismatches expected
- Verify `output/phase7_daily_master_data.csv` was generated in `FPD/FPD_loader/output/`

**Step 4: Verify BigQuery upload to NEW table**

Check that `landing.adif_fpd_data_ranged` was created:
```sql
SELECT COUNT(*) as row_count, MIN(date_final) as min_date, MAX(date_final) as max_date
FROM `looker-studio-pro-452620.landing.adif_fpd_data_ranged`
```

**Step 5: Confirm OLD table is untouched**

Verify that `landing.fpd_data_ranged` still has its original data:
```sql
SELECT COUNT(*) as row_count, MIN(date_final) as min_date, MAX(date_final) as max_date
FROM `looker-studio-pro-452620.landing.fpd_data_ranged`
```

---

## Risk Notes

| Risk | Mitigation |
|------|-----------|
| Broader folder = more sheets discovered | Pattern filter `"De Beers \| Partner Data"` + ARCHIVE exclusion keeps results narrow |
| Recursive scan takes longer | One-time cost at Phase 1; checkpoint system means it only runs once |
| New sheets have unfamiliar column schemas | Phase 4 normalization handles this; inspect `phase4_normalization_mapping.csv` for new `[UNMAPPED]` entries |
| Old shortcuts folder had curated subset | Compare Phase 1 outputs before/after to identify any new/missing sheets |
| Table name collision | Resolved: new table `adif_fpd_data_ranged` isolates from old `fpd_data_ranged` |
| Stale checkpoints in old output_dir | Old CSVs at `util/data_loaders/.../output/` remain untouched; new runs write to `FPD/FPD_loader/output/` |

## What Changes

| Config | Before | After |
|--------|--------|-------|
| `gdrive_folder_id` | `"1cMkgbplZ8sPIsDHluIuIEOmbam-BhZtY"` | `"1d--Bc554eBaRCr8blt1LnUYiOMHQe7jF"` |
| `output_dir` | `.../util/data_loaders/FPD_loader/output` | `.../FPD/FPD_loader/output` |
| BQ table | `landing.fpd_data_ranged` | `landing.adif_fpd_data_ranged` |

## What Stays the Same

- Pattern filter: `"De Beers | Partner Data"`
- Column normalization rules (Phase 4)
- Date coalescing logic (Phase 6)
- Daily expansion (Phase 7)
- BigQuery project/dataset: `looker-studio-pro-452620.landing`

## Downstream Impact

Any dashboards, Looker views, or SQL queries referencing `landing.fpd_data_ranged` will **not** be affected. Downstream consumers that should read from this pipeline's output will need to be updated to reference `landing.adif_fpd_data_ranged`. Check:
- `util_process_updated_fpd.r` (currently `source()`'d at end — commented out on line 1534)
- Any Looker/Omni views in `omni/bigquery_connection_v2/`
- BigQuery scheduled queries that reference `fpd_data_ranged`
