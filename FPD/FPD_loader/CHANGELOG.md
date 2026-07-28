## 2026-07-28

- **FIXED** - Preserved established numeric BigQuery types when a current FPD run contains an entirely blank metric column, so `video_watch_time` no longer becomes text and blocks the production sync. The canonical loader completed a live refresh after the repair.

### Pending Next Actions

- The latest source output still contains rows with missing package IDs and exact duplicate rows; those are source-quality conditions present before the warehouse sync, not a result of this type-preservation repair.

## 2026-07-10

- **CHANGED** - Added a local entry point to the partner-data Google Sheets template guide, so FPD loader users can reach the canonical master-data-model documentation without maintaining a duplicate copy.

### Pending Next Actions

- None currently tracked.

## 2026-07-09

- **FIXED** - Let the main loader continue when a matched sheet adds a brand-new column that is completely blank, while still avoiding a guessed production BigQuery type; a full live run completed the production sync and confirmed NATVLY rows reached the output.

### Pending Next Actions

- None currently tracked.

## 2026-06-24

- **FIXED** - Prevented mixed text and date values in partner placement columns from stopping the multi-sheet combine step.

## 2026-05-08

### Fixed

- **APO creative fallback source column -codexapp (threadID unavailable in local session)**
  Issue: The in-pipeline APO creative refresh skipped WSJ rows where `Final_img_path` was blank even though `Creative img PATH` contained the local creative file path.
  Resolution: Updated `util_collect_fpd_shortcutsFolder.r` so APO refresh detection and row processing use `Creative img PATH` when `Final_img_path` is blank, preserving the existing GitHub upload and sheet write-back flow.

## 2026-05-04

### Fixed

- **Per-sheet cache payload invalidation -codexapp (threadID unavailable in local session)**
  Issue: A changed Google Sheet could refresh only `header_row` or `raw_headers` in the `.rds` cache while preserving older `raw_data`, then mark the whole cache object with the new `last_modified_time`.
  Cause: `write_sheet_cache()` used one shared cache timestamp for mixed fields and did not clear stale sibling fields when the timestamp changed.
  Resolution: Added a cache schema version and reset cached payload fields whenever the cache schema or sheet modified time changes before writing the refreshed field.

## 2026-04-17

### Changed

- **R-native APO creative refresh before BigQuery sync -codexapp (threadID unavailable in local session)**
  What: Replaced the temporary post-sync APO creative handoff with an R-native Phase 5 step in `util_collect_fpd_shortcutsFolder.r` that uploads missing APO creative assets, writes `creative_git_link` plus tracking columns back into the sheet, re-reads the updated APO data, and then continues the normal BigQuery sync in the same run.
  Why: Keeps Google Sheets and BigQuery current together in one pipeline pass, without a second sync or Python sidecar flow.
  <details><summary>Paths — R-native APO creative refresh before BigQuery sync</summary>

  [util_collect_fpd_shortcutsFolder.r](util_collect_fpd_shortcutsFolder.r)
  [README.md](README.md)

  </details>

## 2026-04-09

### Fixed

- **Checkpoint CSV numeric type preservation -codexapp (threadID unavailable in local session)**
  Issue: The main shortcut-aware loader could reread sparse rate columns like `ctr_vcr` from checkpoint CSVs as logical blanks, which later caused a staging/prod BigQuery schema mismatch and blocked the prod sync.
  Cause: `read_csv()` inferred mostly-empty rate columns too loosely during the Phase 5 to Phase 6 checkpoint round-trip, and the later upload logic then wrote the affected column as text.
  Resolution: Added an explicit checkpoint-read numeric coercion safeguard for KPI/rate fields including `ctr` and `ctr_vcr`, then verified the loader completed and updated the prod BigQuery table.

## 2026-03-11

### Changed

- **Per-sheet cache reuse for unchanged Google Sheets -codexapp (threadID unavailable in local session)**
  What: Added per-sheet header and raw-data cache files to `util_collect_fpd_shortcutsFolder.r`, turned cache reuse on by default for unchanged Drive files, and added a `--no-file-cache` escape hatch for forced fresh pulls.
  Why: Reduces repeated Google Sheets API calls on unchanged partner files while keeping a simple way to force a full refresh when needed.
  <details><summary>Paths — Per-sheet cache reuse for unchanged Google Sheets</summary>

  [util_collect_fpd_shortcutsFolder.r](util_collect_fpd_shortcutsFolder.r)
  [README.md](README.md)

  </details>

- **Incremental BigQuery sync for sheet-scoped runs -codexapp (threadID unavailable in local session)**
  What: Updated `util_collect_fpd_shortcutsFolder.r` so the main loader stages the current run, adds any clearly-typed new columns to the live BigQuery table, and then replaces only the destination rows for the sheets included in that run instead of rebuilding the whole table every time.
  Why: Makes focused runs like Apollo-only updates safer while still preserving newly added A:Y sheet columns in BigQuery.
  <details><summary>Paths — Incremental BigQuery sync for sheet-scoped runs</summary>

  [util_collect_fpd_shortcutsFolder.r](util_collect_fpd_shortcutsFolder.r)
  [README.md](README.md)

  </details>

- **Fresh-run default restored for the main shortcut-aware loader**
  What: Reset `util_collect_fpd_shortcutsFolder.r` to start from Phase 1 by default, reserved saved-phase reuse for debugging only, and added a short rollback note to the README.
  Why: Prevents accidental uploads based on stale checkpoint files while keeping the debugging path explicit.
  <details><summary>Paths — Fresh-run default restored for the main shortcut-aware loader</summary>

  [util_collect_fpd_shortcutsFolder.r](util_collect_fpd_shortcutsFolder.r)
  [README.md](README.md)

  </details>

## 2026-02-23

### Added

- **Project overview guide**
  What: Added a new high-level project map document that explains folder structure, both pipelines, run flow, key outputs, and primary configuration knobs.
  Why: Makes onboarding easier for beginners by providing a fast orientation before diving into script-level details.
  <details><summary>Paths — Project overview guide</summary>

  [docs/PROJECT_OVERVIEW.md](docs/PROJECT_OVERVIEW.md)

  </details>

### Changed

- **Runtime pattern override flag -codexapp (threadID unavailable in local session)**
  What: Added an optional `--pattern` command-line override to `util_collect_fpd_shortcutsFolder.r`, so the Phase 1 sheet-name discovery pattern can be set at run time without editing the script.
  Why: Makes one-off pattern changes safer and faster for day-to-day runs.
  <details><summary>Paths — Runtime pattern override flag</summary>

  [util_collect_fpd_shortcutsFolder.r](util_collect_fpd_shortcutsFolder.r)
  [README.md](README.md)

  </details>

- **Shortcut-aware sheet discovery**
  What: Updated Phase 1 discovery to include both direct spreadsheet files and Google Drive shortcuts that point to spreadsheets, then resolve shortcut target IDs before downstream reads.
  Why: Makes the main pipeline work in folders where partner data is shared via shortcuts instead of direct sheet files.
  <details><summary>Paths — Shortcut-aware sheet discovery</summary>

  [util_collect_fpd_shortcutsFolder.r](util_collect_fpd_shortcutsFolder.r)
  [README.md](README.md)
  [docs/PROJECT_OVERVIEW.md](docs/PROJECT_OVERVIEW.md)

  </details>

- **Main pipeline target table and discovery pattern**
  What: Updated the main loader to write to `landing.fpd_data_ranged_shortcutsFolder` and changed the discovery pattern default to `| Partner Data` for broader partner-sheet matching by naming convention.
  Why: Ensures this pipeline writes to the exact BigQuery destination requested and discovers sheets using the updated pattern rule.
  <details><summary>Paths — Main pipeline target table and discovery pattern</summary>

  [util_collect_fpd_shortcutsFolder.r](util_collect_fpd_shortcutsFolder.r)
  [README.md](README.md)
  [docs/PROJECT_OVERVIEW.md](docs/PROJECT_OVERVIEW.md)

  </details>

- **README alignment with current scripts**
  What: Updated the README so commands, script names, defaults, and behavior reflect the active files in this folder (main entry script, folder ID, output paths, week config, and downstream-call status).
  Why: Prevents run mistakes caused by stale references and keeps operational docs accurate.
  <details><summary>Paths — README alignment with current scripts</summary>

  [README.md](README.md)

  </details>

- **Safe run checklist for beginners**
  What: Added a step-by-step pre-run and post-run checklist section to the README covering script selection, output checks, and troubleshooting checkpoints.
  Why: Reduces avoidable run errors and gives a repeatable safety routine for daily use.
  <details><summary>Paths — Safe run checklist for beginners</summary>

  [README.md](README.md)

  </details>

### Fixed

- **Phase 5 summary runtime crash**
  Issue: The main pipeline halted in the "Summary by Sheet" block before Phase 6/7 and upload.
  Cause: Fragile inline package-key assignment in summary logic and use of `view(...)` in non-interactive execution.
  Resolution: Replaced package-key assignments with explicit conditional blocks and switched to `print(...)`; verified by a successful end-to-end pipeline run including BigQuery upload.
