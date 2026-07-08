# 2026-07-08 Manual Editor Refresh And Benchmark Context

## What was learned

- The Manual Data Editor production loader is the canonical `model/manual_editor/load_manual_package_edits.R`; the root `manual_package_edits/load_manual_package_edits.R` is now a launcher.
- The live production raw and daily tables are in the `landing` dataset, not `master_stg`.
- The Purely Elizabeth / QUAN `ccdooh` row was harmed by a refresh loop that treated manual-applied model data as the current baseline and by daily publish logic that only recognized replacement metric columns.
- The repaired loader now reads the existing editor tab as text, writes visible dates back as ISO text, ignores manual-applied rows in the lookup baseline, preserves user-owned manual values, and publishes metric, metadata, or flight-date manual evidence.

## What changed before this compaction

- Repaired and tested the canonical loader and compatibility launcher.
- Updated the source-baseline lookup SQL in both canonical and compatibility locations.
- Repaired the live `ccdooh` sheet row dates and reran the production wrapper.
- Verified `ccdooh` in raw, daily, `master_stg.data_model`, and the clustered advertiser QA table.
- Refreshed dependent clustered and v3 tables; clustered proof passed, while the v3 wrapper ended with a duplicate visible-key QA failure after rebuilding.
- Updated the Manual Data Editor README files and master/model changelogs for the refresh-safety fix.

## Current request

Add two editable metadata columns:

- `Benchmark KPI`: writes `man_benchmark_kpi` as a string.
- `Benchmark Value`: writes `man_benchmark_value` as a numeric value.

Final model fields should be new `_benchmark_*` outputs that prioritize manual benchmark fields first, then FPD benchmark fields.

## Still needs attention

- Inspect live `fpd_benchmark_*` field types and values before choosing the fallback expressions.
- Patch loader schemas, visible columns, marker columns, raw/daily schemas, lookup SQL, and final model SQL.
- Update docs/changelogs for the benchmark feature.
- Run local tests and BigQuery dry runs before any live deployment.
- If deployed, verify schema and one end-to-end benchmark edit path without damaging existing user edits.
