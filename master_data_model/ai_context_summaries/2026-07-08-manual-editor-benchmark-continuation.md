# 2026-07-08 Manual Editor Benchmark Continuation

This summary records the active context after automatic compaction during the Manual Data Editor benchmark-field fix.

## Current User Request

Add `Benchmark KPI` and `Benchmark Value` as editable Manual Data Editor metadata columns.

The required field contract is:

- `Benchmark KPI` writes to `man_benchmark_kpi` as text.
- `Benchmark Value` writes to `man_benchmark_value` as numeric data.
- New final `_benchmark_*` fields prioritize manual values first.
- FPD benchmark values are the next fallback source.

## Evidence Gathered

- Live FPD source `landing.fpd_data_ranged_shortcutsFolder` has `benchmark` as numeric and `benchmark_metric` as numeric.
- `benchmark_metric` is currently populated only with zero values, so it is not a reliable visible KPI label as-is.
- `benchmark` is the usable numeric fallback for `_benchmark_value`.
- Existing local master SQL matched the live view once wrapper comments and the final semicolon were normalized, so local edits are being made against a production-equivalent base.

## Implementation State

- Canonical loader fields, hidden baseline columns, and hidden manual-marker columns were extended for benchmark KPI/value.
- Raw and daily manual landing schemas were extended with `man_benchmark_kpi` and `man_benchmark_value`.
- Master SQL now carries manual benchmark evidence and emits final `_benchmark_kpi` and `_benchmark_value` fields.
- Sheet repair/setup helpers were extended for the new columns, but setup formatting rebuild remains guarded and must not be run against the live sheet unless explicitly requested.

## Remaining Work

- Update README/changelog/model-map documentation.
- Run syntax checks and loader choice tests.
- Apply live schema changes, deploy the master SQL, refresh dependent tables, refresh lookup, run the loader, and verify live schema/results.
