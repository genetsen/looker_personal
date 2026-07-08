# Manual Editor Benchmark Preservation Continuation - 2026-07-08

## Learned

- The benchmark metadata request is: editable `Benchmark KPI` and `Benchmark Value` columns should map to `man_benchmark_kpi` and `man_benchmark_value`.
- Final master fields should be `_benchmark_kpi` and `_benchmark_value`, with manual benchmark values first and FPD benchmark values next.
- `Benchmark KPI` is text/string. `Benchmark Value` is numeric.
- Live FPD currently provides `benchmark` as the usable numeric value and `benchmark_metric` as a numeric field that is usually zero, so zero FPD metric labels should not become visible KPI strings.

## Changed So Far

- Added benchmark columns through the Manual Data Editor loader, raw and daily tables, lookup SQL, master model SQL, sheet setup/repair helpers, tests, README files, model map, and changelogs.
- Deployed new benchmark fields into the master evidence model and mart, refreshed clustered and v3 dependents, and verified field presence before the loader preservation issue appeared.
- Found a loader regression where two refresh runs reduced daily manual rows from 785 rows across 15 package IDs to 29 rows for only `ccdooh`.
- Restored raw and daily manual tables from pre-loader backups after each failed run.

## Current Focus

- The likely issue is that prior manual rows were keyed by package plus visible delivery dates, so if the refreshed editor row lost visible dates, prior trusted manual evidence was not found and valid manual-only rows were treated as blank/new.
- A broader preservation helper now reconstructs trusted previous raw manual rows into editor rows before row-by-row comparison.
- Next proof steps: run focused tests, parse the loader, verify backup-restored warehouse state, rerun the loader, compare daily output to backup, and refresh dependent tables only after the loader preserves all package IDs.
