# SQL Change Guard Report Format

## Default output (approval gate)

Without `--show-details` or `--show-comparisons`, output is intentionally short:

- `RESULT: PASS|FAIL`
- `FAILED_CHECKS: <n>`
- `COMPARISON_ROWS: <n>`
- `QUERY_BACKEND: mcp|bq`
- `BASELINE_SOURCE: <mode>`
- artifact file paths:
  - `SUMMARY_AVAILABLE`
  - `DETAILS_AVAILABLE`
  - `COMPARISONS_AVAILABLE`
- next-step prompt lines:
  - `Show comparisons? Re-run with --show-comparisons.`
  - `Show details? Re-run with --show-details.`

This is the default bite-sized approval chunk.

`QUERY_BACKEND` values:
- `mcp` (default)
- `bq` (fallback)

`BASELINE_SOURCE` values:
- `baseline_live_table`
- `inferred_live_table_from_baseline_script`
- `baseline_query_file`
- `baseline_query_file_fallback`
- `baseline_table_no_check`

## Expanded output options

- `--show-comparisons`
  - Prints at-a-glance comparison rows from baseline vs candidate checks.
  - Row count shown is limited by `--comparison-limit` (default `20`).
- `--show-details`
  - Prints failing check summaries only.
  - Count is limited by `--detail-limit` (default `15`).
- `--prompt-details`
  - After summary output, asks: `Show failing check details now? [y/N/more]:`
  - Details are printed only if approved.

## Artifacts

- `summary.json`
  - Top-level pass/fail result, counts, lineage snapshot, high-risk fields, QA table names.
- `details.json`
  - Full check records, failed checks, baseline/candidate schemas, comparison rows.
- `comparisons.json`
  - Comparison-only rows.
- `comparisons.csv`
  - Spreadsheet-friendly baseline-vs-candidate summary.
