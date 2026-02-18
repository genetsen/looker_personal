---
name: sql-change-guard
description: Validate BigQuery SQL script changes before deployment by comparing baseline vs candidate outputs, tracing two-level lineage, checking schema/key/metric/derived-metric integrity, running custom assertions, and reporting pass/fail first with optional details. Use when adding or changing SQL scripts that feed downstream models, views, or reporting tables.
---

# SQL Change Guard

## Overview

Run baseline-vs-candidate SQL validation with hard proof checks, downstream compatibility checks, custom manifest-driven checks, and staged reporting.
Default query backend is MCP (`--query-backend mcp`), with optional direct BigQuery CLI fallback (`--query-backend bq`).

## Execute Workflow

1. Determine input mode.
- Use "changed script mode" when validating updates to an existing script.
- Use "new script mode" when no baseline script exists and upstream parity is required.

2. Collect required inputs.
- Candidate source: `--candidate-query-file` (single SELECT query only) or `--candidate-table`.
- Baseline source: `--baseline-query-file` or `--baseline-table` for changed script mode.
- Manifest file: use `assets/intended_change_manifest.template.json` as the starting point.
- Default behavior checks for an existing baseline live table first and uses it when available.
- Default query backend uses MCP server `bigquery`; override with `--mcp-server`.

3. Determine analysis window.
- Use 3/6/9/12 month auto-selection when `date_column` exists.
- Default behavior excludes the most recent 5 days (`--exclude-recent-days 5`) to avoid unstable late-arriving rows.
- In changed mode, baseline and candidate are forced to the same shared end date: `CURRENT_DATE() - exclude_recent_days`.
- Fall back to full available range only when date filtering is impossible.

4. Trace lineage before comparisons.
- Capture upstream level 1 from candidate SQL table references.
- Attempt upstream level 2 by reading view SQL text from level-1 objects.
- Capture downstream from manifest declarations (`downstream_checks` and `downstream_objects`).

5. Identify high-risk checks.
- Use manifest-provided `high_risk_metrics` and `high_risk_dimensions`.
- If missing, infer from derived metrics and key columns.

6. Run comparisons in isolated QA tables.
- Materialize baseline and candidate into QA dataset tables.
- Run row count, duplicate-key, key-overlap, metric totals, derived metrics, dimension-cardinality, schema/type checks, downstream checks, and custom checks.

7. Handle script-shaped SQL safely.
- Reject `--candidate-query-file` when it is a multi-statement script.
- Materialize candidate output first, then pass `--candidate-table`.
- If baseline query input is a script, infer its target table and reuse that live table when present.

8. Validate downstream compatibility.
- Run downstream check SQL against both baseline and candidate QA tables.
- Fail if schema, row-count expectations, or custom thresholds are violated.

9. Return output in chunks.
- First response: pass/fail plus failure count only.
- Comparisons are written to `comparisons.csv` and shown only when requested (`--show-comparisons`).
- Failed-check detail is shown only when requested (`--show-details`) or prompted (`--prompt-details`).

## Run Commands

```bash
python3 skills/sql-change-guard/scripts/run_sql_change_guard.py \
  --project looker-studio-pro-452620 \
  --qa-dataset repo_stg \
  --query-backend mcp \
  --candidate-table looker-studio-pro-452620.repo_stg.adif__prisma_expanded_plus_dcm_with_social_tbl \
  --baseline-table looker-studio-pro-452620.stg.adif__prisma_expanded_plus_dcm_with_social_tbl \
  --date-column date \
  --exclude-recent-days 5 \
  --manifest skills/sql-change-guard/assets/intended_change_manifest.template.json \
  --output-dir /tmp/sql-change-guard-report
```

```bash
python3 skills/sql-change-guard/scripts/run_sql_change_guard.py \
  --project looker-studio-pro-452620 \
  --qa-dataset repo_stg \
  --query-backend mcp \
  --candidate-table project.dataset.materialized_candidate_output \
  --manifest path/to/manifest.json \
  --mode new \
  --show-comparisons
```

```bash
python3 skills/sql-change-guard/scripts/run_sql_change_guard.py \
  --project looker-studio-pro-452620 \
  --qa-dataset repo_stg \
  --query-backend mcp \
  --candidate-query-file path/to/updated_script.sql \
  --baseline-query-file path/to/original_script.sql \
  --manifest path/to/manifest.json \
  --output-dir /tmp/sql-change-guard-report \
  --prompt-details
```

```bash
python3 skills/sql-change-guard/scripts/run_sql_change_guard.py \
  --project looker-studio-pro-452620 \
  --qa-dataset repo_stg \
  --query-backend mcp \
  --candidate-query-file path/to/updated_script.sql \
  --baseline-query-file path/to/original_script.sql \
  --manifest path/to/manifest.json \
  --output-dir /tmp/sql-change-guard-report \
  --show-details
```

## Enforce Output Contract

- Print only:
  - `RESULT: PASS` or `RESULT: FAIL`
  - `FAILED_CHECKS: <n>`
  - `COMPARISON_ROWS: <n>`
  - `QUERY_BACKEND: <mode>`
  - `BASELINE_SOURCE: <mode>`
  - `DETAILS_AVAILABLE: <path>`
  - `COMPARISONS_AVAILABLE: <path>`
- Do not print detailed check tables unless `--show-details` or `--show-comparisons` is set.
- If `--prompt-details` is set, ask for approval before printing failed check details.

## Use References

- Read `references/check_catalog.md` for built-in check semantics.
- Read `references/report_format.md` for chunked output behavior and approval flow.
- Start from `assets/intended_change_manifest.template.json` and edit it to add custom validation logic.
