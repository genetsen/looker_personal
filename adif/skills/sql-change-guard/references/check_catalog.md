# SQL Change Guard Check Catalog

## Built-in comparisons (changed mode)

- `row_count`
  - Baseline vs candidate row counts with optional tolerance from `allowed_deltas.row_count`.
- `duplicate_keys_baseline` and `duplicate_keys_candidate`
  - Detect duplicate key groups using `keys`.
- `key_overlap`
  - Compares distinct key sets across baseline/candidate.
  - Uses `key_overlap_tolerance.max_baseline_only` and `key_overlap_tolerance.max_candidate_only`.
- `metric_sum_<metric>`
  - Compares metric sums listed in `metrics`.
  - Uses per-metric tolerances from `allowed_deltas`.
- `derived_metric_<name>`
  - Compares derived metric values listed in `derived_metrics`.
  - Uses per-derived tolerances from `allowed_deltas`.
- `dimension_distinct_<dimension>`
  - Compares distinct-count cardinality for dimensions in `comparison_dimensions` (or `high_risk_dimensions` fallback).
  - Uses tolerance key `allowed_deltas.distinct_<dimension>`.
- `schema_compatibility`
  - Fails if baseline columns are removed or changed (`name/type/nullability`).
- `downstream_<name>`
  - Runs downstream checks defined in `downstream_checks`.

## Input guards

- Query backend:
  - `--query-backend mcp` (default) routes SQL execution via BigQuery MCP.
  - `--query-backend bq` uses direct `bq query` CLI.
- `--candidate-query-file` must be a single SELECT query.
  - Multi-statement scripts are rejected to avoid invalid or accidental heavy execution.
  - Materialize script output first, then run with `--candidate-table`.
- For baseline scripts, runner attempts to infer `CREATE TABLE` target and reuse live table first.

## Built-in checks (new mode)

- `duplicate_keys_candidate`
- `custom_<name>`

For new scripts, use `custom_checks` to compare candidate output to upstream raw/staging tables.

## Custom checks

### 1. Single-table assertion

Evaluate scalar result from SQL against a rule.

Required:
- `name`
- `sql` (can use `{table}`, `{candidate_table}`, `{baseline_table}`)
- one of:
  - `expected_max`
  - `expected_min`
  - `equals` (+ optional `equals_tolerance`)
  - `expectation` object `{ "op": "...", "value": ... }`

Optional:
- `scope`: `candidate` (default) or `baseline`
- `severity`

### 2. Baseline-vs-candidate scalar comparison

Compare scalar result between baseline and candidate.

Required:
- `name`
- either:
  - `sql` + `compare_to_baseline: true`
  - or `baseline_sql` + `candidate_sql`

Optional:
- `max_abs_delta`
- `max_abs_pct`
- `severity`

If no tolerance is provided, exact match is enforced (`max_abs_delta = 0`).

## Downstream checks

### Rowset mode (default)

Required:
- `name`
- one of:
  - `sql`
  - `sql_file`

Optional:
- `max_row_count_delta` (default `0`)
- `severity`

Validation:
- row counts within tolerance
- output schema (column names/order) matches baseline version

### Scalar mode

Required:
- `name`
- `mode: "scalar"`
- either:
  - `sql`
  - or `baseline_sql` + `candidate_sql`

Optional:
- `max_abs_delta`
- `max_abs_pct`
- `severity`
