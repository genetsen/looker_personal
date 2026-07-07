# AGENTS.md

Project-local instructions for `master_data_model`.

## Global BigQuery And Modeling Rules

Apply the global [BigQuery, SQL, and data-modeling rules](/Users/eugenetsenter/.codex/BIGQUERY_SQL_DATA_MODELING_RULES.md) before any master-model warehouse, SQL, or modeling work. This file only adds master-model-specific overlays.

## Missing-Field Lineage Rule

When investigating why a field is missing or unexpectedly blank in the master model, apply the global missing-field rule, then trace the master-model lineage before answering:

1. Identify the live source table and source column.
2. Follow the field through the relevant CTEs, joins, aggregations, and final projection.
3. State the exact step where the field is renamed, aggregated, null-filled, filtered out, or dropped.
4. Use schema/fill-rate checks only as supporting evidence, not as the primary explanation.

## Versioned Object Rule

When the user names a versioned target such as `v2`, create sibling versioned files and BigQuery objects, and leave the existing unversioned production files/views untouched unless the user separately says to replace or overwrite them.

## Local Entrypoint Rule

For current master-model work, start in `model/` and its function/source folders. Treat legacy root-level SQL and docs as compatibility or history unless the user explicitly names them.

## Lean BigQuery Deployment Workflow

For a narrow, schema-preserving master-model change:

1. Inspect the live object and verify local parity once.
2. Edit the permanent SQL and create one isolated QA candidate.
3. Use SQL Change Guard as the sole broad pre-deployment comparison owner. Before configuring it, confirm whether proposed keys are genuinely unique at the live model grain.
4. Reuse the guard's passing schema, row-coverage, and package-level metric evidence. Run an additional query only for a named requirement the guard does not cover.
5. Deploy after the guard passes, then run one focused live check covering the changed fields in the master model and reporting mart.
6. Update required documentation, then delete and confirm removal of temporary artifacts in one cleanup pass.

Do not repeat pre-deployment reconciliation after deployment unless the focused live check conflicts with the validated candidate.

## Dependent Materialized Table Freshness Rule

When a task changes `master_stg.data_model` or any base view that feeds a stored, clustered, reporting, QA, or dashboard-support table, refresh each dependent table in the same work session before claiming the base-view change is complete. If refresh is unsafe, blocked, or intentionally deferred, state that the dependent table is stale, name the table, and give the exact missing refresh proof.

Current dependent tables:

| Dependent table | Base object | Refresh owner | Freshness proof |
|---|---|---|---|
| [Clustered advertiser QA table](https://console.cloud.google.com/bigquery?project=looker-studio-pro-452620&p=looker-studio-pro-452620&d=master_stg&t=data_model_clustered_by_advertiser_qa&page=table) | [Master evidence model](https://console.cloud.google.com/bigquery?project=looker-studio-pro-452620&p=looker-studio-pro-452620&d=master_stg&t=data_model&page=table) | Scheduled query `master_data_model_clustered_advertiser_refresh`; universal runner step `Master Data Model Clustered Advertiser Refresh`; manual same-session refresh after base-view changes | Transfer run succeeds, table remains clustered by `_advertiser`, and row count is reconciled to the source view |
| [Master evidence model v3](https://console.cloud.google.com/bigquery?project=looker-studio-pro-452620&p=looker-studio-pro-452620&d=master_stg&t=data_model_v3&page=table) | [Master evidence model](https://console.cloud.google.com/bigquery?project=looker-studio-pro-452620&p=looker-studio-pro-452620&d=master_stg&t=data_model&page=table) plus DCM and FPD source detail tables | Universal runner step `Master Data Model Clustered Advertiser Refresh`; manual same-session run of `create_master_stg_data_model_v3.sql` after base/source changes | Builder succeeds, table remains clustered by `_advertiser`, no `package_plan` rows exist, visible grain keys are unique, and each package/date has no more than one summable planned carrier |

## Documentation Hygiene

For master model behavior changes, update the repo-level `../CHANGELOG.md` and any stable README field-semantics notes in the same work session. Do this for deployed SQL behavior changes even when row counts or source totals are deliberately left out of durable docs.

When the master model, reporting mart, sibling v2/detail views, or stable upstream source semantics change, update `docs/master-data-model-map.html` in the same work session. Keep the map focused on durable model shape, source lineage, refresh dependencies, precedence rules, and modeling risks; do not update it just to chase fluctuating row counts, source mix counts, package-key counts, or media totals.

In documentation, prefer short human-readable Markdown link labels over raw targets for any referenced files, scripts, folders, BigQuery tables/views, dashboards, sheets, docs, URLs, or other resources. Hide the full target behind the link whenever the reader only needs to navigate to it. Use absolute paths behind local file links and BigQuery console deep links behind warehouse objects. Show raw paths, raw URLs, or full `project.dataset.table` names only when the exact literal value is needed for a command, query, config, or copy/paste instruction.

## Google Sheet Visual Verification

When verifying Google Sheet UX or formatting, distinguish rendered visual order from API/accessibility-tree order. For user-facing layout claims such as slicer order, column visibility, spacing, or on-screen placement, rely on the rendered browser screenshot or explicit pixel/position evidence before reporting the result.

## Manual Package Editor Formatting Preservation

For the live Manual Data Editor Google Sheet, treat user-made formatting edits as the current source of truth. Do not run `manual_package_edits/setup_manual_package_editor_sheet.mjs` or any other formatting rebuild against the live sheet unless the user explicitly asks for a full formatting rebuild. Routine data refreshes should use the loader only. Before any future sheet-formatting change, take a read-only formatting snapshot of the live sheet and compare against the intended edit so user-made formatting is not silently overwritten.

When changing Manual Data Editor headers, column order, visible/hidden column counts, or any field that conditional formatting references, verify and repair the live conditional-format formulas from the current header map before claiming completion. Completion proof must include the conditional-format rule count, sample formulas showing current baseline/manual-marker columns, and live visible-format evidence for the affected user-facing columns/rows. API schema, loader success, or header-value checks alone are not enough.

## Manual Package Editor Marker Debugging

When diagnosing a Manual Data Editor marker/color bug, trace the cell through the full path before naming a cause: visible sheet value, hidden baseline, hidden manual marker, raw manual row, daily manual row, `master_stg.data_model`, `master_stg.data_model_mart`, and the loader's comparison source. For edit detection, distinguish source-derived baseline values from manual-affected final dashboard values; manual-affected final fields can re-mark stale values or clear real edits if used as the comparator.
