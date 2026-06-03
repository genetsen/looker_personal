# AGENTS.md

Project-local instructions for `master_data_model`.

## Missing-Field Lineage Rule

When investigating why a field is missing or unexpectedly blank in the master model, trace the field lineage before answering:

1. Identify the live source table and source column.
2. Follow the field through the relevant CTEs, joins, aggregations, and final projection.
3. State the exact step where the field is renamed, aggregated, null-filled, filtered out, or dropped.
4. Use schema/fill-rate checks only as supporting evidence, not as the primary explanation.

## Versioned Object Rule

When the user names a versioned target such as `v2`, create sibling versioned files and BigQuery objects, and leave the existing unversioned production files/views untouched unless the user separately says to replace or overwrite them.

## Lower-Grain Field Rule

When adding a field that exists at a lower grain than the current model row, such as creative, ad, line item, placement detail, or any one-to-many source field, do not silently collapse it into the existing row with `STRING_AGG`, `ARRAY_AGG`, first-value selection, or similar aggregation. First state the grain conflict and ask the user which representation they want, especially when they also ask to avoid row duplication. Acceptable options may include a separate detail view, a nested/array field, a deliberately expanded-grain v2, or an explicitly approved summary field.

## Collaborative Modeling Decision Rule

When a request can be satisfied multiple ways, especially in data modeling, do not choose the simplest technical implementation silently. Before implementing, identify the business question the user is trying to answer, the hidden modeling choices inside the request, and the tradeoffs between correctness, usability, row grain, downstream dashboard behavior, and implementation effort.

Recommend the approach that is most analytically useful and correct, not the cheapest valid SQL. If the best representation is ambiguous, pause and ask the user to choose before implementing a summary, fallback, aggregation, null-fill, renamed proxy, expanded grain, separate view, or other modeling compromise.

## No Silent Modeling Compromise Rule

When preserving existing row counts, schema shape, or downstream compatibility conflicts with adding a requested field correctly, do not hide that conflict with a lossy workaround. State the conflict plainly and offer options such as keeping the current grain with an explicitly approved summary field, creating a separate detail view, creating a nested or array field, building a new expanded-grain model, or deferring the field until the correct source and grain are agreed.

## Source Column Preservation Rule

When building downstream tables, views, extracts, reports, or derived datasets from source data, never silently remove source columns from the downstream output. If a source column would be dropped, excluded, renamed away, aggregated away, or made unavailable to preserve grain, schema shape, performance, or dashboard compatibility, state the proposed column change and get explicit user approval before implementing it.

## Read-Only BigQuery Permission Classification Rule

Classify BigQuery work by the actual operation, not by generic platform approval wording. Schema inspection, table metadata reads, row counts, fill-rate checks, `SELECT` queries, `INFORMATION_SCHEMA` queries, dry runs, and MCP table/query inspection methods are read-only. Do not ask the user for permission for those operations, and do not describe them as modifications.

Mutating work includes `CREATE`, `CREATE OR REPLACE`, `ALTER`, `DROP`, `DELETE`, `UPDATE`, `MERGE`, `INSERT`, table/view replacement, scheduled-query changes, and metadata writes. For those operations, follow the project versioning and approval rules.

If a read-only MCP path is blocked or misclassified by an approval layer, switch to a safe read-only fallback such as `bq show`, a read-only `SELECT`, or the repo helper `scripts/use_sandbox_gcloud.sh`, but only after confirming the selected CLI auth/config path already has an active account. Do not retry through a fresh sandbox-writable `CLOUDSDK_CONFIG` that has not been authenticated; if that path has no active account, stop the CLI fallback and return to BigQuery MCP or another already-working read-only path.

## Documentation Hygiene

For master model behavior changes, update the repo-level `../CHANGELOG.md` and any stable README field-semantics notes in the same work session. Do this for deployed SQL behavior changes even when row counts or source totals are deliberately left out of durable docs.

In documentation, prefer short human-readable Markdown link labels over raw targets for any referenced files, scripts, folders, BigQuery tables/views, dashboards, sheets, docs, URLs, or other resources. Hide the full target behind the link whenever the reader only needs to navigate to it. Use absolute paths behind local file links and BigQuery console deep links behind warehouse objects. Show raw paths, raw URLs, or full `project.dataset.table` names only when the exact literal value is needed for a command, query, config, or copy/paste instruction.

## Google Sheet Visual Verification

When verifying Google Sheet UX or formatting, distinguish rendered visual order from API/accessibility-tree order. For user-facing layout claims such as slicer order, column visibility, spacing, or on-screen placement, rely on the rendered browser screenshot or explicit pixel/position evidence before reporting the result.

## Manual Package Editor Formatting Preservation

For the live Manual Data Editor Google Sheet, treat user-made formatting edits as the current source of truth. Do not run `manual_package_edits/setup_manual_package_editor_sheet.mjs` or any other formatting rebuild against the live sheet unless the user explicitly asks for a full formatting rebuild. Routine data refreshes should use the loader only. Before any future sheet-formatting change, take a read-only formatting snapshot of the live sheet and compare against the intended edit so user-made formatting is not silently overwritten.

## Manual Package Editor Marker Debugging

When diagnosing a Manual Data Editor marker/color bug, trace the cell through the full path before naming a cause: visible sheet value, hidden baseline, hidden manual marker, raw manual row, daily manual row, `master_stg.data_model`, `master_stg.data_model_mart`, and the loader's comparison source. For edit detection, distinguish source-derived baseline values from manual-affected final dashboard values; manual-affected final fields can re-mark stale values or clear real edits if used as the comparator.
