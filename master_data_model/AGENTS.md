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

## Read-Only BigQuery Permission Classification Rule

Classify BigQuery work by the actual operation, not by generic platform approval wording. Schema inspection, table metadata reads, row counts, fill-rate checks, `SELECT` queries, `INFORMATION_SCHEMA` queries, dry runs, and MCP table/query inspection methods are read-only. Do not ask the user for permission for those operations, and do not describe them as modifications.

Mutating work includes `CREATE`, `CREATE OR REPLACE`, `ALTER`, `DROP`, `DELETE`, `UPDATE`, `MERGE`, `INSERT`, table/view replacement, scheduled-query changes, and metadata writes. For those operations, follow the project versioning and approval rules.

If a read-only MCP path is blocked or misclassified by an approval layer, switch to a safe read-only fallback such as `bq show`, a read-only `SELECT`, or the repo helper `scripts/use_sandbox_gcloud.sh`, but only after confirming the selected CLI auth/config path already has an active account. Do not retry through a fresh sandbox-writable `CLOUDSDK_CONFIG` that has not been authenticated; if that path has no active account, stop the CLI fallback and return to BigQuery MCP or another already-working read-only path.

## Documentation Hygiene

For master model behavior changes, update the repo-level `../CHANGELOG.md` and any stable README field-semantics notes in the same work session. Do this for deployed SQL behavior changes even when row counts or source totals are deliberately left out of durable docs.
