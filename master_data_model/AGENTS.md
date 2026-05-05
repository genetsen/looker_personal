# AGENTS.md

Project-local instructions for `master_data_model`.

## Missing-Field Lineage Rule

When investigating why a field is missing or unexpectedly blank in the master model, trace the field lineage before answering:

1. Identify the live source table and source column.
2. Follow the field through the relevant CTEs, joins, aggregations, and final projection.
3. State the exact step where the field is renamed, aggregated, null-filled, filtered out, or dropped.
4. Use schema/fill-rate checks only as supporting evidence, not as the primary explanation.
