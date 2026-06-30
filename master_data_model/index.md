# Directory Index

This index points to the durable documentation and model-definition files in the
master data model project. Generated outputs, BMAD installer/config files, and
one-off proof artifacts are intentionally omitted so this page stays focused on
the files most useful for day-to-day navigation.

## Project Docs

- **[AGENTS.md](./AGENTS.md)** - Project-local agent operating rules
- **[CHANGELOG.md](./CHANGELOG.md)** - Durable model change history
- **[model/README.md](./model/README.md)** - Current function-organized model workspace
- **[README.md](./README.md)** - Master data model overview

## Audit References

- **[ADIF master missing-information audit](./docs/audits/ADIF_MASTER_MISSING_INFORMATION_AUDIT_2026-05-07.md)** - ADIF versus master schema audit
- **[Full source field audit](./docs/audits/FULL_SOURCE_FIELD_AUDIT.md)** - Source field preservation audit
- **[Schema audit](./docs/audits/SCHEMA_AUDIT.md)** - Missing schema field audit

## Core SQL Models

- **[Current final model](./model/final_model/create_master_stg_data_model_v3.sql)** - Clustered creative-capable v3 table builder
- **[Stable base model](./model/stable_base/create_master_stg_data_model.sql)** - Package/date base model consumed by v3
- **[Upstream support refresh](./model/stable_base/create_master_data_model_upstream_tables_sched.sql)** - Stored upstream source snapshots
- **[Reporting mart](./model/reporting_outputs/create_master_stg_data_model_mart.sql)** - Dashboard-facing mart model
- **[Manual edit schemas](./model/manual_editor/create_manual_package_edit_tables.sql)** - Manual edit landing schemas
- **[Archived deprecated SQL](./model/archive_candidates/deprecated_sql/)** - Retired v2, Ritual, sample, and QA candidate SQL

## Manual Package Editor

- **[Manual Editor workspace](./model/manual_editor/)** - Manual editor guide, QA, loader, Apps Script, repair tools, and tests

## Maps

- **[Master model map](./model/reference_maps/master-data-model-map.html)** - Main interactive map
- **[DCM cost model map](./model/reference_maps/dcm-cost-model-map.html)** - DCM and creative-safe join map
- **[Manual editor workflow map](./model/reference_maps/manual-data-editor-workflow-map.html)** - Manual editor workflow map

## Context Summaries

- **[ai_context_summaries/2026-05-01_column_rename_map_context.md](./ai_context_summaries/2026-05-01_column_rename_map_context.md)** - Column rename context handoff
- **[ai_context_summaries/2026-05-15-manual-package-edits.md](./ai_context_summaries/2026-05-15-manual-package-edits.md)** - Manual editor implementation handoff
- **[ai_context_summaries/2026-05-20-manual-editor-docs-continuation.md](./ai_context_summaries/2026-05-20-manual-editor-docs-continuation.md)** - Manual editor docs continuation
