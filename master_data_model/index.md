# Directory Index

This index points to the durable documentation and model-definition files in the
master data model project. Generated outputs, BMAD installer/config files, and
one-off proof artifacts are intentionally omitted so this page stays focused on
the files most useful for day-to-day navigation.

## Project Docs

- **[ADIF_MASTER_MISSING_INFORMATION_AUDIT_2026-05-07.md](./ADIF_MASTER_MISSING_INFORMATION_AUDIT_2026-05-07.md)** - ADIF versus master schema audit
- **[AGENTS.md](./AGENTS.md)** - Project-local agent operating rules
- **[CHANGELOG.md](./CHANGELOG.md)** - Durable model change history
- **[FULL_SOURCE_FIELD_AUDIT.md](./FULL_SOURCE_FIELD_AUDIT.md)** - Source field preservation audit
- **[README.md](./README.md)** - Master data model overview
- **[SCHEMA_AUDIT.md](./SCHEMA_AUDIT.md)** - Missing schema field audit

## Core SQL Models

- **[create_master_data_model_upstream_tables_sched.sql](./create_master_data_model_upstream_tables_sched.sql)** - Scheduled upstream table snapshots
- **[create_master_stg_data_model.sql](./create_master_stg_data_model.sql)** - Main package/date evidence model
- **[create_master_stg_data_model_mart.sql](./create_master_stg_data_model_mart.sql)** - Dashboard reporting mart model
- **[create_master_stg_data_model_v3.sql](./create_master_stg_data_model_v3.sql)** - V3 lowest-grain evaluation table builder
- **[create_manual_package_edit_tables.sql](./create_manual_package_edit_tables.sql)** - Manual edit landing schemas

## Versioned And Sample SQL

- **[create_data_model_delivery_detail_v2.sql](./create_data_model_delivery_detail_v2.sql)** - Corrected delivery-detail v2 model
- **[create_data_model_delivery_detail_v3_sample.sql](./create_data_model_delivery_detail_v3_sample.sql)** - Sample delivery-detail v3 model
- **[create_data_model_detail_master_v2_sample.sql](./create_data_model_detail_master_v2_sample.sql)** - Sample one-table v2 hierarchy
- **[create_data_model_detail_master_v3_sample.sql](./create_data_model_detail_master_v3_sample.sql)** - Sample one-table v3 hierarchy
- **[create_data_model_package_daily_v3_sample.sql](./create_data_model_package_daily_v3_sample.sql)** - Sample package/date v3 summary
- **[create_master_stg_data_model_mart_v2.sql](./create_master_stg_data_model_mart_v2.sql)** - Reporting mart v2 wrapper
- **[create_master_stg_data_model_v2.sql](./create_master_stg_data_model_v2.sql)** - Package/date v2 compatibility wrapper
- **[create_ritual_data_model_delivery_detail_v2.sql](./create_ritual_data_model_delivery_detail_v2.sql)** - Ritual delivery-detail v2 view
- **[create_ritual_data_model_view.sql](./create_ritual_data_model_view.sql)** - Ritual package/date compatibility view
- **[create_ritual_data_model_view_v2.sql](./create_ritual_data_model_view_v2.sql)** - Ritual v2 compatibility view

## Manual Package Editor

- **[manual_package_edits/QA_RUNBOOK.md](./manual_package_edits/QA_RUNBOOK.md)** - Manual editor QA runbook
- **[manual_package_edits/README.md](./manual_package_edits/README.md)** - Manual package editor guide

## Context Summaries

- **[ai_context_summaries/2026-05-01_column_rename_map_context.md](./ai_context_summaries/2026-05-01_column_rename_map_context.md)** - Column rename context handoff
- **[ai_context_summaries/2026-05-15-manual-package-edits.md](./ai_context_summaries/2026-05-15-manual-package-edits.md)** - Manual editor implementation handoff
- **[ai_context_summaries/2026-05-20-manual-editor-docs-continuation.md](./ai_context_summaries/2026-05-20-manual-editor-docs-continuation.md)** - Manual editor docs continuation
