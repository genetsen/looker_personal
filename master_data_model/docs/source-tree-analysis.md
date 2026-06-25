# Source Tree Analysis

**Date:** 2026-06-15  
**Scope:** Deep scan of durable project docs, SQL definitions, manual editor implementation files, and existing model maps.

## Annotated Tree

```text
master_data_model/
├── README.md                                # Main project overview and deployment notes
├── CHANGELOG.md                             # Durable change history
├── AGENTS.md                                # Project-local operating rules
├── index.md                                 # Human-friendly root navigation index
├── create_master_stg_data_model.sql         # Main package/date evidence model
├── create_master_stg_data_model_mart.sql    # Reporting mart over the evidence layer
├── create_master_data_model_upstream_tables_sched.sql
│                                             # Scheduled upstream table snapshots
├── create_manual_package_edit_tables.sql    # Manual edit raw/daily landing table schemas
├── create_*_v2.sql                          # Compatibility and delivery-detail v2 views
├── create_*_v3_sample.sql                   # Exploratory sample models
├── docs/
│   ├── master-data-model-map.html           # Interactive model map
│   ├── master-data-model-map-v2.html        # Versioned interactive model map
│   ├── project-scan-report.json             # BMAD workflow state
│   └── *.md                                 # Generated BMAD documentation
├── manual_package_edits/
│   ├── README.md                            # Manual editor user/workflow guide
│   ├── QA_RUNBOOK.md                        # Operational QA and troubleshooting guide
│   ├── load_manual_package_edits.R          # Main Sheet/BigQuery loader
│   ├── setup_manual_package_editor_sheet.mjs
│   │                                         # Full formatting/setup rebuild tool
│   ├── repair_manual_package_editor_filters.mjs
│   │                                         # Narrow filter/slicer repair tool
│   ├── repair_manual_package_editor_conditional_formatting.mjs
│   │                                         # Conditional-formatting repair tool
│   ├── apps_script/
│   │   └── Code.js                          # Request-refresh notification script
│   └── tests/
│       ├── test_loader_choice_logic.R       # Edit/baseline choice regression tests
│       └── test_daily_total_proof.R         # Daily allocation proof tests
├── ai_context_summaries/                    # Prior handoff/context summaries
├── amazon_reports/                          # Local Amazon report input snapshot
├── _bmad/                                   # BMAD installer/config surface
└── _bmad-output/                            # Generated BMAD agent context
```

## Critical Folders

| Folder | Purpose | Notes |
|---|---|---|
| [docs](/Users/eugenetsenter/Looker_clonedRepo/looker_personal/master_data_model/docs) | Stable model maps and generated project documentation | New BMAD docs are written here. |
| [manual_package_edits](/Users/eugenetsenter/Looker_clonedRepo/looker_personal/master_data_model/manual_package_edits) | Manual Data Editor loader, UX repair tools, Apps Script, and tests | Treat live Sheet formatting as source of truth unless a full rebuild is explicitly requested. |
| [ai_context_summaries](/Users/eugenetsenter/Looker_clonedRepo/looker_personal/master_data_model/ai_context_summaries) | Short prior-work handoffs | Useful for continuity, not as current production proof. |
| [amazon_reports](/Users/eugenetsenter/Looker_clonedRepo/looker_personal/master_data_model/amazon_reports) | Local Amazon Ads report snapshot | Supports the Amazon Ads branch of the model. |
| [_bmad-output](/Users/eugenetsenter/Looker_clonedRepo/looker_personal/master_data_model/_bmad-output) | Generated BMAD project context | Helpful agent context; not production code. |

## Entry Points

| Entry Point | Kind | Use |
|---|---|---|
| [create_master_stg_data_model.sql](/Users/eugenetsenter/Looker_clonedRepo/looker_personal/master_data_model/create_master_stg_data_model.sql) | SQL model | Main evidence model deploy definition. |
| [create_master_stg_data_model_mart.sql](/Users/eugenetsenter/Looker_clonedRepo/looker_personal/master_data_model/create_master_stg_data_model_mart.sql) | SQL model | Reporting mart deploy definition. |
| [create_master_data_model_upstream_tables_sched.sql](/Users/eugenetsenter/Looker_clonedRepo/looker_personal/master_data_model/create_master_data_model_upstream_tables_sched.sql) | SQL table refresh | Stored upstream source table refresh. |
| [load_manual_package_edits.R](/Users/eugenetsenter/Looker_clonedRepo/looker_personal/master_data_model/manual_package_edits/load_manual_package_edits.R) | R loader | Manual editor refresh, validation, and BigQuery write path. |
| [Code.js](/Users/eugenetsenter/Looker_clonedRepo/looker_personal/master_data_model/manual_package_edits/apps_script/Code.js) | Apps Script | Row-level manual-edit audit stamps plus request-refresh notification control. |

## Generated Or Supporting Areas

| Area | Treatment During Documentation |
|---|---|
| [outputs](/Users/eugenetsenter/Looker_clonedRepo/looker_personal/master_data_model/outputs) | Generated proof/presentation artifacts; skipped for normal architecture docs unless a task targets them. |
| [_bmad](/Users/eugenetsenter/Looker_clonedRepo/looker_personal/master_data_model/_bmad) | Tooling/config; read for workflow behavior, not treated as model code. |
| [_bmad-output](/Users/eugenetsenter/Looker_clonedRepo/looker_personal/master_data_model/_bmad-output) | Generated context; read as supporting context, not source of truth. |
