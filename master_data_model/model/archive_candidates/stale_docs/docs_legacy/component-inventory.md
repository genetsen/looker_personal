# Component Inventory

**Date:** 2026-06-15

This project does not have application UI components in the usual web-app sense. Its components are model definitions, loader scripts, Sheet UX scripts, tests, and documentation assets.

## SQL Components

| Component | File | Responsibility |
|---|---|---|
| Main evidence model | [create_master_stg_data_model.sql](/Users/eugenetsenter/Looker_clonedRepo/looker_personal/master_data_model/create_master_stg_data_model.sql) | Combines source branches, manual values, issue labels, final metric selection, and package/date rollups. |
| Reporting mart | [create_master_stg_data_model_mart.sql](/Users/eugenetsenter/Looker_clonedRepo/looker_personal/master_data_model/create_master_stg_data_model_mart.sql) | Filters reporting-only rows and recalculates rollups. |
| Upstream table refresh | [create_master_data_model_upstream_tables_sched.sql](/Users/eugenetsenter/Looker_clonedRepo/looker_personal/master_data_model/create_master_data_model_upstream_tables_sched.sql) | Builds stored siblings for source views used by the model. |
| Manual edit schemas | [create_manual_package_edit_tables.sql](/Users/eugenetsenter/Looker_clonedRepo/looker_personal/master_data_model/create_manual_package_edit_tables.sql) | Defines manual raw and daily landing tables. |
| Compatibility wrappers | [create_master_stg_data_model_v2.sql](/Users/eugenetsenter/Looker_clonedRepo/looker_personal/master_data_model/create_master_stg_data_model_v2.sql), [create_master_stg_data_model_mart_v2.sql](/Users/eugenetsenter/Looker_clonedRepo/looker_personal/master_data_model/create_master_stg_data_model_mart_v2.sql) | Keep existing v2 consumers pointed at current package/date shapes. |
| Detail models | [create_data_model_delivery_detail_v2.sql](/Users/eugenetsenter/Looker_clonedRepo/looker_personal/master_data_model/create_data_model_delivery_detail_v2.sql) | Preserve lower-grain delivery detail safely. |
| V3 evaluation table | [create_master_stg_data_model_v3.sql](/Users/eugenetsenter/Looker_clonedRepo/looker_personal/master_data_model/create_master_stg_data_model_v3.sql) | Builds the clustered one-table lowest-grain candidate with natural source rows and one summable planned carrier per package/date. |
| Client-specific views | Ritual SQL files | Filter master outputs to Ritual-specific consumers. |
| Sample models | `*_sample.sql` files | Compare alternate package/detail shapes without making them canonical. |

## Refresh Components

| Component | File | Responsibility |
|---|---|---|
| Master stored-table refresh wrapper | [run_master_data_model_clustered_advertiser_refresh.sh](/Users/eugenetsenter/Docs/R_Studio_Projects/universal_cron_runner/automation_hub/workloads/ops/bq_trigger/run_master_data_model_clustered_advertiser_refresh.sh) | Triggers the clustered advertiser QA scheduled query, verifies row-count parity, rebuilds `data_model_v3`, and verifies the v3 grain contract. |

## Manual Editor Components

| Component | File | Responsibility |
|---|---|---|
| Loader | [load_manual_package_edits.R](/Users/eugenetsenter/Looker_clonedRepo/looker_personal/master_data_model/manual_package_edits/load_manual_package_edits.R) | Reads Sheet and warehouse values, detects edits, validates rows, writes manual tables, and refreshes visible values. |
| Full Sheet setup | [setup_manual_package_editor_sheet.mjs](/Users/eugenetsenter/Looker_clonedRepo/looker_personal/master_data_model/manual_package_edits/setup_manual_package_editor_sheet.mjs) | Rebuilds formatting, instructions, protections, hidden helpers, and slicers; guarded against accidental live rebuilds. |
| Filter repair | [repair_manual_package_editor_filters.mjs](/Users/eugenetsenter/Looker_clonedRepo/looker_personal/master_data_model/manual_package_edits/repair_manual_package_editor_filters.mjs) | Expands filters/slicers and points Edited Rows to the correct helper. |
| Conditional formatting repair | [repair_manual_package_editor_conditional_formatting.mjs](/Users/eugenetsenter/Looker_clonedRepo/looker_personal/master_data_model/manual_package_edits/repair_manual_package_editor_conditional_formatting.mjs) | Repairs marker color rules without a full rebuild. |
| Apps Script audit and request control | [Code.js](/Users/eugenetsenter/Looker_clonedRepo/looker_personal/master_data_model/manual_package_edits/apps_script/Code.js) | Stamps row-level manual-edit audit cells, sends notification email or optional Slack request, and resets the checkbox. |

## Test Components

| Test | File | Protects |
|---|---|---|
| Loader choice logic | [test_loader_choice_logic.R](/Users/eugenetsenter/Looker_clonedRepo/looker_personal/master_data_model/manual_package_edits/tests/test_loader_choice_logic.R) | Whether visible values become edits, undos, or baseline refreshes. |
| Daily total proof | [test_daily_total_proof.R](/Users/eugenetsenter/Looker_clonedRepo/looker_personal/master_data_model/manual_package_edits/tests/test_daily_total_proof.R) | Whether replacement totals allocate to daily rows without material drift. |

## Documentation Components

| Component | File | Responsibility |
|---|---|---|
| Main README | [README.md](/Users/eugenetsenter/Looker_clonedRepo/looker_personal/master_data_model/README.md) | Stable model overview, deploy commands, and modeling notes. |
| Manual editor README | [manual_package_edits/README.md](/Users/eugenetsenter/Looker_clonedRepo/looker_personal/master_data_model/manual_package_edits/README.md) | User-facing and operational manual editor workflow. |
| Manual editor runbook | [manual_package_edits/QA_RUNBOOK.md](/Users/eugenetsenter/Looker_clonedRepo/looker_personal/master_data_model/manual_package_edits/QA_RUNBOOK.md) | QA queries, proof path, and troubleshooting map. |
| Model map | [master-data-model-map.html](/Users/eugenetsenter/Looker_clonedRepo/looker_personal/master_data_model/docs/master-data-model-map.html) | Interactive lineage and risk orientation. |
| Audits | Root audit Markdown files | Source-field preservation and ADIF comparison context. |
