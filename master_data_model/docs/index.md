# master_data_model Documentation Index

**Type:** Single-part data model project  
**Primary Language:** BigQuery Standard SQL  
**Architecture:** Warehouse evidence layer plus reporting mart, with a Sheet-backed manual correction workflow  
**Last Updated:** 2026-06-15

## Project Overview

`master_data_model` documents and maintains the generalized package/date reporting model, its reporting mart, its sibling compatibility/detail views, and the Manual Data Editor workflow used for controlled dashboard corrections.

## Quick Reference

| Area | Value |
|---|---|
| Main local model definition | [create_master_stg_data_model.sql](/Users/eugenetsenter/Looker_clonedRepo/looker_personal/master_data_model/create_master_stg_data_model.sql) |
| Reporting mart definition | [create_master_stg_data_model_mart.sql](/Users/eugenetsenter/Looker_clonedRepo/looker_personal/master_data_model/create_master_stg_data_model_mart.sql) |
| V3 evaluation table builder | [create_master_stg_data_model_v3.sql](/Users/eugenetsenter/Looker_clonedRepo/looker_personal/master_data_model/create_master_stg_data_model_v3.sql) |
| Stored support table refresh | [run_master_data_model_clustered_advertiser_refresh.sh](/Users/eugenetsenter/Docs/R_Studio_Projects/universal_cron_runner/automation_hub/workloads/ops/bq_trigger/run_master_data_model_clustered_advertiser_refresh.sh) |
| Manual editor folder | [manual_package_edits](/Users/eugenetsenter/Looker_clonedRepo/looker_personal/master_data_model/manual_package_edits) |
| Interactive model map | [master-data-model-map.html](/Users/eugenetsenter/Looker_clonedRepo/looker_personal/master_data_model/docs/master-data-model-map.html) |
| DCM cost model map | [dcm-cost-model-map.html](/Users/eugenetsenter/Looker_clonedRepo/looker_personal/master_data_model/docs/dcm-cost-model-map.html) |
| Project rules | [AGENTS.md](/Users/eugenetsenter/Looker_clonedRepo/looker_personal/master_data_model/AGENTS.md) |

## Generated Documentation

| Document | Description |
|---|---|
| [Project Overview](./project-overview.md) | Executive summary and high-level architecture. |
| [Architecture](./architecture.md) | System layers, data flow, responsibilities, and warnings. |
| [Data Models](./data-models.md) | Warehouse objects, grains, source families, and merge contracts. |
| [Source Tree Analysis](./source-tree-analysis.md) | Annotated folder and file structure. |
| [Development Guide](./development-guide.md) | Commands, validation approach, deploy order, and guardrails. |
| [Component Inventory](./component-inventory.md) | SQL, loader, Sheet UX, test, and docs components. |
| [Manual Package Editor Workflow](./manual-package-editor-workflow.md) | End-to-end correction workflow from Sheet to mart. |
| [Folder Organization Impact](./folder-organization-impact.md) | Safe-move matrix for docs, generated artifacts, SQL builders, loaders, and workflow folders. |

## Existing Documentation

| Document | Description |
|---|---|
| [Root README](/Users/eugenetsenter/Looker_clonedRepo/looker_personal/master_data_model/README.md) | Main model overview, source notes, deploy commands, and QA guidance. |
| [Root directory index](/Users/eugenetsenter/Looker_clonedRepo/looker_personal/master_data_model/index.md) | Short docs-only navigation index for the project root. |
| [Manual editor README](/Users/eugenetsenter/Looker_clonedRepo/looker_personal/master_data_model/manual_package_edits/README.md) | Manual editor workflow and user-facing rules. |
| [Manual editor QA runbook](/Users/eugenetsenter/Looker_clonedRepo/looker_personal/master_data_model/manual_package_edits/QA_RUNBOOK.md) | Operational proof path and troubleshooting queries. |
| [Full source field audit](/Users/eugenetsenter/Looker_clonedRepo/looker_personal/master_data_model/docs/audits/FULL_SOURCE_FIELD_AUDIT.md) | Source-field preservation audit. |
| [Schema audit](/Users/eugenetsenter/Looker_clonedRepo/looker_personal/master_data_model/docs/audits/SCHEMA_AUDIT.md) | Missing schema field audit. |
| [ADIF comparison audit](/Users/eugenetsenter/Looker_clonedRepo/looker_personal/master_data_model/docs/audits/ADIF_MASTER_MISSING_INFORMATION_AUDIT_2026-05-07.md) | ADIF versus master exact-name and concept comparison. |
| [Interactive model map](/Users/eugenetsenter/Looker_clonedRepo/looker_personal/master_data_model/docs/master-data-model-map.html) | Clickable model lineage and warning map. |
| [Interactive model map v2](/Users/eugenetsenter/Looker_clonedRepo/looker_personal/master_data_model/docs/master-data-model-map-v2.html) | Versioned model lineage map. |
| [DCM cost model map](/Users/eugenetsenter/Looker_clonedRepo/looker_personal/master_data_model/docs/dcm-cost-model-map.html) | Clickable DCM scheduled-query and cost-allocation map. |

## Getting Started

### Read First

1. [Project Overview](./project-overview.md)
2. [Architecture](./architecture.md)
3. [Data Models](./data-models.md)
4. [Manual Package Editor Workflow](./manual-package-editor-workflow.md)

### Common Validation Commands

```bash
Rscript manual_package_edits/tests/test_loader_choice_logic.R
Rscript manual_package_edits/tests/test_daily_total_proof.R
```

```bash
bq query --project_id=looker-studio-pro-452620 --use_legacy_sql=false --dry_run \
  < master_data_model/create_master_stg_data_model.sql
```

## For AI-Assisted Development

| Task | Start With |
|---|---|
| Model behavior or field lineage | [Data Models](./data-models.md), then inspect live BigQuery before claims. |
| SQL architecture change | [Architecture](./architecture.md) and [Development Guide](./development-guide.md). |
| V3 or clustered support-table freshness | [Development Guide](./development-guide.md), then run the stored support table refresh wrapper. |
| Manual editor issue | [Manual Package Editor Workflow](./manual-package-editor-workflow.md), then [QA runbook](/Users/eugenetsenter/Looker_clonedRepo/looker_personal/master_data_model/manual_package_edits/QA_RUNBOOK.md). |
| Folder orientation | [Source Tree Analysis](./source-tree-analysis.md). |
| Component ownership | [Component Inventory](./component-inventory.md). |

## Scan Boundary

This documentation was generated from local files using a deep scan. It is a strong project orientation layer, but it is not a substitute for live BigQuery verification when the question is about current warehouse state, schemas, row counts, or deployed behavior.

---

_Documentation generated by BMAD Method `document-project` workflow._
