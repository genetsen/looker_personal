# master_data_model - Project Overview

**Date:** 2026-06-15  
**Type:** Data / BigQuery semantic model project  
**Architecture:** Warehouse evidence layer plus reporting mart, with a Sheet-backed manual correction workflow

## Executive Summary

`master_data_model` owns the cross-client package/date reporting model for media delivery, planned values, package metadata, and manual correction evidence. The project is centered on BigQuery SQL view/table definitions, with R and JavaScript support for a Google Sheet-based Manual Data Editor.

The important architecture split is simple:

| Layer | Purpose | Main Local Definition |
|---|---|---|
| Upstream snapshots | Store stable table siblings for sources that should be refreshed before the model | [create_master_data_model_upstream_tables_sched.sql](/Users/eugenetsenter/Looker_clonedRepo/looker_personal/master_data_model/create_master_data_model_upstream_tables_sched.sql) |
| Evidence model | Preserve source evidence and apply manual edits before package rollups | [create_master_stg_data_model.sql](/Users/eugenetsenter/Looker_clonedRepo/looker_personal/master_data_model/create_master_stg_data_model.sql) |
| Reporting mart | Apply reporting-only exclusions and recalculate dashboard rollups | [create_master_stg_data_model_mart.sql](/Users/eugenetsenter/Looker_clonedRepo/looker_personal/master_data_model/create_master_stg_data_model_mart.sql) |
| Manual editor | Let users correct visible dashboard values in a controlled Sheet workflow | [manual_package_edits](/Users/eugenetsenter/Looker_clonedRepo/looker_personal/master_data_model/manual_package_edits) |
| Versioned/detail views | Preserve compatibility and lower-grain delivery detail without changing the main package/date grain | [create_data_model_delivery_detail_v2.sql](/Users/eugenetsenter/Looker_clonedRepo/looker_personal/master_data_model/create_data_model_delivery_detail_v2.sql) |
| V3 lowest-grain evaluation table | Test the one-table natural-source-grain model with a deduced planned carrier per package/date | [create_master_stg_data_model_v3.sql](/Users/eugenetsenter/Looker_clonedRepo/looker_personal/master_data_model/create_master_stg_data_model_v3.sql) |

## Project Classification

| Dimension | Value |
|---|---|
| Repository type | Single-part project |
| Primary project type | Data model / semantic-layer workflow |
| Primary language | BigQuery Standard SQL |
| Support languages | R, JavaScript, Google Apps Script |
| Production surface | BigQuery views/tables and the Manual Data Editor Google Sheet |
| Documentation surface | Markdown runbooks plus interactive HTML model maps |

## Key Features

| Feature | What It Does |
|---|---|
| Generalized package/date model | Combines planning, delivery, package metadata, TV, social, Amazon Ads, and manual evidence into one reporting grain. |
| Reporting mart | Keeps the base model as evidence, then filters low-signal rows and recalculates rollups for reporting. |
| Manual Package Editor | Lets users edit visible Sheet values, validates those edits, and writes auditable `man_*` evidence. |
| Delivery-detail v2 | Preserves lower-grain creative and delivery detail in a sibling view instead of collapsing it into the package/date model. |
| Master model v3 | Evaluates a one-table lowest-available-grain shape where natural source rows remain detailed and package/date planned metrics roll up from one deduced carrier row. |
| Model maps | Provide interactive orientation for lineage, dependencies, outputs, and modeling risks. |

## Technology Stack Summary

| Category | Technology | Role |
|---|---|---|
| Warehouse modeling | BigQuery Standard SQL | Defines production and candidate views/tables. |
| Data loading | R with `googlesheets4`, `googledrive`, `bigrquery`, `dplyr`, `tidyr` | Reads the Manual Data Editor, validates changes, and writes manual landing tables. |
| Sheet UX | Node.js / JavaScript | Repairs or rebuilds Sheet formatting, filters, slicers, and hidden helper columns. |
| Notification UX | Google Apps Script | Sends request-refresh email and optional Slack notification from the Sheet. |
| Documentation | Markdown and HTML | Captures model behavior, QA runbooks, and visual model maps. |

## Architecture Highlights

| Highlight | Why It Matters |
|---|---|
| Evidence model and mart are separate | Source visibility stays available even when reporting totals need filters. |
| Lower-grain detail has sibling views | Creative/ad/detail data does not silently duplicate or distort package/date metrics. |
| Manual edits are explicit evidence | Corrected values remain auditable through raw, daily, model, and mart layers. |
| Sheet formatting is not loader-owned | Routine refreshes avoid overwriting user-made formatting changes. |
| Live warehouse is the behavior source of truth | Local SQL files are definitions and deploy inputs, but current behavior must be checked in BigQuery before production claims. |
| Stored support tables need same-session refresh | The clustered advertiser QA table and v3 evaluation table are refreshed together by the universal runner's `Master Data Model Clustered Advertiser Refresh` step. |

## Documentation Map

| Doc | Use It For |
|---|---|
| [Architecture](/Users/eugenetsenter/Looker_clonedRepo/looker_personal/master_data_model/docs/architecture.md) | How the model, mart, manual editor, and sibling views fit together. |
| [Data Models](/Users/eugenetsenter/Looker_clonedRepo/looker_personal/master_data_model/docs/data-models.md) | Major warehouse objects, grains, and merge rules. |
| [Source Tree Analysis](/Users/eugenetsenter/Looker_clonedRepo/looker_personal/master_data_model/docs/source-tree-analysis.md) | What each folder/file group is for. |
| [Development Guide](/Users/eugenetsenter/Looker_clonedRepo/looker_personal/master_data_model/docs/development-guide.md) | Local commands, validation, and safe workflow notes. |
| [Component Inventory](/Users/eugenetsenter/Looker_clonedRepo/looker_personal/master_data_model/docs/component-inventory.md) | Major SQL, R, JS, Sheet, and documentation components. |
| [Manual Package Editor Workflow](/Users/eugenetsenter/Looker_clonedRepo/looker_personal/master_data_model/docs/manual-package-editor-workflow.md) | End-to-end manual correction path. |

## Important Boundary

This documentation was generated from local repo files during a deep documentation scan. It summarizes project structure and intended behavior. For current warehouse state, row counts, schemas, freshness, and production behavior, inspect the live BigQuery objects before making or accepting a data claim.
