# Data Models

**Date:** 2026-06-15  
**Scope:** Local model definitions and documented warehouse contract. Current live state still requires BigQuery inspection before production claims.

## Core Objects

| Object | Grain | Role | Local Definition |
|---|---|---|---|
| [Master evidence model](https://console.cloud.google.com/bigquery?project=looker-studio-pro-452620&ws=!1m5!1m4!4m3!1slooker-studio-pro-452620!2smaster_stg!3sdata_model) | Package/date evidence layer | Combines source rows, manual evidence, issue labels, final metric selection, and package rollups | [create_master_stg_data_model.sql](/Users/eugenetsenter/Looker_clonedRepo/looker_personal/master_data_model/create_master_stg_data_model.sql) |
| [Reporting mart](https://console.cloud.google.com/bigquery?project=looker-studio-pro-452620&ws=!1m5!1m4!4m3!1slooker-studio-pro-452620!2smaster_stg!3sdata_model_mart) | Dashboard-ready package/date | Filters low-signal rows and recalculates reporting rollups | [create_master_stg_data_model_mart.sql](/Users/eugenetsenter/Looker_clonedRepo/looker_personal/master_data_model/create_master_stg_data_model_mart.sql) |
| [Data model v2](https://console.cloud.google.com/bigquery?project=looker-studio-pro-452620&ws=!1m5!1m4!4m3!1slooker-studio-pro-452620!2smaster_stg!3sdata_model_v2) | Compatibility package/date | Wrapper over the main model for existing v2 consumers | [create_master_stg_data_model_v2.sql](/Users/eugenetsenter/Looker_clonedRepo/looker_personal/master_data_model/create_master_stg_data_model_v2.sql) |
| [Delivery detail v2](https://console.cloud.google.com/bigquery?project=looker-studio-pro-452620&ws=!1m5!1m4!4m3!1slooker-studio-pro-452620!2smaster_stg!3sdata_model_delivery_detail_v2) | Source/detail delivery grain | Preserves DCM and original FPD detail without collapsing creative into package/date rows | [create_data_model_delivery_detail_v2.sql](/Users/eugenetsenter/Looker_clonedRepo/looker_personal/master_data_model/create_data_model_delivery_detail_v2.sql) |
| [Master evidence model v3](https://console.cloud.google.com/bigquery?project=looker-studio-pro-452620&p=looker-studio-pro-452620&d=master_stg&t=data_model_v3&page=table) | Natural source grain with one package/date planned carrier | Evaluation table for the one-table lowest-available-grain contract | [create_master_stg_data_model_v3.sql](/Users/eugenetsenter/Looker_clonedRepo/looker_personal/master_data_model/create_master_stg_data_model_v3.sql) |
| [Manual raw edits](https://console.cloud.google.com/bigquery?project=looker-studio-pro-452620&ws=!1m5!1m4!4m3!1slooker-studio-pro-452620!2slanding!3smaster_data_model_manual_package_edits_raw) | One row per editor row | Stores validation status, replacement values, metadata overrides, and baselines | [create_manual_package_edit_tables.sql](/Users/eugenetsenter/Looker_clonedRepo/looker_personal/master_data_model/create_manual_package_edit_tables.sql) |
| [Manual daily rows](https://console.cloud.google.com/bigquery?project=looker-studio-pro-452620&ws=!1m5!1m4!4m3!1slooker-studio-pro-452620!2slanding!3smaster_data_model_manual_package_daily) | Package/date for valid metric edits | Feeds date-bound manual metric values into the master model | [create_manual_package_edit_tables.sql](/Users/eugenetsenter/Looker_clonedRepo/looker_personal/master_data_model/create_manual_package_edit_tables.sql) |

## Source Families

| Source Family | Model Use | Notes |
|---|---|---|
| Prisma | Planned values, package metadata, dates, package IDs | Main source for package context and planned totals. |
| DCM | Delivery evidence and raw ad-server metrics | Aggregated for package/date model; preserved at detail grain in delivery-detail v2. |
| Original FPD | Partner/platform delivery evidence | Preserved separately, then combined into generic FPD fields. |
| Updated FPD | Corrected or refreshed FPD delivery evidence | Layered before final metrics and package actual rollups. |
| TV | Synthetic package/placement rows | TV fields remain visible in `tv_*` columns. |
| Social | Synthetic social keys | Apollo WP-first handling and source-owner review labels are preserved. |
| Amazon Ads | Synthetic Amazon rows | `supply_cost` maps to media spend; sales remain outcome revenue fields. |
| Manual edits | Valid user corrections | `man_*` evidence wins only where valid and grain-compatible. |

## Manual Edit Merge Contract

| Edit Type | Landing Layer | Join Grain | Final Behavior |
|---|---|---|---|
| Delivered metric correction | Manual daily rows | Package/date | Replaces only the edited metric on edited dates. |
| Planned metric correction | Manual daily rows | Package/date across full flight | Replaces planned daily values while keeping package totals aligned. |
| Flight date correction | Manual raw metadata | Package | Applies to every model row for the package. |
| Metadata correction | Manual raw metadata | Package | Applies package-wide, including dates outside metric override windows. |
| Manual-only package | Raw metadata plus daily metric rows | New package/date | Adds rows only when required metadata and metric/date fields validate. |

## Grain Rules

| Field Type | Correct Home |
|---|---|
| Package/date metrics and rollups | Main evidence model or reporting mart |
| Reporting-only filtered totals | Reporting mart |
| Creative/ad/placement detail | Delivery-detail sibling views |
| Manual raw validation state | Manual raw landing table |
| Manual daily metric allocations | Manual daily landing table |
| Repeated package budget context on detail rows | `doNotSum` fields only |
| One-table v3 planned metrics | Exactly one deduced natural row per package/date carries summable `_planned_*`; `qa_v3_package_planned_*_doNotSum` carries repeated planned context. |

## Validation Concepts

| Concept | Meaning |
|---|---|
| Evidence layer | Keeps source visibility and issue labels even when reporting excludes rows later. |
| Reporting mart | Consumer-facing view that filters low-signal rows and recalculates package totals. |
| `man_*` fields | Auditable manual evidence fields, not hidden replacement magic. |
| `doNotSum` fields | Repeated context fields that should not be summed across lower-grain rows. |
| Synthetic keys | Generated package-like keys for sources that do not naturally share Prisma package IDs. |
