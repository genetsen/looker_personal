# Prisma Planning Pipeline

This guide documents how Prisma planning establishes package/date context, planned metrics, and canonical package metadata for the master model. It follows [Master Data Model Pipeline v2](/Users/eugenetsenter/Looker_clonedRepo/looker_personal/master_data_model/README_v2.md).

## Pipeline Overview

```text
Prisma expanded package/date planning
             ↓
Daily planned metrics + latest package metadata
             ↓
Digital package/date assembly with DCM and FPD
             ↓
Stable evidence model and v3 planned-metric carrier rows
```

## Source and Model Contract

| Stage | Object or file | Grain | Responsibility |
|---|---|---|---|
| Planning source | [Prisma expanded planning](https://console.cloud.google.com/bigquery?project=looker-studio-pro-452620&p=looker-studio-pro-452620&d=20250327_data_model&t=prisma_expanded_full&page=table) | Package/date | Planned daily spend, impressions, clicks, package metadata, and report-date freshness. |
| Daily planning CTE | `prisma_daily` in [stable base SQL](/Users/eugenetsenter/Looker_clonedRepo/looker_personal/master_data_model/model/stable_base/create_master_stg_data_model.sql) | Package/date | Sums planned measures and keeps the latest daily report date. |
| Metadata CTE | `prisma_meta` in the same SQL | Package | Selects the most recent report-date record for each package. |
| Shared output | [Master evidence model](https://console.cloud.google.com/bigquery?project=looker-studio-pro-452620&p=looker-studio-pro-452620&d=master_stg&t=data_model&page=table) | Package/date | Supplies planned fields, campaign/package context, and planning-source freshness. |

## Custom Logic

- The active Prisma population excludes `Child` packages and begins at `2025-01-01` based on package start date and row date.
- Prisma is joined by package ID and date to DCM, original FPD, and updated FPD. Delivery-only rows are retained even when their package lacks Prisma metadata; those rows are explicitly labeled as missing a Prisma package rather than silently discarded.
- The latest `report_date` chooses package metadata, while daily planning values are summed by package/date. This prevents an older metadata snapshot from overriding the current package context.
- For digital rows, Prisma planning is distinct from delivered metrics. FPD and DCM determine actuals under the documented precedence; Prisma determines planned spend, planned impressions, and planning context.
- In [v3](https://console.cloud.google.com/bigquery?project=looker-studio-pro-452620&p=looker-studio-pro-452620&d=master_stg&t=data_model_v3&page=table), planned values appear once on a natural package/date carrier row and are repeated as clearly non-summable context on detail rows.

## Field Lineage

| Prisma value | Published field | Meaning |
|---|---|---|
| `planned_daily_spend_pk` | `_planned_spend` | Planned daily spend at package/date grain. |
| `planned_daily_impressions_pk` | `_planned_impressions` | Planned daily impressions at package/date grain. |
| `planned_clicks` | `p_planned_clicks` | Planning evidence; not a delivered click measure. |
| Latest metadata | Package, campaign, advertiser, channel, supplier, and flight context | Preferred context for joined digital rows when available. |
| `report_date` | `p_max_report_date` and freshness evidence | Shows the most recent planning snapshot represented by the model. |

## Debugging Route

| Symptom | First check | Meaning |
|---|---|---|
| A delivery row has no package context | `qa_data_issues` for `missing_prisma_package` | The delivery row is retained but did not match active Prisma planning. |
| A planned field is blank | `prisma_daily` conditions and `p_max_report_date` | The package may exist in metadata but have no planned daily row for that date. |
| A final delivery metric differs from planning | `fpd_*`, `dcm_*`, and final `_` fields | Planning is not delivery; trace the selected actual source. |

## Related Guides

- [DCM delivery pipeline](/Users/eugenetsenter/Looker_clonedRepo/looker_personal/master_data_model/model/branches/dcm/README_dcm-pipeline.md)
- [FPD pipeline](/Users/eugenetsenter/Looker_clonedRepo/looker_personal/master_data_model/model/branches/fpd/README_fpd-pipeline.md)
- [Source branch index](/Users/eugenetsenter/Looker_clonedRepo/looker_personal/master_data_model/model/branches/README.md)
