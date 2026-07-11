# TV Estimates Pipeline

This guide documents how local and national TV estimate rows are normalized into the master data model. It uses the source-to-output format from [Master Data Model Pipeline v2](/Users/eugenetsenter/Looker_clonedRepo/looker_personal/master_data_model/README_v2.md).

## Pipeline Overview

```text
Combined TV estimates
        ↓
Stable TV normalization and synthetic keys
        ↓
Master evidence model
        ↓
Master evidence model v3 natural TV source rows
```

## Source and Output Contract

| Stage | Object or file | Grain | Responsibility |
|---|---|---|---|
| Source | [TV combined table](https://console.cloud.google.com/bigquery?project=looker-studio-pro-452620&p=looker-studio-pro-452620&d=landing&t=tv_combined_tbl&page=table) | Outlet/program/market/date | Combined local and national estimates, including cost, impressions, units, and source refresh date. |
| Transformation | `tv_base`, `tv_daily`, and `tv_final` in [stable base SQL](/Users/eugenetsenter/Looker_clonedRepo/looker_personal/master_data_model/model/stable_base/create_master_stg_data_model.sql) | Synthetic package/placement/date | Converts source text, aggregates like keys, and publishes TV evidence plus final metrics. |
| Evidence output | [Master evidence model](https://console.cloud.google.com/bigquery?project=looker-studio-pro-452620&p=looker-studio-pro-452620&d=master_stg&t=data_model&page=table) | TV placement/date | Exposes `tv_*` evidence and final delivery/planning fields. |
| V3 output | [Master evidence model v3](https://console.cloud.google.com/bigquery?project=looker-studio-pro-452620&p=looker-studio-pro-452620&d=master_stg&t=data_model_v3&page=table) | Source TV daily row | Carries TV as a non-digital natural source row. |

## Custom Logic

- The branch date-gates source rows from `2025-01-01` and normalizes blank strings before key construction.
- A synthetic package ID hashes advertiser, campaign, TV type, and outlet. A distinct synthetic placement ID adds program and market. This preserves TV’s natural structure without inventing a Prisma package ID.
- TV daily rows aggregate net cost, net impressions, and total units by synthetic package, placement, and date. Package start/end dates are calculated from the available TV rows.
- `net_cost` and `net_impressions` are both TV source evidence and planned daily fields. The common planned-metric fallback can therefore fill final TV spend/impressions when a corresponding delivered metric is absent, without replacing a real delivered value.
- Final TV clicks and video metrics are intentionally blank because this source does not supply them.

## Field Lineage

| TV source value | Evidence field | Final field |
|---|---|---|
| `net_cost` | `tv_net_cost` | `_spend` and `_planned_spend`. |
| `net_impressions` | `tv_net_impressions` | `_impressions` and `_planned_impressions`. |
| `total_units` | `tv_total_units` | TV source context only. |
| Outlet, program, market, type, quarter, year | matching `tv_*` fields | Supplier, placement, package, and model context. |
| `data_refresh_date` | `tv_data_refresh_date` | Source freshness evidence; it is not the delivery date. |

## Debugging Route

| Question | Check |
|---|---|
| Did the source arrive? | [TV combined table](https://console.cloud.google.com/bigquery?project=looker-studio-pro-452620&p=looker-studio-pro-452620&d=landing&t=tv_combined_tbl&page=table), then `tv_data_refresh_date`. |
| Why does a row have a synthetic package ID? | TV has no natural Prisma package ID; inspect advertiser, campaign, type, and outlet inputs to the stable-base hash. |
| Why is a metric planned rather than delivered? | Compare final fields to `tv_*` evidence and inspect the common planned-metric fallback rule. |

## Related Guides

- [Prisma planning pipeline](/Users/eugenetsenter/Looker_clonedRepo/looker_personal/master_data_model/model/branches/prisma/README_prisma-pipeline.md)
- [Amazon Ads pipeline](/Users/eugenetsenter/Looker_clonedRepo/looker_personal/master_data_model/model/branches/amazon/README_amazon-pipeline.md)
- [Source branch index](/Users/eugenetsenter/Looker_clonedRepo/looker_personal/master_data_model/model/branches/README.md)
