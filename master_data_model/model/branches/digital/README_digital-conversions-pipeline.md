# Digital Conversion Outcomes Pipeline

This guide documents how Ritual conversion outcomes enter the versioned master evidence model. It uses the source-to-output structure of [Master Data Model Pipeline v2](/Users/eugenetsenter/Looker_clonedRepo/looker_personal/master_data_model/README_v2.md), while keeping outcomes separate from media delivery.

## Pipeline Overview

```text
Ritual conversion-report Sheet mirror
              ↓
Package ID extraction and source-grain normalization
              ↓
Nearest package-context lookup when needed
              ↓
Master evidence model v3 conversion-outcome rows
```

## Source and Output Contract

| Stage | Object or file | Grain | Responsibility |
|---|---|---|---|
| Source | [Ritual conversion report](https://console.cloud.google.com/bigquery?project=looker-studio-pro-452620&p=looker-studio-pro-452620&d=landing&t=rtl_conv_report&page=table) | Package/date/site/creative/activity | Sheet-mirrored conversion activity, source impressions/clicks, creative, and Sheet lineage. |
| Reference normalizer | [Conversion normalization SQL](/Users/eugenetsenter/Looker_clonedRepo/looker_personal/master_data_model/model/branches/digital/conversions/normalize_rtl_conv_report.sql) | Package/date/site/creative/activity | Readable reference for the logic embedded in the active builder. |
| Active builder | [Final model v3 SQL](/Users/eugenetsenter/Looker_clonedRepo/looker_personal/master_data_model/model/final_model/create_master_stg_data_model_v3.sql) | Package/date/site/creative/activity | Matches package context and emits conversion-outcome rows. |
| Output | [Master evidence model v3](https://console.cloud.google.com/bigquery?project=looker-studio-pro-452620&p=looker-studio-pro-452620&d=master_stg&t=data_model_v3&page=table) | Lowest available conversion source grain | Keeps conversion outcomes available without altering delivery totals. |

## Custom Logic and Boundaries

- The normalizer extracts the package ID from `package_roadblock`, accepts rows from `2025-01-01` onward, and excludes rows that cannot yield a package ID.
- It groups by package, date, site, campaign, creative, and activity; source impressions and clicks are preserved in `conv_source_*` evidence fields.
- The builder first seeks exact package/date context in the stable model. When a conversion happens after the final delivery date, it attaches the nearest package context and adds `conversion_date_without_delivery_row` to the QA issues.
- Conversion rows use `qa_v3_row_type = conversion_outcome` and `qa_v3_metric_grain = package_date_site_creative_activity`.
- Conversion outcomes do not populate `_planned_spend`, `_planned_impressions`, `_spend`, `_impressions`, `_clicks`, `_video_plays`, `_video_views`, or `_video_comps`. That protects media-delivery totals from post-media outcome rows.

## Field Lineage

| Source value | v3 evidence field | Meaning |
|---|---|---|
| `total_conversions` | `conv_total_conversions` | The conversion outcome count. |
| Source report impressions/clicks | `conv_source_impressions`, `conv_source_clicks` | Source-report context only, not delivery metrics. |
| Site, campaign, package roadblock, creative, activity | `conv_*` detail fields | Keeps outcome provenance at the source grain. |
| Latest load and Sheet identifiers | `conv_loaded_at`, `conv_source_sheet_*` | Lets a reader trace an outcome to the Sheet load that supplied it. |

## Debugging Route

| Question | First check | Then trace |
|---|---|---|
| Why is an outcome missing? | [Ritual conversion report](https://console.cloud.google.com/bigquery?project=looker-studio-pro-452620&p=looker-studio-pro-452620&d=landing&t=rtl_conv_report&page=table) and extracted package ID | The reference normalizer, then the v3 output. |
| Why is delivery blank on the row? | `qa_v3_row_type` and `qa_v3_metric_behavior` | Blank delivery fields are intentional for an outcome row. |
| Why is a package context from another date used? | `qa_data_issues` for `conversion_date_without_delivery_row` | The event occurred on a date with no exact delivery row. |

## Related Guides

- [Prisma planning pipeline](/Users/eugenetsenter/Looker_clonedRepo/looker_personal/master_data_model/model/branches/prisma/README_prisma-pipeline.md)
- [DCM delivery pipeline](/Users/eugenetsenter/Looker_clonedRepo/looker_personal/master_data_model/model/branches/dcm/README_dcm-pipeline.md)
- [FPD pipeline](/Users/eugenetsenter/Looker_clonedRepo/looker_personal/master_data_model/model/branches/fpd/README_fpd-pipeline.md)
- [Source branch index](/Users/eugenetsenter/Looker_clonedRepo/looker_personal/master_data_model/model/branches/README.md)
