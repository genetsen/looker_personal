# Amazon Ads Pipeline

This guide documents how Ritual Amazon Ads delivery enters the master evidence model. It follows the reader-first pipeline style in [Master Data Model Pipeline v2](/Users/eugenetsenter/Looker_clonedRepo/looker_personal/master_data_model/README_v2.md): source, controlled transformation, output fields, and boundaries.

## Pipeline Overview

```text
Ritual Amazon Ads daily landing report
          ↓
Stable package/date base: amazon_final
          ↓
Master evidence model
          ↓
Master evidence model v3 (natural Amazon source rows)
```

## Source and Output Contract

| Stage | Object or file | Grain | Responsibility |
|---|---|---|---|
| Landing source | [Ritual Amazon Ads daily report](https://console.cloud.google.com/bigquery?project=looker-studio-pro-452620&p=looker-studio-pro-452620&d=landing&t=rit_amzn_report_daily&page=table) | Campaign/ad group/ad/date | Stores Amazon delivery, commercial outcomes, email-file lineage, and load timestamp. |
| Stable transformation | [Stable package/date base SQL](/Users/eugenetsenter/Looker_clonedRepo/looker_personal/master_data_model/model/stable_base/create_master_stg_data_model.sql) | Amazon ad/date | Builds the `amazon_final` branch and a synthetic package ID. |
| Evidence output | [Master evidence model](https://console.cloud.google.com/bigquery?project=looker-studio-pro-452620&p=looker-studio-pro-452620&d=master_stg&t=data_model&page=table) | Amazon ad/date | Publishes final fields plus the full `amzn_*` evidence family. |
| Detail-preserving output | [Master evidence model v3](https://console.cloud.google.com/bigquery?project=looker-studio-pro-452620&p=looker-studio-pro-452620&d=master_stg&t=data_model_v3&page=table) | Source Amazon row/date | Carries Amazon rows as non-digital source actuals; it does not collapse them into a Prisma package. |

## Field Lineage

| Landing value | Stable-base field | Final meaning |
|---|---|---|
| `total_cost` | `final_spend` and `amzn_total_cost` | `_spend`, the Amazon final-cost measure, plus its source lineage. |
| `supply_cost` | `amzn_supply_cost` | Preserved source evidence; it no longer defines final spend. |
| `impressions`, `clicks` | `final_impressions`, `final_clicks` | `_impressions` and `_clicks`. |
| `starts_video_ad`, `complete_views_video_ad` | `final_video_plays`, `final_video_comps` | Video delivery fields. |
| `impressions_video_ad` | `social_video_views` | `_video_views`; Amazon preserves this separately from starts. |
| Campaign, ad group, ad, deal | synthetic package and placement keys | Lets Amazon rows live in the shared model without claiming a Prisma package ID. |
| Sales, purchases, units, branded searches, email-file metadata | matching `amzn_*` fields | Source evidence and commercial outcomes; they are not substituted into delivery metrics. |

## Custom Logic and Boundaries

- The branch accepts rows dated from `2025-01-01` onward and parses the Amazon text date format before filtering.
- It creates the package key from campaign and ad-group IDs; the placement key combines the ad ID with a deal ID, using `no_deal` when no deal exists.
- A row is flagged `missing_final_metrics` only when impressions, clicks, and both video measures are all absent.
- Amazon total cost is final spend. Supply cost, sales, and purchase fields stay in the `amzn_*` evidence family so reporting preserves the source definitions without treating outcomes as media delivery.
- The branch fixes the advertiser short name to `RTL`; the shared advertiser mapping then standardizes the user-facing advertiser label.
- Valid Manual Package Editor delivery overrides can replace final metrics later in the common model flow. That is a model-wide control, not an Amazon-specific source rewrite.

## Working Safely

Use the [stable base SQL](/Users/eugenetsenter/Looker_clonedRepo/looker_personal/master_data_model/model/stable_base/create_master_stg_data_model.sql) when changing Amazon field mapping or synthetic-key behavior. Use the [Master Data Model Pipeline v2](/Users/eugenetsenter/Looker_clonedRepo/looker_personal/master_data_model/README_v2.md) for the model-wide precedence and refresh path.

| Question | First place to check |
|---|---|
| Is the source fresh? | `amzn_loaded_at` and `qa_data_source_refresh_at` in the [Master evidence model](https://console.cloud.google.com/bigquery?project=looker-studio-pro-452620&p=looker-studio-pro-452620&d=master_stg&t=data_model&page=table). |
| Why is a value different from the Amazon report? | Compare the matching `amzn_*` evidence field to its final `_` field. |
| Why is a final metric blank? | Check the source parsing, then the model-wide manual-override evidence and `qa_data_issues`. |

## Related Guides

- [Source branch index](/Users/eugenetsenter/Looker_clonedRepo/looker_personal/master_data_model/model/branches/README.md)
- [Prisma planning pipeline](/Users/eugenetsenter/Looker_clonedRepo/looker_personal/master_data_model/model/branches/prisma/README_prisma-pipeline.md)
- [TV pipeline](/Users/eugenetsenter/Looker_clonedRepo/looker_personal/master_data_model/model/branches/tv/README_tv-pipeline.md)
