# Digital Conversions Branch

This folder owns the direct Campaign Manager 360 (CM360) conversion path for Ritual. It preserves source activity records in BigQuery, updates only the newest valid rolling window, and joins aggregated conversion metrics to delivery at the lowest shared detail.

## Production path

| Step | Grain | Purpose |
| --- | --- | --- |
| [Direct CM360 history](https://console.cloud.google.com/bigquery?project=looker-studio-pro-452620&p=looker-studio-pro-452620&d=master_stg&t=rtl_cm360_direct_conversions&page=table) | Source activity | Retain every CM360 field, parsed package/placement IDs, source-export timestamp, and staging refresh date. |
| Direct conversion summary | Package + date + parsed placement ID + creative | Aggregate activities and revenue without multiplying delivery rows. |
| [V3 master model](https://console.cloud.google.com/bigquery?project=looker-studio-pro-452620&p=looker-studio-pro-452620&d=master_stg&t=data_model_v3&page=table) | Delivery detail plus conversion-only evidence | Attach direct `conv_*` fields only to a unique delivery match; retain unmatched conversions with null delivery metrics. |

The V3 builder does not read the legacy [RTL Sheet landing table](https://console.cloud.google.com/bigquery?project=looker-studio-pro-452620&p=looker-studio-pro-452620&d=landing&t=rtl_conv_report&page=table). That table remains comparison evidence only.

## Refresh scripts

| Script | When to use it | Safety boundary |
| --- | --- | --- |
| [Bootstrap direct history](bootstrap_rtl_cm360_direct_conversions.sql) | One-time approved historical seed or recovery rebuild | Uses the approved enriched backfill and current seed; it does not refresh a Google Sheet. |
| [Merge newest rolling export](merge_rtl_cm360_direct_conversions_latest.sql) | Routine refresh after a new enriched CM360 table lands | Accepts exactly one newest table with `package_roadblock` and `placement` fields and a report window no wider than 14 days. It updates matching source rows and retains older history. |
| [V3 builder](../../../final_model/create_master_stg_data_model_v3.sql) | Rebuild the production model after staging is current | Joins at package/date/placement/creative. Conversion-only records never inherit delivery metrics. |

## Published conversion fields

Direct CM360 preserves source advertiser, campaign, site, activity-group, activity, creative, placement, package/roadblock, conversions, and revenue. In V3, multi-valued activity and activity-group fields are arrays (`conv_activities`, `conv_activity_groups`) and the activity metrics are `conv_site_visits`, `conv_view_products`, `conv_add_to_carts`, `conv_begin_checkouts`, and `conv_purchases`.

Read the [direct CM360 migration guide](../../../../docs/rtl-direct-cm360-conversion-migration.md) before changing the source contract or join grain.
