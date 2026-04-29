# Master Data Model Schema Audit

Created after a user correction on `2026-04-29`: the initial master data model was too curated and did not explicitly audit which source fields were excluded.

## What Went Wrong

The first version of `master_stg.data_model` did not include `p_package_friendly`.

Another agent added `p_package_friendly` to the local SQL first. The live view was later redeployed from that corrected file.

## Current State

`p_package_friendly` is now included in:

- `master_data_model/create_master_stg_data_model.sql`
- `looker-studio-pro-452620.master_stg.data_model`

Verified live schema position:

- `package_name`
- `p_package_friendly`
- `placement_id`

## Other Prisma Fields Not Carried Through Exactly

These fields exist in `20250327_data_model.prisma_expanded_full` but are not exact columns in `master_stg.data_model`.

High-priority candidates to add next:

- `planned_actions`
- `PlacementName`
- `n_of_placements`
- `planned_imps_pk`
- `planned_cost_pk`
- `total_days`
- `daily_spend`
- `daily_impressions`
- `initative`
- `line_item_name`
- `funnel`
- `channel_if_buy_category_custom_45`
- `media_type`
- `kpi`
- `package_group_name`
- `placement_type_site`
- `audience_type`
- `device_type`
- `audience`
- `ad_size_asset`
- `geo_market`

Lower-fill or currently low-value candidates:

- `placement_cap_cost`
- `placement_type`
- `served_by`
- `placement_comments`
- `days_in_out_of_home_end_date`
- `out_of_home_end_date`

Currently empty in the checked 2025+ package universe:

- `click_through_url`
- `dimension`
- `external_entity_id`
- `provider_name`
- `ad_server_placement_id`

## Renamed Or Transformed Fields

These source fields were not omitted outright; they were renamed or transformed:

- `start_date` -> `package_start_date`
- `end_date` -> `package_end_date`
- `planned_clicks` -> `prisma_planned_clicks`
- `report_date` -> `max_prisma_report_date` or `prisma_metadata_report_date` in intermediate SQL
- `package_id` -> `package_id_joined`

## Recommended Fix

The next SQL revision should add the high-priority Prisma metadata fields as pass-through columns for digital rows, with typed `NULL` placeholders for social rows.

Do not assume a curated schema is acceptable unless the project README explicitly says which source fields are intentionally excluded.
