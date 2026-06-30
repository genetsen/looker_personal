# Full Source Field Audit For `master_stg.data_model`

Created on `2026-04-29` after the user correctly flagged missing source fields.

## Bottom Line

The current live view is a curated model, not a full source-field-preserving model. That was not called out clearly enough before deployment.

Live output checked:

- `looker-studio-pro-452620.master_stg.data_model`

Inputs checked:

- `looker-studio-pro-452620.20250327_data_model.prisma_expanded_full`
- `looker-studio-pro-452620.DCM.20250505_costModel_v5`
- `looker-studio-pro-452620.landing.fpd_data_ranged_shortcutsFolder`
- `looker-studio-pro-452620.landing.adif_updated_fpd_daily`
- `looker-studio-pro-452620.repo_stg.stg__olipop__crossplatform_raw_tbl`
- `looker-studio-pro-452620.repo_int.crossplatform_pacing`

## Exact-Name Coverage Summary

| Source | Source columns | Exact names present | Exact names missing |
|---|---:|---:|---:|
| DCM cost model | 51 | 5 | 46 |
| Original FPD shortcuts table | 52 | 3 | 49 |
| Updated FPD daily table | 15 | 3 | 12 |
| Prisma expanded full | 67 | 29 | 38 |
| Social pacing | 16 | 1 | 15 |
| Social raw | 24 | 1 | 23 |

Important: exact-name coverage is conservative. Some fields are represented as renamed, aggregated, or derived fields today, but many source fields are not preserved at all.

## Missing Exact Columns By Source

### Prisma

Missing exact output columns:

- `placement_type`
- `start_date`
- `end_date`
- `click_through_url`
- `planned_actions`
- `planned_clicks`
- `dimension`
- `external_entity_id`
- `provider_name`
- `package_group_name`
- `out_of_home_end_date`
- `placement_cap_cost`
- `served_by`
- `channel_if_buy_category_custom_45`
- `placement_type_site`
- `device_type`
- `audience_type`
- `audience`
- `initative`
- `media_type`
- `line_item_name`
- `ad_size_asset`
- `funnel`
- `kpi`
- `geo_market`
- `ad_server_placement_id`
- `placement_comments`
- `days_in_out_of_home_end_date`
- `package_id`
- `PlacementName`
- `report_date`
- `script_run_date`
- `n_of_placements`
- `planned_imps_pk`
- `planned_cost_pk`
- `total_days`
- `daily_spend`
- `daily_impressions`

Renamed or derived today:

- `package_id` -> `package_id_joined`
- `start_date` -> `package_start_date`
- `end_date` -> `package_end_date`
- `planned_clicks` -> `prisma_planned_clicks`
- `report_date` -> `max_prisma_report_date`

User correction for the next revision:

- Do not rename `start_date` and `end_date`.
- Use `COALESCE(end_date, out_of_home_end_date)` for `end_date`, so normal Prisma `end_date` wins and OOH end date is only the fallback.

### DCM Cost Model

Missing exact output columns:

- `advertiser`
- `campaign`
- `package_roadblock`
- `package_id`
- `impressions`
- `placement`
- `site`
- `KEY`
- `ad`
- `click_rate`
- `clicks`
- `creative`
- `media_cost`
- `rich_media_video_completions`
- `rich_media_video_plays`
- `total_conversions`
- `p_cost_method`
- `p_start_date`
- `p_end_date`
- `p_total_days`
- `p_pkg_daily_planned_cost`
- `p_pkg_total_planned_cost`
- `p_pkg_daily_planned_imps`
- `p_pkg_total_planned_imps`
- `p_channel_group`
- `p_advertiser_name`
- `flight_date_flag`
- `flight_status_flag`
- `rate_raw`
- `n_of_placements`
- `min_flight_date`
- `max_flight_date`
- `pkg_total_imps`
- `total_inflight_impressions`
- `pkg_daily_imps`
- `pkg_daily_imps_perc`
- `pkg_total_imps_perc`
- `pkg_inflight_imps_perc`
- `days_live`
- `prorated_planned_cost_pk`
- `prorated_planned_imps_pk`
- `cpm_overdelivery_flag`
- `daily_cpm`
- `daily_recalculated_cost`
- `daily_recalculated_cost_flag`
- `daily_recalculated_imps`

Partially represented today:

- DCM delivery metrics are aggregated into `d_*` fields such as `d_impressions`, `d_clicks`, `d_media_cost`, `d_video_plays`, `d_video_comps`, `d_daily_recalculated_cost`, and `d_daily_recalculated_imps`.

Not preserved today:

- Raw DCM placement/ad/creative rows and many DCM pacing/cost-model helper fields.

### Original FPD

Missing exact output columns:

- `client`
- `site`
- `campaign`
- `prisma_start_date`
- `prisma_end_date`
- `package_id`
- `week`
- `partner_packagePlacement_name`
- `start_date`
- `end_date`
- `partner_creative_name`
- `spend`
- `impressions`
- `clicks`
- `source_file`
- `source_url`
- `last_modified_time`
- `last_modified_by`
- `partner_sheet`
- `opens`
- `sends`
- `partner_placement_name`
- `benchmark`
- `partner_creative_name__1`
- `partner_creative_name__2`
- `final_img_path`
- `creative_git_link`
- `ctr`
- `geo_type_dma_zip_state_county_national_city`
- `factor`
- `month`
- `benchmark_metric`
- `engagements`
- `views`
- `completed_views`
- `pageviews`
- `audience`
- `geo`
- `ctr_vcr`
- `dma`
- `start_date_final`
- `start_date_source`
- `end_date_final`
- `end_date_source`
- `date_final`
- `data_update_datetime`
- `cpm`
- `creative_git_last_final_img_path`
- `creative_git_last_box_link`

Partially represented today:

- Original FPD delivery metrics are aggregated into `fpd_orig_*` fields.
- Source file and URL are combined into `fpd_orig_source_files` and `fpd_orig_source_urls`.
- Source modified time is represented as `fpd_orig_source_modified_time`.

Not preserved today:

- Many raw partner-sheet fields, creative link fields, geo/audience fields, and date-source fields.

### Updated FPD

Missing exact output columns:

- `data_update_datetime`
- `data_source`
- `total_package_spend`
- `initiative`
- `total_package_impressions`
- `prisma_end_date`
- `daily_fpd_spend`
- `prisma_start_date`
- `daily_fpd_impressions`
- `days_in_package`
- `source_sheet_modified_time`
- `package_id`

Partially represented today:

- Updated FPD metrics are represented as `fpd_updated_impressions`, `fpd_updated_spend`, `fpd_updated_data_timestamp`, `fpd_updated_source_sheet_modified_time`, `fpd_updated_suppliers`, and `fpd_updated_initiatives`.

### Social Raw

Missing exact output columns:

- `source_relation`
- `date_day`
- `platform`
- `account_id`
- `account_name`
- `campaign_id`
- `ad_group_id`
- `ad_group_name`
- `ad_id`
- `ad_name`
- `clicks`
- `impressions`
- `spend`
- `conversions`
- `conversions_value`
- `video_play`
- `video_view`
- `video_views_p_25`
- `video_views_p_50`
- `video_views_p_75`
- `video_views_p_100`
- `hookrate_num`
- `video_flag`

Partially represented today:

- Social fields are renamed into `social_*` fields and final metrics, such as `social_campaign_id`, `social_ad_group_id`, `social_ad_id`, `social_spend`, `social_impressions`, `social_clicks`, `final_spend`, and `final_impressions`.

Not preserved today:

- Raw platform/source identifiers, conversion fields, detailed video quartile fields, and video flag.

### Social Pacing

Missing exact output columns:

- `a_id`
- `ad_name`
- `ag_id`
- `adgroup_name`
- `ag_start_date`
- `ag_end_date`
- `ag_budget`
- `c_id`
- `c_start_date`
- `c_end_date`
- `start_date`
- `end_date`
- `c_budget`
- `final_budget`
- `platform`

Partially represented today:

- Pacing is transformed into `social_pacing_planned_spend` and `planned_daily_spend_pk`.

Not preserved today:

- Raw pacing IDs, budget columns, and campaign/adgroup date fields.

## Fields The User Explicitly Flagged

- `p_package_friendly`: now present after a separate local fix and redeploy.
- `gsMediaTeam_channel`: missing from the current live view at audit time. A local SQL change has been started but should not be treated as complete until dry-run, deploy, and live schema verification pass.
- `start_date`: missing as an exact live output column.
- `end_date`: missing as an exact live output column.

## Required Next Design Decision

There are two viable ways to fix this correctly:

1. Wide top-level preservation: add source-prefixed top-level fields for every source column where names collide, plus exact Prisma `start_date` / `end_date`.
2. Nested source preservation: keep the analytic top-level fields and add source structs or source row arrays, such as `prisma_source`, `dcm_source_rows`, `fpd_original_source_rows`, `fpd_updated_source_rows`, `social_raw_source_rows`, and `social_pacing_source_rows`.

For DCM, original FPD, and social raw, some source tables have multiple rows per package/date. A fully faithful package/date model cannot put every raw column at top level without choosing aggregation rules. To avoid silent data loss, raw multi-row inputs should either be preserved as arrays of structs or explicitly aggregated with approved rules.
