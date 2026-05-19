# ADIF vs Master Missing Information Audit

Checked on 2026-05-07 against live BigQuery.

## Live Objects Checked

- Main ADIF: [`looker-studio-pro-452620.repo_stg.adif__mainDataTable_notebook_v2_test`](https://console.cloud.google.com/bigquery?project=looker-studio-pro-452620&p=looker-studio-pro-452620&d=repo_stg&t=adif__mainDataTable_notebook_v2_test&page=table)
- Master v1: [`looker-studio-pro-452620.master_stg.data_model`](https://console.cloud.google.com/bigquery?project=looker-studio-pro-452620&p=looker-studio-pro-452620&d=master_stg&t=data_model&page=table)
- Master v2: [`looker-studio-pro-452620.master_stg.data_model_v2`](https://console.cloud.google.com/bigquery?project=looker-studio-pro-452620&p=looker-studio-pro-452620&d=master_stg&t=data_model_v2&page=table)

## Bottom Line

The master models are not missing 122 concepts. They are missing 122 exact ADIF column names.

After comparing the live schemas and the local model SQL:

- Many ADIF fields are intentionally renamed into cleaner master fields.
- Some package/date facts are present but labeled as non-summable context fields.
- Some ADIF source/helper fields are genuinely not preserved.
- Some missing exact fields currently have zero live values, so adding them would not add usable data today.

The biggest real gaps are Prisma package metadata, planning helper fields, a few DCM helper fields, source-refresh timestamps, and ADIF-specific final/rollup helper columns.

## Count Summary

| Check | Result |
|---|---:|
| Main ADIF columns | 147 |
| Master v1 columns | 119 |
| Master v2 columns | 120 |
| ADIF exact column names missing from both master v1 and v2 | 122 |
| ADIF exact column names missing from v1 only | 1: `initiative` |
| ADIF exact column names present in both | 24 |

## Not Actually Missing, Mostly Renamed

These ADIF fields are not present by exact name, but the same concept is present under a master name.

| ADIF field | Master field |
|---|---|
| `package_id_joined`, `package_id` | `_package_id` |
| `date` | `_date` |
| `start_date` | `_start_date` |
| `end_date` | `_end_date` |
| `advertiser_name` | `_advertiser_name`; also normalized into `_advertiser` |
| `campaign_name` | `_campaign_name` |
| `campaign_friendly` | `_campaign_friendly` |
| `product_code` | `_product_code` |
| `product_name` | `_product_name` |
| `package_type` | `_package_type` |
| `package_name` | `_package_name` |
| `p_package_friendly` | `_package_name_friendly` |
| `placement_id` | `_placement_id` |
| `placement_name` | `_placement_name` |
| `supplier_code` | `_supplier_code` |
| `supplier_name` | `_supplier_name` |
| `supplier_logo` | `_supplier_logo` |
| `buy_type` | `p_buy_type` |
| `buy_category` | `p_buy_category` |
| `channel` | `_channel` |
| `channel_raw` | `qa_channel_raw` |
| `channel_group` | `_channel_group` |
| `media_name` | `_media_name` |
| `cost_method` | `p_cost_method` |
| `planned_amount` | `p_planned_amount_doNotSum` |
| `planned_impressions` | `p_planned_impressions_doNotSum` |
| `planned_units` | `p_planned_units_doNotSum` |
| `unit_type` | `p_unit_type` |
| `payable_rate` | `p_rate` |
| `planned_daily_spend_pk` | `_planned_spend` |
| `planned_daily_impressions_pk` | `_planned_impressions` |
| `planned_clicks`, `prisma_planned_clicks` | `p_planned_clicks` |
| `max_prismaEXP_report_date`, `report_date` | `p_max_report_date` |
| `gsMediaTeam_channel` | `ADIF_channel` |
| `final_spend`, `final_spend_new` | `_spend` |
| `final_impressions`, `final_impressions_new` | `_impressions` |
| `final_clicks` | `_clicks` |
| `final_video_plays` | `_video_plays` |
| `final_video_comps` | `_video_comps` |
| `pkg_est_spend` | `qa_pkg_est_spend_doNotSum` |
| `pkg_est_imps` | `qa_pkg_est_impressions_doNotSum` |
| `pkg_act_spend` | `qa_pkg_act_spend_doNotSum` |
| `pkg_act_imps` | `qa_pkg_act_impressions_doNotSum` |
| `pkg_fpd_orig_impressions` | `qa_pkg_fpd_orig_impressions_doNotSum` |
| `pkg_fpd_orig_spend` | `qa_pkg_fpd_orig_spend_doNotSum` |
| `pkg_fpd_updated_impressions` | `qa_pkg_fpd_updated_impressions_doNotSum` |
| `pkg_fpd_updated_spend` | `qa_pkg_fpd_updated_spend_doNotSum` |
| `pkg_fpd_combined_impressions` | `qa_pkg_fpd_combined_impressions_doNotSum` |
| `pkg_fpd_combined_spend` | `qa_pkg_fpd_combined_spend_doNotSum` |
| `pkg_over_bool` | `qa_pkg_over_bool` |
| `pkg_over_flag` | `qa_pkg_over_flag` |
| `row_data_source_primary`, `data_source_primary` | `qa_row_data_source_primary` |

## Genuinely Missing Or Partially Missing Information

### 1. Prisma Package Planning And Classification Detail

These are populated in main ADIF and are not carried through as top-level master fields.

| Field | Live non-null rows in main ADIF | Why it matters |
|---|---:|---|
| `planned_actions` | 19,121 / 20,357 | Planned action KPI volume, separate from clicks/impressions. |
| `package_group_name` | 16,385 / 20,357 | Package grouping label from Prisma. |
| `channel_if_buy_category_custom_45` | 18,189 / 20,357 | Planning channel helper. Master has channel fields, but this exact helper is not preserved. |
| `placement_type_site` | 9,057 / 20,357 | Site/placement custom classification. |
| `device_type` | 3,127 / 20,357 | Device targeting detail. |
| `audience_type` | 2,967 / 20,357 | Audience classification. |
| `audience` | 1,416 / 20,357 | Audience label. |
| `media_type` | 15,358 / 20,357 | Creative/media classification. |
| `line_item_name` | 18,865 / 20,357 | Line-item label. |
| `ad_size_asset` | 4,779 / 20,357 | Asset/ad-size descriptor. |
| `funnel` | 18,856 / 20,357 | Funnel stage. |
| `kpi` | 13,984 / 20,357 | KPI label. |
| `geo_market` | 7,805 / 20,357 | Market/geo label. |
| `n_of_placements` | 20,357 / 20,357 | Placement count context. |
| `total_days` | 19,121 / 20,357 | Package flight duration. |
| `daily_spend` | 19,121 / 20,357 | ADIF daily planned spend helper. |
| `daily_impressions` | 19,121 / 20,357 | ADIF daily planned impressions helper. |

Master has several planning replacements, especially `_planned_spend`, `_planned_impressions`, `p_planned_amount_doNotSum`, `p_planned_impressions_doNotSum`, and `p_planned_clicks`. It does not preserve the full ADIF planning context above.

### 2. DCM Helper Fields

Master v2 preserves the main DCM delivery metrics under `dcm_*` names:

- `dcm_daily_recalculated_cost`
- `dcm_daily_recalculated_imps`
- `dcm_impressions`
- `dcm_media_cost`
- `dcm_clicks`
- `dcm_video_plays`
- `dcm_video_comps`
- `dcm_min_date`
- `dcm_max_date`
- `dcm_min_flight_date`
- `dcm_max_flight_date`
- `dcm_daily_cpm`
- `dcm_total_delivered_imps`
- `dcm_total_del_inflight_imps`

But these ADIF DCM helper fields are not currently preserved:

- `d_prorated_planned_cost_pk`
- `d_prorated_planned_imps_pk`

The model also does not preserve lower-grain raw DCM fields like placement/ad/creative rows. That is a grain issue: the master model is package/date shaped, while raw DCM can be more detailed.

### 3. FPD Rollups, Benchmarks, Sends, Opens, And Creative

Master v2 carries several FPD fields directly:

- `fpd_orig_impressions`
- `fpd_orig_spend`
- `fpd_orig_clicks`
- `fpd_orig_sends`
- `fpd_orig_opens`
- `fpd_orig_benchmark`
- `fpd_orig_benchmark_metric`
- `fpd_orig_creative`
- `fpd_updated_impressions`
- `fpd_updated_spend`
- `fpd_updated_suppliers`
- `fpd_updated_initiatives`
- `fpd_updated_data_timestamp`
- `fpd_updated_source_sheet_modified_time`
- `fpd_impressions`
- `fpd_spend`
- `fpd_clicks`
- `fpd_sends`
- `fpd_opens`

Main ADIF also has these combined or older-name fields that master v2 does not preserve by exact name:

| Field | Live non-null rows in main ADIF | Notes |
|---|---:|---|
| `final_sends` | 1,921 / 20,357 | Email/send metric. Master has `fpd_sends`, but no final send metric. |
| `final_opens` | 1,921 / 20,357 | Email/open metric. Master has `fpd_opens`, but no final open metric. |
| `fpd_benchmark` | 1,921 / 20,357 | Combined/old benchmark naming. Master has `fpd_orig_benchmark`. |
| `fpd_benchmark_metric` | not separately exact-counted in this run | Combined/old benchmark metric naming. Master has `fpd_orig_benchmark_metric`. |
| `fpd_creative` | 683 / 20,357 | Combined/old creative field. Master has `fpd_orig_creative`; v2 also has an empty `fpd_creative_img`. |
| `fpd_source_sheet_modified_date` | 4,083 / 20,357 | ADIF source freshness date. Master has source modified timestamps, but not this exact date field. |

### 4. ADIF Final Metrics And Package Rollup Helper Fields

Master intentionally renames final metrics:

- ADIF `final_spend` -> master `_spend`
- ADIF `final_impressions` -> master `_impressions`
- ADIF `final_clicks` -> master `_clicks`
- ADIF `final_video_plays` -> master `_video_plays`
- ADIF `final_video_comps` -> master `_video_comps`

But ADIF also has helper variants and package rollups that are not preserved by exact name:

- `final_spend_new`
- `final_impressions_new`
- `pkg_act_imps`
- `pkg_act_spend`
- `prisma_planned_spend`
- `prisma_planned_impressions`
- `_est_spend`
- `_est_impressions`

Some of this information is represented under master QA fields, but the old ADIF helper names and exact calculation variants are not preserved.

### 5. Refresh And Source-State Fields

Main ADIF has:

| Field | Live non-null rows in main ADIF | Master status |
|---|---:|---|
| `final_table_refresh_date` | 19,121 / 20,357 | Not carried through. |
| `fpd_source_sheet_modified_date` | 4,083 / 20,357 | Partially represented by source modified timestamps, but not exact date. |
| `script_run_date` | filled in main ADIF sample | Not carried through as a final master field. |

Master has `qa_model_view_runtime_timestamp`, but that is not the same thing as the ADIF final-table refresh timestamp.

### 6. Low-Fill Or Currently Empty Fields

These are missing by exact name, but adding them would add little or no live data right now.

| Field | Live non-null rows in main ADIF |
|---|---:|
| `click_through_url` | 0 / 20,357 |
| `dimension` | 0 / 20,357 |
| `external_entity_id` | 0 / 20,357 |
| `provider_name` | 0 / 20,357 |
| `out_of_home_end_date` | 0 / 20,357 |
| `placement_cap_cost` | 0 / 20,357 |
| `ad_server_placement_id` | 0 / 20,357 |
| `placement_comments` | low fill in MCP sample: about 0.7% |

## Present In Both By Exact Name

These ADIF fields are already present in both master v1 and master v2:

- `fpd_orig_impressions`
- `fpd_orig_spend`
- `fpd_orig_clicks`
- `fpd_orig_sends`
- `fpd_orig_opens`
- `fpd_orig_benchmark`
- `fpd_orig_benchmark_metric`
- `fpd_orig_creative`
- `fpd_updated_impressions`
- `fpd_updated_spend`
- `fpd_updated_suppliers`
- `fpd_updated_initiatives`
- `fpd_updated_data_timestamp`
- `fpd_impressions`
- `fpd_spend`
- `fpd_clicks`
- `fpd_sends`
- `fpd_opens`
- `s_spend`
- `s_impressions`
- `s_clicks`
- `s_video_plays`
- `s_video_comps`
- `s_video_views`

## V2 Improvement Over V1

`initiative` is missing from master v1 but present in master v2.

Master v2 derives it from Prisma `initative` and falls back to `_package_name`.

## Priority Recommendation

If the goal is to make master v2 preserve the useful information from main ADIF, the next revision should add source-preserving fields for the populated Prisma/package metadata first:

1. `planned_actions`
2. `package_group_name`
3. `channel_if_buy_category_custom_45`
4. `placement_type_site`
5. `device_type`
6. `audience_type`
7. `audience`
8. `media_type`
9. `line_item_name`
10. `ad_size_asset`
11. `funnel`
12. `kpi`
13. `geo_market`
14. `n_of_placements`
15. `total_days`
16. `daily_spend`
17. `daily_impressions`
18. `final_table_refresh_date`

After that, decide whether to preserve ADIF helper/legacy names as compatibility aliases or keep only the master canonical names.

## Modeling Decision Needed Before Changing SQL

Some missing information lives below the package/date grain. For those fields, we should not silently aggregate or pick one value.

The clean options are:

1. Add package/date-safe Prisma fields directly to `data_model_v2`.
2. Add compatibility aliases for renamed ADIF fields.
3. Create or extend a detail model for lower-grain DCM/FPD creative and placement/ad detail.
4. Add nested source structs or arrays if we want full source preservation without duplicating package/date rows.

The safest first move is option 1 plus selected option 2 aliases.
