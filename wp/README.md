# WP Delivery Workbook Shared Social Source

This folder documents and controls the WP delivery workbook workflow for Apollo shared-social reporting. The workflow reads the visible [WP delivery workbook](https://docs.google.com/spreadsheets/d/1fen46Ugxx12PYRzCDT88z8ENVcVl_MjlQhqkGDxZNbc/edit?gid=982708559#gid=982708559), normalizes each daily ad row, and lets WP values take precedence for Apollo records while the existing shared-social source continues to fill non-Apollo rows and missing metrics.

In plain English: WP is now the first source for Apollo social/search/video delivery details; the older shared-social feed remains the fallback and still owns everything outside that Apollo WP scope.

## Current Status

| Area | Current behavior | Control or open item |
| --- | --- | --- |
| Source workbook | Reads `Report` for visible delivery values and `Import_blend` for supporting ad-group IDs. | The workbook is still titled "APO Search Data Template" upstream. |
| Production staging | [WP normalized staging](https://console.cloud.google.com/bigquery?project=looker-studio-pro-452620&p=looker-studio-pro-452620&d=repo_stg&t=stg__wp__search_data_template_daily&page=table) and [shared cross-platform raw staging](https://console.cloud.google.com/bigquery?project=looker-studio-pro-452620&p=looker-studio-pro-452620&d=repo_stg&t=stg__olipop__crossplatform_raw_tbl&page=table) exist live. | Verified by live BigQuery schema inspection on 2026-06-05. |
| Final production model | [Master data model](https://console.cloud.google.com/bigquery?project=looker-studio-pro-452620&p=looker-studio-pro-452620&d=master_stg&t=data_model&page=table) exposes WP/social fields through the `s_` provenance columns and final `_` reporting fields. | The QA-only social final view can be rebuilt, but it is not currently live. |
| WP precedence | For matching Apollo campaign/ad rows, WP nonblank values win; numeric zeroes are treated as real values, not blanks. | Existing shared-social values fill fields WP does not provide, such as conversions and some video-percentile fields. |
| Channel rules | `_Search_` in campaign names maps to Paid Search; `_YT_` maps to Online Video; otherwise the Sheet channel is used as fallback. | Rows with both markers are excluded as ambiguous. |
| Agency campaign exclusion | Campaign names containing `1000heads`, case-insensitive, are removed in the shared-social production builder before the rows can feed the master model. | This is a source exclusion, not a dashboard filter or zeroed-out row. |
| Pending source-owner clarification | Cross-campaign records sharing date/platform/ad ID are included and flagged `publish_pending_source_owner_review`. | Keep the flag visible until the source owner confirms the correct ownership rule. |
| Dates | Uses `Report Start Date` as the reporting date. | Placeholder flight-date fields are intentionally ignored. |
| Authentication | Uses the consolidated Google login for Sheets and BigQuery first, with the prior cached package credentials retained as fallback. | The canonical runner loaded 24,992 rows successfully on 2026-08-10. |

## Folder Inventory

| File | Role | Keep or remove? | Why it stays |
| --- | --- | --- | --- |
| [README.md](/Users/eugenetsenter/Looker_clonedRepo/looker_personal/wp/README.md) | Runbook and field mapping | Keep | Main documentation for this workflow. |
| [load_wp_search_data_template.R](/Users/eugenetsenter/Looker_clonedRepo/looker_personal/wp/load_wp_search_data_template.R) | Loader | Keep | Reads Sheets, normalizes rows, and optionally uploads to BigQuery. |
| [wp_search_social_logic.R](/Users/eugenetsenter/Looker_clonedRepo/looker_personal/wp/wp_search_social_logic.R) | Business rules | Keep | Holds pure normalization, classification, and precedence logic. |
| [test_wp_search_social_logic.R](/Users/eugenetsenter/Looker_clonedRepo/looker_personal/wp/tests/test_wp_search_social_logic.R) | Tests | Keep | Proves the rules without touching Sheets or BigQuery. |
| [create_stg_crossplatform_wp_primary_production.sql](/Users/eugenetsenter/Looker_clonedRepo/looker_personal/wp/sql/create_stg_crossplatform_wp_primary_production.sql) | Production builder | Keep | Builds the live shared-social staging table with WP precedence and the `1000heads` campaign exclusion. |
| [rollback_stg_crossplatform_pre_wp_production.sql](/Users/eugenetsenter/Looker_clonedRepo/looker_personal/wp/sql/rollback_stg_crossplatform_pre_wp_production.sql) | Rollback builder | Keep | Restores the pre-WP shared-social logic if a rollback is approved. |
| [create_stg_crossplatform_wp_primary_qa.sql](/Users/eugenetsenter/Looker_clonedRepo/looker_personal/wp/sql/create_stg_crossplatform_wp_primary_qa.sql) | QA builder | Keep | Rebuilds the WP-first shared-social candidate without touching production, using the maintained WP production staging input. |
| [create_data_model_social_wp_primary_qa.sql](/Users/eugenetsenter/Looker_clonedRepo/looker_personal/wp/sql/create_data_model_social_wp_primary_qa.sql) | QA final-shape builder | Keep | Rebuilds a social-only final-data candidate for review. |
| [validate_wp_primary_social_qa.sql](/Users/eugenetsenter/Looker_clonedRepo/looker_personal/wp/sql/validate_wp_primary_social_qa.sql) | Read-only QA checks | Keep | Checks classification, overlap, new rows, and protected non-Apollo scopes. |

Cleanup note: the only unnecessary file found during cleanup was `.DS_Store`, a local macOS Finder metadata file. It was removed. No tracked workflow file was removed because each remaining file is either active code, active documentation, a test, a QA proof step, or rollback protection.

## Final Data Field Mapping

This matrix shows how workbook values move into staging and then into final reporting fields. A field name starting with `_` is a final reporting field. A field name starting with `s_` is a final social-source provenance field: it helps explain where the final value came from.

| Source value | WP normalized staging | Shared-social production staging | Final master data field | Rule |
| --- | --- | --- | --- | --- |
| `Report Start Date` | `date_day` | `date_day` | `_date` | The visible report date becomes the reporting date. Flight-date placeholders are ignored. |
| `Platform` | `platform`, `source_relation` | `platform`, `source_relation` | `s_platform`, plus `_supplier_code` and `_supplier_name` | Platform text is normalized, for example Google becomes `google_ads` and LinkedIn becomes `linkedin_ads`. |
| `Client` | `account_name`, `account_id` | `account_name`, `account_id` | `_advertiser`, `s_account_name` | Apollo workbook rows are normalized to Apollo reporting identity downstream. |
| `Campaign` | `campaign_name`, synthetic `campaign_id` | `campaign_name`, `campaign_id` | `_campaign_name`, `_package_id` | WP campaign names are preserved unless they contain `1000heads`, which is excluded before shared-social staging is written. Campaign IDs are generated when the workbook does not provide source campaign IDs. |
| `Ad Group` | `ad_group_name`, `ad_group_id` | `ad_group_name`, `ad_group_id` | `_package_name`, `_package_id`, `s_ad_group_id` | The `Import_blend` tab ad-group ID is used when available; otherwise a readable synthetic ID is generated. |
| `Ad ID` | `ad_id`, `wp_row_key` | `ad_id`, `wp_row_key` | `_placement_id`, `s_ad_id`, `s_wp_row_key` | Ad ID stays text so large IDs are not rounded. `wp_row_key` preserves the campaign/ad-group/ad grain. |
| `Ad Name` | `ad_name` | `ad_name` | `_placement_name`, `s_creative_name` | Ad name becomes the final placement name and social creative label. |
| `Channel` and campaign markers | `channel`, `channel_group`, `media_name`, `ADIF_channel`, `wp_classification_source` | `wp_channel`, `wp_channel_group`, `wp_media_name`, `wp_ADIF_channel`, `wp_classification_source` | `_channel`, `_channel_group`, `_media_name`, `ADIF_channel`, `s_channel_classification_source` | Campaign markers win first: `_Search_` means Paid Search and `_YT_` means Online Video. If no marker exists, the Sheet channel is fallback. |
| `Spend` | `spend` | `spend` | `_spend`, `s_spend` | WP value wins when present, including zero. Shared-social spend fills only when WP is missing. |
| `Impressions` | `impressions` | `impressions` | `_impressions`, `s_impressions` | WP value wins when present, including zero. |
| `Clicks` | `clicks` | `clicks` | `_clicks`, `s_clicks` | WP value wins when present, including zero. |
| `Video Views` | `video_view` | `video_view` | `_video_views`, `s_video_views` | WP video views win when present. Other video metrics can still come from shared-social fallback. |
| Not supplied by WP | `conversions`, `conversions_value`, `video_play`, percentile video fields, `hookrate_num` are blank in WP staging | Same field names, filled from shared-social when available | `_video_plays`, `_video_comps`, and related source fields | These fields are fallback-owned because the workbook does not supply them. |
| `Creative Name` | `wp_creative_name` | `wp_creative_name` | `s_creative_name` when used downstream | Kept as source evidence for creative review. |
| `Creative Box Link` | `wp_creative_img` | `wp_creative_img` | `_creative_img`, `man_creative_img` where mapped downstream | Supported YouTube links are converted to thumbnail URLs. Non-YouTube or blank values stay missing. |
| Publication decision | `wp_publication_status` | `wp_publication_status`, `wp_record_source` | `s_publication_status`, `s_record_source` | Normal rows publish. Ambiguous rows are excluded. Cross-campaign ad-ID conflicts publish but stay flagged for source-owner review. |
| Source evidence | `wp_source_sheet_url`, `wp_loaded_at` | `wp_source_sheet_url`, `wp_loaded_at`, `wp_fallback_fields` | `s_source_sheet_url`, `s_loaded_at`, `s_fallback_fields` | These fields explain which source was used and which fallback fields were needed. |

## Live Objects

| Object | Purpose | Current note |
| --- | --- | --- |
| [WP normalized staging](https://console.cloud.google.com/bigquery?project=looker-studio-pro-452620&p=looker-studio-pro-452620&d=repo_stg&t=stg__wp__search_data_template_daily&page=table) | Production daily-ad input from the workbook. | Live table verified on 2026-06-05. |
| [Shared cross-platform raw staging](https://console.cloud.google.com/bigquery?project=looker-studio-pro-452620&p=looker-studio-pro-452620&d=repo_stg&t=stg__olipop__crossplatform_raw_tbl&page=table) | Production shared-social staging with WP-first Apollo behavior. | Excludes campaign names containing `1000heads` at source before the master model reads social rows. |
| [Master data model](https://console.cloud.google.com/bigquery?project=looker-studio-pro-452620&p=looker-studio-pro-452620&d=master_stg&t=data_model&page=table) | Final reporting model. | Live view verified on 2026-06-05. |
| [WP-primary cross-platform QA](https://console.cloud.google.com/bigquery?project=looker-studio-pro-452620&p=looker-studio-pro-452620&d=repo_stg&t=stg__crossplatform_wp_primary_qa&page=table) | Rebuildable QA merge candidate. | Created only when the QA SQL command runs. |
| [WP-primary social reporting QA](https://console.cloud.google.com/bigquery?project=looker-studio-pro-452620&p=looker-studio-pro-452620&d=master_stg&t=data_model_social_wp_primary_qa&page=table) | Rebuildable final-shape social QA view. | Not live during the 2026-06-05 cleanup check; rebuild before using. |

## Controlled Production Refresh

The production staging refresh is intentionally manual while the pending source-owner question remains open. Run from the repository root after reviewing the Sheet:

```bash
WP_SEARCH_TABLE=stg__wp__search_data_template_daily \
WP_SEARCH_UPLOAD=TRUE \
WP_SEARCH_ALLOW_PRODUCTION=TRUE \
Rscript wp/load_wp_search_data_template.R

bq query --project_id=looker-studio-pro-452620 --use_legacy_sql=false \
  < wp/sql/create_stg_crossplatform_wp_primary_production.sql

bq query --project_id=looker-studio-pro-452620 --use_legacy_sql=false \
  < master_data_model/create_master_stg_data_model.sql
```

The daily shared-social scheduled query already runs the production builder SQL. The controlled loader command is required only when the source Sheet should be re-read into staging. After changing this SQL, update the saved scheduled-query config too; BigQuery does not automatically copy local file edits into the scheduled query.

## Safe QA Run Order

Run from the repository root. These commands replace only QA objects.

```bash
Rscript wp/tests/test_wp_search_social_logic.R

bq query --project_id=looker-studio-pro-452620 --use_legacy_sql=false \
  < wp/sql/create_stg_crossplatform_wp_primary_qa.sql

bq query --project_id=looker-studio-pro-452620 --use_legacy_sql=false \
  < wp/sql/create_data_model_social_wp_primary_qa.sql

bq query --project_id=looker-studio-pro-452620 --use_legacy_sql=false \
  < wp/sql/validate_wp_primary_social_qa.sql
```

QA objects remain rebuildable for review and can be deleted after the source-owner rule is resolved. Production behavior includes pending records until the source owner provides a different rule.

## Terms

`QA` means a test or review copy that should not affect production reporting. `Fallback` means the older shared-social source fills a field only when WP does not provide that value. `Provenance` means evidence fields that explain where a final value came from and why it was used.
