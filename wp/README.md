# WP Delivery Workbook Shared Social Source

This workflow loads the [WP delivery workbook](https://docs.google.com/spreadsheets/d/1fen46Ugxx12PYRzCDT88z8ENVcVl_MjlQhqkGDxZNbc/edit?gid=982708559#gid=982708559) as the production Apollo source inside the shared-social delivery flow. The established shared-social source remains the field and row fallback for Apollo and remains unchanged for non-Apollo accounts.

## Current Status

| Area | Production behavior | Control / open item |
| --- | --- | --- |
| Sheet input | Reads `Report` for reporting values and hidden `Import` for supporting IDs. | Production staging refresh is controlled manually until source ownership is confirmed. |
| WP precedence | WP nonblank values, including numeric zeroes, win for matching Apollo campaign/ad rows; standard delivery fills missing fields and unmatched rows. | Non-Apollo standard rows are unchanged. |
| Channel rules | `_Search_` maps to Paid Search; `_YT_` maps to Online Video; otherwise the Sheet channel is retained as fallback. | Both markers remain excluded for QA review. |
| Record identity | Rows use date, platform, synthetic campaign ID, ad-group ID, and ad ID so differently named campaign records remain separate. | Synthetic key name-change risk remains visible in QA. |
| Pending source-owner clarification | Cross-campaign records sharing date/platform/ad ID are included with `publish_pending_source_owner_review`; exact duplicate campaign-grain records are excluded. | The pending status and master-model callout remain visible until clarified. |
| Dates | Uses `Report Start Date`; does not model placeholder `Flight Start Date` or `Flight End Date` values. | Correct only upstream if real flight fields are supplied. |

## Objects And Files

| Purpose | Location |
| --- | --- |
| Normalization rules | [wp_search_social_logic.R](/Users/eugenetsenter/Looker_clonedRepo/looker_personal/wp/wp_search_social_logic.R) |
| Sheet loader | [load_wp_search_data_template.R](/Users/eugenetsenter/Looker_clonedRepo/looker_personal/wp/load_wp_search_data_template.R) |
| Loader tests | [test_wp_search_social_logic.R](/Users/eugenetsenter/Looker_clonedRepo/looker_personal/wp/tests/test_wp_search_social_logic.R) |
| Production normalized input | [WP normalized staging](https://console.cloud.google.com/bigquery?project=looker-studio-pro-452620&p=looker-studio-pro-452620&d=repo_stg&t=stg__wp__search_data_template_daily&page=table) |
| Production shared-social output | [Shared cross-platform raw staging](https://console.cloud.google.com/bigquery?project=looker-studio-pro-452620&p=looker-studio-pro-452620&d=repo_stg&t=stg__olipop__crossplatform_raw_tbl&page=table) |
| Production master output | [Master data model](https://console.cloud.google.com/bigquery?project=looker-studio-pro-452620&p=looker-studio-pro-452620&d=master_stg&t=data_model&page=table) |
| Production scheduled builder SQL | [create_stg_crossplatform_wp_primary_production.sql](/Users/eugenetsenter/Looker_clonedRepo/looker_personal/wp/sql/create_stg_crossplatform_wp_primary_production.sql) |
| Shared-social rollback SQL | [rollback_stg_crossplatform_pre_wp_production.sql](/Users/eugenetsenter/Looker_clonedRepo/looker_personal/wp/sql/rollback_stg_crossplatform_pre_wp_production.sql) |
| QA normalized source input | [WP normalized staging QA](https://console.cloud.google.com/bigquery?project=looker-studio-pro-452620&p=looker-studio-pro-452620&d=repo_stg&t=stg__wp__search_data_template_daily_qa&page=table) |
| QA merged shared-social candidate | [WP-primary cross-platform QA](https://console.cloud.google.com/bigquery?project=looker-studio-pro-452620&p=looker-studio-pro-452620&d=repo_stg&t=stg__crossplatform_wp_primary_qa&page=table) |
| QA reporting-shaped social candidate | [WP-primary social reporting QA](https://console.cloud.google.com/bigquery?project=looker-studio-pro-452620&p=looker-studio-pro-452620&d=master_stg&t=data_model_social_wp_primary_qa&page=table) |
| Read-only validation queries | [validate_wp_primary_social_qa.sql](/Users/eugenetsenter/Looker_clonedRepo/looker_personal/wp/sql/validate_wp_primary_social_qa.sql) |

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

The daily shared-social scheduled query runs the production builder SQL already. The controlled loader command is required only when the source Sheet should be re-read into staging.

## Safe QA Run Order

Run from the repository root. These commands replace only QA objects.

```bash
Rscript wp/tests/test_wp_search_social_logic.R
WP_SEARCH_UPLOAD=TRUE Rscript wp/load_wp_search_data_template.R
bq query --project_id=looker-studio-pro-452620 --use_legacy_sql=false \
  < wp/sql/create_stg_crossplatform_wp_primary_qa.sql
bq query --project_id=looker-studio-pro-452620 --use_legacy_sql=false \
  < wp/sql/create_data_model_social_wp_primary_qa.sql
bq query --project_id=looker-studio-pro-452620 --use_legacy_sql=false \
  < wp/sql/validate_wp_primary_social_qa.sql
```

QA objects remain available for review and can be rebuilt without changing production. The deployed production behavior includes pending records until the source owner provides a different rule.
