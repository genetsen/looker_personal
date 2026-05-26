# Apollo Search Data Template Social QA

This workflow loads the [APO Search Data Template](https://docs.google.com/spreadsheets/d/1fen46Ugxx12PYRzCDT88z8ENVcVl_MjlQhqkGDxZNbc/edit?gid=982708559#gid=982708559) into a QA-only Apollo-first shared-social candidate. It exists to review source precedence and reporting-channel behavior before any production shared staging or master-model replacement.

## Current Status

| Area | QA behavior | Production impact |
| --- | --- | --- |
| Sheet input | Reads `Report` for reporting values and hidden `Import` for supporting IDs. | None. |
| Apollo precedence | Publishable APO values win for Apollo daily-ad matches; the current shared social source fills absent rows or fields. | Not promoted. |
| Channel rules | `_Search_` maps to Paid Search; `_YT_` maps to Online Video; otherwise the sheet channel is retained as fallback. | QA view only. |
| Conflicting daily-ad keys | Source rows sharing the same Apollo `date_day`, `platform`, and `ad_id` are retained with `exclude_duplicate_daily_ad_key` and held out of the merged candidate until a resolution is approved. | Prevents unresolved conflicts from affecting QA candidate totals. |
| Dates | Uses `Report Start Date`; does not model placeholder `Flight Start Date` or `Flight End Date` values. | None. |

## Objects And Files

| Purpose | Location |
| --- | --- |
| Normalization rules | [apo_search_social_logic.R](/Users/eugenetsenter/Looker_clonedRepo/looker_personal/apollo/apo_search_social_logic.R) |
| Sheet loader | [load_apo_search_data_template.R](/Users/eugenetsenter/Looker_clonedRepo/looker_personal/apollo/load_apo_search_data_template.R) |
| Loader tests | [test_apo_search_social_logic.R](/Users/eugenetsenter/Looker_clonedRepo/looker_personal/apollo/tests/test_apo_search_social_logic.R) |
| QA normalized source input | [APO normalized staging QA](https://console.cloud.google.com/bigquery?project=looker-studio-pro-452620&p=looker-studio-pro-452620&d=repo_stg&t=stg__apo__search_data_template_daily_qa&page=table) |
| QA merged shared-social candidate | [APO-primary cross-platform QA](https://console.cloud.google.com/bigquery?project=looker-studio-pro-452620&p=looker-studio-pro-452620&d=repo_stg&t=stg__crossplatform_apo_primary_qa&page=table) |
| QA reporting-shaped social candidate | [APO-primary social reporting QA](https://console.cloud.google.com/bigquery?project=looker-studio-pro-452620&p=looker-studio-pro-452620&d=master_stg&t=data_model_social_apo_primary_qa&page=table) |
| Read-only validation queries | [validate_apo_primary_social_qa.sql](/Users/eugenetsenter/Looker_clonedRepo/looker_personal/apollo/sql/validate_apo_primary_social_qa.sql) |

## Safe QA Run Order

Run from the repository root. These commands replace only QA objects.

```bash
Rscript apollo/tests/test_apo_search_social_logic.R
APO_SEARCH_UPLOAD=TRUE Rscript apollo/load_apo_search_data_template.R
bq query --project_id=looker-studio-pro-452620 --use_legacy_sql=false \
  < apollo/sql/create_stg_crossplatform_apo_primary_qa.sql
bq query --project_id=looker-studio-pro-452620 --use_legacy_sql=false \
  < apollo/sql/create_data_model_social_apo_primary_qa.sql
bq query --project_id=looker-studio-pro-452620 --use_legacy_sql=false \
  < apollo/sql/validate_apo_primary_social_qa.sql
```

Do not replace the production shared social staging build or the production master model until the duplicate-key resolution and historical Apollo impact have been reviewed and approved.
