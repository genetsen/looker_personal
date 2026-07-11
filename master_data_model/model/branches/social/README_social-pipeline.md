# Social Delivery Pipeline

This guide documents the cross-platform social branch: standard social delivery, Reddit email ingestion, and WP workbook precedence for Apollo. It follows the pipeline structure in [Master Data Model Pipeline v2](/Users/eugenetsenter/Looker_clonedRepo/looker_personal/master_data_model/README_v2.md).

## Pipeline Overview

```text
Platform delivery feeds ─┐
Reddit email landing ────┼─→ shared cross-platform staging
WP Apollo workbook ──────┘          ↓
                              stable social branch
                                      ↓
                          master evidence model / v3
```

## Source Inventory

| Source | Grain | Role | Key boundary |
|---|---|---|---|
| [Shared cross-platform staging](https://console.cloud.google.com/bigquery?project=looker-studio-pro-452620&p=looker-studio-pro-452620&d=repo_stg&t=stg__olipop__crossplatform_raw_tbl&page=table) | Platform/campaign/ad-group/ad/date | The stable base’s direct social input. | It is the assembled source, not every platform’s raw API table. |
| [Reddit Ads email landing](https://console.cloud.google.com/bigquery?project=looker-studio-pro-452620&p=looker-studio-pro-452620&d=landing&t=reddit-ads-email&page=table) | Reddit campaign/ad-group/ad/date | Normalized to the shared staging contract by the [Reddit staging SQL](/Users/eugenetsenter/Looker_clonedRepo/looker_personal/master_data_model/model/branches/social/reddit/stg__olipop_reddit_crossplatform.sql). | Reddit joins staging as a standardized daily-ad source. |
| [WP normalized staging](https://console.cloud.google.com/bigquery?project=looker-studio-pro-452620&p=looker-studio-pro-452620&d=repo_stg&t=stg__wp__search_data_template_daily&page=table) | Apollo campaign/ad-group/ad/date | Controlled workbook delivery, classification, creative, and provenance. | WP is primary only for matching Apollo records; it does not replace other social clients. |

## Pipeline and Custom Logic

### Standard Social and Reddit

- The stable base aggregates source rows at date/platform/campaign/ad-group/ad identity, normalizes platform names, and creates synthetic social package IDs from platform, campaign, and ad group.
- [Reddit loader](/Users/eugenetsenter/Looker_clonedRepo/looker_personal/master_data_model/model/branches/social/reddit/load_reddit_csv.r) cleans headers, removes the requested CPC field, preserves text IDs, parses dates, and replaces the Reddit landing-table schema.
- [Reddit staging SQL](/Users/eugenetsenter/Looker_clonedRepo/looker_personal/master_data_model/model/branches/social/reddit/stg__olipop_reddit_crossplatform.sql) maps actual campaign, ad-group, and ad IDs into the shared daily-ad column contract.

### WP Apollo Precedence

- [WP normalization rules](/Users/eugenetsenter/Looker_clonedRepo/looker_personal/master_data_model/model/branches/social/wp_search/wp_search_social_logic.R) normalize platform labels, preserve actual zero metrics, create readable synthetic IDs when the Sheet has none, and turn supported YouTube links into thumbnails.
- Campaign markers take precedence over the displayed Sheet channel: `_Search_` means Paid Search and `_YT_` means Online Video. A campaign containing both markers is excluded as ambiguous.
- [Production shared-staging builder](/Users/eugenetsenter/Looker_clonedRepo/looker_personal/master_data_model/model/branches/social/wp_search/sql/create_stg_crossplatform_wp_primary_production.sql) uses WP values first for matching Apollo date/platform/ad/campaign rows. The standard feed fills only WP-missing fields, including conversions and absent video percentiles.
- Exact duplicate WP records are excluded. Cross-campaign reuse of the same ad ID remains visible with `publish_pending_source_owner_review` until a source owner resolves the rule.
- Campaigns containing `1000heads` are excluded before they reach shared staging or the master model.

## Output Fields

| Source information | Final evidence and reporting fields |
|---|---|
| Platform, campaign, ad group, ad, account | `s_platform`, `s_campaign_id`, `s_ad_group_id`, `s_ad_id`, `s_account_name`; plus synthetic `_package_id` and `_placement_id`. |
| Delivery metrics | `s_spend`, `s_impressions`, `s_clicks`, video evidence, then final `_spend`, `_impressions`, `_clicks`, and `_video_views`. |
| WP provenance | `s_channel_classification_source`, `s_record_source`, `s_fallback_fields`, `s_source_sheet_url`, `s_loaded_at`, and `s_wp_row_key`. |
| Creative evidence | `s_creative_name` and the normalized image URL; the common model makes social creative a fallback after original FPD and Amazon creative. |

## Verification and Rollback Assets

| Purpose | Asset |
|---|---|
| Unit-level business-rule proof | [WP logic tests](/Users/eugenetsenter/Looker_clonedRepo/looker_personal/master_data_model/model/branches/social/wp_search/tests/test_wp_search_social_logic.R) |
| Isolated candidate build | [WP staging QA SQL](/Users/eugenetsenter/Looker_clonedRepo/looker_personal/master_data_model/model/branches/social/wp_search/sql/create_stg_crossplatform_wp_primary_qa.sql) and [reporting-shape QA SQL](/Users/eugenetsenter/Looker_clonedRepo/looker_personal/master_data_model/model/branches/social/wp_search/sql/create_data_model_social_wp_primary_qa.sql) |
| Focused QA queries | [WP validation SQL](/Users/eugenetsenter/Looker_clonedRepo/looker_personal/master_data_model/model/branches/social/wp_search/sql/validate_wp_primary_social_qa.sql) |
| Approved rollback only | [Pre-WP rollback SQL](/Users/eugenetsenter/Looker_clonedRepo/looker_personal/master_data_model/model/branches/social/wp_search/archive_candidates/rollback_stg_crossplatform_pre_wp_production.sql) |

## Related Guides

- [WP workbook workflow](/Users/eugenetsenter/Looker_clonedRepo/looker_personal/master_data_model/model/branches/social/wp_search/README.md)
- [TV pipeline](/Users/eugenetsenter/Looker_clonedRepo/looker_personal/master_data_model/model/branches/tv/README_tv-pipeline.md)
- [Amazon Ads pipeline](/Users/eugenetsenter/Looker_clonedRepo/looker_personal/master_data_model/model/branches/amazon/README_amazon-pipeline.md)
