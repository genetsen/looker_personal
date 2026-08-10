# Master Data Model

This project owns the generalized cross-client master data model. Its current
production output is [Master evidence model v3](https://console.cloud.google.com/bigquery?project=looker-studio-pro-452620&p=looker-studio-pro-452620&d=master_stg&t=data_model_v3&page=table), a clustered table that preserves the lowest available source grain.

[Master evidence model](https://console.cloud.google.com/bigquery?project=looker-studio-pro-452620&p=looker-studio-pro-452620&d=master_stg&t=data_model&page=table) remains a live package/date compatibility base while the reporting-mart migration and the later file/BigQuery cleanup are planned. It is not the current production model for new master-model work.

It was split out of the ADIF workspace because the model intentionally removes ADIF-only filters and is meant to support broader analysis across clients.

## Current Workspace

Start in [model](./model/) for current work. It organizes the project by function and source branch: current final model, stable base, Manual Editor, mappings, source branches, rollups, reporting outputs, automation, and reference maps.

The organized workspace is now the canonical local surface for runner-used master-model files. Legacy root-level files remain only where they are still needed for compatibility or history.

Agent entrypoints:

- [AGENTS.md](/Users/eugenetsenter/Looker_clonedRepo/looker_personal/master_data_model/AGENTS.md) is the authoritative project rule file.
- [CLAUDE.md](/Users/eugenetsenter/Looker_clonedRepo/looker_personal/master_data_model/CLAUDE.md) gives Claude Code a concise architecture and workflow guide and explicitly defers to `AGENTS.md` when they differ.

Interactive orientation:

- [`docs/master-data-model-map.html`](docs/master-data-model-map.html) - clickable map of live inputs, branch logic, rollups, production outputs, and model-warning callouts.
- [`docs/dcm-cost-model-map.html`](docs/dcm-cost-model-map.html) - clickable DCM scheduled-query map for package rollups, pricing logic, the creative-safe join key, and Master Model handoff.
- [`docs/manual-data-editor-workflow-map.html`](docs/manual-data-editor-workflow-map.html) - clickable map of the Manual Data Editor loop from Sheet edit, request notification, loader writes, manual evidence tables, model merge, mart output, and troubleshooting path.
- [Master Data Model Package Lookup Sheet](https://docs.google.com/spreadsheets/d/1Q_KK6WWqB4aUGNezFmWTMTKz43rQyARqXVGgNs5IT3c) - use one search box to find a partial Package ID, Package Name, or Package Friendly Name and return package-level flight, plan, delivery, advertiser, supplier, campaign, initiative, channel, and GS Channel fields.
- [Direct CM360 conversion migration](docs/rtl-direct-cm360-conversion-migration.md) - deployed RTL conversion source, history-preserving staging, direct activity metrics, and the explicit conversion-only safety rule.
- [Manual Data Editor product brief](/Users/eugenetsenter/Looker_clonedRepo/looker_personal/master_data_model/_bmad-output/planning-artifacts/briefs/brief-master_data_model-2026-06-18/brief.md) - slide-ready brief for presenting what the editor is, how it works, and which correction bottlenecks it removes.

## Manual Package Edit Quick Reference

Use this when a dashboard value needs a direct manual correction.

- Detailed QA runbook: `manual_package_edits/QA_RUNBOOK.md`.
- Sheet: `Package Editor` in the manual updates Google Sheet.
- User action: filter to the package, edit the visible value directly, and look for markers: orange means changed from the current dashboard snapshot, purple means already using a validated manual update, and red means a started new row needs fixing.
- Best identifiers: `Package ID`, `Site`, and `Package Friendly Name`.
- Flight dates and package metadata: package-level corrections apply to the whole `Package ID`, across all delivery dates.
- Delivered metrics: can be edited for a full flight, one week, or one day by changing `Delivery Override Start Date` and `Delivery Override End Date`.
- Planned metrics: `Planned Spend` and `Planned Impressions` are full-flight only. Partial-range planned edits are blocked.
- New rows: require `Package ID`, `Site`, `Package Friendly Name`, flight dates, delivery override dates, at least one metric, and the visible required metadata fields.
- Refresh request: users can check the `Request refresh` control in the sheet to send Gene an email notification. It does not run the loader by itself.
- Loader: `manual_package_edits/load_manual_package_edits.R`, also registered in the universal script runner through `automation_hub/workloads/ops/master_manual_package_edits/load_master_manual_package_edits.R`.
- Backend evidence: valid edits land in `man_*` fields and take priority for final `_` fields with `COALESCE(man_value, normal_value)` behavior.
- QA path: visible Sheet row -> raw manual table -> daily manual table -> `master_stg.data_model` -> `master_stg.data_model_mart`.

## Package Lookup Sheet

Use the [Master Data Model Package Lookup Sheet](https://docs.google.com/spreadsheets/d/1Q_KK6WWqB4aUGNezFmWTMTKz43rQyARqXVGgNs5IT3c) when a user needs to find current package-level values without editing the Manual Data Editor.

| Search field | Fields checked | Match behavior |
|---|---|---|
| Any part of an ID, name, site, or supplier | Package ID, Package Name, Package Friendly Name, Supplier Code, and Supplier Name | Case-insensitive partial match; a match in any one field is returned |

The Sheet reads the [complete stored master-model support table](https://console.cloud.google.com/bigquery?project=looker-studio-pro-452620&p=looker-studio-pro-452620&d=master_stg&t=data_model_clustered_by_advertiser_qa&page=table), including valid manual-only packages, and groups matching daily rows into one result per package. One search checks all five identifying and supplier fields, regardless of capitalization, and each search returns at most 200 packages. For example, `QUAN`, `Columbus Circle DOOH`, and `ccdooh` all return the Columbus Circle DOOH package.

Delivery Override Start Date and Delivery Override End Date are intentionally blank because they are user-entered correction windows, not source fields from the master model. The corrected one-field search must pass the three Columbus Circle acceptance searches before it is verified end to end.

Maintained source: [package lookup Sheet code](/Users/eugenetsenter/Looker_clonedRepo/looker_personal/master_data_model/model/package_lookup_sheet/README.md).

## What The View Does

The current final model builder is in:

`model/final_model/create_master_stg_data_model_v3.sql`

The stable package/date base that v3 reads from is in:

`model/stable_base/create_master_stg_data_model.sql`

The reporting mart definition is in:

`model/reporting_outputs/create_master_stg_data_model_mart.sql`

Deprecated v2, Ritual, sample, and temporary QA SQL are no longer active root-level files. They are preserved under:

`model/archive_candidates/deprecated_sql/`

The dashboard-like manual package edit path is in:

- `model/manual_editor/create_manual_package_edit_tables.sql`
- `model/manual_editor/load_manual_package_edits.R`
- `model/manual_editor/QA_RUNBOOK.md`

The view:

1. Reads Prisma rows for packages with `start_date >= 2025-01-01`.
2. Reads DCM, original FPD, and updated FPD rows with `date >= 2025-01-01`.
3. Removes the ADIF-only advertiser, source-file, and account filters used by the ADIF production pipeline.
4. Combines original and updated FPD before final digital metrics are calculated.
5. Recalculates package rollups after final spend, impressions, and clicks are assigned.
6. Adds TV rows from the combined local/national TV estimate view with synthetic package and placement IDs.
7. Adds social rows from the cross-platform raw social table when compatible daily social grain is available, with WP workbook rows primary for Apollo. Zero-value rows after an already-known pacing end are excluded before they enter the base model; the raw source records remain available for audit.
8. Adds Ritual Amazon Ads rows from the runner-maintained landing table, mapping supply cost, impressions, clicks, video starts, and video completions into generic delivery metrics while preserving all Amazon report fields in `amzn_*` columns.
9. Exposes the daily driving source in `qa_row_data_source_primary`, all daily detected sources in `qa_row_data_sources_available`, and compact source or metric problems in `qa_data_issues`.
10. Standardizes `_advertiser`, `_advertiser_name`, and `_advertiser_short_name` through the separate advertiser mapping table so mapped clients use the same advertiser label and short code across digital, social, TV, Amazon, and manual rows. Apollo LinkedIn rows with the raw account name `Apollo Corporate` use the canonical advertiser `Apollo` while retaining the raw source text. Other unmapped clients default to Prisma's advertiser name after removing legal suffixes such as `Inc`, `LLC`, `Corp`, and `Ltd`, and keep any available source short code as a fallback.
11. Applies valid active manual package edits from `landing.master_data_model_manual_package_daily` and package-level metadata edits from `landing.master_data_model_manual_package_edits_raw` before package rollups are calculated.
12. Adds `initiative` from Prisma's source column `initative` at the package/date model grain.
13. Preserves Prisma's `CAMPAIGN_PUBLIC_ID` from the raw landing table through both scheduled branches: `Prisma_expanded` publishes it in `prisma_expanded_full`, while `process_prisma` publishes it in `prisma_porcessed`, `prisma_porcessed_with_placements`, and the downstream `prisma_processed_plusDCMimps` view. Downstream master-model projections must still select it explicitly if they publish it.
14. Adds canonical `_creative_name` using original FPD creative, Amazon ad-name, and social creative fields. DCM creative remains in the delivery-detail view because one package/date can contain multiple DCM creatives.
15. Adds canonical source lineage and freshness fields: `qa_data_source` names the raw or controlled source table driving the row, `qa_data_source_refresh_at` shows when that represented data last successfully reached the table consumed by the model, and `qa_data_source_content_modified_at` separately shows reliable source-content modification times for original FPD, revised FPD, and Manual Data Editor Google Sheets.

## Source Tables And Views

For the complete live Prisma object inventory, table purposes, grain definitions, refresh ownership, and field lineage, see the [Prisma table catalog](model/branches/prisma/PRISMA_TABLE_CATALOG.md). The shorter [Prisma pipeline guide](model/branches/prisma/README_prisma-pipeline.md) is the recommended starting point.

Digital inputs:

- `looker-studio-pro-452620.20250327_data_model.prisma_expanded_full`
- `looker-studio-pro-452620.DCM.20250505_costModel_v5`
- `looker-studio-pro-452620.landing.fpd_data_ranged_shortcutsFolder`
- `looker-studio-pro-452620.landing.adif_updated_fpd_daily`
- [Direct CM360 RTL conversion history](https://console.cloud.google.com/bigquery?project=looker-studio-pro-452620&p=looker-studio-pro-452620&d=master_stg&t=rtl_cm360_direct_conversions&page=table) - persistent source-activity history for RTL conversions. The existing `master_data_model_upstream_tables_sched` BigQuery schedule merges the newest eligible direct CM360 report into this history daily at 10:15 UTC, and the universal runner triggers that same schedule before its V3 refresh step. Neither path reads Google Sheets.
- [Legacy-schema RTL compatibility table](https://console.cloud.google.com/bigquery?project=looker-studio-pro-452620&p=looker-studio-pro-452620&d=landing&t=rtl_conv_report&page=table) - optional comparison and compatibility evidence refreshed from direct CM360 history by the standalone loader. It retains the old 18-column shape, with unavailable delivery-only and Sheet-lineage fields blank. V3 and the universal runner do not read it, and the canonical V3 builder contains no dormant Sheet-conversion branch.
- [Advertiser mapping](https://console.cloud.google.com/bigquery?project=looker-studio-pro-452620&p=looker-studio-pro-452620&d=master_stg&t=advertiser_mapping&page=table)

Social inputs:

- [Shared cross-platform raw staging](https://console.cloud.google.com/bigquery?project=looker-studio-pro-452620&p=looker-studio-pro-452620&d=repo_stg&t=stg__olipop__crossplatform_raw_tbl&page=table)
- [Reddit Ads email landing table](https://console.cloud.google.com/bigquery?project=looker-studio-pro-452620&p=looker-studio-pro-452620&d=landing&t=reddit-ads-email&page=table)
- [Reddit shared-social staging view](https://console.cloud.google.com/bigquery?project=looker-studio-pro-452620&p=looker-studio-pro-452620&d=repo_stg&t=stg__olipop_reddit_crossplatform&page=table)
- [Social pacing table](https://console.cloud.google.com/bigquery?project=looker-studio-pro-452620&p=looker-studio-pro-452620&d=repo_int&t=crossplatform_pacing_tbl&page=table)

Shared-social staging excludes campaign names containing `1000heads` or `PROS_Dysrupt` before the master model reads social rows. This keeps those agency-side campaigns out of the source path instead of hiding them later in the reporting layer.

Amazon Ads input:

- [Ritual Amazon Ads landing table](https://console.cloud.google.com/bigquery?project=looker-studio-pro-452620&p=looker-studio-pro-452620&d=landing&t=rit_amzn_report_daily&page=table)

Apollo production source and review controls:

- [APO Search Data Template](https://docs.google.com/spreadsheets/d/1fen46Ugxx12PYRzCDT88z8ENVcVl_MjlQhqkGDxZNbc/edit?gid=982708559#gid=982708559) supplies production Apollo delivery fields and creative metadata.
- [WP normalized staging](https://console.cloud.google.com/bigquery?project=looker-studio-pro-452620&p=looker-studio-pro-452620&d=repo_stg&t=stg__wp__search_data_template_daily&page=table) is the controlled production Sheet snapshot used by shared social staging.
- [Shared cross-platform raw staging](https://console.cloud.google.com/bigquery?project=looker-studio-pro-452620&p=looker-studio-pro-452620&d=repo_stg&t=stg__olipop__crossplatform_raw_tbl&page=table) applies WP-first cell precedence for Apollo and retains standard rows/fields as fallback.
- [Master data model](https://console.cloud.google.com/bigquery?project=looker-studio-pro-452620&p=looker-studio-pro-452620&d=master_stg&t=data_model&page=table) maps WP Search and YouTube rows to Paid Search and Online Video and exposes canonical `_creative_name`, source-specific `s_creative_name`, `man_creative_img`, `_creative_img`, `_video_views`, and `s_*` provenance fields.
- [WP-primary cross-platform QA](https://console.cloud.google.com/bigquery?project=looker-studio-pro-452620&p=looker-studio-pro-452620&d=repo_stg&t=stg__crossplatform_wp_primary_qa&page=table) applies WP-first column precedence at candidate campaign/ad-group/ad grain from the maintained WP staging table, while retaining the current shared source as fallback.
- [WP-primary social reporting QA](https://console.cloud.google.com/bigquery?project=looker-studio-pro-452620&p=looker-studio-pro-452620&d=master_stg&t=data_model_social_wp_primary_qa&page=table) exposes candidate Apollo Paid Search and Online Video reporting labels plus creative and provenance fields.
- Records with cross-campaign ad-ID ambiguity are included in production for now and remain visibly marked `publish_pending_source_owner_review` / `pending_wp_source_owner_review` until the source owner clarifies the intended identity rule.
- The WP Sheet-to-staging load remains a controlled manual refresh while that clarification is open; the daily shared-social SQL builder is scheduled.

TV inputs:

- [TV combined table](https://console.cloud.google.com/bigquery?project=looker-studio-pro-452620&p=looker-studio-pro-452620&d=landing&t=tv_combined_tbl&page=table)
- [TV combined view](https://console.cloud.google.com/bigquery?project=looker-studio-pro-452620&p=looker-studio-pro-452620&d=landing&t=tv_combined&page=table) remains available for source-lineage comparison.

Manual package edit inputs:

- `looker-studio-pro-452620.landing.master_data_model_manual_package_edits_raw`
- `looker-studio-pro-452620.landing.master_data_model_manual_package_daily`

Output:

- [Master evidence model v3](https://console.cloud.google.com/bigquery?project=looker-studio-pro-452620&p=looker-studio-pro-452620&d=master_stg&t=data_model_v3&page=table) - **Current production master model.** Clustered by `_advertiser`, it preserves source rows at their natural grain, carries summable `_planned_*` metrics once per package/date, keeps repeated plan context in `qa_v3_package_planned_*_doNotSum`, and joins direct CM360 RTL conversions at package/date/placement/creative detail. Planned-only TV, Print, OOH, and dOOH rows use planned spend and impressions as delivered values when planned impressions are available, while real delivery still takes precedence. Conversion-only evidence rows keep conversion fields but no delivery metrics. The universal runner's `Master Data Model Clustered Advertiser Refresh` step rebuilds and verifies it after refreshing the compatibility support table.
- [Ritual dashboard compatibility view](https://console.cloud.google.com/bigquery?project=looker-studio-pro-452620&p=looker-studio-pro-452620&d=master_stg&t=ritual_data_model&page=table) - Ritual-only projection over `data_model_v3` that preserves the established dashboard names and Omni topic while exposing v3's wider schema. It currently has 241 fields: 125 compatibility fields, all 115 v3 fields that were previously hidden, and a friendly `creative_name` alias for canonical `_creative_name`. Both creative fields remain available, while Omni presents the alias as `Creative Name`. Its wildcard exclusion contract also carries future non-conflicting v3 fields automatically. The [maintained Ritual view builder](model/reporting_outputs/create_master_stg_ritual_data_model.sql) is the deployment source, and its [SQL Change Guard manifest](model/reporting_outputs/ritual_data_model_v3_schema_expansion.qa.json) protects existing rows, measures, derived rates, dimensions, the creative alias, and the Ritual-only filter.
- [Master evidence model](https://console.cloud.google.com/bigquery?project=looker-studio-pro-452620&p=looker-studio-pro-452620&d=master_stg&t=data_model&page=table) - Live package/date compatibility base that supplies stable context to v3. Keep it available until the later reporting-mart migration and cleanup; do not use it as the default for new master-model work.
- [Clustered advertiser QA table](https://console.cloud.google.com/bigquery?project=looker-studio-pro-452620&p=looker-studio-pro-452620&d=master_stg&t=data_model_clustered_by_advertiser_qa&page=table) - Stored compatibility/QA snapshot of the package/date base, clustered by `_advertiser` for advertiser-filtered checks. It is refreshed by the scheduled query `master_data_model_clustered_advertiser_refresh` at `10:45` UTC daily, before the same runner step rebuilds v3.
- `looker-studio-pro-452620.master_stg.data_model_v2` - Compatibility wrapper over `data_model` for existing v2 consumers.
- `looker-studio-pro-452620.master_stg.data_model_delivery_detail_v2` - Sibling v2 delivery-detail model. Preserves DCM and original FPD creative at the source/detail grain, with package budget fields marked as non-summable context.
- `looker-studio-pro-452620.master_stg.data_model_detail_master_v2_sample` - One-table comparison sample that stacks package/date rows with delivery-detail rows. This is useful for evaluating a single-table shape but is not the canonical reporting model because unfiltered sums can double-count package and delivery metrics.
- [Reporting mart](https://console.cloud.google.com/bigquery?project=looker-studio-pro-452620&p=looker-studio-pro-452620&d=master_stg&t=data_model_mart&page=table) - Package/date compatibility reporting mart over `data_model`. It remains live for existing consumers until a separately verified v3 reporting migration.
- [Purely Elizabeth reporting view](https://console.cloud.google.com/bigquery?project=looker-studio-pro-452620&p=looker-studio-pro-452620&d=master_ext_west&t=mart_data_model_purelyElizabeth&page=table) - Advertiser-specific west-region output. Linear rows and supplier-code `QUAN` rows publish planned spend and impressions in the delivered metric fields; other rows preserve underlying delivery values. Its maintained SQL is the [Purely Elizabeth view builder](model/reporting_outputs/create_master_ext_west_mart_data_model_purely_elizabeth.sql).
- [Purely Elizabeth delivery-plus-sales weekly table](https://console.cloud.google.com/bigquery?project=looker-studio-pro-452620&ws=!1m5!1m4!4m3!1slooker-studio-pro-452620!2smaster_ext_west!3smart_PE_delivery_plus_sales_weekly) - Stored metric-only output refreshed every two hours by `master_raw_CopyToWest`, with one Sunday-ending row per mapped product group or `UNMAPPED` delivery group that has media delivery. Its `PE` mapping table controls sales eligibility only: unmapped delivery remains visible with null sales. It exposes eight approved media measures plus `sales_dollars` and `sales_tdp`, and the current Protein Granola sales source joins only to that group.
- `looker-studio-pro-452620.master_stg.data_model_mart_v2` - Reporting mart over `data_model_v2`.
- `looker-studio-pro-452620.master_stg.data_model_qa_tv_layer` - QA validation view used before the TV layer was promoted to production.
- `looker-studio-pro-452620.master_stg.data_model_qa_source_issues` - QA validation view for source visibility, issue labels, and non-Prisma DCM/FPD rows.
- `looker-studio-pro-452620.master_stg.data_model_mart_qa_source_issues` - QA validation mart built from the source-issue QA view.
Deprecated Ritual and v2 compatibility SQL is archived in `model/archive_candidates/deprecated_sql/`. Do not use it as the current path unless you are answering a historical or rollback question.
- Source view: `looker-studio-pro-452620.master_stg.data_model_delivery_detail_v2`

## Important Modeling Notes

- Global behavior rules for BigQuery access, SQL safety, live source-of-truth checks, grain conflicts, and modeling decisions live in [BigQuery, SQL, and data-modeling rules](/Users/eugenetsenter/.codex/BIGQUERY_SQL_DATA_MODELING_RULES.md). This section records master-model field semantics and durable local behavior.
- This view is separate from the ADIF scheduled refresh and does not replace `looker-studio-pro-452620.repo_stg.adif__mainDataTable_notebook_v2_test`.
- For redesign and QA validation, row count is a diagnostic only when grain can change. Approval proof should use overall and package-level spend, impressions, and clicks reconciliation, with any intended filters, transformations, exclusions, allocations, or source-scope changes applied consistently to both baseline and candidate.
- Digital rows keep package IDs from Prisma/DCM/FPD.
- Social rows use a synthetic package key: `social:<platform>:<campaign_id>:<ad_group_id>`, because social data does not naturally share Prisma package IDs. Campaigns containing `1000heads` or `PROS_Dysrupt` are excluded before shared-social staging reaches this model, and the Manual Data Editor loader also drops stale manual rows for those excluded campaign/package records. Reddit email-ingested rows use the same social key shape, preserve [Reddit Ads email landing table](https://console.cloud.google.com/bigquery?project=looker-studio-pro-452620&p=looker-studio-pro-452620&d=landing&t=reddit-ads-email&page=table) as their source lineage, and derive pacing from Reddit campaign budget plus campaign flight dates in that landing table.
- In Apollo production social rows, `_Search_` campaign markers map to Paid Search and `_YT_` campaign markers map to Online Video; rows without either marker use the Sheet channel as fallback. Identity includes campaign, ad group, and ad, so cross-campaign ad-ID conflicts are included as separately flagged `publish_pending_source_owner_review` rows pending source-owner clarification; exact duplicate records remain excluded.
- WP social creative and lineage are visible in `s_creative_name`, `man_creative_img`, `_creative_img`, `s_channel_classification_source`, `s_publication_status`, `s_source_sheet_url`, `s_loaded_at`, `s_wp_row_key`, `s_record_source`, and `s_fallback_fields`.
- `_creative_name` is the package/date model's canonical creative label. It uses consolidated `fpd_creative`, then `amzn_ad_name`, then `s_creative_name`; Amazon is checked before the shared social field because Amazon rows currently carry their ad name in both fields. DCM creative names are intentionally excluded because they can be one-to-many at package/date grain and remain available in `data_model_delivery_detail_v2`.
- TV rows use synthetic package and placement keys because the TV estimate view does not naturally share Prisma package IDs.
- TV source fields are preserved in `tv_*` fields, including outlet, type, program, market, quarter, year, net impressions, net cost, total units, and data refresh date.
- TV, Print, OOH, and dOOH package rows use planned cost and planned impressions as the final `_spend` / `_impressions` fallback only when the delivered metric is unavailable and planned impressions are available. Real delivered values still win when present, and `qa_data_issues` can still show the source cause, such as `missing_actuals`, so users know the value came from the plan.
- Amazon Ads rows use synthetic package keys because the Amazon report does not naturally share Prisma package IDs.
- Amazon Ads source fields are preserved in `amzn_*` fields. The model maps Amazon supply cost into `_spend`, and maps impressions, clicks, video starts, and complete video views into the generic delivery metric fields. Amazon sales, purchases, units sold, branded searches, source email metadata, and source file metadata stay visible in the Amazon-specific columns.
- Amazon Ads rows use `qa_media_data_type = 'amazon_ads'` and `qa_row_data_source_primary = 'amazon_ads'`. Amazon `sales` is not mapped into `_spend` because it is outcome revenue, not media spend; Amazon `supply_cost` is preserved as `amzn_supply_cost` and mapped into `_spend`.
- Updated FPD is layered before `final_spend`, `final_impressions`, and package actual rollups are calculated.
- Original and revised FPD inputs are consolidated into one public `fpd_*` family before the package/date model is exposed. The model publishes final FPD metrics, benchmark, factor, creative, image, and contributing-source lineage without separate original/revised output columns.
- Manual package edits are layered after the normal digital/social/TV rows and before planned backfills, row callouts, and package rollups. Non-null `man_daily_*` metric values win for their matching final `_` fields on the edited delivery dates, while package-level `man_*` metadata values override Prisma metadata for the whole package.
- The manual edit sheet uses a single `Package Editor` tab. It shows package lookup context and dashboard values in the same row; users edit the value cell directly, and the loader compares that cell to source-derived baselines plus the last run to decide whether to write, preserve, block, or explicitly clear a backend manual value.
- The loader treats source values as the baseline for new edit detection. Planned package totals come directly from PRISMA package totals, while delivered metric baselines are recalculated from the raw delivery fields instead of manual-affected final `_` fields. Prior trusted manual values stay user-owned even if the refreshed source snapshot later matches them, so a feedback loop from manual-backed tables cannot silently clear the edit.
- The visible editor starts with the fields a media buyer needs to recognize the row: `Package ID`, `Site`, and `Package Friendly Name`, followed by pale-yellow editable date and metric columns.
- The visible editor also shows manual-edit audit columns: `Manually Edited?`, `Manual Edit At`, `Manual Edit By`, and `Manual Edit Published At`. The edit timestamp and editor value come from the bound Apps Script when Google exposes the editor identity; the published timestamp is set when the loader accepts a valid manual edit into BigQuery.
- The package lookup facets live in native Google Sheets slicers for Advertiser, Channel, Campaign, and Site. They filter the real editable package rows without Apps Script-driven dropdowns or hidden helper columns.
- The sheet has a `Request refresh` checkbox-style control. Checking it notifies Gene by email; if a Slack webhook script property exists, the same request can post to Slack. It is notification-only and does not run the loader or write to the warehouse by itself.
- `Flight Start Date` and `Flight End Date` are package-level fields. `Delivery Override Start Date` and `Delivery Override End Date` define the metric override window. To change delivery for one week while leaving other weeks as-is, users add or duplicate a row, set the delivery override dates to that week, and enter replacement delivered totals for that week only.
- Planned metrics display as full-flight totals and can only be edited on full-flight rows. Delivered actual metrics can use day, week, or full-flight replacement totals.
- To undo a manual value, clear the visible edited cell or set it back to a different displayed baseline/source value where that applies. If the normal source later catches up to a manual replacement, the loader keeps the backend manual value and formatting marker until the user explicitly clears or deletes it.
- Manual daily allocation preserves exact replacement totals after upload: count metrics distribute whole units across the selected dates, and spend metrics distribute by cents.
- New manual-only rows need enough metadata to enter the main model: `Package ID`, `Site`, `Package Friendly Name`, `Flight Start Date`, `Flight End Date`, `Delivery Override Start Date`, `Delivery Override End Date`, at least one edited metric, plus visible required metadata fields for Advertiser, Package Type, Channel, Campaign, Package Name, and GS Channel. Started new rows turn red when required fields are missing or dates are invalid.
- The editor UX is managed separately from the R loader by `manual_package_edits/setup_manual_package_editor_sheet.mjs`. That setup owns the instruction area, frozen header row, base table filter, table banding, borders, protected source/context columns, hidden baseline comparison columns, visual validation rules, native slicers, and pale-yellow editable date/metric columns.
- The R loader only refreshes data values and backend manual tables. It does not own sheet design, filters, widths, colors, or table formatting.
- Edited/manual-backed cells are marked only with Google Sheet formatting. The user-facing sheet does not expose replacement columns, delta columns, validation tabs, proof tabs, or `man_*` backend fields.
- The loader still proves each uploaded daily metric sums back to the edited total before it replaces the daily manual table.
- Manual-only packages can enter the model only from valid active manual rows with required metadata. They use `qa_media_data_type = 'manual'` and `qa_row_data_source_primary = 'manual_package_edits'`.
- Manual evidence is visible in `man_*` fields, including edit ID, reason, loader account, source sheet URL, modified time, row-level edit audit fields, daily values, and total replacement values marked with `_doNotSum` where they are repeated across daily rows. `qa_manual_edit_flag` provides the dashboard-friendly boolean indicator; the non-boolean edit timestamps and editor identity remain in the `man_*` source-evidence family.
- `qa_row_data_source_primary` is a priority label. Consolidated FPD wins over DCM when both are present.
- `qa_row_data_sources_available` lists every detected source for the row, such as `prisma | prisma_daily | dcm | fpd`.
- `qa_pkg_primary_data_source` identifies the source categories that actually populate final `_` metrics anywhere in the package. It returns the single contributing source, `multiple` when different sources contribute across metrics or dates, and `none` when no source populates final actual metrics.
- `qa_pkg_data_sources_available` lists every distinct source available anywhere in the package, including planning and pacing sources that do not populate final actual metrics.
- `qa_data_source` names the actual source table driving the row rather than the intermediate master-model view. Standard social rows resolve to their platform raw table, including separate TikTok and TikTok ADIF sources.
- `qa_data_source_refresh_at` is an operational timestamp: when the represented data successfully reached the source table consumed by the model. For pipelines with both raw ingestion and an aggregate stage, it uses the earlier timestamp so the field cannot claim freshness later than an unfinished downstream stage.
- `qa_data_source_content_modified_at` is intentionally separate from operational refresh. It is populated only where a reliable source-content timestamp exists: original FPD source files, revised FPD source Sheets, and the Manual Data Editor Sheet. Other sources remain null instead of receiving an unrelated load timestamp.
- `qa_data_issues` uses compact source/cause labels such as `missing_prisma_package`, `missing_prisma_daily`, `actual_source_conflict`, `missing_actuals`, `missing_social_pacing`, and `missing_final_metrics`. Healthy rows use `no_issues`.
- Boolean QA fields use the `_flag` suffix. `qa_manual_edit_flag` marks rows affected by manual inputs, and `qa_package_spend_over_plan_flag` compares package actual spend with package planned spend.
- Package-level QA totals repeat on daily rows and therefore retain the `_doNotSum` suffix. Consolidated FPD totals are exposed as `qa_pkg_fpd_spend_doNotSum` and `qa_pkg_fpd_impressions_doNotSum`.
- `_advertiser` and `_advertiser_name` are the same canonical advertiser value. `_advertiser_short_name` is the canonical short code for the mapped advertiser, so the same client keeps the same code across channels, platforms, and source systems. The [advertiser mapping](https://console.cloud.google.com/bigquery?project=looker-studio-pro-452620&p=looker-studio-pro-452620&d=master_stg&t=advertiser_mapping&page=table) groups known source-name and short-code variants. LinkedIn's `Apollo Corporate` account is normalized to `Apollo` before the generic fallback, with `s_account_name` preserved for audit. When another new client has no mapping row, the model uses Prisma's advertiser name and removes trailing legal suffixes such as `Inc`, `LLC`, `Corp`, and `Ltd`, then falls back to any available source short code.
- `_package_name_friendly` falls back to the full `_package_name` when the friendly name is blank, so package lookup rows do not show empty friendly labels when a usable full package name exists.
- Candidate source-issue logic lets DCM and FPD rows enter even when their package ID is not present in Prisma. These rows stay visible in their raw `d_*` or `fpd_*` fields, but their `final_*` metrics remain null so they do not contribute to package actual rollups. When Prisma metadata is unavailable, the model falls back to DCM/FPD names, dates, supplier, and package identifiers where available.
- Highlights rows are excluded from the master evidence model before final publication.
- Candidate master logic labels low-signal DCM rows with `low_signal_dcm` when each row has fewer than `1,000` raw impressions, less than `$0.10` raw media cost, fewer than `5` raw clicks, and the package's filtered-row average is below `100` raw impressions per row.
- The reporting mart filters `low_signal_dcm` rows and recalculates package rollups after filtering. Keep the master model as the evidence layer; use the mart for reporting-ready totals.
- The reporting mart also recalculates `qa_pkg_primary_data_source` and `qa_pkg_data_sources_available` after its row exclusions so its package-source labels describe only the rows available to reporting.
- `initiative` now lives directly in the main package/date `data_model`, sourced from Prisma's source column `initative`. The package/date `data_model_v2` view is a compatibility wrapper over `data_model`.
- Creative lives in `data_model_delivery_detail_v2`, where DCM rows are preserved at package/date/placement/ad/creative grain and original FPD rows are preserved at package/date/partner-placement/partner-creative grain.
- `creative_git_link` is preserved from original FPD detail rows when the partner data source supplies a Git-hosted creative image link.
- Package budget fields in `data_model_delivery_detail_v2` use `doNotSum` names because they are context copied onto lower-grain rows. Use package/date tables for budget totals.
- `data_model_detail_master_v2_sample` exists only to compare the single-table approach. Any query against it must filter `row_level` or `metric_grain` before summing metrics.
- The package/date v2 view is compatibility-only; detail/sample v2 views still carry the lower-grain creative/detail structures.
- `data_model_v3` is the current production master model for new work. It follows the DCM pattern: source rows stay at their natural grain, package planned values repeat as `qa_v3_package_planned_spend_doNotSum` and `qa_v3_package_planned_impressions_doNotSum` context, and summable `_planned_*` metrics are carried on exactly one deduced natural row per package/date so package/date and higher rollups work without selecting a grain field. Manual delivery override rows replace lower-grain final actuals for the same package/date; the suppressed source rows remain visible as evidence with blank final actual metrics. DCM source `impressions` populate final DCM impressions; `daily_recalculated_imps` remains context only. Direct CM360 RTL conversions populate `conv_*` fields only after a unique package/date/placement/creative match; conversion-only rows use `qa_v3_source_detail_type = 'cm360_direct_conversion'` and leave delivery metrics blank. The older package/date views, mart, and related files are intentionally retained as compatibility surfaces pending a later migration and cleanup pass.

## Verification Guidance

Row counts, source mix, package-key counts, and media totals fluctuate as source tables refresh. Do not treat those numbers as documentation state, and do not update this README just to chase count changes.

When validating a candidate or production view, run live QA and summarize the result in plain English:

- Confirm the date gates still behave as expected, such as no pre-`2025-01-01` output rows unless the model scope changes.
- Confirm required synthetic keys are populated for social and TV rows.
- Compare production and QA candidates live when reviewing a change.
- Confirm rows labeled `missing_prisma_package` have null `final_*` metrics and do not inflate `pkg_act_*` rollups.
- Confirm mart package rollups are recalculated after reporting-only filters such as `low_signal_dcm`.
- Keep durable findings in docs, such as field semantics, source lineage, filters, deployment commands, and known modeling warnings.
- Keep fluctuating counts in the query result or handoff note for that run, not as permanent README values.

Recent stable QA context:

- `looker-studio-pro-452620.master_stg.data_model_qa_tv_layer` was the QA validation view used before the TV layer was promoted.
- `looker-studio-pro-452620.master_stg.data_model_qa_source_issues` is the QA validation view for source visibility, issue labels, and non-Prisma DCM/FPD rows.
- `looker-studio-pro-452620.master_stg.data_model_mart_qa_source_issues` is the QA mart that filters `low_signal_dcm` rows after the master evidence layer.
- The source-issue QA candidates passed dry run and production was not replaced.

## Deploy Or Refresh The View

From the repo root:

Create the empty manual-edit landing tables before deploying the manual-aware model:

```bash
bq query --project_id=looker-studio-pro-452620 --use_legacy_sql=false \
  < master_data_model/create_manual_package_edit_tables.sql
```

Load or refresh the Google Sheet manual-edit path:

```bash
Rscript /Users/eugenetsenter/Looker_clonedRepo/looker_personal/master_data_model/manual_package_edits/load_manual_package_edits.R
```

The production Manual Data Editor sheet is the new Google Drive version. Set `MASTER_MANUAL_EDIT_SHEET_ID` only when intentionally running against a different sheet copy.
By default, the loader authenticates to Sheets and Drive through the new-account R OAuth cache and uses the active `gcloud` access token for BigQuery. Set `MASTER_MANUAL_EDIT_USE_GCLOUD_TOKEN=false` only when intentionally testing cached BigQuery credentials.

The Manual Data Editor's visible row set is source-first: current lookup packages plus audited manual-only packages. The Sheet supplies edits to known packages but cannot revive an inactive, unedited leftover as a normal editor row. Planned totals use package flight dates; delivered actual overrides use delivery override dates.

Refresh the master view:

```bash
bq query --project_id=looker-studio-pro-452620 --use_legacy_sql=false \
  < master_data_model/create_master_data_model_upstream_tables_sched.sql
```

```bash
bq query --project_id=looker-studio-pro-452620 --use_legacy_sql=false \
  < master_data_model/create_master_stg_data_model.sql
```

Refresh the controlled WP production input and shared-social staging before the master view when the source Sheet changes:

```bash
WP_SEARCH_TABLE=stg__wp__search_data_template_daily \
WP_SEARCH_UPLOAD=TRUE \
WP_SEARCH_ALLOW_PRODUCTION=TRUE \
Rscript wp/load_wp_search_data_template.R

bq query --project_id=looker-studio-pro-452620 --use_legacy_sql=false \
  < wp/sql/create_stg_crossplatform_wp_primary_production.sql
```

Refresh the current final v3 table:

```bash
bq query --project_id=looker-studio-pro-452620 --use_legacy_sql=false \
  < master_data_model/model/final_model/create_master_stg_data_model_v3.sql
```

Refresh the reporting mart after the master view is current:

```bash
bq query --project_id=looker-studio-pro-452620 --use_legacy_sql=false \
  < master_data_model/model/reporting_outputs/create_master_stg_data_model_mart.sql
```

Dry-run validation:

```bash
bq query --project_id=looker-studio-pro-452620 --use_legacy_sql=false --dry_run \
  < master_data_model/create_master_stg_data_model.sql
```

## Quick QA Queries

Row mix:

```sql
SELECT
  row_type,
  row_data_source_primary,
  row_data_sources_available,
  row_data_issue_category,
  COUNT(*) AS row_count,
  MIN(date) AS min_date,
  MAX(date) AS max_date,
  COUNT(DISTINCT package_id_joined) AS package_count
FROM `looker-studio-pro-452620.master_stg.data_model`
GROUP BY row_type, row_data_source_primary, row_data_sources_available, row_data_issue_category
ORDER BY row_type, row_count DESC;
```

Filter gates:

```sql
SELECT
  COUNT(*) AS total_rows,
  COUNTIF(date < DATE '2025-01-01') AS rows_before_2025,
  MIN(date) AS min_date,
  MIN(IF(row_type = 'digital', package_start_date, NULL)) AS min_digital_package_start_date,
  COUNTIF(row_type = 'digital' AND package_start_date < DATE '2025-01-01') AS digital_rows_with_pre_2025_start,
  COUNTIF(qa_media_data_type = 'digital' AND (fpd_impressions IS NOT NULL OR fpd_spend IS NOT NULL)) AS digital_rows_with_fpd,
  COUNTIF(row_type = 'social') AS social_rows
FROM `looker-studio-pro-452620.master_stg.data_model`;
```
