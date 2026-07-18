# Reporting Outputs

Dashboard-facing mart and reporting output logic belongs here.

Keep this separate from the final model because dashboard filters and reporting exclusions can differ from the evidence/final table behavior.

## Ritual Dashboard Compatibility View

The [Ritual dashboard compatibility view](https://console.cloud.google.com/bigquery?project=looker-studio-pro-452620&p=looker-studio-pro-452620&d=master_stg&t=ritual_data_model&page=table) filters the v3 master model to Ritual while preserving the field names used by the existing Omni dashboard. The view currently exposes 241 fields: 125 established compatibility fields, 115 additional v3 fields, and `creative_name` as a friendly alias for canonical `_creative_name`. The raw underscore field remains available for lineage, while Omni displays the alias as `Creative Name`. Its `v3.* EXCEPT (...)` contract keeps the established aliases stable and automatically includes future v3 fields whose names do not conflict with an existing compatibility output.

Use the [Ritual compatibility view builder](./create_master_stg_ritual_data_model.sql) for deployment. Before replacing the live view, create an isolated candidate with the [Ritual QA builder](./create_master_stg_ritual_data_model_v3_schema_expansion_qa.sql) and run the [Ritual SQL Change Guard manifest](./ritual_data_model_v3_schema_expansion.qa.json). Existing row counts, reporting totals, derived rates, dimensions, and the Ritual-only filter must remain unchanged; the guard also requires `creative_name` to match `_creative_name` on every row.

## Purely Elizabeth West-Region View

The [Purely Elizabeth reporting view](https://console.cloud.google.com/bigquery?project=looker-studio-pro-452620&p=looker-studio-pro-452620&d=master_ext_west&t=mart_data_model_purelyElizabeth&page=table) filters the west-region model to Purely Elizabeth. For rows whose channel group is `linear`, or whose supplier code is `QUAN`, its delivered spend and impression fields intentionally use the corresponding planned values. Other rows retain the delivered values from the underlying model.

Use the [Purely Elizabeth view builder](./create_master_ext_west_mart_data_model_purely_elizabeth.sql) to reproduce or update the view. Preserve its advertiser filter and output column order so existing dashboard fields remain compatible.

## Purely Elizabeth Delivery Plus Sales Weekly Table

The [delivery-plus-sales weekly table](https://console.cloud.google.com/bigquery?project=looker-studio-pro-452620&ws=!1m5!1m4!4m3!1slooker-studio-pro-452620!2smaster_ext_west!3smart_PE_delivery_plus_sales_weekly) combines every Purely Elizabeth campaign's delivery with available weekly retail sales. Protein Granola is currently the only product group with a [weekly retail-sales source](https://console.cloud.google.com/bigquery?project=looker-studio-pro-452620&ws=!1m5!1m4!4m3!1slooker-studio-pro-452620!2sPE!3sprotein_granola_weekly_sales). Its grain is one Monday-through-Sunday week and product group, labeled with the Sunday end date. A campaign without a valid mapping is labeled `UNMAPPED`; its delivery remains visible and its sales measures remain null. A full timeline preserves sales-only, media-only, and overlapping weeks; measures unavailable on one side remain null. The table is rebuilt every two hours by the saved `master_raw_CopyToWest` scheduled query.

The [campaign mapping table](https://console.cloud.google.com/bigquery?project=looker-studio-pro-452620&ws=!1m5!1m4!4m3!1slooker-studio-pro-452620!2sPE!3spurely_elizabeth_campaign_product_mapping) controls product-specific sales eligibility:

1. Every valid exact campaign mapping is included under its year-free product group.
2. Campaigns absent from the table remain visible as `UNMAPPED` delivery and never receive retail sales.
3. A mapped product group or `UNMAPPED` appears after it has delivery.

Use the [add or update query](./upsert_PE_purely_elizabeth_campaign_product_mapping.sql) and [remove query](./delete_PE_purely_elizabeth_campaign_product_mapping.sql) in BigQuery Studio for routine maintenance. The [mapping-table builder](./create_PE_purely_elizabeth_campaign_product_mapping.sql) creates or migrates the table structure but never changes mapping rows. When rebuilding from scratch, create the table, add its mappings, and then run the [weekly-table builder](./create_master_ext_west_mart_PE_delivery_plus_sales_weekly.sql). The table intentionally inherits the existing Purely Elizabeth view's linear and `QUAN` planned-as-delivered interpretation. Retail sales are joined by time for comparison and are not represented as campaign-attributed outcomes.

The production table intentionally exposes only `_date`, a year-free mapped `product_group` or `UNMAPPED` delivery group, eight approved media measures, and `sales_dollars` / `sales_tdp`. Campaign fields classify sales eligibility internally but are not output dimensions. The currently available Protein Granola sales source joins only to `PROTEIN GRANOLA`; other delivered product groups and `UNMAPPED` remain media-only until a matching sales source exists. Fields explicitly labeled `doNotSum` are excluded from production because they cannot be safely aggregated at weekly grain.
Weekly SPINS source loading remains a separate manual workflow. Refreshing the typed sales source updates this stored table on the next successful `master_raw_CopyToWest` run.
