# Reporting Outputs

Dashboard-facing mart and reporting output logic belongs here.

Keep this separate from the final model because dashboard filters and reporting exclusions can differ from the evidence/final table behavior.

## Purely Elizabeth West-Region View

The [Purely Elizabeth reporting view](https://console.cloud.google.com/bigquery?project=looker-studio-pro-452620&p=looker-studio-pro-452620&d=master_ext_west&t=mart_data_model_purelyElizabeth&page=table) filters the west-region model to Purely Elizabeth. For rows whose channel group is `linear`, or whose supplier code is `QUAN`, its delivered spend and impression fields intentionally use the corresponding planned values. Other rows retain the delivered values from the underlying model.

Use the [Purely Elizabeth view builder](./create_master_ext_west_mart_data_model_purely_elizabeth.sql) to reproduce or update the view. Preserve its advertiser filter and output column order so existing dashboard fields remain compatible.

## Purely Elizabeth Delivery Plus Sales Weekly Table

The [delivery-plus-sales weekly table](https://console.cloud.google.com/bigquery?project=looker-studio-pro-452620&ws=!1m5!1m4!4m3!1slooker-studio-pro-452620!2smaster_ext_west!3smart_PE_delivery_plus_sales_weekly) combines mapped Purely Elizabeth product-group delivery with available weekly retail sales. Protein Granola is currently the only product group with a [weekly retail-sales source](https://console.cloud.google.com/bigquery?project=looker-studio-pro-452620&ws=!1m5!1m4!4m3!1slooker-studio-pro-452620!2sPE!3sprotein_granola_weekly_sales). Its grain is one Monday-through-Sunday week and product group, labeled with the Sunday end date. A full timeline preserves sales-only, media-only, and overlapping weeks; measures unavailable on one side remain null. The table is rebuilt every two hours by the saved `master_raw_CopyToWest` scheduled query.

The [campaign mapping table](https://console.cloud.google.com/bigquery?project=looker-studio-pro-452620&ws=!1m5!1m4!4m3!1slooker-studio-pro-452620!2sPE!3spurely_elizabeth_campaign_product_mapping) is the complete inclusion list:

1. Every valid exact campaign mapping is included under its year-free product group.
2. Campaigns absent from the table are excluded; there is no campaign-name fallback.
3. A mapped product group appears after at least one of its campaigns has delivery.

Use the [add or update query](./upsert_PE_purely_elizabeth_campaign_product_mapping.sql) and [remove query](./delete_PE_purely_elizabeth_campaign_product_mapping.sql) in BigQuery Studio for routine maintenance. The [mapping-table builder](./create_PE_purely_elizabeth_campaign_product_mapping.sql) creates or migrates the table structure but never changes mapping rows. When rebuilding from scratch, create the table, add its mappings, and then run the [weekly-table builder](./create_master_ext_west_mart_PE_delivery_plus_sales_weekly.sql). The table intentionally inherits the existing Purely Elizabeth view's linear and `QUAN` planned-as-delivered interpretation. Retail sales are joined by time for comparison and are not represented as campaign-attributed outcomes.

The production table intentionally exposes only `_date`, a year-free mapped `product_group` with media delivery, eight approved media measures, and `sales_dollars` / `sales_tdp`. Campaign fields classify products internally but are not output dimensions. The currently available Protein Granola sales source joins only to `PROTEIN GRANOLA`; other delivered product groups remain media-only until a matching sales source exists. Fields explicitly labeled `doNotSum` are excluded from production because they cannot be safely aggregated at weekly grain.
Weekly SPINS source loading remains a separate manual workflow. Refreshing the typed sales source updates this stored table on the next successful `master_raw_CopyToWest` run.
