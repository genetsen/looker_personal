# Reporting Outputs

Dashboard-facing mart and reporting output logic belongs here.

Keep this separate from the final model because dashboard filters and reporting exclusions can differ from the evidence/final table behavior.

## Purely Elizabeth West-Region View

The [Purely Elizabeth reporting view](https://console.cloud.google.com/bigquery?project=looker-studio-pro-452620&p=looker-studio-pro-452620&d=master_ext_west&t=mart_data_model_purelyElizabeth&page=table) filters the west-region model to Purely Elizabeth. For rows whose channel group is `linear`, or whose supplier code is `QUAN`, its delivered spend and impression fields intentionally use the corresponding planned values. Other rows retain the delivered values from the underlying model.

Use the [Purely Elizabeth view builder](./create_master_ext_west_mart_data_model_purely_elizabeth.sql) to reproduce or update the view. Preserve its advertiser filter and output column order so existing dashboard fields remain compatible.
