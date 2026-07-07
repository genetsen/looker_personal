# Digital Conversions Branch

This folder owns conversion outcome logic that attaches post-media actions to digital package IDs.

The current placeholder source is the BigQuery table [Ritual conversion report](https://console.cloud.google.com/bigquery?project=looker-studio-pro-452620&p=looker-studio-pro-452620&d=landing&t=rtl_conv_report&page=table). It is a Google Sheet mirror with package IDs embedded in `package_roadblock`, daily conversion activities, creative labels, and source Sheet lineage.

## Current Model Path

| Step | Grain | Purpose |
|---|---|---|
| Raw source | Package/date/site/creative/activity | Preserve the conversion report as loaded from the Sheet. |
| Normalized conversion detail | Package/date/site/creative/activity | Parse package ID, standardize field names, and keep Sheet lineage. |
| [v3 clustered table](https://console.cloud.google.com/bigquery?project=looker-studio-pro-452620&p=looker-studio-pro-452620&d=master_stg&t=data_model_v3&page=table) | Lowest available source grain | Add conversion outcome rows with delivery metrics intentionally blank. |

Conversions are outcome evidence, not media delivery. Conversion rows must not populate `_spend`, `_impressions`, `_clicks`, `_video_plays`, `_video_views`, or `_video_comps`.

## Match Rule

Conversion package IDs must match known master-model package IDs. Conversion dates can extend past the delivery row dates because post-flight attribution can happen after the media package ends. In v3, those rows use the nearest package context and are marked with `conversion_date_without_delivery_row`.

