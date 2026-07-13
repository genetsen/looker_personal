# Digital Conversion Outcomes Pipeline

This guide explains how Ritual conversion outcomes move from direct Campaign Manager 360 (CM360) history into the current V3 master evidence model. Conversion evidence stays separate from media-delivery measures so outcome rows cannot inflate spend, impressions, or clicks.

## Pipeline Overview

```text
Newest eligible CM360 rolling export
              ↓
Persistent direct conversion history
              ↓
Package + date + placement + creative summary
              ↓
Unique delivery-detail join or explicit conversion-only row
              ↓
Master evidence model V3 conversion fields
```

## Source and Output Contract

| Stage | Object or file | Grain | Responsibility |
|---|---|---|---|
| Production source | [Direct CM360 conversion history](https://console.cloud.google.com/bigquery?project=looker-studio-pro-452620&p=looker-studio-pro-452620&d=master_stg&t=rtl_cm360_direct_conversions&page=table) | Source activity record | Retains the rolling-report history, conversion and revenue fields, source keys, and refresh timestamps. |
| Compatibility reference | [Legacy-schema RTL compatibility table](https://console.cloud.google.com/bigquery?project=looker-studio-pro-452620&p=looker-studio-pro-452620&d=landing&t=rtl_conv_report&page=table) | Source activity record in the old 18-column shape | Mirrors direct history for optional comparisons. Its Google Sheet path is retired, and unavailable delivery and Sheet-lineage fields are blank. |
| Active builder | [Final model V3 SQL](/Users/eugenetsenter/Looker_clonedRepo/looker_personal/master_data_model/model/final_model/create_master_stg_data_model_v3.sql) | Delivery detail plus conversion-only evidence | Aggregates activity records, joins only to one matching delivery row, and retains unmatched outcomes without delivery measures. |
| Output | [Master evidence model V3](https://console.cloud.google.com/bigquery?project=looker-studio-pro-452620&p=looker-studio-pro-452620&d=master_stg&t=data_model_v3&page=table) | Package/date/parsed-placement/creative for conversions | Publishes direct conversion, revenue, activity, source, and refresh evidence without altering delivery totals. |

## Matching and Safety Rules

- The routine source refresh selects one newest enriched CM360 export whose report window is no wider than 14 days.
- Persistent history updates matching source records and retains older dates outside the newest rolling window.
- Conversion activities aggregate at package, date, parsed placement ID, and creative before joining delivery.
- A conversion summary attaches only when exactly one delivery row has the same detail key.
- Unmatched or ambiguous summaries remain visible as conversion-only evidence with null delivery measures.
- The compatibility table keeps its original 18 columns but does not invent impressions, clicks, campaign IDs, channel values, or Sheet lineage that direct CM360 does not supply.

## Published Conversion Fields

| Field group | Examples | Meaning |
|---|---|---|
| Totals | `conv_total_conversions`, `conv_click_through_conversions`, `conv_view_through_conversions` | Conversion counts from direct CM360. |
| Revenue | `conv_total_revenue`, `conv_click_through_revenue`, `conv_view_through_revenue` | Revenue attributed by CM360. |
| Activities | `conv_site_visits`, `conv_view_products`, `conv_add_to_carts`, `conv_begin_checkouts`, `conv_purchases` | Named outcome counts derived from source activity records. |
| Detail and match status | `conv_model_detail_key`, `conv_model_detail_join_status`, `conv_model_detail_match_count` | Shows how conversion evidence did or did not match delivery detail. |
| Freshness and source | `conv_source_table_names`, `conv_source_exported_at`, `conv_data_refresh_date`, `conv_staged_at` | Separates source-export time from staging refresh time. |

## Debugging Route

| Question | First check | Then trace |
|---|---|---|
| Why is an outcome missing? | [Direct CM360 conversion history](https://console.cloud.google.com/bigquery?project=looker-studio-pro-452620&p=looker-studio-pro-452620&d=master_stg&t=rtl_cm360_direct_conversions&page=table) and its `conversion_row_key` | Aggregate by `model_detail_key`, then inspect the V3 join status. |
| Why is delivery blank? | `conv_model_detail_join_status` | `conversion_only` and ambiguous matches intentionally retain null delivery measures. |
| Why does the compatibility table lack impressions or clicks? | The table description and legacy-only columns | Direct CM360 conversion history does not provide those delivery values, so the compatibility refresh leaves them blank. |
| Why are older dates still present? | `source_table_name` and `source_exported_at` | Persistent history keeps older evidence while the newest rolling report updates only matching recent records. |

## Related Guides

- [Direct CM360 migration guide](/Users/eugenetsenter/Looker_clonedRepo/looker_personal/master_data_model/docs/rtl-direct-cm360-conversion-migration.md)
- [Digital conversion helper guide](/Users/eugenetsenter/Looker_clonedRepo/looker_personal/master_data_model/model/branches/digital/conversions/README.md)
- [DCM delivery pipeline](/Users/eugenetsenter/Looker_clonedRepo/looker_personal/master_data_model/model/branches/dcm/README_dcm-pipeline.md)
- [Source branch index](/Users/eugenetsenter/Looker_clonedRepo/looker_personal/master_data_model/model/branches/README.md)
