# Changelog

## 2026-06-15

| Area | Change | Verification |
| --- | --- | --- |
| RTL conversion reporting | Added `load_rtl_conv_report.R` to mirror the Ritual reporting sheet's `raw sales` tab into [RTL conversion report landing table](https://console.cloud.google.com/bigquery?project=looker-studio-pro-452620&p=looker-studio-pro-452620&d=landing&t=rtl_conv_report&page=table). | Live run loaded 2,482 rows with `data_refresh_date = 2026-06-15`. |
| Universal runner | Added the new loader as the daily runner entry `RTL Conversion Report`. | Runner parse check passed after config update. |
