# Changelog

## 2026-06-08

### Changed

- **Amazon Ads supply cost spend mapping**
  What: Mapped Amazon Ads `supply_cost` into the generic `_spend` field and preserved the raw source value as `amzn_supply_cost`.
  Why: Uses the Amazon media-cost field for spend reporting while keeping Amazon `sales` as outcome revenue instead of media spend.

  <details><summary>Paths - Amazon Ads supply cost spend mapping</summary>

  [Master data model SQL](/Users/eugenetsenter/Looker_clonedRepo/looker_personal/master_data_model/create_master_stg_data_model.sql)
  [README.md](/Users/eugenetsenter/Looker_clonedRepo/looker_personal/master_data_model/README.md)

  </details>

### Added

- **Amazon Ads source in master data model**
  What: Added Ritual Amazon Ads rows from the runner-maintained landing table to `master_stg.data_model`, mapped compatible delivery metrics into the generic model fields, and preserved Amazon report fields as `amzn_*` columns.
  Why: Makes the daily Amazon Ads report available in the downstream master model without treating Amazon sales as media spend or losing source-level report detail.

  <details><summary>Paths - Amazon Ads source in master data model</summary>

  [Master data model SQL](/Users/eugenetsenter/Looker_clonedRepo/looker_personal/master_data_model/create_master_stg_data_model.sql)
  [README.md](/Users/eugenetsenter/Looker_clonedRepo/looker_personal/master_data_model/README.md)

  </details>
