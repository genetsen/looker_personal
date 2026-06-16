# Changelog

## 2026-06-16

### Added

- **DCM Cost Model Interactive Map -codexapp [thread](https://chatgpt.com/codex)**
  What: Added a standalone clickable DCM cost-model map, linked it from the Master Model maps, docs index, and README, and clarified old caution labels as warnings.
  Why: Makes the scheduled query, package rollups, pricing logic, creative-safe join key, and Master Model handoff easier to inspect without reading the full SQL first.

  <details><summary>Paths - DCM Cost Model Interactive Map</summary>

  [DCM cost model map](/Users/eugenetsenter/Looker_clonedRepo/looker_personal/master_data_model/docs/dcm-cost-model-map.html)
  [Master model map](/Users/eugenetsenter/Looker_clonedRepo/looker_personal/master_data_model/docs/master-data-model-map.html)
  [Master model map v2](/Users/eugenetsenter/Looker_clonedRepo/looker_personal/master_data_model/docs/master-data-model-map-v2.html)
  [Documentation index](/Users/eugenetsenter/Looker_clonedRepo/looker_personal/master_data_model/docs/index.md)
  [README.md](/Users/eugenetsenter/Looker_clonedRepo/looker_personal/master_data_model/README.md)

  </details>

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
