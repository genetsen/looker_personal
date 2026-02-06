# Changelog

All notable changes to this repository are documented in this file.

## [Unreleased]

### Added - 2026-02-06

- Added ADIF social layering SQL at `/Users/eugenetsenter/Looker_clonedRepo/looker_personal/adif/sql/stg__adif__social_crossplatform.sql`.
- Added root documentation in `/Users/eugenetsenter/Looker_clonedRepo/looker_personal/README.md`, including the ADIF social layering capability and default output view.
- Added cross-brand data flow diagram documentation for OLI, MassMutual, and ADIF at `/Users/eugenetsenter/Looker_clonedRepo/looker_personal/docs/DATA_FLOW_DIAGRAMS.md` covering ingestion -> BigQuery -> dbt -> dashboards.

### Changed - 2026-02-06

- Updated ADIF social staging filter in `/Users/eugenetsenter/Looker_clonedRepo/looker_personal/adif/sql/stg__adif__social_crossplatform.sql` to require both an `account_name` allowlist (`ADIF USA`, `A Diamond is Forever - US`, `A Diamond is Forever`, `De Beers Group`) and literal `WP_` presence in `campaign_name`.
