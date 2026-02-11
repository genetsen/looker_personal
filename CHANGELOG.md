# Changelog

All notable changes to this repository are documented in this file.

## [Unreleased]

### Added - 2026-02-06

- Added FPD loader documentation TODO to create a reusable Codex skill for Excel-to-standard-input conversion in `/Users/eugenetsenter/Looker_clonedRepo/looker_personal/util/data_loaders/FPD_loader/README.md`.
- Added ADIF social layering SQL at `/Users/eugenetsenter/Looker_clonedRepo/looker_personal/adif/sql/stg__adif__social_crossplatform.sql`.
- Added cross-brand data flow diagram documentation for OLI, MassMutual, and ADIF at `/Users/eugenetsenter/Looker_clonedRepo/looker_personal/docs/DATA_FLOW_DIAGRAMS.md` covering ingestion -> BigQuery -> dbt -> dashboards.

### Changed - 2026-02-06

- Updated ADIF social staging filter in `/Users/eugenetsenter/Looker_clonedRepo/looker_personal/adif/sql/stg__adif__social_crossplatform.sql` to require both an `account_name` allowlist (`ADIF USA`, `A Diamond is Forever - US`, `A Diamond is Forever`, `De Beers Group`) and literal `WP_` presence in `campaign_name`.

### Changed - 2026-02-09

- Updated `/Users/eugenetsenter/Looker_clonedRepo/looker_personal/util/data_loaders/FPD_loader/util_collect_fpd_v3.r` so final Phase 7 outputs round `impressions` and `clicks` to whole-number integers before validation, CSV output, and BigQuery upload.
- Enhanced `/Users/eugenetsenter/Looker_clonedRepo/looker_personal/util/data_loaders/FPD_loader/util_collect_fpd_v3.r` to add Phase 6 filter-audit output (`phase6_filter_audit.csv`), save a full Phase 5-vs-Phase 7 validation table (`phase7_validation_table.csv`), add aggregated `filter_reason`, and print mismatches at end-of-run output.
- Updated `/Users/eugenetsenter/Looker_clonedRepo/looker_personal/adif/sql/build__adif__prisma_expanded_plus_dcm_with_social_tbl.sql` to clone base schema/data from `looker-studio-pro-452620.repo_stg.adif__prisma_expanded_plus_dcm_updated_fpd_view` instead of `...view_v3_test`, preserving updated-FPD fields in the social-layered table.
- Updated `/Users/eugenetsenter/Looker_clonedRepo/looker_personal/adif/sql/query__adif__prisma_expanded_plus_dcm_with_social_tbl_sched.sql` to use `repo_stg.adif__prisma_expanded_plus_dcm_updated_fpd_view` for both schema cloning and final base-row union output.
- Updated `/Users/eugenetsenter/Looker_clonedRepo/looker_personal/AGENTS.md` with sectioned workflow formatting and the ADIF social layer + updated FPD run commands.
