# Changelog

All notable changes to this repository are documented in this file.

## 2026-05-19

### Changed

- **Manual Package Editor Package-Level Metadata Overrides**
  What: Split package flight dates from delivery override dates, made visible package metadata edits write backend `man_*` metadata fields, and updated the master model so package metadata overrides apply across every row for the package while delivered metric overrides remain limited to their selected delivery dates.
  Why: Lets users correct package grouping fields such as GS Channel, Campaign, Package Name, Package Type, supplier, and initiative without creating date-limited metric side effects, while preserving the ability to make one-day or one-week delivered metric corrections.

  <details><summary>Paths - Manual Package Editor Package-Level Metadata Overrides</summary>

  [master_data_model/create_manual_package_edit_tables.sql](master_data_model/create_manual_package_edit_tables.sql)
  [master_data_model/create_master_stg_data_model.sql](master_data_model/create_master_stg_data_model.sql)
  [master_data_model/manual_package_edits/load_manual_package_edits.R](master_data_model/manual_package_edits/load_manual_package_edits.R)
  [master_data_model/manual_package_edits/setup_manual_package_editor_sheet.mjs](master_data_model/manual_package_edits/setup_manual_package_editor_sheet.mjs)
  [master_data_model/manual_package_edits/README.md](master_data_model/manual_package_edits/README.md)
  [master_data_model/README.md](master_data_model/README.md)
  [CHANGELOG.md](CHANGELOG.md)

  </details>

### Fixed

- **Manual Package Editor Formatting And Exact Totals**
  What: Fixed the manual package editor setup so internal baseline/manual-marker columns stay hidden after formatting, added number/date formats to hidden baseline helper columns, and changed manual daily allocation so count metrics preserve exact replacement totals as whole daily units while spend metrics preserve exact totals by cents.
  Why: Prevents backend helper columns and raw date serials from appearing in the user-facing sheet, and makes manual replacement totals land exactly in the daily table, final data model, and reporting mart instead of drifting by small floating-point amounts.

  <details><summary>Paths - Manual Package Editor Formatting And Exact Totals</summary>

  [master_data_model/manual_package_edits/setup_manual_package_editor_sheet.mjs](master_data_model/manual_package_edits/setup_manual_package_editor_sheet.mjs)
  [master_data_model/manual_package_edits/load_manual_package_edits.R](master_data_model/manual_package_edits/load_manual_package_edits.R)
  [master_data_model/manual_package_edits/README.md](master_data_model/manual_package_edits/README.md)
  [master_data_model/README.md](master_data_model/README.md)
  [CHANGELOG.md](CHANGELOG.md)

  </details>

## 2026-05-15

### Added

- **Master Data Model Manual Package Edit Path**
  What: Added a manual package edit path with landing table schemas, a single-tab Google Sheet package editor, a loader that detects in-place edits against the live mart and last run, and master-model logic that applies backend `man_` values before final package rollups.
  Why: Lets users find a package and edit the dashboard value directly without separate lookup, replacement, delta, validation, or proof tabs while keeping manual overrides visible and auditable in the final model.

  <details><summary>Paths - Master Data Model Manual Package Edit Path</summary>

  [master_data_model/create_manual_package_edit_tables.sql](master_data_model/create_manual_package_edit_tables.sql)
  [master_data_model/manual_package_edits/load_manual_package_edits.R](master_data_model/manual_package_edits/load_manual_package_edits.R)
  [master_data_model/manual_package_edits/setup_manual_package_editor_sheet.mjs](master_data_model/manual_package_edits/setup_manual_package_editor_sheet.mjs)
  [master_data_model/create_master_stg_data_model.sql](master_data_model/create_master_stg_data_model.sql)
  [master_data_model/create_ritual_data_model_view.sql](master_data_model/create_ritual_data_model_view.sql)
  [master_data_model/create_ritual_data_model_view_v2.sql](master_data_model/create_ritual_data_model_view_v2.sql)
  [master_data_model/README.md](master_data_model/README.md)
  [CHANGELOG.md](CHANGELOG.md)

  </details>

- **Manual Package Editor UX Setup Split**
  What: Split Google Sheet formatting and user-experience setup out of the R loader into a standalone sheet setup script, added native slicers for Advertiser/Channel/Campaign/Site browsing, added a dedicated `Instructions` tab, and kept required metadata columns visible at the far right while hiding only internal baseline comparison columns.
  Why: Keeps the loader focused on data refresh and backend writes, while browsing controls, table formatting, frozen panes, instructions, metadata visibility, and editable-cell styling are managed as intentional sheet configuration.

- **Manual Package Editor Planned Metrics And Request Flow**
  What: Added changed-cell conditional formatting against hidden baseline columns, restricted planned metric edits to full-flight rows, kept planned totals visible as flight totals, added a sheet-level update-request control that emails Gene, registered the loader in the universal script runner, and changed `_package_name_friendly` to fall back to the full package name when blank.
  Why: Makes the sheet easier for media buyers to use safely while keeping backend `man_*` evidence, final planned totals, and package lookup labels consistent.

### Changed

- **Master Data Model Initiative Consolidation**
  What: Moved `initiative` into `master_stg.data_model`, simplified `master_stg.data_model_v2` to a compatibility `SELECT *` wrapper over the main model, and refreshed `master_stg.data_model_mart` so the reporting mart includes the consolidated field.
  Why: Makes `data_model` the owner of the package/date reporting shape while keeping existing v2 consumers working without duplicate initiative logic.

  <details><summary>Paths - Master Data Model Initiative Consolidation</summary>

  [master_data_model/create_master_stg_data_model.sql](master_data_model/create_master_stg_data_model.sql)
  [master_data_model/create_master_stg_data_model_v2.sql](master_data_model/create_master_stg_data_model_v2.sql)
  [master_data_model/create_master_stg_data_model_mart.sql](master_data_model/create_master_stg_data_model_mart.sql)
  [master_data_model/README.md](master_data_model/README.md)
  [CHANGELOG.md](CHANGELOG.md)

  </details>

## 2026-05-08

### Changed

- **Master Data Model Planned Metrics And Callout Cleanup**
  What: Reconciled the local master SQL with the live `master_stg.data_model` definition, carried the FPD creative image link field through the model, populated linear TV planned spend and impressions from TV delivery values, backfilled planned impressions from final impressions when spend exists on both plan and actuals, and simplified row callouts so source/cause labels suppress redundant metric symptoms. Added the `spend_no_imps` callout for rows with planned and final spend but zero planned and final impressions.
  Why: Keeps local SQL deploy-safe against production, makes TV planned metrics usable, and gives dashboard users shorter row callouts that point to the real issue instead of repeating symptoms.
  <details><summary>Paths - Master Data Model Planned Metrics And Callout Cleanup</summary>

  [master_data_model/create_master_stg_data_model.sql](master_data_model/create_master_stg_data_model.sql)
  [master_data_model/README.md](master_data_model/README.md)
  [master_data_model/AGENTS.md](master_data_model/AGENTS.md)
  [CHANGELOG.md](CHANGELOG.md)

  </details>

## 2026-05-01

### Changed

- **Master Data Model Interactive Map Refinement**
  What: Reworked the map into collapsible parallel lanes for digital, social, and TV capture; clarified that the lanes meet only at union and rollups; showed the Ritual slice as flowing out of the Master View; rewrote the node descriptions to focus on production fields, mappings, and caveats; and added update paths for the upstream tables/views.
  Why: Makes the map match the real model shape and shows which upstream objects need to refresh before each production element is current.
  <details><summary>Paths - Master Data Model Interactive Map Refinement</summary>

  [master_data_model/docs/master-data-model-map.html](master_data_model/docs/master-data-model-map.html)
  [CHANGELOG.md](CHANGELOG.md)

  </details>

- **Master Data Model Count Documentation Policy**
  What: Added the repo rule that fluctuating master-model row counts, source mix counts, package-key counts, and media totals should be checked live for QA instead of tracked in durable docs.
  Why: Prevents README and map churn when source tables refresh naturally.
  <details><summary>Paths - Master Data Model Count Documentation Policy</summary>

  [AGENTS.md](AGENTS.md)
  [master_data_model/README.md](master_data_model/README.md)
  [master_data_model/docs/master-data-model-map.html](master_data_model/docs/master-data-model-map.html)
  [CHANGELOG.md](CHANGELOG.md)

  </details>

- **Master Data Model Source Issue QA Candidate**
  What: Added candidate logic for `row_data_sources_available`, `row_data_issue_category`, and `row_data_callouts`; removed the Prisma-only gate from DCM and FPD inputs; kept non-Prisma rows out of `final_*` metrics; moved DCM low-signal filtering into a new reporting mart script; and validated the result in QA views without replacing production.
  Why: Keeps the master model as an evidence layer while letting the mart apply reporting-only exclusions and recalculate package rollups after filtering.
  <details><summary>Paths - Master Data Model Source Issue QA Candidate</summary>

  [master_data_model/create_master_stg_data_model.sql](master_data_model/create_master_stg_data_model.sql)
  [master_data_model/create_master_stg_data_model_mart.sql](master_data_model/create_master_stg_data_model_mart.sql)
  [master_data_model/README.md](master_data_model/README.md)
  [CHANGELOG.md](CHANGELOG.md)

  </details>

## 2026-04-29

### Changed

- **BigQuery Documentation Orientation Rule**
  What: Updated the repo instructions to allow narrow Markdown docs as orientation before BigQuery inspection while still requiring production validation before relying on doc or local SQL takeaways.
  Why: Keeps repo docs useful for navigation without letting stale local guidance override the live warehouse.
  <details><summary>Paths - BigQuery Documentation Orientation Rule</summary>

  [AGENTS.md](AGENTS.md)
  [README.md](README.md)
  [CHANGELOG.md](CHANGELOG.md)

  </details>

### Added

- **Master Data Model TV Layer Deployment**
  What: Added `landing.tv_combined` as a TV branch in the master data model SQL, preserving TV source fields in `tv_*` columns, validating the candidate in `master_stg.data_model_qa_tv_layer`, and deploying it to production `master_stg.data_model`.
  Why: Brings local and national TV estimate rows into the cross-client model with production proof for date gates, synthetic keys, and TV field coverage.
  <details><summary>Paths - Master Data Model TV Layer QA Candidate</summary>

  [master_data_model/create_master_stg_data_model.sql](master_data_model/create_master_stg_data_model.sql)
  [master_data_model/README.md](master_data_model/README.md)
  [README.md](README.md)
  [CHANGELOG.md](CHANGELOG.md)

  </details>

- **Master Data Model Project Folder**
  What: Moved the generalized `looker-studio-pro-452620.master_stg.data_model` SQL and continuation notes into the top-level `master_data_model/` project folder, with a dedicated README covering purpose, sources, verification, deploy commands, and QA queries.
  Why: Keeps the cross-client master model out of the ADIF project boundary while preserving enough context to continue work seamlessly.
  <details><summary>Paths — Master Data Model Project Folder</summary>

  [master_data_model/README.md](master_data_model/README.md)
  [master_data_model/create_master_stg_data_model.sql](master_data_model/create_master_stg_data_model.sql)
  [README.md](README.md)
  [CHANGELOG.md](CHANGELOG.md)

  </details>

- **Ritual Master Data Model View**
  What: Created `looker-studio-pro-452620.master_stg.ritual_data_model`, a Ritual-only view over the generalized master data model, and saved the reusable SQL definition.
  Why: Gives Ritual a stable client-specific surface while keeping the generalized master model unchanged.
  <details><summary>Paths — Ritual Master Data Model View</summary>

  [master_data_model/README.md](master_data_model/README.md)
  [master_data_model/create_ritual_data_model_view.sql](master_data_model/create_ritual_data_model_view.sql)
  [CHANGELOG.md](CHANGELOG.md)

  </details>

## 2026-04-06

### Fixed

- **TV Gmail impressions header compatibility**
  Issue: The local and national Gmail-to-BigQuery TV loaders stopped reading impressions when the source CSV exports changed their planned-impressions header names.
  Cause: The scripts only recognized older impressions columns and missed newer variants such as `total_planned_impressions_all_demos`, `total_planned_impressions_all_demos_000`, and `total_planned_impressions_000`.
  Resolution: Updated both TV Gmail loaders to prefer the current planned-impressions headers, then changed the national loader to use objective impressions only when a row's planned impressions are blank or 0; verified the new mappings against the latest local and national Gmail source CSVs. -codexapp thread `019d649f-e2a4-7032-8f6b-33f234873900`

## 2026-02-13

### Added

- **Subrepo AGENTS Starter Template**
  What: Added a reusable template for project-level `AGENTS.md` files to support consistent instructions in nested project repos.
  Why: Standardizes how project-specific agent rules are documented while keeping monorepo and subrepo responsibilities clear.
  <details><summary>Paths — Subrepo AGENTS Starter Template</summary>

  [docs/SUBREPO_AGENTS_TEMPLATE.md](docs/SUBREPO_AGENTS_TEMPLATE.md)
  [CHANGELOG.md](CHANGELOG.md)

  </details>

### Changed

- **Monorepo and Subrepo Boundary Model**
  What: Added explicit monorepo/subrepo structure guidance in the root README, including ownership rules and where to keep shared vs project-specific assets.
  Why: Reduces structural ambiguity and supports separate instruction files per project section.
  <details><summary>Paths — Monorepo and Subrepo Boundary Model</summary>

  [README.md](README.md)
  [CHANGELOG.md](CHANGELOG.md)

  </details>

- **Dual AGENTS Reference Policy**
  What: Updated root and subrepo instruction docs to require that each subrepo `AGENTS.md` references both the monorepo `AGENTS.md` and its own local `AGENTS.md`.
  Why: Ensures consistent global guidance while preserving project-specific instruction behavior in each subrepo.
  <details><summary>Paths — Dual AGENTS Reference Policy</summary>

  [README.md](README.md)
  [docs/SUBREPO_AGENTS_TEMPLATE.md](docs/SUBREPO_AGENTS_TEMPLATE.md)
  [mft/AGENTS.md](mft/AGENTS.md)
  [CHANGELOG.md](CHANGELOG.md)

  </details>

- **Single-Repo Default Policy**
  What: Updated the root README to make a single repo with folders the default long-term structure, with subrepos treated as exceptions only when strict criteria are met.
  Why: Reduces Git complexity and lowers operational risk for day-to-day maintenance.
  <details><summary>Paths — Single-Repo Default Policy</summary>

  [README.md](README.md)
  [CHANGELOG.md](CHANGELOG.md)

  </details>

- **Dev Cutover Restore Runbook**
  What: Added explicit rollback and restore instructions to the root README for the ADIF-to-dev cutover, including stash recovery, backup-branch reset, remote rollback push, and verification checks.
  Why: Makes recovery steps repeatable and reduces risk during branch cutovers used by external workspace integrations.
  <details><summary>Paths — Dev Cutover Restore Runbook</summary>

  [README.md](README.md)
  [CHANGELOG.md](CHANGELOG.md)

  </details>

- **Git Beginner Daily Cheat Sheet**
  What: Added a step-by-step Git beginner section to the root README with safe daily commands for status checks, sync, branch creation, commit/push, stash recovery, and rollback.
  Why: Reduces operator anxiety and makes common Git tasks repeatable with a low-risk workflow.
  <details><summary>Paths — Git Beginner Daily Cheat Sheet</summary>

  [README.md](README.md)
  [CHANGELOG.md](CHANGELOG.md)

  </details>

- **Git Practice TODO Tracking**
  What: Added a recurring practice TODO in the project backlog to reinforce the new Git beginner drill workflow.
  Why: Keeps skill-building visible in day-to-day priorities and supports consistent repetition.
  <details><summary>Paths — Git Practice TODO Tracking</summary>

  [AGENTS.md](AGENTS.md)
  [CHANGELOG.md](CHANGELOG.md)

  </details>

- **Deferred Main/Dev Trial Alignment Task**
  What: Added a deferred task in the root README to run a non-destructive trial branch alignment and smoke-test checklist before simplifying `main` and `dev`.
  Why: Reduces risk of delayed breakage by requiring validation before promoting branch-history changes.
  <details><summary>Paths — Deferred Main/Dev Trial Alignment Task</summary>

  [README.md](README.md)
  [CHANGELOG.md](CHANGELOG.md)

  </details>

## 2026-02-12

### Added

- **Basis UTMs Workspace README**
  What: Added a dedicated utilities README describing how Basis UTM assets are organized into active and archived subfolders.
  Why: Creates a clear entrypoint for running current scripts and locating legacy references.
  <details><summary>Paths — Basis UTMs Workspace README</summary>

  [util/basis_utms/README.md](util/basis_utms/README.md)

  </details>

- **FY26-to-0929 Basis UTM Backfill Script**
  What: Added an idempotent SQL script to insert missing rows from `landing.basis_utms_pivoted_fy26_q1` into `landing.basis_utms_unioned-0929`, with before/after row-count output.
  Why: Provides a repeatable way to update the pipeline’s `-0929` unioned table from newly loaded FY26 Q1 pivoted data.
  <details><summary>Paths — FY26-to-0929 Basis UTM Backfill Script</summary>

  [util/basis_utms/essential/load_basis_utms_unioned_0929_from_fy26_q1.sql](util/basis_utms/essential/load_basis_utms_unioned_0929_from_fy26_q1.sql)
  [util/basis_utms/README.md](util/basis_utms/README.md)

  </details>

### Changed

- **Basis UTMs Script Reorganization**
  What: Reorganized Basis UTM R/SQL assets into `util/basis_utms/essential` for active workflows and `util/basis_utms/archive` for legacy/scratch artifacts.
  Why: Reduces root-level utility clutter and separates operational scripts from historical/debug materials.
  <details><summary>Paths — Basis UTMs Script Reorganization</summary>

  [util/basis_utms/essential/util__basis__utm_pivot_longer_loop.r](util/basis_utms/essential/util__basis__utm_pivot_longer_loop.r)
  [util/basis_utms/essential/util_b_utm_validation.r](util/basis_utms/essential/util_b_utm_validation.r)
  [util/basis_utms/essential/stg3_b_plus_utms_PnS.sql](util/basis_utms/essential/stg3_b_plus_utms_PnS.sql)
  [util/basis_utms/essential/get_distinct_creative_names.sql](util/basis_utms/essential/get_distinct_creative_names.sql)
  [util/basis_utms/archive/util__basis__utm_pivot_longer.r](util/basis_utms/archive/util__basis__utm_pivot_longer.r)
  [util/basis_utms/archive/util__basis__utm_pivot_longer_clean.r](util/basis_utms/archive/util__basis__utm_pivot_longer_clean.r)
  [util/basis_utms/archive/scrap.sql](util/basis_utms/archive/scrap.sql)
  [util/basis_utms/archive/testsAndScrap.sql](util/basis_utms/archive/testsAndScrap.sql)
  [util/basis_utms/archive/utm_validation_scrap.sql](util/basis_utms/archive/utm_validation_scrap.sql)
  [util/basis_utms/archive/union_basis_utms.ipynb](util/basis_utms/archive/union_basis_utms.ipynb)
  [util/basis_utms/archive/b_utms_diagram.md](util/basis_utms/archive/b_utms_diagram.md)

  </details>

- **FY26-to-0929 Backfill Size Mapping**
  What: Updated the FY26 backfill script to derive `size` from creative fields (`name`, `tag_placement`, `line_item`) instead of inserting `NULL`.
  Why: Preserves size information when loading `basis_utms_unioned-0929` and keeps idempotent dedupe matching aligned with inserted size values.
  <details><summary>Paths — FY26-to-0929 Backfill Size Mapping</summary>

  [util/basis_utms/essential/load_basis_utms_unioned_0929_from_fy26_q1.sql](util/basis_utms/essential/load_basis_utms_unioned_0929_from_fy26_q1.sql)
  [CHANGELOG.md](CHANGELOG.md)

  </details>

- **Basis UTMs Documentation Path Updates**
  What: Updated MFT and root README references to point at the new Basis UTMs folder structure.
  Why: Prevents stale links and keeps project navigation accurate after the script move.
  <details><summary>Paths — Basis UTMs Documentation Path Updates</summary>

  [mft/README.md](mft/README.md)
  [README.md](README.md)

  </details>

- **FY26 Q1 Traffic Sheet Source Configuration**
  What: Added a new `fy26_q1` source in the Basis UTM loop script for `/Users/eugenetsenter/Downloads/MassMutual_FY26_Q1_Traffic Sheet.xlsx` using tab `MASSMUTUAL004_updated 1.14.26`, plus included it in `sources_to_process`.
  Why: Ensures the FY26 Q1 MASSMUTUAL004 trafficking sheet is processed in the existing batch ingestion flow.
  <details><summary>Paths — FY26 Q1 Traffic Sheet Source Configuration</summary>

  [util/basis_utms/essential/util__basis__utm_pivot_longer_loop.r](util/basis_utms/essential/util__basis__utm_pivot_longer_loop.r)
  [CHANGELOG.md](CHANGELOG.md)

  </details>

- **Basis UTM Union and Staging Critical SQL Coverage**
  What: Updated the Basis UTM union SQL to include `landing.basis_utms_pivoted_fy26_q1` and documented `load_basis_utms_union.sql` plus `stg__basis__utms.sql` as pipeline-critical dependencies in project docs.
  Why: Keeps the downstream staging view in sync with the new FY26 Q1 source and makes core SQL dependencies explicit for ongoing operations.
  <details><summary>Paths — Basis UTM Union and Staging Critical SQL Coverage</summary>

  [util/basis_utms/essential/load_basis_utms_union.sql](util/basis_utms/essential/load_basis_utms_union.sql)
  [util/basis_utms/essential/stg__basis__utms.sql](util/basis_utms/essential/stg__basis__utms.sql)
  [util/basis_utms/README.md](util/basis_utms/README.md)
  [mft/README.md](mft/README.md)

  </details>

- **Basis UTM Core SQL Relocation to Essential Folder**
  What: Moved `load_basis_utms_union.sql` and `stg__basis__utms.sql` into `util/basis_utms/essential` and updated cross-doc references.
  Why: Keeps all operational Basis UTM extraction and staging assets co-located in one maintained essential workspace.
  <details><summary>Paths — Basis UTM Core SQL Relocation to Essential Folder</summary>

  [util/basis_utms/essential/load_basis_utms_union.sql](util/basis_utms/essential/load_basis_utms_union.sql)
  [util/basis_utms/essential/stg__basis__utms.sql](util/basis_utms/essential/stg__basis__utms.sql)
  [util/basis_utms/README.md](util/basis_utms/README.md)
  [mft/README.md](mft/README.md)
  [CLAUDE.md](CLAUDE.md)

  </details>

- **Basis UTM Rebuild and Cleanup TODOs**
  What: Added explicit project TODOs to rebuild the Basis UTM pipeline with one canonical runbook and clean up confusing remnant local/warehouse assets.
  Why: Reduces operational ambiguity and prepares a safer long-term maintenance path for the Basis-to-MFT endpoint workflow.
  <details><summary>Paths — Basis UTM Rebuild and Cleanup TODOs</summary>

  [AGENTS.md](AGENTS.md)
  [CHANGELOG.md](CHANGELOG.md)

  </details>

- **Systemwide SQL QA Safety Gate**
  What: Added a systemwide SQL QA protocol requiring isolated `_qa` validation, proof output review, and explicit approval before live SQL patches.
  Why: Prevents accidental production-impacting SQL changes and makes QA evidence-driven by default.
  <details><summary>Paths — Systemwide SQL QA Safety Gate</summary>

  [AGENTS.md](AGENTS.md)
  [README.md](README.md)
  [CHANGELOG.md](CHANGELOG.md)

  </details>

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
