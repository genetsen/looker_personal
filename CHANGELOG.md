# Changelog

All notable changes to this repository are documented in this file.

## 2026-04-07

### Changed

- **FPD shortcuts-folder Drive metadata extraction refactor**
  What: shortcut sheets now use the target spreadsheet's Drive modified time for cache checks; before, the same per-sheet `.rds` cache was already reused when the stored timestamp matched, but the timestamp pull was done inline and was easier to misread.
  Why: keeps cache reuse tied to the real sheet instead of the shortcut wrapper, while making the modified-date logic clearer. -codexapp (thread link unavailable in local session).
  <details><summary>Paths — FPD shortcuts-folder Drive metadata extraction refactor</summary>

  [FPD/FPD_loader/util_collect_fpd_shortcutsFolder.r](FPD/FPD_loader/util_collect_fpd_shortcutsFolder.r)
  [CHANGELOG.md](CHANGELOG.md)

  </details>

- **FPD shortcuts-folder cache usage tracking**
  What: added reusable cache-status helpers and threaded `cache_used`, `cache_fields`, and `cache_last_modified_time` through the Phase 2, Phase 3, and Phase 5 outputs in `util_collect_fpd_shortcutsFolder.r`.
  Why: makes it visible when sheet data came from cache versus a fresh read, which helps debugging and auditability across the loader phases. -codexapp (thread link unavailable in local session).
  <details><summary>Paths — FPD shortcuts-folder cache usage tracking</summary>

  [FPD/FPD_loader/util_collect_fpd_shortcutsFolder.r](FPD/FPD_loader/util_collect_fpd_shortcutsFolder.r)
  [CHANGELOG.md](CHANGELOG.md)

  </details>

### Fixed

- **FPD shortcuts-folder BigQuery upload startup**
  Issue: `util_collect_fpd_shortcutsFolder.r` failed at the BigQuery upload step with `could not find function "bq_table"`.
  Cause: the script called BigQuery helper functions without loading the `bigrquery` package first.
  Verification: pending
  <details><summary>Paths — FPD shortcuts-folder BigQuery upload startup</summary>

  [FPD/FPD_loader/util_collect_fpd_shortcutsFolder.r](FPD/FPD_loader/util_collect_fpd_shortcutsFolder.r)
  [CHANGELOG.md](CHANGELOG.md)

  </details>

## 2026-04-02

### Added

- **Prisma sibling view with package-level FPD actuals**
  What: added a new Prisma SQL definition that builds the sibling view `looker-studio-pro-452620.Prisma.prisma_processed_plusDCMFPD`, preserving the current package-level DCM rollup while adding package-level FPD impressions, spend, clicks, and FPD date bounds from `landing.fpd_data_ranged_shortcutsFolder`. -codexapp (thread link unavailable in local session).
  Why: lets Prisma packages count partner-reported FPD delivery when DCM is missing without changing or overwriting the existing DCM fields.
  <details><summary>Paths — Prisma sibling view with package-level FPD actuals</summary>

  [Prisma/prisma_processed_plusDCMFPD.sql](Prisma/prisma_processed_plusDCMFPD.sql)
  [docs/SCHEDULED_QUERIES.md](docs/SCHEDULED_QUERIES.md)
  [README.md](README.md)
  [CHANGELOG.md](CHANGELOG.md)

  </details>

## 2026-04-01

### Added

- **Shared BigQuery loader alert helper**
  What: added one reusable R helper in `util/R_functions` that centralizes BigQuery overwrite, schema-retry fallback, failure email formatting, script-path capture, BigQuery table deeplink generation, and a shared helper README for update instructions. -codexapp (thread link unavailable in local session).
  Why: makes the failure-alert workflow reusable across scripts and keeps future maintenance in one shared helper instead of repeated inline blocks.
  <details><summary>Paths — Shared BigQuery loader alert helper</summary>

  [util/R_functions/bq_write_with_email_alerts.r](util/R_functions/bq_write_with_email_alerts.r)
  [util/R_functions/README.md](util/R_functions/README.md)
  [util/data_loaders/shared_bq_write_with_alerts.r](util/data_loaders/shared_bq_write_with_alerts.r)
  [util/data_loaders/README.md](util/data_loaders/README.md)
  [README.md](README.md)
  [CHANGELOG.md](CHANGELOG.md)

  </details>

### Changed

- **TV Gmail loader failure emails now include script location and table shortcut**
  What: updated the shared failure email format so alerts now use the subject line `🚨 Error in (R) Script | [script_name] failed to update [table_name]`, show a 12-hour timestamp without seconds, include direct paths to the failed script folder plus the helper script and helper README for update instructions, and safely MIME-chunk long Unicode subjects so Gmail renders them correctly. -codexapp (thread link unavailable in local session).
  Why: makes alert emails easier to scan in the inbox and easier to act on when you need to inspect or update the shared alert logic.
  <details><summary>Paths — TV Gmail loader failure emails now include script location and table shortcut</summary>

  [util/R_functions/bq_write_with_email_alerts.r](util/R_functions/bq_write_with_email_alerts.r)
  [util/R_functions/README.md](util/R_functions/README.md)
  [util/data_loaders/gmail_to_bq__tv_local.r](util/data_loaders/gmail_to_bq__tv_local.r)
  [util/data_loaders/gmail_to_bq__tv_nat.r](util/data_loaders/gmail_to_bq__tv_nat.r)
  [util/data_loaders/README.md](util/data_loaders/README.md)
  [README.md](README.md)
  [CHANGELOG.md](CHANGELOG.md)

  </details>

### Fixed

- **TV loader helper lookup in the universal runner**
  Issue: the local and national TV loaders failed at startup during universal-runner execution because they could not find the shared BigQuery alert helper.
  Cause: when the loaders were sourced by the external runner, their helper-path search only checked direct-run locations and could resolve the runner file instead of the loader folder.
  Resolution: added runner-compatible helper search paths for the `util/data_loaders -> ../R_functions` layout plus the local compatibility shim, and verified the exact runner-style lookup now resolves and sources the shared helper successfully. -codexapp (thread link unavailable in local session).

## 2026-03-31

### Added

- **Olipop folder guide and MMM dependency documentation**
  What: added a beginner-friendly Olipop folder guide that explains each SQL file in `/olipop`, documents the live `looker-studio-pro-452620.Olipop.MMM_crossplatform` dependency graph, and records which upstream objects are documented here versus in other repo docs. -codexapp (thread link unavailable in local session).
  Why: gives one clear source of truth for understanding the Olipop reporting path without reverse-engineering the warehouse object tree by hand.
  <details><summary>Paths — Olipop folder guide and MMM dependency documentation</summary>

  [olipop/README.md](olipop/README.md)
  [README.md](README.md)
  [CHANGELOG.md](CHANGELOG.md)

  </details>

### Changed

- **Olipop raw-table runbook now matches live production behavior**
  What: updated the shared scheduled-query documentation to reflect the live `stg__olipop__crossplatform_raw_tbl_sched` logic, including runtime source selection between the two ad-reporting tables, the current `video_flag` rule, and a link back to the new Olipop guide. -codexapp (thread link unavailable in local session).
  Why: keeps shared docs aligned with production after the local SQL and the live scheduled query drifted apart.
  <details><summary>Paths — Olipop raw-table runbook now matches live production behavior</summary>

  [docs/SCHEDULED_QUERIES.md](docs/SCHEDULED_QUERIES.md)
  [olipop/README.md](olipop/README.md)
  [README.md](README.md)
  [CHANGELOG.md](CHANGELOG.md)

  </details>

## 2026-03-30

### Added

- **Shared TV Gmail loader documentation**
  What: added one detailed documentation file for both TV Gmail loaders, covering the Gmail search rules, attachment selection, column mapping, BigQuery write behavior, differences between local and national runs, and troubleshooting notes for common failures. -codexapp (thread link unavailable in local session).
  Why: gives one beginner-friendly place to understand how the TV loaders work without reading both R scripts line by line.
  <details><summary>Paths — Shared TV Gmail loader documentation</summary>

  [util/data_loaders/README.md](util/data_loaders/README.md)
  [README.md](README.md)
  [CHANGELOG.md](CHANGELOG.md)

  </details>

### Changed

- **TV loader verification ownership moved to the universal runner**
  What: removed the loader-owned TV validation hook from the local and national TV Gmail loaders and updated the loader docs to point to the runner-owned verification workflow in [/Users/eugenetsenter/Docs/R_Studio_Projects/universal_cron_runner/verifications/](/Users/eugenetsenter/Docs/R_Studio_Projects/universal_cron_runner/verifications/) and the saved implementation plan at [/Users/eugenetsenter/Docs/R_Studio_Projects/universal_cron_runner/docs/plans/2026-03-30_minimal_tv_verification_in_runner.md](/Users/eugenetsenter/Docs/R_Studio_Projects/universal_cron_runner/docs/plans/2026-03-30_minimal_tv_verification_in_runner.md). -codexapp (thread link unavailable in local session).
  Why: keeps the loaders focused on ingestion while the runner owns the before/after table check and the status display. 
  <details><summary>Paths — TV Loader Verification Ownership</summary>

  [util/data_loaders/gmail_to_bq__tv_local.r](util/data_loaders/gmail_to_bq__tv_local.r)
  [util/data_loaders/gmail_to_bq__tv_nat.r](util/data_loaders/gmail_to_bq__tv_nat.r)
  [util/data_loaders/README.md](util/data_loaders/README.md)
  [README.md](README.md)
  [CHANGELOG.md](CHANGELOG.md)
  [/Users/eugenetsenter/Docs/R_Studio_Projects/universal_cron_runner/docs/plans/2026-03-30_minimal_tv_verification_in_runner.md](/Users/eugenetsenter/Docs/R_Studio_Projects/universal_cron_runner/docs/plans/2026-03-30_minimal_tv_verification_in_runner.md)

  </details>

### Fixed

- **TV national impression-column matching**
  Issue: The TV national loader refreshed the landing table on March 30, 2026 but wrote `net_impressions = 0` for every row from the latest national CSV.
  Cause: The script did not recognize the current cleaned source header `total_planned_impressions_all_demos` and skipped the non-zero impression field present in the latest Gmail attachment.
  Resolution: Updated the national loader to check the current planned-impressions header plus the non-suffixed objective-impressions fallback, then verified the latest attachment maps to non-zero impressions in a read-only dry run. -codexapp (thread link unavailable in local session).

## 2026-03-27

### Changed

- **Omni dashboard preview-first confirmation workflow**
  What: added a repo-wide rule that Omni dashboard edits should be previewed before live changes, with the preview showing the entire widget and clearly stating whether the proposed update is visual, behavioral, or both.
  Why: makes dashboard edits easier to confirm safely before publishing and avoids approving changes from partial or misleading cropped previews. -codexapp (thread link unavailable in local session).
  <details><summary>Paths — Omni dashboard preview-first confirmation workflow</summary>

  [AGENTS.md](AGENTS.md)
  [README.md](README.md)
  [CHANGELOG.md](CHANGELOG.md)

  </details>

## 2026-03-15

### Added

- **Prisma supplier logo reload script**
  What: added a Prisma shell script that rebuilds `looker-studio-pro-452620.landing.prisma_supplier_logos` from `Supplier_logos.xlsx` `Logos!A:C`, removes the blank/error footer rows, and prints a verification query after the load.
  Why: makes the supplier logo table refresh repeatable without rerunning ad hoc terminal commands. -codexapp (thread link unavailable in local session).
  <details><summary>Paths — Prisma supplier logo reload script</summary>

  [Prisma/reload_prisma_supplier_logos.sh](Prisma/reload_prisma_supplier_logos.sh)
  [README.md](README.md)
  [CHANGELOG.md](CHANGELOG.md)

  </details>

## 2026-03-13

### Changed

- **Live BigQuery object precedence**
  What: added a standing repo rule that when a BigQuery object path is referenced, the live production table or view should be inspected first and then compared to any matching local SQL or docs before trusting the local version.
  Why: keeps future warehouse QA anchored on the real production object and makes local-versus-production drift visible earlier. -codexapp (thread link unavailable in local session).
  <details><summary>Paths — Live BigQuery object precedence</summary>

  [AGENTS.md](AGENTS.md)
  [README.md](README.md)
  [mft/AGENTS.md](mft/AGENTS.md)

  </details>

- **MFT DCM UTM hardening and QA runbooks**
  What: hardened the local `repo_stg.dcm_plus_utms` deploy SQL with Mass-only creative fallbacks plus placement-name-only rescue when the placement exists in the UTM source, and added QA SQL files plus MFT documentation for validating staged completeness, `utm_content` ID fidelity, and creative checks through `utm_creative_assignment`.
  Why: reduces blank UTM fields in the Mass DCM reporting slice without using placeholders and makes the proof workflow repeatable. -codexapp (thread link unavailable in local session).
  <details><summary>Paths — MFT DCM UTM hardening and QA runbooks</summary>

  [mft/scripts/sql/repo_stg__dcm_plus_utms.sql](mft/scripts/sql/repo_stg__dcm_plus_utms.sql)
  [mft/scripts/sql/qa__repo_stg__dcm_plus_utms_mass_validation.sql](mft/scripts/sql/qa__repo_stg__dcm_plus_utms_mass_validation.sql)
  [mft/scripts/sql/qa__repo_stg__dcm_plus_utms_mass_exceptions.sql](mft/scripts/sql/qa__repo_stg__dcm_plus_utms_mass_exceptions.sql)
  [mft/README.md](mft/README.md)
  [mft/AGENTS.md](mft/AGENTS.md)
  [mft/docs/dcm_plus_utms_lineage.md](mft/docs/dcm_plus_utms_lineage.md)
  [README.md](README.md)

  </details>

## 2026-03-11

### Changed

- **Safer cleanup boundaries for active repo review**
  What: Added ignore rules for generated workspace artifacts, reset tracked local helper files out of the active diff, and documented the nested Streamlit app as archived side work under ADIF.
  Why: Keeps review focused on real pipeline changes instead of machine-specific clutter. -codexapp (thread link unavailable in local session).
  <details><summary>Paths — Safer cleanup boundaries for active repo review</summary>

  [.gitignore](.gitignore)
  [adif/README.md](adif/README.md)

  </details>

- **Safer default behavior for FPD and ADIF loaders**
  What: Reset the shared shortcut-aware FPD loader to fresh-run defaults and changed the ADIF TV/digital base loader so downstream loaders stay off unless explicitly enabled, with matching runbook updates.
  Why: Reduces stale uploads and surprise multi-script runs during daily use. -codexapp (thread link unavailable in local session).
  <details><summary>Paths — Safer default behavior for FPD and ADIF loaders</summary>

  [FPD/FPD_loader/util_collect_fpd_shortcutsFolder.r](FPD/FPD_loader/util_collect_fpd_shortcutsFolder.r)
  [FPD/FPD_loader/README.md](FPD/FPD_loader/README.md)
  [adif/projects/tv_digital_pipeline/util_collect_fpd_v2.r](adif/projects/tv_digital_pipeline/util_collect_fpd_v2.r)
  [adif/projects/tv_digital_pipeline/README - ADIF TV & Digital Data Pipeline.md](adif/projects/tv_digital_pipeline/README%20-%20ADIF%20TV%20%26%20Digital%20Data%20Pipeline.md)

  </details>

### Fixed

- **TV estimate loader impression-column matching**
  Issue: The TV local and national loaders were not consistently matching the cleaned impressions column names from the latest CSV exports.
  Cause: The scripts checked incomplete column-name variants after `clean_names()` reshaped the headers.
  Resolution: Updated the loader notes and national column selection logic, then reran the loaders and confirmed refreshed landing tables on March 11, 2026. -codexapp (thread link unavailable in local session).

## 2026-03-06

### Changed

- **Scheduled query runbook accuracy and monitoring query reliability**
  What: Updated `docs/SCHEDULED_QUERIES.md` to fix the maintenance health-check SQL, standardize UTC schedule labels, clarify that failed/deprecated jobs are included, add actionable timing-dependency guidance, and refresh the document date stamp.
  Why: Prevents monitoring confusion and query failures while making operational timing expectations clearer for daily pipeline checks. -codexapp (thread link unavailable in local session).
  <details><summary>Paths — Scheduled query runbook accuracy and monitoring query reliability</summary>

  [docs/SCHEDULED_QUERIES.md](docs/SCHEDULED_QUERIES.md)
  [CHANGELOG.md](CHANGELOG.md)

  </details>

## 2026-02-26

### Added

- **ADIF notebook QA dashboard (single HTML)**
  What: Added a self-contained dashboard file for `repo_stg.adif__mainDataTable_notebook` grouped by supplier, package, data source, and impression type, focused on planned-vs-actual spend/impressions with upstream reference columns.
  Why: Creates a fast local QA artifact that can be shared and opened without a backend service. -codexapp (thread link unavailable in local session).
  <details><summary>Paths — ADIF notebook QA dashboard (single HTML)</summary>

  [adif/projects/social_layering/adif_mainDataTable_notebook_qa_dashboard.html](adif/projects/social_layering/adif_mainDataTable_notebook_qa_dashboard.html)
  [adif/README.md](adif/README.md)
  [adif/CHANGELOG.md](adif/CHANGELOG.md)

  </details>

### Changed

- **Root workflow index now includes ADIF QA dashboard**
  What: Updated the root `README.md` Core Workflows list to describe the grouped planned-vs-actual QA dashboard and upstream reference context.
  Why: Keeps top-level navigation accurate so the QA entrypoint is easy to find. -codexapp (thread link unavailable in local session).
  <details><summary>Paths — Root workflow index now includes ADIF QA dashboard</summary>

  [README.md](README.md)
  [CHANGELOG.md](CHANGELOG.md)

  </details>

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
