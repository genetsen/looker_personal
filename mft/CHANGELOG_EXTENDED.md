# MFT Data Pipeline Extended Changelog (Base Path: /Users/eugenetsenter/Looker_clonedRepo/looker_personal/mft)

Verbose session-level and implementation-level change details are documented in this file.
For concise daily essentials, see `[BASE]/CHANGELOG.md`.
All relative paths below resolve from `[BASE]` = /Users/eugenetsenter/Looker_clonedRepo/looker_personal/mft.

## 2026-07-23

### Fixed

- FY26 Q2/Q3 historical Basis UTM coverage (`[BASE]/scripts/sql/repo_stg__basis_plus_utms_v4_PnS_table.sql`, `[BASE]/scripts/sql/qa__repo_stg__basis_plus_utms_fy26_q2_q3.sql`, `[BASE]/README.md`, and the parent Basis UTM utility)
  - Issue: the user-provided export identified 117 delivered placement-and-creative keys with blank UTMs in the final FY26 Q2/Q3 report.
  - Root cause: the maintained loader and production union retained only `MASSMUTUAL005_Updated 7.7`. The partner's approved `MASSMUTUAL005_Updated 6.15` worksheet contained all 117 missing source mappings, including paused creatives omitted from the current tab. Some CTV delivery names also contained `streamingburnedincaptions16x9` or a trailing Peacock label not present in the source mapping name.
  - Source load: added a dedicated historical landing table, kept the current worksheet in its existing table, loaded 340 historical and 331 current rows, and promoted 133 new complete mappings into the active union. The active union increased from 2,155 to 2,288 rows.
  - Matching logic: added a final FY26 source-backed fallback using placement ID plus normalized creative name. It removes only the confirmed audio wrapper, long CTV wrapper, and trailing Peacock label, and it returns a mapping only when the approved source rows resolve to one complete distinct URL.
  - Approved replacement boundary: official partner mappings replace extrapolated values for 54 daily rows across six placement-and-creative combinations. The guard proved zero populated UTM changes outside those combinations.
  - Production proof: `UTM UPDATES` run `6a69a125-0000-26de-bc4e-24058872e074` succeeded; `ext_mm_mft_scheadule_s2` run `6a6e7cab-0000-280f-a549-94eb2c0b009e` succeeded; the deployed Basis view has zero distinct tested-row differences from the guarded candidate.
  - Final output: the live mart and refreshed stored table each contain 11,533 FY26 Q2/Q3 report rows, zero missing report keys, 4,629,186 impressions, $154,941.79 cost, and 1,842 clicks.

### Changed

- Basis UTM maintenance guidance (`[BASE]/README.md` and the parent Basis UTM utility README)
  - What: replaced the stale “latest worksheet only” rule with separate historical-and-current worksheet loading and documented the unique official-source fallback.
  - Why: paused historical creative mappings must remain available after the current traffic sheet removes them.

## 2026-07-20

### Fixed

- FY26 Q2/Q3 Basis UTM missing-key regression (`[BASE]/scripts/sql/repo_stg__basis_plus_utms_v4_PnS_table.sql`, `[BASE]/scripts/sql/qa__repo_stg__basis_plus_utms_fy26_q2_q3.sql`, `[BASE]/README.md`)
  - Issue: the user-provided pre-refresh export contained 117 missing placement-and-creative keys, while the audio-normalized candidate exposed 135.
  - Diagnosis: frozen SQL-change-guard tables proved the audio normalization changed 4,235 daily audio rows from blank to populated without changing CTV rows, delivery keys, impressions, cost, or clicks. A literal rollback would have restored 8,940 blank daily rows and 231 report-level missing keys.
  - Resolution: retained the audio normalization and added an explicit allowlist for the 18 newly exposed CTV combinations. Each fallback preserves one same-placement `utm_source`, `utm_medium`, `utm_campaign`, `utm_term`, and tracking suffix while changing only the approved video-length and creative-name portion of `utm_content`.
  - Safety: exact mappings remain first priority; the CTV fallback is second; the audio fallback is third. Conflicting same-placement template values prevent a fallback from being built.
  - Proof: candidate and CSV each contained 117 missing keys with SHA-256 `9eafbdf6d846c3534e156936cbaa78d01e05513d5fdf06d113b62f88f818b427`; the SQL change guard passed eight comparisons with zero failures; 112 daily rows across 18 report keys changed from blank to populated; zero existing populated UTM rows changed; and zero populated rows became blank.
  - Production: the Basis UTM view deployment succeeded, the final MFT scheduled refresh succeeded, and both the live report and stored table reconcile at 4,337,803 impressions, $145,072.21 cost, 1,714 clicks, and the exact 117-key missing set.

### Changed

- Basis UTM maintenance proof (`[BASE]/README.md`)
  - What: documented the live match priority and required exact-key comparison before and after any Basis UTM change.
  - Why: prevent a successful refresh or unchanged total count from being mistaken for proof that the same placement-and-creative gaps remain.

## 2026-07-16

### Changed

- FY26 Q2/Q3 Basis UTM mapping load (`[BASE]/README.md` and the parent Basis UTM utility)
  - Source selection: downloaded the separate partner workbook `MASSMUTUAL005 - Creative Trafficking Sheet_Q3 7.7.xlsx` and selected only `MASSMUTUAL005_Updated 7.7`; the older `MASSMUTUAL005_Updated 6.15` worksheet was not combined.
  - Loader change: added a dedicated `fy26_q2_q3` configuration targeting `landing.basis_utms_pivoted_fy26_q2_q3`, removed embedded whitespace from URL cells, and added an idempotent production-promotion script.
  - Landing proof: 331 distinct placement-and-creative assignments were loaded; 322 have complete five-parameter UTM URLs, zero contain URL whitespace, zero populated mapping keys are duplicated, and all nine blank URLs belong to paused assignments.
  - Production lookup proof: the active workbook union accepted exactly 322 rows and has zero remaining eligible inserts; `UTM UPDATES` succeeded; and `utm_scrap.b_sup_pivt_unioned_tab` contains all 322 mappings with zero missing rows, duplicate keys, whitespace, or incomplete URLs. The lookup grew from 2,112 to 2,434 rows.
  - Final-output boundary: `ext_mm_mft_scheadule_s2` succeeded, and the live MFT mart and stored table reconcile at 46,539 FY26 rows, 16,689,254 impressions, $564,616.16 cost, 4,102 clicks, and zero blank `utm_source` rows. No Q2/Q3 final rows exist yet because `repo_stg.basis_delivery` contains zero `MASSMUTUAL005` placements.

## 2026-07-15

### Fixed

- Repeated DCM creative-size suffix normalization (`[BASE]/scripts/sql/repo_stg__dcm_plus_utms.sql`, `[BASE]/README.md`, `[BASE]/docs/dcm_plus_utms_lineage.md`)
  - Issue: 336 `MassMutual20252026Media` records representing 9,301,443 impressions had blank UTM fields even though all seven placements had a `WhatItsAllAbout30_0x0` UTM assignment.
  - Cause: the loose and extension-stripped keys removed exactly one trailing size token from each side. That left `WhatItsAllAbout30_0x0_0x0` and `WhatItsAllAbout30_0x0` with different normalized keys.
  - Resolution: changed all six DCM-side, UTM-side, and UTM-deduplication expressions to remove every consecutive trailing size token at the end of the creative name; deployed `repo_stg.dcm_plus_utms`; and ran the canonical `ext_mm_mft_scheadule_s2` endpoint refresh.
  - Proof: the SQL change guard passed 11 comparisons with zero failures; the live staging view retained 153,564 unique keys, 1,455,294,999 impressions, and 1,069,079 clicks; the live mart and both stored endpoint tables contain all 336 corrected records with 9,301,443 impressions, $188,927.59 cost, and 754 clicks.
  - Remaining gaps: 885 records remain source exceptions—736 from campaigns absent from the UTM reference, 40 from one missing placement in an existing campaign, and 109 from placement-to-creative assignment differences.

- FY26 Basis CTV UTM enrichment (`[BASE]/scripts/sql/repo_stg__basis_delivery_fy26_ctv_utm_key.sql`, `[BASE]/reports/basis_fy26_missing_utm_mappings_2026-07-15.csv`)
  - Issue: 4,300 final MFT rows for six FY26 creative-and-length combinations had blank UTM fields, representing 1,519,667 impressions and $55,074.76 in media cost.
  - Cause: 104 active placement-and-creative mappings were absent from the lookup. After those mappings were derived, the long Autograph and Play by Play names still missed because delivery retained the `16x9` token while the lookup cleanup removed it.
  - Resolution: added 104 same-placement B2C CTV mappings to the internal supplement. A 64-row `_0x0` alias test proved that suffix removal alone did not resolve the long-name mismatch, so those test aliases were removed and a fallback limited to the FY26 campaign, CTV placements, and the two still-unmatched creative families was deployed instead.
  - Proof: the UTM lookup refresh and final MFT scheduled refresh both succeeded; the source and enriched FY26 Basis branches reconciled at 46,539 rows, 16,689,254 impressions, $564,616.16 in media cost, and 4,102 clicks; the affected final slice now has 4,300 rows, zero blank UTM rows, 1,519,667 impressions, $55,074.76 in cost, and 32 clicks.

### Changed

- Basis UTM operating documentation (`[BASE]/README.md`)
  - What: replaced the stale Basis UTM source description and three-file loader example with the current workbook repository, FY26 worksheet configuration, internal supplement, production lookup, scheduled refreshes, safe CTV extrapolation rule, and missing-UTM diagnosis matrix.
  - Why: make it explicit that a new campaign file is not loaded until the loader configuration, landing table, active union, UTM refresh, delivery join, and final-table refresh all pass.

## 2026-03-13

### Added
- Mass DCM UTM QA queries (`[BASE]/scripts/sql/qa__repo_stg__dcm_plus_utms_mass_validation.sql`, `[BASE]/scripts/sql/qa__repo_stg__dcm_plus_utms_mass_exceptions.sql`)
  - What: added one QA SQL script that compares the two-pass baseline, the loose-normalized intermediate state, and the final file-suffix-aware four-pass logic on the Mass DCM reporting slice, plus a second QA SQL script that lists the remaining unmatched exceptions with candidate UTM creatives.
  - Why: provide repeatable proof before deployment and make it easy to separate “needs more SQL normalization” from “the source UTM sheet is still missing or mismatched”.

### Changed
- BigQuery object reference default (`[BASE]/AGENTS.md`)
  - What: added a standing rule that when the user references a BigQuery object path instead of a local file path, the live production object should be inspected first and any matching local SQL or lineage note should be checked for drift before relying on it.
  - Why: keep future MFT QA grounded in the real warehouse object and surface local-versus-production mismatches before they cause wrong assumptions.
- Mass-only DCM UTM fallback chain (`[BASE]/scripts/sql/repo_stg__dcm_plus_utms.sql`, `[BASE]/README.md`, `[BASE]/docs/dcm_plus_utms_lineage.md`)
  - What: replaced the earlier two-pass local SQL with a deployable `repo_stg.dcm_plus_utms` definition that keeps exact matching first, then adds three constrained Mass-only creative fallbacks plus three placement-name-only rescue passes: same-campaign `campaign + placement_id`, then placement-only `placement_id`, then the live DCM `placement` field when no UTM placement exists.
  - Why: close the known blank-UTM gaps for Mass DCM rows while keeping non-Mass campaigns on strict exact matching only and avoiding unsafe backfill of creative-level UTM fields when only the placement metadata can be trusted.
- DCM UTM QA documentation (`[BASE]/README.md`, `[BASE]/AGENTS.md`, `[BASE]/docs/dcm_plus_utms_lineage.md`)
  - What: updated the MFT README, the local agent instructions, and the focused lineage note to describe the new fallback order, show the safe-query wrapper commands, and document the recommended validation order for completeness checks, `utm_content` ID checks, creative validation through `utm_creative_assignment`, placement-only rescue, and final DCM placement fallback.
  - Why: keep pipeline docs aligned with the new logic and make the QA steps easy to rerun without reconstructing the workflow from chat history or repeating the same false-start creative checks.

## 2026-03-12

### Added
- DCM Plus UTMs lineage note (`[BASE]/docs/dcm_plus_utms_lineage.md`, `[BASE]/README.md`)
  - What: documented the live lineage of `repo_stg.dcm_plus_utms` from `DCM.20250505_costModel_v5` and `landing.adswerve_utms` through `final_views.dcm` and `final_views.utms_view`, including exact-match logic, normalized fallback logic, and downstream usage in `repo_mart.mft_view`.
  - Why: give one beginner-friendly reference for debugging DCM UTM enrichment and clarify that `repo_stg.dcm_plus_utms_upload` is not an input to this view. -codexapp thread `019ce41b-ea18-7492-86c5-e9c259b77c94` (link unavailable in local session).

## 2026-02-12

### Changed
- Changelog governance codified globally (`[BASE]/AGENTS.md`, `[BASE]/README.md`, `/Users/eugenetsenter/.codex/AGENTS.md`)
  - What: updated local and global agent instructions plus README changelog policy to enforce outcome-focused entries, preserve unrelated history, and use collapsible path blocks with one path per line.
  - Why: prevent repeated formatting drift and keep changelog updates consistent across sessions.
- DCM UTM join fallback scoped to target campaigns (`[BASE]/scripts/sql/repo_stg__dcm_plus_utms.sql`, `[BASE]/README.md`)
  - What: added a deployable `CREATE OR REPLACE VIEW` SQL for `repo_stg.dcm_plus_utms` that preserves exact `placement_id + creative_assignment` matching and adds a normalized fallback (case-insensitive + `px` handling) only for `MassMutual20252026Media` and `MassMutualLVGP2025`.
  - Why: resolve known null-UTM enrichment misses for the two active problem campaigns without broad backfill impacts to unrelated historical campaigns.

### Fixed
- MassMutual DCM UTM enrichment mismatch (`[BASE]/scripts/sql/repo_stg__dcm_plus_utms.sql`, `[BASE]/README.md`)
  - Issue: 2026 rows for `MassMutual20252026Media` and `MassMutualLVGP2025` had null UTM fields after staging enrichment.
  - Cause: exact-key join between DCM creative names and UTM creative assignments did not tolerate case differences and `px` creative suffix variants.
  - Resolution: deployed campaign-scoped normalized fallback join in `repo_stg.dcm_plus_utms`; post-deploy validation confirmed `null_utm_content_rows = 0` for both target campaigns in `repo_stg.dcm_plus_utms` and `repo_mart.mft_view` for 2026 scope.

## 2026-02-11

### Added
- Safe Query Guardrails (`[BASE]/scripts/bq-safe-query.sh`, `[BASE]/README.md`, `[BASE]/AGENTS.md`)
  - What: added a safe BigQuery wrapper with schema-only support and documented guardrail usage/defaults.
  - Why: reduce expensive/token-heavy queries and standardize safer BigQuery reads.
- Guardrail fallback summaries (`[BASE]/scripts/bq-safe-query.sh`, `[BASE]/README.md`, `[BASE]/AGENTS.md`)
  - What: when a query is blocked (for example `SELECT *` or byte cap), the wrapper now prints summary-first SQL suggestions, and MCP `run_query` policy now mirrors the same guardrails.
  - Why: keep analysis workflows moving safely when large raw-row pulls are blocked.
- Guardrail bypass guidance (`[BASE]/scripts/bq-safe-query.sh`, `[BASE]/README.md`)
  - What: guardrail failure output now includes explicit one-run and environment-variable commands for turning off limits when intentionally needed.
  - Why: make override paths clear without removing safety defaults.
- Offline Sheet Daily Sync (`[BASE]/scripts/setup-mft-offline-daily-sheet-sync.sh`, `[BASE]/scripts/sql/mft_offline_daily_sheet_sync.sql`, `[BASE]/README.md`)
  - What: added a daily sheet-to-native-table sync setup script, SQL template, and setup/verification documentation.
  - Why: automate refresh of `looker-studio-pro-452620.mass_mutual_mft_ext.mft_offline` from the connected sheet source.
- Direct SQL runbooks (`[BASE]/scripts/sql/stg__mm__mft_offline_connected_gsheet.sql`, `[BASE]/scripts/sql/mft_offline_update_manual.sql`, `[BASE]/README.md`)
  - What: added copy/paste-ready BigQuery SQL files to build staging and refresh output without using the shell wrapper.
  - Why: support manual execution directly in the BigQuery UI.

### Changed
- Changelog session formatting (`[BASE]/CHANGELOG.md`, `[BASE]/AGENTS.md`)
  - What: for session `019c4e54-8fb9-74a1-8933-0020b1a791a5`, standardized entries to concise section-consistent structure (`Added/Changed` use `What` + `Why`; `Fixed` uses `Issue` + `Cause` with verification gating).
  - Why: make updates easier to scan and prevent unverified fixes from being recorded as resolved.
- MFT Endpoint Documentation (`[BASE]/README.md`)
  - What: updated endpoint target docs, pipeline visuals, metadata, and query/troubleshooting examples for current schema.
  - Why: align documentation with the production pipeline and reporting endpoint.
- BigQuery default byte cap (`[BASE]/scripts/bq-safe-query.sh`, `[BASE]/README.md`, `[BASE]/AGENTS.md`)
  - What: increased default `--max-bytes` guardrail from `500000000` to `176000000000` (about `$1` at `$6.25/TiB`).
  - Why: align default blocking behavior with a dollar-based BigQuery cost threshold instead of a sub-cent threshold.
- Offline data Sync Automation (`[BASE]/scripts/setup-mft-offline-daily-sheet-sync.sh`, `[BASE]/README.md`)
  - What: updated setup flow for SQL-template rendering, existing-transfer updates via `--transfer-config-id`, and staging dataset control via `--staging-dataset`.
  - Why: support both new/existing scheduled queries and keep staging tables in the intended dataset.
- Offline output shaping (`[BASE]/scripts/sql/mft_offline_daily_sheet_sync.sql`, `[BASE]/README.md`)
  - What: updated scheduled-query SQL to emit lowercase output columns and filter out rows where `COALESCE(spend,0) + COALESCE(impressions,0) = 0`.
  - Why: enforce cleaner downstream schema and remove zero/null activity records from `mft_offline`.
- Offline staging range/schema alignment (`[BASE]/scripts/sql/mft_offline_daily_sheet_sync.sql`, `[BASE]/scripts/setup-mft-offline-daily-sheet-sync.sh`, `[BASE]/README.md`)
  - What: aligned staging coverage to `A:U`, and updated output SQL/docs to include `col_j` alongside the lowercase core fields.
  - Why: support the new connected-sheet layout while keeping filtered/lowercase output behavior.
- Offline actual-name mapping (`[BASE]/scripts/sql/mft_offline_daily_sheet_sync.sql`, `[BASE]/scripts/sql/stg__mm__mft_offline_connected_gsheet.sql`, `[BASE]/scripts/sql/mft_offline_update_manual.sql`)
  - What: replaced placeholder fields (for example `col_j`) with actual mapped names from the sheet headers (`data_type`, `month`, `quarter`, `year`, `key_simp`, `total_act_cost_key`, `total_est_cost_key`, `full_key`, `year_quarter`).
  - Why: ensure staging/output schemas use meaningful names and include column J by its real label.

### Fixed
- Scheduled query script not working (`[BASE]/scripts/setup-mft-offline-daily-sheet-sync.sh`, `[BASE]/scripts/sql/mft_offline_daily_sheet_sync.sql`)
  - Issue: offline daily sync setup/run could fail.
  - Cause: SQL rendering and transfer-config parameters were not fully aligned with BigQuery DTS requirements.
  - Verification: pending. See `/Users/eugenetsenter/Looker_clonedRepo/looker_personal/mft/AGENTS.md` under `## Verification TODOs`.
