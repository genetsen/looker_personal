# MFT Data Pipeline Extended Changelog (Base Path: /Users/eugenetsenter/Looker_clonedRepo/looker_personal/mft)

Verbose session-level and implementation-level change details are documented in this file.
For concise daily essentials, see `[BASE]/CHANGELOG.md`.
All relative paths below resolve from `[BASE]` = /Users/eugenetsenter/Looker_clonedRepo/looker_personal/mft.

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
