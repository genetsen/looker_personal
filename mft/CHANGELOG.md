# MFT Data Pipeline Changelog (Base Path: /Users/eugenetsenter/Looker_clonedRepo/looker_personal/mft)

Concise daily essentials are documented in this file.
Detailed session-level changes are documented in [CHANGELOG_EXTENDED.md](CHANGELOG_EXTENDED.md).

## 2026-06-26

### Changed
#### **MFT instruction ownership cleanup**
What: updated local MFT agent rules to point general BigQuery, SQL, source-of-truth, read-only permission, field-lineage, and grain decisions to the global rule document while keeping MFT's stricter safe-query helper, row limits, cost caps, and DCM UTM validation defaults local.
Why: prevents duplicated warehouse policy from drifting while preserving the MFT-specific guardrails that reduce expensive or overly broad data pulls.

<details>
<summary>Paths - MFT instruction ownership cleanup</summary>

- [AGENTS.md](AGENTS.md)

</details>

## 2026-03-13

### Added
#### **Mass DCM UTM QA queries**
What: added one QA SQL file that compares the two-pass, three-pass, and four-pass Mass DCM UTM results side by side, plus one QA SQL file that lists the final unmatched exceptions with candidate UTM creatives.
Why: make the proof workflow repeatable before any live deployment and separate join-logic misses from source-sheet gaps. -codexapp (thread link unavailable in local session).

<details>
<summary>Paths — Mass DCM UTM QA queries</summary>

- [scripts/sql/qa__repo_stg__dcm_plus_utms_mass_validation.sql](scripts/sql/qa__repo_stg__dcm_plus_utms_mass_validation.sql)
- [scripts/sql/qa__repo_stg__dcm_plus_utms_mass_exceptions.sql](scripts/sql/qa__repo_stg__dcm_plus_utms_mass_exceptions.sql)

</details>

### Changed
#### **BigQuery object reference default**
What: added a standing rule in the MFT local agent instructions that a referenced BigQuery object path should be checked in live production first, then compared against any matching local SQL or lineage documentation before trusting the local copy.
Why: keep future QA and debugging anchored on the real warehouse object and make local-versus-production drift visible early. -codexapp (thread link unavailable in local session).

<details>
<summary>Paths — BigQuery object reference default</summary>

- [AGENTS.md](AGENTS.md)

</details>

#### **DCM UTM fallback hardening for Mass rows**
What: updated the local deploy SQL for `repo_stg.dcm_plus_utms` to keep exact matching first, then add Mass-only normalized, size-stripped, and file-suffix-stripped creative fallbacks plus placement-name-only rescue on `campaign + placement_id`, then `placement_id`, and finally the live DCM `placement` field when no UTM placement exists; refreshed the README, lineage note, and `AGENTS.md` to document the new join order and the correct validation playbook for completeness, ID checks, creative checks, and placement-only rescue.
Why: reduce blank UTM enrichment fields in the Mass DCM slice without widening fuzzy matching to unrelated non-Mass rows. -codexapp (thread link unavailable in local session).

<details>
<summary>Paths — DCM UTM fallback hardening for Mass rows</summary>

- [scripts/sql/repo_stg__dcm_plus_utms.sql](scripts/sql/repo_stg__dcm_plus_utms.sql)
- [README.md](README.md)
- [AGENTS.md](AGENTS.md)
- [docs/dcm_plus_utms_lineage.md](docs/dcm_plus_utms_lineage.md)

</details>

## 2026-03-12

### Added
#### **DCM Plus UTMs lineage note**
What: added a focused lineage document for `repo_stg.dcm_plus_utms`, corrected the README to name `landing.adswerve_utms` as the active DCM UTM source, and linked the note from the main pipeline guide.
Why: make the DCM UTM enrichment path easy to trace from base tables through the mart and avoid confusing `mm_utms_snapshot` or `dcm_plus_utms_upload` with the live parents. -codexapp thread `019ce41b-ea18-7492-86c5-e9c259b77c94` (link unavailable in local session).

<details>
<summary>Paths — DCM Plus UTMs lineage note</summary>

- [docs/dcm_plus_utms_lineage.md](docs/dcm_plus_utms_lineage.md)
- [README.md](README.md)

</details>

## 2026-02-12

### Changed
#### **Global changelog content standards**  
What: codified changelog content rules in repo guidance and docs, including preserving unrelated history, outcome-first `What`/`Why`, and dedicated collapsible path sections.  
Why: keep future updates consistent and reduce formatting churn across sessions.

<details>
<summary>Paths — Global changelog content standards</summary>

- [AGENTS.md](AGENTS.md)
- [README.md](README.md)

</details>

#### **DCM UTM join hardening (campaign-scoped fallback)**  
What: added a deployable SQL definition for `repo_stg.dcm_plus_utms` with exact-key UTM matching plus a normalized fallback scoped to `MassMutual20252026Media` and `MassMutualLVGP2025`; updated README lineage notes to match.  
Why: fix known creative-key mismatches for targeted MassMutual campaigns while preventing unintended backfill changes in other campaigns.

<details>
<summary>Paths — DCM UTM join hardening (campaign-scoped fallback)</summary>

- [scripts/sql/repo_stg__dcm_plus_utms.sql](scripts/sql/repo_stg__dcm_plus_utms.sql)
- [README.md](README.md)

</details>

### Fixed
#### **MassMutual DCM UTM enrichment gaps (2026 null `utm_content`)**  
Issue: targeted MassMutual campaign rows in 2026 were flowing to the endpoint with null UTM fields.  
Cause: exact join on `placement_id + creative_assignment` in `repo_stg.dcm_plus_utms` failed on creative naming mismatches (case differences and `px` suffix variants).  
Resolution: deployed constrained normalized fallback join logic for `MassMutual20252026Media` and `MassMutualLVGP2025`, and verified post-deploy that null `utm_content` dropped to `0` for those campaigns in both `repo_stg.dcm_plus_utms` and `repo_mart.mft_view`.

<details>
<summary>Paths — MassMutual DCM UTM enrichment gaps (2026 null utm_content)</summary>

- [scripts/sql/repo_stg__dcm_plus_utms.sql](scripts/sql/repo_stg__dcm_plus_utms.sql)
- [README.md](README.md)
- [CHANGELOG.md](CHANGELOG.md)

</details>

## 2026-02-11

### Added
#### **BigQuery guardrails and fallback guidance**  
What: added safe-query defaults, schema-only support, summary-first fallback guidance, and explicit bypass instructions.  
Why: reduce expensive/token-heavy pulls while keeping analyst workflows unblocked.

<details>
<summary>Paths — BigQuery guardrails and fallback guidance</summary>

- [scripts/bq-safe-query.sh](scripts/bq-safe-query.sh)
- [README.md](README.md)
- [AGENTS.md](AGENTS.md)

</details>

#### **MM MMM EXTERNAL Offline DATA PIPELINE**  
##### What:   created a gsheets connected staging table  and created an update script that updates external table daily
<details><summary>Paths</summary>
- created a gsheets connected staging table  
   gsheet path: [Google Sheet](https://docs.google.com/spreadsheets/d/15DddW291w_O7WWv8F0AcOcumEMYJSdm9hWKU9vE5WPQ/edit?gid=1999096908#gid=1999096908) (`[NEW] INTERNAL | COMBINED DATA`, `A:U`)  
   bq staging table (full path): `looker-studio-pro-452620.repo_stg.stg__mm__mft_offline_connected_gsheet`  
- created an update script that updates external table daily (full overwrite)  
   bq ext table (full path): `looker-studio-pro-452620.mass_mutual_mft_ext.mft_offline`  
   BQ scheadule: [Transfer Config](https://console.cloud.google.com/bigquery/transfers/location/us/configs/699421ab-0000-2129-a27e-883d24f0f1b8?project=looker-studio-pro-452620)  
</details>
 Why: enable reliable refresh of `looker-studio-pro-452620.mass_mutual_mft_ext.mft_offline` from connected sheet sources.

<details>
<summary>Paths — MM MMM EXTERNAL Offline DATA PIPELINE</summary>

- [scripts/sql/mft_offline_daily_sheet_sync.sql](scripts/sql/mft_offline_daily_sheet_sync.sql)
- [scripts/sql/stg__mm__mft_offline_connected_gsheet.sql](scripts/sql/stg__mm__mft_offline_connected_gsheet.sql)
- [scripts/sql/mft_offline_update_manual.sql](scripts/sql/mft_offline_update_manual.sql)
- [README.md](README.md)

</details>

### Changed
#### **Documentation and changelog standards**  
What: aligned endpoint/pipeline docs and split changelog into concise daily essentials plus extended details.  
Why: improve readability while preserving full implementation history.

<details>
<summary>Paths — Documentation and changelog standards</summary>

- [README.md](README.md)
- [CHANGELOG.md](CHANGELOG.md)
- [CHANGELOG_EXTENDED.md](CHANGELOG_EXTENDED.md)
- [AGENTS.md](AGENTS.md)

</details>
