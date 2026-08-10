# MFT Data Pipeline Changelog (Base Path: /Users/eugenetsenter/Looker_clonedRepo/looker_personal/mft)

Concise daily essentials are documented in this file.
Detailed session-level changes are documented in [CHANGELOG_EXTENDED.md](CHANGELOG_EXTENDED.md).

## 2026-07-23

### Fixed

#### **FY26 Q2/Q3 historical Basis UTM coverage**

Issue: 117 delivered placement-and-creative keys in the FY26 Q2/Q3 report had no matching UTM values.
Cause: production retained the current July 7 worksheet but not the approved June 15 historical worksheet, and several delivered CTV creative names contained wrapper text not present in the partner mapping names.
Resolution: loaded the historical and current worksheets to separate landing tables, promoted 133 complete official mappings, and added a unique source-backed fallback using placement ID plus normalized creative name.
Verification: the SQL change guard passed all eight comparisons; only the 54 approved daily rows across six official mapping combinations replaced extrapolated values; no other populated UTM changed. The live mart and refreshed stored table both have zero missing report keys and reconcile at 11,533 rows, 4,629,186 impressions, $154,941.79 cost, and 1,842 clicks.

<details>
<summary>Paths — FY26 Q2/Q3 historical Basis UTM coverage</summary>

- [Basis delivery-to-UTM SQL](scripts/sql/repo_stg__basis_plus_utms_v4_PnS_table.sql)
- [Basis UTM verification query](scripts/sql/qa__repo_stg__basis_plus_utms_fy26_q2_q3.sql)
- [MFT pipeline README](README.md)
- [Basis UTM loader](</Users/eugenetsenter/Looker_clonedRepo/looker_personal/util/basis_utms/essential/util__basis__utm_pivot_longer_loop.r>)
- [FY26 Q2/Q3 promotion SQL](</Users/eugenetsenter/Looker_clonedRepo/looker_personal/util/basis_utms/essential/load_basis_utms_unioned_0929_from_fy26_q2_q3.sql>)

</details>

## 2026-07-20

### Fixed

#### **FY26 Basis UTM missing-key regression**

Issue: the FY26 Q2/Q3 missing-UTM report increased from 117 placement-and-creative keys to 135 after the audio normalization refresh.
Cause: 18 CTV combinations were outside the exact mapping keys; a literal rollback of the audio change would have increased the report to 231 missing keys.
Resolution: kept the verified audio normalization and added an explicit 18-key CTV fallback that derives only creative length/name from one unambiguous same-placement template.
Verification: the SQL change guard passed with zero failures; zero existing populated UTM rows changed; the live report and refreshed stored table both contain the exact 117-key set from the pre-refresh CSV and reconcile at 4,337,803 impressions, $145,072.21 cost, and 1,714 clicks.

<details>
<summary>Paths — FY26 Basis UTM missing-key regression</summary>

- [Basis delivery-to-UTM SQL](scripts/sql/repo_stg__basis_plus_utms_v4_PnS_table.sql)
- [Basis UTM verification query](scripts/sql/qa__repo_stg__basis_plus_utms_fy26_q2_q3.sql)
- [MFT pipeline README](README.md)

</details>

## 2026-07-16

### Changed

#### **FY26 Q2/Q3 Basis UTM mappings**

What: loaded the separate FY26 Q2/Q3 trafficking file, selected only its approved `MASSMUTUAL005_Updated 7.7` worksheet, and refreshed the production UTM lookup with all 322 complete mappings.
Why: prepare Q2/Q3 delivery to receive UTMs as soon as `MASSMUTUAL005` placements arrive, without combining the older worksheet version or duplicating mapping keys.
Verification: the landing table contains 331 distinct assignments, including 322 complete URLs and nine paused-only blanks; the refreshed lookup contains all 322 complete mappings with zero missing rows, duplicate keys, whitespace, or incomplete UTM URLs. The MFT mart and stored table still reconcile at 46,539 FY26 rows, 16,689,254 impressions, $564,616.16 cost, and 4,102 clicks. Final Q2/Q3 row verification remains pending because live Basis delivery contains no `MASSMUTUAL005` placements yet.

<details>
<summary>Paths — FY26 Q2/Q3 Basis UTM mappings</summary>

- [MFT pipeline README](README.md)

</details>

### Pending Next Actions

- **Since Jul 1** - Build a Looker-owned replacement refresh for `landing.basis_master` before attempting an MFT Basis cutover
- **Since Jul 1** - Create an isolated QA version of the MFT Basis branch and compare it to the current client-facing output before approval

## 2026-07-15

### Fixed

#### **FY26 Basis CTV UTM gaps**

Issue: FY26 Autograph, Play by Play, One Man Show, and Welcome to Florida CTV delivery reached MFT with blank UTM fields.
Cause: 104 active placement-and-creative mappings were missing, and the production delivery and lookup cleanup rules handled long `16x9_0x0` names differently.
Resolution: derived source-consistent CTV mappings from each placement's existing B2C template, refreshed the production lookup, added a narrowly scoped FY26 join-key correction, and verified all 4,300 affected final-table rows now have UTMs without changing 1,519,667 impressions, $55,074.76 in cost, or 32 clicks.

<details>
<summary>Paths — FY26 Basis CTV UTM gaps</summary>

- [FY26 Basis CTV join-key correction](scripts/sql/repo_stg__basis_delivery_fy26_ctv_utm_key.sql)
- [FY26 missing-mapping report](reports/basis_fy26_missing_utm_mappings_2026-07-15.csv)

</details>

#### **Repeated DCM creative-size suffixes**

Issue: 336 `MassMutual20252026Media` delivery records had blank UTM fields even though all seven placements contained the correct UTM creative assignment.
Cause: the fallback removed only one trailing size token from each side, so DCM `WhatItsAllAbout30_0x0_0x0` became `WhatItsAllAbout30_0x0` while UTM `WhatItsAllAbout30_0x0` became `WhatItsAllAbout30`.
Resolution: changed both DCM and UTM fallback keys to remove every consecutive trailing size token, deployed the production view, and refreshed the final MFT endpoint. All 336 records and 9,301,443 impressions now have UTMs without changing delivery totals or unique staging keys.

<details>
<summary>Paths — Repeated DCM creative-size suffixes</summary>

- [DCM UTM deploy SQL](scripts/sql/repo_stg__dcm_plus_utms.sql)
- [MFT pipeline README](README.md)
- [DCM UTM lineage note](docs/dcm_plus_utms_lineage.md)

</details>

### Changed

#### **Basis UTM maintenance and troubleshooting guide**

What: documented the partner workbook repository, internal supplement, production lookup, campaign-loading steps, safe CTV extrapolation rule, refresh sequence, and four distinct causes of missing Basis UTMs.
Why: make new-campaign updates and missing-UTM recovery repeatable without confusing the overall workflow with the `utm_source` field.

<details>
<summary>Paths — Basis UTM maintenance and troubleshooting guide</summary>

- [MFT pipeline README](README.md)

</details>

### Pending Next Actions

- **Since Jul 1** - Build a Looker-owned replacement refresh for `landing.basis_master` before attempting an MFT Basis cutover - BLOCKER
- **Since Jul 1** - Create an isolated QA version of the MFT Basis branch and compare it to the current client-facing output before approval

## 2026-07-01

- **CHANGED** - Documented the client-shared MFT Basis dependency risk: the live MFT Basis branch reads `landing.basis_master`, not the newly migrated `repo_stg.basis_master2` path, and a direct candidate swap would remove 2026 Basis delivery from the client-facing MFT output.

### Pending Next Actions

- **Since Jul 1** - Build a Looker-owned replacement refresh for `landing.basis_master` before attempting an MFT Basis cutover - BLOCKER
- **Since Jul 1** - Create an isolated QA version of the MFT Basis branch and compare it to the current client-facing output before approval - RECOMMENDED

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
