# Changelog

All notable changes to this repository are documented in this file.

## 2026-08-10

**First-party data publishing, Apollo identity, master-model delivery, and shared runner authentication recovered** ([R](/Users/eugenetsenter/Looker_clonedRepo/looker_personal/FPD/FPD_loader/util_collect_fpd_shortcutsFolder.r)) — 🟢 **Verified and committed**<br>The [FPD loader](/Users/eugenetsenter/Looker_clonedRepo/looker_personal/FPD/FPD_loader/README.md) now preserves blank numeric warehouse fields, fails closed on unsuccessful BigQuery jobs, and removes warehouse rows only for Sheets explicitly named `ARCHIVE`, using the exact source URL while preserving merely missing sources. FPD, WP, and TV BigQuery access prefer the consolidated Google login while retaining their established cached-token fallbacks. The canonical runner completed 19 of 19 workloads successfully; Purely Elizabeth's August 7 rows reconciled at 15,328,236 impressions and $68,167.14 spend, and its full MIQ feed reconciled across the landing, base, clustered, and V3 models. Apollo's 22,406 LinkedIn rows use the canonical advertiser `Apollo` in all three model layers while retaining both raw account spellings. The [master-model refresh](/Users/eugenetsenter/Docs/R_Studio_Projects/universal_cron_runner/automation_hub/workloads/ops/bq_trigger/run_master_data_model_clustered_advertiser_refresh.sh) also publishes planned delivery for eligible offline planned-only rows, with 409 live rows verified and the west-region reporting tables refreshed.

**MFT historical UTM and WP workbook-tab repairs consolidated on the canonical branch** ([SQL](/Users/eugenetsenter/Looker_clonedRepo/looker_personal/mft/scripts/sql/repo_stg__basis_plus_utms_v4_PnS_table.sql)) — 🟡 **Partially verified**<br>The [MFT workflow](/Users/eugenetsenter/Looker_clonedRepo/looker_personal/mft/README.md) now preserves the previously deployed historical/current FY26 mapping logic on `dev`; live QA has zero duplicate keys and zero missing reported UTM keys. The [WP workflow](/Users/eugenetsenter/Looker_clonedRepo/looker_personal/wp/README.md) now reads the live `Import_blend` tab under the newer shared-authentication path, and a no-write preview normalized 24,992 rows without the old fallback warning.
- **⚠️ Unverified** - The WP production upload has not been rerun with the corrected `Import_blend` code path.

### Pending Next Actions

- **Since Jul 28** - Reconcile all preserved side-branch and stash work into canonical branches before deleting those preservation surfaces - BLOCKER
- **Since Jul 14** - Decide and implement the weekly Purely Elizabeth SPINS source-file refresh workflow - RECOMMENDED
- **Since Jul 14** - Run one final Package Lookup menu search after the one-field layout update and confirm its returned packages against the live warehouse
- **Since Jul 7** - Update the saved shared-social transfer configuration with owner-account credentials
- **Since Jun 24** - Observe the current Apollo, Ritual, and Olipop dashboards during normal end-user use

## 2026-07-27

**WP delivery-workbook loader uses its current ID-source tab** ([R](/Users/eugenetsenter/Looker_clonedRepo/looker_personal/wp/load_wp_search_data_template.R)) — 🟢 **Verified and committed**<br>The [WP delivery-workbook workflow](/Users/eugenetsenter/Looker_clonedRepo/looker_personal/wp/README.md) now reads the verified `Import_blend` worksheet rather than a stale tab name, preventing the preview path from falling back to generated ad-group IDs. Production upload remains deliberately manual. [More details](/Users/eugenetsenter/Looker_clonedRepo/looker_personal/wp/README.md)

## 2026-07-23

- **FIXED** - Restored all 117 missing FY26 Q2/Q3 Basis placement-and-creative UTM mappings by preserving the approved June 15 historical worksheet alongside the July 7 current worksheet and using a unique official-source creative fallback. Both production refreshes succeeded; the live mart and stored MFT table now have zero missing report keys with unchanged reporting totals.

## 2026-07-17

- **FIXED** - Corrected the Purely Elizabeth weekly delivery-and-sales table so unmapped campaigns retain their delivery as `UNMAPPED`; sales remain blank unless a product mapping and matching sales source exist. The updated scheduled refresh and live delivery totals were verified.

### Pending Next Actions

- **Since Jul 14** - Decide and implement the weekly Purely Elizabeth SPINS source-file refresh workflow - RECOMMENDED
- **Since Jul 14** - Run one final Package Lookup menu search after the one-field layout update and confirm its returned packages against the live warehouse - RECOMMENDED
- **Since Jul 7** - Update the saved `stg__olipop__crossplatform_raw_tbl_sched` transfer config with owner-account credentials so future scheduled runs keep excluding `1000heads` campaigns
- **Since Jun 24** - Observe the current Apollo, Ritual, and Olipop dashboards during normal end-user use

## 2026-07-16

- **CHANGED** - Added the separate FY26 Q2/Q3 Basis trafficking file to the maintained UTM loader and production mapping union. The live lookup now contains all 322 complete mappings from the approved `7.7` worksheet, while nine paused assignments without URLs remain excluded from production.
- **CHANGED** - Moved the live Ritual dashboard compatibility view to the v3 master model and expanded it from 125 to 241 fields without renaming the view or its Omni topic. All 115 previously hidden v3 fields are available, the canonical creative label now appears in Omni as `Creative Name` while its raw warehouse field remains intact, future non-conflicting v3 fields pass through automatically, and the established rows and reporting totals remained unchanged in validation.
- **CHANGED** - Converted the Purely Elizabeth delivery-plus-sales weekly output to a stored table and connected it to the existing two-hour `master_raw_CopyToWest` scheduled refresh.

- **CHANGED** - Updated the live Prisma scheduled queries so `CAMPAIGN_PUBLIC_ID` now flows from `landing.prisma_master_2025` into the expanded and processed Prisma outputs, including `20250327_data_model.prisma_expanded_full` and `prisma_processed_plusDCMimps`; the refreshed tables and downstream view were verified with populated values.
- **ADDED** - Added a concise Prisma pipeline guide and table catalog so each live Prisma table and view has a readable purpose, grain, lineage, refresh, and risk description.

## 2026-07-15

- **CHANGED** - Started the cross-repo Google auth unification rollout in `DCM_API`. The CM360 and Ritual Gmail Python helpers now prefer shared application-default credentials first, while older local token files remain as temporary fallbacks so existing runs are not cut off during migration.
- **ADDED** - Published and verified the Purely Elizabeth delivery-plus-sales weekly view and its campaign mapping table. The live output retains unique Sunday-ending week-and-product rows, sales-only and media-only history, and reconciled sales, TDP, and media measures.
- **CHANGED** - Standardized the weekly view's public fields so shared media measures mirror the main model and retail measures use `sales_dollars` and `sales_tdp`.
- **CHANGED** - Kept the Purely Elizabeth production weekly view metric-only at date and year-free product grain.
- **CHANGED** - Generalized the Purely Elizabeth weekly output to mapped product groups with delivery without relabeling the Protein Granola-specific sales source.
- **CHANGED** - Simplified Purely Elizabeth campaign maintenance so every valid mapping row is included, unmapped campaigns are excluded without a name fallback, and add, update, or remove actions can be run directly in BigQuery Studio.

## 2026-07-14

- **ADDED** - Published and verified a weekly Purely Elizabeth Protein Granola reporting view that combines the approved MULO and Natural Expanded product set. The live result contains 12 unique weeks with no missing dates and reconciles its Dollar sales and TDP totals to the approved source calculation.
- **CHANGED** - Condensed the native master-model package lookup Sheet to one case-insensitive partial search across Package ID, Package Name, and Package Friendly Name, while retaining the Manual Data Editor-style flight, plan, delivery, and metadata result fields.
- **FIXED** - Corrected the package lookup source so valid manual-only packages are included, and expanded the single search field to Supplier Code and Supplier Name so `QUAN`, `Columbus Circle DOOH`, and `ccdooh` can find the same package.
- **VERIFIED** - Confirmed that the signed-in Package Lookup menu can return a live package. A final menu check of the new one-field partial search remains pending.

### Pending Next Actions

- **Since Jul 14** - Decide and implement the weekly Purely Elizabeth SPINS source-file refresh workflow - RECOMMENDED
- **Since Jul 14** - Run one final Package Lookup menu search after the one-field layout update and confirm its returned packages against the live warehouse - RECOMMENDED
- **Since Jul 7** - Update the saved `stg__olipop__crossplatform_raw_tbl_sched` transfer config with owner-account credentials so future scheduled runs keep excluding `1000heads` campaigns
- **Since Jun 24** - Observe the current Apollo, Ritual, and Olipop dashboards during normal end-user use

## 2026-07-13

- **CHANGED** - Updated the Purely Elizabeth west-region reporting view so linear rows and `QUAN` supplier rows show planned spend and impressions in the delivered metric fields, while other rows keep their underlying delivered values.
- **CHANGED** - Converted the standalone RTL compatibility-table refresh from Google Sheets to persistent direct CM360 history. The live table keeps its original 18-column structure, and fields unavailable from direct CM360 remain blank instead of receiving invented values.
- **CHANGED** — Added the direct CM360 history `MERGE` to the existing 10:15 UTC master upstream scheduled query and its dedicated universal-runner upstream step. It refreshes the master-model CM360 history without reading the Google Sheet landing source, while leaving the schedule's social-pacing and TV snapshot logic unchanged.
- **REMOVED** — Removed the legacy RTL Google Sheet loader from the universal runner. Its old landing table remains comparison evidence only and no longer receives runner refreshes.
- **CHANGED** — Deployed direct CM360 RTL conversions into the current master-data-model V3 table. Persistent direct history now replaces the Google Sheet landing source; the model joins only at package/date/parsed-placement/creative detail, retains unmatched conversions as delivery-null evidence rows, and no longer builds then removes a dormant `conversion_activity` branch.
- **VERIFIED** — The direct-CM360 V3 candidate passed all 13 SQL Change Guard checks, and the live V3 model reconciled 83,402 conversions and $37,587.88 revenue to the persistent direct source.
- **CHANGED** — Added a master-data-model migration guide for moving RTL conversions from the current Google Sheet mirror to a direct CM360 source. The guide documents rolling-window history, lowest shared join grain, activity metrics, and the required QA-before-cutover boundary; it does not change production behavior.
- **ADDED** — Built the master-data-model QA-only direct-CM360 raw staging and detail-metrics candidate. It preserves direct source and revenue evidence, exposes the parsed detail key and unmatched-key status, and leaves production unchanged.
- **ADDED** — Extended that QA candidate with a full-outer delivery/conversion output, preserving conversion-only evidence without a new union branch or inferred delivery metrics.
- **ADDED** — Added a QA-only direct-CM360 history seed from the enriched backfill and current source, including a full-outer delivery/conversion proof. The production staging merge and v3 cutover remain pending approval.

## 2026-07-10

- **CHANGED** - Documented `master_stg.data_model_v3` as the current production master-model table. Existing package/date models, marts, and v2 outputs remain live compatibility surfaces pending a later verified migration and cleanup.

## 2026-07-09

- **FIXED** - Added a Manual Data Editor pre-upload preservation guard so a refresh stops before publishing if any previously accepted manual edit would disappear, become inactive or blocked, or lose an edited field without a newer user edit stamp.

### Pending Next Actions

- **Since Jul 7** - Update the saved `stg__olipop__crossplatform_raw_tbl_sched` transfer config with owner-account credentials so future scheduled runs keep excluding `1000heads` campaigns
- **Since Jun 24** - Observe the current Apollo, Ritual, and Olipop dashboards during normal end-user use

## 2026-07-08

- **ADDED** - Added Manual Data Editor benchmark metadata so users can edit Benchmark KPI as text and Benchmark Value as a number, with manual values feeding the master-model benchmark reporting fields before FPD benchmark fallbacks.
- **FIXED** - Added a Manual Data Editor fallback so trusted prior manual-only dates, metrics, and metadata survive blank sheet reads when no live source baseline exists.
- **FIXED** - Changed the Manual Data Editor backend merge rule so trusted prior manual rows replace stale generated sheet rows unless a source-backed sheet row has real user edit evidence or trusted raw history, preventing refreshes from splitting edits into blocked/valid duplicates, inventing source-backed manual rows, or dropping known package IDs.
- **FIXED** - Reverted the Manual Data Editor split-tab interface change so users keep editing in the existing `Package Editor` tab while benchmark fields and refresh-preservation safeguards remain in place.

### Pending Next Actions

- **Since Jul 7** - Update the saved `stg__olipop__crossplatform_raw_tbl_sched` transfer config with owner-account credentials so future scheduled runs keep excluding `1000heads` campaigns
- **Since Jun 24** - Observe the current Apollo, Ritual, and Olipop dashboards during normal end-user use

## 2026-07-07

- **FIXED** - Added a planned-metric fallback for master-model TV, Print, OOH, and dOOH package rows so reporting can use planned cost and impressions when delivered metrics are unavailable and planned impressions exist, without replacing real delivered values.
- **FIXED** - Stopped Manual Data Editor refreshes from clearing user-owned manual values when refreshed baselines match the manual replacement, including planned-only rows, manual-only rows, stale no-edit source rows, and date-serial readbacks that previously could turn modern flight dates into 1950s dates.
- **FIXED** - Hardened the Manual Data Editor refresh against feedback-loop deletion by excluding manual-applied rows from lookup baselines, publishing valid user-owned rows with metric, metadata, or flight-date evidence, reading editor cells as text, writing editor dates back as ISO text, falling back from manual-only delivery dates to missing package flight dates, and replacing the duplicate root loader implementation with a launcher to the canonical model/manual_editor loader.
- **FIXED** - Excluded `1000heads` campaigns from the shared-social source path before those rows can enter the master data model.
- **CHANGED** - Added a Campaign Manager 360 starter workflow note so future DCM API work starts from the documented read-only profile-list smoke test and existing Python environment.

### Pending Next Actions

- **Since Jul 7** - Update the saved `stg__olipop__crossplatform_raw_tbl_sched` transfer config with owner-account credentials so future scheduled runs keep excluding `1000heads` campaigns
- **Since Jun 24** - Observe the current Apollo, Ritual, and Olipop dashboards during normal end-user use

## 2026-07-06

- **ADDED** - Added and refreshed a master-model v3 conversion outcome path so Ritual conversion activity can be evaluated at package/date/site/creative/activity grain without changing package/date delivery metrics.

### Pending Next Actions

- **Since Jun 24** - Observe the current Apollo, Ritual, and Olipop dashboards during normal end-user use

## 2026-07-02

- **REVERTED** - Rolled back the automatic FPD metric pass-through experiment because the dynamic master-model wrapper made routine queries too expensive to plan. Existing unrelated work remains intact while the FPD metric-onboarding design is reconsidered.
- **FIXED** - Updated the live shared-social scheduled query config so `stg__olipop__crossplatform_raw_tbl_sched` uses the Reddit-aware production builder and appends Reddit staging rows on future scheduled runs.

### Pending Next Actions

- **Since Jun 24** - Observe the current Apollo, Ritual, and Olipop dashboards during normal end-user use

## 2026-07-01

- **FIXED** - Repaired the Basis UTM scratch view chain so `basis_utms_0519` reads from Looker-owned Basis and UTM staging objects instead of Giant Spoon or a missing `final_views` wrapper, while documenting its detail-grain row multiplication risk.
- **CHANGED** - Migrated the active Basis delivery refresh and main Looker Basis views away from Giant Spoon master-table reads. The Looker scheduled query now sources the Looker-owned external Google Sheets table `repo_stg.basis_gsheet2` and refreshes `repo_stg.basis_master2` without reading Giant Spoon Basis tables.
- **ADDED** - Documented the Basis, DCM, and master data model handoff so the Looker-owned Basis refresh, combined DCM/Basis reporting view, and master-model DCM branch have one shared troubleshooting map.

### Pending Next Actions

- **Since Jun 24** - Observe the current Apollo, Ritual, and Olipop dashboards during normal end-user use

## 2026-06-30

- **ADDED** - Created a live Omni AI eval prompt set for the shared Master Stg Data Model so future AI-answer checks cover normal performance questions, Ritual and Apollo topic routing, and the highest-risk QA cases: unmatched packages, missing delivery, pacing issues, source reconciliation, manual overrides, QTD reach, and v3 metric-grain behavior.
- **FIXED** - Made Reddit social ingestion work through the master-model mapping path and made the WP social QA builder runnable from maintained production staging instead of a missing scratch input table.
- **FIXED** - Added Reddit campaign-budget pacing to the master-model upstream social pacing refresh, removing the false missing-pacing status for Reddit delivery rows.

### Pending Next Actions

- **Since Jun 24** - Observe the current Apollo, Ritual, and Olipop dashboards during normal end-user use

## 2026-06-29

- **ADDED** - Built the master data model v3 sibling table as a clustered, lowest-available-grain evaluation version. It keeps the stable production master model untouched while preserving natural source grain, carrying summable planned metrics on one deduced package/date row, expanding DCM/FPD actuals to detail grain, and applying manual delivery overrides as package/date replacement rows.
- **CHANGED** - Added v3 package/date planned context fields with `doNotSum` names so rollups can reference planned spend and impressions while `_planned_*` sums remain correct without selecting a grain field.
- **CHANGED** - Added the v3 table refresh to the universal runner's master data model clustered advertiser refresh step, so the stored clustered QA table and v3 evaluation table are refreshed and verified together.
- **CHANGED** - Clarified the Prisma digital-plus-linear runbook so freshness checks follow the live view chain through processed Prisma, DCM, FPD, and TV sources instead of treating the final view as a self-refreshing table.
- **ADDED** - Exposed delivery-source evidence as a separate field on the Prisma digital-plus-linear planning view and scheduled snapshot table, so rows can show whether `Tracking Delivery` is backed by DCM, FPD, or both without changing the existing status label.

### Pending Next Actions

- **Since Jun 24** - Observe the current Apollo, Ritual, and Olipop dashboards during normal end-user use

## 2026-06-26

- **ADDED** - Added a daily scheduled refresh and dedicated universal runner line for the master data model's clustered advertiser QA table, with project instructions requiring that dependent stored tables be refreshed whenever their base view changes.
- **CHANGED** - Reduced duplicated BigQuery and data-modeling policy in repository instructions by pointing project rules to the global warehouse/modeling rule document and keeping only local proof ownership, QA isolation, and source-specific safeguards here.
- **FIXED** - Standardized advertiser short codes in the live master data model so mapped clients keep the same short code across social, WP social, manual, digital, TV, and Amazon rows.
- **REMOVED** - Filtered Highlights rows out of the live master data model and reporting mart.

### Pending Next Actions

- **Since Jun 24** - Observe the current Apollo, Ritual, and Olipop dashboards during normal end-user use

## 2026-06-25

- **ADDED** - Published package-level primary and available source fields to the master evidence model and reporting mart, with reporting labels recalculated after mart-only exclusions.
- **VERIFIED** - Confirmed the change against Olipop data from March 1 through May 31 with unchanged row coverage and delivery metrics.
- **CHANGED** - Published a simpler master-data-model schema with one consolidated FPD field family and clearer QA names, removing duplicate source totals, duplicate manual audit columns, and misleading query-runtime metadata.
- **CHANGED** - Updated Ritual, OLIPOP, Manual Data Editor, and active redesign consumers to use the new master-model contract without changing reporting totals.
- **FIXED** - Ensured explanatory comments are retained inside all affected live BigQuery view definitions by placing them directly under the outer query selection.

### Pending Next Actions

- **Since Jun 24** - Observe the current Apollo, Ritual, and Olipop dashboards during normal end-user use

## 2026-06-24

### Changed

- **FIXED** - Made the FPD multi-sheet combine tolerate mixed text and date cells in `partner_placement_name` by converting that one field to a stable character representation before binding source sheets.

  <details><summary>Paths - FPD partner placement normalization</summary>

  [FPD loader](/Users/eugenetsenter/Looker_clonedRepo/looker_personal/FPD/FPD_loader/util_collect_fpd_shortcutsFolder.r)
  [FPD loader changelog](/Users/eugenetsenter/Looker_clonedRepo/looker_personal/FPD/FPD_loader/CHANGELOG.md)

  </details>

- **CHANGED** - Added Manual Data Editor audit timestamps and editor identity from the live Google Sheet through the raw and daily landing tables into the master-model evidence fields. The loader now records the publish timestamp separately from the user edit timestamp, and the sheet-repair tooling preserves filters, conditional formatting, protections, and visibility settings.

  <details><summary>Paths - Manual Data Editor audit trail</summary>

  [Manual Data Editor README](/Users/eugenetsenter/Looker_clonedRepo/looker_personal/master_data_model/manual_package_edits/README.md)
  [QA runbook](/Users/eugenetsenter/Looker_clonedRepo/looker_personal/master_data_model/manual_package_edits/QA_RUNBOOK.md)
  [Table builder](/Users/eugenetsenter/Looker_clonedRepo/looker_personal/master_data_model/create_manual_package_edit_tables.sql)

  </details>

- **CHANGED** - Published Omni advertiser access control for the shared Master Stg Data Model topic after synchronizing `client_access` for 26 users and granting Ritual and Olipop the same `QUERY_TOPICS` role used by Apollo. Production tests verified all single-client and multi-client combinations, administrator bypass, and fail-closed behavior for unassigned users.

  <details><summary>Paths - Omni client access control</summary>

  [Omni operations README](/Users/eugenetsenter/Looker_clonedRepo/looker_personal/omni/README.md)
  [Omni changelog](/Users/eugenetsenter/Looker_clonedRepo/looker_personal/omni/CHANGELOG.md)

  </details>

- **BigQuery validation responsibility split**
  What: Consolidated repeated warehouse instructions into one workflow: semantic layers choose the live object and grain, SQL Change Guard owns broad pre-deployment comparison, and one focused live check proves the deployed behavior.
  Why: Prevents multiple skills and AGENTS rules from rerunning equivalent schema, key, and metric checks.
  Control: Preserved isolated QA objects, approval before production mutation, source-column coverage, unmatched-row metric exclusion, and QA-object metadata requirements.

  <details><summary>Paths - BigQuery validation responsibility split</summary>

  [Repository agent rules](/Users/eugenetsenter/Looker_clonedRepo/looker_personal/AGENTS.md)
  [Master data model agent rules](/Users/eugenetsenter/Looker_clonedRepo/looker_personal/master_data_model/AGENTS.md)

  </details>

### Added

- **Master Model Canonical Creative Name**
  What: Published `_creative_name` to the live package/date master model and reporting mart using original FPD creative, Amazon ad-name, and social creative fields.
  Why: Gives reporting consumers one consistent creative-label column without silently collapsing DCM's multiple creative-level records into a package/date value.
  Verification: Confirmed 80,639 populated live rows, zero precedence mismatches, and no changes to row count, spend, impressions, or clicks.
  Boundary: DCM creative remains available through the delivery-detail model.

  <details><summary>Paths - Master Model Canonical Creative Name</summary>

  [Master model SQL](/Users/eugenetsenter/Looker_clonedRepo/looker_personal/master_data_model/create_master_stg_data_model.sql)
  [Master model README](/Users/eugenetsenter/Looker_clonedRepo/looker_personal/master_data_model/README.md)
  [Master model map](/Users/eugenetsenter/Looker_clonedRepo/looker_personal/master_data_model/docs/master-data-model-map.html)

  </details>

### Pending Next Actions

- **Since Jun 24** - Observe the current Apollo, Ritual, and Olipop dashboards during normal end-user use

## 2026-06-05

### Changed

- **Manual Package Editor Daily Spend Allocation -codexapp [thread](https://chatgpt.com/codex)**
  What: Updated the manual package edit loader so spend overrides preserve source/editor decimal precision during daily allocation and daily proof allows sub-cent reconciliation noise.
  Why: The live mart can contain fractional-cent spend values, and the loader should block material daily-total mismatches instead of tiny float/rounding differences.

  <details><summary>Paths - Manual Package Editor Daily Spend Allocation</summary>

  [Manual package edit loader](/Users/eugenetsenter/Looker_clonedRepo/looker_personal/master_data_model/manual_package_edits/load_manual_package_edits.R)
  [Daily proof test](/Users/eugenetsenter/Looker_clonedRepo/looker_personal/master_data_model/manual_package_edits/tests/test_daily_total_proof.R)
  [Manual package edit README](/Users/eugenetsenter/Looker_clonedRepo/looker_personal/master_data_model/manual_package_edits/README.md)

  </details>

- **Manual Package Editor Visible Row Diagnostics -codexapp [thread](https://chatgpt.com/codex)**
  What: Added visible `Primary Row Data Source`, `Validation Status`, and `Validation Reason` columns to the package editor sheet.
  Why: Blocked rows should explain themselves directly in the editor instead of requiring a separate warehouse lookup.
  Control: Repaired conditional formatting, changed filter/slicer maintenance so ranges cover the full sheet grid instead of only the currently populated rows, added a hidden `Edited Row Filter` helper so the `Edited Rows` slicer no longer points to a single manual-marker column, and made the loader resolve the filter-repair helper and Node binary correctly when sourced by the universal runner.

  <details><summary>Paths - Manual Package Editor Visible Row Diagnostics</summary>

  [Manual package edit loader](/Users/eugenetsenter/Looker_clonedRepo/looker_personal/master_data_model/manual_package_edits/load_manual_package_edits.R)
  [Manual package edit README](/Users/eugenetsenter/Looker_clonedRepo/looker_personal/master_data_model/manual_package_edits/README.md)
  [Manual package edit QA runbook](/Users/eugenetsenter/Looker_clonedRepo/looker_personal/master_data_model/manual_package_edits/QA_RUNBOOK.md)
  [Filter range repair](/Users/eugenetsenter/Looker_clonedRepo/looker_personal/master_data_model/manual_package_edits/repair_manual_package_editor_filters.mjs)
  [Conditional formatting repair](/Users/eugenetsenter/Looker_clonedRepo/looker_personal/master_data_model/manual_package_edits/repair_manual_package_editor_conditional_formatting.mjs)

  </details>

- **Master Model Upstream Table Refresh -codexapp [thread](https://chatgpt.com/codex)**
  What: Added a master upstream scheduled-query SQL script that refreshes the stored social pacing and TV combined sibling tables, switched the master model TV branch to read the TV table sibling, and documented the table-refresh lineage.
  Why: Keeps expensive upstream dependencies materialized before the master model reads them while preserving the existing same-name views for lineage/debugging.

  <details><summary>Paths - Master Model Upstream Table Refresh</summary>

  [Upstream table refresh SQL](/Users/eugenetsenter/Looker_clonedRepo/looker_personal/master_data_model/create_master_data_model_upstream_tables_sched.sql)
  [Master model SQL](/Users/eugenetsenter/Looker_clonedRepo/looker_personal/master_data_model/create_master_stg_data_model.sql)
  [Master model README](/Users/eugenetsenter/Looker_clonedRepo/looker_personal/master_data_model/README.md)
  [Master model map](/Users/eugenetsenter/Looker_clonedRepo/looker_personal/master_data_model/docs/master-data-model-map.html)
  [Scheduled-query documentation](/Users/eugenetsenter/Looker_clonedRepo/looker_personal/docs/SCHEDULED_QUERIES.md)

  </details>

- **Master Data Model Map Current-State Refresh -codexapp [thread](https://chatgpt.com/codex)**
  What: Updated the interactive master model map for the current WP-first social path, canonical video views, manual package edit layer, reporting mart, and v2/detail sibling outputs. Added clickable dot-definition highlights, clarified stable-table versus view/logic indicators, added a visible `master_stg.data_model` view boundary, and made the master-script column show that separate manual edit tables are joined into the main rows before package rollups recalculate.
  Control: Added a project agent rule requiring `docs/master-data-model-map.html` to be updated in the same work session whenever durable master-model, mart, sibling-view, or stable upstream source semantics change.

  <details><summary>Paths - Master Data Model Map Current-State Refresh</summary>

  [Master model map](/Users/eugenetsenter/Looker_clonedRepo/looker_personal/master_data_model/docs/master-data-model-map.html)
  [Project agent rules](/Users/eugenetsenter/Looker_clonedRepo/looker_personal/master_data_model/AGENTS.md)
  [CHANGELOG.md](/Users/eugenetsenter/Looker_clonedRepo/looker_personal/CHANGELOG.md)

  </details>

- **Master Data Model Map v2 Documentation Upgrade -codexapp [thread](https://chatgpt.com/codex)**
  What: Created an advanced, dark-mode, glassmorphic interactive map (`docs/master-data-model-map-v2.html`) to complement the legacy map. Introduced versioned pipeline toggles for v1 Canonical, v2 Compatibility, and v3 Sibling sample views. Added a dynamic schema search engine mapping key column lineages directly to contributing nodes, with clickable deep absolute paths to local SQL codes and clickable BigQuery console URLs.
  Why: Gives developers an immersive, premium, and error-free tracing utility to audit schemas and model modifications in line with project repository rules.

  <details><summary>Paths - Master Data Model Map v2 Upgrade</summary>

  [Master model map v2](/Users/eugenetsenter/Looker_clonedRepo/looker_personal/master_data_model/docs/master-data-model-map-v2.html)
  [CHANGELOG.md](/Users/eugenetsenter/Looker_clonedRepo/looker_personal/CHANGELOG.md)

  </details>

## 2026-06-04

### Changed

- **WP Video Views Ingestion -codexapp [thread](https://chatgpt.com/codex)**
  What: Mapped the WP workbook `Report` tab `Video Views` metric into staging `video_view`, carried it through shared social, and added canonical master-model `_video_views` using source video views where available and video starts/plays as fallback.
  Control: The WP loader now forces Sheet columns to text before typed normalization so blank leading rows no longer cause zero-row loads.

## 2026-05-29

### Changed

- **WP Creative Image Canonicalization -codexapp [thread](https://chatgpt.com/codex)**
  What: WP YouTube Creative Box links now normalize to thumbnail URLs, WP YouTube channel codes use `video_yt`, social creative names come from ad names, and the master model exposes transformed WP images as `man_creative_img`.
  Reporting: Added canonical `_creative_img` as `COALESCE(fpd_creative_img, man_creative_img)` and removed the old `s_creative_box_link` output from the WP social path.

## 2026-05-27

### Changed

- **WP Delivery Workbook Promoted To Shared Social Production -codexapp [thread](https://chatgpt.com/codex)**
  What: Promoted the WP delivery workbook to the primary Apollo input in shared-social staging and the master model. Apollo Search and YouTube rows now report as Paid Search and Online Video, and social creative/source-provenance fields remain available in the master output.
  Decision: Campaign-separated Apollo records that share an ad ID are included for now and retain visible pending-source-owner provenance; exact duplicate campaign-grain records remain excluded.
  Control: The shared-social SQL builder is updated in its existing daily schedule. The WP Sheet loader remains a controlled manual production refresh until the source-owner identity question is resolved.
  Rollback: Preserved the pre-WP shared-social builder in SQL and saved a pre-WP master-model rollback checkpoint view before the production replacement.

  <details><summary>Paths - WP Production Promotion</summary>

  [WP workflow runbook](/Users/eugenetsenter/Looker_clonedRepo/looker_personal/wp/README.md)
  [WP production shared-social builder](/Users/eugenetsenter/Looker_clonedRepo/looker_personal/wp/sql/create_stg_crossplatform_wp_primary_production.sql)
  [Pre-WP shared-social rollback builder](/Users/eugenetsenter/Looker_clonedRepo/looker_personal/wp/sql/rollback_stg_crossplatform_pre_wp_production.sql)
  [Master model SQL](/Users/eugenetsenter/Looker_clonedRepo/looker_personal/master_data_model/create_master_stg_data_model.sql)
  [Scheduled-query documentation](/Users/eugenetsenter/Looker_clonedRepo/looker_personal/docs/SCHEDULED_QUERIES.md)

  </details>

## 2026-05-26

### Added

- **WP-First Shared Social QA Candidate -codexapp [thread unavailable](https://chatgpt.com/codex)**
  What: Added a QA-only WP delivery workbook loader, WP-first shared-social merge candidate, reporting-shaped social review view, focused rule tests, and validation queries. Candidate identity includes campaign, ad group, and ad; cross-campaign ad-ID conflicts are temporarily included as visibly flagged pending-review records rather than silently aggregated or selected.
  Why: Makes Apollo Paid Search, Online Video, creative metadata, and historical LinkedIn precedence testable without replacing production shared staging or the production master model before approval.

  <details><summary>Paths - WP-First Shared Social QA Candidate</summary>

  [WP QA runbook](/Users/eugenetsenter/Looker_clonedRepo/looker_personal/wp/README.md)
  [WP normalization rules](/Users/eugenetsenter/Looker_clonedRepo/looker_personal/wp/wp_search_social_logic.R)
  [WP Sheet loader](/Users/eugenetsenter/Looker_clonedRepo/looker_personal/wp/load_wp_search_data_template.R)
  [WP rule tests](/Users/eugenetsenter/Looker_clonedRepo/looker_personal/wp/tests/test_wp_search_social_logic.R)
  [QA SQL folder](/Users/eugenetsenter/Looker_clonedRepo/looker_personal/wp/sql)
  [Master model README](/Users/eugenetsenter/Looker_clonedRepo/looker_personal/master_data_model/README.md)
  [CHANGELOG.md](/Users/eugenetsenter/Looker_clonedRepo/looker_personal/CHANGELOG.md)

  </details>

### Fixed

- **TV Loader And Verifier Impressions Contract -codexapp [thread unavailable](https://chatgpt.com/codex)**
  Issue: The Universal Runner TV verifier could report impression mismatches even when the TV loaders wrote the intended table totals, because the verifier kept its own stale impression-column logic.
  Cause: Local and national TV loader impression rules changed after the runner verifier was created, but the verifier was not updated with the same source-file contract.
  Resolution: Added a shared TV impressions contract helper, updated both production TV loaders to use it, and added a focused contract test so loader and verifier logic stay aligned.

  <details><summary>Paths - TV Loader And Verifier Impressions Contract</summary>

  [TV loader README](/Users/eugenetsenter/Looker_clonedRepo/looker_personal/util/data_loaders/README.md)
  [TV impressions contract](/Users/eugenetsenter/Looker_clonedRepo/looker_personal/util/data_loaders/tv_impressions_contract.R)
  [TV impressions contract test](/Users/eugenetsenter/Looker_clonedRepo/looker_personal/util/data_loaders/tests/test_tv_impressions_contract.R)
  [Local TV loader](/Users/eugenetsenter/Looker_clonedRepo/looker_personal/util/data_loaders/gmail_to_bq__tv_local.r)
  [National TV loader](/Users/eugenetsenter/Looker_clonedRepo/looker_personal/util/data_loaders/gmail_to_bq__tv_nat.r)
  [CHANGELOG.md](/Users/eugenetsenter/Looker_clonedRepo/looker_personal/CHANGELOG.md)

  </details>

## 2026-05-20

### Added

- **Manual Package Editor QA Runbook**
  What: Added a scan-friendly QA runbook for the Manual Data Editor covering the end-to-end data flow, scripts, BigQuery objects, sheet controls, edit-detection baselines, validation rules, filters, run commands, QA queries, troubleshooting, and actions to avoid during live-sheet checks.
  Why: Makes it easier to investigate manual editor behavior without relying on memory, especially for stale markers, blocked new rows, request-refresh confusion, low-signal mart filtering, and package-specific warehouse traces.

  <details><summary>Paths - Manual Package Editor QA Runbook</summary>

  [master_data_model/manual_package_edits/QA_RUNBOOK.md](master_data_model/manual_package_edits/QA_RUNBOOK.md)
  [master_data_model/manual_package_edits/README.md](master_data_model/manual_package_edits/README.md)
  [master_data_model/README.md](master_data_model/README.md)
  [CHANGELOG.md](CHANGELOG.md)

  </details>

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

- **Manual Package Editor Refresh Command**
  What: Made the production Manual Data Editor sheet the default loader target and simplified the request-refresh notification command to a single copy/paste-safe `Rscript` command.
  Why: The email previously showed an environment-variable prefix split across lines, which was easy to paste incorrectly even though routine production refreshes should target the one live editor sheet.

  <details><summary>Paths - Manual Package Editor Refresh Command</summary>

  [master_data_model/manual_package_edits/load_manual_package_edits.R](master_data_model/manual_package_edits/load_manual_package_edits.R)
  [master_data_model/manual_package_edits/apps_script/Code.js](master_data_model/manual_package_edits/apps_script/Code.js)
  [master_data_model/manual_package_edits/README.md](master_data_model/manual_package_edits/README.md)
  [master_data_model/manual_package_edits/QA_RUNBOOK.md](master_data_model/manual_package_edits/QA_RUNBOOK.md)
  [master_data_model/README.md](master_data_model/README.md)
  [CHANGELOG.md](CHANGELOG.md)

  </details>

- **Manual Package Editor Stale Manual Marker Cleanup**
  What: Changed the manual package editor loader so edit detection compares visible Sheet values to source-derived baselines instead of manual-affected final fields. Planned package totals now come directly from PRISMA package totals, delivered metric baselines are recalculated from raw delivery fields, and regression tests cover undo, correction-of-correction, blank-cell reversion, zero overrides, and stale manual cleanup.
  Why: Prevents false purple manual markers when filtered mart row sums differ from full-flight planned totals, and clears stale backend `man_*` values when source data catches up to an old manual correction without changing the final dashboard value.

  <details><summary>Paths - Manual Package Editor Stale Manual Marker Cleanup</summary>

  [master_data_model/manual_package_edits/load_manual_package_edits.R](master_data_model/manual_package_edits/load_manual_package_edits.R)
  [master_data_model/manual_package_edits/tests/test_loader_choice_logic.R](master_data_model/manual_package_edits/tests/test_loader_choice_logic.R)
  [master_data_model/manual_package_edits/README.md](master_data_model/manual_package_edits/README.md)
  [master_data_model/README.md](master_data_model/README.md)
  [CHANGELOG.md](CHANGELOG.md)

  </details>

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
