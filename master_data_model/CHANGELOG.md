# Changelog

## 2026-08-10

**Offline delivery fallback restored in the production master model** ([SQL](/Users/eugenetsenter/Looker_clonedRepo/looker_personal/master_data_model/model/final_model/create_master_stg_data_model_v3.sql)) — 🟢 **Verified and committed**<br>The [master-model refresh](/Users/eugenetsenter/Docs/R_Studio_Projects/universal_cron_runner/automation_hub/workloads/ops/bq_trigger/run_master_data_model_clustered_advertiser_refresh.sh) again publishes planned spend and impressions as delivered values for eligible planned-only TV, Print, OOH, and dOOH rows while preserving real delivery and every unaffected row. The live model contains 409 eligible rows with zero fallback mismatches, and the west-region reporting copy was refreshed successfully. [More details](/Users/eugenetsenter/Looker_clonedRepo/looker_personal/master_data_model/model/final_model/data_model_v3_offline_fallback.qa.json)

**Apollo LinkedIn identity is canonical across every production model layer** ([SQL](/Users/eugenetsenter/Looker_clonedRepo/looker_personal/master_data_model/model/stable_base/create_master_stg_data_model.sql)) — 🟢 **Verified and committed**<br>The [master data model](/Users/eugenetsenter/Looker_clonedRepo/looker_personal/master_data_model/README.md) reports LinkedIn rows whose source account is `Apollo Corporate` under the canonical advertiser `Apollo`, while retaining the original account text. All 22,406 matching rows are correct in the base, clustered, and V3 layers with zero incorrect labels and both raw account spellings preserved. [More details](/Users/eugenetsenter/Looker_clonedRepo/looker_personal/master_data_model/model/stable_base/tests/test_apollo_advertiser_normalization.R)

### Pending Next Actions

- **Since Aug 6** - Reconcile preserved master-model, reporting, and loader work into the canonical branch before deleting the remaining preservation branches - BLOCKER
- **Since Jul 14** - Decide and implement the weekly Purely Elizabeth SPINS source-file refresh workflow - RECOMMENDED
- **Since Jul 14** - Run one final Package Lookup menu search and confirm its result against the live warehouse

## 2026-07-20

- **CHANGED** - Restored the Purely Elizabeth dashboard fields `sales_dollars` and `sales_tdp` as temporary Protein Granola-only compatibility copies while dashboards move to `product_group_sales_dollars` and `product_group_tdp`.
- **CHANGED** - Updated the Purely Elizabeth weekly dashboard table with renamed product-group sales/TDP fields and four Total Brand and Total Brand Granola benchmark fields. The scheduled rebuild now joins the expanded weekly sales view by date, keeps all sales blank for `UNMAPPED`, and includes a dashboard migration guide for replacing the previous field names.

## 2026-07-17

- **FIXED** - Corrected the Purely Elizabeth weekly delivery-and-sales table so campaigns without a product mapping retain their delivery as `UNMAPPED`, while retail sales remain limited to eligible mapped products. The scheduled refresh and live delivery totals were verified.
- **FIXED** - Aligned Manual Data Editor visible text and hidden comparison baselines by normalizing source text on both paths. Unedited rows with harmless leading or trailing source whitespace no longer receive false orange change formatting. The corrected loader and exact regression test were verified in the v2 test Sheet, then deployed to and verified in the production Sheet.
- **CHANGED** - Added a stable-base source boundary that excludes literal zero-metric social API tail rows after a known pacing end, while preserving the physical raw source for audit. The isolated candidate removed 5,769 non-contributing base-model rows across 73 packages with no meaningful delivery-total change; production was deployed July 17, and the dependent support table, v3 model, reporting mart, production lookup, and Manual Data Editor were refreshed and checked.
- **CHANGED** - Extended the shared-social source exclusion to campaign names containing `PROS_Dysrupt`, alongside the existing `1000heads` agency exclusion, so these rows do not enter the master-model social input.
- **ADDED** - Added a permanent Manual Data Editor history ledger. Before the loader replaces the current raw snapshot, it now appends every accepted edit to a partitioned recovery/audit table; the live accepted edits were seeded into that ledger.
- **FIXED** - Cleared the entire existing Manual Data Editor value grid before each rewrite, while preserving formatting. Old values can no longer remain below a shorter refreshed package list and appear as fake editor rows.
- **FIXED** - Made the Manual Data Editor source-first: inactive, unedited Sheet-only leftovers can no longer become visible package rows. The loader retains only current lookup packages and audited manual-only packages, stops before clearing unaudited Sheet-only values, and refuses to choose between duplicate Sheet rows for one source package.
- **FIXED** - Separated planned-flight and delivery-override date authority in Manual Data Editor validation and daily allocation. Planned Spend and Planned Impressions now use package flight dates, while delivered metric overrides use delivery override dates; one no longer invalidates or redistributes the other.
- **FIXED** - Stopped false incomplete-row and planned-cell red formatting caused by blank source flight dates or delivery dates that differ from the planned flight.

## 2026-07-16

- **CHANGED** - Moved the live Ritual dashboard compatibility view to the v3 master model and expanded it from 125 to 241 fields. All 115 previously hidden v3 fields are available, and the canonical `_creative_name` now also appears in Omni as the friendly `Creative Name` field while the raw field remains intact. Established dashboard names, rows, and reporting totals remain unchanged; future non-conflicting v3 fields also pass through automatically. The live Omni topic and the unmerged review branch both queried the expanded fields successfully.
- **CHANGED** - Converted the Purely Elizabeth delivery-plus-sales weekly output to a stored table and moved its rebuild into the existing `master_raw_CopyToWest` scheduled query, preserving the two-hour refresh cadence.
- **CHANGED** - Updated the live `Prisma_expanded` scheduled query to carry `CAMPAIGN_PUBLIC_ID` from `landing.prisma_master_2025` into `20250327_data_model.prisma_expanded_full`, then refreshed and verified the expanded table.
- **ADDED** - Added a concise Prisma pipeline guide and table catalog documenting the purpose, grain, upstream/downstream lineage, refresh ownership, field paths, and modeling risks for the live Prisma tables and views.
- **FIXED** - Extended the separate `process_prisma` package aggregation so `CAMPAIGN_PUBLIC_ID` survives into `prisma_porcessed`, `prisma_porcessed_with_placements`, and the DCM/FPD-enriched `prisma_processed_plusDCMimps` view.

## 2026-07-15

- **ADDED** - Published the Purely Elizabeth delivery-plus-sales weekly reporting view and its `PE` campaign mapping table. The view preserves sales-only, media-only, and overlapping Sunday-ending weeks, inherits the existing Purely Elizabeth linear and `QUAN` planned-as-delivered interpretation, and keeps unavailable measures null.
- **VERIFIED** - Confirmed unique week-and-product rows with no null grain values, exact sales and TDP reconciliation, media reconciliation within floating-point precision, and no sales multiplication across campaign rows.
- **CHANGED** - Aligned the Purely Elizabeth weekly view's shared media fields with the main model and renamed its retail measures to `sales_dollars` and `sales_tdp`.
- **CHANGED** - Reduced the Purely Elizabeth production weekly view to 12 fields: Sunday-ending date, year-free product group, eight approved media measures, and two sales measures.
- **CHANGED** - Removed the Protein-Granola-only media restriction. Included mapping-table product groups now qualify when they have delivery, while Protein Granola sales remain scoped to their actual source product.
- **CHANGED** - Made the campaign mapping table the complete inclusion list. Every valid mapping row is included, unmapped campaigns are excluded without a name fallback, and routine BigQuery Studio queries now add, update, or remove mappings without editing the weekly view.

## 2026-07-14

- **ADDED** - Created a native Google Sheet for finding package-level master-model values by Package ID, Package Name, or Package Friendly Name. The bound script and live source are in place, but the first signed-in menu authorization and returned-result check remain pending.

### Pending Next Actions

- **Since Jul 14** - Authorize the Package Lookup menu in Google Sheets and confirm one returned package against the live warehouse - RECOMMENDED
- **Since Jul 10** - Restore access to the FPD partner template's bound Apps Script, then inspect and recreate its workbook dependencies - BLOCKER
- **Since Jul 10** - Recreate or locate the current template's sheet-bound Process FPD wrapper, then update its library's archived master-workbook reference - BLOCKER
- **Since Jul 10** - Plan and verify the v3 reporting migration before deleting or replacing any compatibility files, tables, or views.

## 2026-07-13

- **CHANGED** - Updated the Purely Elizabeth west-region reporting view so linear rows and `QUAN` supplier rows show planned spend and impressions in the delivered metric fields, while other rows keep their underlying delivered values.
- **CHANGED** - Refreshed the legacy-schema RTL compatibility table from persistent direct CM360 history instead of Google Sheets. The table keeps all 18 existing columns; delivery-only and Sheet-lineage fields that direct CM360 does not provide remain blank.
- **CHANGED** — Added the direct CM360 history `MERGE` to the existing 10:15 UTC master upstream scheduled query and its dedicated universal-runner upstream step. The job now refreshes direct CM360 history without using the Google Sheet landing source; its social-pacing and TV snapshot statements remain unchanged.
- **REMOVED** — Removed the legacy RTL Google Sheet loader from the universal runner. The old Sheet landing table remains comparison evidence only and is no longer refreshed by the runner.
- **CHANGED** — Deployed direct CM360 RTL conversions to the current V3 master model. V3 now reads persistent direct CM360 history instead of the Google Sheet landing table, joins only at package/date/parsed-placement/creative detail, preserves conversion-only evidence rows with null delivery metrics, exposes direct conversion, revenue, activity, and refresh fields, and no longer builds then filters out a dormant `conversion_activity` branch.
- **ADDED** — Added production bootstrap and routine history-merge SQL. The routine accepts only an enriched CM360 export with package/placement fields and a report window no wider than 14 days; it updates matching source rows without deleting historical conversion evidence.
- **VERIFIED** — The isolated direct-CM360 V3 candidate passed all 13 SQL Change Guard checks. The deployed V3 live check reconciled 83,402 conversions and $37,587.88 revenue to direct CM360, with zero retired Sheet conversion rows and zero conversion-only rows carrying delivery metrics.
- **CHANGED** — Documented the planned direct-CM360 conversion source: it will retain rolling-window history, preserve conversion and revenue evidence, and join activity metrics to delivery detail without duplicating delivery metrics. The current Sheet-based production source remains unchanged pending QA and approval.
- **ADDED** — Built QA-only direct-CM360 raw staging and detail-metrics sidecar tables. They preserve all source fields, activity and revenue metrics, refresh metadata, parsed package/placement keys, and visible unmatched-key statuses; the current Sheet source and production model remain unchanged.
- **ADDED** — Extended the direct-CM360 QA candidate with a full-outer DCM-detail/conversion output. It visibly retains delivery-only, matched delivery-plus-conversion, and conversion-only records; conversion-only records have null delivery metrics and are not unioned into other branches.
- **ADDED** — Built a QA-only direct-CM360 history seed and full-outer output using the corrected enriched historical backfill plus the current enriched source. It proves source-field preservation, deterministic overlap handling, delivery-metric preservation, and explicit conversion-only records; production remains unchanged pending deployment approval.

## 2026-07-10

- **CHANGED** - Promoted `master_stg.data_model_v3` to the documented current production master model. The lower-grain v3 table is now the default for new master-model work; `data_model`, its reporting mart, and related v2 outputs remain live compatibility surfaces until a separately verified reporting migration and cleanup.
- **CHANGED** - Corrected the FPD partner-template documentation to identify the post-Workplace-migration workbook as the current source and to label the prior workbook findings as historical until the workbook and its bound Apps Script can be inspected and recreated.
- **CHANGED** - Documented the FPD template migration QA: the recovered duplicator projects are standalone libraries, recent direct editor runs used the wrong entrypoint, and the library still reads mappings and logs from the archived template rather than the current migrated workbook.
- **CHANGED** - Published a separate FPD duplicator-library v3 release that uses the current partner template for mappings and logging, while preserving the existing v2 library release for older sheets.

### Pending Next Actions

- **Since Jul 10** - Restore access to the FPD partner template's bound Apps Script, then inspect and recreate its workbook dependencies - BLOCKER
- **Since Jul 10** - Recreate or locate the current template's sheet-bound Process FPD wrapper, then update its library's archived master-workbook reference - BLOCKER
- **Since Jul 10** - Plan and verify the v3 reporting migration before deleting or replacing any compatibility files, tables, or views.

## 2026-07-09

- **FIXED** - Added a Manual Data Editor pre-upload preservation guard so a refresh stops before publishing if any previously accepted manual edit would disappear, become inactive or blocked, or lose an edited field without a newer user edit stamp.

### Pending Next Actions

- **Since Jul 7** - Resolve the separate v3 visible-key duplicate QA failure found after the `1000heads` exclusion refresh
- **Since Jun 16** - Build the redesigned reporting tables described by the plan
- **Since Jun 16** - Finish the interactive workflow for matching each source's fields to the master table
- **Since Jun 16** - Publish the updated interactive maps to the shared location

## 2026-07-08

- **ADDED** - Added Manual Data Editor benchmark metadata so users can edit Benchmark KPI as text and Benchmark Value as a number, with manual values feeding the new final benchmark reporting fields before FPD benchmark fallbacks.
- **FIXED** - Added a Manual Data Editor fallback so trusted prior manual-only dates, metrics, and metadata survive blank sheet reads when no live source baseline exists.
- **FIXED** - Changed the Manual Data Editor backend merge rule so trusted prior manual rows replace stale generated sheet rows unless a source-backed sheet row has real user edit evidence or trusted raw history, preventing refreshes from splitting edits into blocked/valid duplicates, inventing source-backed manual rows, or dropping known package IDs.
- **FIXED** - Reverted the Manual Data Editor split-tab interface change so users keep editing in the existing `Package Editor` tab while benchmark fields and refresh-preservation safeguards remain in place.
- **CHANGED** - Documented the FPD partner template mapping-table correction so duplicate package labels and partner aliases resolve through a dynamic stable-label block instead of the old typed-table dropdown.
- **CHANGED** - Updated the FPD partner template notes after moving the temp-copy mapping prototype back to `Config!H:J`, including the typed-table dropdown blocker and the next likely formula/validation failure paths.

### Pending Next Actions

- **Since Jul 7** - Resolve the separate v3 visible-key duplicate QA failure found after the `1000heads` exclusion refresh
- **Since Jun 17** - Define who must sign off and what proof they need before replacing the production model - BLOCKER
- **Since Jun 16** - Build the redesigned reporting tables described by the plan
- **Since Jun 16** - Finish the interactive workflow for matching each source's fields to the master table
- **Since Jun 16** - Publish the updated interactive maps to the shared location

## 2026-07-07

- **FIXED** - Added a planned-metric fallback for TV, Print, OOH, and dOOH package rows so package/date outputs can use planned cost and impressions when delivered metrics are unavailable and planned impressions exist, without replacing real delivered values.
- **FIXED** - Stopped Manual Data Editor refreshes from clearing user-owned manual values when refreshed baselines match the manual replacement, including planned-only rows, manual-only rows, stale no-edit source rows, and date-serial readbacks that previously could turn modern flight dates into 1950s dates.
- **FIXED** - Hardened the Manual Data Editor refresh against feedback-loop deletion by excluding manual-applied rows from lookup baselines, publishing valid user-owned rows with metric, metadata, or flight-date evidence, reading editor cells as text, writing editor dates back as ISO text, falling back from manual-only delivery dates to missing package flight dates, and replacing the duplicate root loader implementation with a launcher to the canonical model/manual_editor loader.
- **FIXED** - Excluded `1000heads` campaigns from the shared-social source path before those rows can enter the master model, with the Manual Data Editor loader also dropping stale manual rows tied to those excluded social packages.
- **CHANGED** - Updated the Manual Data Editor sheet workflow to use standard column header filters, show Advertiser as the first visible column, and highlight the specific required cells that need attention on started new rows.
- **CHANGED** - Clarified that current master-model work should start in the organized `model/` workspace, keeping legacy root-level SQL and docs as compatibility or history unless explicitly requested.
- **CHANGED** - Kept Manual Data Editor script authentication pointed at the newer Google account by using cached R OAuth for Sheets/Drive and the active `gcloud` token for BigQuery, so non-interactive refreshes do not fall back to the legacy account.
- **CHANGED** - Pointed the Manual Data Editor loader and repair helpers at the new Google Drive workbook so normal refreshes no longer default to the legacy copy.
- **FIXED** - Added a Manual Data Editor loader stop condition so manual-only draft packages cannot be silently rewritten as inactive blank rows.
- **FIXED** - Reduced false Manual Data Editor blockers by leaving unknown social flight dates blank and by stopping prior blocked rows without manual evidence from reactivating themselves as edits.
- **FIXED** - Updated README_v2 with the deployed v3 digital conversion outcome path, source inventory row, field-family notes, and conversion-row warning.

### Pending Next Actions

- **Since Jul 7** - Resolve the separate v3 visible-key duplicate QA failure found after the `1000heads` exclusion refresh
- **Since Jun 17** - Define who must sign off and what proof they need before replacing the production model - BLOCKER
- **Since Jun 16** - Build the redesigned reporting tables described by the plan
- **Since Jun 16** - Finish the interactive workflow for matching each source's fields to the master table
- **Since Jun 16** - Publish the updated interactive maps to the shared location

## 2026-07-06

- **ADDED** - Added and refreshed the v3 digital conversion outcome path so Ritual conversion rows can be evaluated at package/date/site/creative/activity grain without changing package/date delivery metrics.

### Pending Next Actions

- **Since Jun 17** - Define who must sign off and what proof they need before replacing the production model - BLOCKER
- **Since Jun 16** - Build the redesigned reporting tables described by the plan
- **Since Jun 16** - Finish the interactive workflow for matching each source's fields to the master table
- **Since Jun 16** - Publish the updated interactive maps to the shared location

## 2026-07-02

- **REVERTED** - Rolled back the automatic FPD metric wrapper because it made the public master view too expensive to plan for routine queries. The master model is back to the static view shape while a leaner downstream metric-onboarding design is reconsidered.
- **FIXED** - Updated the live shared-social scheduled query config so the daily social staging refresh includes the Reddit staging union instead of overwriting the shared social table from the older WP-only SQL.
- **FIXED** - Moved the Manual Data Editor package lookup into a stored BigQuery table so loader runs download a ready package snapshot instead of forcing the reporting mart to plan another complex package rollup.
- **FIXED** - Corrected the Manual Data Editor planned-cell color rule so valid full-flight planned manual rows stay purple instead of showing a misleading red warning.

### Pending Next Actions

- **Since Jun 17** - Define who must sign off and what proof they need before replacing the production model - BLOCKER
- **Since Jun 16** - Build the redesigned reporting tables described by the plan
- **Since Jun 16** - Finish the interactive workflow for matching each source's fields to the master table
- **Since Jun 16** - Publish the updated interactive maps to the shared location

## 2026-07-01

- **ADDED** - Created a v2 master-model README in the MFT pipeline documentation style, with a visual pipeline overview, source inventory, output map, operational scripts, verification guidance, and troubleshooting path.
- **ADDED** - Documented the DCM branch boundary so Basis delivery, the joined DCM/Basis reporting view, DCM cost-model evidence, and package/date master-model rows are easier to debug without mixing their source paths.

### Pending Next Actions

- **Since Jun 17** - Define who must sign off and what proof they need before replacing the production model - BLOCKER
- **Since Jun 16** - Build the redesigned reporting tables described by the plan - RECOMMENDED
- **Since Jun 16** - Finish the interactive workflow for matching each source's fields to the master table
- **Since Jun 16** - Publish the updated interactive maps to the shared location

## 2026-06-30

- **ADDED** - Created the organized `model/` workspace so current work can start from function and source-branch folders instead of the legacy root-level file pile, with runner-used files now canonical there, WP Search and Reddit social assets copied under the same roof, and stale root docs/deprecated SQL removed from the active path.
- **CHANGED** - Added the v3 lowest-grain evaluation table refresh to the universal runner's master-model clustered advertiser refresh step, so a normal runner refresh now rebuilds both stored master-model support tables and checks v3's no-duplicate/no-extra-planned-carrier contract.
- **FIXED** - Deployed Reddit social-row ingestion through the master-model mapping path, including Reddit source lineage, refresh timestamps, platform normalization, and the Olipop Reddit advertiser alias. Verified the master model, reporting mart, clustered QA table, and v3 table all retain 654 Reddit rows with Olipop advertiser mapping.
- **FIXED** - Made the WP social QA builders runnable without a missing scratch input table by pointing the QA candidate at the maintained WP staging table.
- **FIXED** - Added Reddit campaign-budget pacing to the upstream social pacing snapshot so Reddit actuals can receive planned spend from their source campaign budget instead of being flagged as missing social pacing.

## 2026-06-29

- **ADDED** - Built the clustered `master_stg.data_model_v3` sibling table as a lowest-available-grain master-model candidate. Natural source rows stay at source grain, summable planned metrics are carried on one deduced package/date row, and manual delivery overrides suppress lower-grain final actuals for the same package/date.
- **CHANGED** - Added `qa_v3_package_planned_spend_doNotSum` and `qa_v3_package_planned_impressions_doNotSum` to v3 rows so rollups can access package/date planned context while `_planned_*` sums remain correct without selecting a grain field.
- **VERIFIED** - Compared June 2026 v3 totals against stable `master_stg.data_model`: package/date count stayed aligned, planned spend/impressions matched to floating-point noise, every package/date had at most one summable planned carrier, and the visible v3 grain key had zero duplicates.

## 2026-06-26

- **ADDED** - Created a daily scheduled refresh and a dedicated universal runner line for the clustered advertiser QA table, with a project rule that dependent stored tables must be refreshed whenever their base master view changes.
- **CHANGED** - Simplified local master-model instructions so global BigQuery, SQL, grain, read-only permission, and modeling-compromise rules live in the global rule document, while this project keeps only master-specific overlays and field semantics.
- **FIXED** - Standardized advertiser short codes across the live master evidence model and reporting mart so social, WP social, manual, digital, TV, and Amazon rows use the same mapped client code where one exists.
- **REMOVED** - Filtered Highlights rows out of the live master evidence model and reporting mart.

### Pending Next Actions

- **Since Jun 17** - Define who must sign off and what proof they need before replacing the production model - BLOCKER
- **Since Jun 16** - Build the redesigned reporting tables described by the plan - RECOMMENDED
- **Since Jun 16** - Finish the interactive workflow for matching each source's fields to the master table
- **Since Jun 16** - Publish the updated interactive maps to the shared location

## 2026-06-25

- **ADDED** - Published package-level source fields that identify the source populating final delivery metrics and list every source available to the package. Packages with different metric contributors are labeled `multiple`, while packages with no final actual source are labeled `none`.
- **VERIFIED** - Validated the evidence model and reporting mart against Olipop data from March 1 through May 31, confirming unchanged rows and metrics, stable package labels, and mart-side recalculation after reporting filters.
- **CHANGED** - Simplified the live master evidence model and reporting mart by removing duplicate QA columns, renaming ambiguous fields, standardizing healthy rows as `no_issues`, and keeping boolean indicators under the `_flag` suffix.
- **CHANGED** - Replaced separate original/revised FPD reporting columns with one consolidated `fpd_*` family, including final metrics, benchmark, factor, creative, and contributing-source lineage.
- **FIXED** - Moved purpose notes directly under each outer `SELECT` so BigQuery preserves the comments in all 14 affected live view definitions instead of stripping file-leading headers.
- **VERIFIED** - Confirmed the live master, marts, Ritual views, redesign views, and OLIPOP downstream view all query successfully while row count and reporting metrics remain aligned with the validated baseline.

### Pending Next Actions

- **Since Jun 17** - Define who must sign off and what proof they need before replacing the production model - BLOCKER
- **Since Jun 16** - Build the redesigned reporting tables described by the plan - RECOMMENDED
- **Since Jun 16** - Finish the interactive workflow for matching each source's fields to the master table
- **Since Jun 16** - Publish the updated interactive maps to the shared location

## 2026-06-24

- **ADDED** - Published canonical source and freshness fields to the live master evidence model and reporting mart. Every row now identifies its driving source table and operational refresh time, while FPD and Manual Data Editor rows separately expose reliable source-content modification time.
- **VERIFIED** - Confirmed the source fields are populated across all current rows, social rows resolve to their real platform tables, and row count, spend, impressions, and clicks remain unchanged apart from normal floating-point precision.
- **CHANGED** - Replaced overlapping validation passes with one lean deployment workflow: verify live/local parity once, use SQL Change Guard as the sole broad pre-deployment comparison, perform one focused live master/mart check, and clean up temporary artifacts once.
- **CHANGED** - Standardized both advertiser fields to one readable client name through a separate mapping table. New clients now default to Prisma's advertiser name, with legal suffixes such as `Inc`, `LLC`, `Corp`, and `Ltd` removed automatically.
- **ADDED** - Published a canonical creative-name field to the live package/date model and reporting mart using available FPD, Amazon, and social labels. DCM creative remains in the delivery-detail model so multiple creatives are not silently collapsed into one value.
- **VERIFIED** - Confirmed the live field has 80,639 populated rows, follows the intended FPD-to-Amazon-to-social precedence with no mismatches, and leaves row count, spend, impressions, and clicks unchanged.

### Next

- Publish the verified `_creative_name` change to the live master model and confirm downstream visibility - DONE

### Pending Next Actions

- **Since Jun 17** - Define who must sign off and what proof they need before replacing the production model - BLOCKER
- **Since Jun 16** - Build the redesigned reporting tables described by the plan - RECOMMENDED
- **Since Jun 16** - Finish the interactive workflow for matching each source's fields to the master table
- **Since Jun 16** - Publish the updated interactive maps to the shared location

## 2026-06-23

- **ADDED** - Prepared Manual Data Editor audit fields for the Sheet, manual landing tables, master evidence model, and reporting mart: row-level manual-edit status, edit timestamp, editor when Google exposes it, and loader publish timestamp.
- **IMPROVED** - Documented the difference between row edit time and loader publish time so manual-edit audit fields do not overstate what Google Sheets can identify.

### Next

- Deploy the manual table schema and master views after approval, then run the loader and verify the live Sheet plus BigQuery fields end to end - PENDING

## 2026-06-18

- **IMPROVED** - Expanded the Manual Data Editor product brief into slide-ready presentation notes and added the problems and bottlenecks the workflow resolves.
- **IMPROVED** - Replaced the duplicated Manual Data Editor workflow flowchart with a decision flow showing request boundaries, edit detection, validation, grain handling, and dashboard debugging.
- **ADDED** - Created a BMAD product brief for presenting the Manual Data Editor to a technical team, including an operator workflow diagram and main data model integration diagram.
- **ADDED** - Created a clickable Manual Data Editor workflow map showing the Sheet edit surface, request notification path, loader write path, manual evidence tables, model merge, reporting mart, and troubleshooting loop.
- **IMPROVED** - Added technical-audience notes to the Manual Data Editor workflow map covering write boundaries, comparison inputs, warehouse grain, model merge rules, validation proof, and common dashboard filter traps.
- **IMPROVED** - Simplified the Manual Data Editor workflow map into a presentation-ready five-step flow with technical details moved into expandable notes.
- **IMPROVED** - Added a workflow flowchart and main data model integration map to the Manual Data Editor workflow artifact.

### Next

- Publish the updated interactive maps to the shared location - PENDING

## 2026-06-17

- **FIXED** - Rebuilt the redesign's separate test version so it preserves every current master field and matches spend, impressions, and clicks both overall and by package.
- **IMPROVED** - Reduced the final test table from 258 columns to 217 by removing internal troubleshooting fields and keeping only 20 useful new fields.
- **FIXED** - Corrected project status so the verified test version is clearly separated from unfinished work to match source fields and safely replace the production model.

Related sessions:

- [Redesign test and proof][session-redesign-test-proof]

### Next

- Define who must sign off and what proof they need before replacing the production model - PENDING

### Pending Next Actions

- **Since Jun 17** - Define who must sign off and what proof they need before replacing the production model - BLOCKER
- **Since Jun 16** - Build the redesigned reporting tables described by the plan - RECOMMENDED
- **Since Jun 16** - Finish the interactive workflow for matching each source's fields to the master table
- **Since Jun 16** - Publish the updated interactive maps to the shared location

## 2026-06-16

- **PLANNED** - Turned the redesign idea into a build-ready plan for one flexible master table that supports package, creative, market, and other detail levels without hiding missing information.
- **ADDED** - Created a clickable DCM cost-model map and replaced confusing risk language with plain warnings and `join key` wording—the shared value used to connect matching records.
- **ADDED** - Created a four-page redesign report showing the proposed master table, how new data sources would be added, what still needs to be built, and how production could be replaced safely.

Related sessions:

- [Flexible master-table design][session-flexible-master-table]
- [DCM cost-model map][session-dcm-cost-model-map]
- [Redesign report][session-redesign-report]

### Next

- Complete a verified test version alongside production - DONE
- Reduce the final table to only useful new fields - DONE
- Finish the interactive workflow for matching each source's fields to the master table - PENDING
- Build the redesigned reporting tables described by the plan - PENDING
- Publish the updated interactive maps to the shared location - PENDING

## 2026-06-08

- **ADDED** - Amazon Ads is now included in master reporting, with Amazon's media cost counted as spend while sales remain revenue.

Related sessions:

- [Amazon Ads reporting][session-amazon-ads-reporting]

[session-redesign-test-proof]: /Users/eugenetsenter/.codex/sessions/2026/06/16/rollout-2026-06-16T13-22-25-019ed174-adb3-7001-93cb-3fb1add59980.jsonl
[session-flexible-master-table]: /Users/eugenetsenter/.codex/sessions/2026/06/16/rollout-2026-06-16T13-22-25-019ed174-adb3-7001-93cb-3fb1add59980.jsonl
[session-dcm-cost-model-map]: /Users/eugenetsenter/.codex/sessions/2026/06/16/rollout-2026-06-16T13-58-30-019ed195-b67a-7d42-8fdb-50738d95734f.jsonl
[session-redesign-report]: /Users/eugenetsenter/.codex/sessions/2026/06/16/rollout-2026-06-16T14-26-49-019ed1af-a24f-7992-8b7c-c4272fef4124.jsonl
[session-amazon-ads-reporting]: /Users/eugenetsenter/.codex/sessions/2026/06/08/rollout-2026-06-08T15-51-10-019ea8c9-fb8a-7a60-aa11-28c4eca53a6f.jsonl
