# Changelog

## 2026-07-08

- **CHANGED** - Documented the FPD partner template mapping-table correction so duplicate package labels and partner aliases resolve through a dynamic stable-label block instead of the old typed-table dropdown.

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
