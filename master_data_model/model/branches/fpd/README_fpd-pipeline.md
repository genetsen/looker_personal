---
pipeline: First-Party Data (FPD)
source_type: delivery — partner-reported
output: looker-studio-pro-452620.master_stg.data_model_v3
output_grain: fpd_original — package_date_placement_factor_creative; fpd_updated_package — package_date
source_tables:
  - looker-studio-pro-452620.landing.fpd_data_ranged_shortcutsFolder
  - looker-studio-pro-452620.landing.adif_updated_fpd_daily
loader_script: util_collect_fpd_shortcutsFolder.r (original FPD only — see Known Gaps for updated FPD)
refresh: manual R loader run, then the shared master-model rebuild (no scheduled transfer for FPD itself)
verified: 2026-09-14
verified_against:
  - model/stable_base/create_master_stg_data_model.sql
  - model/final_model/create_master_stg_data_model_v3.sql
  - model/branches/fpd/CURRENT_STATE.md
  - model/branches/fpd/fpd-publish-button-fix-design.md
  - model/branches/fpd/partner-data-collection-template.md
  - model/branches/fpd/polaris/README_polaris-email-pipeline.md
  - FPD/FPD_loader/README.md and util_collect_fpd_shortcutsFolder.r
  - master_data_model/CHANGELOG.md (fpd-related entries)
  - live query: master_stg.data_model_v3, landing.fpd_data_ranged_shortcutsFolder, landing.adif_updated_fpd_daily
reviewers:
  - gene <gene.tsenter@giantspoon.com>
---

# First-Party Data (FPD) Pipeline

Partner-reported delivery — spend, impressions, clicks, and (for original FPD only)
creative/placement detail — collected through a partner-facing Google Sheet, then
loaded into the master data model as two independent evidence families, `fpd_*`
and (inside its own coverage window) `polaris_*`.

**FPD is not one pipeline; it is two landing tables plus a request-and-publish
workflow that feeds one of them.** "Original FPD" carries creative-level detail;
"updated FPD" carries only spend and impressions at package/date grain.

| To do this | Use |
|---|---|
| Isolate original FPD rows | `qa_v3_source_detail_type = 'fpd_original'` |
| Isolate updated FPD rows | `qa_v3_source_detail_type = 'fpd_updated_package'` |
| Read FPD's own values | `fpd_*` fields — unchanged evidence, never summed as final |
| Read reporting values | `_spend`, `_impressions`, `_clicks`, `_video_views`, `_video_comps` |
| Request data from a partner | [Partner Data Collection Template guide](/Users/eugenetsenter/Looker_clonedRepo/looker_personal/master_data_model/model/branches/fpd/partner-data-collection-template.md) |
| MIQ Meta/TikTok delivery specifically | [Polaris Email pipeline](polaris/README_polaris-email-pipeline.md) — not this guide |

## Table of Contents

- [Canonical Files and Protected Surfaces](#canonical-files-and-protected-surfaces)
- [Pipeline Overview](#pipeline-overview)
- [Source Inventory](#source-inventory)
- [Join, Grain, and Precedence](#join-grain-and-precedence)
- [Partner Request-to-Publish Path](#partner-request-to-publish-path)
- [Known Gaps and Open Items](#known-gaps-and-open-items)
- [Operational Commands](#operational-commands)
- [Verify Current State Yourself](#verify-current-state-yourself)
- [Definitions](#definitions)
- [Related Guides](#related-guides)

---

## Canonical Files and Protected Surfaces

| Purpose | File |
|---|---|
| Original-FPD loader (canonical) | [util_collect_fpd_shortcutsFolder.r](/Users/eugenetsenter/Looker_clonedRepo/looker_personal/FPD/FPD_loader/util_collect_fpd_shortcutsFolder.r) |
| Manual-correction loader (separate table, not read by V3 — see below) | [manually_updated_data_loader.r](/Users/eugenetsenter/Looker_clonedRepo/looker_personal/FPD/FPD_loader/manually_updated_data_loader.r) |
| Package/date base | [create_master_stg_data_model.sql](/Users/eugenetsenter/Looker_clonedRepo/looker_personal/master_data_model/model/stable_base/create_master_stg_data_model.sql) |
| Final model | [create_master_stg_data_model_v3.sql](/Users/eugenetsenter/Looker_clonedRepo/looker_personal/master_data_model/model/final_model/create_master_stg_data_model_v3.sql) |
| Publish-button library (live, version 11) | [Version 11 library](/Users/eugenetsenter/Looker_clonedRepo/looker_personal/master_data_model/model/branches/fpd/apps_script/publish_fpd_template_fpdLib__v11/Library.gs) |
| Publish-button bound wrapper | [Stub.gs](/Users/eugenetsenter/Looker_clonedRepo/looker_personal/master_data_model/model/branches/fpd/apps_script/current_workbook_bound_stub/Stub.gs) |

`util_collect_fpd_v2.r` / `util_collect_fpd_v3.r` under `util/data_loaders/FPD_loader/`
and the older `adif/` project copies are **not the primary entrypoint** — the
`FPD/FPD_loader/README.md` itself says so. `apps_script/publish_fpd_template__OLD_acct/`
and `apps_script/publish_fpd_template_fpdLib__v3/` are historical, pre-migration
library versions; do not wire a button to either. Authority order for any
disagreement: **live `data_model_v3` (production) → `CURRENT_STATE.md` (for the
publish workflow) → the SQL and R below.**

| Surface | Rule |
|---|---|
| `landing.fpd_data_ranged_shortcutsFolder`, `landing.adif_updated_fpd_daily` | Read-only inputs to the master model. Rebuilt by their own loaders; a direct write is overwritten at the next load. |
| `master_stg.data_model` | **A view, not a table.** Editing it is a schema change for every consumer. |
| Central template workbook's `Log`, `ClientMapping`, `CustomFolderPrefs` tabs (in [`1BYq…`](https://docs.google.com/spreadsheets/d/1BYqrQrjL4_rf5-LKTlGkR94CkqSW6QOsYzAPLHxucfY/edit)) | This one workbook is both the template operators duplicate *and* the central mappings/log hub the library reads. Every duplicated partner copy carries its own unused copies of these tabs — never treat a dupe's copy as authoritative. |
| Central `IMPORTRANGE` source workbooks (`1T4PCz…`, `1qi2bDaV…`) | Read-only, and **not partner-safe today** — see the confidentiality gap under [Known Gaps](#known-gaps-and-open-items). Do not share a published sheet externally until that gap is closed. |
| Apps Script editor | Never run `duplicateAndSetup` directly from the editor — it needs the active spreadsheet and interactive UI. Call only through the bound `Stub.gs` menu/button. |
| The master template file | Never edit or publish from it directly; it is intentionally named "Dupe before using." |

---

## Pipeline Overview

```text
Giant Spoon Config scope (client / partner / campaign / channel)
       │
       ▼
Partner Data Collection Template (Google Sheet, duplicated per partner)
       │  partner enters spend / impressions / clicks / ... on `data`
       ▼
Publish button — FPDLib v11 via bound Stub.gs
       │  duplicates a locked copy, routes it into the client's Shared Drive
       │  "First Party Data" folder, creates an ingestion shortcut,
       │  logs the run to the central template's `Log` tab
       ▼
Canonical shortcut folder — Analytics "First_Party_Data"
       │
       ▼
util_collect_fpd_shortcutsFolder.r  (discovers shortcuts, normalizes
       │   columns, expands date ranges to daily rows, uploads)
       ▼
landing.fpd_data_ranged_shortcutsFolder  ("original FPD")

landing.adif_updated_fpd_daily  ("updated FPD" — separate feed;
       entrypoint unresolved, see Known Gaps)

MIQ email reports → Polaris loader → landing.polaris_email_delivery_daily
       (own pipeline — see the Polaris guide)
       │
       ▼  (all three land here together)
Package/date base (create_master_stg_data_model.sql)
       — keeps fpd_*, polaris_* as separate evidence; derives each
         package's loaded Polaris min/max date to choose final fields
       ▼
Final model (create_master_stg_data_model_v3.sql)
       — source-detail rows: fpd_original, fpd_updated_package,
         polaris_email, dcm, ...
       ▼
master_stg.data_model_v3
```

---

## Source Inventory

| Source | Grain | Supplies | Boundary |
|---|---|---|---|
| [Partner Data Collection Template](/Users/eugenetsenter/Looker_clonedRepo/looker_personal/master_data_model/model/branches/fpd/partner-data-collection-template.md) | Partner request/entry row | Requested package context, date grain, dimensions, and partner-entered raw metrics | A request builder, never the model's BigQuery source. |
| [`landing.fpd_data_ranged_shortcutsFolder`](https://console.cloud.google.com/bigquery?project=looker-studio-pro-452620&p=looker-studio-pro-452620&d=landing&t=fpd_data_ranged_shortcutsFolder&page=table) ("original FPD") | One source row per day after date-range expansion | Spend, impressions, clicks, sends, opens, benchmark, factor, creative, source-file lineage | Preserves placement/factor/creative detail through to V3; the only FPD source with creative detail. |
| [`landing.adif_updated_fpd_daily`](https://console.cloud.google.com/bigquery?project=looker-studio-pro-452620&p=looker-studio-pro-452620&d=landing&t=adif_updated_fpd_daily&page=table) ("updated FPD") | Package/date | Daily spend and impressions, supplier, initiative, source-sheet freshness | No clicks or creative detail. Loader currently unresolved — see [Known Gaps](#known-gaps-and-open-items). |
| [`landing.manually_updated_fpd_daily`](https://console.cloud.google.com/bigquery?project=looker-studio-pro-452620&p=looker-studio-pro-452620&d=landing&t=manually_updated_fpd_daily&page=table) | Package/date | Manually corrected FPD figures from a single sheet | Written by `manually_updated_data_loader.r`. **Not read by `create_master_stg_data_model_v3.sql`** — do not assume this is "updated FPD"; it is a separate table with a similar name. |
| [Polaris Email delivery pipeline](polaris/README_polaris-email-pipeline.md) | Package/date/platform/campaign/ad group/ad | Normalized MIQ Meta/TikTok delivery | Owned entirely by its own guide; wins over FPD's final fields inside its own loaded package/date coverage. |
| [Polaris Email package mapping](https://console.cloud.google.com/bigquery?project=looker-studio-pro-452620&p=looker-studio-pro-452620&d=landing&t=polaris_email_package_mapping&page=table) | Feed/platform/campaign/ad group | Assigns exactly one Prisma package to a Polaris row | Identity only; never supplies metrics. |

---

## Join, Grain, and Precedence[^1]

Both FPD sources join the model on `package_id` + `date`, filtered to
`date >= 2025-01-01` and a non-null `package_id`. Detail grain differs by
`qa_v3_source_detail_type`, confirmed live 2026-09-14:

| `qa_v3_source_detail_type` | `qa_v3_metric_grain` | Live rows | Rows with a final metric |
|---|---|---:|---:|
| `fpd_original` | `package_date_placement_factor_creative` | 22,600 | 22,183 |
| `fpd_updated_package` | `package_date` | 2,162 | 2,162 |

Original FPD's detail grain is `package_id, date, placement (coalesced from
partner placement/package-placement name), factor, creative name, creative
Git link` — rows sharing all six values are summed together. This is a
genuine many-to-one collapse from the raw landing table (see the
[reconciliation query](#verify-current-state-yourself)), not a bug.

**Which field to read.** `_spend`, `_impressions`, `_clicks`, `_video_views`,
and `_video_comps` are the only fields to report from directly. `fpd_*`
fields are unchanged partner evidence for audit and reconciliation — never
sum them as if they were final. `qa_v3_source_detail_type` (and, on the
output row, `qa_row_data_source_primary`) tells you which source produced
the final value on that row.

**Precedence for final fields**, from `create_master_stg_data_model_v3.sql`:

| Final field | Winning order |
|---|---|
| `_spend`, `_impressions` | Polaris (inside its loaded coverage) → FPD (original + updated combined) → DCM. DCM's own spend/impressions are set to NULL on a row where FPD already supplies a nonzero value. |
| `_clicks` | Polaris → original FPD → DCM. Updated FPD never supplies clicks. |
| `_video_views`, `_video_comps` | Polaris → original FPD → DCM. Updated FPD never supplies video. |

A package/date can carry **both** DCM and FPD evidence at once — this is
tagged `qa_data_issues = 'actual_source_conflict'` (2,204 live `fpd_original`
rows as of 2026-09-14) and is evidence for review, not permission to add the
two together. A package/date can also carry both FPD and Polaris evidence at
the same time; that is intentional — compare the prefixed fields and read
only the underscore-prefixed final fields for reporting. See the
[Polaris guide's Output Contract](polaris/README_polaris-email-pipeline.md#output-contract)
for exactly how Polaris coverage suppresses FPD's final metrics without
touching the underlying `fpd_*` evidence.

---

## Partner Request-to-Publish Path

This is the workflow the "Publish button" Asana work refers to. Read
[CURRENT_STATE.md](/Users/eugenetsenter/Looker_clonedRepo/looker_personal/master_data_model/model/branches/fpd/CURRENT_STATE.md)
before changing any part of it — it is the shared Codex/Claude handoff for
exactly this surface.

**Entrypoint today:** the in-sheet menu **Publish New FPD Sheet ▸ Publish**
(bound by [Stub.gs](/Users/eugenetsenter/Looker_clonedRepo/looker_personal/master_data_model/model/branches/fpd/apps_script/current_workbook_bound_stub/Stub.gs))
calls `publishFpdTemplate()`, which passes the active spreadsheet into
`FPDLib.duplicateAndSetup(ss)` on **library version 11** — the live,
user-verified version as of 2026-08-14. It duplicates the workbook, hides
every tab except `data`, routes the copy into the client's Shared Drive
"First Party Data" folder (Shared-Drive membership controls access, not
`setOwner`), creates a Drive shortcut in the canonical
[Analytics `First_Party_Data`](https://drive.google.com/drive/folders/1pqQVdROIhOkfuBLwexH00uW4eiqkb0GY)
folder, and logs the run.

**Why "v4" appears in the repo but the live version is 11:**
[fpd-publish-button-fix-design.md](/Users/eugenetsenter/Looker_clonedRepo/looker_personal/master_data_model/model/branches/fpd/fpd-publish-button-fix-design.md)
designed version 4 (Shared-Drive routing, tab locking, source-dupe archiving)
as the fix for the account-migration break. The library shipped past that
design to version 11 (adding unpublished-warning-image handling and the
shortcut-folder action), but the repo only keeps folders for **v3** and
**v11** — the intermediate v4–v10 iterations are not preserved here, so which
of the v4 design's specific functions (`archiveSourceDupe_`,
`lockPublishedSheet_`, the `CLIENT_FPD_FOLDERS` map) survived into v11 cannot
be confirmed from this repo alone. Confirm against the live Apps Script
project before relying on any of them by name.

---

## Known Gaps and Open Items

Grounded in dated repo evidence; verify before treating any of these as
resolved.

- **Per-partner BigQuery isolation is not built — BLOCKER on external sharing.**
  Per [fpd-publish-button-fix-design.md §10](/Users/eugenetsenter/Looker_clonedRepo/looker_personal/master_data_model/model/branches/fpd/fpd-publish-button-fix-design.md),
  the template's ~1,343 live `IMPORTRANGE` cells render with the *owner's*
  access, so any editor of a published sheet can pull another client or
  partner's rows by typing a new `IMPORTRANGE` formula — hiding tabs and
  protecting ranges do not stop a formula from *reading*. The approved fix
  (a partner-scoped BigQuery view via Connected Sheets, confirmed feasible
  against `Prisma.prisma__stg__digital_plus_linear_view`) is "a design sketch,
  not an implementation." The `master_data_model` CHANGELOG's most recent
  FPD-tagged "Pending Next Actions" (2026-08-14) still lists this as a
  BLOCKER. **Do not share a published partner sheet outside Giant Spoon until
  it ships.**
- **The entrypoint for `landing.adif_updated_fpd_daily` ("updated FPD") is
  unresolved.** The active `FPD/FPD_loader/README.md` states its own
  `util_collect_fpd_v2.r`/`v3.r` are "not the primary entrypoint," yet an
  older `util/data_loaders/FPD_loader/CLAUDE.md` describes exactly
  `util_collect_fpd_v3.r` sourcing `util_process_updated_fpd.r` (in
  `/looker_personal/adif/`) to populate this table. No file found during this
  review confirms which script currently, actually refreshes
  `adif_updated_fpd_daily`, or whether it still runs on any schedule. Treat
  this table's freshness as unverified until an owner confirms the live
  pipeline.
- **Remaining clients are not yet mapped to a Shared Drive `First Party Data`
  folder** (Pending Next Actions, 2026-08-14) — those clients still fall back
  to the legacy My-Drive prompt in the publish flow.
- **One Package Lookup menu result was still awaiting a final confirmation
  against the live warehouse** as of the 2026-08-14 Pending Next Actions
  entry (open since 2026-07-14).
- **The central source workbook's Google Workspace migration** was an open
  BLOCKER as of the 2026-08-12 CHANGELOG entry but no longer appears in the
  2026-08-14 Pending Next Actions list — likely resolved, but no entry
  explicitly confirms it; verify live before assuming it is done.
- **The partner template's own caveats are tracked in its own guide, not
  repeated here**: the typed-table dropdown blocking a dynamic mapping list,
  duplicate short package names resolving to the wrong package, a `#REF!`
  formula in `data!L2` of unconfirmed purpose, and a 500-row cap on
  alias-detection formulas. See
  [partner-data-collection-template.md § Known Caveats](/Users/eugenetsenter/Looker_clonedRepo/looker_personal/master_data_model/model/branches/fpd/partner-data-collection-template.md#known-caveats-found-during-review).
- **`missing_prisma_daily`** is the dominant `qa_data_issues` value on
  `fpd_original` rows (11,086 of 22,600 live rows, 2026-09-14) — expected
  where FPD delivery exists for a package/date with no matching Prisma plan
  row, not itself a loader defect, but not investigated further in this pass.

---

## Operational Commands

Run the loader from `/Users/eugenetsenter/Looker_clonedRepo/looker_personal/FPD/FPD_loader`:

```bash
# Full original-FPD collection (7 phases + BigQuery upload to
# landing.fpd_data_ranged_shortcutsFolder)
Rscript util_collect_fpd_shortcutsFolder.r

# Scope to one client's sheets only
Rscript util_collect_fpd_shortcutsFolder.r --pattern="APO | Partner Data"
```

The manual-correction loader writes a **different** table
(`landing.manually_updated_fpd_daily`) that is not read by V3 today — run it
only when correcting that specific dataset, not as an "updated FPD" refresh:

```bash
Rscript manually_updated_data_loader.r
```

Rebuild the final model after either FPD table changes (same command used by
every other branch in this model — see the
[Master Data Model Pipeline v2](/Users/eugenetsenter/Looker_clonedRepo/looker_personal/master_data_model/README_v2.md)):

```bash
bq query --project_id=looker-studio-pro-452620 --use_legacy_sql=false < /Users/eugenetsenter/Looker_clonedRepo/looker_personal/master_data_model/model/final_model/create_master_stg_data_model_v3.sql
```

For MIQ Meta/TikTok delivery through Polaris, use that pipeline's own
[Run and Prove](polaris/README_polaris-email-pipeline.md#run-and-prove)
section — it is not duplicated here.

For the publish button, there is no CLI entrypoint: an authorized operator
runs **Publish New FPD Sheet ▸ Publish** from the template workbook itself,
authorizing Drive access on first use.

Loader-level regression tests (proof for code changes to the original-FPD
loader, not for a production run) live in
[FPD/FPD_loader/tests/](/Users/eugenetsenter/Looker_clonedRepo/looker_personal/FPD/FPD_loader/tests/):
`test_archive_source_cleanup_scope.R`, `test_drop_blank_generated_columns.R`,
`test_fpd_bigquery_safety.R`, and `test_partner_placement_name_character.R`.

---

## Verify Current State Yourself

```sql
-- Coverage and grain by FPD source-detail type
-- Expected 2026-09-14: fpd_original ~22,600 rows (grain
-- package_date_placement_factor_creative); fpd_updated_package ~2,162 rows
-- (grain package_date). Investigate only on a sharp, unexplained change.
SELECT qa_v3_source_detail_type, qa_v3_metric_grain,
       COUNT(*) AS rows_,
       COUNTIF(_spend IS NOT NULL OR _impressions IS NOT NULL) AS with_final_metrics,
       MIN(_date) AS min_date, MAX(_date) AS max_date
FROM `looker-studio-pro-452620.master_stg.data_model_v3`
WHERE qa_v3_source_detail_type IN ('fpd_original', 'fpd_updated_package')
GROUP BY 1, 2 ORDER BY rows_ DESC;
```

```sql
-- QA callout mix on FPD rows
-- Expected 2026-09-14: missing_prisma_daily dominates fpd_original (~11,086);
-- actual_source_conflict (~2,204) means DCM and FPD both evidenced the same
-- package/date -- read this as a flag for review, not a defect.
SELECT qa_v3_source_detail_type, qa_data_issues, COUNT(*) AS rows_
FROM `looker-studio-pro-452620.master_stg.data_model_v3`
WHERE qa_v3_source_detail_type IN ('fpd_original', 'fpd_updated_package')
GROUP BY 1, 2 ORDER BY rows_ DESC;
```

```sql
-- Landing-to-model reconciliation for original FPD
-- Expected: passes_filters is total_rows minus rows with a null package_id
-- or a date before 2025-01-01. The model's fpd_original row count will be
-- lower still -- that is the placement/factor/creative grouping described
-- above, not lost data. Investigate only if passes_filters itself drops
-- sharply between runs.
SELECT
  COUNT(*) AS total_rows,
  COUNTIF(package_id IS NULL) AS null_package_id,
  COUNTIF(DATE(date_final) < DATE '2025-01-01') AS before_2025,
  COUNTIF(package_id IS NOT NULL AND DATE(date_final) >= DATE '2025-01-01') AS passes_filters,
  MAX(DATE(date_final)) AS max_date
FROM `looker-studio-pro-452620.landing.fpd_data_ranged_shortcutsFolder`;
```

None of a dry-run script, a passing test suite alone, or a populated schema
proves a production load reached `data_model_v3` — run the queries above
against the live table after any refresh.

---

## Definitions

[^1]: **Grain:** what a single row represents. Original FPD's natural grain is finer (package/date/placement/factor/creative) than updated FPD's (package/date); mixing the two, or summing a `fpd_*` evidence field as if it were a final reporting field, is the most common cause of a double-counted FPD total.

---

## Related Guides

- [MIQ Polaris Email delivery pipeline](polaris/README_polaris-email-pipeline.md)
- [Polaris Email V3 MVP plan](polaris-email-v3-mvp-plan.md)
- [Partner Data Collection Template guide](partner-data-collection-template.md)
- [FPD Publisher and Shortcut Current State](CURRENT_STATE.md)
- [FPD Publish Button — Understanding & Fix Design](fpd-publish-button-fix-design.md)
- [DCM delivery pipeline](/Users/eugenetsenter/Looker_clonedRepo/looker_personal/master_data_model/model/branches/dcm/README_dcm-pipeline.md)
- [Prisma planning pipeline](/Users/eugenetsenter/Looker_clonedRepo/looker_personal/master_data_model/model/branches/prisma/README_prisma-pipeline.md)
- [Master Data Model Pipeline v2](/Users/eugenetsenter/Looker_clonedRepo/looker_personal/master_data_model/README_v2.md)
