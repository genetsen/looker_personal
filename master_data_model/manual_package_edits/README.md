# Manual Package Edits

This folder owns the manual package editor for the master data model. The goal is simple: let a media buyer find a package, edit the dashboard value they need to correct, and let the loader turn that edit into auditable backend `man_*` fields.

For deep QA, troubleshooting, scripts, tables, filters, and package-trace queries, use `QA_RUNBOOK.md`.

For the day-to-day two-tab workflow, use [Manual Data Editor Two-Tab Operating Note](</Users/eugenetsenter/Looker_clonedRepo/looker_personal/master_data_model/manual_package_edits/TWO_TAB_OPERATING_NOTE.md>).

For the safety contract behind the split input/preview design, use [Manual Data Editor Design Contract](</Users/eugenetsenter/Looker_clonedRepo/looker_personal/master_data_model/manual_package_edits/DESIGN_CONTRACT.md>).

## Sheet Workflow

Use the `Manual Edits` tab for user-entered corrections. Use the `Package Editor` tab as the refreshed package lookup/preview.

1. Use `Package Editor` to find the package row by Advertiser, Channel, Campaign, Site, `Package ID`, or `Package Friendly Name`.
2. Find the package row by `Package ID`, `Site`, and `Package Friendly Name`.
3. Enter the correction in `Manual Edits`, not in the refreshed preview tab.
   - Orange means the value differs from the current dashboard snapshot.
   - Purple means the value is already using a validated manual update.
   - Red means a specific cell in a started new row needs to be fixed before it can load.
   - Package-level fields: `Flight Start Date`, `Flight End Date`, and visible metadata such as `GS Channel`, `Campaign`, `Package Name`, `Package Type`, `Benchmark KPI`, and `Benchmark Value`.
   - Metric date window: `Delivery Override Start Date` and `Delivery Override End Date`.
   - Delivered actual metrics: `Spend`, `Impressions`, `Clicks`, `Video Plays`, `Video Completions`
   - Planned flight totals: `Planned Spend`, `Planned Impressions`
4. Run the loader.

The loader reads user-owned rows from `Manual Edits`, compares those rows to source-derived baselines and the last loader run, then writes active valid rows into the manual landing tables. The loader may rebuild `Package Editor`, but a preview rebuild is not treated as evidence that a user deleted an edit. Planned package totals come directly from PRISMA package totals. Delivered metric baselines are recalculated from raw delivery fields, not from manual-affected final `_` fields.

If users need a refresh but do not run the loader themselves, they can use the `Request refresh` checkbox-style control at the top of the sheet. It sends Gene an email. If the bound Apps Script has a `MANUAL_EDITOR_SLACK_WEBHOOK_URL` script property, it also posts the same request to Slack. This checkbox is only a notification; it does not validate rows, run the loader, or write to the warehouse by itself.

Started new rows highlight the specific missing required cells or invalid date cells. Planned cells turn red only when a planned edit uses a partial date range instead of the full visible flight dates. Values that are already backed by validated manual updates turn purple. The loader also blocks invalid rows backend-side, so red frontend feedback is a warning to fix the row before requesting a refresh.

Filtering uses the standard Google Sheets column header filter controls. They are intentionally not powered by Apps Script, because users need browsing to feel responsive while editing the real source rows.

The `Instructions` tab is the user-facing quick guide. It explains the normal edit flow, date-range rules, new-row requirements, color meanings, and what not to edit.

Existing package identity fields, table headers, request-helper text, visible status/context fields, and hidden baseline comparison fields are protected in the preview sheet. The intended editable fields live in `Manual Edits`: visible flight dates, delivery override dates, metric values, and metadata correction columns. Blank rows in `Manual Edits` remain available for manual-only package rows, including the required identity and metadata fields.

The editor includes audit columns for `Manually Edited?`, `Manual Edit At`, `Manual Edit By`, and `Manual Edit Published At`. `Manual Edit At` and `Manual Edit By` are stamped by the bound Apps Script when a user edits an editable package row. Google may hide the editor email in some trigger/security contexts, so `Manual Edit By` can show an unidentified-editor message instead of an email. `Manual Edit Published At` is set by the loader after a valid manual edit is accepted into BigQuery.

The request checkbox requires the bound Apps Script's installable edit trigger. If the checkbox does not send, use `Manual Editor` -> `Authorize request button` once from the sheet menu, then try again.

## Displayed Data Sources

The editor displays a current dashboard snapshot from the combined reporting model.

- Planned values come from media planning, buying, trafficking, and package setup sources.
- Delivered actuals come from ad server, platform, partner, and delivery reporting sources.
- Package context comes from package setup and reporting metadata sources.
- User-facing instructions should describe sources this way:
  - Planned Values: Planned spend, planned impressions, rates, and flight date information come from PRISMA.
  - Delivered values: Delivered spend, impressions, clicks, video plays, and video completions come from ad server, platform, or partner-specific First Party Data sheets.
  - Package metadata: Advertiser, campaign, channel, supplier, site, package name, initiative, and classification fields come from PRISMA.
  - Benchmark values: Benchmark KPI is editable text. Benchmark Value is editable numeric data and falls back to the FPD benchmark when no manual value is active.
- Prior valid manual corrections are kept as user-owned manual override evidence and used before normal source values.
- When the normal source later matches a manual correction, the loader preserves the manual evidence and marker instead of clearing it automatically. A script can block invalid input, but it should not delete a user's edit or draft value unless the user explicitly clears or deletes it.

The sheet should use general business labels for users. Do not expose warehouse table names in the user-facing instructions.

## Data Model Overview

The data model combines planning, delivery, and package metadata into one package/date reporting layer. Dashboards and partner reporting use that combined layer so package fields, dates, planned values, delivered values, and manual evidence stay aligned.

When the loader runs, it compares edited Sheet values to the current dashboard snapshot. Valid edits are written as backend `man_*` fields and take priority in the final reporting fields. Benchmark edits write to `man_benchmark_kpi` and `man_benchmark_value`, then feed final `_benchmark_kpi` and `_benchmark_value` fields. Edits are reflected in client dashboards and external partner reporting after the loader and downstream reporting refresh finish.

## Date-Range Rules

Flight dates and delivery override dates do different jobs.

- `Flight Start Date` and `Flight End Date` are package-level fields. A manual correction to those fields applies to the whole package.
- `Delivery Override Start Date` and `Delivery Override End Date` are the metric override window.
- To correct the full package flight, keep the delivery override dates on the full package flight and enter the replacement total.
- To correct one week of delivered data, add or duplicate a row, set the delivery override dates to that week, and enter the replacement delivered totals for that week only.
- Dates outside the edited delivery override range keep the normal dashboard metric values.
- Duplicate active edits for the same package, date, and metric are blocked.

## Metadata Rules

Metadata corrections are package-level.

- Editing `Advertiser`, `Package Type`, `Channel`, `Campaign`, `Initiative`, `Supplier Code`, `Supplier Name`, `Package Name`, `Package Friendly Name`, `GS Channel`, `Benchmark KPI`, or `Benchmark Value` creates backend `man_*` metadata evidence.
- `Primary Row Data Source`, `Validation Status`, and `Validation Reason` are read-only context fields. They explain where the package row is coming from and whether the loader will publish or block the row.
- Metadata overrides do not need a metric edit to become active.
- Metadata overrides apply to all rows for the package, including delivery dates outside a metric override window.
- Metric overrides stay date-bound to the delivery override dates.

## Planned Metric Rules

Planned metrics are flight-level only.

- `Planned Spend` and `Planned Impressions` must be edited only when the row covers the full flight.
- Partial-week or day-level planned edits are blocked by the loader.
- In the sheet, planned cells should not stay red when `Delivery Override Start Date` and `Delivery Override End Date` match the visible `Flight Start Date` and `Flight End Date`.
- The sheet displays planned values as full-flight totals, not daily prorated values.
- Planned package totals are compared against PRISMA package totals, not filtered mart row sums, so low-signal row filtering cannot create false manual planned markers.
- The final model keeps package-level planned totals aligned with the manual replacement total.
- The model still stores daily planned values in `_planned_spend` and `_planned_impressions` so dashboard sums work correctly.

## Undo And Corrections

- To undo a manual correction, clear the visible edited cell or set it back to a different displayed source/baseline value and rerun the loader.
- If an edited value needs another correction, overwrite the same visible cell with the new intended value and rerun the loader.
- If source delivery or PRISMA later catches up to a manual value, the loader keeps the `man_*` value as user-owned evidence until an explicit undo or row deletion.
- If a prior loader run marked a row as blocked but there is no manual-edit evidence, the next run treats that prior replacement as stale display state instead of preserving it as a real manual correction.
- If a blocked manual-only draft has no live source baseline to fall back to, the loader preserves the entered metric, date, and metadata values so the row can be fixed instead of erased.
- Blank metric cells mean no manual override for that metric; they revert to the current source baseline rather than zero.

## Delivered Metric Rules

Delivered actual metrics can be edited for any valid date range.

- For one-day edits, set `Delivery Override Start Date` and `Delivery Override End Date` to the same date.
- For one-week edits, set the dates to that week and enter weekly replacement totals.
- The loader spreads replacement totals across the selected dates while preserving the exact total after upload. Count metrics allocate whole units across days; spend metrics preserve decimal precision from the source/editor value.
- The daily proof step blocks the upload if daily rows do not sum back to the replacement total beyond a one-cent tolerance.

## New Manual-Only Packages

Manual-only packages can enter the model only when required metadata is complete.

Add a new row only when the package does not already exist in the editor, or when you need a separate date range for delivered actuals, such as one specific week.

Visible required fields:

- `Package ID`
- `Site`
- `Package Friendly Name`
- `Flight Start Date`
- `Flight End Date`
- `Delivery Override Start Date`
- `Delivery Override End Date`
- At least one metric value

Required metadata fields at the far right of the editor:

- `Advertiser`
- `Package Type`
- `Channel`
- `Campaign`
- `Package Name`
- `GS Channel`

Optional benchmark fields at the far right of the editor:

- `Benchmark KPI`
- `Benchmark Value`

Those metadata fields are visible because brand-new packages need them for grouping, filtering, and reporting. They are also editable for package-level metadata corrections on existing packages. The backend still fills redundant internal fields from these visible values where needed.

New-row steps:

1. Insert a new row below the existing package list, or duplicate a similar fee/package row and replace the values.
2. Fill `Package ID`, `Site`, `Package Friendly Name`, flight dates, delivery override dates, and at least one metric value.
3. Fill the required metadata fields at the far right.
4. Keep planned metrics on the full-flight date range; use partial ranges only for delivered actuals.
5. Check `Request refresh` when the row is complete.

Common blockers: missing `Package ID`, invalid dates, no changed metric/date value, duplicate active package/date/metric edits, or missing required metadata for a new package.

## Scripts

- `create_manual_package_editor_package_lookup.sql` refreshes the package-level lookup table that the loader downloads. It builds the lookup from source-backed rows in the materialized master support table, excludes rows already produced by manual package edits, applies the same reporting-only low-signal DCM and excluded-campaign removals, joins PRISMA package totals, and carries current benchmark KPI/value baselines for the editable benchmark columns.
- For social rows that do not expose true flight dates, the lookup leaves the editor flight dates blank. If a user fills those blank cells, the loader treats the entered dates as manual flight-date changes.
- `load_manual_package_edits.R` in this folder is a launcher for the canonical loader in [model/manual_editor](</Users/eugenetsenter/Looker_clonedRepo/looker_personal/master_data_model/model/manual_editor/load_manual_package_edits.R>). The canonical loader refreshes the package-level lookup table, reads user-owned corrections from `Manual Edits`, seeds that tab from trusted prior raw evidence if it is missing or empty, validates rows, writes raw and daily manual tables, rewrites the `Package Editor` preview values, and refreshes column header filter ranges plus narrow column visibility/protection settings after header shifts. When called from the universal script runner, it resolves sibling helper scripts from the canonical folder and checks `MASTER_MANUAL_EDIT_NODE`, `PATH`, `/usr/local/bin/node`, `/opt/homebrew/bin/node`, and `/usr/bin/node` for Node.
- The loader refuses to publish if a manual-only package row would be rewritten as inactive and blank. It also preserves prior manual-only metric, date, and metadata values when there is no live baseline to fall back to. Restore or intentionally delete that draft row before rerunning, because manual-only rows have no live baseline that can safely recreate the user's values.
- The loader reads editor cells as text, writes editor dates back as ISO text, and accepts both Google Sheets date serials and R date serials for modern campaign dates, so a refreshed sheet value cannot turn a 2026 flight date into a 1950s date or blank date on the next run.
- For manual-only rows, a valid delivery/manual date range can fill missing package flight dates instead of blocking the row and erasing the publishable manual values.
- Valid user-owned rows are eligible for daily publication when they contain metric, metadata, or flight-date manual evidence. A row is not dropped from daily publication just because the manual evidence is not in a metric replacement column.
- Stale source rows that are visibly marked `No`, `inactive`, and `not edited` do not block a refresh just because they disappeared from the current lookup; real manual-only drafts and rows with manual evidence still block instead of being erased.
- Legacy-tab cleanup and tab-relocation checks run after the raw/daily writes. If Google throttles those metadata calls, the loader warns and keeps the completed data refresh result instead of failing the whole run after the important writes have already succeeded.
- The production Manual Data Editor sheet is the loader default. Use `MASTER_MANUAL_EDIT_SHEET_ID=...` only when intentionally testing another sheet copy.
- The default script-auth account is `gene.tsenter@giantspoon.com`. If a refresh fails after BigQuery upload with a Sheet permission error, grant that account access to the production Sheet and confirm its `gcloud` token includes Drive/Sheets OAuth scope before rerunning the Sheet repair helper.
- `setup_manual_package_editor_sheet.mjs` is a rebuild tool for Google Sheet formatting, the `Instructions` tab, visible metadata columns, hidden internal baseline/manual-marker columns, notes, warnings, widths, and colors. Do not run it against the live sheet after user-made manual formatting edits unless the user explicitly asks for a full formatting rebuild. The script is guarded and now requires `MASTER_MANUAL_EDIT_ALLOW_FORMAT_REBUILD=YES` to run.
- `repair_manual_package_editor_filters.mjs` is a narrow sheet-geometry repair tool. It removes slicers, expands the standard column header filter range to the full current sheet grid, keeps user-facing audit/status columns visible, and keeps backend baseline/marker/helper columns hidden and protected.
- `apps_script/Code.js` is the bound Apps Script for the update-request notification control and row-level manual-edit audit stamps. Filtering should stay native through Google Sheets column header filters.
- The universal script runner entrypoint is `/Users/eugenetsenter/Docs/R_Studio_Projects/universal_cron_runner/automation_hub/workloads/ops/master_manual_package_edits/load_master_manual_package_edits.R`. It launches this loader, and the loader refreshes the lookup table before reading package rows.

R should stay focused on data loading. The current live sheet formatting is the source of truth once users have made manual formatting edits. Before future formatting work, take a read-only formatting snapshot and preserve user-made changes unless a full rebuild is explicitly requested.

## Verification Checklist

Before calling the path ready:

1. Run the loader and confirm it completes with daily proof status `passed`.
2. Run the loader again and confirm the `Package Editor` formatting still holds after the loader writes values. Do not run the setup script unless the user explicitly requested a full formatting rebuild.
3. Test a fee package delivered metric edit on a short date range.
4. Test that a started but incomplete new row is blocked and visibly marked red.
5. Test that a complete temporary fee/manual-only row enters the raw table, daily table, final model, and mart with the exact replacement totals.
6. Test that a partial-date planned edit is blocked.
7. Test a full-flight planned edit and confirm package planned totals match the replacement.
8. Confirm the manual daily table totals equal replacement totals.
9. Confirm the final model exposes `man_*` evidence fields and uses manual values for final `_` fields, including `man_benchmark_kpi`, `man_benchmark_value`, `_benchmark_kpi`, and `_benchmark_value`.
10. Confirm cleanup removes temporary test rows from the sheet, raw table, daily table, final model, and mart. Existing legitimate manual rows may remain.
11. Confirm the request-refresh checkbox resets, updates the status cell, and delivers the notification email.

For exact queries and proof paths, use `QA_RUNBOOK.md`. The short checklist above is only a reminder; the runbook is the operational source for QA.
