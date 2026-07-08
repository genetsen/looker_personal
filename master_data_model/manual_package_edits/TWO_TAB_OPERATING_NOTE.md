# Manual Data Editor Two-Tab Operating Note

This note explains the safer two-tab Manual Data Editor workflow in plain English. It is for day-to-day use, not debugging. For technical details, use the [design contract](</Users/eugenetsenter/Looker_clonedRepo/looker_personal/master_data_model/manual_package_edits/DESIGN_CONTRACT.md>).

## Short Version

Use `Package Editor` to look things up. Use `Manual Edits` to enter corrections.

The loader can refresh `Package Editor` whenever it needs to. It should not treat a refreshed preview row as proof that someone deleted a manual edit. `Manual Edits` is the user-owned input tab.

## Which Tab Do I Use?

| Task | Use This Tab | What To Do |
|---|---|---|
| Find a package | `Package Editor` | Filter/search by package ID, site, package name, advertiser, campaign, channel, or other visible package context. |
| Enter a correction | `Manual Edits` | Copy the package ID/context you need, then enter the date window and corrected value. |
| Add a brand-new manual-only package | `Manual Edits` | Add a new row with package ID, dates, required metadata, and at least one corrected value. |
| Check current source values | `Package Editor` | Use this as the refreshed preview of package baselines and status. |
| Request a refresh | Sheet request control | Use the existing request control if you need Gene/the loader to process the rows. |
| Investigate a blocked row | `Manual Edits`, then QA runbook | Read the validation status/reason first. Use the QA runbook if the reason is unclear. |

## Normal Correction Flow

1. Open `Package Editor`.
2. Find the package row.
3. Copy the package ID and any helpful package context.
4. Go to `Manual Edits`.
5. Enter or update the correction row there.
6. Make sure the delivery override dates cover the exact period you want changed.
7. Fill the corrected metric or metadata field.
8. Request or run the loader.
9. After the loader runs, check the row status in `Manual Edits`.

## Common Edit Types

| Edit Type | Fields To Fill | Important Rule |
|---|---|---|
| Delivered metric correction | `Package ID`, delivery override dates, and one or more of `Spend`, `Impressions`, `Clicks`, `Video Plays`, `Video Completions` | The replacement total is spread across the selected date range. |
| Planned metric correction | `Package ID`, full flight dates, delivery override dates matching the full flight, `Planned Spend`, or `Planned Impressions` | Planned values are full-flight totals, not partial-week totals. |
| Metadata correction | `Package ID` plus fields such as `Campaign`, `Channel`, `Package Name`, `GS Channel`, `Benchmark KPI`, or `Benchmark Value` | Metadata applies package-wide, not just inside one date window. |
| New manual-only package | `Package ID`, site/package name, flight dates, delivery dates, required metadata, and at least one metric | Missing required metadata blocks the row. |
| Benchmark correction | `Benchmark KPI` and/or `Benchmark Value` | `Benchmark KPI` is text. `Benchmark Value` is numeric. |

## What Not To Do

| Do Not | Why |
|---|---|
| Do not enter corrections in `Package Editor`. | The loader refreshes this tab as a preview, so it is not the durable edit source. |
| Do not delete rows from `Manual Edits` unless you mean to remove that manual correction. | `Manual Edits` is the user-owned source of truth for manual input. |
| Do not use partial date ranges for planned metrics. | Planned spend and planned impressions are full-flight totals. |
| Do not rerun formatting/setup scripts to fix a data issue. | Formatting rebuilds can overwrite user-made sheet formatting. Data issues should be fixed through the loader or QA path. |
| Do not assume a blank preview row means the edit is gone. | The durable edit should be checked in `Manual Edits` and the manual landing tables. |

## If Something Looks Wrong

| Symptom | First Check |
|---|---|
| A correction is not showing in dashboards | Confirm the row exists in `Manual Edits`, then confirm the loader ran successfully. |
| A row is blocked | Read `Validation Status` and `Validation Reason` on the row. |
| A package disappeared from `Package Editor` | Check `Manual Edits` before assuming the correction was removed. |
| Benchmark looks wrong | Confirm `Benchmark KPI` is text and `Benchmark Value` is numeric in `Manual Edits`. |
| The sheet looks visually broken | Do not run setup automatically. Take a formatting snapshot or ask for a formatting-specific repair. |

## Plain-English Rule

The script can rebuild the preview. It cannot decide that a user edit should disappear just because the preview changed.
