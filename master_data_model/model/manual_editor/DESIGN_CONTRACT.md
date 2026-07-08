# Manual Data Editor Design Contract

This document defines the safer Manual Data Editor contract. The main rule is:
user-authored edits are source data. Loader refreshes may enrich, validate, and
publish those edits, but they must never infer-delete or overwrite them.

## Target Architecture

| Surface | Owner | May Be Rebuilt By Script? | Purpose | Deletion Rule |
|---|---:|---:|---|---|
| `Manual Edits` tab | User | No | Durable user input rows: package ID, date window, replacement metrics, metadata, benchmark fields, edit audit fields, and optional reason. | A row disappears only when the user clears/deletes it or an explicit archive/delete action is run. |
| `Package Editor` tab | Script | Yes | Refreshed read-only package lookup and preview of current baselines/manual status. | Rebuilds here do not delete edits because this tab is not the edit source. |
| `Instructions` tab | Script/manual docs | Only during approved setup | User-facing quick guide. | Formatting rebuild requires explicit approval. |
| Raw manual table | Loader | Yes, from durable input snapshot | Auditable package-level manual evidence. | Refresh must preserve valid prior evidence unless the user-owned input row explicitly removes it. |
| Daily manual table | Loader | Yes, derived output | Package/date manual rows consumed by the master model. | May be replaced only after validation proves the durable input rows still publish as expected. |
| Lookup table | SQL/script | Yes | Current source-backed package baselines and benchmark fallback values. | Must not include manual-applied rows as baselines for user deletion decisions. |

## Current File And Table Map

| Item | Current Role | Target Role Under This Contract | Notes |
|---|---|---|---|
| `model/manual_editor/load_manual_package_edits.R` | Canonical loader that reads and rewrites `Package Editor`. | Canonical loader that reads `Manual Edits`, refreshes `Package Editor`, validates, and publishes raw/daily tables. | This is the only implementation file that should contain loader behavior. |
| `manual_package_edits/load_manual_package_edits.R` | Historical launcher. | Keep as launcher only. | Do not reintroduce duplicate loader logic here. |
| `model/manual_editor/create_manual_package_editor_package_lookup.sql` | Builds refreshed package lookup. | Same, but output feeds the read-only lookup/preview tab. | Current benchmark fallback columns belong here. |
| `model/manual_editor/setup_manual_package_editor_sheet.mjs` | Full formatting/setup rebuild. | Setup/repair both `Manual Edits` and `Package Editor` only after explicit rebuild approval. | Do not run against live formatting casually. |
| `model/manual_editor/repair_manual_package_editor_filters.mjs` | Repairs `Package Editor` filter geometry and formatting-dependent rules. | Repair read-only lookup geometry and protect refreshed columns. | A separate narrow repair may be needed for `Manual Edits`. |
| `model/manual_editor/repair_manual_package_editor_conditional_formatting.mjs` | Rebuilds conditional formatting formulas. | Keep formatting formulas aligned to each tab's header map. | Formula rules must resolve by header names, not fixed letters. |
| `landing.master_data_model_manual_package_edits_raw` | Latest raw loader snapshot. | Snapshot generated from durable user input plus lookup context. | Backup before every risky migration or shape change. |
| `landing.master_data_model_manual_package_daily` | Latest valid daily manual rows. | Derived output from validated raw edits. | Replace only after row/package preservation proof passes. |
| `master_stg.manual_package_editor_package_lookup` | Source-backed package lookup. | Read-only package preview source. | Manual-applied rows stay excluded from deletion inference. |

## Loader Contract

| Step | Required Behavior | Proof |
|---|---|---|
| Read input | Read user-owned edits from `Manual Edits`, not from a script-rebuilt preview range. | Header check shows loader input tab is `Manual Edits`. |
| Refresh lookup | Refresh/rewrite package preview separately from user input. | `Package Editor` can change while `Manual Edits` row count and edit values remain unchanged. |
| Validate | Validate dates, duplicate package/date/metric conflicts, required manual-only metadata, numeric benchmark value, and planned full-flight rules. | Blocked rows explain the exact failing field without deleting the row. |
| Publish raw | Write raw evidence only after validation and preservation checks pass. | Raw active/valid package counts reconcile to expected input rows. |
| Publish daily | Write daily rows only if daily totals reconcile and no known-good package disappears unexpectedly. | Missing-package proof versus prior valid daily snapshot is zero unless an explicit user deletion is detected. |
| Write back status | Write validation status/audit output to `Manual Edits` without changing user-entered fields. | User-edit columns are unchanged after loader run. |

## Field Contract

| User Field | Raw Field | Daily Field | Final Field | Type | Priority |
|---|---|---|---|---|---|
| `Benchmark KPI` | `man_benchmark_kpi` | `man_benchmark_kpi` | `_benchmark_kpi` | String | Manual first, then non-empty FPD/source fallback. |
| `Benchmark Value` | `man_benchmark_value` | `man_benchmark_value` | `_benchmark_value` | Numeric | Manual first, then FPD/source fallback. |
| Metrics | `replacement_*` | `man_daily_*` | Final `_` metric fields | Numeric | Manual date-window replacement first. |
| Metadata | `man_*` metadata columns | Passed through daily where needed | Final package metadata fields | String | Manual first, then source-backed lookup. |

## Migration Rule

1. Create `Manual Edits` from the restored trusted raw table and any live sheet edits that contain manual evidence.
2. Keep the existing `Package Editor` tab as a read-only preview until the new path passes proof.
3. Do not run a full formatting rebuild on the live workbook unless explicitly approved.
4. Before replacing live raw/daily tables, compare the new loader output to the trusted backup:
   - valid package count
   - daily row count
   - missing package IDs
   - daily metric total proof
5. If any preservation proof fails, restore raw/daily from the backup and leave the user input tab untouched.
