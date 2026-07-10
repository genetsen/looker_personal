# 2026-07-08 Manual Editor Backend Logic Fix

## What We Learned

- The user reported that the Purely Elizabeth QUAN manual row had a package ID and that any missing package ID was caused by the refresh path, not by user omission.
- Clean BigQuery snapshots showed the preserved row with package ID `ccdooh`, valid status, planned spend `210000`, planned impressions `2610585`, and delivery dates from `2026-06-08` through `2026-07-06`.
- The bad current run created extra blocked rows beside valid recovered rows. The visible interface itself should remain the existing `Package Editor` tab.

## What Changed

- The loader now treats trusted prior raw manual rows as durable edit state.
- Generated rows from a refreshed `Package Editor` are dropped or reset unless they carry real user edit evidence: `Manual Edit At` or `Manual Edit By`, or trusted prior raw history already accepted by the loader.
- Display labels such as `Manually Edited?`, `Validation Status`, `Validation Reason`, and loose `Manual Edit Published At` sheet values no longer count as proof that a user intended a new manual edit.

## What Still Needs Attention

- The focused merge tests prove the backend choice logic, and the live loader was rerun after restoring the clean raw/daily backups.
- The final live proof after rerun showed 15 active valid rows, 0 blocked rows, 785 daily rows, and the `ccdooh` QUAN row preserved.
- The live sheet should not be formatting-rebuilt unless the user explicitly asks for a full rebuild.
