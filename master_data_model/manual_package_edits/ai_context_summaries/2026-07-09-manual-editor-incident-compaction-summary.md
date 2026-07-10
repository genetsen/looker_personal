# Manual Editor Incident Compaction Summary - 2026-07-09

This thread resumed from a compacted context during the Manual Data Editor
incident investigation. The active task was to continue options `2,3`: try to
obtain Drive Activity metadata and search for older workbook copies/audit
evidence before 2026-06-26.

## What Was Already Known

- The Purely Elizabeth QUAN row had package ID `ccdooh`; any missing package ID
  was caused by a later refresh/update path, not by the user omitting it.
- The archived workbook revision pair `2574 -> 2576` on 2026-07-07 changed 8
  cells on one row for Package Friendly Name `Columbus Circle DOOH`.
- Current production revision pairs were bulk refreshes, not small-cell edits.

## What This Continuation Checked

- Drive Activity API access was retried through the R/gargle path and the known
  `gcloud-giantspoon` ADC profile.
- Drive file search was run for Manual Data Editor workbook names and for text
  related to `ccdooh`, Columbus Circle DOOH, and the Purely Elizabeth campaign.
- The rollback copy named `Manual Data Editor rollback copy - before manual edit
  audit - 2026-06-23` was exported through Drive and parsed locally.
- Archived workbook revisions from 2026-06-24 and 2026-06-25 were downloaded and
  diffed against the first 2026-06-26 revision.

## Current Evidence State

- Drive Activity remains blocked by missing `drive.activity.readonly` scope.
- The rollback copy is content-readable through Drive export but not through the
  Sheets API credential path.
- The rollback copy does not contain `ccdooh` or `Columbus Circle DOOH`.
- The early archived revision pairs before 2026-06-26 were all bulk/refresh
  changes, not small-cell edits.
- A field-level rollback-copy diff file now lists 116 non-social, non-Amazon
  2026 manual-marker differences with visible value, baseline value, and numeric
  delta.
