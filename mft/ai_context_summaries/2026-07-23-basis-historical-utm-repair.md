# July 23, 2026 — Basis historical UTM repair

## What was learned

- The 117 missing FY26 Q2/Q3 placement-and-creative keys were present in the partner workbook's approved `MASSMUTUAL005_Updated 6.15` worksheet.
- Production had retained only the current `MASSMUTUAL005_Updated 7.7` worksheet.
- Several delivered CTV creative names also contained confirmed wrapper text absent from the partner mapping name.

## What changed

- Historical and current worksheets now load to separate landing tables.
- The active union received 133 additional complete official mappings.
- The Basis join now has a final unique source-backed fallback using placement ID plus normalized creative name.
- Official partner mappings replace 54 extrapolated daily-row values across six approved mapping combinations; no other populated UTM changes.

## Verified result

- The SQL change guard passed all eight comparisons.
- Both production scheduled refreshes succeeded.
- The live mart and stored table each contain zero missing report keys and reconcile at 11,533 rows, 4,629,186 impressions, $154,941.79 cost, and 1,842 clicks.

## Remaining attention

- For future partner workbooks, preserve every approved worksheet that contains mappings for delivered media; do not load only the latest tab when older paused creatives still deliver.
