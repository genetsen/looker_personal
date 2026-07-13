# Changelog

## 2026-07-13

- **CHANGED** - Replaced the standalone RTL Google Sheet loader with a direct-CM360 compatibility refresh. It keeps the existing 18-column BigQuery table structure, leaves unavailable legacy delivery and Sheet-lineage fields blank, and stops before writing if the live schema or direct-source keys are unsafe.
- **VERIFIED** - Confirmed the live compatibility table matches direct CM360 with 5,394 records and 83,402 conversions, while retaining the original column names, order, types, and nullability.

## 2026-06-15

- **ADDED** - Set up RTL conversion sales to refresh each day from the shared spreadsheet into BigQuery. A verified refresh loaded 2,482 rows; the complete daily automation still needs testing.

Related sessions:

- [RTL conversion refresh](/Users/eugenetsenter/.codex/sessions/2026/06/15/rollout-2026-06-15T16-25-03-019eccf5-849f-7af2-bbf8-22c17c139cb8.jsonl)

### Next

- Test the RTL refresh through the complete daily automation - PENDING

### Pending Next Actions

- **Since Jun 15** - Test the RTL refresh through the complete daily automation - RECOMMENDED
