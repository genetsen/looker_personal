# Source-Sheet Schema Validation Design

## Status

Superseded by the narrower implementation completed on June 24, 2026.

The observed failure was limited to `partner_placement_name`: Google Sheets
returned a list-column when the source cells mixed text and date values. Rather
than stopping every multi-sheet load on any type difference, the implemented
fix converts that one field to character immediately before the Phase 5
cross-sheet combine. Dates are preserved as `YYYY-MM-DD`, text is preserved as
text, and empty values remain missing.

The diagnostic validator below remains a possible future enhancement if type
conflicts recur in other fields. It is not the current loader behavior.

## Intended outcome

Before the FPD loader combines data from multiple Google Sheets, it will inspect
the column types from every successfully ingested sheet. If two sheets provide
incompatible types for the same normalized field, the loader will stop with a
plain-English report that identifies the responsible source sheets.

The report replaces opaque errors such as `..17$partner_placement_name <list>`
with actionable source information.

## Scope

| Surface | Planned behavior |
|---|---|
| Active loader | Update `util_collect_fpd_shortcutsFolder.r` |
| Validation point | Immediately before the Phase 5 cross-sheet combine |
| Data handling | Inspect only; do not silently coerce or discard values |
| Failure behavior | Print all detected type conflicts, then stop |
| Successful behavior | Continue to the existing combine unchanged |
| Live systems | No Google Sheet or BigQuery writes are introduced |

## Validation design

Each ingested data frame already has source metadata, including the source sheet
name and URL. Before combining the frames, the loader will build a schema
inventory with one row per source sheet and column.

| Inventory field | Meaning |
|---|---|
| Source sheet | Human-readable Google Sheet name |
| Source URL | Clickable link for investigating the sheet |
| Field | Normalized column name used by the loader |
| R class | High-level data type, such as character or list |
| Storage type | Underlying type returned by R |

For each field appearing in multiple sheets, the validator will compare its
types. A field fails validation when the participating sheets do not have a
compatible type signature.

## Error output

The failure log will begin with a clear headline:

```text
✗ Phase 5 schema validation failed before combining source sheets.
```

It will then print one block per conflicting field:

```text
Field: partner_placement_name
  Expected type: character
  Conflicting source:
    Sheet: ICE | Partner Data Collection | MOBREW | JAN 26 (6)
    Type: list
    URL: https://docs.google.com/...
```

If more than two source types exist, the report will list every participating
sheet and type rather than choosing only one offender. The original combine
operation will not run after validation fails.

## Type policy

The validator is diagnostic, not corrective:

- It will not convert list-columns to text automatically.
- It will not skip a conflicting sheet.
- It will not choose a type based only on whichever sheet appeared first.
- It will report all source sheets involved so the sheet data or parsing logic
  can be corrected deliberately.

## Tests and proof

| Test case | Expected proof |
|---|---|
| Character field plus list field | Validation fails and names the list-valued source sheet |
| Three sheets with two conflicting types | Report lists every sheet and its type |
| Matching character fields | Validation passes |
| Field present in only one sheet | Validation passes |
| Known June 24 failure shape | Report names `ICE | Partner Data Collection | MOBREW | JAN 26 (6)` rather than `..17` |

The implementation will first use a small local reproduction with synthetic
data frames. After that passes, the loader will be run far enough to exercise
the real Phase 5 validation path. A full production upload will not be required
to prove the logging behavior.

## Documentation impact

Because this changes operator-facing diagnostics, the active loader README and
the repository changelog will be updated in the implementation pass. The docs
will explain what the schema report means and how to use the named source sheet
and URL to investigate the data.
