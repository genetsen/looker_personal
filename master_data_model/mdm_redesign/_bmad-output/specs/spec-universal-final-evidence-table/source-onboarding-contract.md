# Source Onboarding Contract

This companion defines how a new source enters the universal final evidence table. A source adapter is the small mapping layer that turns one source's fields into the shared final table contract.

## Onboarding Checklist

| Step | Required Output | Proof Check |
|---|---|---|
| Identify source grain | State what one source row represents. | Example rows confirm the stated grain. |
| Map dimensions | Map package, placement, ad, creative, DMA, campaign, client, supplier, and date where available. | Unavailable dimensions have placeholders. |
| Map metrics | Map spend, impressions, clicks, video metrics, conversions, and source totals. | Each metric has value status and summability labels. |
| Define metadata inference | State which blanks can be inferred from delivery details. | Actual values are not overwritten. |
| Define source precedence | State how this source competes or coexists with existing sources. | Conflict cases have expected outcomes. |
| Configure mapping in mapper | Use the interactive source mapper for dimensions, metrics, placeholders, inference, and tests. | Saved mapping config validates before dbt models consume it. |
| Preserve source lineage | Keep source object, file, row, or load metadata. | A final row can be traced back to source evidence. |
| Validate rollups | Compare source totals to final-table totals at the declared additive grain. | Differences are explained and approved. |
| Validate legacy parity | Compare the candidate output to the current live master table. | No current columns are missing and approved legacy totals reconcile. |

## Adapter Output Contract

| Output Area | Adapter Must Provide |
|---|---|
| Identity | `univ_source_system`, `univ_source_row_id`, `univ_source_lineage`, `univ_row_grain`. |
| Time | `univ_record_date` and source-specific date evidence. |
| Dimensions | Actual values, inferred values, or placeholders for each selected slot. |
| Metrics | Additive delivery metrics plus non-summable context totals where useful, with value status and summability labels. |
| Metadata status | Flags showing actual, manual, inferred, mixed, or placeholder values. |
| QA labels | Row-level warnings, issue categories, and inference confidence. |
| Mapper metadata | Mapping version, reviewer, mapping status, and validation timestamp. |

## Source Addition Definition Of Done

| Area | Done Means |
|---|---|
| Readability | A non-technical user can group the final table by available dimensions without losing rows. |
| Safety | Package totals do not inflate when grouped by creative, DMA, or other lower-grain fields. |
| Evidence | Sample rows show actual, inferred, and placeholder metadata behavior. |
| Traceability | Every final row can point back to source evidence or a documented inference rule. |
| Compatibility | Existing production objects are untouched unless the user approves replacement. |
| Legacy parity | Candidate outputs include every current master-table column and reconcile approved totals before migration. |
| Configuration | Source mapping decisions are stored outside core SQL and can be reviewed or edited later. |

## Business Rule Boundaries

| Boundary | Rule |
|---|---|
| Placeholder use | Placeholders represent unavailable dimensions; they are not inferred facts. |
| Inference use | Inference fills missing metadata only. |
| Allocation use | Allocation may spread package totals to detail rows only when the allocation method is declared and tested. |
| Reporting use | Saved package, creative, or DMA views may exist as shortcuts, but the universal final table remains the primary evidence surface. |
| dbt use | Source adapters and downstream transformations should be represented as dbt models, tests, docs, and exposures where practical. |
