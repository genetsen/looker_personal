# Brownfield Evidence

This companion preserves the concrete evidence from the design discussion so downstream architecture work does not lose the reason behind the contract.

## Current Setup Difference

| Current Pattern | Proposed Pattern |
|---|---|
| Package/date master model is central. | Universal final evidence table is central. |
| Lower-grain detail lives in sibling views. | Detail rows feed the main final table first. |
| New sources often need custom master-flow logic. | New sources enter through a source-adapter contract. |
| Missing metadata is handled source by source. | Inferred metadata is a formal fill-blanks-only layer. |
| Business logic is mixed into large model SQL. | Reusable rules and grain-specific safeguards are explicit. |

## DCM Evidence From Live Data

| Evidence Point | Actual Example | Design Meaning |
|---|---|---|
| DCM pricing math is reusable. | CPM standard example: package `P37G8FD` had `1,421,927` impressions at `$45` CPM, producing `$63,986.715`. | Row-level formulas can be adapted when inputs are present. |
| DCM budget allocation is package-centered. | Package `P37ZVPK` had `12` creative rows sharing a `$2,300` planned cost. | Allocation buckets must be declared before metrics are exposed as summable. |
| Package totals repeat on detail rows. | `P37ZVPK` repeated `pkg_total_imps = 28,470` across creative rows. | Repeated context totals need `doNotSum` treatment. |
| Daily share is package/date based. | On `2025-11-01`, `Gifting_1 x 1` used `7,310 / 26,533 = 27.5506%`. | Daily shares depend on a declared bucket, not pure grain-agnostic logic. |
| Package/placement/date is not unique enough for detail identity. | `P37ZVPK + P37ZVSS + 2025-11-01` had two creatives: `Gifting_300x250` and `SelfGifting_300x250`. | Detail identity must include enough source-grain fields or a stable source row ID. |
| CPC and CPA examples were unavailable. | Live grouped counts showed CPM, Flat, Free, CPV, null, and out-of-flight states, but no CPC/CPA examples. | Supported logic should not be treated as live-proven without matching data. |

## Source Objects Used For Evidence

| Object | Role |
|---|---|
| `looker-studio-pro-452620.DCM.20250505_costModel_v5` | Live DCM cost-model evidence table. |
| `giant-spoon-299605.data_model_2025.new_md` | Raw DCM delivery source used for duplicate-key checks. |
| `sql/base/dcm/20250505_costModel_v5.sql` | Local builder SQL used to inspect current DCM logic shape. |

## Preservation Notes

| Thread Claim | Preserved In |
|---|---|
| User prefers one final master table readable at any granularity. | `SPEC.md` CAP-1 and Constraints. |
| Placeholders should represent unavailable lower-grain dimensions. | `SPEC.md` CAP-2 and `field-contract.md`. |
| Inferred metadata should use delivery details. | `SPEC.md` CAP-3 and `field-contract.md`. |
| Inference only applies when actual metadata is missing. | `SPEC.md` Constraints and `field-contract.md`. |
| Flight start/end should use min/max delivery date when missing. | `SPEC.md` Constraints and `field-contract.md`. |
| Planned spend/impressions should use total delivery spend/impressions when missing. | `SPEC.md` Constraints and `field-contract.md`. |
| New sources and granularities should be easier to add through a standard process. | `SPEC.md` CAP-5 and `source-onboarding-contract.md`. |
| Interactive source mapper should guide new and existing source mapping configuration. | `SPEC.md` CAP-6 and `source-mapper-contract.md`. |
| Redesign should leverage dbt. | `SPEC.md` CAP-7 and `dbt-bigquery-structure.md`. |
| BigQuery should be organized into clean projects, datasets, and tables. | `SPEC.md` CAP-8 and `dbt-bigquery-structure.md`. |
| Generated report prefers the `mdm_*` warehouse layout and readable placeholder style. | `SPEC.md`, `field-contract.md`, `dbt-bigquery-structure.md`, and PRD addendum. |
| User requires the new version to include all current master-table columns and align final totals. | `SPEC.md` CAP-9, `source-onboarding-contract.md`, `dbt-bigquery-structure.md`, PRD FR-16, and PRD addendum. |
| User requires new universal fields to use a prefix. | `SPEC.md`, `field-contract.md`, PRD, and PRD addendum. |
