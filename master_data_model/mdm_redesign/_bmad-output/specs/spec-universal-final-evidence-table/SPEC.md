---
id: SPEC-universal-final-evidence-table
companions:
  - field-contract.md
  - source-onboarding-contract.md
  - source-mapper-contract.md
  - dbt-bigquery-structure.md
  - architecture-diagrams.md
  - brownfield-evidence.md
  - /Users/eugenetsenter/Docs/dbt/TABLE_CONTRACTS.md
  - /Users/eugenetsenter/Docs/dbt/fivetran/README.md
  - ../../project-context.md
sources:
  - ../../planning-artifacts/prds/prd-master_data_model-2026-06-15/prd.md
---

> **Canonical contract.** This SPEC and the files in `companions:` are the complete, preservation-validated contract for what to build, test, and validate. Source documents listed in frontmatter are for traceability only.

# Universal Final Evidence Table

## Why

The master data model needs one readable final table that can support package, creative, DMA, and future grains without forcing every source into package/date shape or hiding unavailable dimensions.

## Capabilities

- id: CAP-1
  intent: The model provides one universal final evidence table that users can query at multiple granularities.
  success: A user can group the same final table by package, creative, DMA, source, campaign, or date without switching to separate truth tables.

- id: CAP-2
  intent: The model preserves each source row at its natural useful grain while filling unavailable dimension slots with explicit placeholders.
  success: Every final row declares its row grain, populated dimensions, unavailable dimensions, and source lineage.

- id: CAP-3
  intent: The model creates inferred metadata from delivery evidence only when actual or trusted source metadata is missing.
  success: Flight start, flight end, planned spend, and planned impressions keep actual values when present and otherwise use delivery-derived fallback values with source flags.

- id: CAP-4
  intent: The model makes every exposed metric explain where it came from and whether it can be summed.
  success: Metrics carry value-status labels such as direct, inferred, allocated, or unavailable, and summability labels such as additive, doNotSum, or blocked.

- id: CAP-5
  intent: New sources can be onboarded through a standard source-adapter contract.
  success: A new source can declare its grain, dimensions, metrics, placeholders, inference rules, and validation checks before entering the final table.

- id: CAP-6
  intent: An agent-guided source mapper helps users configure new source mappings and edit existing mappings.
  success: A user can launch an interactive HTML mapping widget from the onboarding skill, review suggested mappings, save mapping decisions, and rerun validation without hand-editing core SQL.

- id: CAP-7
  intent: The redesign uses dbt as the main transformation, documentation, and validation framework.
  success: Source adapters, staging models, intermediate logic, final evidence models, tests, and documentation are represented in a dbt project structure that can be parsed, run, tested, and reviewed.

- id: CAP-8
  intent: BigQuery storage is organized into clean zones for raw inputs, configured mappings, dbt-built models, QA outputs, and user-facing final tables.
  success: A reviewer can tell from project, dataset, and table naming whether an object is raw source, mapping configuration, staging, intermediate, final, QA, or published reporting output.

- id: CAP-9
  intent: The redesigned version preserves the current master table contract while adding grain flexibility.
  success: A validation run proves the candidate includes every current column from the live master table and reconciles approved legacy totals before migration.

## Constraints

- The final design must support one human-readable final table as the primary user-facing evidence surface.
- New universal fields must use the agreed `univ_` prefix.
- Unavailable lower-grain dimensions must use explicit readable placeholders, such as `not_available_at_source`, rather than disappearing silently.
- Inferred metadata must fill blanks only; it must not overwrite actual source metadata, approved manual metadata, or trusted source-provided planning values.
- If actual flight dates are missing, inferred flight start and end come from minimum and maximum delivery dates and are updated as delivery evidence changes.
- If actual planned spend or impressions are missing, inferred plan-like totals come from total observed delivery spend and impressions and are updated as delivery evidence changes.
- Every metric must explain value status, such as direct, inferred, allocated, or unavailable, before it is exposed in the final table.
- Every metric must explain summability, such as additive, doNotSum, or blocked, before it is exposed in the final table.
- Lower-grain rows must not silently duplicate package totals as summable metrics.
- Mapping configuration must be editable outside core model SQL, versionable, and validated before it affects final outputs.
- dbt should own transformation logic and tests for the redesign unless a non-dbt tool is explicitly better for a source ingestion or UI task.
- BigQuery object names should follow the report-aligned `mdm_*` zone pattern: raw, config, staging, intermediate, QA, mart, publish, and sandbox.
- The candidate version must include every current column from the live [master table](https://console.cloud.google.com/bigquery?project=looker-studio-pro-452620&p=looker-studio-pro-452620&d=master_stg&t=data_model&page=table), unless the user explicitly approves an omission or rename.
- The candidate version must reconcile approved final totals against the live [master table](https://console.cloud.google.com/bigquery?project=looker-studio-pro-452620&p=looker-studio-pro-452620&d=master_stg&t=data_model&page=table) using a fresh validation-time baseline.
- BigQuery-backed claims and migrations require live warehouse validation before production claims or deploys.

## Non-goals

- This spec does not replace the current production master model or mart.
- This spec does not define live deployment SQL.
- This spec does not change the Manual Package Editor workflow.
- This spec does not promise that all business logic is grain-agnostic.
- This spec does not finalize the source-mapper widget UX or storage implementation.
- This spec does not require moving raw third-party ingestion into dbt.

## Success signal

A source with only package/date data and a source with creative or DMA detail can both land in the same final table; grouping by package, creative, or DMA produces understandable rows, additive metrics remain stable, and inferred metadata is auditable field by field.

## Assumptions

- The first implementation should be a sibling or versioned model until the user explicitly approves replacement of current production objects.
- "Actual metadata" means trusted source-provided values or approved manual metadata.
- Delivery-derived planned values are plan-like fallbacks, not original media-plan truth.

## Open Questions

- Which exact placeholder strings should be standard beyond the default `not_available_at_source` style?
- Should inferred plan-like values appear directly in final planned fields, or should raw actual, inferred, and selected final fields all remain visible?
- Which dimension slots are required for the first version: package, placement, ad, creative, DMA, campaign, source, client, supplier, and audience?
- What exactly counts as missing for metadata: null only, blank strings, zero values, invalid dates, or unknown labels?
- How should source-precedence conflicts work when two sources provide valid values for the same dimension or metric?
- What is the exact storage split between BigQuery `mdm_config` tables, dbt seeds, and checked-in YAML?
- Should the interactive source mapper be a local HTML artifact, a hosted internal app, or a Codex/BMAD widget launched by the skill?
- Should the redesign live in the existing `looker-studio-pro-452620` project or use new BigQuery projects for development, QA, and production separation?
- What exact tolerance should be used when reconciling floating-point totals to the current master table?
