# Source Mapper Contract

This companion captures the broad requirement for an agent skill plus interactive HTML widget that helps configure source mappings. The exact widget UX is intentionally deferred.

## Purpose

The source mapper should make source integration a guided process instead of a hand-edited SQL exercise. The agent skill walks the user through source discovery, launches an HTML widget for manual configuration, saves mapping decisions, and triggers validation before dbt models consume the source.

## Skill Flow

| Step | Agent Skill Responsibility | Widget Responsibility |
|---|---|---|
| Source discovery | Inspect source schema, sample rows, grain clues, and metric candidates. | Show source fields and sample values. |
| Mapping proposal | Suggest dimensions, metrics, row grain, placeholders, and inference rules. | Let user accept, edit, reject, or add mappings. |
| Metric status and safety | Classify where metric values come from and whether they can be summed. | Show warnings when a mapping could duplicate totals. |
| Inferred metadata | Suggest fill-blanks-only inference rules. | Let user choose which missing fields can be inferred. |
| Validation | Run schema, grain, totals, placeholder, and inference checks. | Display pass/fail status and examples. |
| Save mapping | Write versioned mapping configuration. | Record reviewer-visible choices and notes. |

## Mapping Configuration Contract

| Config Area | Examples |
|---|---|
| Source identity | Source system, BigQuery object, source owner, refresh cadence. |
| Grain | Row grain label, unique key fields, date field. |
| Dimensions | Package, placement, ad, creative, DMA, campaign, client, supplier, audience. |
| Metrics | Spend, impressions, clicks, video metrics, conversions, source totals. |
| Placeholder rules | Standard placeholder for each unavailable dimension. |
| Inference rules | Fields eligible for fill-blanks-only inference and confidence rules. |
| Metric rules | Value status such as direct, inferred, allocated, or unavailable; summability such as additive, doNotSum, or blocked. |
| Tests | Required dbt tests and source-specific assertion queries. |
| Review state | Draft, validated, approved, rejected, superseded. |

## Broad Implementation Requirement

The mapper should save configuration in a form that is versionable and machine-readable. The preferred planning shape is approved rules in BigQuery `mdm_config` tables plus dbt documentation. dbt models should read or compile from approved mapping configuration rather than requiring manual edits to the core final-table SQL for every new source.

## Deferred Details

| Topic | Deferred Decision |
|---|---|
| Widget runtime | Local HTML artifact, hosted Shareable/Sites page, or Codex/BMAD-native widget. |
| Storage format | Exact split between BigQuery `mdm_config` tables, dbt seeds, YAML files, or hybrid. |
| Approval workflow | Whether approval is one user action, PR review, BigQuery status flag, or dbt test gate. |
| Auth model | Whether the widget writes directly or produces a file/table for the agent to apply. |
