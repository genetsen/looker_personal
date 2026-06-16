---
title: Universal Final Evidence Table and Source Onboarding for Master Data Model
status: draft
created: 2026-06-15
updated: 2026-06-16
---

# PRD: Universal Final Evidence Table and Source Onboarding for Master Data Model

## 0. Document Purpose

This PRD defines the user-facing requirements for redesigning the master data model around one readable final evidence table, a repeatable source-onboarding workflow, and a guided source-mapping experience. It is grounded in the BMAD spec at [Universal Final Evidence Table SPEC](/Users/eugenetsenter/Looker_clonedRepo/looker_personal/master_data_model/_bmad-output/specs/spec-universal-final-evidence-table/SPEC.md). Deeper implementation direction lives in [addendum.md](/Users/eugenetsenter/Looker_clonedRepo/looker_personal/master_data_model/_bmad-output/planning-artifacts/prds/prd-master_data_model-2026-06-15/addendum.md).

## 1. Vision

The master data model should feel like one reliable map, not a stack of special-case tables. A user should be able to query one final table by package, creative, DMA, source, campaign, or date and still understand what each row means.

The redesign keeps source detail visible instead of forcing every source into package/date shape too early. When a source lacks a dimension, the final table uses explicit placeholders. When trusted metadata is missing, the model can infer fallback metadata from delivery evidence, but only as a fill-blanks rule.

New sources should become easier to add. Instead of hand-editing core SQL for every source, an agent-guided mapper should help inspect a source, propose mappings, launch an interactive HTML widget for manual configuration, validate the mapping, and feed approved mapping decisions into dbt-backed models.

## 2. Target User

### 2.1 Jobs To Be Done

- As a model owner, I want one final table that works across grains so I do not need to remember which detail table is the real source for each question.
- As a source integrator, I want a repeatable source-mapping workflow so adding a new source does not require risky custom SQL edits.
- As an analyst, I want unavailable dimensions to show as explicit placeholders so grouped reports do not silently drop or hide rows.
- As a reviewer, I want metric safety, inference choices, and source lineage to be auditable before a source affects reporting.
- As a dashboard owner, I want the new version to preserve the current master table's columns and totals so existing reporting does not quietly break.
- As a beginner learning dbt, I want the warehouse and model layers to be named plainly so I can tell raw, staging, intermediate, final, mart, and QA objects apart.

### 2.2 Non-Users (v1)

- External client dashboard viewers who only consume already-published dashboards.
- Source owners who only maintain upstream files or vendor feeds and do not configure mappings.
- General-purpose data-platform teams looking for a cross-company ingestion framework.

### 2.3 Key User Journeys

- **UJ-1. Gene asks a package, creative, or DMA question from one table.**
  - **Persona + context:** Gene owns the model and wants fewer “which table should I use?” decisions.
  - **Entry state:** A universal final evidence table exists in a non-production or versioned location.
  - **Path:** Gene filters by source, groups by package, then groups by creative or DMA using the same final table.
  - **Climax:** Additive metrics stay stable, placeholders keep unavailable dimensions visible, and repeated context totals are marked as not safe to sum.
  - **Resolution:** Gene can decide whether a shortcut mart is needed without treating the mart as the source of truth.

- **UJ-2. Gene adds a new source through the source mapper.**
  - **Persona + context:** Gene has a new source with unknown grain and messy field names.
  - **Entry state:** The onboarding skill can inspect the source schema and sample rows.
  - **Path:** The skill proposes dimensions, metrics, row grain, placeholders, inference rules, and tests. Gene opens the HTML widget, edits the mapping, and saves a draft.
  - **Climax:** Validation shows whether the mapping is safe, including grain checks and metric-safety checks.
  - **Resolution:** Approved mapping configuration becomes available to dbt models without direct edits to core final-table SQL.

- **UJ-3. A reviewer audits inferred metadata.**
  - **Persona + context:** A reviewer wants to know whether plan-like fields came from actual metadata or delivery-derived fallbacks.
  - **Entry state:** Final rows contain source, manual, inferred, and placeholder status fields.
  - **Path:** The reviewer filters rows where metadata was inferred and checks inference reason, evidence, and confidence.
  - **Climax:** The reviewer can confirm actual metadata was not overwritten.
  - **Resolution:** The reviewer approves, rejects, or asks for a mapping/inference rule change.

## 3. Glossary

- **Universal final evidence table** — The primary user-facing final table. It contains rows from multiple sources and can be grouped at multiple granularities.
- **Grain** — What one row represents, such as package/date, creative/date, DMA/date, or source-row/date.
- **Source adapter** — The mapping layer that converts one source into the shared final-table contract.
- **Source mapper** — The agent-guided workflow and interactive HTML widget used to configure source mappings.
- **Placeholder** — A visible value used when a dimension is unavailable, such as `not_available_at_source`.
- **Inferred metadata** — Metadata derived from delivery clues only when actual or trusted metadata is missing.
- **Additive metric** — A metric that is safe to sum across rows at approved grains, such as row-level impressions.
- **doNotSum context** — A repeated context value that may be useful on a row but must not be summed, such as package totals repeated on creative rows.
- **Metric value status** — A small label explaining where a number came from: direct, inferred, allocated, or unavailable.
- **Metric summability** — A small label explaining whether a number can be safely summed: additive, doNotSum, or blocked.
- **dbt model** — A SQL transformation managed by dbt, with documentation, tests, and lineage.
- **Mart** — A reporting shortcut derived from the final evidence table, not a separate source of truth.

## 4. Features

### 4.1 Universal Final Evidence Table

**Description:** The system provides one primary final table that remains readable across package, creative, DMA, source, campaign, and date groupings. It realizes UJ-1.

**Functional Requirements:**

#### FR-1: Multi-Grain Final Table

The model owner can query one universal final evidence table across multiple grains.

**Consequences (testable):**
- The table contains a declared prefixed row-grain field, such as `univ_row_grain`.
- New universal fields use an agreed prefix so they are easy to separate from legacy and source-specific fields.
- Rows can be grouped by package, creative, DMA, source, campaign, and date without switching truth tables.
- The table preserves source lineage for every row.

#### FR-2: Explicit Missing Dimensions

The final table uses standardized placeholders when a source does not provide a selected dimension.

**Consequences (testable):**
- Required selected dimension fields do not disappear silently.
- Unavailable dimensions are distinguishable from actual unknown source values.
- The default unavailable-dimension placeholder style is readable, such as `not_available_at_source`, not an empty blank or opaque code.
- Placeholder behavior is documented and testable.

#### FR-3: Natural Source Grain Preservation

The final table keeps each source at its natural useful grain unless an approved allocation or rollup rule changes it.

**Consequences (testable):**
- Lower-grain source rows are not silently collapsed into package/date rows.
- Any rollup, allocation, or summarization rule is documented and validated.

### 4.2 Inferred Metadata and Metric Safety

**Description:** The system fills missing metadata from delivery evidence only where trusted values are blank, and it separates summable metrics from repeated context. It realizes UJ-1 and UJ-3.

**Functional Requirements:**

#### FR-4: Fill-Blanks-Only Inference

The model can infer metadata from delivery evidence only when actual or approved manual metadata is missing.

**Consequences (testable):**
- Actual source metadata is not overwritten by inferred metadata.
- Approved manual metadata is not overwritten by inferred metadata.
- Each inferred field records source, reason, evidence, and confidence.

#### FR-5: Delivery-Derived Fallbacks

When actual values are missing, the model can infer flight start, flight end, planned spend, and planned impressions from delivery evidence.

**Consequences (testable):**
- Missing flight start uses minimum delivery date.
- Missing flight end uses maximum delivery date.
- Missing planned spend uses total observed delivery spend.
- Missing planned impressions uses total observed delivery impressions.

#### FR-6: Metric Status and Safety Classification

Every exposed metric explains both where the value came from and whether it is safe to sum.

**Consequences (testable):**
- Each metric can be marked direct, inferred, allocated, or unavailable.
- Additive metrics can be summed at approved grains.
- Repeated package or source context totals are not exposed as normal summable metrics.
- Metrics that are unavailable or unsafe are visibly blocked or marked doNotSum.
- QA checks detect likely metric duplication across lower-grain rows.

### 4.3 Interactive Source Mapper

**Description:** An agent-guided source onboarding skill launches an interactive HTML widget so the user can configure new source mappings and edit existing mappings. It realizes UJ-2.

**Functional Requirements:**

#### FR-7: Guided Source Discovery

The source onboarding skill can inspect a source and summarize candidate grain, dimensions, metrics, source lineage, and risks.

**Consequences (testable):**
- The skill produces a candidate source profile before mapping is approved.
- The profile includes source object, sample rows, candidate keys, candidate metrics, and grain concerns.

#### FR-8: Interactive Mapping Configuration

The user can launch an HTML mapping widget from the onboarding skill to review and edit mappings.

**Consequences (testable):**
- The widget displays source fields and sample values.
- The widget lets the user accept, edit, reject, or add dimension and metric mappings.
- The widget highlights metric-safety and grain-risk warnings.

#### FR-9: Versioned Mapping Decisions

The mapper saves approved mapping decisions outside core model SQL.

**Consequences (testable):**
- Mapping configuration records version, reviewer, status, and validation timestamp.
- Existing mappings can be opened and edited.
- dbt models can consume or compile from approved mapping configuration.

#### FR-10: Mapper Validation Loop

The source mapper can run validation checks before a mapping affects final outputs.

**Consequences (testable):**
- Grain, totals, placeholder, inference, and source-lineage checks can pass or fail visibly.
- Failed validation prevents promotion to approved mapping status.

### 4.4 dbt-Backed Model Stack and BigQuery Organization

**Description:** The redesign uses dbt as the main transformation, documentation, validation, and lineage layer, while BigQuery is organized into clean zones. It realizes UJ-2 and UJ-3.

**Functional Requirements:**

#### FR-11: dbt Transformation Stack

The redesign represents source declarations, source adapters, staging, intermediate logic, final evidence, marts, and tests in dbt.

**Consequences (testable):**
- The dbt project can parse after changes.
- dbt can build selected redesign models in a non-production or versioned target.
- dbt tests cover source keys, grain, placeholders, inference, and metric safety.

#### FR-12: Clean BigQuery Zones

BigQuery storage is organized so object names reveal whether they are raw source, mapping config, staging, intermediate, final, mart, or QA.

**Consequences (testable):**
- Proposed objects use clear lifecycle-zone naming.
- QA artifacts do not blend into published reporting datasets.
- New dbt objects do not replace legacy production objects without approval.

#### FR-13: Published Shortcut Marts

The system may expose package, creative, DMA, and dashboard shortcut marts derived from the universal final evidence table.

**Consequences (testable):**
- Shortcut marts are clearly documented as derived outputs.
- Shortcut marts do not become competing sources of truth.

### 4.5 Evidence, Review, and Handoff

**Description:** The redesign must remain source-backed, auditable, compatible with the current master table, and safe to migrate.

**Functional Requirements:**

#### FR-14: Source-Backed Proof

Before production claims or deployments, the system must verify current live warehouse behavior.

**Consequences (testable):**
- Live BigQuery inspection is used before production behavior claims.
- Local SQL and docs are not treated as proof of current live state.

#### FR-15: Migration-Safe Versioning

The first implementation uses sibling or versioned models until the user approves replacement.

**Consequences (testable):**
- Current production master model and mart remain untouched during design and QA.
- Replacement requires explicit approval and proof.

#### FR-16: Legacy Column and Total Parity

The new version must preserve the current [master table](https://console.cloud.google.com/bigquery?project=looker-studio-pro-452620&p=looker-studio-pro-452620&d=master_stg&t=data_model&page=table) contract unless the user explicitly approves a change.

**Consequences (testable):**
- The implementation pulls a fresh live schema from the current master table at validation time.
- Every current master-table column is included in the new version, with the same name available to downstream users unless a rename is explicitly approved.
- Final totals for the current master-table reporting metrics reconcile to the current master table at validation time.
- Any metric that should not be summed remains visibly marked as doNotSum or blocked.

## 5. Cross-Cutting NFRs

| Area | Requirement |
|---|---|
| Auditability | Final rows must expose source lineage, mapping status, metadata status, and inference evidence where relevant. |
| Beginner readability | Table, field, and dataset names should communicate purpose without requiring tribal knowledge. |
| Backward compatibility | The new version must preserve current master-table columns and reconcile current master-table totals before it can be considered migration-ready. |
| Safety | Metric duplication must be treated as a blocking validation risk. |
| Maintainability | New sources should mostly add source-adapter/mapping configuration, not hand edits to core final-table SQL. |
| Documentation | dbt YAML docs and companion planning docs must explain grain, placeholders, metric safety, and inference behavior. |

## 6. Non-Goals (Explicit)

- This PRD does not replace current production BigQuery objects.
- This PRD does not define final live deployment SQL.
- This PRD does not finalize the source-mapper widget UX.
- This PRD does not require raw vendor ingestion to move into dbt.
- This PRD does not change the Manual Package Editor workflow.
- This PRD does not claim all business logic is grain-agnostic.

## 7. MVP Scope

### 7.1 In Scope

- Universal final evidence table contract.
- Placeholder semantics for unavailable dimensions.
- Fill-blanks-only inferred metadata rules.
- Metric safety classification.
- Source onboarding workflow with source mapper requirement.
- dbt-backed transformation and validation direction.
- Proposed `mdm_*` BigQuery zone structure.
- Versioned or sibling implementation path.
- Legacy column coverage and total reconciliation against the current master table.

### 7.2 Out of Scope for MVP

- Final HTML widget UX and interaction design.
- Final mapping-storage choice.
- Full production replacement.
- Rebuilding upstream raw ingestion.
- Dashboard redesign.
- Manual Package Editor behavior changes.

## 8. Recommended Migration Path

| Step | Goal | Proof Check |
|---|---|---|
| 1. Freeze current behavior | Document current live outputs before changing anything. | Row counts, totals, and known examples match today. |
| 2. Build adapters | Create source translators one at a time. | Each adapter passes schema and fill-rate checks. |
| 3. Build universal detail | Create the shared row shape. | No source rows vanish without an approved reason. |
| 4. Add inferred metadata | Fill only missing metadata. | Inferred flags are visible and totals reconcile. |
| 5. Publish final and views | Expose the master table plus convenience views. | Dashboards can read stable views while analysts can inspect detail. |

## 9. Success Metrics

**Primary**

- **SM-1:** A new source can be profiled, mapped, validated, and represented in the universal final table without editing core final-table SQL by hand. Validates FR-7 through FR-10.
- **SM-2:** Grouping the universal final table by package, creative, or DMA keeps additive metrics stable and repeated context metrics non-summable. Validates FR-1, FR-3, and FR-6.
- **SM-3:** dbt parse, build, and test workflows pass for the redesign stack in a non-production or versioned target. Validates FR-11 and FR-12.
- **SM-4:** The new version has zero missing columns versus the current master table and reconciles approved legacy totals. Validates FR-16.

**Secondary**

- **SM-5:** Reviewers can trace sampled final rows to source evidence, mapping config, and inference status. Validates FR-4, FR-9, and FR-14.

**Counter-metrics (do not optimize)**

- **SM-C1:** Do not optimize for a single very wide table at the cost of metric safety. A readable final table is valuable only if row grain and summability are explicit.
- **SM-C2:** Do not optimize for fully automated mapping if it hides business decisions that require user review.

## 10. Decisions To Lock Next

| Decision | Recommendation | Why It Matters |
|---|---|---|
| Placeholder names | Use readable values like `not_available_at_source`, then decide whether each dimension also needs a dimension-specific value. | Users need obvious values instead of blanks. |
| Metric status fields | Track both value source and summability. Simple meaning: where did the number come from, and can I add it up? | Every number needs to explain itself. |
| Final table name | Pick the stable BI-facing name before dashboards depend on it. | Renaming later creates dashboard churn. |
| Universal field prefix | Use `univ_` for new universal fields. | Prefixes prevent new fields from blending into legacy/source-specific columns. |
| Source mapper storage | Prefer BigQuery config tables in the `mdm_config` zone plus dbt docs; avoid rules living only in code. | Mapping choices need to be reviewable and editable. |

## 11. Open Questions

1. Which exact placeholder strings should be standard beyond the default `not_available_at_source` style?
2. Should final planned fields show selected values only, or should raw actual, inferred, and selected final fields all remain visible?
3. Which dimension slots are required in the first version?
4. What exactly counts as missing: null, blank string, zero, invalid date, unknown label, or some mix?
5. How should source-precedence conflicts work when multiple sources provide valid values?
6. Should mapping configuration live first in BigQuery tables, dbt seeds, checked-in YAML, or a hybrid?
7. Should the source mapper be a local HTML artifact, hosted internal page, or Codex/BMAD-native widget?
8. Should the redesign stay in the existing BigQuery project with new datasets, or use separate dev/QA/prod projects?
9. What exact reconciliation tolerance should be used for floating-point totals: exact cents, one-cent total tolerance, or a percent-based tolerance?

## 12. Assumptions Index

- [ASSUMPTION: The first implementation should be sibling/versioned until replacement is approved.]
- [ASSUMPTION: Actual metadata means trusted source metadata or approved manual metadata.]
- [ASSUMPTION: Delivery-derived planned values are plan-like fallbacks, not original media-plan truth.]
- [ASSUMPTION: dbt should own transformation logic and tests, while non-dbt tools may own UI or source ingestion where appropriate.]
