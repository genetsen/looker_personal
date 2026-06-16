---
stepsCompleted:
  - step-01-requirements-extracted
  - step-02-epics-approved
  - step-03-stories-generated
  - step-04-final-validation-complete
  - workflow-complete
inputDocuments:
  - /Users/eugenetsenter/Looker_clonedRepo/looker_personal/master_data_model/_bmad-output/planning-artifacts/prds/prd-master_data_model-2026-06-15/prd.md
  - /Users/eugenetsenter/Looker_clonedRepo/looker_personal/master_data_model/_bmad-output/planning-artifacts/prds/prd-master_data_model-2026-06-15/addendum.md
  - /Users/eugenetsenter/Looker_clonedRepo/looker_personal/master_data_model/_bmad-output/specs/spec-universal-final-evidence-table/SPEC.md
  - /Users/eugenetsenter/Looker_clonedRepo/looker_personal/master_data_model/_bmad-output/specs/spec-universal-final-evidence-table/field-contract.md
  - /Users/eugenetsenter/Looker_clonedRepo/looker_personal/master_data_model/_bmad-output/specs/spec-universal-final-evidence-table/source-onboarding-contract.md
  - /Users/eugenetsenter/Looker_clonedRepo/looker_personal/master_data_model/_bmad-output/specs/spec-universal-final-evidence-table/source-mapper-contract.md
  - /Users/eugenetsenter/Looker_clonedRepo/looker_personal/master_data_model/_bmad-output/specs/spec-universal-final-evidence-table/dbt-bigquery-structure.md
  - /Users/eugenetsenter/Looker_clonedRepo/looker_personal/master_data_model/_bmad-output/specs/spec-universal-final-evidence-table/architecture-diagrams.md
  - /Users/eugenetsenter/Looker_clonedRepo/looker_personal/master_data_model/_bmad-output/specs/spec-universal-final-evidence-table/brownfield-evidence.md
workflowState: complete_ready_for_development
---

# master_data_model - Epic Breakdown

## Overview

This document provides the complete epic and story breakdown for `master_data_model`, decomposing the requirements from the PRD, technical addendum, and SPEC companions into implementable stories.

## Requirements Inventory

### Functional Requirements

FR1: The model owner can query one universal final evidence table across multiple grains.

FR2: The final table uses standardized placeholders when a source does not provide a selected dimension.

FR3: The final table keeps each source at its natural useful grain unless an approved allocation or rollup rule changes it.

FR4: The model can infer metadata from delivery evidence only when actual or approved manual metadata is missing.

FR5: When actual values are missing, the model can infer flight start, flight end, planned spend, and planned impressions from delivery evidence.

FR6: Every exposed metric explains both where the value came from and whether it is safe to sum.

FR7: The source onboarding skill can inspect a source and summarize candidate grain, dimensions, metrics, source lineage, and risks.

FR8: The user can launch an HTML mapping widget from the onboarding skill to review and edit mappings.

FR9: The mapper saves approved mapping decisions outside core model SQL.

FR10: The source mapper can run validation checks before a mapping affects final outputs.

FR11: The redesign represents source declarations, source adapters, staging, intermediate logic, final evidence, marts, and tests in dbt.

FR12: BigQuery storage is organized so object names reveal whether they are raw source, mapping config, staging, intermediate, final, mart, or QA.

FR13: The system may expose package, creative, DMA, and dashboard shortcut marts derived from the universal final evidence table.

FR14: Before production claims or deployments, the system must verify current live warehouse behavior.

FR15: The first implementation uses sibling or versioned models until the user approves replacement.

FR16: The new version must preserve the current [master table](https://console.cloud.google.com/bigquery?project=looker-studio-pro-452620&p=looker-studio-pro-452620&d=master_stg&t=data_model&page=table) contract unless the user explicitly approves a change.

### NonFunctional Requirements

NFR1: Final rows must expose source lineage, mapping status, metadata status, and inference evidence where relevant.

NFR2: Table, field, and dataset names should communicate purpose without requiring tribal knowledge.

NFR3: The new version must preserve current master-table columns and reconcile current master-table totals before it can be considered migration-ready.

NFR4: Metric duplication must be treated as a blocking validation risk.

NFR5: New sources should mostly add source-adapter or mapping configuration, not hand edits to core final-table SQL.

NFR6: dbt YAML docs and companion planning docs must explain grain, placeholders, metric safety, and inference behavior.

NFR7: Current production master model and mart must remain untouched during design and QA unless the user explicitly approves replacement.

NFR8: Live BigQuery inspection must be used before production behavior claims; local SQL and docs are not proof of current live state.

### Additional Requirements

- The universal final evidence table must include prefixed row identity fields such as `univ_source_system`, `univ_source_row_id`, `univ_row_grain`, `univ_source_lineage`, and `univ_record_date`.
- New universal fields must use the agreed `univ_` prefix, producing fields such as `univ_source_system`, `univ_source_row_id`, `univ_row_grain`, `univ_source_lineage`, and `univ_record_date`.
- The selected dimension slots should cover package, placement, ad, creative, DMA, campaign, client or advertiser, supplier or site, and date where available.
- Placeholder rules should distinguish `not_available_at_source`, `unknown_from_source`, and `not_applicable_to_source`.
- Metadata priority should be actual source metadata, approved manual metadata, high-confidence inferred metadata, lower-confidence inferred metadata, then placeholder.
- Inferred metadata must expose audit fields for metadata status, field source, reason, evidence, and confidence.
- Flight start and flight end fallback rules use minimum and maximum delivery dates only when actual values are missing.
- Planned spend and planned impressions fallback rules use total observed delivery spend and impressions only when actual planned values are missing.
- Metric rules must track value status such as direct, inferred, allocated, or unavailable.
- Metric rules must track summability such as additive, doNotSum, or blocked.
- Source onboarding must identify grain, map dimensions, map metrics, define metadata inference, define source precedence, configure mapping, preserve lineage, validate rollups, and validate legacy parity.
- Adapter output must include identity, time, dimensions, metrics, metadata status, QA labels, and mapper metadata.
- The source mapper skill must support source discovery, mapping proposal, metric status and safety, inferred metadata choices, validation, and saving mappings.
- Mapping configuration should be versionable and machine-readable, preferably through BigQuery `mdm_config` tables plus dbt documentation.
- dbt layers should include sources, base, staging, mapping config, intermediate, final fact, marts, and tests.
- Proposed BigQuery zones are [mdm_raw](https://console.cloud.google.com/bigquery?project=looker-studio-pro-452620&ws=!1m4!1m3!3m2!1slooker-studio-pro-452620!2smdm_raw), [mdm_config](https://console.cloud.google.com/bigquery?project=looker-studio-pro-452620&ws=!1m4!1m3!3m2!1slooker-studio-pro-452620!2smdm_config), [mdm_stg](https://console.cloud.google.com/bigquery?project=looker-studio-pro-452620&ws=!1m4!1m3!3m2!1slooker-studio-pro-452620!2smdm_stg), [mdm_int](https://console.cloud.google.com/bigquery?project=looker-studio-pro-452620&ws=!1m4!1m3!3m2!1slooker-studio-pro-452620!2smdm_int), [mdm_qa](https://console.cloud.google.com/bigquery?project=looker-studio-pro-452620&ws=!1m4!1m3!3m2!1slooker-studio-pro-452620!2smdm_qa), [mdm_mart](https://console.cloud.google.com/bigquery?project=looker-studio-pro-452620&ws=!1m4!1m3!3m2!1slooker-studio-pro-452620!2smdm_mart), [mdm_publish](https://console.cloud.google.com/bigquery?project=looker-studio-pro-452620&ws=!1m4!1m3!3m2!1slooker-studio-pro-452620!2smdm_publish), and [mdm_sandbox](https://console.cloud.google.com/bigquery?project=looker-studio-pro-452620&ws=!1m4!1m3!3m2!1slooker-studio-pro-452620!2smdm_sandbox).
- Suggested core config and output objects include source field mappings, dimension placeholders, metadata inference rules, metric rules, a universal evidence fact, a stable BI view, and package, creative, and DMA shortcut marts.
- Validation must cover source grain, legacy column coverage, legacy total reconciliation, placeholder safety, inference safety, additive metric safety, doNotSum safety, and mapping approval.
- Legacy column coverage must compare the candidate version to the live [master table](https://console.cloud.google.com/bigquery?project=looker-studio-pro-452620&p=looker-studio-pro-452620&d=master_stg&t=data_model&page=table) schema at validation time.
- Legacy total reconciliation must compare approved candidate totals to fresh totals from the live [master table](https://console.cloud.google.com/bigquery?project=looker-studio-pro-452620&p=looker-studio-pro-452620&d=master_stg&t=data_model&page=table).
- doNotSum fields must remain doNotSum or blocked when lower-grain rows are added.
- The migration path should freeze current behavior, build adapters, build universal detail, add inferred metadata, then publish final and convenience views.
- Brownfield DCM evidence shows reusable pricing math, package-centered allocation, repeated package totals on creative rows, package/date daily share logic, and detail identity issues around package/placement/date.

### UX Design Requirements

No standalone UX design document was found. The source mapper still has broad UX requirements from the PRD: show source fields and sample values, allow the user to accept/edit/reject/add mappings, highlight grain and metric-safety warnings, display validation status, and record reviewer-visible choices. Detailed widget layout and interaction design remain deferred.

### FR Coverage Map

FR1: Epic 2 - Readable Universal Evidence Table supports package, creative, DMA, source, campaign, and date groupings with prefixed universal fields.

FR2: Epic 2 - Readable Universal Evidence Table uses explicit placeholders for unavailable dimensions.

FR3: Epic 2 - Readable Universal Evidence Table preserves natural source grain unless an approved rollup or allocation changes it.

FR4: Epic 3 - Auditable Metadata and Metric Safety fills only missing metadata and preserves actual/manual values.

FR5: Epic 3 - Auditable Metadata and Metric Safety adds delivery-derived fallback rules for missing flight dates and plan-like totals.

FR6: Epic 3 - Auditable Metadata and Metric Safety marks metric value source and summability.

FR7: Epic 4 - Guided Source Onboarding and Mapper profiles candidate source grain, dimensions, metrics, lineage, and risks.

FR8: Epic 4 - Guided Source Onboarding and Mapper launches an HTML mapping widget for review and editing.

FR9: Epic 4 - Guided Source Onboarding and Mapper stores versioned mapping decisions outside core model SQL.

FR10: Epic 4 - Guided Source Onboarding and Mapper validates mappings before they affect final outputs.

FR11: Epic 1 - Safe Versioned Redesign Workspace and Parity Baseline establishes the dbt transformation stack.

FR12: Epic 1 - Safe Versioned Redesign Workspace and Parity Baseline establishes clean `mdm_*` BigQuery zones.

FR13: Epic 5 - Dashboard-Safe Publishing and Migration Readiness exposes shortcut marts and stable BI views.

FR14: Epic 1 - Safe Versioned Redesign Workspace and Parity Baseline requires source-backed live proof before production claims.

FR15: Epic 1 - Safe Versioned Redesign Workspace and Parity Baseline keeps the first implementation sibling/versioned until replacement is approved.

FR16: Epic 1 establishes the live baseline and Epic 5 rechecks it before publishing. The candidate must preserve current columns and reconcile approved totals against the live master table.

## Epic List

### Epic 1: Safe Versioned Redesign Workspace and Parity Baseline

Gene can build and test the redesign in a versioned dbt/BigQuery workspace without touching current production, while capturing fresh live baselines for current columns, totals, metric semantics, and doNotSum fields from the current master table.

**FRs covered:** FR11, FR12, FR14, FR15, FR16

**Implementation notes:** This epic should establish the safe working area, source declarations, naming pattern, and validation baseline before any candidate table is trusted.

### Story 1.1: Create The Versioned Redesign Workspace

As a model owner,
I want a clearly separated dbt and BigQuery workspace,
So that the redesign can be built without touching current production outputs.

**Acceptance Criteria:**

**Given** the current production model exists
**When** the redesign workspace is created
**Then** new work uses versioned or sandbox locations only
**And** no current production table, view, dashboard-facing object, or loader is replaced.

### Story 1.2: Capture The Live Master Baseline

As a model owner,
I want a fresh baseline from the current master table,
So that the candidate model can prove it kept today's columns and totals.

**Acceptance Criteria:**

**Given** the live [master table](https://console.cloud.google.com/bigquery?project=looker-studio-pro-452620&p=looker-studio-pro-452620&d=master_stg&t=data_model&page=table)
**When** the baseline job runs
**Then** it captures current column names, data types, key totals, row count, package count, and doNotSum fields
**And** the baseline is marked with the run timestamp.

### Story 1.3: Add Candidate Parity Checks

As a reviewer,
I want automated checks comparing the candidate to the current master table,
So that missing columns or shifted totals block migration.

**Acceptance Criteria:**

**Given** a candidate universal table exists
**When** parity checks run
**Then** the checks fail if any current master-table column is missing
**And** the checks fail if approved totals do not reconcile within the agreed tolerance
**And** doNotSum/context fields remain visibly non-additive.

### Story 1.4: Publish A Readable Proof Report

As a dashboard owner,
I want a readable proof summary for the candidate,
So that I can see whether it is safe to inspect before dashboard migration.

**Acceptance Criteria:**

**Given** the baseline and parity checks have run
**When** the proof report is generated
**Then** it shows pass/fail status for schema coverage, total reconciliation, doNotSum safety, and production isolation
**And** failures include enough detail to know what must be fixed.

### Epic 2: Readable Universal Evidence Table

Analysts can query one candidate final table by package, creative, DMA, source, campaign, or date without losing rows, while all current master-table columns remain available and new universal fields use an agreed prefix.

**FRs covered:** FR1, FR2, FR3

**Implementation notes:** This epic should prove the universal row contract and field organization, including legacy columns, prefixed universal fields, placeholder fields, and source lineage.

### Story 2.1: Define The Universal Row Contract

As an analyst,
I want the candidate table to declare its row meaning with prefixed universal fields,
So that I can understand what each row represents before grouping or summing.

**Acceptance Criteria:**

**Given** the candidate universal table is being designed
**When** the row contract is defined
**Then** it includes `univ_row_grain`, `univ_source_system`, `univ_source_row_id`, `univ_source_lineage`, and `univ_record_date`
**And** existing legacy columns keep their current names.

### Story 2.2: Build The Compatibility-First Candidate View

As a dashboard owner,
I want the first candidate view to include every current master-table column,
So that existing reporting fields remain available while new universal fields are added.

**Acceptance Criteria:**

**Given** the live current master-table schema
**When** the candidate view is built
**Then** every current master-table column exists in the candidate view
**And** new universal fields use the `univ_` prefix
**And** no current production object is replaced.

### Story 2.3: Add Explicit Placeholder Semantics

As an analyst,
I want unavailable lower-grain dimensions to use clear placeholders,
So that grouping by creative, DMA, or other dimensions does not silently hide rows.

**Acceptance Criteria:**

**Given** a source does not provide a selected dimension
**When** the candidate view fills that dimension
**Then** it uses an approved placeholder such as `not_available_at_source`
**And** placeholder values are distinguishable from actual unknown source values.

### Story 2.4: Prove A Pilot Lower-Grain Slice

As a model owner,
I want one pilot lower-grain slice, likely DCM creative detail,
So that we can prove the flexible-grain idea before onboarding every source.

**Acceptance Criteria:**

**Given** a pilot source with lower-grain detail exists
**When** the candidate represents that detail
**Then** rows keep enough identity to avoid package/placement/date collisions
**And** package totals are not exposed as normal additive metrics on lower-grain rows
**And** parity checks from Epic 1 still pass for the compatibility layer.

### Epic 3: Auditable Metadata and Metric Safety

Reviewers can tell which values are actual, manual, inferred, allocated, unavailable, additive, doNotSum, or blocked before the candidate is trusted for reporting.

**FRs covered:** FR4, FR5, FR6

**Implementation notes:** This epic should make inferred metadata and metric safety visible enough that lower-grain detail cannot silently inflate package totals.

### Story 3.1: Add Metadata Source Fields

As a reviewer,
I want metadata fields to explain where their selected values came from,
So that inferred values are not mistaken for actual source metadata.

**Acceptance Criteria:**

**Given** candidate metadata fields exist
**When** metadata is selected for final output
**Then** each governed metadata field can identify whether it is actual, manual, inferred, mixed, or placeholder
**And** actual or approved manual metadata is not overwritten by inferred metadata.

### Story 3.2: Add Delivery-Derived Fallback Rules

As a model owner,
I want missing flight dates and plan-like values to be inferred only from delivery evidence,
So that rows without actual metadata still have useful backup labels.

**Acceptance Criteria:**

**Given** actual metadata is missing
**When** fallback rules run
**Then** flight start uses minimum delivery date
**And** flight end uses maximum delivery date
**And** planned spend uses total observed delivery spend only when actual planned spend is missing
**And** planned impressions uses total observed delivery impressions only when actual planned impressions is missing.

### Story 3.3: Add Metric Value Status and Summability

As an analyst,
I want every metric to say where it came from and whether I can sum it,
So that I do not accidentally inflate totals when grouping lower-grain rows.

**Acceptance Criteria:**

**Given** a metric is exposed in the candidate table
**When** metric rules are applied
**Then** the metric has a value-status meaning such as direct, inferred, allocated, or unavailable
**And** the metric has a summability meaning such as additive, doNotSum, or blocked
**And** repeated package/context totals are not exposed as normal additive metrics.

### Story 3.4: Add Metric Safety QA Checks

As a reviewer,
I want QA checks that catch likely metric duplication,
So that lower-grain rows cannot silently inflate package-level totals.

**Acceptance Criteria:**

**Given** the candidate includes lower-grain rows or repeated context totals
**When** metric safety checks run
**Then** checks identify metrics that are unsafe to sum
**And** checks fail if package/context totals appear as ordinary additive metrics
**And** the proof output explains which metric and grain caused the failure.

### Epic 4: Guided Source Onboarding and Mapper

Source integrators can inspect a new source, configure mappings, validate grain and metric risks with examples, and save approved rules outside core final-table SQL.

**FRs covered:** FR7, FR8, FR9, FR10

**Implementation notes:** This epic should avoid a field-name-only mapper. Validation examples should show source rows, current output, proposed output, and why the proposed output is safer.

### Story 4.1: Profile A New Source Before Mapping

As a source integrator,
I want the onboarding skill to profile a source before any mapping is saved,
So that I can understand the source grain, useful fields, metrics, lineage, and risks first.

**Acceptance Criteria:**

**Given** a candidate source table or view
**When** the onboarding skill profiles it
**Then** the output identifies likely row grain, candidate keys, date fields, dimensions, metrics, source lineage fields, and obvious data-quality risks
**And** the profiling step does not change core model SQL or final outputs.

### Story 4.2: Propose Source Field And Metric Mappings

As a source integrator,
I want the onboarding skill to propose mappings into the universal contract,
So that I have a useful first draft before manual review.

**Acceptance Criteria:**

**Given** a profiled source
**When** mapping proposal runs
**Then** it proposes source-to-target field mappings, placeholder candidates, metadata inference candidates, metric value status, and metric summability
**And** the proposal includes example source rows with the proposed output shape.

### Story 4.3: Launch Interactive Mapping Review Widget

As a reviewer,
I want an HTML mapping widget for accepting, editing, rejecting, or adding mappings,
So that source onboarding can be configured without hand-editing core SQL.

**Acceptance Criteria:**

**Given** a mapping proposal exists
**When** the reviewer launches the mapping widget
**Then** the widget shows source fields, sample values, proposed target fields, metric safety labels, and grain warnings
**And** the reviewer can accept, edit, reject, or add mapping decisions before anything is promoted.

### Story 4.4: Save Versioned Mapping Configuration

As a model owner,
I want approved mapping decisions saved outside core final-table SQL,
So that new sources can be added by configuration instead of rewriting the model.

**Acceptance Criteria:**

**Given** a reviewer has approved mapping choices
**When** the mapping configuration is saved
**Then** the saved configuration includes source name, version, reviewer-visible status, source fields, target fields, placeholder rules, metric rules, inference rules, and timestamp
**And** the configuration is machine-readable and suitable for dbt model generation or dbt model inputs.

### Story 4.5: Validate Mapping Before Promotion

As a reviewer,
I want mapping validation to run before a source can affect candidate final outputs,
So that bad grain, duplicate metrics, missing placeholders, or broken parity are caught early.

**Acceptance Criteria:**

**Given** a saved mapping configuration exists
**When** validation runs
**Then** it checks schema coverage, source grain, placeholder safety, metadata inference safety, additive metric safety, source-total reconciliation, and legacy parity impact
**And** failed validation blocks the mapping from approved promotion status until the issue is resolved.

### Epic 5: Dashboard-Safe Publishing and Migration Readiness

Dashboard owners can use stable shortcut views and marts only after side-by-side totals, column coverage, semantic parity, and rollback expectations are proven.

**FRs covered:** FR13 plus final proof from FR14, FR15, and FR16

**Implementation notes:** This epic should treat publishing as a trust workflow, not just a final view creation task. It should verify compatibility with current dashboard-facing behavior before replacement is considered.

### Story 5.1: Define The Published Output Contract

As a dashboard owner,
I want the candidate publish layer to declare which views and marts are safe to use,
So that reporting teams know which objects are stable shortcuts and which object is the source of truth.

**Acceptance Criteria:**

**Given** the candidate universal table exists
**When** the publish contract is defined
**Then** it names the stable BI-facing view, package shortcut mart, creative shortcut mart, DMA shortcut mart, and any dashboard-specific view
**And** each published object identifies its intended grain, source table, and whether it is a shortcut or source-of-truth object.

### Story 5.2: Build Shortcut Marts From The Universal Table

As an analyst,
I want package, creative, DMA, and dashboard-friendly shortcut marts,
So that common reporting questions are easier without hiding the universal evidence table.

**Acceptance Criteria:**

**Given** the universal evidence table has passed basic parity checks
**When** shortcut marts are built
**Then** each mart is derived from the universal evidence table
**And** each mart preserves required legacy columns or clearly documents fields that are intentionally not available at that shortcut grain
**And** no shortcut mart replaces the current production master table or mart.

### Story 5.3: Run Side-By-Side Parity Checks

As a reviewer,
I want the candidate publish layer compared side by side with the current master table,
So that dashboard migration cannot happen with missing columns or shifted totals.

**Acceptance Criteria:**

**Given** the candidate publish objects exist
**When** side-by-side validation runs
**Then** it compares candidate columns to the fresh live master-table schema
**And** it compares approved totals to fresh live master-table totals
**And** missing legacy columns, unexpected total differences, or unsafe metric changes block migration readiness.

### Story 5.4: Publish A Migration Readiness Report

As a model owner,
I want a readable migration readiness report,
So that business reviewers can understand what is safe, what changed, and what still needs work.

**Acceptance Criteria:**

**Given** publish-layer checks have run
**When** the readiness report is generated
**Then** it shows schema coverage, total reconciliation, shortcut mart status, metric safety status, known differences, and open blockers
**And** it explains failures in plain language with enough detail for the next fix.

### Story 5.5: Require Approval Before Production Replacement

As a dashboard owner,
I want explicit approval and rollback expectations before any production replacement,
So that existing dashboards are not moved to the candidate model accidentally.

**Acceptance Criteria:**

**Given** the candidate is ready for migration review
**When** production replacement is considered
**Then** the current production model remains untouched until the user explicitly approves replacement
**And** the handoff includes the approved replacement target, rollback expectation, validation evidence, and any dashboard retesting required.
