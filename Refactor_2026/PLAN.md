# Plan

We are rebuilding the current ADIF pipeline from scratch into a cleaner `dbt + BigQuery + Cube` structure without changing business logic unless a logic change is explicitly approved.[1] This plan starts by defining the target pipeline shape, then lays out the smallest safe sequence of steps to get from the current pipeline to that target.

## Scope
- In:
  - define the target rebuilt pipeline
  - define the constraints the rebuild must follow
  - define the step-by-step migration plan
  - define what each phase should produce before moving on
- Out:
  - implementing `dbt` models now
  - changing live business logic now
  - changing live warehouse objects now

## Constraints
- Preserve current business logic unless a logic change is explicitly approved.
- Separate reusable logic from project-specific logic.
- Keep a stable final compatibility output for downstream users.
- Work in teachable phases instead of one large rewrite.
- Validate each rebuilt slice against the current pipeline before moving forward.

## Target Pipeline Outline

The rebuilt pipeline should have five layers.

### 1. Sources

Raw warehouse inputs and externally loaded landing tables.

Examples:
- DCM delivery source
- broad FPD landing table
- Prisma planning source
- raw social inputs
- pacing upstreams

### 2. Shared source models

Reusable cleaned upstream models with no ADIF-specific rules.

Examples:
- shared FPD daily source model
- shared Prisma package-day source model
- shared delivery daily source model
- shared social source prep where it is truly reusable

### 3. Common foundation models

Reusable patterns many clients may need, even if they sit inside this repo first.

Examples:
- source stitching pattern
- package-and-date grain shaping
- unmatched-row preservation
- generic source fallback framework

Important rule:
- the pattern can be common
- the exact ADIF version of the pattern can still be project-specific

### 4. ADIF project models

ADIF-only logic built on top of shared and common layers.

Examples:
- De Beers / FMUS source filtering
- Forevermark Prisma filtering
- ADIF package remaps
- ADIF actuals priority
- updated-FPD overlay
- ADIF social mapping and append logic

### 5. Final outputs

Curated ADIF marts plus a final compatibility model and a Cube semantic layer.

Examples:
- final ADIF compatibility table
- official debug marts
- Cube semantic models on top of trusted ADIF marts

## Action items
[ ] Write down the current pipeline in one canonical lineage view so every rebuild step maps back to a current source or rule.
[ ] List the exact current rules that must be preserved, including filters, package remaps, join behavior, and source-priority logic.
[ ] Define the target rebuilt pipeline as five layers: sources, shared source models, common foundation models, ADIF project models, and final outputs.
[ ] Define the contract for shared source models so they stay free of ADIF-specific filters and business decisions.
[ ] Rebuild the digital base first by separating reusable source prep from ADIF-specific stitching rules, then validate old vs new before moving on.
[ ] Rebuild the updated-FPD branch next, keeping the current date-spreading and overlay behavior unchanged unless approved otherwise.
[ ] Rebuild the social branch after the digital base is trusted, keeping ADIF social mapping and pacing logic isolated in project-specific models.
[ ] Build the final compatibility model only after the upstream rebuilt slices match the current pipeline closely enough to trust.
[ ] Add tests and comparison checks for every phase so each rebuilt slice can be proven against the current output before the next phase begins.
[ ] Add the Cube layer last, after the ADIF marts and compatibility model are stable.

## Phase-by-phase Rebuild Sequence

### Phase 01: Plan and boundaries

Output:
- clean target pipeline outline
- clean migration plan
- preserved-logic checklist

Success gate:
- we can explain what belongs in each future layer

### Phase 02: Shared digital inputs and ADIF digital base

Output:
- shared FPD source model
- shared Prisma source model
- shared delivery source model
- ADIF digital base model

Success gate:
- rebuilt digital base matches current digital behavior closely enough to trust

### Phase 03: Updated-FPD overlay

Output:
- rebuilt updated-FPD daily model
- rebuilt ADIF overlay model

Success gate:
- overlay behavior matches the current updated-FPD branch

### Phase 04: Social branch

Output:
- rebuilt ADIF social mapping models
- rebuilt ADIF social append model

Success gate:
- social rows and pacing behavior match the current branch

### Phase 05: Final compatibility model

Output:
- trusted ADIF final mart
- compatibility output that preserves the current downstream contract

Success gate:
- downstream-facing schema and key behavior are stable

### Phase 06: Cube semantic layer

Output:
- Cube semantic definitions on top of trusted ADIF marts

Success gate:
- semantic layer points only at trusted rebuilt outputs

## Validation Rules

For every rebuild phase:

- compare rebuilt outputs to the current pipeline
- inspect row counts, date ranges, and key totals
- check schema compatibility where needed
- document any logic difference before changing behavior

## Open questions
- Which exact final table should be treated as the long-term compatibility target?
- Which common foundation patterns should stay in this repo first versus become a future shared package later?
- Which debug marts are worth formalizing as official outputs instead of temporary QA tables?

[1] `dbt` is a data transformation tool that turns pipeline logic into named models with tests and documentation. In plain English, it helps us rebuild a messy SQL pipeline into smaller, clearer steps.
