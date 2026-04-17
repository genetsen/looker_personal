# Phase 01: Shared Source Modeling

- Status: `defined`
- Brainstorm source: [2026-04-10-adif-dbt-bigquery-cube-rebuild-brainstorm.md](../brainstorms/2026-04-10-adif-dbt-bigquery-cube-rebuild-brainstorm.md)

## Goal

Decide which current pipeline pieces belong in the future shared layer and which pieces must stay in the ADIF layer.

This phase is documentation only. We are not building `dbt` models yet.[1]

## Simple Answer

### Shared layer

Put reusable upstream building blocks here:

- broad FPD loader output before ADIF filtering
- Prisma planning data before ADIF filtering
- delivery-source cleanup only where it is truly reusable
- raw social and pacing upstreams before ADIF mapping

### ADIF layer

Keep ADIF-only business rules here:

- De Beers / FMUS source filtering
- Forevermark Prisma filtering
- ADIF package remaps and join behavior
- actuals precedence
- updated-FPD overlay
- social mapping and social append logic
- final compatibility output

## Scope

### In scope

- inventory the current pipeline
- mark each major step as shared or ADIF-specific
- define what Phase 02 should build next

### Out of scope

- writing `dbt` code
- changing business logic
- changing live BigQuery objects

## Source Inputs

Reviewed live objects:

- `landing.fpd_data_ranged_shortcutsFolder`
- `landing.adif_updated_fpd_daily`
- `repo_stg.adif__prisma_expanded_plus_dcm_view_v3_test`
- `repo_stg.adif__prisma_expanded_plus_dcm_updated_fpd_view`

Reviewed docs:

- `../../AGENTS.md`
- `../brainstorms/2026-04-10-adif-dbt-bigquery-cube-rebuild-brainstorm.md`
- `../../adif/projects/tv_digital_pipeline/README - ADIF TV & Digital Data Pipeline.md`
- `../../adif/projects/updated_fpd_integration/README_Updated_FPD_Integration.md`
- `../../adif/projects/social_layering/README.md`
- `../../adif/projects/social_layering/ADIF_MAIN_PIPELINE_LINEAGE_1PAGER.md`
- `../../util/data_loaders/FPD_loader/README.md`

## Outputs

This phase now produces:

1. A simple boundary definition: [shared-source-model.md](../definitions/shared-source-model.md)
2. A simple pipeline inventory: [2026-04-10-current-adif-pipeline-inventory.md](../inventories/2026-04-10-current-adif-pipeline-inventory.md)
3. An updated brainstorm that links to both docs

## Logic To Preserve

Do not change these behaviors in Phase 01:

1. The broad FPD landing table stays broad, not ADIF-only.
2. ADIF still filters original FPD by De Beers / FMUS sheet rules.
3. ADIF still filters Prisma to Forevermark and excludes child packages.
4. Actuals priority stays:
   1. updated FPD
   2. original FPD
   3. DCM
5. Updated FPD and social logic stay outside this phase.

## Validation Plan

Phase 01 is complete enough to move forward when:

- the shared-vs-ADIF boundary is written down clearly
- each major current pipeline step has a home
- the brainstorm links to the new source-of-truth docs
- we still have not changed any business logic

## Learning Notes

The key lesson in this phase is simple:

Do not put ADIF-specific rules into the shared layer just because they appear early in the current pipeline.

Good test:

> If another client used this same upstream source, would we still want this exact rule?

If the answer is no, it should stay in the ADIF layer.

## Linked Follow-On Docs

- [shared-source-model.md](../definitions/shared-source-model.md)
- [2026-04-10-current-adif-pipeline-inventory.md](../inventories/2026-04-10-current-adif-pipeline-inventory.md)
- [02-digital-base-rebuild.md](./02-digital-base-rebuild.md)
- [03-updated-fpd-overlay.md](./03-updated-fpd-overlay.md)
- [04-social-branch-rebuild.md](./04-social-branch-rebuild.md)

## Current Status

Phase 01 is `defined`, not `done`.

That means:

- the docs are in place
- the boundary is decided at a high level
- implementation has not started yet

[1] `dbt` is a tool for organizing data transformation logic into named, testable models. In plain English, it helps us rebuild messy pipeline logic into cleaner building blocks.
