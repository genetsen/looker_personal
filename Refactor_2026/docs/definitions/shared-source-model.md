# Shared Source Model

## Plain-English Definition

A shared source model is a cleaned upstream input that more than one client could use before client-specific rules are added.

## Put It In The Shared Layer If

- it is useful before ADIF filtering
- it would still make sense for another client
- it is cleanup, shaping, or normalization rather than business choice

## Keep It Out Of The Shared Layer If

- it filters to De Beers, FMUS, or Forevermark
- it decides ADIF source priority
- it maps ADIF social data
- it shapes the final ADIF output contract

## Fast Test

Ask:

> Would we still want this rule if ADIF did not exist?

If no, it is probably ADIF-specific.

## Examples

Shared:

- broad FPD normalization
- Prisma package-day planning before client filtering
- reusable delivery cleanup

Not shared:

- ADIF source-file filters
- updated-FPD overlay priority
- ADIF social append logic

## Related Docs

- [Phase 01: Shared Source Modeling](../phases/01-shared-source-modeling.md)
- [2026-04-10 Current ADIF Pipeline Inventory](../inventories/2026-04-10-current-adif-pipeline-inventory.md)
