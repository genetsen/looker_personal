---
date: 2026-04-10
topic: adif-dbt-bigquery-cube-rebuild
---

# ADIF dbt + BigQuery + Cube Rebuild

## Current Status

This brainstorm is now a background document, not the main project plan.

The main source of truth is:

- [PLAN.md](../../PLAN.md)

Use `PLAN.md` first when you want:

- the target rebuilt pipeline
- the step-by-step rebuild path
- the current phase sequence

Use this brainstorm only for background context and original reasoning.

## Core Goal

Rebuild the current ADIF pipeline from scratch into `dbt + BigQuery + Cube` while preserving business logic unless a logic change is explicitly approved.

## Architecture Direction

- `dbt` for transformation logic
- `BigQuery` for execution and storage
- `Cube` for the semantic layer

## Rebuild Principles

- separate reusable logic from project-specific logic
- keep a stable final compatibility output
- rebuild in phases instead of one large rewrite
- validate each rebuilt slice against the current pipeline

## Supporting Docs

- Main plan: [PLAN.md](../../PLAN.md)
- Phase 01 support: [01-shared-source-modeling.md](../phases/01-shared-source-modeling.md)
- Shared-source definition: [shared-source-model.md](../definitions/shared-source-model.md)
- Current pipeline inventory: [2026-04-10-current-adif-pipeline-inventory.md](../inventories/2026-04-10-current-adif-pipeline-inventory.md)

## Original Open Questions Still Worth Revisiting

- Should the shared layer live as one dbt project or multiple tightly scoped packages?
- Which debug marts should be official outputs?
- Which common foundation patterns should stay local first versus become a broader shared package later?
