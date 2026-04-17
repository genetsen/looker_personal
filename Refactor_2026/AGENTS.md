# AGENTS.md

Operational rules for `/Users/eugenetsenter/Looker_clonedRepo/looker_personal/Refactor_2026`.

## Project Goal

This project rebuilds the ADIF pipeline as a learning-oriented refactor using `dbt + BigQuery + Cube` while preserving existing business logic unless a logic change is explicitly approved.

The target architecture is:

- a shared cross-client foundation layer
- an ADIF-specific client layer
- a Cube semantic layer on top of curated ADIF marts

## Working Principles

- Treat this as both a production-minded refactor and a teaching project.
- Prefer phased replacement by teachable slice over large rewrites.
- Keep logic changes out of scope unless explicitly approved.
- Separate shared cross-client logic from ADIF-only logic.
- Keep a stable final compatibility contract for downstream users.

## Documentation Contract

Every meaningful step in this refactor must be documented and linked forward.

This rule applies to all open-ended portions of any document, not only phase trackers.

This means:

1. The brainstorm document is the starting map.
2. Each planned phase must have its own phase document in `docs/phases/`.
3. When a phase is defined, the brainstorm document must link to that phase document.
4. When a phase is completed, the brainstorm document entry must be updated to include `(done)` and keep a link to the document where the work is defined.
5. Every phase document must link back to the brainstorm and to any related downstream design, implementation, validation, or learning notes.
6. If a concept becomes important enough to deserve its own definition, create a document in `docs/definitions/` and link to it from the phase document that introduced it.
7. Any open-ended section, subsection, bullet, checklist item, question, placeholder, or future-work note must eventually resolve to a linked source of truth.
8. When an open-ended portion is resolved, update the original document in place so the reader can see both the resolved status and the link to the canonical follow-up document.
9. Do not leave `TBD`, `pending`, or loosely worded future intent in a canonical doc once that topic has been formally defined elsewhere; replace it with a status marker and link.

## Canonical Docs

- Brainstorm:
  - `docs/brainstorms/2026-04-10-adif-dbt-bigquery-cube-rebuild-brainstorm.md`
- Phase docs:
  - `docs/phases/01-shared-source-modeling.md`
  - `docs/phases/02-digital-base-rebuild.md`
  - `docs/phases/03-updated-fpd-overlay.md`
  - `docs/phases/04-social-branch-rebuild.md`
  - `docs/phases/05-final-compatibility-model.md`
  - `docs/phases/06-cube-semantic-layer.md`

## Required Phase Document Sections

Every phase document should include:

- goal
- scope
- source inputs
- outputs
- logic to preserve
- validation plan
- learning notes
- linked follow-on docs
- current status

## Status Language

Use these exact status words:

- `pending`
- `in_progress`
- `defined`
- `done`

Only mark a phase as `done` when:

- the scope is documented
- the implementation artifact exists
- validation is documented
- the learning notes exist
- the brainstorm document has been updated with `(done)` and a link

## Change Discipline

- Keep changes small and traceable.
- Do not quietly move logic between shared and ADIF layers without documenting why.
- If a naming decision changes the internal structure, document the old name, the new name, and why.
- If a logic difference from the current production pipeline is discovered, stop and document it before changing behavior.
- If a document contains an open-ended portion, treat closing that loop with a linked follow-up as part of the work, not as optional cleanup.

## Communication Style for This Project

- Explain new architecture choices in plain English.
- Add beginner-friendly explanations when introducing dbt, semantic-layer, lineage, or modeling terms.
- Prefer docs that help a future reader understand both what changed and why it changed.
