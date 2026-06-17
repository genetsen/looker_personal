# Story 5.1: Define The Published Output Contract

Status: done


## Story

As a dashboard owner,
I want the candidate publish layer to declare which views and marts are safe to use,
So that reporting teams know which objects are stable shortcuts and which object is the source of truth.

## Acceptance Criteria

- [ ] **Given** the candidate universal table exists
- [ ] **When** the publish contract is defined
- [ ] **Then** it names the stable BI-facing view, package shortcut mart, creative shortcut mart, DMA shortcut mart, and any dashboard-specific view
- [ ] **And** each published object identifies its intended grain, source table, and whether it is a shortcut or source-of-truth object.

## Tasks / Subtasks

- [ ] Task 1: Publish Contract Manifest (AC: 1, 2)
  - [ ] Create a published schema registry (or dbt schema.yml configuration) detailing each BI-facing output.
- [ ] Task 2: Metadata Documentation (AC: 3, 4)
  - [ ] Add explicit BigQuery descriptions and markdown metadata documenting intended grain and source tables.

## Dev Notes

- Enforce shared BigQuery object hygiene rules: metadata must explain ownership and cleanup conditions.
- **References**: [Source: _bmad-output/planning-artifacts/epics.md#Story 5.1]

## Dev Agent Record

### Agent Model Used

Gemini 3.1 Pro Preview

### Debug Log References

### Completion Notes List

### File List
