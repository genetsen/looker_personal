# Story 2.2: Build The Compatibility-First Candidate View

Status: done


## Story

As a dashboard owner,
I want the first candidate view to include every current master-table column,
So that existing reporting fields remain available while new universal fields are added.

## Acceptance Criteria

- [ ] **Given** the live current master-table schema
- [ ] **When** the candidate view is built
- [ ] **Then** every current master-table column exists in the candidate view
- [ ] **And** new universal fields use the `univ_` prefix
- [ ] **And** no current production object is replaced.

## Tasks / Subtasks

- [ ] Task 1: View Generation (AC: 1, 2)
  - [ ] Write a dbt model `mdm_int.int_universal_compat_view` or similar sibling view that references the baseline source but projects every legacy column.
- [ ] Task 2: Universal Field Layering (AC: 3, 4)
  - [ ] Append the prefixed `univ_` fields to this view and ensure it materializes only in the sandboxed redesign workspace without overwriting live objects.

## Dev Notes

- Do not deploy to `master_stg`. Make sure all target schemas use the `mdm_` workspace prefix.
- Keep the view thin and compatible with downstream dashboards.
- **References**: [Source: _bmad-output/planning-artifacts/epics.md#Story 2.2]

## Dev Agent Record

### Agent Model Used

Gemini 3.1 Pro Preview

### Debug Log References

### Completion Notes List

### File List
