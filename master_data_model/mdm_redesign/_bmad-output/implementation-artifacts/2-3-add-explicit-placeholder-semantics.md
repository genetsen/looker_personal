# Story 2.3: Add Explicit Placeholder Semantics

Status: done


## Story

As an analyst,
I want unavailable lower-grain dimensions to use clear placeholders,
So that grouping by creative, DMA, or other dimensions does not silently hide rows.

## Acceptance Criteria

- [ ] **Given** a source does not provide a selected dimension
- [ ] **When** the candidate view fills that dimension
- [ ] **Then** it uses an approved placeholder such as `not_available_at_source`
- [ ] **And** placeholder values are distinguishable from actual unknown source values.

## Tasks / Subtasks

- [ ] Task 1: Placeholders Setup (AC: 1, 2)
  - [ ] Implement conditional checks (`COALESCE` or `IFNULL`) that map missing creative or DMA values to `not_available_at_source`.
- [ ] Task 2: Standardizing Codes (AC: 3, 4)
  - [ ] Ensure that other semantic variants like 'unknown' or 'not applicable' map to their designated clean string representation.

## Dev Notes

- Ensure `not_available_at_source` is used instead of blank or generic `NULL` values.
- **References**: [Source: _bmad-output/planning-artifacts/epics.md#Story 2.3]

## Dev Agent Record

### Agent Model Used

Gemini 3.1 Pro Preview

### Debug Log References

### Completion Notes List

### File List
