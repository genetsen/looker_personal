# Story 2.4: Prove A Pilot Lower-Grain Slice

Status: done


## Story

As a model owner,
I want one pilot lower-grain slice, likely DCM creative detail,
So that we can prove the flexible-grain idea before onboarding every source.

## Acceptance Criteria

- [ ] **Given** a pilot source with lower-grain detail exists
- [ ] **When** the candidate represents that detail
- [ ] **Then** rows keep enough identity to avoid package/placement/date collisions
- [ ] **And** package totals are not exposed as normal additive metrics on lower-grain rows
- [ ] **And** parity checks from Epic 1 still pass for the compatibility layer.

## Tasks / Subtasks

- [ ] Task 1: Pilot Lower-Grain Adapter (AC: 1, 2)
  - [ ] Implement a pilot adapter for DCM creative-grain detail rows.
  - [ ] Generate unique composite keys using package, placement, creative, and date to avoid collisions.
- [ ] Task 2: Metric Safety & Parity Checks (AC: 3, 4, 5)
  - [ ] Mark package totals on creative-detail rows as `doNotSum` and hide them from ordinary additive expressions.
  - [ ] Re-run schema parity and totals reconciliation to prove the compatibility layer is unaffected by adding these detailed rows.

## Dev Notes

- Avoid siloing or flattening lower-grain rows into package totals.
- Treat duplicate metrics on lower-grain rows as a critical error.
- **References**: [Source: _bmad-output/planning-artifacts/epics.md#Story 2.4]

## Dev Agent Record

### Agent Model Used

Gemini 3.1 Pro Preview

### Debug Log References

### Completion Notes List

### File List
