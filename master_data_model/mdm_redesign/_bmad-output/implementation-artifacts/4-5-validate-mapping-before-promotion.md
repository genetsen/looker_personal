# Story 4.5: Validate Mapping Before Promotion

Status: done


## Story

As a reviewer,
I want mapping validation to run before a source can affect candidate final outputs,
So that bad grain, duplicate metrics, missing placeholders, or broken parity are caught early.

## Acceptance Criteria

- [ ] **Given** a saved mapping configuration exists
- [ ] **When** validation runs
- [ ] **Then** it checks schema coverage, source grain, placeholder safety, metadata inference safety, additive metric safety, source-total reconciliation, and legacy parity impact
- [ ] **And** failed validation blocks the mapping from approved promotion status until the issue is resolved.

## Tasks / Subtasks

- [ ] Task 1: Mapping Validator Routine (AC: 1, 2)
  - [ ] Implement validator routines checking schema coverage, placeholder safety, and metric sum safety.
- [ ] Task 2: Gatekeeper Rules (AC: 3, 4)
  - [ ] Enforce that validation failures strictly halt the configuration promotion step.

## Dev Notes

- Make sure any failure output is descriptive and points directly to the breaking rule or column.
- **References**: [Source: _bmad-output/planning-artifacts/epics.md#Story 4.5]

## Dev Agent Record

### Agent Model Used

Gemini 3.1 Pro Preview

### Debug Log References

### Completion Notes List

### File List
