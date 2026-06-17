# Story 5.3: Run Side-By-Side Parity Checks

Status: done


## Story

As a reviewer,
I want the candidate publish layer compared side by side with the current master table,
So that dashboard migration cannot happen with missing columns or shifted totals.

## Acceptance Criteria

- [ ] **Given** the candidate publish objects exist
- [ ] **When** side-by-side validation runs
- [ ] **Then** it compares candidate columns to the fresh live master-table schema
- [ ] **And** it compares approved totals to fresh live master-table totals
- [ ] **And** missing legacy columns, unexpected total differences, or unsafe metric changes block migration readiness.

## Tasks / Subtasks

- [ ] Task 1: Parity Checker Script (AC: 1, 2)
  - [ ] Implement a final query comparing target schemas and metrics between candidate and production master tables.
- [ ] Task 2: Variance Reporting (AC: 3, 4)
  - [ ] Calculate and flag any numeric differences to prevent broken downstream dashboards.

## Dev Notes

- Reconcile approved legacy totals precisely.
- **References**: [Source: _bmad-output/planning-artifacts/epics.md#Story 5.3]

## Dev Agent Record

### Agent Model Used

Gemini 3.1 Pro Preview

### Debug Log References

### Completion Notes List

### File List
