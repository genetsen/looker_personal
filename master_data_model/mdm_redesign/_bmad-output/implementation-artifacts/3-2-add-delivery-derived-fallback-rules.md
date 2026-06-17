# Story 3.2: Add Delivery-Derived Fallback Rules

Status: done


## Story

As a model owner,
I want missing flight dates and plan-like values to be inferred only from delivery evidence,
So that rows without actual metadata still have useful backup labels.

## Acceptance Criteria

- [ ] **Given** actual metadata is missing
- [ ] **When** fallback rules run
- [ ] **Then** flight start uses minimum delivery date
- [ ] **And** flight end uses maximum delivery date
- [ ] **And** planned spend uses total observed delivery spend only when actual planned spend is missing
- [ ] **And** planned impressions uses total observed delivery impressions only when actual planned impressions is missing.

## Tasks / Subtasks

- [ ] Task 1: Date and Totals Fallback (AC: 1, 2, 3)
  - [ ] Implement aggregation logic grouping delivery records by package/source to compute minimum date, maximum date, total spend, and total impressions.
- [ ] Task 2: Merge and Null Fill (AC: 4, 5, 6)
  - [ ] Integrate these derived totals as the fallback layer when actual planning figures are missing or null.

## Dev Notes

- Ensure fallback logic does not apply if original planning figures exist.
- Label any delivery-derived fallback values visibly in metadata audit fields.
- **References**: [Source: _bmad-output/planning-artifacts/epics.md#Story 3.2]

## Dev Agent Record

### Agent Model Used

Gemini 3.1 Pro Preview

### Debug Log References

### Completion Notes List

### File List
