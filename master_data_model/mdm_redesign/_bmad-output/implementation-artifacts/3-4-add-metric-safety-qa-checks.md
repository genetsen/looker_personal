# Story 3.4: Add Metric Safety QA Checks

Status: ready-for-dev

<!-- Note: Validation is optional. Run validate-create-story for quality check before dev-story. -->

## Story

As a reviewer,
I want QA checks that catch likely metric duplication,
So that lower-grain rows cannot silently inflate package-level totals.

## Acceptance Criteria

- [ ] **Given** the candidate includes lower-grain rows or repeated context totals
- [ ] **When** metric safety checks run
- [ ] **Then** checks identify metrics that are unsafe to sum
- [ ] **And** checks fail if package/context totals appear as ordinary additive metrics
- [ ] **And** the proof output explains which metric and grain caused the failure.

## Tasks / Subtasks

- [ ] Task 1: dbt Validation Tests (AC: 1, 2)
  - [ ] Write dbt tests that check for metric duplication on child rows.
- [ ] Task 2: Descriptive Failures (AC: 3, 4)
  - [ ] Format the test failure output to name the specific metric, package key, and row grain that triggered the warning.

## Dev Notes

- Metric duplication is treated as a blocking migration risk.
- Ensure automated QA checks block deployment if any sum is unsafe.
- **References**: [Source: _bmad-output/planning-artifacts/epics.md#Story 3.4]

## Dev Agent Record

### Agent Model Used

Gemini 3.1 Pro Preview

### Debug Log References

### Completion Notes List

### File List
