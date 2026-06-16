# Story 1.4: Publish A Readable Proof Report

Status: ready-for-dev

<!-- Note: Validation is optional. Run validate-create-story for quality check before dev-story. -->

## Story

As a dashboard owner,
I want a readable proof summary for the candidate,
So that I can see whether it is safe to inspect before dashboard migration.

## Acceptance Criteria

- [ ] **Given** the baseline and parity checks have run
- [ ] **When** the proof report is generated
- [ ] **Then** it shows pass/fail status for schema coverage, total reconciliation, doNotSum safety, and production isolation
- [ ] **And** failures include enough detail to know what must be fixed.

## Tasks / Subtasks

- [ ] Task 1: Generate Proof Report (AC: 1, 2, 3)
  - [ ] Implement a script (or dbt post-hook/documentation helper) that compiles the results of the schema comparison and totals validation into a markdown proof summary.
  - [ ] Format the output as a clear table with green checkmarks or red fail flags.
- [ ] Task 2: Actionable Defect Reporting (AC: 4)
  - [ ] For any failed validation (missing column, total mismatch), write detailed context (expected vs actual) to help the engineer debug.

## Dev Notes

- Make reports user-friendly and highly visual.
- Avoid writing raw row counts to durable documentation; summarize them in this run-specific proof report instead.
- **References**: [Source: _bmad-output/planning-artifacts/epics.md#Story 1.4]

## Dev Agent Record

### Agent Model Used

Gemini 3.1 Pro Preview

### Debug Log References

### Completion Notes List

### File List
