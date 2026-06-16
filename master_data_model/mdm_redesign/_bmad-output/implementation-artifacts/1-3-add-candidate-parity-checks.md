# Story 1.3: Add Candidate Parity Checks

Status: ready-for-dev

<!-- Note: Validation is optional. Run validate-create-story for quality check before dev-story. -->

## Story

As a reviewer,
I want automated checks comparing the candidate to the current master table,
So that missing columns or shifted totals block migration.

## Acceptance Criteria

- [ ] **Given** a candidate universal table exists
- [ ] **When** parity checks run
- [ ] **Then** the checks fail if any current master-table column is missing
- [ ] **And** the checks fail if approved totals do not reconcile within the agreed tolerance
- [ ] **And** doNotSum/context fields remain visibly non-additive.

## Tasks / Subtasks

- [ ] Task 1: Column Schema Parity (AC: 1, 3)
  - [ ] Implement a comparison test in dbt or a direct BigQuery SQL test checking that every single column from the captured baseline exists in the candidate model.
- [ ] Task 2: Total Reconciliation & doNotSum Checks (AC: 4, 5)
  - [ ] Build queries that compare key reporting metric sums (e.g., spend, impressions, clicks) between the baseline and the candidate.
  - [ ] Ensure any doNotSum field (such as plan-based values or packaged totals repeated on lower-grain rows) is caught if mistakenly summed.

## Dev Notes

- Enforce strict parity checking: column names must map perfectly.
- Set reconciliation tolerance exactly (e.g., 0.01 cents/row or total).
- **References**: [Source: _bmad-output/planning-artifacts/epics.md#Story 1.3]

## Dev Agent Record

### Agent Model Used

Gemini 3.1 Pro Preview

### Debug Log References

### Completion Notes List

### File List
