# Story 5.4: Publish A Migration Readiness Report

Status: ready-for-dev

<!-- Note: Validation is optional. Run validate-create-story for quality check before dev-story. -->

## Story

As a model owner,
I want a readable migration readiness report,
So that business reviewers can understand what is safe, what changed, and what still needs work.

## Acceptance Criteria

- [ ] **Given** publish-layer checks have run
- [ ] **When** the readiness report is generated
- [ ] **Then** it shows schema coverage, total reconciliation, shortcut mart status, metric safety status, known differences, and open blockers
- [ ] **And** it explains failures in plain language with enough detail for the next fix.

## Tasks / Subtasks

- [ ] Task 1: Readiness Report Generator (AC: 1, 2, 3)
  - [ ] Implement a generator creating a clean, Markdown-based Migration Readiness report summarizing schema, metrics, and marts.
- [ ] Task 2: Variance Analysis Summary (AC: 4)
  - [ ] Format any known variances in a simple human-readable table with clear mitigation advice.

## Dev Notes

- Follow Markdown guidelines (short, clean link labels instead of raw paths).
- Keep fluctuating row counts out of durable docs; summarize them here.
- **References**: [Source: _bmad-output/planning-artifacts/epics.md#Story 5.4]

## Dev Agent Record

### Agent Model Used

Gemini 3.1 Pro Preview

### Debug Log References

### Completion Notes List

### File List
