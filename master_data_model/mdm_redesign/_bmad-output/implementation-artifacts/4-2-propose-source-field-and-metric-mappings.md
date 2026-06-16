# Story 4.2: Propose Source Field And Metric Mappings

Status: ready-for-dev

<!-- Note: Validation is optional. Run validate-create-story for quality check before dev-story. -->

## Story

As a source integrator,
I want the onboarding skill to propose mappings into the universal contract,
So that I have a useful first draft before manual review.

## Acceptance Criteria

- [ ] **Given** a profiled source
- [ ] **When** mapping proposal runs
- [ ] **Then** it proposes source-to-target field mappings, placeholder candidates, metadata inference candidates, metric value status, and metric summability
- [ ] **And** the proposal includes example source rows with the proposed output shape.

## Tasks / Subtasks

- [ ] Task 1: Auto-Mapping Proposal Engine (AC: 1, 2)
  - [ ] Implement a draft proposal rule matching source fields to `univ_` targets using lexical and semantic analysis.
- [ ] Task 2: Sample Proposed Rows (AC: 3, 4)
  - [ ] Generate mock or sample row comparisons showing the source row and how it transforms into the final table.

## Dev Notes

- The proposal serves as a helpful draft. Always include reviewer warnings for complex mappings.
- **References**: [Source: _bmad-output/planning-artifacts/epics.md#Story 4.2]

## Dev Agent Record

### Agent Model Used

Gemini 3.1 Pro Preview

### Debug Log References

### Completion Notes List

### File List
