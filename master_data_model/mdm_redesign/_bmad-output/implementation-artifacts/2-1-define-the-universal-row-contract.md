# Story 2.1: Define The Universal Row Contract

Status: done


## Story

As an analyst,
I want the candidate table to declare its row meaning with prefixed universal fields,
So that I can understand what each row represents before grouping or summing.

## Acceptance Criteria

- [ ] **Given** the candidate universal table is being designed
- [ ] **When** the row contract is defined
- [ ] **Then** it includes `univ_row_grain`, `univ_source_system`, `univ_source_row_id`, `univ_source_lineage`, and `univ_record_date`
- [ ] **And** existing legacy columns keep their current names.

## Tasks / Subtasks

- [ ] Task 1: Contract Design (AC: 1, 2)
  - [ ] Design the schema structure using the `univ_` prefixed metadata fields to declare row granularity and source origin clearly.
- [ ] Task 2: Schema Integration (AC: 3, 4)
  - [ ] Map the newly added prefixed metadata fields alongside all existing legacy reporting columns, ensuring zero name collisions.

## Dev Notes

- Every universal metadata column must start with `univ_`.
- Legacy columns MUST retain their exact naming from the baseline schema.
- **References**: [Source: _bmad-output/planning-artifacts/epics.md#Story 2.1]

## Dev Agent Record

### Agent Model Used

Gemini 3.1 Pro Preview

### Debug Log References

### Completion Notes List

### File List
