# Story 3.1: Add Metadata Source Fields

Status: ready-for-dev

<!-- Note: Validation is optional. Run validate-create-story for quality check before dev-story. -->

## Story

As a reviewer,
I want metadata fields to explain where their selected values came from,
So that inferred values are not mistaken for actual source metadata.

## Acceptance Criteria

- [ ] **Given** candidate metadata fields exist
- [ ] **When** metadata is selected for final output
- [ ] **Then** each governed metadata field can identify whether it is actual, manual, inferred, mixed, or placeholder
- [ ] **And** actual or approved manual metadata is not overwritten by inferred metadata.

## Tasks / Subtasks

- [ ] Task 1: Audit Fields Setup (AC: 1, 2)
  - [ ] Add tracking audit fields (e.g., `univ_flight_start_source`, `univ_spend_metadata_status`) explaining origin.
- [ ] Task 2: Protection of Actuals (AC: 3, 4)
  - [ ] Implement conditional routing so that if actual source or manually overridden metadata exists, it is strictly preserved.

## Dev Notes

- Ensure metadata status is clearly populated for auditing.
- Do not let inferred fallbacks overwrite true actuals.
- **References**: [Source: _bmad-output/planning-artifacts/epics.md#Story 3.1]

## Dev Agent Record

### Agent Model Used

Gemini 3.1 Pro Preview

### Debug Log References

### Completion Notes List

### File List
