# Story 4.1: Profile A New Source Before Mapping

Status: done


## Story

As a source integrator,
I want the onboarding skill to profile a source before any mapping is saved,
So that I can understand the source grain, useful fields, metrics, lineage, and risks first.

## Acceptance Criteria

- [ ] **Given** a candidate source table or view
- [ ] **When** the onboarding skill profiles it
- [ ] **Then** the output identifies likely row grain, candidate keys, date fields, dimensions, metrics, source lineage fields, and obvious data-quality risks
- [ ] **And** the profiling step does not change core model SQL or final outputs.

## Tasks / Subtasks

- [ ] Task 1: Onboarding Script Profiler (AC: 1, 2)
  - [ ] Implement a profiling utility (e.g., in Python or SQL) that inspects schema shape and sample data of a target source view.
- [ ] Task 2: Profile Summary (AC: 3, 4)
  - [ ] Return a structured analysis with key identifiers, dimension candidates, metric candidates, and apparent grain without modifying any existing SQL.

## Dev Notes

- Ensure the profiling step remains completely read-only.
- **References**: [Source: _bmad-output/planning-artifacts/epics.md#Story 4.1]

## Dev Agent Record

### Agent Model Used

Gemini 3.1 Pro Preview

### Debug Log References

### Completion Notes List

### File List
