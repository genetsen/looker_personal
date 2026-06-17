# Story 4.4: Save Versioned Mapping Configuration

Status: done


## Story

As a model owner,
I want approved mapping decisions saved outside core final-table SQL,
So that new sources can be added by configuration instead of rewriting the model.

## Acceptance Criteria

- [ ] **Given** a reviewer has approved mapping choices
- [ ] **When** the mapping configuration is saved
- [ ] **Then** the saved configuration includes source name, version, reviewer-visible status, source fields, target fields, placeholder rules, metric rules, inference rules, and timestamp
- [ ] **And** the configuration is machine-readable and suitable for dbt model generation or dbt model inputs.

## Tasks / Subtasks

- [ ] Task 1: Mapping Schema & File Persistence (AC: 1, 2)
  - [ ] Implement a serializer to save approved configurations as versioned JSON/YAML files under `mdm_config` target directory or BigQuery tables.
- [ ] Task 2: Downstream Compatibility (AC: 3, 4)
  - [ ] Design the layout so dbt model generators can readily consume these files to compile active mapping configurations.

## Dev Notes

- Decouple mapping decisions entirely from core SQL code blocks.
- **References**: [Source: _bmad-output/planning-artifacts/epics.md#Story 4.4]

## Dev Agent Record

### Agent Model Used

Gemini 3.1 Pro Preview

### Debug Log References

### Completion Notes List

### File List
