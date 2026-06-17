# Story 5.2: Build Shortcut Marts From The Universal Table

Status: done


## Story

As an analyst,
I want package, creative, DMA, and dashboard-friendly shortcut marts,
So that common reporting questions are easier without hiding the universal evidence table.

## Acceptance Criteria

- [ ] **Given** the universal evidence table has passed basic parity checks
- [ ] **When** shortcut marts are built
- [ ] **Then** each mart is derived from the universal evidence table
- [ ] **And** each mart preserves required legacy columns or clearly documents fields that are intentionally not available at that shortcut grain
- [ ] **And** no shortcut mart replaces the current production master table or mart.

## Tasks / Subtasks

- [ ] Task 1: Mart Construction (AC: 1, 2, 3)
  - [ ] Write dbt models materializing package, creative, and DMA shortcut marts sourced directly from the universal evidence model.
- [ ] Task 2: Schema Compatibility Layers (AC: 4, 5)
  - [ ] Enforce schema matching to prevent downstream dashboard failures, ensuring raw production marts are left untouched.

## Dev Notes

- Enforce proper dbt layer boundaries: marts reference ONLY staging or intermediate final facts, never raw sources directly.
- **References**: [Source: _bmad-output/planning-artifacts/epics.md#Story 5.2]

## Dev Agent Record

### Agent Model Used

Gemini 3.1 Pro Preview

### Debug Log References

### Completion Notes List

### File List
