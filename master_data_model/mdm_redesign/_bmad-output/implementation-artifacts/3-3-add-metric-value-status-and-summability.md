# Story 3.3: Add Metric Value Status and Summability

Status: ready-for-dev

<!-- Note: Validation is optional. Run validate-create-story for quality check before dev-story. -->

## Story

As an analyst,
I want every metric to say where it came from and whether I can sum it,
So that I do not accidentally inflate totals when grouping lower-grain rows.

## Acceptance Criteria

- [ ] **Given** a metric is exposed in the candidate table
- [ ] **When** metric rules are applied
- [ ] **Then** the metric has a value-status meaning such as direct, inferred, allocated, or unavailable
- [ ] **And** the metric has a summability meaning such as additive, doNotSum, or blocked
- [ ] **And** repeated package/context totals are not exposed as normal additive metrics.

## Tasks / Subtasks

- [ ] Task 1: Metric Fields Annotations (AC: 1, 2)
  - [ ] Add explicit status and summability flag columns (e.g., `impressions_status`, `impressions_summability`) for each numeric metric.
- [ ] Task 2: Block Invalid Summability (AC: 3, 4, 5)
  - [ ] Enforce that package-level totals are marked `doNotSum` when represented on child rows.

## Dev Notes

- Help downstream dashboard builders understand which metrics are safe to aggregate.
- **References**: [Source: _bmad-output/planning-artifacts/epics.md#Story 3.3]

## Dev Agent Record

### Agent Model Used

Gemini 3.1 Pro Preview

### Debug Log References

### Completion Notes List

### File List
