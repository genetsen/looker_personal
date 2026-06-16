# Story 1.2: Capture The Live Master Baseline

Status: done

<!-- Note: Validation is optional. Run validate-create-story for quality check before dev-story. -->

## Story

As a model owner,
I want a fresh baseline from the current master table,
So that the candidate model can prove it kept today's columns and totals.

## Acceptance Criteria

- [x] **Given** the live [master table](https://console.cloud.google.com/bigquery?project=looker-studio-pro-452620&p=looker-studio-pro-452620&d=master_stg&t=data_model&page=table)
- [x] **When** the baseline job runs
- [x] **Then** it captures current column names, data types, key totals, row count, package count, and doNotSum fields
- [x] **And** the baseline is marked with the run timestamp.

## Tasks / Subtasks

- [x] Task 1: Baseline Query & Schema Capture (AC: 1, 2, 3)
  - [x] Implement a dry-run or `INFORMATION_SCHEMA` metadata query targeting `looker-studio-pro-452620.master_stg.data_model` to extract columns and data types.
  - [x] Query and capture key summaries from the live table (total row counts, sum of actual spend/impressions, unique packages/campaigns/dates).
- [x] Task 2: Output Persistence & Timestamping (AC: 4)
  - [x] Store the output of the captured baseline as a stable JSON or structured YAML artifact with the exact run timestamp under the `mdm_qa` zone (or as a dbt seed/source helper).

## Dev Notes

- **Live Object Source of Truth**: Treat the live BigQuery dataset and schema as the absolute source of truth.
- **Durable Baseline**: Preserve a schema/total snapshot before any candidate is built.
- **References**: [Source: _bmad-output/planning-artifacts/epics.md#Story 1.2]

## Dev Agent Record

### Agent Model Used

Gemini 3.1 Pro Preview

### Debug Log References

### Completion Notes List

### File List
