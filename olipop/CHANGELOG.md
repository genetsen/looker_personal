# Changelog

This changelog tracks reader-visible changes to the OLIPOP BigQuery model notes and live MMM view behavior.

## 2026-06-25

- **CHANGED** - Updated the live OLIPOP cross-platform view to use the renamed master-model media-type field while preserving the same social, digital, TV, and manual row classifications.
- **ADDED** - Added a maintained SQL definition for the top-level OLIPOP cross-platform view so its live master-model dependency is no longer documentation-only.
- **FIXED** - Preserved the OLIPOP view’s purpose note inside the live BigQuery definition instead of leaving it only as a file-leading comment.

### Pending Next Actions

- **Since Jun 23** - Decide whether the OLIPOP MMM output should add a separate metric-source column for manual-adjusted digital rows

## 2026-06-23

- **FIXED** - Updated the live OLIPOP MMM view so TV and standalone manual correction rows are labeled as `tv` and `manual` instead of being hidden under `digital`, while preserving the same total spend, impressions, and clicks.
- **CHANGED** - Refreshed the local project guide so it points to the master data model as the current non-social source for the OLIPOP MMM output.

### Pending Next Actions

- **Since Jun 23** - Decide whether the OLIPOP MMM output should add a separate metric-source column for manual-adjusted digital rows.
