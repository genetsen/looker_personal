# Changelog

This changelog tracks reader-visible changes to the OLIPOP BigQuery model notes and live MMM view behavior.

## 2026-06-23

- **FIXED** - Updated the live OLIPOP MMM view so TV and standalone manual correction rows are labeled as `tv` and `manual` instead of being hidden under `digital`, while preserving the same total spend, impressions, and clicks.
- **CHANGED** - Refreshed the local project guide so it points to the master data model as the current non-social source for the OLIPOP MMM output.

### Pending Next Actions

- **Since Jun 23** - Decide whether the OLIPOP MMM output should add a separate metric-source column for manual-adjusted digital rows.
