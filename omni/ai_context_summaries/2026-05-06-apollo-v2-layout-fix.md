# Apollo V2 Layout Fix Handoff - 2026-05-06

## Goal

Fix the Apollo V2 dashboard layout without editing the original Apollo workbook.

## User Direction

- Original Apollo workbook must stay untouched.
- Existing workbooks should not be edited.
- A new dashboard can be created on main.
- Filters are no longer in scope for this pass.
- The remaining issue is the dashboard layout, because the original layout had significant manual work.

## Current Working Dashboard

- Original Apollo workbook: `000d07be`
- Layout-preserving duplicate: `cb17dc1e`
- Duplicate URL: `https://giantspoon.omniapp.co/w/cb17dc1e`

The duplicate was created from the original Apollo workbook so Omni should preserve the internal layout metadata that is not exposed by `omni documents get`.

## Technical State

- Target model ID: `9979e3b3-a8a5-4d9c-a1f4-53e0ed991065`
- Target topic: `apollo_v2`
- Target table: `master_stg__mart__data_model_apollo_v2`
- Target BigQuery object: `looker-studio-pro-452620.master_stg.mart__data_model_apollo_v2`

The retarget payload is at `/tmp/apollo_layout_preserve_retarget_payload.json`.

## Next Step

Before updating the duplicate, sanitize the retargeted queries so inherited sorts and column totals only reference fields present in each query. Prior smoke tests showed bad inherited metadata on the Data Sources tile and one fallback tile. Then rerun query smoke tests and update only `cb17dc1e`.
