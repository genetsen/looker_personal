# 2026-04-10 Current ADIF Pipeline Inventory

## Purpose

This is the simple source-of-truth list for Phase 01.

It answers:

1. What looks shared?
2. What is common logic many clients will need?
3. What is clearly ADIF-specific?
4. Which later phase owns each ADIF-specific piece?

## Current Pipeline In One Line

DCM + broad original FPD + Prisma -> ADIF digital base -> updated FPD overlay -> ADIF social append -> final ADIF table

## Three Buckets

To make this easier to follow, use these three buckets:

### 1. Shared sources

Upstream inputs that many clients can use before client-specific rules are added.

### 2. Common logic

Patterns many clients will probably need, even if the current ADIF version is not reusable yet.

Examples:

- stitching multiple sources into one table
- preserving unmatched rows during joins
- getting data to package-and-date grain
- setting fallback order across sources

### 3. Project-specific logic

Rules tied to one client's business meaning.

Examples:

- client filters
- package remaps for one project
- project-specific source-priority decisions
- final output contract for one project

## Shared Cross-Client Building Blocks

### 1. Broad FPD loader output

- Current source:
  - `landing.fpd_data_ranged_shortcutsFolder`
- Why it looks shared:
  - it contains multiple clients
  - ADIF rows are filtered later

### 2. Prisma planning before ADIF filtering

- Current source:
  - `20250327_data_model.prisma_expanded_full`
- Why it looks shared:
  - the source is broader than ADIF
  - ADIF filters are applied downstream

### 3. Delivery cleanup before ADIF business rules

- Current source area:
  - DCM input feeding the ADIF digital base
- Why it is only a candidate shared piece:
  - some cleanup is reusable
  - the current ADIF view mixes cleanup with ADIF-specific logic

### 4. Raw social and pacing upstreams

- Current source area:
  - upstream inputs feeding the ADIF social branch
- Why they look shared:
  - ADIF mapping happens later

## Common Logic Many Clients Will Need

### 1. Multi-source stitching

- Why this is common:
  - most clients will need to combine delivery, planning, and first-party sources into one table
- Important distinction:
  - the pattern is common
  - the current ADIF version is still project-specific

### 2. Preserving unmatched rows

- Why this is common:
  - many pipelines need to keep rows that exist in one source but not another
- Current ADIF example:
  - the ADIF digital base uses `FULL OUTER JOIN`[1] so source-only rows are not dropped

### 3. Package-and-date grain shaping

- Why this is common:
  - many clients need one row per package and date before they can compare sources cleanly

### 4. Fallback priority across sources

- Why this is common:
  - many clients need a rule for which source wins when multiple sources report the same metric
- Important distinction:
  - the need is common
  - ADIF's exact order is project-specific

## ADIF-Specific Logic

### 1. Original FPD filtering

- Rule:
  - `source_file` contains `De Beers`
  - or starts with `FMUS | Partner Data Collection |`
- Future owner:
  - Phase 02

### 2. Prisma filtering

- Rule:
  - `advertiser_name = 'Forevermark US'`
  - exclude `package_type = 'Child'`
- Future owner:
  - Phase 02

### 3. Digital base join behavior

- Includes:
  - ADIF package remaps
  - ADIF-specific version of the multi-source stitching logic
  - ADIF-specific use of unmatched-row preservation
- Future owner:
  - Phase 02

### 4. Actuals precedence

- Current order:
  1. updated FPD
  2. original FPD
  3. DCM
- Future owner:
  - Phase 02 and Phase 03

### 5. Updated FPD overlay

- Includes:
  - package-total spreading across Prisma dates
  - overlay on top of the digital base
- Future owner:
  - Phase 03

### 6. Social mapping and append logic

- Includes:
  - ADIF social filtering
  - platform normalization for ADIF use
  - mapping ad set to package and ad to placement
  - pacing enrichment used in the ADIF table
- Future owner:
  - Phase 04

### 7. Final ADIF compatibility output

- Current target:
  - `repo_stg.adif__mainDataTable_notebook_v2_test`
- Future owner:
  - Phase 05

## Best First Shared Models

If we start building in Phase 02, the safest first shared models are:

1. broad FPD daily source model without ADIF filtering
2. Prisma package-day source model without ADIF filtering
3. delivery daily source model with only reusable cleanup

## What Phase 01 Resolves

- shared vs ADIF boundary: `defined`
- common logic vs project-specific logic distinction: `defined`
- inventory of major current pieces: `defined`
- decision not to build dbt models yet: `defined`

## Related Docs

- [Phase 01: Shared Source Modeling](../phases/01-shared-source-modeling.md)
- [Shared Source Model](../definitions/shared-source-model.md)

[1] `FULL OUTER JOIN` means “keep rows from both sides even when they do not match.” In plain English, if one source has a row and another source does not, the row can still stay in the result.
