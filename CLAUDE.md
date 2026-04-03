# Looker Personal — Analytics Data Platform

SQL/R data pipelines + LookML/Omni models for marketing analytics. BigQuery project: `looker-studio-pro-452620`.

## Directory Structure

| Directory | Purpose |
|-----------|---------|
| `sql/base/` | Staging SQL (basis, dcm, prisma raw transforms) |
| `sql/marts/` | Analytical marts (delivery, unified views) |
| `sql/stg/` | Cross-platform staging (video views, meta) |
| `sql/tiktok/` | TikTok-specific staging + deduplication |
| `FPD/` | First Party Data loader pipelines (R) |
| `adif/` | ADIF framework scripts (Streamlit reporting, FPD processing) |
| `olipop/` | Olipop client-specific staging SQL |
| `apollo/` | Apollo client pipelines |
| `omni/` | Omni/LookML view definitions + BigQuery connection YAML |
| `Prisma/` | Prisma media data processing |
| `util/` | Shared utilities and data loaders |
| `dim_model/` | Dimensional model definitions |
| `docs/` | Documentation and research notes |

## Key Patterns

- **Staging convention:** `stg__<source>__<entity>.sql` → `stg2__` for second-pass enrichment
- **Marts convention:** `mart__<domain>__<description>.sql`
- **BigQuery datasets:** `landing` (raw/staged), `data_model_2025` (modeled)
- **Deduplication:** Common pattern across sources — see `test__basis__duplicateDetector.r`, `int__tiktok_combined_history_dedupe.sql`

## Gotchas

- Two FPD_loader copies exist: `FPD/FPD_loader/` (uses Google Drive shortcuts path) and `util/data_loaders/FPD_loader/` (uses direct folder ID). Each has its own CLAUDE.md.
- Omni YAML views reference BigQuery tables directly — changing table names in BQ requires updating corresponding `.view.yaml` files.
- `archive/` directories should never be deleted per global preference — they're gitignored.
- Research notes from CodeViz are in `docs/codeviz-research.md` (moved from this file on 2026-02-24).
