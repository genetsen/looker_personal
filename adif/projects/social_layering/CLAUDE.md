# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

Social Layering Pipeline — notebook-driven sub-project that appends normalized social media data (Meta, TikTok) into the ADIF main data table. Production runs from a BigQuery-connected Jupyter notebook, not scheduled SQL.

## Architecture

**Two-stage notebook build** writes to `repo_stg.adif__mainDataTable_notebook`:

1. **Section 1 — Table Rebuild:** `CREATE OR REPLACE TABLE` from `repo_stg.adif__prisma_expanded_plus_dcm_updated_fpd_view` (the base Prisma+DCM+FPD view)
2. **Section 2 — Social Append:** `INSERT INTO` normalized social rows with mapping, pacing, and metric alignment

**Data lineage:**
```
stg__olipop__crossplatform_raw_tbl  →  stg__adif__social_crossplatform (view)  →  notebook Section 2  →  adif__mainDataTable_notebook
                                                                                   ↑
                                         repo_int.crossplatform_pacing ────────────┘
```

## Key Files

| File | Role |
|------|------|
| `build__adif__prisma_expanded_plus_dcm_with_social_tbl.ipynb` | **Production notebook** — runs both stages + verification queries |
| `sql/stg__adif__social_crossplatform.sql` | View definition: filters raw social to ADIF accounts with `WP_` campaigns, normalizes platform names, adds row keys |
| `sql/test__adif__social_mapping_v2_vs_current.sql` | Validation: 10-section QA script comparing proposed mapping totals vs raw and current output |
| `social_mapping_matrix_editable.csv` | Living spec: token-based mapping rules for `ad_set→package` and `ad→placement` with status/notes columns |
| `archive/legacy_scheduled_sql/` | Retired scheduled SQL approach (replaced by notebook) |

## Commands

```bash
# Run validation SQL against BigQuery
bq query --project_id=looker-studio-pro-452620 --use_legacy_sql=false \
  < sql/test__adif__social_mapping_v2_vs_current.sql

# Deploy the staging view
bq query --project_id=looker-studio-pro-452620 --use_legacy_sql=false \
  < sql/stg__adif__social_crossplatform.sql
```

The production notebook is executed interactively (BigQuery DataFrames / Colab connected to `looker-studio-pro-452620`).

## Critical Conventions

- **Social row inclusion:** Account name must be in `('ADIF USA', 'A Diamond is Forever - US', 'A Diamond is Forever', 'De Beers Group')` AND campaign must match `WP_` regex — both filters are enforced in the staging view.
- **Platform normalization:** `facebook_ads`/`instagram_ads` → `meta`, `tiktok_ads` → `tiktok` — applied in both the staging view and validation script.
- **Token-based mapping:** Ad group and ad names are split on `_` to extract `initiative`, `funnel`, `supplier`, `line_item`, `placement_type`, `buy_category`. Token positions are documented in `social_mapping_matrix_editable.csv`.
- **Pacing allocation:** Planned spend from `crossplatform_pacing` is joined at `date + platform + campaign_id + ad_group_id` grain, then pro-rated to ad level by spend share (or evenly if no spend).
- **DCM columns stay null for social:** `d_media_cost`, `d_impressions`, `d_daily_recalculated_cost`, `d_daily_recalculated_imps` are always `NULL` for social rows — these are DCM-only fields.
- **`p_package_friendly` format:** `Social_[supplier_code]_[initiative]_[line_item_name]` (e.g., `Social_FB_GoldenGlobes_DesertDiamonds`)
- **Supplier codes:** `FB` (Facebook), `IG` (Instagram), `TT` (TikTok), `PT` (Pinterest) — never use `META` as a supplier code. Extracted via case-insensitive regex from ad_group_name platform token.

### Production SQL Verification (Required)

Before approving or deploying any production SQL change, verify live BigQuery state with `bq`.

Required checks:
1. Confirm object exists and type is correct (`bq show project:dataset.object`).
2. Confirm live schema (`bq show --schema ...` or `INFORMATION_SCHEMA`).
3. Run a read-only sanity query (row counts/date range/null checks).
4. If live BigQuery and local SQL/docs differ, treat live BigQuery as source of truth, document drift, then update local files.
5. Do not deploy until verification evidence is captured in task notes/PR notes.

Example verification commands:
```bash
bq show looker-studio-pro-452620:repo_stg.adif__mainDataTable_notebook
bq show --schema looker-studio-pro-452620:repo_stg.adif__mainDataTable_notebook
bq query --project_id=looker-studio-pro-452620 --use_legacy_sql=false \
  'SELECT COUNT(*) AS row_count FROM `looker-studio-pro-452620.repo_stg.adif__mainDataTable_notebook`'
```

## BigQuery Tables

| Table/View | Dataset | Purpose |
|------------|---------|---------|
| `stg__olipop__crossplatform_raw_tbl` | `repo_stg` | Raw social data (all clients) |
| `stg__adif__social_crossplatform` | `repo_stg` | Filtered/flagged ADIF social view |
| `crossplatform_pacing` | `repo_int` | Planned spend by platform/campaign/ad_group |
| `adif__prisma_expanded_plus_dcm_updated_fpd_view` | `repo_stg` | Base ADIF table source (Prisma+DCM+FPD) |
| `adif__mainDataTable_notebook` | `repo_stg` | **Production output** — combined ADIF data |

## Validation

The test script (`sql/test__adif__social_mapping_v2_vs_current.sql`) runs 10 QA checks:
1. Overall raw vs proposed totals (spend/impressions must match exactly)
2. Daily total mismatches (tolerance: 0.01)
3. Current vs proposed at campaign grain
4. Pacing coverage summary (expected vs proposed vs current planned spend)
5. Ad-set level pacing re-aggregation check
6. Sample mapped rows for manual review
