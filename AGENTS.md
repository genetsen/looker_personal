# AGENTS.md

Operational notes for looker_personal repo, grounded in the documented pipelines and scripts.

## Key Workflows

### ADIF Updated FPD Integration (adif/)
Primary docs: `adif/projects/updated_fpd_integration/README_Updated_FPD_Integration.md` and `adif/projects/updated_fpd_integration/DEPLOYMENT_CHECKLIST.md`.

Commands (from the deployment checklist):

```bash
# Verify BigQuery table exists
bq show looker-studio-pro-452620:landing.adif_updated_fpd_daily

# Run SQL validation
bq query --project_id=looker-studio-pro-452620 --use_legacy_sql=false \
  < adif/projects/updated_fpd_integration/validate_updated_fpd_detailed_v2.sql

# Run R validation
Rscript adif/projects/updated_fpd_integration/util_validate_updated_fpd_impact.r

# Review validation CSVs
ls -lh data/validation_*.csv
```

Deploy the updated FPD view:

```bash
# Create new view (recommended first)
bq query --project_id=looker-studio-pro-452620 --use_legacy_sql=false \
  < adif/projects/updated_fpd_integration/deploy_updated_fpd_view.sql
```

Rollback guidance and verification SQL are in `adif/projects/updated_fpd_integration/DEPLOYMENT_CHECKLIST.md`.

### ADIF TV & Digital Data Pipeline (adif/)
Primary docs: `adif/projects/tv_digital_pipeline/README - ADIF TV & Digital Data Pipeline.md`.

Key ingestion scripts:

```bash
# Ingest first-party data (FPD) from Google Sheets
Rscript adif/projects/tv_digital_pipeline/util_collect_fpd_v2.r

# Ingest TV monthly estimates (local + national)
Rscript adif/projects/tv_digital_pipeline/util_collect_monthly_estimates.r
```

### FPD Loader Pipelines (FPD/FPD_loader and util/data_loaders/FPD_loader)
Primary docs: `FPD/FPD_loader/README.md` for the shortcut-aware loader and `util/data_loaders/FPD_loader/README.md` for the older utility copy.

Quick start commands:

```bash
# Install R packages (one-time)
Rscript -e 'install.packages(c("googledrive", "googlesheets4", "dplyr", "stringr", "readr", "lubridate", "janitor", "bigrquery", "tidyr"))'

# Run the main shortcut-aware FPD collection pipeline
Rscript FPD/FPD_loader/util_collect_fpd_shortcutsFolder.r

# Run the older utility-copy loader only when that path is explicitly targeted
Rscript util/data_loaders/FPD_loader/util_collect_fpd_v3.r

# Run the manually-updated data loader
Rscript util/data_loaders/FPD_loader/manually_updated_data_loader.r
```

For FPD Loader cache, Google Sheet, shortcut, or timestamp mismatches, audit the cache boundary before naming a root cause. Distinguish formula edits, row data edits, Drive metadata freshness, cache-read freshness checks, and cache-write persistence as separate failure modes. Inspect the exact affected rows across live sheet, `.rds` cache, phase outputs, and BigQuery when applicable, and quote the cache read/write condition and compared values before proposing a cause.

## Repo Map

- `adif/` - ADIF project work, including social layering, updated FPD integration, and TV/digital pipeline assets.
- `mft/` - MFT project work, including guarded BigQuery query helpers and DCM UTM QA SQL.
- `omni/` - Omni-specific instruction surface; default to live Omni work instead of local files.
- `util/` - Shared loaders, R helpers, and reusable warehouse utilities.
- `docs/` - Cross-project documentation and scheduled-query runbooks.

### BigQuery Scheduled Queries (docs/)
Primary docs: `docs/SCHEDULED_QUERIES.md`.

Health check query (recent scheduled-query runs):

```sql
SELECT
  transfer_config_id,
  run_time,
  state,
  error_status
FROM `region-us`.INFORMATION_SCHEMA.JOBS
WHERE job_type = 'QUERY'
  AND creation_time >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 24 HOUR)
ORDER BY creation_time DESC
```

### MFT View Pipeline (mft/)
Primary docs: `mft/README.md` (pipeline overview and SQL references).

TODO: Confirm if there is a preferred local entrypoint or run command for MFT beyond the SQL views documented in `mft/README.md`.

## BigQuery Workflow Routing

- Apply the global [BigQuery, SQL, and data-modeling rules](/Users/eugenetsenter/.codex/BIGQUERY_SQL_DATA_MODELING_RULES.md) for live source-of-truth selection, read-only inspection, permission classification, field lineage, grain conflicts, and durable documentation boundaries.
- Repo-specific proof ownership: use the relevant semantic-layer skill for orientation only, then use `sql-change-guard` as the sole broad pre-deployment comparison owner for SQL changes that feed downstream models, views, or reporting tables. Reuse its passing schema, key-coverage, and metric evidence instead of repeating equivalent queries.
- Repo-specific deployment proof: after deployment, use one focused live check for the changed behavior and downstream surface. Do not rerun the pre-deployment baseline comparison unless live evidence conflicts.

## SQL QA Safety Requirements

1. Run QA in isolated `_qa` tables/views only.
2. Do not patch live scripts, configs, tables, or views until explicit user approval after proof review.
3. Before configuring uniqueness checks, confirm the candidate keys are genuinely unique at the live model grain. Do not turn a known baseline condition into a false candidate failure.
4. For new or replacement combined/master views, apply the global source-column preservation rule before excluding user-facing planning, reporting, or lineage fields.
5. For `master_data_model`, unmatched DCM/FPD rows are raw evidence only unless metric inclusion is explicitly approved. They may preserve source fields and issue labels, but must not populate final metrics or package actual rollups.

## Shared BigQuery QA Object Hygiene

For `master_data_model`, any QA/test BigQuery object published into a shared dataset such as `master_stg` must have warehouse-visible metadata before handoff. At minimum, set a BigQuery `description` and include a preservable SQL comment directly under the outer `SELECT` statement. Both should state: what the object is for, what changed versus the prior/local SQL with local file and line references where applicable, whether it is safe to delete, and any cleanup owner or condition. Do not publish context-free QA views/tables into shared datasets.

## Omni Dashboard Change Preview Default

When making or proposing changes to an Omni dashboard or widget in this workspace, use a preview-first workflow unless the user explicitly asks to skip it.

Use this order:

1. Inspect the live dashboard element or widget first.
2. Build and show a rendered before-and-after preview before making the live change.
3. Show the entire widget in the preview, not just a cropped detail or sub-element.
4. Call out whether the proposed change is visual, behavioral, or both.
5. Wait for user confirmation before applying the real dashboard change.

## TODO Backlog

- Rebuild the Basis UTM pipeline end-to-end with one canonical runbook that starts from trafficking-sheet ingestion and ends at `looker-studio-pro-452620.mass_mutual_mft_ext.mft_data`.
- Clean up confusing remnant Basis UTM assets across local folders and BigQuery (especially `landing`, `repo_stg`, and `utm_scrap`) by defining source-of-truth tables/views and archiving or deleting superseded scripts/tables.
- Practice the daily Git beginner drill from `README.md` at 5:00 PM local time and track confidence improvements week over week.

## Verification TODOs

- Re-run `FPD/FPD_loader/util_collect_fpd_shortcutsFolder.r` end to end and confirm the BigQuery upload succeeds after loading `bigrquery`.
