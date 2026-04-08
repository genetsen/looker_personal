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

### FPD Loader Pipelines (util/data_loaders/FPD_loader)
Primary docs: `util/data_loaders/FPD_loader/README.md`.

Quick start commands:

```bash
# Install R packages (one-time)
Rscript -e 'install.packages(c("googledrive", "googlesheets4", "dplyr", "stringr", "readr", "lubridate", "janitor", "bigrquery", "tidyr"))'

# Run the main FPD collection pipeline
Rscript util/data_loaders/FPD_loader/util_collect_fpd_v3.r

# Run the manually-updated data loader
Rscript util/data_loaders/FPD_loader/manually_updated_data_loader.r
```

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

## SQL QA Safety Protocol (Systemwide)

Applies to all SQL QA work in this workspace across all datasets/projects.

1. Run QA in isolated `_qa` tables/views only.
2. Provide proof before live changes (for example: query results, row counts, schema compatibility checks, and error diffs).
3. Do not patch live scripts/configs/tables until explicit user approval after proof review.

## BigQuery Object Reference Default

When the user mentions a BigQuery table or view path, treat the live warehouse object as the default source of truth unless the user explicitly asks for the local SQL file instead.

Use this order:

1. Inspect the live production object first.
2. If a likely local SQL file or documentation entry also exists, compare the local definition or description against production.
3. Tell the user clearly if the local file appears to drift from production before relying on the local version for analysis or edits.
4. If the user mentions a local file path, then work from the file directly instead of assuming the production object is the target.

When referencing a BigQuery table or view in user-facing outputs, render it as a clickable deep link whenever the client supports one instead of plain text only.

## BigQuery Access Default

Use BigQuery MCP first for warehouse inspection work in this workspace.

Use this order:

1. Prefer BigQuery MCP for dataset discovery, schema inspection, and read-only query work.
2. Use direct `gcloud` / `bq` command flows only when MCP is unavailable, when authentication needs repair, or when the task requires advanced access such as token-based Dataform notebook file reads.
3. If a helper script exists for sandbox-safe auth in the current project, use that helper instead of raw home-folder `gcloud` config paths.

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
