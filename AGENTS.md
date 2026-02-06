# AGENTS.md

Operational notes for this repo, grounded in the documented pipelines and scripts.

## Key Workflows

### ADIF Updated FPD Integration (adif/)
Primary docs: `adif/README_Updated_FPD_Integration.md` and `adif/DEPLOYMENT_CHECKLIST.md`.

Commands (from the deployment checklist):

```bash
# Verify BigQuery table exists
bq show looker-studio-pro-452620:landing.adif_updated_fpd_daily

# Run SQL validation
bq query --project_id=looker-studio-pro-452620 --use_legacy_sql=false \
  < validate_updated_fpd_detailed_v2.sql

# Run R validation
Rscript util_validate_updated_fpd_impact.r

# Review validation CSVs
ls -lh data/validation_*.csv
```

Deploy the updated FPD view:

```bash
# Create new view (recommended first)
bq query --project_id=looker-studio-pro-452620 --use_legacy_sql=false \
  < deploy_updated_fpd_view.sql
```

Rollback guidance and verification SQL are in `adif/DEPLOYMENT_CHECKLIST.md`.

### ADIF TV & Digital Data Pipeline (adif/)
Primary docs: `adif/README - ADIF TV & Digital Data Pipeline.md`.

Key ingestion scripts:

```bash
# Ingest first-party data (FPD) from Google Sheets
Rscript adif/util_collect_fpd_v2.r

# Ingest TV monthly estimates (local + national)
Rscript adif/util_collect_monthly_estimates.r
```

### FPD Loader Pipelines (util/data_loaders/FPD_loader)
Primary docs: `util/data_loaders/FPD_loader/README.md`.

Quick start commands:

```bash
# Install R packages (one-time)
Rscript -e 'install.packages(c("googledrive", "googlesheets4", "dplyr", "stringr", "readr", "lubridate", "janitor", "bigrquery", "tidyr"))'

# Run the main FPD collection pipeline
Rscript util_collect_fpd_v3.r

# Run the manually-updated data loader
Rscript manually_updated_data_loader.r
```

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
