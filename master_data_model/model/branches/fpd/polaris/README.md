# MIQ Polaris Email Reader and Loader

MIQ supplies delivery through two ingestion paths: the existing FPD path and the Polaris Email path. This folder contains the QA-only reader and the guarded manual production loader for the Polaris Email Meta and TikTok exports.

## Run The Preview

From the `master_data_model` folder, choose a new or empty output folder:

```bash
Rscript model/branches/fpd/polaris/preview_polaris_fpd.R \
  --output-dir /tmp/polaris-fpd-preview \
  --compare-live-model
```

The default GCS prefix is the approved Polaris connection for Purely Elizabeth. Use `--source-prefix` only when reviewing another explicit Polaris prefix; the Stage 1 mappings remain limited to Facebook, Instagram, and TikTok for the three approved MIQ packages.

## Safety Contract

| Control | Behavior |
|---|---|
| Source selection | Requires exactly one Meta CSV and one TikTok CSV. Multiple snapshots stop the run and remain listed in `source_inventory.csv`. |
| Raw preservation | Copies the two source CSVs byte-for-byte into the local `raw` folder. |
| Mapping | Applies only Facebook → Facebook Awareness (`P3HF7QB`), Instagram → Instagram (`P3HF7T8`), and TikTok → TikTok (`P3HF88Q`). |
| Invalid data | Retains and labels unmapped platforms, missing required values, invalid dates or metrics, and duplicate natural keys. |
| Warehouse access | `--compare-live-model` issues a read-only `SELECT` against the v3 original-FPD rows. |
| Output protection | Requires an explicit new or empty `--output-dir`; it does not mix runs or overwrite existing review files. |

## Run the Guarded Manual Load

The mapping and empty delivery contracts are created first:

```bash
bq query --project_id=looker-studio-pro-452620 --use_legacy_sql=false \
  < model/branches/fpd/polaris/create_polaris_email_package_mapping.sql

bq query --project_id=looker-studio-pro-452620 --use_legacy_sql=false \
  < model/branches/fpd/polaris/create_polaris_email_delivery_daily.sql
```

Exercise the exact sources and live mapping table without writing delivery:

```bash
Rscript model/branches/fpd/polaris/load_polaris_email_delivery.R --dry-run
```

After the dry run passes, replace the production snapshot through staging:

```bash
Rscript model/branches/fpd/polaris/load_polaris_email_delivery.R
```

| Production gate | Required behavior |
|---|---|
| Source inventory | Exactly one current Meta CSV and one current TikTok CSV. |
| Mapping | Every row matches one active feed/platform/campaign/ad-group key in the mapping table. |
| Row quality | Required dimensions and dates parse; metric text is numeric; natural keys are unique. |
| Replacement | Upload to a described staging table, validate again in BigQuery, then replace the prior snapshot in one transaction. |
| Failure | Keep the last good production snapshot. Retain a failed staging table only when warehouse validation evidence is needed. |

The production model uses Polaris Email inside the loaded minimum-to-maximum date range for each package. Both existing FPD landing tables remain physically unchanged and continue to supply dates outside that range. Manual delivery edits still override every source path.

## Review Files

| File | What it shows |
|---|---|
| `raw/meta/*.csv` and `raw/tiktok/*.csv` | Exact source-file copies for field-level comparison. |
| `source_inventory.csv` | Every CSV found under the requested prefix and its classified feed. |
| `normalized_rows.csv` | Daily creative-level rows with source lineage, normalized metrics, package candidates, and review statuses. |
| `mapping_review.csv` | Row counts by platform, package candidate, mapping result, and validation result. |
| `source_reconciliation.csv` | Raw-versus-normalized row and metric totals by platform. |
| `legacy_overlap.csv` | Optional package/snapshot comparison. Each row compares cumulative Polaris delivery through the native FPD `date_final` snapshot, not a weekly increment. It includes raw and distinct source-row counts and marks duplicate snapshots for review rather than presenting them as aligned. |
| `run_summary.md` | Human-readable totals, unresolved counts, comparison status, and the production safety boundary. |

Generated CSVs are local evidence and are excluded by the repository's existing CSV/output ignore rules. They are not production inputs.

## Tests

```bash
Rscript model/branches/fpd/polaris/tests/test_polaris_fpd_logic.R
```

The fixtures cover Meta's UTF-8 header marker, TikTok's alternate schema, zero metrics, all three approved mappings, unknown platforms, missing required values, invalid dates and metrics, duplicate reporting, exact source reconciliation, Sunday-start week logic, and cumulative snapshot logic.

## Current Boundary

The [MVP plan](/Users/eugenetsenter/Looker_clonedRepo/looker_personal/master_data_model/model/branches/fpd/polaris-email-v3-mvp-plan.md) is deployed manually through V3. The mapping table is the temporary central owner. A Google Sheet editor and automated scheduling are intentionally deferred; neither requires changing the loader's mapping or validation contract.
