---
pipeline: MIQ Polaris Email Delivery
source_type: first-party delivery
output: looker-studio-pro-452620.master_stg.data_model_v3
output_grain: package_date_platform_campaign_ad_group_ad
source_tables:
  - looker-studio-pro-452620.landing.polaris_email_package_mapping
  - looker-studio-pro-452620.landing.polaris_email_delivery_daily
refresh: guarded manual load, then the master-model refresh wrapper
loader_script: load_polaris_email_delivery.R
verified: 2026-08-21
verified_against:
  - load_polaris_email_delivery.R --dry-run
  - preview_polaris_email_delivery.R
  - master_stg.data_model_v3 live schema and Polaris field values
  - create_master_stg_data_model.sql
  - create_master_stg_data_model_v3.sql
reviewers: []
---

# MIQ Polaris Email Delivery Pipeline

**For:** people loading or checking Purely Elizabeth Meta and TikTok delivery.
**Produces:** Polaris detail in the
[Master evidence model v3](https://console.cloud.google.com/bigquery?project=looker-studio-pro-452620&p=looker-studio-pro-452620&d=master_stg&t=data_model_v3&page=table).
**Related:** [FPD pipeline](/Users/eugenetsenter/Looker_clonedRepo/looker_personal/master_data_model/model/branches/fpd/README_fpd-pipeline.md)
and [Master Data Model Pipeline v2](/Users/eugenetsenter/Looker_clonedRepo/looker_personal/master_data_model/README_v2.md).

## Table of Contents

- [Terms](#terms)
- [Safe Operating Path](#safe-operating-path)
- [Data Contract](#data-contract)
- [Output Contract](#output-contract)
- [Run and Prove](#run-and-prove)
- [Verify Current State](#verify-current-state)
- [Known Limits and Troubleshooting](#known-limits-and-troubleshooting)
- [Definitions](#definitions)

## Terms

A **rolling snapshot** contains all available history through its newest source
date. One output row has package/date/platform/campaign/ad-group/ad **grain**[^1].
A successful load **rebuilds**[^2] the full Polaris landing snapshot.

## Safe Operating Path

| Need | Use |
|---|---|
| Load | [Production loader](/Users/eugenetsenter/Looker_clonedRepo/looker_personal/master_data_model/model/branches/fpd/polaris/load_polaris_email_delivery.R) |
| Review without writing | [Preview](/Users/eugenetsenter/Looker_clonedRepo/looker_personal/master_data_model/model/branches/fpd/polaris/preview_polaris_email_delivery.R) |
| Consume Polaris detail | [V3 model](https://console.cloud.google.com/bigquery?project=looker-studio-pro-452620&p=looker-studio-pro-452620&d=master_stg&t=data_model_v3&page=table) |
| Refresh downstream models | [Refresh wrapper](/Users/eugenetsenter/Docs/R_Studio_Projects/universal_cron_runner/automation_hub/workloads/ops/bq_trigger/run_master_data_model_clustered_advertiser_refresh.sh) |

Do not directly change vendor files, FPD landing tables, Manual Package Editor
data, production tables, or scheduler configuration. The loader is the only
approved Polaris landing-table writer; the named builders and wrapper own model
refreshes.

## Data Contract

```text
Cloud Storage CSVs
  -> identify Meta or TikTok from columns
  -> select each feed's uniquely newest source-date snapshot
  -> validate and map every row
  -> replace the Polaris landing snapshot
  -> refresh V3
```

Filenames, folder names, upload times, and listing order never identify a feed or
the current snapshot. The loader stops before writing when a feed is missing, a
schema is unsupported, a date is invalid, or newest files tie.

| Source | Purpose | Boundary |
|---|---|---|
| Polaris CSVs | Rolling Meta or TikTok ad delivery | Feed identity comes only from required columns. |
| [Package mapping](https://console.cloud.google.com/bigquery?project=looker-studio-pro-452620&p=looker-studio-pro-452620&d=landing&t=polaris_email_package_mapping&page=table) | Assign one Prisma package | Supplies identity, never metrics. |
| [Polaris landing](https://console.cloud.google.com/bigquery?project=looker-studio-pro-452620&p=looker-studio-pro-452620&d=landing&t=polaris_email_delivery_daily&page=table) | Normalized ad-level snapshot | Source QA surface, not a package/date rollup. |
| Original and updated FPD | Delivery fallback | Used outside Polaris coverage; never rewritten here. |
| Prisma plans | Package context and plan | Does not supply Polaris actuals. |
| Manual Package Editor | Approved corrections | Outranks automated actuals. |

The mapping join uses trimmed, case-insensitive feed + platform + campaign + ad
group. Every row must match exactly one active mapping:

| Feed | Platform | Campaign / ad group | Package |
|---|---|---|---|
| Meta | Facebook | Awareness Campaign / ACR - Facebook | `P3HF7QB` |
| Meta | Facebook | Awareness Campaign / Interests - Facebook | `P3HF7QB` |
| Meta | Instagram | Awareness Campaign / ACR - Instagram | `P3HF7T8` |
| Meta | Instagram | Awareness Campaign / Interests - Instagram | `P3HF7T8` |
| TikTok | TikTok | Purely Elizabeth - Awareness Q3 / Interests | `P3HF88Q` |

Discovery owns object listing; schema inspection owns feed identity; snapshot
selection owns freshness; mapping owns package identity; the loader owns landing
replacement; the model owns precedence. Fix the first failed stage rather than
patching a later output.

## Output Contract

| Consumer need | Field |
|---|---|
| Select Polaris rows | `qa_v3_source_detail_type = 'polaris_email'` |
| Confirm grain | `qa_v3_metric_grain = 'package_date_platform_campaign_ad_group_ad'` |
| Package/date | `_package_id`, `_date` |
| Platform | `polaris_platform` — `s_platform` is empty for current Polaris rows |
| Campaign/ad group/ad | `polaris_campaign_name`, `polaris_ad_group_name`, `polaris_ad_name` |
| Reporting actuals | `_spend`, `_impressions`, `_clicks`, `_video_views`, `_video_comps` |
| Source trace | `polaris_raw_*`, `polaris_source_object_uri`, `polaris_source_row_number` |
| Summable plan | `_planned_spend`, `_planned_impressions` on at most one row per package/date |
| Reference-only plan | `qa_v3_package_planned_*_doNotSum` — never sum these fields |

Polaris replaces original and updated FPD only between each package's minimum and
maximum loaded Polaris dates. FPD remains outside that range. Manual corrections
still win. The compatibility model remains available for existing consumers but
does not preserve Polaris ad detail; use V3 for new work.

## Run and Prove

Run from the `master_data_model` folder.

| Step | Command | Pass condition |
|---|---|---|
| Tests | `Rscript model/branches/fpd/polaris/tests/test_polaris_email_delivery_logic.R` | Ends with `All Polaris Email delivery logic tests passed.` |
| Preview | `Rscript model/branches/fpd/polaris/preview_polaris_email_delivery.R --output-dir /tmp/polaris-email-delivery-preview --compare-live-model` | Selects one file per feed and writes local evidence only. |
| Dry run | `Rscript model/branches/fpd/polaris/load_polaris_email_delivery.R --dry-run` | Every row validates, maps once, and reconciles; no table changes. |
| Approved load | `Rscript model/branches/fpd/polaris/load_polaris_email_delivery.R` | Reports terminal production success. Requires explicit approval. |
| Publish | Run the [refresh wrapper](/Users/eugenetsenter/Docs/R_Studio_Projects/universal_cron_runner/automation_hub/workloads/ops/bq_trigger/run_master_data_model_clustered_advertiser_refresh.sh) | Wrapper and its clustered/V3 checks pass. |

Tests plus the live dry run prove source-code changes before writing. The loader's
terminal success proves landing replacement. The refresh wrapper plus the queries
below prove publication. Script startup, schema presence, row count alone, a dry
run, or a started BigQuery job do not prove publication.

Maintenance is limited to four habits: test and dry-run before loading; review the
inventory when source objects change; add mappings only after package ownership is
confirmed; and refresh downstream models in the same session as a production load.

## Verify Current State

Confirm the published field values:

```sql
SELECT
  qa_v3_source_detail_type,
  qa_v3_metric_grain,
  ARRAY_AGG(DISTINCT polaris_source_feed IGNORE NULLS ORDER BY polaris_source_feed) AS feeds,
  ARRAY_AGG(DISTINCT polaris_platform IGNORE NULLS ORDER BY polaris_platform) AS platforms,
  ARRAY_AGG(DISTINCT polaris_partner IGNORE NULLS ORDER BY polaris_partner) AS partners
FROM `looker-studio-pro-452620.master_stg.data_model_v3`
WHERE qa_v3_source_detail_type = 'polaris_email'
GROUP BY 1, 2;
```

**Expected:** one row; grain `package_date_platform_campaign_ad_group_ad`; feeds
Meta and TikTok; platforms Facebook, Instagram, and TikTok; partner MIQ.

Run the three safety checks together:

```sql
WITH coverage AS (
  SELECT package_id, MIN(date) AS first_date, MAX(date) AS last_date
  FROM `looker-studio-pro-452620.landing.polaris_email_delivery_daily`
  GROUP BY package_id
), checks AS (
  SELECT 'mapping_conflicts' AS check_name, COUNT(*) AS failures
  FROM (
    SELECT source_feed, platform, campaign_name, ad_group_name
    FROM `looker-studio-pro-452620.landing.polaris_email_package_mapping`
    WHERE client_id = 'C70545844' AND connection_id = '11694' AND is_active
    GROUP BY 1, 2, 3, 4 HAVING COUNT(*) != 1
  )
  UNION ALL
  SELECT 'fpd_inside_polaris_coverage', COUNT(*)
  FROM `looker-studio-pro-452620.master_stg.data_model_v3` v
  JOIN coverage c ON v._package_id = c.package_id
    AND v._date BETWEEN c.first_date AND c.last_date
  WHERE v.qa_v3_source_detail_type IN ('fpd_original', 'fpd_updated_package')
  UNION ALL
  SELECT 'multiple_planned_carriers', COUNT(*)
  FROM (
    SELECT _package_id, _date
    FROM `looker-studio-pro-452620.master_stg.data_model_v3`
    GROUP BY 1, 2
    HAVING COUNTIF(_planned_spend IS NOT NULL OR _planned_impressions IS NOT NULL) > 1
  )
)
SELECT * FROM checks ORDER BY check_name;
```

**Expected:** three rows, each with `failures = 0`.

## Known Limits and Troubleshooting

- Scheduling is not configured; production loading is manual.
- New campaign or ad-group names require reviewed mappings.
- Meta and TikTok may legitimately have different newest dates.
- Preview files are local evidence, never production inputs.

| Symptom | Check |
|---|---|
| `permission denied` | Run the loader with `Rscript`; it is not a shell executable. |
| Unsupported or missing feed | Compare CSV columns with the schemas in the shared logic; ignore paths. |
| Newest-date tie | Decide which snapshot is authoritative; do not use filename or upload order. |
| Unmapped rows | Check the complete feed/platform/campaign/ad-group key. |
| Landing changed but V3 did not | Run the dependent refresh wrapper. |
| V3 is lower than landing | Check approved Manual Package Editor overlaps. |

## Definitions

[^1]: **Grain:** determines what one row represents and which fields can be safely grouped or summed.
[^2]: **Rebuild:** replaces the prior snapshot; rows absent from the validated candidate do not remain.
