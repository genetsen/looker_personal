---
pipeline: MIQ Polaris Email Delivery
source_type: first-party delivery
output: looker-studio-pro-452620.master_stg.data_model_v3
output_grain: package_date_platform_campaign_ad_group_ad
source_tables:
  - looker-studio-pro-452620.landing.polaris_email_package_mapping
  - looker-studio-pro-452620.landing.polaris_email_delivery_daily
refresh: universal runner jobs 12 and 20, or direct loader followed by the refresh wrapper
loader_script: load_polaris_email_delivery.R
verified: 2026-09-10
verified_against:
  - production runner jobs 12 and 20 with thirteen waiting Meta and TikTok objects through September 9
  - restrictive-text-setting header and late-backfill regression tests
  - production runner jobs 12 and 20 with August 27 Meta and TikTok objects
  - case-insensitive Meta and TikTok header regression fixtures
  - universal runner jobs 12 and 20 with a new Meta object
  - runner-owned Polaris wrapper --dry-run with no new source files
  - in-memory August 24 terminal-report regression fixture
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

A source file contains a recent delivery window through its newest source date;
it is not guaranteed to contain all prior history. One output row has
package/date/platform/campaign/ad-group/ad **grain**[^1].
A successful load **upserts**[^2] rows from new source objects.

## Safe Operating Path

| Need | Use |
|---|---|
| Load | [Production loader](/Users/eugenetsenter/Looker_clonedRepo/looker_personal/master_data_model/model/branches/fpd/polaris/load_polaris_email_delivery.R) |
| Scheduled load | Universal runner job 12, `Polaris Email Loader` |
| Initialize source state once | [State-table setup](/Users/eugenetsenter/Looker_clonedRepo/looker_personal/master_data_model/model/branches/fpd/polaris/create_polaris_email_source_state.sql) |
| Review without writing | [Preview](/Users/eugenetsenter/Looker_clonedRepo/looker_personal/master_data_model/model/branches/fpd/polaris/preview_polaris_email_delivery.R) |
| Consume Polaris detail | [V3 model](https://console.cloud.google.com/bigquery?project=looker-studio-pro-452620&p=looker-studio-pro-452620&d=master_stg&t=data_model_v3&page=table) |
| Refresh downstream models | [Refresh wrapper](/Users/eugenetsenter/Docs/R_Studio_Projects/universal_cron_runner/automation_hub/workloads/ops/bq_trigger/run_master_data_model_clustered_advertiser_refresh.sh) |

Do not directly change vendor files, FPD landing tables, Manual Package Editor
data, production tables, or scheduler configuration. The loader is the only
approved Polaris landing-table writer; the named builders and wrapper own model
refreshes.

## Data Contract

```text
Cloud Storage object metadata
  -> discard accepted generations
  -> identify new Meta or TikTok files from header bytes
  -> download every object added after each feed's last successful checkpoint
  -> process all arrivals, including late backfills, validate, and map every row
  -> keep the newest copy only where delivery windows overlap
  -> update overlapping natural keys, append new keys, and advance feed state
  -> refresh V3
```

Filenames and folder names never identify a feed. Cloud Storage generation and
creation time identify new objects; CSV columns identify Meta or TikTok without
treating capitalization, order, or UTF-8 file markers as meaningful. Late backfills
can arrive after newer reports because the loader preserves the newest date covered
by the complete batch. Duplicate columns that differ only by case remain invalid
because the loader cannot safely choose between them. The loader stops before
writing on unsupported schemas, invalid dates, backward batch coverage, mapping
failures, or duplicate rows. No new object is a clean no-op.

| Source | Purpose | Boundary |
|---|---|---|
| Polaris CSVs | Rolling Meta or TikTok ad delivery | Feed identity comes only from required columns. |
| Source state | One accepted object generation per feed | Updated in the same transaction as that feed's landing rows. |
| [Package mapping](https://console.cloud.google.com/bigquery?project=looker-studio-pro-452620&p=looker-studio-pro-452620&d=landing&t=polaris_email_package_mapping&page=table) | Assign one Prisma package | Supplies identity, never metrics. |
| [Polaris landing](https://console.cloud.google.com/bigquery?project=looker-studio-pro-452620&p=looker-studio-pro-452620&d=landing&t=polaris_email_delivery_daily&page=table) | Normalized ad-level snapshot | Source QA surface, not a package/date rollup. |
| Original and updated FPD | Independent partner-reported evidence | Retained inside and outside Polaris coverage; Polaris never rewrites or replaces the `fpd_*` evidence fields. |
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

Metadata and source state own freshness; schema inspection owns feed identity;
mapping owns package identity; the loader owns natural-key upserts; the model
owns precedence. Fix the first failed stage rather than patching a later output.

## Output Contract

| Consumer need | Field |
|---|---|
| Select Polaris rows | `qa_v3_source_detail_type = 'polaris_email'` |
| Confirm grain | `qa_v3_metric_grain = 'package_date_platform_campaign_ad_group_ad'` |
| Package/date | `_package_id`, `_date` |
| Platform | `polaris_platform` — `s_platform` is empty for current Polaris rows |
| Campaign/ad group/ad | `polaris_campaign_name`, `polaris_ad_group_name`, `polaris_ad_name` |
| Reporting actuals | `_spend`, `_impressions`, `_clicks`, `_video_views`, `_video_comps` |
| Polaris evidence | `polaris_spend`, `polaris_impressions`, `polaris_clicks`, `polaris_video_views`, `polaris_video_completions` |
| FPD evidence | `fpd_spend`, `fpd_impressions`, `fpd_clicks`, `fpd_video_views`, `fpd_video_completions` |
| Source trace | `polaris_raw_date` is `DATE`; `polaris_raw_spend` is `NUMERIC`; raw impression, click, and video fields are `INT64`; object URI and row number retain source identity. |
| Summable plan | `_planned_spend`, `_planned_impressions` on at most one row per package/date |
| Reference-only plan | `qa_v3_package_planned_*_doNotSum` — never sum these fields |

FPD and Polaris are separate evidence sources at all dates. Between each package's
minimum and maximum loaded Polaris dates, Polaris supplies only the final reporting
fields (`_spend`, `_impressions`, `_clicks`, `_video_views`, and `_video_comps`);
the FPD rows and `fpd_*` values remain visible for comparison. Manual corrections
still win as a later approved override. The compatibility model keeps both evidence
families at package/date grain; use V3 when Polaris ad detail is required.

## Run and Prove

Run from the `master_data_model` folder.

| Step | Command | Pass condition |
|---|---|---|
| One-time setup | Run `create_polaris_email_source_state.sql` in BigQuery | State contains one current row for Meta and TikTok. Requires explicit approval. |
| Tests | `Rscript --no-init-file model/branches/fpd/polaris/tests/test_polaris_email_delivery_logic.R` | Ends with `All Polaris Email delivery logic tests passed.` and proves the reader-facing report against an in-memory fixture. |
| Preview | `Rscript model/branches/fpd/polaris/preview_polaris_email_delivery.R --output-dir /tmp/polaris-email-delivery-preview --compare-live-model` | Selects one file per feed and writes local evidence only. |
| Dry run | `Rscript --no-init-file model/branches/fpd/polaris/load_polaris_email_delivery.R --dry-run` | Reads headers only for new candidates, downloads selected objects, and validates without table changes or personal R startup output. |
| Approved direct load | `Rscript --no-init-file model/branches/fpd/polaris/load_polaris_email_delivery.R` | Replaces only matching natural rows, appends new rows, advances source state, and prints the exact ingested objects plus package-level production changes. Requires explicit approval. |
| Runner load and publish | `Rscript /Users/eugenetsenter/Docs/R_Studio_Projects/universal_cron_runner/universal_script_runner.R --12 --20` | Runs the loader first, then passes the clustered/V3 checks. |

Tests plus the live dry run prove source-code changes before writing. The loader's
terminal success names every ingested Cloud Storage object, its incoming date range,
rows replaced and added, package coverage before and after, source ad-group assignment,
and compact before/after/change values for production spend and impressions. Those
production totals are an optional reader summary: if BigQuery temporarily cannot
return them after the validated write, the loader warns and still reports the data
update as successful. Mapping, uniqueness, required-value, date, and metric checks
remain mandatory and still stop an unsafe update. The
refresh wrapper plus the queries below prove publication. Script startup, schema
presence, row count alone, a dry run, or a started BigQuery job do not prove
publication. A no-new-file dry run also cannot prove the complete production report;
that proof requires a genuinely new source object.

Maintenance is limited to four habits: test and dry-run before loading; review
new-object failures; add mappings only after package ownership is confirmed; and
refresh downstream models in the same session as a production load.

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
  SELECT 'missing_fpd_evidence_inside_polaris_coverage', COUNTIF(
    v.fpd_spend IS NULL
    AND v.fpd_impressions IS NULL
    AND v.fpd_clicks IS NULL
    AND v.fpd_video_views IS NULL
    AND v.fpd_video_completions IS NULL
  )
  FROM `looker-studio-pro-452620.master_stg.data_model_v3` v
  JOIN coverage c ON v._package_id = c.package_id
    AND v._date BETWEEN c.first_date AND c.last_date
  WHERE v.qa_v3_source_detail_type = 'fpd_original'
  UNION ALL
  SELECT 'non_polaris_final_metrics_inside_polaris_coverage', COUNTIF(
    v.qa_v3_source_detail_type != 'polaris_email'
    AND (v._spend IS NOT NULL OR v._impressions IS NOT NULL OR v._clicks IS NOT NULL
      OR v._video_views IS NOT NULL OR v._video_comps IS NOT NULL)
  )
  FROM `looker-studio-pro-452620.master_stg.data_model_v3` v
  JOIN coverage c ON v._package_id = c.package_id
    AND v._date BETWEEN c.first_date AND c.last_date
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

**Expected:** four rows, each with `failures = 0`. This proves that FPD evidence
remains present while only Polaris rows carry final metrics in Polaris coverage.

## Known Limits and Troubleshooting

- The universal runner schedules the loader; direct `Rscript` execution remains supported for approved manual runs.
- The source-state table is deployed and tracks one accepted generation per feed.
- New campaign or ad-group names require reviewed mappings.
- Meta and TikTok may legitimately have different newest dates.
- Preview files are local evidence, never production inputs.

| Symptom | Check |
|---|---|
| `permission denied` | Run the loader with `Rscript`; it is not a shell executable. |
| Source state is not initialized | Run the state-table setup after verifying the current landing objects. |
| Unsupported new object | Compare its meaningful CSV columns with the shared schemas; encoding markers, capitalization, and column order do not matter. |
| Snapshot moved backward | Confirm whether the vendor uploaded stale history or an intentional correction. |
| Unmapped rows | Check the complete feed/platform/campaign/ad-group key. |
| Landing changed but V3 did not | Run the dependent refresh wrapper. |
| V3 is lower than landing | Check approved Manual Package Editor overlaps. |

## Definitions

[^1]: **Grain:** determines what one row represents and which fields can be safely grouped or summed.
[^2]: **Upsert:** updates a row when its natural key already exists and appends it when the key is new; older non-overlapping history remains unchanged.
