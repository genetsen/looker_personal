---
pipeline: MIQ Polaris Email Delivery
source_type: first-party delivery
output: looker-studio-pro-452620.master_stg.data_model_v3
output_grain: package_date_platform_campaign_ad_group_ad
source_tables:
  - looker-studio-pro-452620.landing.polaris_email_package_mapping
  - looker-studio-pro-452620.landing.polaris_email_delivery_daily
refresh: guarded manual loader followed by the master-model refresh wrapper
loader_script: load_polaris_email_delivery.R
verified: 2026-08-21
verified_against:
  - load_polaris_email_delivery.R --dry-run
  - preview_polaris_email_delivery.R
  - create_master_stg_data_model.sql
  - create_master_stg_data_model_v3.sql
---

# MIQ Polaris Email Delivery Pipeline

This pipeline turns rolling Meta and TikTok CSV exports delivered through Polaris
Email into guarded MIQ first-party delivery rows in the
[Master evidence model v3](https://console.cloud.google.com/bigquery?project=looker-studio-pro-452620&p=looker-studio-pro-452620&d=master_stg&t=data_model_v3&page=table).

Polaris Email is an ingestion path, not a new supplier or standalone social
source. MIQ remains the supplier; Facebook, Instagram, and TikTok remain the
platforms. Inside each loaded package's date coverage, Polaris Email replaces
the older modeled FPD path. Existing FPD evidence remains physically unchanged
and continues outside that coverage.

## Table of Contents

- [Terminology](#terminology)
- [Pipeline Overview](#pipeline-overview)
- [Canonical Files](#canonical-files)
- [Protected Surfaces](#protected-surfaces)
- [Sources and Boundaries](#sources-and-boundaries)
- [Layer Responsibilities](#layer-responsibilities)
- [How Files Are Selected](#how-files-are-selected)
- [How Rows Are Mapped and Loaded](#how-rows-are-mapped-and-loaded)
- [Output Grain and Field Meaning](#output-grain-and-field-meaning)
- [Precedence in the Master Model](#precedence-in-the-master-model)
- [Run and Refresh](#run-and-refresh)
- [Known Gaps and Durable Warnings](#known-gaps-and-durable-warnings)
- [Useful Queries](#useful-queries)
- [Verification Contract](#verification-contract)
- [Maintenance](#maintenance)
- [Troubleshooting](#troubleshooting)
- [Related Guides](#related-guides)

## Terminology

- **Polaris Email:** the Wpromote delivery mechanism that places the exported
  CSV snapshots in Cloud Storage. It describes how MIQ data arrives.
- **Feed:** one supported source shape. This pipeline currently supports Meta
  and TikTok feeds.
- **Schema:** the required CSV column names that identify a feed. Filenames and
  folder names do not identify feeds.
- **Rolling snapshot:** a file containing the available history through its
  newest source date, rather than only new rows since the previous file.
- **Business date:** the delivery date stored inside a source row. The loader
  uses the greatest valid business date in each file to choose the current
  rolling snapshot.
- **Mapping key:** feed, platform, campaign, and ad group together. That key
  assigns a source row to one approved Prisma package.
- **Natural row key:** feed, platform, date, campaign, ad group, and ad. It
  represents one unique source-detail row and must not repeat.
- **Coverage:** the minimum through maximum loaded date for one package. Polaris
  Email takes precedence over modeled FPD only inside that interval.
- **Grain:** what one output row represents. Polaris rows preserve package,
  date, platform, campaign, ad group, and ad detail.
- **Full replacement:** a successful production load replaces the entire prior
  Polaris delivery table in one transaction; it does not append new rows.

## Pipeline Overview

```text
Polaris Email Cloud Storage prefix
  -> inspect every CSV header
  -> identify Meta or TikTok schema
  -> choose each feed's uniquely newest business-date snapshot
  -> normalize and validate every source row
  -> join the active feed/platform/campaign/ad-group mapping
  -> upload and validate an isolated staging table
  -> atomically replace landing.polaris_email_delivery_daily
  -> refresh the package/date compatibility base and dependent tables
  -> publish natural-detail rows in master_stg.data_model_v3
```

The preview follows the same discovery, schema, selection, normalization, and
reconciliation logic but writes only local review files.

## Canonical Files

**What this means:** these are the supported files for this pipeline. No root-
level or alternate Polaris copies are canonical.

| Responsibility | Canonical file |
|---|---|
| Production loader | [load_polaris_email_delivery.R](/Users/eugenetsenter/Looker_clonedRepo/looker_personal/master_data_model/model/branches/fpd/polaris/load_polaris_email_delivery.R) |
| Shared schema, normalization, mapping, and reconciliation rules | [polaris_email_delivery_logic.R](/Users/eugenetsenter/Looker_clonedRepo/looker_personal/master_data_model/model/branches/fpd/polaris/polaris_email_delivery_logic.R) |
| Read-only preview | [preview_polaris_email_delivery.R](/Users/eugenetsenter/Looker_clonedRepo/looker_personal/master_data_model/model/branches/fpd/polaris/preview_polaris_email_delivery.R) |
| Logic tests | [test_polaris_email_delivery_logic.R](/Users/eugenetsenter/Looker_clonedRepo/looker_personal/master_data_model/model/branches/fpd/polaris/tests/test_polaris_email_delivery_logic.R) |
| Mapping-table setup | [create_polaris_email_package_mapping.sql](/Users/eugenetsenter/Looker_clonedRepo/looker_personal/master_data_model/model/branches/fpd/polaris/create_polaris_email_package_mapping.sql) |
| Delivery-table setup | [create_polaris_email_delivery_daily.sql](/Users/eugenetsenter/Looker_clonedRepo/looker_personal/master_data_model/model/branches/fpd/polaris/create_polaris_email_delivery_daily.sql) |
| Package/date precedence | [stable base builder](/Users/eugenetsenter/Looker_clonedRepo/looker_personal/master_data_model/model/stable_base/create_master_stg_data_model.sql) |
| Natural-detail output | [V3 builder](/Users/eugenetsenter/Looker_clonedRepo/looker_personal/master_data_model/model/final_model/create_master_stg_data_model_v3.sql) |
| Historical deployment decision | [Polaris Email V3 MVP plan](/Users/eugenetsenter/Looker_clonedRepo/looker_personal/master_data_model/model/branches/fpd/polaris-email-v3-mvp-plan.md) |

## Protected Surfaces

**What this means:** these objects contain source, user-owned, or production
state. Use the named workflow instead of writing to them directly.

| Surface | Protection rule |
|---|---|
| `gs://bkt-plrs-prd-data-imports-7nqd/data/client_id=C70545844/connection_id=11694/` | Read-only source prefix. Do not rename, move, overwrite, or delete vendor objects from this pipeline. |
| [Polaris package mapping](https://console.cloud.google.com/bigquery?project=looker-studio-pro-452620&p=looker-studio-pro-452620&d=landing&t=polaris_email_package_mapping&page=table) | Change mappings only through reviewed mapping SQL or another explicitly approved mapping owner. |
| [Polaris delivery snapshot](https://console.cloud.google.com/bigquery?project=looker-studio-pro-452620&p=looker-studio-pro-452620&d=landing&t=polaris_email_delivery_daily&page=table) | Replace only through the guarded loader. Direct append, delete, or manual editing bypasses its validation and transaction. |
| Original and updated FPD landing tables | Read-only evidence for this workflow. Polaris precedence must not delete or rewrite them. |
| Manual Package Editor tables and Sheet | User-owned override path. Polaris must not write to them or outrank valid manual edits. |
| `master_stg.data_model` and `master_stg.data_model_v3` | Shared production outputs. Refresh with their canonical builders or wrapper; do not patch individual rows. |

## Sources and Boundaries

**What this means:** several sources describe the same packages, but each has a
different responsibility. Similar subject matter does not make them interchangeable.

| Source | Purpose and grain | Boundary |
|---|---|---|
| Polaris Email Cloud Storage CSVs | Rolling Meta or TikTok creative-level delivery snapshots | May feed only the guarded Polaris reader/loader. A path name is not a feed contract. |
| [Package mapping table](https://console.cloud.google.com/bigquery?project=looker-studio-pro-452620&p=looker-studio-pro-452620&d=landing&t=polaris_email_package_mapping&page=table) | One active mapping per feed/platform/campaign/ad-group key | Assigns package identity only. It must not supply or alter delivery metrics. |
| [Delivery landing table](https://console.cloud.google.com/bigquery?project=looker-studio-pro-452620&p=looker-studio-pro-452620&d=landing&t=polaris_email_delivery_daily&page=table) | One package/date/platform/campaign/ad-group/ad source row | Full-snapshot source for the model. It is not a package/date rollup and must not be joined as though it were one. |
| Original FPD landing | Partner/package/date with available placement and creative evidence | Remains the fallback outside Polaris coverage. It is suppressed in the model, not deleted, inside coverage. |
| Updated FPD landing | Package/date revised spend and impressions | Remains the fallback outside Polaris coverage. It does not provide Polaris creative detail. |
| Prisma planning data | Package identity, dates, metadata, and planned metrics | Supplies plan/context, not Polaris actual delivery. |
| Manual Package Editor | Approved package/date corrections | Applies after source assembly and remains authoritative over Polaris metrics. |

## Layer Responsibilities

**What this means:** each stage owns one decision. Debug the first stage whose
contract is wrong instead of patching a downstream output.

| Stage | Responsibility | Does not own |
|---|---|---|
| Cloud Storage discovery | List every CSV under the approved client/connection prefix | Feed identity, package identity, or freshness by filename |
| Schema inspection | Identify Meta or TikTok from required headers | Which rolling snapshot is current |
| Snapshot selection | Choose one uniquely newest business-date file per feed | Package assignment or metric precedence |
| Shared normalization logic | Parse dates/metrics, preserve raw values, build natural keys, and reconcile totals | Warehouse writes |
| Active mapping table | Assign each feed/platform/campaign/ad-group key to one Prisma package | Delivery values or source-row validation |
| Guarded production loader | Validate, stage, and atomically replace the delivery landing snapshot | Refreshing the master model |
| Stable package/date base | Apply package-specific Polaris-versus-FPD coverage precedence | Natural creative-detail publication |
| V3 builder | Publish source-detail rows, final metrics, lineage, and one planned carrier | Source ingestion or mapping maintenance |
| Manual Package Editor | Apply approved package/date corrections after source assembly | Altering Polaris landing evidence |

## How Files Are Selected

**What this means:** feed identity comes from columns inside the CSV, and current
snapshot identity comes from delivery dates inside the rows. Neither decision
uses a filename, folder name, upload timestamp, or listing order.

### Supported schemas

| Feed | Required columns |
|---|---|
| Meta | `campaign_name`, `adset_name`, `Platform`, `ad_name`, `date`, `Billable Spend`, `impressions`, `Link_click`, `video_view`, `Video View to 100%` |
| TikTok | `Campaign Name`, `Ad Group Name`, `Ad Name`, `Date Start`, `Billable Spend`, `Impressions`, `Clicks (Destination)`, `Video Views`, `Video Views at 100%` |

The loader removes a UTF-8 byte-order marker from the first header before
classification. A file matching neither schema is unsupported. A file matching
more than one supported schema is ambiguous.

### Current-snapshot rule

For each feed, the loader finds the greatest valid source date represented in
each matching file. It selects the single file with the newest date and marks
older files `superseded_snapshot` in the preview inventory.

The run stops before mapping or upload when:

- Meta or TikTok is missing;
- a CSV has an unsupported or ambiguous schema;
- a supported file has no valid source date; or
- two files for one feed tie for the newest source date.

Meta and TikTok are selected independently. Their newest dates may differ; that
is visible evidence, not an automatic failure.

## How Rows Are Mapped and Loaded

**What this means:** recognizing a feed does not authorize a package assignment.
Every source row must separately match one active warehouse mapping.

### Approved mapping keys

| Feed | Platform | Campaign | Ad group | Package |
|---|---|---|---|---|
| Meta | Facebook | Awareness Campaign | ACR - Facebook | Facebook Awareness (`P3HF7QB`) |
| Meta | Facebook | Awareness Campaign | Interests - Facebook | Facebook Awareness (`P3HF7QB`) |
| Meta | Instagram | Awareness Campaign | ACR - Instagram | Instagram (`P3HF7T8`) |
| Meta | Instagram | Awareness Campaign | Interests - Instagram | Instagram (`P3HF7T8`) |
| TikTok | TikTok | Purely Elizabeth - Awareness Q3 | Interests | TikTok (`P3HF88Q`) |

Names are matched case-insensitively after trimming. The loader does not infer a
package from platform alone and stops when a key is unmapped or maps to multiple
active packages.

### Replacement sequence

1. Normalize the two selected files without collapsing creative detail.
2. Require every row to be mapped, valid, and unique at its natural key.
3. Reconcile parsed and normalized row/metric totals exactly.
4. Create a timestamped staging table with a deletion-safe description.
5. Upload the candidate and repeat row-count, mapping, required-field, and
   duplicate checks in BigQuery.
6. In one transaction, delete the old production snapshot and insert the
   validated staging rows.
7. Delete the staging table after success. If warehouse validation fails, keep
   it for investigation and leave production unchanged.

## Output Grain and Field Meaning

**What this means:** one Polaris V3 row is one package, date, platform, campaign,
ad group, and ad. Summing repeated package-level context would overstate totals.

### Output objects

| Object | Current role |
|---|---|
| [Polaris delivery landing](https://console.cloud.google.com/bigquery?project=looker-studio-pro-452620&p=looker-studio-pro-452620&d=landing&t=polaris_email_delivery_daily&page=table) | Canonical normalized Polaris source snapshot at natural detail grain. Use it for source QA and lineage. |
| [Master evidence model](https://console.cloud.google.com/bigquery?project=looker-studio-pro-452620&p=looker-studio-pro-452620&d=master_stg&t=data_model&page=table) | Compatibility package/date view. It applies Polaris coverage precedence but intentionally aggregates source detail. |
| [Master evidence model v3](https://console.cloud.google.com/bigquery?project=looker-studio-pro-452620&p=looker-studio-pro-452620&d=master_stg&t=data_model_v3&page=table) | Current production output for new work. Use it when Polaris platform, campaign, ad-group, ad, raw metric, or source-object lineage matters. |
| Clustered advertiser support table | Stored compatibility support refreshed with V3 dependencies. Do not use it as the Polaris source of truth. |

### Field contract

| Field | Meaning and safe use |
|---|---|
| `qa_v3_source_detail_type` | Use `polaris_email` to isolate Polaris source rows. |
| `qa_v3_metric_grain` | `package_date_platform_campaign_ad_group_ad` for Polaris rows. |
| `_package_id`, `_date` | Approved Prisma package and source delivery date. |
| `_placement_id` | SHA-256 identity from feed, platform, campaign, and ad group. It does not include the ad. |
| `_placement_name` | Polaris ad-group name. |
| `_creative_name` | Polaris ad name. |
| `_spend`, `_impressions`, `_clicks` | Canonical reporting metrics after precedence and manual-override checks. |
| `_video_views`, `_video_comps` | Polaris video views and 100-percent completions. Polaris does not populate `_video_plays`. |
| `fpd_*` | FPD-compatible evidence fields used by the model's existing actuals logic. |
| `polaris_*` | Raw Polaris lineage: partner, ingestion path, feed, platform, campaign, ad group, ad, object URI, row number, raw metric text, and load time. |
| `_planned_spend`, `_planned_impressions` | Summable on at most one natural row per package/date. Do not fill or sum repeated plan context as delivery. |
| `qa_v3_package_planned_*_doNotSum` | Repeated package-level context for reference only. Never sum it across source-detail rows. |

## Precedence in the Master Model

**What this means:** source selection happens twice—first among rolling Polaris
files, then among modeled delivery paths for the same package and date.

1. The landing table defines each mapped package's minimum and maximum loaded
   Polaris dates.
2. Inside that interval, original and updated FPD rows are excluded from modeled
   actuals and Polaris supplies the FPD-compatible metrics.
3. Outside that interval, the established original/updated FPD behavior remains.
4. The underlying FPD landing rows are never deleted.
5. A valid Manual Package Editor delivery override suppresses Polaris and other
   source actuals for its package/date.
6. Planned metrics are attached to only one ranked source row per package/date;
   Polaris ranks before original FPD, updated FPD, and DCM for that carrier.

## Run and Refresh

**What this means:** a passing loader changes only the landing snapshot. The
master-model refresh and downstream proof are separate required steps.

Run commands from the `master_data_model` folder.

### Read-only preview

```bash
Rscript model/branches/fpd/polaris/preview_polaris_email_delivery.R \
  --output-dir /tmp/polaris-email-delivery-preview \
  --compare-live-model
```

The output folder must be new or empty. The preview writes exact copies of the
two selected source files, the complete source inventory, normalized rows,
mapping review, source reconciliation, optional FPD overlap, and a readable run
summary. It does not change cloud data.

### Logic tests

```bash
Rscript model/branches/fpd/polaris/tests/test_polaris_email_delivery_logic.R
```

### Production dry run

```bash
Rscript model/branches/fpd/polaris/load_polaris_email_delivery.R --dry-run
```

This reads Cloud Storage and the live mapping table but does not create, replace,
update, or delete BigQuery tables.

### Production snapshot replacement

```bash
Rscript model/branches/fpd/polaris/load_polaris_email_delivery.R
```

This is a production write. Run it only after the dry-run evidence is reviewed
and the replacement is explicitly approved.

### Dependent master-model refresh

After a successful production replacement, run the canonical wrapper:

```bash
/Users/eugenetsenter/Docs/R_Studio_Projects/universal_cron_runner/automation_hub/workloads/ops/bq_trigger/run_master_data_model_clustered_advertiser_refresh.sh
```

The wrapper must finish successfully, reconcile the clustered support table,
preserve `_advertiser` clustering, rebuild V3, and pass its visible-grain and
planned-carrier checks before the new source snapshot is considered published.

The two table-creation SQL files are bootstrap/schema tools, not daily refresh
commands. They mutate BigQuery and require separate approval when used.

## Known Gaps and Durable Warnings

- Scheduling remains deferred. The supported production entrypoint is manual.
- The mapping table owns only the approved campaign/ad-group keys. A new name in
  either feed stops the load until its package ownership is reviewed.
- Source identity is name-based because the exports do not provide durable
  platform IDs. Do not replace the maintained mapping with generated name hashes.
- The feed schemas are exact contracts. A vendor column rename is a source-
  contract change and should fail visibly until reviewed.
- Meta and TikTok can have different newest business dates. A successful load
  means each feed had one uniquely newest snapshot, not that their dates match.
- The Cloud Storage prefix and BigQuery dataset are in incompatible location
  scopes for a direct external table; the guarded copy-and-load path is intentional.
- Preview CSVs are local evidence, excluded from Git, and never production inputs.
- Loader success does not prove that V3 or a dashboard is fresh. The dependent
  refresh and live output queries below own that proof.

## Useful Queries

### Current landing coverage and freshness

```sql
SELECT
  package_id,
  source_feed,
  MIN(date) AS minimum_date,
  MAX(date) AS maximum_date,
  COUNT(*) AS source_rows,
  MAX(loaded_at) AS loaded_at
FROM `looker-studio-pro-452620.landing.polaris_email_delivery_daily`
GROUP BY package_id, source_feed
ORDER BY package_id, source_feed;
```

### Active mapping ownership

```sql
SELECT
  source_feed,
  platform,
  campaign_name,
  ad_group_name,
  ARRAY_AGG(package_id ORDER BY package_id) AS package_ids,
  COUNT(*) AS active_mapping_rows
FROM `looker-studio-pro-452620.landing.polaris_email_package_mapping`
WHERE client_id = 'C70545844'
  AND connection_id = '11694'
  AND is_active
GROUP BY 1, 2, 3, 4
ORDER BY 1, 2, 3, 4;
```

Every row should have exactly one active mapping and one package ID.

### V3 rows and landing-metric reconciliation

```sql
WITH landing AS (
  SELECT
    SUM(spend) AS spend,
    SUM(impressions) AS impressions,
    SUM(clicks) AS clicks,
    SUM(video_views) AS video_views,
    SUM(video_completions) AS video_completions
  FROM `looker-studio-pro-452620.landing.polaris_email_delivery_daily`
),
v3 AS (
  SELECT
    SUM(_spend) AS spend,
    SUM(_impressions) AS impressions,
    SUM(_clicks) AS clicks,
    SUM(_video_views) AS video_views,
    SUM(_video_comps) AS video_completions
  FROM `looker-studio-pro-452620.master_stg.data_model_v3`
  WHERE qa_v3_source_detail_type = 'polaris_email'
)
SELECT
  v3.spend - landing.spend AS spend_difference,
  v3.impressions - landing.impressions AS impressions_difference,
  v3.clicks - landing.clicks AS clicks_difference,
  v3.video_views - landing.video_views AS video_views_difference,
  v3.video_completions - landing.video_completions AS video_completions_difference
FROM landing, v3;
```

All differences should be zero unless an approved manual override suppresses a
Polaris package/date in V3. When a difference exists, check manual overrides
before treating it as a source-load defect.

### FPD exclusion inside Polaris coverage

```sql
WITH coverage AS (
  SELECT package_id, MIN(date) AS minimum_date, MAX(date) AS maximum_date
  FROM `looker-studio-pro-452620.landing.polaris_email_delivery_daily`
  GROUP BY package_id
)
SELECT COUNT(*) AS conflicting_fpd_rows
FROM `looker-studio-pro-452620.master_stg.data_model_v3` AS v
JOIN coverage AS c
  ON v._package_id = c.package_id
 AND v._date BETWEEN c.minimum_date AND c.maximum_date
WHERE v.qa_v3_source_detail_type IN ('fpd_original', 'fpd_updated_package');
```

The result should be zero.

### One summable planned carrier per package/date

```sql
SELECT _package_id, _date
FROM `looker-studio-pro-452620.master_stg.data_model_v3`
GROUP BY _package_id, _date
HAVING COUNTIF(_planned_spend IS NOT NULL OR _planned_impressions IS NOT NULL) > 1;
```

The query should return no rows.

## Verification Contract

**What this means:** different changes require different proof. Reusing one
successful owner is safer than stacking unrelated checks that appear thorough.

| Work type | Proof owner | Minimum acceptable proof |
|---|---|---|
| Schema, selection, normalization, or mapping-code change | Logic tests plus the real `--dry-run` | Tests pass; live inventory identifies current files by schema/date; all rows map, validate, remain unique, and reconcile exactly. |
| Preview-only change | Preview command and generated artifacts | Run completes in a new folder; inventory records every file and selection status; normalized and reconciliation artifacts agree. |
| Production landing replacement | Guarded loader plus landing queries | Terminal production success; staging gate passes; production table has the intended load time, coverage, keys, and metrics. |
| Stable-base or V3 behavior change | SQL Change Guard | Schema, package/date coverage, metrics, precedence, supplier, manual override, planned carrier, and unrelated-package checks pass. |
| Published source refresh | Canonical dependent-refresh wrapper plus focused V3 queries | Wrapper succeeds; dependent tables are fresh; V3 reconciles to landing with approved manual exceptions. |

The following are not completion proof by themselves:

- `Rscript` exiting without a production-success message;
- a dry run when production freshness is being claimed;
- a passing schema check without mapping and row validation;
- a successful landing load without dependent model refresh;
- row counts without metric, grain, precedence, and lineage checks; or
- a transfer or query job that has started but has not reached terminal success.

## Maintenance

- Run the logic tests before changing supported headers, dates, mapping keys, or
  normalization rules.
- Run `--dry-run` before every approved production replacement.
- Review `source_inventory.csv` when the number of source objects changes; older
  objects are expected, but unsupported schemas and newest-date ties are not.
- Add mapping keys deliberately through the mapping owner; never infer a package
  from platform alone.
- Refresh the dependent master-model tables in the same session after a source
  replacement.
- Keep this guide, the FPD branch guide, and the architecture map aligned when
  the stable source semantics, precedence, grain, or refresh path changes.

## Troubleshooting

**What this means:** begin with the failing stage and preserve the last good
production snapshot. A nearby warning is not automatically the cause.

| Symptom | Check | Why |
|---|---|---|
| `permission denied` when launching the loader | Invoke it with `Rscript`; do not execute the `.R` file as a shell program. | The file is an R entrypoint, not a directly executable binary. |
| `unsupported CSV object` | Compare the file headers with [Supported schemas](#supported-schemas). | Paths are intentionally ignored; an unsupported message means the columns do not match. |
| `found no Meta-schema CSV` or `found no TikTok-schema CSV` | Inspect every CSV header and confirm the required columns still exist. | A renamed folder cannot cause this; a missing feed or schema change can. |
| `tied for newest source date` | Compare the tied files' contents and determine which source snapshot is authoritative. | Upload order and filenames are not safe tie-breakers. |
| `unmapped row(s)` | Group the rejected rows by feed, platform, campaign, and ad group; compare with the active mapping table. | Package assignment requires the full maintained mapping key. |
| `duplicate natural key` | Compare feed, platform, date, campaign, ad group, and ad across the selected files. | Loading both copies would double-count source delivery. |
| Warehouse validation failed | Inspect the retained staging table named in the error. | Production remains unchanged; the staging table is the exact failed candidate. |
| Landing changed but V3 did not | Run the dependent-refresh wrapper and inspect its terminal result. | The loader does not rebuild V3. |
| V3 is lower than landing | Check Manual Package Editor overlaps before investigating load loss. | Valid manual actuals intentionally suppress source metrics. |
| Package is missing outside Polaris dates | Trace original and updated FPD separately. | Polaris owns only the package's loaded minimum-to-maximum interval. |

## Related Guides

- [FPD pipeline](/Users/eugenetsenter/Looker_clonedRepo/looker_personal/master_data_model/model/branches/fpd/README_fpd-pipeline.md)
- [Master Data Model Pipeline v2](/Users/eugenetsenter/Looker_clonedRepo/looker_personal/master_data_model/README_v2.md)
- [Polaris Email V3 MVP plan](/Users/eugenetsenter/Looker_clonedRepo/looker_personal/master_data_model/model/branches/fpd/polaris-email-v3-mvp-plan.md)
- [Master-model architecture map](/Users/eugenetsenter/Looker_clonedRepo/looker_personal/master_data_model/docs/master-data-model-map.html)
