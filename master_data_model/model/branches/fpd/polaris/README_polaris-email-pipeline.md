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

**Who this is for:** people loading, validating, or consuming Purely Elizabeth
Meta and TikTok delivery received through Polaris Email.

**What it covers:** how the files are identified, mapped to Prisma packages,
loaded safely, published in the master model, and verified.

**Where to go next:** use the
[FPD pipeline guide](/Users/eugenetsenter/Looker_clonedRepo/looker_personal/master_data_model/model/branches/fpd/README_fpd-pipeline.md)
for the wider first-party-data workflow and the
[Master Data Model Pipeline v2](/Users/eugenetsenter/Looker_clonedRepo/looker_personal/master_data_model/README_v2.md)
for downstream model behavior.

The final output for new work is the
[Master evidence model v3](https://console.cloud.google.com/bigquery?project=looker-studio-pro-452620&p=looker-studio-pro-452620&d=master_stg&t=data_model_v3&page=table).

## Table of Contents

- [Terms Used Below](#terms-used-below)
- [Canonical and Protected Surfaces](#canonical-and-protected-surfaces)
- [How the Data Arrives](#how-the-data-arrives)
- [Source and Output Contract](#source-and-output-contract)
- [How Rows Are Selected, Mapped, and Published](#how-rows-are-selected-mapped-and-published)
- [Output Fields and Precedence](#output-fields-and-precedence)
- [Known Gaps and Durable Warnings](#known-gaps-and-durable-warnings)
- [Run and Refresh](#run-and-refresh)
- [Verify Current State Yourself](#verify-current-state-yourself)
- [Maintenance](#maintenance)
- [Troubleshooting](#troubleshooting)
- [Definitions](#definitions)

## Terms Used Below

A **rolling snapshot** contains the available history through its newest business
date, not only rows added since the previous file. The **grain**[^1] of a Polaris
output row is package, date, platform, campaign, ad group, and ad. A successful
load performs a full **rebuild**[^2] of the Polaris landing snapshot after every
candidate row passes the contract[^3].

## Canonical and Protected Surfaces

The [FPD pipeline guide](/Users/eugenetsenter/Looker_clonedRepo/looker_personal/master_data_model/model/branches/fpd/README_fpd-pipeline.md)
owns project-level canonical status. For this workflow, use the surfaces below.

| Responsibility | Current surface | Rule |
|---|---|---|
| Production entrypoint | [Polaris Email loader](/Users/eugenetsenter/Looker_clonedRepo/looker_personal/master_data_model/model/branches/fpd/polaris/load_polaris_email_delivery.R) | Use `Rscript`; do not launch the `.R` file as a shell program. |
| Read-only review | [Polaris Email preview](/Users/eugenetsenter/Looker_clonedRepo/looker_personal/master_data_model/model/branches/fpd/polaris/preview_polaris_email_delivery.R) | Writes local evidence only. |
| Published output | [Master evidence model v3](https://console.cloud.google.com/bigquery?project=looker-studio-pro-452620&p=looker-studio-pro-452620&d=master_stg&t=data_model_v3&page=table) | Use for Polaris source detail and reporting metrics. |
| Refresh path | [Master-model refresh wrapper](/Users/eugenetsenter/Docs/R_Studio_Projects/universal_cron_runner/automation_hub/workloads/ops/bq_trigger/run_master_data_model_clustered_advertiser_refresh.sh) | Required after an approved production load. |

The source Cloud Storage prefix, the original and updated FPD landing tables, the
Manual Package Editor, and production model tables are protected. This workflow
may read them but must not rename, move, delete, patch, or directly edit them.
Production replacement is allowed only through the guarded loader; model refresh
is allowed only through the named builders or wrapper.

## How the Data Arrives

```text
Polaris Email Cloud Storage CSVs
  -> inspect every CSV header
  -> identify the Meta or TikTok schema
  -> select each feed's uniquely newest source-date snapshot
  -> normalize and validate source rows
  -> map each row to one approved Prisma package
  -> stage and atomically replace the Polaris landing snapshot
  -> refresh the stable model and V3
  -> verify live V3 fields, grain, precedence, and totals
```

Feed identity comes from the CSV columns. Filename, folder name, upload time,
and object-list order are never feed or freshness evidence.

| Feed | Required source columns |
|---|---|
| Meta | `campaign_name`, `adset_name`, `Platform`, `ad_name`, `date`, `Billable Spend`, `impressions`, `Link_click`, `video_view`, `Video View to 100%` |
| TikTok | `Campaign Name`, `Ad Group Name`, `Ad Name`, `Date Start`, `Billable Spend`, `Impressions`, `Clicks (Destination)`, `Video Views`, `Video Views at 100%` |

For each feed, the loader selects the single file with the greatest valid source
date. Older matching files remain visible as `superseded_snapshot` evidence. The
run stops before any warehouse write if a schema is unsupported or ambiguous, a
feed is missing, a file has no valid source date, or two files tie for newest.

## Source and Output Contract

| Source or layer | Purpose and grain | Boundary |
|---|---|---|
| Polaris Email CSVs | Rolling Meta or TikTok ad-level delivery snapshots | May feed only the guarded reader and loader. Paths cannot identify a feed. |
| [Package mapping](https://console.cloud.google.com/bigquery?project=looker-studio-pro-452620&p=looker-studio-pro-452620&d=landing&t=polaris_email_package_mapping&page=table) | One active package assignment per feed, platform, campaign, and ad-group key | Assigns package identity only; it must not supply delivery metrics. |
| [Polaris landing snapshot](https://console.cloud.google.com/bigquery?project=looker-studio-pro-452620&p=looker-studio-pro-452620&d=landing&t=polaris_email_delivery_daily&page=table) | One package/date/platform/campaign/ad-group/ad row | Source QA and lineage surface; do not treat it as a package/date rollup. |
| Original and updated FPD | Established package/date delivery evidence | Remains the fallback outside Polaris coverage and is never rewritten by Polaris. |
| Prisma planning data | Package identity, dates, metadata, and planned metrics | Supplies plan context, not Polaris actual delivery. |
| Manual Package Editor | Approved package/date corrections | Applies after source assembly and outranks source actuals. |
| [Compatibility model](https://console.cloud.google.com/bigquery?project=looker-studio-pro-452620&p=looker-studio-pro-452620&d=master_stg&t=data_model&page=table) | Package/date compatibility output | Preserves existing consumers but intentionally loses Polaris ad detail. |
| [V3 model](https://console.cloud.google.com/bigquery?project=looker-studio-pro-452620&p=looker-studio-pro-452620&d=master_stg&t=data_model_v3&page=table) | Natural Polaris detail plus the other master-model sources | Current output for new work. |

Each layer owns one decision: discovery lists objects; schema inspection identifies
feeds; snapshot selection chooses current files; normalization parses metrics and
keys; the mapping table assigns packages; the loader validates and replaces
landing; the stable model applies coverage precedence; V3 publishes source detail;
the Manual Package Editor applies approved corrections. Debug the first broken
contract rather than patching a later output.

## How Rows Are Selected, Mapped, and Published

The join key is the trimmed, case-insensitive combination of `source_feed`,
`platform`, `campaign_name`, and `ad_group_name`. Every row must match exactly one
active mapping. Platform alone is never enough to assign a package.

| Feed | Platform | Campaign | Ad group | Prisma package |
|---|---|---|---|---|
| Meta | Facebook | Awareness Campaign | ACR - Facebook | Facebook Awareness (`P3HF7QB`) |
| Meta | Facebook | Awareness Campaign | Interests - Facebook | Facebook Awareness (`P3HF7QB`) |
| Meta | Instagram | Awareness Campaign | ACR - Instagram | Instagram (`P3HF7T8`) |
| Meta | Instagram | Awareness Campaign | Interests - Instagram | Instagram (`P3HF7T8`) |
| TikTok | TikTok | Purely Elizabeth - Awareness Q3 | Interests | TikTok (`P3HF88Q`) |

The natural row key is feed, platform, date, campaign, ad group, and ad. After
normalization, the loader requires every row to be valid, uniquely keyed, mapped,
and reconciled to the parsed source totals. It then uploads a timestamped staging
table, repeats the gates in BigQuery, and replaces production in one transaction.
If a warehouse gate fails, production remains unchanged and the staging candidate
is retained for investigation.

## Output Fields and Precedence

Live V3 inspection on 2026-08-21 confirmed `polaris_email` rows at
`package_date_platform_campaign_ad_group_ad` grain, sourced from MIQ with Meta and
TikTok feeds and Facebook, Instagram, and TikTok platform values.

| Consumer need | Field to read | Meaning or warning |
|---|---|---|
| Polaris row filter | `qa_v3_source_detail_type` | `polaris_email` identifies these rows. |
| Detail grain | `qa_v3_metric_grain` | `package_date_platform_campaign_ad_group_ad`. |
| Package and date | `_package_id`, `_date` | Approved Prisma package and source business date. |
| Platform | `polaris_platform` | Live Polaris platform field. Do not use `s_platform`; it is currently empty on Polaris rows. |
| Campaign, ad group, ad | `polaris_campaign_name`, `polaris_ad_group_name`, `polaris_ad_name` | Source-detail names preserved for lineage. |
| Placement identity | `_placement_id` | A synthetic SHA-256 hash[^4] of feed, platform, campaign, and ad group; it does not include the ad. |
| Reporting actuals | `_spend`, `_impressions`, `_clicks`, `_video_views`, `_video_comps` | Canonical delivery metrics after precedence and manual-override rules. `_video_plays` is not populated by Polaris. |
| Raw source evidence | `polaris_raw_*`, `polaris_source_object_uri`, `polaris_source_row_number` | Use to trace the normalized value back to its source row. |
| Summable plan | `_planned_spend`, `_planned_impressions` | May be populated on at most one source row per package/date. |
| Repeated plan context | `qa_v3_package_planned_*_doNotSum` | Reference only; never sum across detail rows. |

Polaris replaces original and updated FPD actuals only between each mapped
package's minimum and maximum loaded Polaris dates. Existing FPD remains available
outside that interval and is never deleted. A valid Manual Package Editor override
still outranks Polaris. Planned metrics attach to one ranked row per package/date,
with Polaris ranked before the other automated actual sources.

## Known Gaps and Durable Warnings

- Scheduling remains deferred; the production entrypoint is manual.
- A new campaign or ad-group name stops the load until package ownership is reviewed.
- The exports provide names rather than durable platform IDs, so the maintained
  mapping cannot safely be replaced with generated name matches.
- Meta and TikTok may have different newest source dates. That difference is visible
  evidence, not an automatic failure.
- Preview CSVs are local evidence, excluded from Git, and never production inputs.
- Loader success proves only the landing replacement. It does not prove that V3 or
  a dashboard is fresh.

<details><summary>A vendor column rename stops the load before production changes</summary>

The required columns are an exact source contract. Review a vendor schema change,
update the tests and shared logic deliberately, and rerun the preview and dry run.
Do not work around it by recognizing a filename.

</details>

<details><summary>Cloud Storage cannot be queried as a direct BigQuery external table here</summary>

The source prefix and dataset use incompatible location scopes. The guarded
copy-and-load path is intentional.

</details>

## Run and Refresh

Run from `/Users/eugenetsenter/Looker_clonedRepo/looker_personal/master_data_model`.

| Work | Command | Expected result |
|---|---|---|
| Logic tests | `Rscript model/branches/fpd/polaris/tests/test_polaris_email_delivery_logic.R` | Ends with `All Polaris Email delivery logic tests passed.` |
| Read-only preview | `Rscript model/branches/fpd/polaris/preview_polaris_email_delivery.R --output-dir /tmp/polaris-email-delivery-preview --compare-live-model` | Completes without a warehouse write; inventory includes every CSV and exactly one selected file for each feed. The output folder must be new or empty. |
| Production dry run | `Rscript model/branches/fpd/polaris/load_polaris_email_delivery.R --dry-run` | All selected rows validate, map uniquely, and reconcile; no BigQuery table is created or replaced. |
| Approved production load | `Rscript model/branches/fpd/polaris/load_polaris_email_delivery.R` | Reports terminal production success after staging and production validation. Requires explicit approval because it replaces the live snapshot. |
| Dependent refresh | `/Users/eugenetsenter/Docs/R_Studio_Projects/universal_cron_runner/automation_hub/workloads/ops/bq_trigger/run_master_data_model_clustered_advertiser_refresh.sh` | Wrapper succeeds, reconciles the clustered table, preserves clustering, rebuilds V3, and passes visible-grain and planned-carrier checks. |

The preview proves file selection and transformation behavior. The dry run proves
the current source and mapping pass without writing. Neither proves production
freshness. A production-load message without the dependent refresh also does not
prove V3 freshness.

## Verify Current State Yourself

### Confirm the published Polaris field contract

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

**Expected result:** one row with source type `polaris_email`, grain
`package_date_platform_campaign_ad_group_ad`, feeds `meta` and `tiktok`, platforms
Facebook, Instagram, and TikTok, and partner MIQ.

### Find conflicting active mappings

```sql
SELECT source_feed, platform, campaign_name, ad_group_name, COUNT(*) AS mapping_rows
FROM `looker-studio-pro-452620.landing.polaris_email_package_mapping`
WHERE client_id = 'C70545844'
  AND connection_id = '11694'
  AND is_active
GROUP BY 1, 2, 3, 4
HAVING COUNT(*) != 1;
```

**Expected result:** no rows. Any result means the loader cannot assign that key
to exactly one package.

### Confirm older FPD is excluded inside Polaris coverage

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

**Expected result:** `conflicting_fpd_rows = 0`.

### Confirm planned metrics remain summable

```sql
SELECT _package_id, _date
FROM `looker-studio-pro-452620.master_stg.data_model_v3`
GROUP BY _package_id, _date
HAVING COUNTIF(_planned_spend IS NOT NULL OR _planned_impressions IS NOT NULL) > 1;
```

**Expected result:** no rows. Any result would allow package/date plan totals to be
counted more than once.

For source-code changes, the logic tests and current dry run own pre-write proof.
For a production landing replacement, the loader's terminal success plus landing
coverage and metric checks own proof. For stable-base or V3 behavior changes, SQL
Change Guard owns the broad comparison. For publication, the refresh wrapper and
the focused live queries above own proof.

Do not treat script startup, schema presence, row count alone, a dry run, or a
started-but-incomplete BigQuery job as proof of publication.

## Maintenance

- Run the logic tests and current dry run before every approved production load.
- Review the preview inventory whenever the number of source objects changes.
- Add mapping keys only after package ownership is confirmed; never infer them
  from platform alone.
- Refresh the dependent master-model tables in the same session after replacing
  the landing snapshot.
- Revise this guide when the source schema, mapping key, grain, precedence, field
  contract, or refresh path changes.

## Troubleshooting

| Symptom | Check first | Why |
|---|---|---|
| `permission denied` when launching the loader | Run it with `Rscript`. | The file is an R entrypoint, not a shell executable. |
| `unsupported CSV object` | Compare the CSV headers with the required schemas above. | Path names are intentionally ignored. |
| Missing Meta or TikTok schema | Inspect every CSV header and its source-date column. | A missing feed or vendor schema change can cause this; folder names cannot. |
| Newest-date tie | Compare the tied snapshots and identify the authoritative export. | Filename and upload order are unsafe tie-breakers. |
| Unmapped rows | Compare the full feed/platform/campaign/ad-group key with active mappings. | Package assignment requires all four values. |
| Duplicate natural key | Compare feed, platform, date, campaign, ad group, and ad. | Loading both copies would double-count delivery. |
| Warehouse validation failed | Inspect the retained staging table named in the error. | It preserves the exact failed candidate while production remains unchanged. |
| Landing changed but V3 did not | Run the dependent refresh and inspect its terminal result. | The loader does not rebuild V3. |
| V3 metrics are lower than landing | Check Manual Package Editor overlaps. | Valid manual corrections intentionally suppress source actuals. |

## Definitions

[^1]: **Grain:** determines what one row represents and therefore which fields may be safely grouped or summed.
[^2]: **Rebuild:** replaces the complete prior snapshot; anything absent from the validated candidate will not remain in the rebuilt table.
[^3]: **Contract:** defines the required inputs, keys, outputs, and failure conditions between stages; breaking it must stop the workflow rather than silently alter data.
[^4]: **Hash:** produces a repeatable fixed-length identity from source values; changing any included value creates a different placement ID.
