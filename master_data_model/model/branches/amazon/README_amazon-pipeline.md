---
pipeline: Amazon Ads
source_type: delivery actuals + commercial outcomes — no plan
output: looker-studio-pro-452620.master_stg.data_model_v3
output_grain: source_amazon_daily
source_tables:
  - looker-studio-pro-452620.landing.rit_amzn_report_daily
refresh: none in this repo — landing table loaded by an email process outside master_data_model
loader_script: none — there is no branch-local code
verified: 2026-08-21
verified_against:
  - model/stable_base/create_master_stg_data_model.sql (amazon_final, lines 1453–1623)
  - model/final_model/create_master_stg_data_model_v3.sql (lines 129–139, 653–706)
  - live looker-studio-pro-452620.master_stg.data_model_v3
reviewers:
  - gene <gene.tsenter@giantspoon.com>
---

# Amazon Ads Pipeline

Amazon Ads (Ritual, `RTL`) delivery enters the master data model as **non-digital
source rows** under synthetic keys[^2], because Amazon reports campaign/ad-group/ad
IDs and never a Prisma package ID.

**This branch folder contains no SQL, script, or notebook.** All Amazon logic is the
`amazon_final` CTE at **lines 1453–1623** of
[create_master_stg_data_model.sql](/Users/eugenetsenter/Looker_clonedRepo/looker_personal/master_data_model/model/stable_base/create_master_stg_data_model.sql).
There has never been a branch script — do not go looking for one.

| To do this | Use |
|---|---|
| Isolate Amazon rows | `qa_media_data_type = 'amazon_ads'` — identical to `qa_v3_source_detail_type = 'amazon_ads'`, `qa_row_data_source_primary = 'amazon_ads'`, `qa_v3_metric_grain = 'source_amazon_daily'`, and `_package_id LIKE 'amazon_ads:%'` (all select the same 5,091 rows, *verified 2026-08-21*) |
| Read Amazon media spend | `_spend`, sourced from `total_cost` — **not** `supply_cost`. See [Output Fields](#output-fields-and-precedence) |
| Read Amazon's own values | `amzn_*` — unchanged source strings |
| Read sales / purchases / units | `amzn_sales*`, `amzn_purchases*`, `amzn_units_sold*` — outcomes, never delivery |

## Table of Contents

- [Canonical files](#canonical-files) · [Protected surfaces](#protected-surfaces)
- [How the Data Arrives](#how-the-data-arrives)
- [Source and Output Contract](#source-and-output-contract)
- [How Amazon Rows Are Built](#how-amazon-rows-are-built)
- [Output Fields and Precedence](#output-fields-and-precedence)
- [Known Gaps and Gotchas](#known-gaps-and-gotchas)
- [Verify Current State Yourself](#verify-current-state-yourself) · [Definitions](#definitions)

---

## Canonical files

Authority order for any disagreement: **live `data_model_v3` → semantic model
documentation → repository SQL.**

| Purpose | File | Amazon lines |
|---|---|---|
| Amazon normalization and field naming | [create_master_stg_data_model.sql](/Users/eugenetsenter/Looker_clonedRepo/looker_personal/master_data_model/model/stable_base/create_master_stg_data_model.sql) | `amazon_final` 1453–1623; union at 1625; `row_callouts` 1980; output naming 2249–2349 |
| Final model | [create_master_stg_data_model_v3.sql](/Users/eugenetsenter/Looker_clonedRepo/looker_personal/master_data_model/model/final_model/create_master_stg_data_model_v3.sql) | `package_plan_daily` 129–139; `non_digital_source_ranked` 653–672; `non_digital_source_rows` 674–706 |
| Layer-wide behavior — manual overrides, precedence, refresh (not repeated here) | [Stable Base README](/Users/eugenetsenter/Looker_clonedRepo/looker_personal/master_data_model/model/stable_base/README.md) · [Final Model README](/Users/eugenetsenter/Looker_clonedRepo/looker_personal/master_data_model/model/final_model/README.md) | — |

## Protected surfaces

| Surface | Rule |
|---|---|
| [`landing.rit_amzn_report_daily`](https://console.cloud.google.com/bigquery?project=looker-studio-pro-452620&p=looker-studio-pro-452620&d=landing&t=rit_amzn_report_daily&page=table) | The actual source, written by a loader outside this repo. Read-only from here; a manual write is unrecoverable lineage loss. |
| [`master_stg.data_model`](https://console.cloud.google.com/bigquery?project=looker-studio-pro-452620&p=looker-studio-pro-452620&d=master_stg&t=data_model&page=table) | **A view, not a table.** Editing it is a schema change for every consumer. |
| `landing.master_data_model_manual_package_daily` | Manual overrides that suppress Amazon actuals. Owned by the Manual Package Editor. |
| `qa_data_issues` | Rebuilt downstream from an allow-list — never write a branch token into it. See the gotcha below. |

---

## How the Data Arrives

There is **no loader script and no scheduled query in this repository.**
`landing.rit_amzn_report_daily` is populated by an email-attachment ingestion process
living outside `master_data_model/`; its only trace in the model is the lineage
family `amzn_source_email_id`, `amzn_source_email_timestamp`, `amzn_source_filename`,
`amzn_source_file_size_bytes`, `amzn_loaded_at`. **Which system runs that loader is
undocumented anywhere in this repo** — locating the BigQuery job or Cloud Function
that writes the table would settle it.

Source freshness is `MAX(loaded_at)`, surfaced as `qa_data_source_refresh_at`
(stable base 54–57 and 2218–2219). Latest load: **2026-08-21 18:41:18 UTC**.

One filter stands between source and model, at line 1622:
`WHERE SAFE.PARSE_DATE('%b %e, %Y', date) >= DATE '2025-01-01'`. Amazon supplies its
date as text such as `Aug 20, 2026`, so an unparseable date becomes NULL and is
dropped silently. **Today it drops nothing** — 5,091 landing rows produce 5,091 model
rows, with zero unparseable and zero pre-2025 dates (*verified 2026-08-21*).

---

## Source and Output Contract[^1]

| Stage | Object | Grain[^3] — one row is… | What it promises |
|---|---|---|---|
| Arrival | `landing.rit_amzn_report_daily` | One campaign / ad group / ad / deal on one date | `row_hash` unique per row (5,091 of 5,091 distinct), plus email lineage |
| Normalization | `amazon_final` CTE | Same grain, renamed and cast | No aggregation, no deduplication, no row loss |
| Output | [`master_stg.data_model_v3`](https://console.cloud.google.com/bigquery?project=looker-studio-pro-452620&p=looker-studio-pro-452620&d=master_stg&t=data_model_v3&page=table) | One Amazon ad/deal on one date | `qa_v3_metric_grain = 'source_amazon_daily'`, `qa_v3_row_type = 'source_actual'` |

Amazon is **row-preserving end to end.** `_package_id` + `_date` + `_placement_id`
is unique across all 5,091 rows, and `amzn_row_hash` is 1:1 with that triple. JOIN on
either and you cannot fan out.

---

## How Amazon Rows Are Built

Amazon has no Prisma package ID, so the branch builds synthetic keys[^2] by plain
string concatenation — **no hashing**, unlike TV. The IDs stay readable and reversible.

| Key | Built from (line) | Shape |
|---|---|---|
| `_package_id` | `campaign_id`, `ad_group_id` (1466) | `amazon_ads:<campaign_id>:<ad_group_id>` |
| `_placement_id` | `ad_id`, `deal_id` (1480) | `<ad_id>:<deal_id>`, with the literal `no_deal` when the deal is blank |
| `_placement_name` | `ad_name` → `deal_name` → the placement ID (1481) | first non-blank wins |

Live shape: 25 packages, 1,077 package/date combinations, 575 rows on `no_deal`
(*verified 2026-08-21*). `advertiser_short_name` is hard-coded to `'RTL'` (line 1471)
and the shared advertiser mapping supplies the user-facing label; one advertiser
account is live.

---

## Output Fields and Precedence

**`total_cost` is Amazon's media spend; `supply_cost` is not.** Line 1540 reads
`SAFE_CAST(total_cost AS FLOAT64) AS final_spend`, renamed to `_spend` at line 2344.
`supply_cost` is carried only as `amzn_supply_cost` and feeds nothing. Live proof:
**0 of 5,091 rows** disagree, and totals are `_spend` = `amzn_total_cost` =
**$477,255.17** against `amzn_supply_cost` = **$345,697.98** (*verified 2026-08-21*).
The semantic model documentation's claim that `supply_cost` is canonical Amazon media
spend is **wrong and should be corrected there.**

| Final field | Amazon source (line) | Notes |
|---|---|---|
| `_spend` | `total_cost` (1540) | Outranks `supply_cost`, `sales`, and every budget field |
| `_impressions`, `_clicks` | `impressions`, `clicks` (1541–1542) | Populated on all 5,091 rows |
| `_video_plays`, `_video_comps` | `starts_video_ad`, `complete_views_video_ad` (1543–1544) | |
| `_video_views` | `impressions_video_ad` via `social_video_views` (1565, 2348) | Equals `amzn_impressions_video_ad` on every row but `_video_plays` on only 2,318 — genuinely different measures |
| `_planned_spend`, `_planned_impressions` | none | **Always NULL** (0 of 5,091). Amazon supplies no plan, and its synthetic packages match no Prisma plan row |
| `p_planned_amount_doNotSum` | `ad_group_budget_amount` (1492, 2277) | A repeated ad-group total — see the double-count gotcha |
| `amzn_sales*`, `amzn_purchases*`, `amzn_units_sold*`, `amzn_branded_searches` | as named (1602–1613) | **Commercial outcomes, not delivery.** Raw strings; cast before arithmetic |

Amazon ranks second behind TV when a non-digital package/date must nominate one row
to carry package planned values (v3 line 661). Inert today, since no Amazon
package/date has a plan.

---

## Known Gaps and Gotchas

<details><summary><b>Summing the ad-group budget overstates it roughly 300x</b></summary>

`p_planned_amount_doNotSum` carries the ad group's **whole** budget and repeats on
every ad/date row beneath it — populated on all 5,091 rows. `SUM()` returns
**$242,536,821.66**; one value per package summed returns **$792,938.80**
(*verified 2026-08-21*). The `doNotSum` suffix is the only guardrail, and
`amzn_campaign_budget_amount` / `amzn_ad_group_budget_amount` carry the same trap with
no warning in their names.

</details>

<details><summary><b>qa_data_issues is rebuilt downstream, so branch tokens can vanish</b></summary>

`amazon_final` sets `row_data_issue_category` to `missing_final_metrics` or
`no_issues` (1458–1465), but `row_callouts` (stable base 1980) **reconstructs the
field from a fixed allow-list** and appends metric symptoms; an unlisted token is
dropped silently. `missing_final_metrics` is allow-listed and would survive — but
**never document this field from branch SQL, query it.** Live Amazon values:
`actual_imps_no_plan | actual_spend_no_plan` (3,530), `no_issues` (1,528),
`actual_imps_no_plan` (33); `missing_final_metrics` on zero rows
(*verified 2026-08-21*). The `actual_*_no_plan` labels are the expected consequence
of Amazon having no plan, not a defect.

</details>

<details><summary><b>Most Amazon rows carry impressions of zero, not missing impressions</b></summary>

Verified 2026-08-25. Of 5,352 Amazon rows, **1,624 carry `_impressions = 0` together with
`_spend = 0`** and are labelled `no_issues`. They are not partially delivered rows — both
measures are genuinely zero. Treating `_impressions` as "populated" because it is
non-NULL will silently inflate any denominator: a CTR or CPM computed across all rows
divides by these.

The genuinely odd cohort is much smaller: **33 rows** labelled `actual_imps_no_plan`
carry real impressions with zero spend. Whether those are unbilled placements or a
source gap is **undetermined**; reconciling one date against the Amazon console would
settle it.

The branch flags a row only when impressions, clicks, and **both** video measures are
all unparseable, so a zero-valued row passes every check by design.

</details>

<details><summary><b>Sales and revenue look like delivery metrics and are not</b></summary>

`amzn_sales` totals **$105,977.30**, and `amzn_sales_combined` is identical on current
data. Treating either as spend, or dividing it by impressions as if it were media
performance, compares an outcome to delivery. That is why they stay in `amzn_*`.

</details>

<details><summary><b>Two nearby claims that this branch contradicts</b></summary>

- The semantic model documentation calls `supply_cost` canonical Amazon media spend.
  The SQL and live data say `total_cost`, unambiguously. **A human should fix the
  semantic-layer reference.**
- Amazon is **present** in the v3 builder (lines 661 and 679), contrary to notes
  claiming otherwise — `source_amazon_daily` is assigned there.
- Live coverage starts **2026-05-28** and ends **2026-08-20** with no future-dated
  rows, so Amazon history does not reach back to the 2025 filter boundary.

</details>

Every Amazon numeric arrives as text and is safe-cast[^4], so a NULL metric may be
unparseable source text rather than an absent day. Read the matching `amzn_*` string
before concluding delivery was zero.

---

## Refresh

Amazon adds no refresh step of its own. Rebuild the final table with:

```bash
bq query --project_id=looker-studio-pro-452620 --use_legacy_sql=false < /Users/eugenetsenter/Looker_clonedRepo/looker_personal/master_data_model/model/final_model/create_master_stg_data_model_v3.sql
```

`master_stg.data_model` is a view, so it needs rebuilding only when its own SQL
changes. New Amazon data appears when the external email loader writes to the landing
table; nothing in this repo triggers that.

---

## Verify Current State Yourself

```sql
-- Coverage, labeling, QA tokens, and freshness in one pass
SELECT qa_v3_row_type, qa_v3_metric_grain, qa_v3_source_detail_type, qa_data_issues,
       COUNT(*) AS rows_, MIN(_date) AS min_date, MAX(_date) AS max_date,
       ROUND(SUM(_spend), 2) AS spend_,
       COUNTIF(_planned_spend IS NOT NULL) AS with_planned_spend,
       MAX(qa_data_source_refresh_at) AS source_loaded_at
FROM `looker-studio-pro-452620.master_stg.data_model_v3`
WHERE qa_media_data_type = 'amazon_ads'
GROUP BY 1,2,3,4 ORDER BY rows_ DESC;
```

```sql
-- Which field is spend, and does the grain hold?
-- Expect rows_disagreeing = 0, supply_cost_ different, and all three counts equal.
SELECT COUNTIF(_spend != SAFE_CAST(amzn_total_cost AS FLOAT64)) AS rows_disagreeing,
       ROUND(SUM(_spend), 2) AS spend_,
       ROUND(SUM(SAFE_CAST(amzn_supply_cost AS FLOAT64)), 2) AS supply_cost_,
       COUNT(*) AS rows_,
       COUNT(DISTINCT amzn_row_hash) AS distinct_row_hash,
       COUNT(DISTINCT FORMAT('%s|%t|%s', _package_id, _date, _placement_id)) AS distinct_grain_keys,
       ROUND(SUM(p_planned_amount_doNotSum), 2) AS naive_budget_sum
FROM `looker-studio-pro-452620.master_stg.data_model_v3`
WHERE qa_media_data_type = 'amazon_ads';
```

```sql
-- Source-to-model reconciliation. Expect landing_rows = model_rows.
SELECT (SELECT COUNT(*) FROM `looker-studio-pro-452620.landing.rit_amzn_report_daily`) AS landing_rows,
       (SELECT COUNTIF(SAFE.PARSE_DATE('%b %e, %Y', date) IS NULL)
          FROM `looker-studio-pro-452620.landing.rit_amzn_report_daily`) AS unparseable_dates,
       (SELECT COUNT(*) FROM `looker-studio-pro-452620.master_stg.data_model_v3`
          WHERE qa_media_data_type = 'amazon_ads') AS model_rows;
```

---

## Definitions

[^1]: **Contract:** a promise about a table's shape that other work is allowed to depend on — what one row represents, which columns exist, and what will not change without warning. The word signals obligation: something elsewhere is relying on this.

[^2]: **Synthetic key:** an ID this model invents because the source has none. Built only from values already in the source, so it is reproducible, but it exists nowhere outside this model and cannot be looked up in the planning system.

[^3]: **Grain:** what a single row represents. Combining tables of different grains without accounting for it is the most common cause of double-counted totals.

[^4]: **Safe cast:** converting text to a number so non-numeric text yields NULL instead of an error. It stops one bad row from failing the build, at the cost of making bad data look like missing data.

---

- [Source branch index](/Users/eugenetsenter/Looker_clonedRepo/looker_personal/master_data_model/model/branches/README.md)
- [TV pipeline](/Users/eugenetsenter/Looker_clonedRepo/looker_personal/master_data_model/model/branches/tv/README_tv-pipeline.md)
- [Prisma planning pipeline](/Users/eugenetsenter/Looker_clonedRepo/looker_personal/master_data_model/model/branches/prisma/README_prisma-pipeline.md)
