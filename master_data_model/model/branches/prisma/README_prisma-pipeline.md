---
pipeline: Prisma Planning
source_type: plan — never delivery
output: looker-studio-pro-452620.master_stg.data_model_v3
output_grain: package_date (planned metrics carried once per package/date)
source_tables:
  - looker-studio-pro-452620.20250327_data_model.prisma_expanded_full
refresh: scheduled query `Prisma_expanded`, Mon–Fri 07:00 UTC
loader_script: none — no branch-local SQL exists
verified: 2026-08-25
verified_against:
  - model/stable_base/create_master_stg_data_model.sql
  - model/final_model/create_master_stg_data_model_v3.sql
  - docs/SCHEDULED_QUERIES.md
  - live master_stg.data_model_v3 and 20250327_data_model.prisma_expanded_full
reviewers:
  - gene <gene.tsenter@giantspoon.com>
---

# Prisma Planning Pipeline

Prisma is the planning system of record: it defines the **package universe** — which packages exist
and over which dates. Every planned number in
[`master_stg.data_model_v3`](https://console.cloud.google.com/bigquery?project=looker-studio-pro-452620&p=looker-studio-pro-452620&d=master_stg&t=data_model_v3&page=table)
originates here. **No delivery metric does.**

| To do this | Use |
|---|---|
| Sum a planned total | `_planned_spend`, `_planned_impressions` — on one row per package/date |
| Read package context on a detail row | `p_*`, `qa_v3_package_planned_*_doNotSum` — never sum these |
| Find plans with no delivery | `qa_v3_row_type = 'planned_only_package_date'` |
| Spot a key Prisma never issued | `_package_id` contains `prefix:` — see [Real package or minted key](#real-package-or-minted-key) |

## Table of Contents

- [Canonical files — there is no branch-local script](#canonical-files--there-is-no-branch-local-script)
- [How a plan becomes package/date rows](#how-a-plan-becomes-packagedate-rows)
- [Which row carries the plan](#which-row-carries-the-plan)
- [Planned-only rows](#planned-only-rows)
- [Real package or minted key](#real-package-or-minted-key)
- [Known gaps and gotchas](#known-gaps-and-gotchas)
- [Open QA item — a human must decide](#open-qa-item--a-human-must-decide)
- [Refresh and verification](#refresh-and-verification)
- [Definitions](#definitions)

---

## Canonical files — there is no branch-local script

`model/branches/prisma/` holds **documentation only**. Prisma logic lives in two shared files, and
the date expansion[^2] is in neither — it runs inside the `Prisma_expanded` scheduled query.
`model/**` is the only active workspace; authority order is **live `data_model_v3` → semantic model
documentation → repository SQL.**

| Stage | Where | CTEs | Locator |
|---|---|---|---|
| Read the plan, aggregate to package/date, broadcast metadata | [Stable base](/Users/eugenetsenter/Looker_clonedRepo/looker_personal/master_data_model/model/stable_base/README.md) | `prisma_daily_raw` → `prisma_daily`; `prisma_meta_src` → `prisma_meta`; used in `digital_joined`, `digital_with_meta` | `grep -n "^prisma_daily_raw AS (" model/stable_base/create_master_stg_data_model.sql` |
| Carry planned metrics once per package/date; mint planned-only rows | [Final model](/Users/eugenetsenter/Looker_clonedRepo/looker_personal/master_data_model/model/final_model/README.md) | `package_plan_daily`, `digital_detail_ranked`, `non_digital_source_ranked`, `planned_only_rows` | `grep -n "^package_plan_daily AS (" model/final_model/create_master_stg_data_model_v3.sql` |

**Protected:** [`prisma_expanded_full`](https://console.cloud.google.com/bigquery?project=looker-studio-pro-452620&p=looker-studio-pro-452620&d=20250327_data_model&t=prisma_expanded_full&page=table)
and everything upstream is owned by `Prisma_expanded` and rebuilt wholesale — a direct write is
erased at the next run, and the live BigQuery transfer config, not any repository file, is the truth
about that schedule. Object inventory: [Prisma table catalog](PRISMA_TABLE_CATALOG.md).

## How a plan becomes package/date rows

A Prisma planning row is **per package with a flight window** — `start_date`, `end_date`,
`planned_amount`, `planned_impressions`. `Prisma_expanded` turns it into one row per day:

```sql
-- inside the Prisma_expanded scheduled query, not this repo
SELECT pm.*, date_day AS date,
       pm.planned_amount / (DATE_DIFF(pm.end_date, pm.start_date, DAY) + 1) AS daily_spend
FROM prisma_master pm, UNNEST(GENERATE_DATE_ARRAY(pm.start_date, pm.end_date)) AS date_day
```

- **The daily plan is a straight average, not a pacing curve.** A front-loaded buy looks flat.
- **A flight extending past today produces future-dated planned rows.** `16,044` rows carry
  `_planned_spend` on a date after today, worth `$18,360,407`, running to `2027-02-01` *(verified
  2026-08-25)*. A planned total without a `_date <= CURRENT_DATE()` filter includes budget not yet
  spendable — the usual cause of "why is pacing so far behind".
- **A re-plan moves the whole curve.** Widening `end_date` re-divides the budget across all days,
  changing every historical daily plan for that package.

`prisma_daily_raw` then drops `package_type = 'Child'` placement rows (`!=` also drops a NULL
`package_type`, of which there are none today) and `prisma_daily` sums `planned_daily_spend_pk`,
`planned_daily_impressions_pk` and `planned_clicks` to package/date. Metadata takes a separate path:
`prisma_meta` keeps the latest `report_date` row per package **with no date bound**.

| Stage | Object | Grain[^1] — one row is… |
|---|---|---|
| Arrival | `prisma_expanded_full` | One planning row on one date — **not** one package/date |
| Aggregation | `prisma_daily` | One package/date; planned metrics summed, `MAX(report_date)` kept |
| Output | `data_model_v3` | One row at its source's natural grain; `_planned_*` on exactly one row per package/date |

**The 2025 gate excludes packages, it does not truncate them.** `prisma_daily_raw` filters
`start_date >= DATE '2025-01-01'` **and** `date >= DATE '2025-01-01'`. Because it tests the flight's
start as well as the day, a package that started in 2024 and ran into 2025 is dropped **in full** —
its 2025 days never appear, and `prisma_meta` applies the same gate so its metadata is gone too.
Verified 2026-08-25: **6 packages** (`P2XS60Z`, `P2XS435`, `P2ZGGTH`, `P2XYZ28`, `P2XRTG2`,
`P2RG8GQ`), `1,458` rows, `$1,186,078` of 2025-dated planned spend.

## Which row carries the plan

v3 keeps delivery rows at placement/ad/creative grain, so a package plan repeated on each row would
multiply when summed. `package_plan_daily` computes the package/date plan once, and attaches it to a
single **planned carrier row**[^3] picked by `ROW_NUMBER()` over `_package_id, _date`:

| Row family | Ordering that decides rank 1 | CTE |
|---|---|---|
| Manual override | No ranking — a manual row always wins its package/date and takes the plan | `manual_delivery_rows` |
| Digital delivery | `polaris_email` → `fpd_original` → `fpd_updated_package` → `dcm`, then `_placement_id`, `qa_v3_ad_name`, `creative_name`, `fpd_factor` | `digital_detail_ranked` |
| Non-digital delivery | `tv_combined` → `amazon_ads` → `wp_search_data_template` → `social`, then `_placement_id`, `_creative_name` | `non_digital_source_ranked` |
| No delivery at all | The planned-only row is itself the carrier | `planned_only_rows` |

- **`_planned_*` on a delivery row is the PACKAGE-level total for that date, not that row's own
  plan.** A placement showing `$4,000` planned did not have a `$4,000` plan; its package did.
  Per-placement plans do not exist in this model.
- **Rows that are not rank 1 have `_planned_*` NULLed**, as does every row whose package/date has a
  manual override. NULL here means "carried elsewhere", never "no plan".
- Package context stays readable on every row through `qa_v3_package_planned_spend_doNotSum`,
  `qa_v3_package_planned_impressions_doNotSum` and the `p_*` family (`p_planned_amount_doNotSum`,
  `p_planned_impressions_doNotSum`, `p_planned_units_doNotSum`, `p_rate`, `p_buy_type`,
  `p_cost_method`, `p_max_report_date`). The `doNotSum` suffix is literal.

**Invariant, re-verified 2026-08-25:** `90,682` rows carry a planned value across `90,682` distinct
package/dates — **zero** package/dates carry more than one planned row. Query 1 below.

## Planned-only rows

A `planned_only_package_date` row exists because Prisma planned a package/date and **no delivery row
of any kind arrived**. `planned_only_rows` enforces that with three `NOT EXISTS` guards against
manual, digital-detail and non-digital source rows, so it can never double-count against delivery.
Live: `56,721` rows across `1,228` packages, concentrated in `display` (25,445), `audio` (14,611) and
`video` (10,358) *(verified 2026-08-25)*.

`qa_data_issues` on these rows is `missing_actuals` everywhere except 28 `ooh_d` rows reading
`no_issues`. That field is **rebuilt** by the `row_callouts` step in the stable base from a fixed
allow-list and preserves nothing a branch writes, so read it from live data only —
`grep -n "^row_callouts AS (" model/stable_base/create_master_stg_data_model.sql`.

<details><summary><b>The planned-delivery fallback names TV, print and magazines but only ever fires for OOH</b></summary>

`planned_only_rows` publishes the plan into `_spend`/`_impressions` as a last resort for offline
channels, testing `qa_media_data_type = 'tv'`, `_channel_group IN ('linear','print','ooh','ooh_d')`,
`_channel IN ('linear_tv','print','ooh','ooh_d')` and `_media_name IN ('tv','print','magazine',
'newspaper','ooh')`. Live, only `ooh` and `ooh_d` rows qualify — re-verified 2026-08-25, unchanged
from the previous check.

The gate is the second condition: it requires `NULLIF(planned_impressions, 0) IS NOT NULL`. All 87
print rows and 1,644 of the 2,193 `ooh` rows carry zero or NULL planned impressions, so their plan is
never published as delivery, and TV never appears because TV always has a source row of its own.
Total published this way: `577` rows, `$1,876,533`.

</details>

## Real package or minted key

A real Prisma package carries the planning system's own ID (`P37K96P`, no prefix); the model mints a
synthetic key[^4] with a `prefix:` for sources that have no planning row at all. Verified 2026-08-25:

| `_package_id` shape | Packages | Rows | Rows with `_planned_spend` | Planned-only rows |
|---|---:|---:|---:|---:|
| Prisma ID (no prefix) | 1,539 | 194,080 | 84,428 | 56,721 |
| `social:` | 405 | 60,621 | 5,100 | 0 |
| `tv_pkg:` | 121 | 2,057 | 1,153 | 0 |
| `amazon_ads:` | 25 | 5,352 | 0 | 0 |

**Only real Prisma IDs ever produce planned-only rows** — a minted key exists because delivery
arrived, so it cannot be plan-without-delivery. And `_planned_spend` under `social:` or `tv_pkg:` is
not Prisma: social uses the pacing budget, TV reuses its estimate as both plan and actual (see the
[TV pipeline](/Users/eugenetsenter/Looker_clonedRepo/looker_personal/master_data_model/model/branches/tv/README_tv-pipeline.md)).
Within the real-ID group `_package_type` separates the states — `Package` 948, `PrintInsertion` 276,
`Standalone` 225, `FeeOrder` 11, `OOH` 1, `TV` 1, and `UnmatchedDigitalPackage` **77 packages /
10,092 rows carrying zero planned and zero final spend**, delivery whose package ID matches nothing in
Prisma. Filtering to `_package_type = 'Package'` silently drops Standalone and PrintInsertion
delivery.

## Known gaps and gotchas

<details><summary><b>prisma_expanded_full is NOT package/date grain — the table catalog says it is, and that is wrong</b></summary>

Verified 2026-08-25: `780,399` rows over `181,919` distinct package/date combinations. `598,459` are
`package_type = 'Child'` placement rows the model filters out; after the model's full filter set,
`84,400` rows remain over `84,379` package/dates — *near* package/date but not guaranteed, and the 21
remaining duplicates are a live defect quantified below. The
[Prisma table catalog](PRISMA_TABLE_CATALOG.md) states "One row per package/date planning record" for
this table; treat that as false. The `SUM ... GROUP BY package_id, date` in `prisma_daily` exists
precisely because it is not true.

</details>

- **Prisma metadata appears on dates outside the flight, and on dates with no plan.** `prisma_meta`
  joins on `package_id` with no date bound, so a package's latest metadata attaches to every date it
  appears on. Rows where the package exists in Prisma but that date has no daily plan are flagged
  `missing_prisma_daily` — `13,859` rows across `232` packages, plus `3,959` also flagged
  `low_signal_dcm` *(verified 2026-08-25)*. Metadata with a NULL plan is expected there.
- **Digital delivery with no Prisma package contributes nothing to any total.** The stable base gates
  final digital metrics on the Prisma lookup succeeding and flags failures `missing_prisma_package` —
  `3,666` rows across `26` packages, plus `6,426` across `62` packages also flagged `low_signal_dcm`
  *(verified 2026-08-25)*. Raw `dcm_*` and `fpd_*` columns stay populated, so the delivery is visible
  but unreportable; the fix is an upstream package-ID mapping job, not a model change.
- **Fee packages are excluded from `prisma_porcessed` but still reach the master model.** The
  package-grain table drops them; the `Prisma_expanded` path does not. `11` `FeeOrder` packages
  contribute `732` planned-only rows and `$134,278` of planned spend *(verified 2026-08-25)*, so a
  fee-free total has to exclude them explicitly.
- **A manual package edit overrides the plan, not just the delivery.** The manual row becomes the
  plan carrier, and the stable base prefers `man_total_planned_spend_doNotSum` over
  `p_planned_amount_doNotSum` for the package-level planned amount.
- **The `p_` prefix does not mean "from Prisma", and two upstream typos are load-bearing.**
  `p_planned_amount_doNotSum` holds the Amazon ad-group budget on Amazon rows and NULL on social and
  TV rows; only the digital branch fills `p_*` from Prisma. Keep the field name `initative` and the
  object name `prisma_porcessed` exactly as they are.

## Open QA item — a human must decide

<details><summary><b>21 duplicated print insertions inflate production planned spend by $1,545,869.56</b></summary>

Verified 2026-08-25. After the model's own filters, `21` package/date combinations carry **two
byte-identical rows** each (42 rows) — every one a `PrintInsertion`, single-day flight
(`total_days = 1`, `n_of_placements = 1`), one advertiser, channel groups `print` and `ooh`. Example:
package `P304R7H` on `2025-05-04`, "New York Times Magazine", `planned_cost_pk = 50,000`, twice, with
identical `report_date` and `script_run_date`. Because `prisma_daily` sums per package/date the plan
doubles, and it reaches production: true plan **$1,545,869.56**, live `SUM(_planned_spend)` for the
same 21 **$3,091,739.12** — 1.06% of the model's $145,478,038 planned spend.

A defect, not a modelling choice, but the fix is not decidable from here: deduplicating in
`prisma_daily_raw` would mask a genuine two-insertion buy on the same day, and fixing it upstream
needs to know whether Prisma legitimately reports one print insertion twice. **Someone who owns the
print buy must confirm whether a same-day duplicate insertion is ever real** before either change
lands. Reproduce with query 2.

</details>

## Refresh and verification

Prisma has no loader script and nothing in this branch to deploy. Planning data becomes current when
`Prisma_expanded` runs (**Mon–Fri 07:00 UTC** — Saturday and Sunday serve Friday's plan). To
republish the model afterwards:

```bash
bq query --project_id=looker-studio-pro-452620 --use_legacy_sql=false < /Users/eugenetsenter/Looker_clonedRepo/looker_personal/master_data_model/model/final_model/create_master_stg_data_model_v3.sql
```

Freshness is `MAX(p_max_report_date)` — `2026-08-24` as of 2026-08-25 — **not** `MAX(_date)`, which on
a plan is the furthest future flight day. A successful `Prisma_expanded` run is not proof the plan is
right; it proves the table was rebuilt. Both queries below are the minimum proof for any change to the
planned path.

```sql
-- 1. The planned invariant. Expect exactly 0 rows returned.
SELECT `_package_id`, `_date`, COUNT(*) AS planned_rows
FROM `looker-studio-pro-452620.master_stg.data_model_v3`
WHERE `_planned_spend` IS NOT NULL OR `_planned_impressions` IS NOT NULL
GROUP BY 1, 2 HAVING planned_rows > 1;
```

```sql
-- 2. Source-side duplication and the 2025 date gate. Expect dup_combos = 0 (today it is 21) and a
-- small, stable gated_out_packages count; a growing one means 2024-start flights are being dropped whole.
WITH f AS (
  SELECT package_id, date, start_date, planned_daily_spend_pk
  FROM `looker-studio-pro-452620.20250327_data_model.prisma_expanded_full`
  WHERE package_type != 'Child' AND date >= DATE '2025-01-01'
), d AS (
  SELECT package_id, date, COUNT(*) AS n,
         SUM(planned_daily_spend_pk) AS summed, MAX(planned_daily_spend_pk) AS true_plan
  FROM f WHERE start_date >= DATE '2025-01-01' GROUP BY 1, 2 HAVING n > 1
)
SELECT (SELECT COUNT(*) FROM d) AS dup_combos,
       (SELECT ROUND(SUM(summed - true_plan), 2) FROM d) AS inflated_planned_spend,
       COUNT(DISTINCT IF(start_date < DATE '2025-01-01', package_id, NULL)) AS gated_out_packages,
       ROUND(SUM(IF(start_date < DATE '2025-01-01', planned_daily_spend_pk, 0)), 0) AS gated_out_planned_spend
FROM f;
```

## Definitions

[^1]: **Grain:** what a single row represents. Combining tables of different grains without accounting for it is the most common cause of double-counted totals.

[^2]: **Date expansion:** turning one row covering a date range into one row per day, dividing the range's total evenly across those days. It invents rows that were never reported, so a daily planned number is an arithmetic average, not evidence of what was planned for that specific day.

[^3]: **Planned carrier row:** the one row per package/date chosen to hold the package's planned values, so summing them across many detail rows returns the plan once instead of multiplying it. Every other row for that package/date has its planned fields NULLed on purpose — NULL means "carried elsewhere", not "no plan".

[^4]: **Synthetic key:** an ID this model invents because the source has none. Reproducible from source values, but it exists nowhere outside this model and cannot be looked up in Prisma.

---

- [Prisma table catalog](PRISMA_TABLE_CATALOG.md)
- [v3 Prisma lineage map](/Users/eugenetsenter/Looker_clonedRepo/looker_personal/master_data_model/docs/v3-prisma-lineage.md)
- [BigQuery Scheduled Queries](/Users/eugenetsenter/Looker_clonedRepo/looker_personal/docs/SCHEDULED_QUERIES.md)
