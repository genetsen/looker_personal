---
pipeline: TV Estimates
source_type: estimate — not delivery
output: looker-studio-pro-452620.master_stg.data_model_v3
output_grain: source_tv_daily
source_tables:
  - looker-studio-pro-452620.landing.tv_local_estimates
  - looker-studio-pro-452620.landing.tv_national_estimates
refresh: scheduled query, daily 10:15 UTC
loader_script: none
verified: 2026-08-21
verified_against:
  - model/stable_base/create_master_stg_data_model.sql
  - model/final_model/create_master_stg_data_model_v3.sql
  - create_master_data_model_upstream_tables_sched.sql
reviewers:
  - gene <gene.tsenter@giantspoon.com>
---

# TV Estimates Pipeline

Linear TV spend and impression estimates, entering the master data model as
non-digital source rows under synthetic keys[^3] because TV has no planning-system
package ID.

**TV is an estimate source, not a delivery source.** Rows describe planned airings,
including dates that have not happened yet.

| To do this | Use |
|---|---|
| Isolate genuine TV rows | `qa_v3_source_detail_type = 'tv_combined'` |
| Include the manual-override rows | `_package_id LIKE 'tv_pkg:%'` |
| Read TV's own values | `tv_*` fields — unchanged source evidence |
| Read reporting values | `_spend`, `_impressions` — see [Output Fields](#output-fields-and-precedence) |

## Table of Contents

- [Canonical files](#canonical-files)
- [Protected surfaces](#protected-surfaces)
- [How the Data Arrives](#how-the-data-arrives)
- [Source and Output Contract](#source-and-output-contract)
- [How TV Rows Are Built](#how-tv-rows-are-built)
- [Output Fields and Precedence](#output-fields-and-precedence)
- [Known Gaps and Gotchas](#known-gaps-and-gotchas)
- [Refresh](#refresh)
- [Verify Current State Yourself](#verify-current-state-yourself)
- [Definitions](#definitions)

---

## Canonical files

Every duplicate copy of these was archived on 2026-08-21; `model/**` is the only
active workspace. Authority order for any disagreement: **live `data_model_v3`
(production) → semantic model documentation → the SQL below.**

| Purpose | File |
|---|---|
| Ingestion | [create_master_data_model_upstream_tables_sched.sql](/Users/eugenetsenter/Looker_clonedRepo/looker_personal/master_data_model/model/stable_base/create_master_data_model_upstream_tables_sched.sql) |
| TV normalization | [create_master_stg_data_model.sql](/Users/eugenetsenter/Looker_clonedRepo/looker_personal/master_data_model/model/stable_base/create_master_stg_data_model.sql) |
| Final model | [create_master_stg_data_model_v3.sql](/Users/eugenetsenter/Looker_clonedRepo/looker_personal/master_data_model/model/final_model/create_master_stg_data_model_v3.sql) |

## Protected surfaces

| Surface | Rule |
|---|---|
| `landing.tv_local_estimates`, `landing.tv_national_estimates` | The actual sources. Owned upstream, read-only from here. |
| `landing.tv_combined_tbl` | Rebuilt by the scheduled query. A direct write is overwritten at the next run. |
| `landing.tv_combined` | A view. Do not materialize it or point the model at it. |
| `master_stg.data_model` | **A view, not a table.** Editing it is a schema change for every consumer. |
| `landing.master_data_model_manual_package_daily` | Manual overrides that suppress TV actuals. Owned by the Manual Package Editor. |
| The live BigQuery transfer config | The real source of truth for the schedule — see [Refresh](#refresh). |

---

## How the Data Arrives

TV has **no loader script**. `landing.tv_combined_tbl` is rebuilt daily at 10:15 UTC
by the `master_data_model_upstream_tables_sched` scheduled query, documented in
[BigQuery Scheduled Queries](/Users/eugenetsenter/Looker_clonedRepo/looker_personal/docs/SCHEDULED_QUERIES.md).
That query also serves social pacing, so changes affect more than TV.

It unions the two estimate tables directly — **not** the `tv_combined` view, which
exists for lineage only and is not the model's input:

```sql
SELECT * FROM landing.tv_local_estimates
UNION ALL SELECT * FROM landing.tv_national_estimates
WHERE media_outlet IS NOT NULL
```

Two filters remove source data before the model ever sees it:

| Filter | Where | Effect as of 2026-08-21 |
|---|---|---|
| `media_outlet IS NOT NULL` | ingestion | Drops **645 of 2,618 source rows (24.6%), carrying $636,419** of net cost — see the open QA item below. |
| `date >= 2025-01-01` | `tv_base` in the stable base | Currently drops nothing; applies the moment older data lands. |

The first is the answer to "why is TV spend below plan" and is invisible downstream.

<details><summary><b>Open QA item — the outlet filter removes two different things</b></summary>

The filter is doing two unrelated jobs at once, and only one of them is clearly
correct. Of the 496 dropped rows from the local table:

| `program_name` contains | Rows | With cost | Cost |
|---|---|---|---|
| A real-looking program name | 284 | 22 | $214,734 |
| A number, e.g. `8,225.60` | 117 | 18 | $246,160 |
| A label, e.g. `Rate`, `Rescheduled` | 95 | 7 | $175,525 |

The latter two shapes are malformed — a dollar amount or a label sitting in the
program column is the signature of a column misalignment in the source sheet, and no
row that survives the filter has a numeric `program_name`. Dropping those 212 rows is
correct. The 149 dropped rows from the national table carry no cost at all.

The **284 structurally normal rows** are the open question: real advertiser, campaign,
market, and a plausible program name, missing only an outlet, with $214,734 of net
cost across 22 of them. Whether those are legitimately excluded, or whether the outlet
is missing upstream by mistake, is **undecided and needs the source owner**. All
affected rows have `net_impressions = 0`, so only spend totals are ever affected.

Tracked as a separate QA task; do not resolve it by loosening the filter without that
decision.

</details>

---

## Source and Output Contract[^1]

Dashboards, saved queries, and downstream models rely on the guarantees below.

| Stage | Object | Grain[^2] — one row is… | What it promises |
|---|---|---|---|
| Arrival | `landing.tv_combined_tbl` | One outlet, program, and market on one date | Cost, impressions, units, and a source refresh date. Fully replaced daily; never appended to. |
| Normalization | `tv_base` → `tv_daily` → `tv_with_package_dates` → `tv_final` (CTEs in the stable base) | One synthetic key[^3] pair on one date | Synthetic keys stay stable for identical inputs; matching rows are aggregated. |
| Output | `master_stg.data_model_v3` | One TV source row at its natural grain | `qa_v3_metric_grain = 'source_tv_daily'`, `qa_v3_row_type = 'source_actual'` |

---

## How TV Rows Are Built

TV has no package ID from the planning system, so this branch derives synthetic
keys[^3] by concatenating source fields and hashing[^4] them with MD5, hex-encoded
and truncated to 16 characters.

| Key | Prefix | Built from |
|---|---|---|
| Package | `tv_pkg:` | advertiser, campaign name, TV type, media outlet |
| Placement | `tv_plc:` | those same four, **plus** program name and market |

Each value is lowercased, trimmed, and joined with `|`. The placement key is a
superset of the package key, so one package always holds one or more placements.

<details><summary><b>Only the hashed fields determine row identity</b></summary>

Two source rows differing *only* in a field outside the key — quarter, year, units —
are the same row to this model and are aggregated together. That is the usual answer
to "why is this one row instead of two?"

</details>

<details><summary><b>NULL and blank do not hash the same way</b></summary>

The hash coalesces NULL to a literal string (`unknown_advertiser`, `unknown_outlet`,
and so on), so every row missing the same field collapses into one shared package. But
the hash does **not** apply the blank-to-NULL normalization used for the display
columns: an empty string stays empty inside the hash.

So a blank outlet and a NULL outlet produce **different** package keys while both
displaying as `Unknown Outlet`. Latent today — the source currently has neither — but
it is a real trap.

</details>

<details><summary><b>Renaming a source value creates a new package</b></summary>

Identical inputs always yield an identical hash, which keeps packages stable across
rebuilds. The flip side: if an outlet or campaign is renamed upstream, its rows hash
to a new key and the old package stops receiving rows. A rename is indistinguishable
from one package ending and another beginning.

</details>

<details><summary><b>Conflicting values are resolved silently, and not uniformly</b></summary>

When rows sharing a key disagree, text fields keep the first value alphabetically —
outlet, campaign, type, program, market, quarter. But `year` and `data_refresh_date`
take the **maximum**, not the alphabetical first. Metrics are summed. No warning is
raised for any of it.

</details>

<details><summary><b>Package start and end dates are derived, not planned</b></summary>

They are the minimum and maximum dates present for that synthetic package, so loading
a new date shifts them. They are not a flight plan.

</details>

---

## Output Fields and Precedence

TV's own values keep a `tv_*` prefix and stay unchanged. Final fields are prefixed
with an underscore.

| Field | TV behavior |
|---|---|
| `_spend`, `_impressions` | Set directly from `net_cost` and `net_impressions` in `tv_final` |
| `_planned_spend`, `_planned_impressions` | The **package-level** total, not this row's own plan. Carried on the single row where `planned_carrier_rank = 1` so totals stay summable |
| `_clicks`, `_video_*` | No TV source. Always NULL on TV source rows |
| `tv_total_units` | Evidence only; no final field |
| `tv_data_refresh_date` | Source freshness. **Not** the delivery date |
| `qa_data_issues` | Live values are `no_issues` (1,629), `spend_no_imps` (336), and `manual_delivery_override` (64). Metric callouts are appended by a shared step downstream of this branch, not by `tv_final` |

**Manual overrides suppress TV actuals.** When a manual daily edit exists for a
package/date, that TV source row's `_spend`, `_impressions`, `_clicks`, and video
fields are set to NULL, `qa_manual_edit_flag` becomes TRUE, and `qa_data_issues`
becomes `manual_delivery_override_source_suppressed`. The manual numbers arrive on a
*separate* row at `package_date` grain — a different grain from TV's. No package/date
currently overlaps, so this is latent, but a query unioning both will mix grains.

---

## Known Gaps and Gotchas

Working as designed, and reliably mistaken for defects. Counts as of 2026-08-21; use
the queries below rather than trusting them later.

<details><summary><b>TV rows include dates that have not happened yet</b></summary>

199 of 2,029 rows are dated after today, extending to 2026-12-25. Aggregating TV
spend without a date filter includes airings that have not aired — the most likely
cause of a TV total that looks too high. Digital sources do not behave this way, so a
query written for digital will silently mislead.

</details>

<details><summary><b>Rows under a tv_pkg: package are not all TV rows</b></summary>

64 of them carry clicks and video. Those are Manual Package Editor rows at
`package_date` grain, labeled `qa_v3_row_type = 'manual_delivery_override'` — not TV
source rows. A TV-looking row with clicks is not a TV data problem.

</details>

<details><summary><b>Planned and delivered are the same numbers</b></summary>

`net_cost` and `net_impressions` serve as both the reported and the planned values, so
a planned-versus-actual comparison on TV compares a number to itself and always shows
perfect pacing. Note this is not the model's planned-only fallback, which fires only
when no source row exists for a package/date and never applies to TV today.

</details>

<details><summary><b>A NULL metric may be unparseable source text</b></summary>

Metrics are cast safely[^5], so text that is not numeric becomes NULL instead of
failing the build. Inspect the raw source value before concluding an airing has no
data.

</details>

---

## Refresh

Only `data_model_v3` is a table; `master_stg.data_model` is a view, so rebuilding it
moves no data and is needed only when its own SQL changes. To publish new TV data:

```bash
bq query --project_id=looker-studio-pro-452620 --use_legacy_sql=false < /Users/eugenetsenter/Looker_clonedRepo/looker_personal/master_data_model/model/final_model/create_master_stg_data_model_v3.sql
```

For the source snapshot itself, **do not redeploy the upstream SQL file wholesale** —
it carries an unrelated pending change, and the live transfer configuration, not the
file, is the source of truth. Trigger the existing transfer instead. See
[BigQuery Scheduled Queries](/Users/eugenetsenter/Looker_clonedRepo/looker_personal/docs/SCHEDULED_QUERIES.md).

---

## Verify Current State Yourself

```sql
-- Coverage, future-dating, and which rows are not really TV
SELECT qa_v3_row_type, qa_v3_metric_grain, qa_data_issues,
       COUNT(*) AS rows_,
       COUNTIF(_date > CURRENT_DATE()) AS future_dated,
       COUNTIF(_clicks IS NOT NULL) AS with_clicks,
       COUNTIF(_planned_spend IS NOT NULL) AS with_planned_spend
FROM `looker-studio-pro-452620.master_stg.data_model_v3`
WHERE _package_id LIKE 'tv_pkg:%'
GROUP BY 1,2,3 ORDER BY rows_ DESC;
```

```sql
-- Source-to-model reconciliation: how much is the outlet filter dropping?
-- Expect dropped_rows > 0; investigate only if it changes sharply.
WITH src AS (
  SELECT * FROM `looker-studio-pro-452620.landing.tv_local_estimates`
  UNION ALL SELECT * FROM `looker-studio-pro-452620.landing.tv_national_estimates`
)
SELECT COUNT(*) AS source_rows,
       COUNTIF(media_outlet IS NULL) AS dropped_rows,
       ROUND(SUM(IF(media_outlet IS NULL, SAFE_CAST(net_cost AS FLOAT64), 0)), 0) AS dropped_cost
FROM src;
```

```sql
-- Planned stays summable: one row per package/date should carry it
SELECT COUNT(DISTINCT FORMAT('%s|%t', _package_id, _date)) AS package_date_combos,
       COUNTIF(_planned_spend IS NOT NULL) AS rows_with_planned_spend
FROM `looker-studio-pro-452620.master_stg.data_model_v3`
WHERE _package_id LIKE 'tv_pkg:%';
```

Snapshot freshness is the scheduled query's last successful run, not `MAX(_date)` —
which on an estimate source is the furthest future airing.

---

## Definitions

[^1]: **Contract:** a promise about a table's shape that other work is allowed to depend on — what one row represents, which columns exist, and what will not change without warning. The word signals obligation: something elsewhere is relying on this.

[^2]: **Grain:** what a single row represents. Combining tables of different grains without accounting for it is the most common cause of double-counted totals.

[^3]: **Synthetic key:** an ID this model invents because the source has none. Derived only from values already in the source, so it is reproducible, but it exists nowhere outside this model and cannot be looked up in the planning system.

[^4]: **Hash:** a short fixed-length code produced from text. Identical text always produces an identical code, and the code cannot be reversed. Used here to compress four or six source fields into one compact ID.

[^5]: **Safe cast:** converting text to a number so that non-numeric text yields NULL instead of an error. It stops one bad row from failing the build, at the cost of making bad data look like missing data.

---

- [Master Data Model Pipeline v2](/Users/eugenetsenter/Looker_clonedRepo/looker_personal/master_data_model/README_v2.md)
- [BigQuery Scheduled Queries](/Users/eugenetsenter/Looker_clonedRepo/looker_personal/docs/SCHEDULED_QUERIES.md)
