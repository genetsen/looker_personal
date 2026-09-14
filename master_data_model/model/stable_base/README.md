---
layer: Stable Base
output: looker-studio-pro-452620.master_stg.data_model
output_object_type: VIEW
output_grain: package_id + placement_id + date
canonical_file: model/stable_base/create_master_stg_data_model.sql
supporting_file: model/stable_base/create_master_data_model_upstream_tables_sched.sql
date_gate: ">= 2025-01-01, applied per source"
consumed_by: [master_stg.data_model_v3, master_stg.data_model_mart]
verified: 2026-08-21
verified_against: [live master_stg.data_model via SELECT, create_master_stg_data_model.sql, create_master_stg_data_model_v3.sql]
reviewers:
  - gene <gene.tsenter@giantspoon.com>
---

# Stable Base — the shared spine

Every source branch — digital (DCM, FPD, Polaris email, Prisma), social, WP search,
TV, Amazon Ads, manual edits — is normalized into one shape by a single file. Branch
docs describe *their* source; this page describes the spine they all cross.

**`master_stg.data_model` is a VIEW.** It stores no rows, so its table metadata row
count is meaningless — query it. Only `master_stg.data_model_v3` is a base table.

| To do this | Use |
|---|---|
| Read reported delivery | `_spend`, `_impressions`, `_clicks`, `_video_*` |
| Tell which source produced a row | `qa_row_data_source_primary` (11 live values) |
| Tell what *could* have produced it | `qa_row_data_sources_available` |
| See why a row looks wrong | `qa_data_issues` — see [QA vocabulary](#qa-vocabulary) |
| Jump to a branch's code | [CTE map](#cte-map) |

## Table of Contents

- [Canonical files](#canonical-files) · [Protected surfaces](#protected-surfaces) · [Grain and keys](#grain-and-keys)
- [Which field to read](#which-field-to-read) · [CTE map](#cte-map) · [QA vocabulary](#qa-vocabulary)
- [Known gaps and gotchas](#known-gaps-and-gotchas) · [Verify current state yourself](#verify-current-state-yourself) · [Definitions](#definitions)

---

## Canonical files

`model/**` is the only active workspace. Authority order for any disagreement:
**live production tables → semantic model documentation → repository SQL.**

| Purpose | File |
|---|---|
| The whole stable base (2,630 lines) | [create_master_stg_data_model.sql](/Users/eugenetsenter/Looker_clonedRepo/looker_personal/master_data_model/model/stable_base/create_master_stg_data_model.sql) |
| Upstream snapshot refresh (150 lines) | [create_master_data_model_upstream_tables_sched.sql](/Users/eugenetsenter/Looker_clonedRepo/looker_personal/master_data_model/model/stable_base/create_master_data_model_upstream_tables_sched.sql) |
| Downstream base table | [create_master_stg_data_model_v3.sql](/Users/eugenetsenter/Looker_clonedRepo/looker_personal/master_data_model/model/final_model/create_master_stg_data_model_v3.sql) |
| Advertiser name mapping | [create_advertiser_mapping.sql](/Users/eugenetsenter/Looker_clonedRepo/looker_personal/master_data_model/model/mappings/create_advertiser_mapping.sql) |

**TV, Amazon Ads, and Prisma have no SQL of their own** — their branch folders hold
documentation only, and all of their normalization lives in this one file. FPD and
social have local SQL for *loading* (Polaris email, Reddit, WP search) but not for
normalization; DCM's local file builds a separate detail view, not this one. Digital
**conversions** never touch this file — the string `conversion` appears nowhere in the
stable base and no `_conversions` field exists.

## Protected surfaces

| Surface | Rule |
|---|---|
| [`master_stg.data_model`](https://console.cloud.google.com/bigquery?project=looker-studio-pro-452620&p=looker-studio-pro-452620&d=master_stg&t=data_model&page=table) | A view. Changing its column list is a schema change for `data_model_v3`, the mart, and every dashboard behind them. |
| [`landing.tv_combined_tbl`](https://console.cloud.google.com/bigquery?project=looker-studio-pro-452620&p=looker-studio-pro-452620&d=landing&t=tv_combined_tbl&page=table), [`repo_int.crossplatform_pacing_tbl`](https://console.cloud.google.com/bigquery?project=looker-studio-pro-452620&p=looker-studio-pro-452620&d=repo_int&t=crossplatform_pacing_tbl&page=table) | Rebuilt wholesale by the upstream scheduled query. A direct write is erased at the next run. |
| `landing.tv_combined`, `repo_int.crossplatform_pacing` | Same-name sibling **views**, kept for lineage only. Do not materialize them; do not repoint the model at them. |
| `20250327_data_model.prisma_expanded_full`, `DCM.20250505_costModel_v5`, `repo_stg.stg__olipop__crossplatform_raw_tbl`, the `landing.*` feeds | Owned upstream. Read-only from here. |
| `landing.master_data_model_manual_package_daily`, `landing.master_data_model_manual_package_edits_raw` | Owned by the Manual Package Editor. |
| The live BigQuery transfer config | The real source of truth for the upstream schedule — not the `_sched.sql` file, which carries an unrelated pending change. Trigger the existing transfer rather than redeploying the file. |

---

## Grain and keys

One row is **one package, one placement, one date**. Verified 2026-08-21: 160,629 rows,
160,629 distinct `(_package_id, _placement_id, _date)` triples — but only 114,176
distinct package/date pairs, so **the view is not unique on package/date** and joining
to anything at that grain[^1] fans out.

`COUNT(*)` counts plan-or-delivery row slots, not delivered media: 61,565 rows have
NULL `_spend` and 61,180 have neither spend nor impressions. Filter on
`_spend IS NOT NULL` or `qa_row_data_source_primary` before counting anything.

Four key families coexist, because only Prisma-matched media has a real package ID;
everything else gets a synthetic key[^2] minted in this file.

| Family | Prefix | Built from | Rows, verified 2026-08-21 |
|---|---|---|---|
| Prisma | none — numeric ID | Prisma package ID, carried through | 93,573 |
| Social + WP search | `social:` | `platform : campaign_id : ad_group_id`; placement is `ad_id` | 59,908 |
| Amazon Ads | `amazon_ads:` | `campaign_id : ad_group_id`; placement is `ad_id : deal_id` | 5,091 |
| TV | `tv_pkg:` / `tv_plc:` | MD5 of lowercased source fields — see the [TV pipeline](/Users/eugenetsenter/Looker_clonedRepo/looker_personal/master_data_model/model/branches/tv/README_tv-pipeline.md) | 2,057 |

Digital rows with no placement fall back to the package ID as the placement ID, so
`_placement_id` is never NULL and never safe to read as a real placement.

**The date gate is `>= 2025-01-01`, applied inside every branch** rather than once at
the end — 15 separate `WHERE` clauses. Prisma applies it to *both* `start_date` and
`date`, so a package that started in 2024 is excluded entirely, not truncated.

---

## Which field to read

Prefixes are the contract[^3]: `_` = read this, `p_` = Prisma plan context, `qa_` =
provenance and diagnostics, `dcm_`/`fpd_`/`tv_`/`amzn_`/`social_` = untouched source
evidence. **`_spend` and `_impressions` outrank every source-prefixed column**,
including `dcm_media_cost`, `dcm_impressions`, and `fpd_spend`, which are evidence only
and are *not* what the model reports.

Digital precedence, and it is deliberately asymmetric:

| Final field | Precedence |
|---|---|
| `_spend` | FPD original + FPD updated (summed, zero treated as absent) → `dcm_daily_recalculated_cost`, a **recomputed** value |
| `_impressions` | FPD original + FPD updated (summed) → **`dcm_impressions`**, the **raw** source column. `dcm_daily_recalculated_imps` stays QA context and never becomes a final metric |
| `_clicks` | `fpd_clicks` → `dcm_clicks` |
| `_planned_spend`, `_planned_impressions` | Prisma daily plan for digital; evenly-spread pacing budget for social. Carried on one row per package/date so it stays summable |

A CPM from `_spend / _impressions` therefore mixes a recalculated numerator with a raw
denominator — and `actual_source_conflict` checks FPD against
`COALESCE(dcm_daily_recalculated_imps, dcm_impressions)`, a third column set again.
`p_planned_amount_doNotSum` and its siblings are package-level values repeated on every
row; the suffix is the warning[^4].

---

## CTE map

Jump to a branch instead of grepping 2,630 lines. Numbers are opening lines.

| Lines | CTE family | What it does |
|---|---|---|
| 21 | `source_refreshes` | One row of freshness timestamps, one per upstream feed (19 of them). Feeds `qa_data_source_refresh_at`. |
| 101 | `social_source_by_final_row` | Retains the *raw* platform label so TikTok and TikTok ADIF stay distinguishable after both normalize to `tiktok`. |
| 199 | `dcm_daily` | DCM cost model, daily. |
| 233–370 | `polaris_email_coverage`, `fpd_original_raw`, `fpd_original_daily_source`, `polaris_email_daily`, `fpd_original_daily`, `fpd_video_daily`, `fpd_updated_daily` | FPD family. Original FPD sheet and Polaris email union into `fpd_original_daily`; the updated ADIF sheet stays a separate additive input; video metrics rejoin at line 2474. |
| 371–486 | `manual_package_daily`, `manual_package_metadata` | Manual Package Editor daily values and package metadata. |
| 487–530 | `prisma_daily_raw`, `prisma_daily`, `prisma_meta_src`, `prisma_meta` | Prisma plan — supplies the package ID, planned daily spend/impressions, and package metadata. |
| 531–578 | `digital_joined`, `digital_with_meta`, `dcm_low_signal_primary_packages` | Four-way `FULL OUTER JOIN` of DCM + original FPD + updated FPD + Prisma, then a package-level "low signal" flag for DCM packages averaging under 100 impressions per row. |
| **579** | **`digital_final`** | Largest branch (~300 lines). Picks `row_data_source_primary`, builds the digital half of `qa_data_issues`, resolves final metrics. |
| 886–1186 | `social_daily`, `social_pacing_dedup`, `social_pacing_daily`, `social_with_pacing`, **`social_final`** | Social and WP search. Pacing budget is spread evenly across the flight to make planned daily spend. |
| 1187–1452 | `tv_base`, `tv_daily`, `tv_with_package_dates`, **`tv_final`** | TV estimates; synthetic keys minted at lines 1203 and 1216. |
| 1453 | `amazon_final` | Amazon Ads. Dates and metrics arrive as text and are safe-cast[^5], so a NULL metric here may be unparseable source text rather than missing delivery. |
| 1625 | `all_rows` | `UNION ALL` of `digital_final`, `social_final`, `tv_final`, `amazon_final`. Column order must match across all four. |
| 1635–1926 | `manual_existing_rows`, `manual_only_rows`, `manual_applied_rows` | Manual edits overlay existing rows, then add rows for package/dates no source produced. |
| 1927 | `planned_metric_backfills` | Fills a missing actual from the plan for TV, print, and OOH — see the gotcha below. |
| **1980** | **`row_callouts`** | Rewrites `qa_data_issues` for **every** row from every branch. See [QA vocabulary](#qa-vocabulary). |
| 2045–2131 | `with_rollups`, `with_initiative`, `advertiser_mapping`, `canonical_advertiser_short_names`, `with_standardized_advertiser_base`, `with_standardized_advertiser` | Package-level rollup totals, initiative, advertiser-name standardization. |
| 2132 | `final_model_rows` | The ~350-line output projection: internal names → published `_`/`p_`/`qa_`/`dcm_`/`fpd_` names. |
| 2480–2630 | `row_source_contributors`, `package_list`, `package_contributors`, `package_available_source_tokens`, `package_available_sources`, `package_qa` | Row- and package-level source attribution. Adds `qa_pkg_primary_data_source` and `qa_pkg_data_sources_available` without changing grain. |

---

## QA vocabulary

`row_callouts` (line 1980) is the single place `qa_data_issues` is finalized. It runs
downstream of **every** branch, discards what the branch wrote, and rebuilds the string
from a fixed vocabulary in a fixed order — causes first, then metric symptoms. That is
why a branch's own issue value is not what appears in the output.

| Token | Meaning | Set by | Rows, verified 2026-08-21 |
|---|---|---|---|
| `no_issues` | Nothing flagged | fallback | 47,752 |
| `missing_prisma_package` | Delivery exists, no Prisma package matched | `digital_final` | 4,825 |
| `missing_prisma_daily` | Package matched, no daily plan | `digital_final` | 4,644 |
| `missing_social_pacing` | No pacing budget for this campaign/ad group | `social_final` | 36,347 |
| `missing_actuals` | Plan exists, no delivery from any source | `digital_final` | 56,538 |
| `actual_source_conflict` | DCM and FPD both present and disagree by more than 1 | `digital_final` | 1,317 |
| `low_signal_dcm` | DCM-only package averaging under 100 impressions/row | `digital_final` | 7,272 |
| `missing_final_metrics` | Source row present, all metrics NULL | digital, social, TV, Amazon | 166 |
| `pending_wp_source_owner_review` | WP search row awaiting owner sign-off | `social_final` | 624 |
| `spend_no_imps` | Spend and planned spend present, impressions zero on both sides | `row_callouts` | 465 |
| `actual_imps_no_plan` | Delivered impressions, no plan | `row_callouts` | 7,173 |
| `actual_spend_no_plan` | Delivered spend, no plan | `row_callouts` | 3,620 |
| `planned_imps_no_actual` | Planned impressions, none delivered | `row_callouts` | 0 — possible in code, absent live |
| `planned_spend_no_actual` | Planned spend, none delivered | `row_callouts` | 1,162 |

Symptoms are suppressed when a cause already explains them: `missing_actuals`
suppresses both `planned_*_no_actual` labels, and `missing_prisma_daily` or
`missing_social_pacing` suppress both `actual_*_no_plan` labels. An absent symptom does
not mean the symptom is absent.

<details><summary><b>The vocabulary is an allow-list, so a new branch token would be silently dropped</b></summary>

`row_callouts` does not append to the incoming value — it tests for each of the eight
cause tokens by name and reassembles. All eight upstream tokens are in the list, so
nothing is lost today. But a branch that invents a ninth will see it vanish with no
error and `no_issues` reported instead. Adding a token means editing line 1980 too.

</details>

---

## Known gaps and gotchas

<details><summary><b>Manual delivery overrides are metric-specific and use one daily carrier row</b></summary>

Manual values arrive at package/date grain, but the stable model can contain several
source-detail rows for the same package/date. Each populated manual metric replaces
only that metric and is published on one deterministic row; unedited metrics remain
on their source rows. For example, an impression-only override must not replace spend
and must not repeat the daily impression value across multiple TV rows.

</details>

<details><summary><b>4,825 digital rows report NULL spend and impressions while carrying real DCM delivery</b></summary>

`digital_final` gates every final metric on `WHEN prisma_package_id IS NULL THEN NULL`,
so a row whose delivery arrived from DCM or FPD but matched no Prisma package reports
nothing. Verified 2026-08-21: all 4,825 `missing_prisma_package` rows have NULL `_spend`
and NULL `_impressions` while carrying **$982,620** of `dcm_daily_recalculated_cost`,
**38,061,729** `dcm_daily_recalculated_imps`, and **$442,507** of `fpd_spend`.
Deliberate in code, not a build failure — and the largest single source of "the
dashboard is missing spend I know we ran."

</details>

<details><summary><b>Open QA item — whether that suppressed delivery should stay invisible needs a human decision</b></summary>

Two readings are both defensible and the code does not say which is intended.
**Correct as-is:** unmatched delivery has no advertiser, package, or plan context, and
3,524 of the 4,825 rows are also `low_signal_dcm`, consistent with test or trafficking
noise. **A gap:** 38.1M impressions and ~$1.4M of combined DCM/FPD cost is not noise,
and the remaining 1,301 rows carry no low-signal flag at all.

What would settle it: reviewing those 1,301 non-low-signal rows against Prisma to see
whether the package match is genuinely absent or merely failing to join. **Do not
resolve it by removing the `prisma_package_id IS NULL` gate** — that changes reported
spend for every consumer at once.

</details>

<details><summary><b>381 rows report a plan as if it were delivery</b></summary>

`planned_metric_backfills` (line 1927) copies `_planned_spend` into `_spend` when no
actual exists, for TV, print, and OOH rows only, and only when planned impressions are
nonzero. Verified 2026-08-21: 381 rows with `qa_row_data_source_primary =
'planned_only'` carry `_spend` this way — 270 `ooh_d` ($623,448) and 111 `ooh`
($545,698). `qa_row_data_source_primary` is the only signal that it is a plan.

The same CTE runs the reverse too: when a row delivered both spend and impressions but
its plan has zero impressions, `_planned_impressions` is backfilled *from the actual*.
Perfect impression pacing on such a row is an artifact.

</details>

---

## Verify current state yourself

Query the view directly — its metadata row count is not a row count.

```sql
-- Grain integrity. Expect rows_ = pkg_plc_date, and pkg_date strictly smaller.
SELECT COUNT(*) AS rows_,
       COUNT(DISTINCT FORMAT('%t|%t|%t', _package_id, _placement_id, _date)) AS pkg_plc_date,
       COUNT(DISTINCT FORMAT('%t|%t', _package_id, _date)) AS pkg_date
FROM `looker-studio-pro-452620.master_stg.data_model`;
```

```sql
-- Branch coverage: rows, packages, date span, how much reports no spend.
SELECT qa_media_data_type, qa_row_data_source_primary,
       COUNT(*) AS rows_, COUNT(DISTINCT _package_id) AS pkgs,
       MIN(_date) AS min_date, MAX(_date) AS max_date,
       COUNTIF(_spend IS NULL) AS null_spend
FROM `looker-studio-pro-452620.master_stg.data_model`
GROUP BY 1,2 ORDER BY rows_ DESC;
```

```sql
-- QA vocabulary, one row per token rather than per combination.
SELECT tok, COUNT(*) AS rows_
FROM `looker-studio-pro-452620.master_stg.data_model`,
     UNNEST(SPLIT(qa_data_issues, ' | ')) AS tok
GROUP BY 1 ORDER BY rows_ DESC;
```

```sql
-- How much delivery the missing-Prisma gate suppresses, and how much is low-signal.
SELECT COUNT(*) AS rows_, COUNTIF(_spend IS NOT NULL) AS with_final_spend,
       ROUND(SUM(COALESCE(dcm_daily_recalculated_cost, 0)), 0) AS dcm_cost_hidden,
       ROUND(SUM(COALESCE(dcm_daily_recalculated_imps, 0)), 0) AS dcm_imps_hidden,
       COUNTIF(qa_data_issues LIKE '%low_signal_dcm%') AS also_low_signal
FROM `looker-studio-pro-452620.master_stg.data_model`
WHERE qa_data_issues LIKE '%missing_prisma_package%';
```

Freshness is `qa_data_source_refresh_at` — when data reached the source table — not
`MAX(_date)`, which reaches into the future on plan and estimate sources.

---

## Definitions

[^1]: **Grain:** what a single row represents. Joining or unioning two tables of different grain without accounting for it is the most common cause of double-counted totals.

[^2]: **Synthetic key:** an ID this model invents because the source has none. Reproducible from source values, but it exists nowhere outside this model and cannot be looked up in the planning system.

[^3]: **Contract:** a promise about a table's shape that other work is allowed to depend on — which columns exist, what one row means, what will not change without warning. The word signals obligation: something downstream is relying on it.

[^4]: **doNotSum:** a suffix marking a package-level value repeated on every row of that package. Summing it multiplies the real figure by the row count.

[^5]: **Safe cast:** converting text to a number so non-numeric text yields NULL instead of failing the build. It keeps one bad row from breaking everything, at the cost of making bad data indistinguishable from missing data.

---

Branch docs: [TV](/Users/eugenetsenter/Looker_clonedRepo/looker_personal/master_data_model/model/branches/tv/README_tv-pipeline.md) · [social](/Users/eugenetsenter/Looker_clonedRepo/looker_personal/master_data_model/model/branches/social/README_social-pipeline.md) · [amazon](/Users/eugenetsenter/Looker_clonedRepo/looker_personal/master_data_model/model/branches/amazon/README_amazon-pipeline.md) · [prisma](/Users/eugenetsenter/Looker_clonedRepo/looker_personal/master_data_model/model/branches/prisma/README_prisma-pipeline.md) · [fpd](/Users/eugenetsenter/Looker_clonedRepo/looker_personal/master_data_model/model/branches/fpd/README_fpd-pipeline.md) · [dcm](/Users/eugenetsenter/Looker_clonedRepo/looker_personal/master_data_model/model/branches/dcm/README_dcm-pipeline.md) · [Pipeline v2](/Users/eugenetsenter/Looker_clonedRepo/looker_personal/master_data_model/README_v2.md)
