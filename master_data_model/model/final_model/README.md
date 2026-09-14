---
layer: Final Model
output: looker-studio-pro-452620.master_stg.data_model_v3
output_type: BASE TABLE (257 columns, clustered by `_advertiser`)
output_grain: mixed — one row per source-detail grain; read qa_v3_metric_grain
canonical_file: model/final_model/create_master_stg_data_model_v3.sql
write_statements: 2 (lines 36 and 816) — both required, same run
refresh: manual deploy, no scheduled query
verified: 2026-08-21
verified_against: live master_stg.data_model_v3; create_master_stg_data_model_v3.sql; create_master_stg_data_model.sql
reviewers: gene <gene.tsenter@giantspoon.com>
---

# Final Model Layer

The shared spine of the master data model. Every branch — Prisma, DCM, FPD, Polaris email, social, TV, Amazon,
manual edits, CM360 conversions — lands here and obeys the rules on this page; branch guides cover their own source
logic and link back here for anything shared. Read the underscore finals (`_spend`, `_impressions`, `_clicks`,
`_video_*`, `_planned_*`) for resolved numbers, `qa_v3_row_type` + `qa_v3_metric_grain` + `qa_v3_source_detail_type`
to know what a row is, and `qa_v3_metric_behavior` then `qa_data_issues` for why a metric is NULL.

## Table of Contents

- [Canonical file and protected surfaces](#canonical-file-and-protected-surfaces)
- [Two writes, one run](#two-writes-one-run)
- [Grain, keys, and row types](#grain-keys-and-row-types)
- [Which field a consumer should read](#which-field-a-consumer-should-read)
- [Field families, `_doNotSum`, and QA fields](#field-families-_donotsum-and-qa-fields)
- [Known gaps and gotchas](#known-gaps-and-gotchas)
- [Open QA items — a human must decide](#open-qa-items--a-human-must-decide)
- [Deploy and verify](#deploy-and-verify)
- [Definitions](#definitions)

## Canonical file and protected surfaces

One file builds the whole layer: [create_master_stg_data_model_v3.sql](/Users/eugenetsenter/Looker_clonedRepo/looker_personal/master_data_model/model/final_model/create_master_stg_data_model_v3.sql)
(1,005 lines). Authority order for any disagreement: **live `data_model_v3` (production) → semantic model documentation → this repository's SQL.**

Upstream, [create_master_stg_data_model.sql](/Users/eugenetsenter/Looker_clonedRepo/looker_personal/master_data_model/model/stable_base/create_master_stg_data_model.sql)
builds the package/date stable base behind the `master_stg.data_model` view. **Branch logic lives in two files**:
v3 also reads `landing.adif_updated_fpd_daily`, `landing.polaris_email_delivery_daily`,
`landing.master_data_model_manual_package_daily`, `landing.apo_dcm_creative_image_asset_map`,
`landing.fpd_data_ranged_shortcutsFolder`, `DCM.20250505_costModel_v5`, and
`master_stg.rtl_cm360_direct_conversions` directly, so a branch change may need edits in both files.

| Surface | Rule |
|---|---|
| [master_stg.data_model](https://console.cloud.google.com/bigquery?project=looker-studio-pro-452620&p=looker-studio-pro-452620&d=master_stg&t=data_model&page=table) | **A view, not a table**, and v3's only stable input. Never materialize it or repoint v3 elsewhere. |
| [master_stg.data_model_v3](https://console.cloud.google.com/bigquery?project=looker-studio-pro-452620&p=looker-studio-pro-452620&d=master_stg&t=data_model_v3&page=table) | Fully replaced on every deploy. A direct write, added column, or patched row is destroyed at the next run. |
| Every source table listed above | Owned by loaders and editors upstream. Read-only from here. |

## Two writes, one run

The file contains **two `CREATE OR REPLACE TABLE data_model_v3` statements**; reading only the first describes a table that does not exist in production.

| # | Line | What it builds |
|---|---|---|
| 1 | 36 | Snapshots the stable view to a temp table, then assembles manual-override, digital-detail, non-digital source, and planned-only rows; adds the seven `qa_v3_*` fields and typed NULL placeholders for the `conv_*` and `polaris_*` families |
| 2 | 816 | Snapshots the just-written v3, aggregates `rtl_cm360_direct_conversions` to a detail key, attaches conversions to uniquely matching DCM rows, appends conversion-only rows, and adds ~24 conversion columns plus `qa_cm360_record_key` |

A trailing `ALTER TABLE … SET OPTIONS` writes the table description. Statement 1 first drops `data_model_v3` if it exists as a view, since `CREATE OR REPLACE TABLE` cannot replace one.

<details><summary><b>Statement 2 alone fails; statement 1 alone ships a broken schema</b></summary>

Statement 2 re-adds `conv_advertiser`, `conv_activities`, and the rest by name; against an already-complete table
those names collide and BigQuery rejects the query. Stopping after statement 1 is worse — it succeeds, and
publishing a v3 missing every conversion column consumers select. This is one multi-statement script (one
submission, a `DECLARE`, statements run in order), not independent queries: deploy the whole file.

</details>

## Grain, keys, and row types

**There is no single grain[^1].** Each row sits at the natural grain of the source that produced it, and
`qa_v3_metric_grain` names it. The contract[^2] that does hold: `(_package_id, _date)` is the **join key to
anything package/date-shaped** — plans, manual edits, package context — and is *not* unique; uniqueness is
`_package_id` + `_date` + whatever `qa_v3_metric_grain` names. `_date` is gated `>= 2025-01-01` in every source
read and **not gated at the top end** — live data runs to 2027-02-01, because plans and TV estimates describe
the future. Live taxonomy, verified 2026-08-21 (260,004 rows, 14 advertisers):

| `qa_v3_row_type` | `qa_v3_metric_grain` | `qa_v3_source_detail_type` | Rows |
|---|---|---|---|
| `source_actual` | `package_date_placement_ad_creative` | `dcm` | 109,730 |
| `planned_only_package_date` | `package_date` | `planned_only` | 56,566 |
| `source_actual` | `source_social_daily` | `social` | 32,950 |
| `source_actual` | `source_social_daily` | `wp_search_data_template` | 26,958 |
| `source_actual` | `package_date_placement_factor_creative` | `fpd_original` | 22,319 |
| `source_actual` | `source_amazon_daily` | `amazon_ads` | 5,091 |
| `source_actual` | `package_date` | `fpd_updated_package` | 2,162 |
| `source_actual` | `source_tv_daily` | `tv_combined` | 1,993 |
| `source_actual` | `package_date_platform_campaign_ad_group_ad` | `polaris_email` | 992 |
| `manual_delivery_override` | `package_date` | `manual_package_daily` | 820 |
| `source_actual` | `package_date_placement_creative_conversion` | `cm360_direct_conversion` | 423 |

Conversion rows are typed `source_actual` too, so **`qa_v3_row_type` alone does not separate delivery from
conversions** — add `qa_v3_source_detail_type`. The grain `stable_source_daily` is the code's catch-all for a
non-digital source that is not social, Amazon, or TV; it matches zero rows today.

## Which field a consumer should read

The underscore finals are the only fields where precedence is resolved; every prefixed family is unresolved source evidence. Precedence, highest first:

| Rank | Source | What it outranks, and how |
|---|---|---|
| 1 | Manual override | Replaces only the populated manual metrics. The separate `manual_delivery_override` row carries those package/date values; matching metrics are null on source rows, while unedited source metrics remain available. For example, an impression-only edit preserves source spend. |
| 2 | Polaris email | Replaces the FPD path inside a loaded package's date coverage, enforced by `NOT EXISTS` filters so the conflicting FPD rows never exist. |
| 3 | FPD over DCM | Where FPD has a non-zero value for the package/date, the DCM row's `_spend` and `_impressions` are NULL. DCM keeps `_clicks` unless FPD supplied clicks. |
| 4 | DCM | `_spend` from `dcm_daily_recalculated_cost`; `_impressions` from source `dcm_impressions` — **not** `dcm_daily_recalculated_imps`, which stays QA context only. |
| 5-6 | Non-digital, then planned-only | Social, WP search, TV, and Amazon pass stable metrics through unchanged. Planned-only rows come last, only where no manual, digital-detail, or non-digital source row exists for that package/date. |

`_planned_spend` and `_planned_impressions` stay summable because exactly one row per package/date carries them —
the row where `planned_carrier_rank = 1`[^3]. Verified 2026-08-21: 114,230 package/date combinations, 90,348 planned
rows, **zero combinations with more than one**. Two independent `ROW_NUMBER() OVER (PARTITION BY _package_id,
_date …)` rankings pick it:

| Family | Source order | Tie-breakers |
|---|---|---|
| Digital detail | `polaris_email` → `fpd_original` → `fpd_updated_package` → `dcm` | `_placement_id`, ad name, creative name, `fpd_factor` |
| Non-digital | `tv_combined` → `amazon_ads` → `wp_search_data_template` → `social` | `_placement_id`, `_creative_name` |

Source rows matched by a manual override never carry planned values; the manual row does. Actual metrics are suppressed individually only when that same manual metric is populated.

## Field families, `_doNotSum`, and QA fields

| Prefix | Cols | Meaning |
|---|---|---|
| `_` | 34 | **Finals.** Resolved identity, plan, delivery. What consumers read. |
| `qa_v3_` / `qa_` | 7 / 20 | Row classification written by this layer; lineage, freshness, issue labels, package rollups. |
| `amzn_` / `s_` / `tv_` / `man_` / `dcm_` / `fpd_` / `polaris_` | 45 / 20 / 10 / 28 / 14 / 14 / 16 | Amazon, social, TV, Manual Editor, DCM, FPD, and Polaris email source evidence (Polaris includes raw unparsed strings). |
| `conv_` / `p_` | 37 / 8 | CM360 conversion outcomes from statement 2 (counts, revenue, activity arrays, join status); Prisma package-level planning context. |

**`_doNotSum`[^4]** marks an already-aggregated value copied onto detail rows: `qa_v3_package_planned_*_doNotSum`,
`qa_pkg_act_*`, `qa_pkg_est_*`, `qa_pkg_fpd_*`, `p_planned_*`. Use `MAX()` or `ANY_VALUE()` per package, not `SUM()`.

`qa_data_issues` is **not** authored by any branch CTE. The `row_callouts` step (~line 1980 of the
[stable base SQL](/Users/eugenetsenter/Looker_clonedRepo/looker_personal/master_data_model/model/stable_base/create_master_stg_data_model.sql))
appends metric-symptom labels — `spend_no_imps`, `actual_imps_no_plan`, `actual_spend_no_plan`,
`planned_imps_no_actual`, `planned_spend_no_actual` — to the cause labels already present, pipe-separated,
defaulting to `no_issues`; cause labels suppress redundant symptoms (`missing_actuals` hides the
planned-without-actual pair). A branch CTE is never the final word here, and v3 overwrites the field only for
manual overrides and conversion-only rows. Trust lineage in this order: `qa_v3_source_detail_type` (what v3
actually built the row from) → `qa_row_data_source_primary` → `qa_data_source` (a literal string; see the open
item below) → `qa_row_data_sources_available` (accumulated history).

## Known gaps and gotchas

<details><summary><b>A detail row's metric can be NULL even when its own source reported a number</b></summary>

Digital finals are gated by `has_stable_spend` / `has_stable_impressions` / `has_stable_clicks` / `has_stable_video_*`,
computed from the *stable base* for that package/date. If the stable base has no non-NULL value, v3 nulls the final
even though DCM or FPD reported one — the source value survives in `dcm_*` / `fpd_*`, so a final-versus-evidence
mismatch here is expected, not corruption.

</details>

<details><summary><b>Package context on a detail row is one arbitrary winning stable row</b></summary>

`stable_context` collapses each package/date to one stable row via `ARRAY_AGG(… LIMIT 1)`, preferring
manual-edited, then planned-bearing, then advertiser-named, then package-named. Every descriptive column on every
detail row of that package/date comes from that winner — including inherited `qa_data_issues` and
`qa_manual_edit_flag`, which is why 28 `planned_only_package_date` rows carry `qa_manual_edit_flag = TRUE` despite
being defined as having no manual override.

</details>

<details><summary><b>The planned-only offline fallback names four channels but fires for OOH only</b></summary>

It publishes plan as delivery where planned impressions are non-zero and the row looks like TV, print, OOH, or
dOOH. Live, all 409 such rows are `_channel_group` `ooh` or `ooh_d` (verified 2026-08-21); TV and print never
qualify today, and on these rows a plan-versus-actual comparison compares a number to itself.

</details>

<details><summary><b>Conversions attach only where exactly one DCM row matches</b></summary>

The key is `lower(package_id)|date|lower(placement_id)|lower(creative)`. `matched_unique` attaches conversions
onto the delivery row (1,900 rows, 96,337 conversions). `conversion_only` and `ambiguous_delivery_match` both
become standalone rows with **all inherited columns NULL**, built from `LEFT JOIN v3_delivery_base ON FALSE`.
Live: 423 conversion-only rows carrying 6,320 conversions, zero ambiguous — ambiguity is silent today but would
move conversions off delivery rows, so `conv_model_detail_join_status` is the field to watch.

</details>

Two smaller traps: statement 1 ends with `WHERE _advertiser_name != 'Highlights'`, which **excludes NULL advertiser
names too** and runs only in statement 1, so conversion-only rows bypass it (latent — the stable view has zero
Highlights and zero NULL advertiser names, verified 2026-08-21). And `qa_cm360_record_key` is `SHA256` over
`TO_JSON_STRING()` of the whole row, so any column addition, rename, or value change changes it — useful for
finding duplicates within one snapshot, never across deploys.

## Open QA items — a human must decide

<details><summary><b>DCM rows carry a lineage string that does not match the table the builder reads</b></summary>

`qa_data_source` is the literal `giant-spoon-299605.data_model_2025.new_md` on 107,830 DCM rows (verified
2026-08-21; the other 1,900 are relabeled `dcm | cm360_direct_conversion` by statement 2). But `dcm_detail`
reads `looker-studio-pro-452620.DCM.20250505_costModel_v5`, which is a **BASE TABLE**, not a view over
`new_md`. Either the label is stale, or the cost-model table is a copy of `new_md` and the label records its origin.
**Unknown:** whether the two objects still carry the same data — settled by comparing row counts and metric totals
for a shared date range, which needs access to the `giant-spoon-299605` project. Do not "fix" the label before that
comparison exists; it may be the only surviving record of the real origin.

</details>

<details><summary><b>The Sheet-based conversion path is declared in the schema and empty in the data</b></summary>

`conv_activity`, `conv_source_impressions`, `conv_source_clicks`, `conv_source_sheet_id`, `conv_source_sheet_tab`,
and `conv_source_sheet_gid` exist as columns and are **NULL on all 260,004 rows** (verified 2026-08-21). Statement 1
declares them as typed NULLs, nothing populates them, and statement 2 explicitly nulls them for CM360 rows —
consistent with the table description's note that the builder does not read the legacy compatibility table.
**Unknown:** whether they are kept for schema compatibility or are dead weight; settled by an owner confirming
nothing downstream selects them. Until then treat them as reserved, not broken.

</details>

## Deploy and verify

No scheduled query builds this layer. Deploy the whole file, both statements, and rebuild the stable base first if
the change touches package/date assembly (statement 1 snapshots the view at its start). Freshness is the deploy
time, not `MAX(_date)`, which is a future plan date.

```bash
bq query --project_id=looker-studio-pro-452620 --use_legacy_sql=false < /Users/eugenetsenter/Looker_clonedRepo/looker_personal/master_data_model/model/final_model/create_master_stg_data_model_v3.sql
```

```sql
-- Taxonomy, metric coverage, manual suppression, future dating, advertiser nulls
SELECT qa_v3_row_type, qa_v3_metric_grain, qa_v3_source_detail_type, COUNT(*) AS rows_,
       COUNTIF(_spend IS NOT NULL) AS with_spend, COUNTIF(_planned_spend IS NOT NULL) AS with_planned,
       COUNTIF(qa_manual_edit_flag) AS manual_flagged, COUNTIF(_date > CURRENT_DATE()) AS future_dated,
       COUNTIF(_advertiser_name IS NULL) AS null_advertiser
FROM `looker-studio-pro-452620.master_stg.data_model_v3`
GROUP BY 1,2,3 ORDER BY rows_ DESC;
```

```sql
-- Planned stays summable. combos_over_1 must be 0; anything else is a real defect.
SELECT COUNT(*) AS package_date_combos, SUM(planned_rows) AS planned_rows,
       COUNTIF(planned_rows > 1) AS combos_over_1
FROM (SELECT _package_id, _date, COUNTIF(_planned_spend IS NOT NULL) AS planned_rows
      FROM `looker-studio-pro-452620.master_stg.data_model_v3` GROUP BY 1, 2);

-- Conversion attachment health. ambiguous_delivery_match should not appear.
SELECT conv_model_detail_join_status, COUNT(*) AS rows_,
       COUNTIF(qa_v3_source_detail_type = 'dcm') AS on_dcm_delivery,
       SUM(conv_total_conversions) AS conversions
FROM `looker-studio-pro-452620.master_stg.data_model_v3`
WHERE conv_model_detail_join_status IS NOT NULL GROUP BY 1;
```

## Definitions

[^1]: **Grain:** what a single row represents. This table mixes grains on purpose, so combining rows without checking `qa_v3_metric_grain` is the most common cause of double-counted totals.

[^2]: **Contract:** a promise about a table's shape that other work may depend on — what one row means, which columns exist, what will not change without warning. The word signals obligation: something elsewhere relies on it.

[^3]: **Planned carrier row:** the single row per package/date chosen to hold the package's plan so that summing `_planned_spend` cannot double-count. Every other row of that package/date is NULL there — a NULL plan means "not the carrier", not "no plan".

[^4]: **`_doNotSum` suffix:** the column already holds a total for a package or package/date and is copied onto every detail row for convenience. Adding it up multiplies it by the row count.

Related: [Master Data Model Pipeline v2](/Users/eugenetsenter/Looker_clonedRepo/looker_personal/master_data_model/README_v2.md) · [Source branch guides](/Users/eugenetsenter/Looker_clonedRepo/looker_personal/master_data_model/model/branches/README.md)
