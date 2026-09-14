---
pipeline: Digital Conversions (direct CM360)
source_type: outcome — not delivery
output: looker-studio-pro-452620.master_stg.data_model_v3
output_grain: package_date_placement_creative_conversion
source_tables:
  - looker-studio-pro-452620.master_stg.rtl_cm360_direct_conversions
  - giant-spoon-299605.ALL_DCM_adswerve.Ritual_conversions_last14_cm360_1123381_1665558564_*
refresh: manual — no scheduled query for the loader or the model
loader_script: model/branches/digital/conversions/merge_rtl_cm360_direct_conversions_latest.sql
verified: 2026-08-21
verified_against:
  - live master_stg.data_model_v3 via SELECT
  - live master_stg.rtl_cm360_direct_conversions via SELECT
  - model/final_model/create_master_stg_data_model_v3.sql
reviewers:
  - gene <gene.tsenter@giantspoon.com>
---

# Digital Conversions Pipeline

Ritual conversion outcomes from Campaign Manager 360 (CM360), entering the master data
model as **outcome rows, not delivery rows**. Nothing here writes `_spend`,
`_impressions`, `_clicks`, or `_video_*`, so adding conversion evidence cannot move a
delivery total.

**Conversions exist only in v3** — the stable base never mentions the word. All logic is
the *second* write statement of the v3 builder. Nothing to look for upstream.

| To do this | Use |
|---|---|
| Read conversion counts | `conv_total_conversions`, `conv_click_through_conversions`, `conv_view_through_conversions` |
| Read conversion revenue | `conv_total_revenue`, `conv_click_through_revenue`, `conv_view_through_revenue` |
| Read named outcomes | `conv_site_visits`, `conv_view_products`, `conv_add_to_carts`, `conv_begin_checkouts`, `conv_purchases` |
| Find rows carrying conversions | `conv_model_detail_key IS NOT NULL` — 2,323 of 260,004 rows (verified 2026-08-21) |
| Isolate rows with **no** delivery | `qa_v3_source_detail_type = 'cm360_direct_conversion'` |
| Tell whether delivery is attached | `conv_model_detail_join_status` |

Shared rules are not repeated here — read
[Final Model layer](/Users/eugenetsenter/Looker_clonedRepo/looker_personal/master_data_model/model/final_model/README.md)
and [Stable Base layer](/Users/eugenetsenter/Looker_clonedRepo/looker_personal/master_data_model/model/stable_base/README.md).

## Table of Contents

- [Canonical files](#canonical-files) · [Protected surfaces](#protected-surfaces) · [The two-write trap](#the-two-write-trap)
- [Grain, keys, and the join](#grain-keys-and-the-join) · [Matched vs conversion-only](#matched-vs-conversion-only) · [Which field to read](#which-field-to-read)
- [Known gaps and gotchas](#known-gaps-and-gotchas) · [Open QA items](#open-qa-items--a-human-must-decide)
- [Refresh](#refresh) · [Verify current state yourself](#verify-current-state-yourself) · [Definitions](#definitions)

---

## Canonical files

Authority order for any disagreement: **live `data_model_v3` (production) → semantic
model documentation → the repository SQL below.**

| Status | File | Role |
|---|---|---|
| **Canonical** | [create_master_stg_data_model_v3.sql](/Users/eugenetsenter/Looker_clonedRepo/looker_personal/master_data_model/model/final_model/create_master_stg_data_model_v3.sql) | All conversion logic, lines 809–1005 (write statement 2) |
| **Canonical** | [merge_rtl_cm360_direct_conversions_latest.sql](/Users/eugenetsenter/Looker_clonedRepo/looker_personal/master_data_model/model/branches/digital/conversions/merge_rtl_cm360_direct_conversions_latest.sql) | Routine loader — run after each new enriched export lands |
| **Canonical, run-once** | [bootstrap_rtl_cm360_direct_conversions.sql](/Users/eugenetsenter/Looker_clonedRepo/looker_personal/master_data_model/model/branches/digital/conversions/bootstrap_rtl_cm360_direct_conversions.sql) | Creates and seeds the history table; also the recovery path |
| **Superseded** | `create_rtl_cm360_direct_conversions_qa.sql`, `create_rtl_cm360_direct_history_qa.sql`, the three `*_qa_manifest.json` files | Migration-decision scaffolding. Their own headers call the outputs disposable, and the migration shipped |
| **Superseded** | [normalize_rtl_conv_report.sql](/Users/eugenetsenter/Looker_clonedRepo/looker_personal/master_data_model/model/branches/digital/conversions/normalize_rtl_conv_report.sql) | Reference query over the retired legacy `landing.rtl_conv_report`. The builder does **not** read it |

## Protected surfaces

| Surface | Rule |
|---|---|
| `ALL_DCM_adswerve.Ritual_conversions_last14_*` | Adswerve-owned exports, read-only. 24 distinct tables already feed history |
| `master_stg.rtl_cm360_direct_conversions` | Persistent history, 7,141 rows. Updated only by MERGE[^4] — never truncate; dates outside the rolling window exist nowhere else |
| `master_stg.data_model_v3` | Rebuilt whole by the canonical builder. A direct write is destroyed at the next deploy |
| `landing.rtl_conv_report` | Legacy compatibility shape, not a production input |

---

## The two-write trap

The builder holds **two** `CREATE OR REPLACE TABLE data_model_v3` statements — lines 36
and 816 — and both must run in one execution. Statement 2 snapshots statement 1's output
into the `v3_delivery_base` temp table, then republishes v3 as that base plus the
conversion column family.

| What you ran | Result |
|---|---|
| Both (the whole file) | Correct — conversion columns present and populated |
| Statement 1 only | A v3 in which **22 of the 37 `conv_*` columns do not exist at all** — only the 15 retired Sheet-era columns are declared. A consumer selecting `conv_total_conversions` gets an *unknown column error*, not a zero. The failure is loud but looks like a broken dashboard rather than a partial build |
| Statement 2 only | Fails on duplicate column names — it re-adds a family the prior run already added |

Never deploy a fragment of this file. See
[Two writes, one run](/Users/eugenetsenter/Looker_clonedRepo/looker_personal/master_data_model/model/final_model/README.md#two-writes-one-run).

---

## Grain, keys, and the join

| Stage | Object | Grain[^2] — one row is… | Key |
|---|---|---|---|
| History | `rtl_cm360_direct_conversions` | One CM360 source activity record | `conversion_row_key` — SHA256 of date, advertiser, campaign, site, activity group, activity, creative, placement, package/roadblock |
| Summary | `cm360_by_detail` CTE, lines 818–849 | One package/date/placement/creative combination | `model_detail_key` = `package_id \| date \| placement_id \| LOWER(TRIM(creative))` |
| Output | `data_model_v3` | Either an enriched DCM delivery row, or a standalone conversion row | `conv_model_detail_key` |

`package_id` and `placement_id` are not source columns — the loader parses them out of
`package_roadblock` and `placement` with `REGEXP_EXTRACT(..., r'\|([^|_]+)_')`. A value
that does not fit that shape yields NULL, and a NULL parse can never join.

<details><summary><b>Source activity count is not v3 row count — a 3:1 collapse</b></summary>

Many activities share one `model_detail_key`: **7,141 source activity records collapse to
2,323 keys** (verified 2026-08-21), up to 10 activities on one key. "Conversions in the
source" and "conversion rows in v3" are different populations, so `COUNT(*)` on history is
not a row-count check against v3. The audit trail is `conv_source_row_count` /
`conv_source_activity_record_count` — how many source records folded into that row. Live
totals: **102,657 conversions, $45,520.43 revenue.**

</details>

<details><summary><b>The join counts candidates first and refuses to pick</b></summary>

`dcm_detail_counts` (lines 850–860) counts DCM rows per `model_detail_key` *before*
joining; only `dcm_detail_match_count = 1` attaches. Two matching delivery rows attach to
neither — the model declines rather than splitting or duplicating. That is why conversion
evidence can never double-count into delivery. Only `qa_v3_source_detail_type = 'dcm'`
rows are candidates, so FPD, Polaris email, social, and TV delivery are structurally
unmatchable by design.

</details>

---

## Matched vs conversion-only

Verified 2026-08-21:

| `conv_model_detail_join_status` | Rows | Delivery metrics | `qa_v3_source_detail_type` |
|---|---|---|---|
| `matched_unique` | **1,900** | **Present** — `_spend` and `_impressions` non-NULL on all 1,900 | `dcm` — an unchanged DCM delivery row, enriched |
| `conversion_only` | **423** | NULL — all delivery metrics sum to exactly 0 | `cm360_direct_conversion` |
| `ambiguous_delivery_match` | **0** | NULL if it ever occurs | `cm360_direct_conversion` |

**A conversion row is not automatically a delivery-blank row.** The prior README said
conversion evidence "intentionally retains null delivery measures" without qualification —
true of the 423 conversion-only rows, false of the 1,900 matched ones. Filtering
`conv_model_detail_key IS NOT NULL` to find rows without delivery returns mostly rows that
have it.

---

## Which field to read

`conv_*` outranks nothing in the delivery families and is outranked by nothing — the two
families never overlap, and that is the entire safety model.

| Read | Not | Because |
|---|---|---|
| `conv_total_conversions` | anything `_`-prefixed | `_` finals are resolved *delivery*; no conversion contributes to them |
| `_impressions`, `_clicks` | `conv_source_impressions`, `conv_source_clicks` | Legacy Sheet-era columns, **NULL on all 260,004 rows** — see the open QA item |
| `conv_activities`, `conv_activity_groups` (ARRAYs) | `conv_activity` | The ARRAYs are the live fields, 2,323 rows populated. Scalar `conv_activity` is NULL everywhere |
| `conv_staged_at` | `conv_source_exported_at` | Staging refresh vs. CM360 export — different clocks, both published |

On a matched row, `qa_data_source` and `qa_row_data_sources_available` become
`dcm | cm360_direct_conversion` and `qa_data_source_refresh_at` is overwritten with the
*conversion* staging timestamp — so a delivery-freshness check on those 1,900 rows reads a
conversion clock.

---

## Known gaps and gotchas

<details><summary><b>qa_data_issues means two different things on these rows</b></summary>

On the 423 conversion-only rows the builder overwrites it with the join status, so it reads
`conversion_only`. On the 1,900 matched rows it keeps the **DCM delivery** callout — live
values `no_issues` (1,796), `low_signal_dcm` (53), `missing_prisma_daily` (38),
`missing_prisma_daily | low_signal_dcm` (13) — which describe delivery quality and say
nothing about the conversion. The field is rebuilt from a fixed allow-list by a
`row_callouts` step in the stable base, so never infer its values from branch SQL; query
them.

</details>

<details><summary><b>The loader never deletes, so retracted and renamed source rows persist</b></summary>

The MERGE[^4] has only `WHEN MATCHED THEN UPDATE` and `WHEN NOT MATCHED THEN INSERT`. A
record CM360 later removes stays forever. And because `conversion_row_key` hashes creative,
placement, and package/roadblock, an upstream **rename** mints a new key instead of
updating the old one — both records survive and both aggregate into their own detail keys.

</details>

<details><summary><b>A green loader run is not evidence that data moved</b></summary>

The loader accepts exactly one newest export carrying both `package_roadblock` and
`placement` whose filename date range spans 0–14 days. If none qualifies it returns
`skipped_no_eligible_rolling_export` and exits successfully. Check `MAX(staged_at)` —
currently `2026-08-21 18:44:37`. It does hard-fail on one thing: an `ASSERT` aborts the
run if any `total_conversions` is fractional, protecting the INT64 contract[^1] on
`conv_total_conversions`.

</details>

<details><summary><b>Conversion revenue is CM360's attribution[^3], and the types are inconsistent</b></summary>

`conv_total_revenue` is what CM360's attribution model credited, not booked revenue, and it
is reconciled against no finance source; the click- and view-through splits are that same
opinion, subdivided. Separately: `conv_total_conversions` is INT64 while the five
named-outcome metrics are FLOAT64, and `conv_source_impressions`/`conv_source_clicks` are
declared INT64 in statement 1 but land as FLOAT64 because statement 2's `REPLACE` recasts
them. Harmless today, surprising to a strictly typed consumer.

</details>

---

## Open QA items — a human must decide

<details><summary><b>Seven conv_* columns are NULL on every row — reserved, or dead?</b></summary>

Verified 2026-08-21, zero non-NULL across all 260,004 rows: `conv_activity`,
`conv_source_impressions`, `conv_source_clicks`, `conv_campaign_id`,
`conv_source_sheet_id`, `conv_source_sheet_tab`, `conv_source_sheet_gid`.

All seven belong to the retired Sheet-based path — declared in statement 1's `stable` CTE
(lines 43–57) and exactly the output columns of the superseded
[normalize_rtl_conv_report.sql](/Users/eugenetsenter/Looker_clonedRepo/looker_personal/master_data_model/model/branches/digital/conversions/normalize_rtl_conv_report.sql).
Statement 2 explicitly re-NULLs six of them on matched rows; `conv_campaign_id` is never
assigned by either write. Direct CM360 supplies no equivalent for any of them.

Two of the seven carry documentation weight: `conv_source_impressions` and
`conv_source_clicks` exist so nobody confuses source-side delivery context with
`_impressions`/`_clicks`. Empty, they enforce nothing — the outcome/delivery separation
currently rests on the join design alone, so do not cite these columns as evidence of it.

**Undecided:** reserved shape to preserve, or dead columns to drop. Already tracked as a
separate QA task; do not resolve it here. What would settle it: a consumer inventory of
Looker/Omni fields and saved queries — if nothing selects them, drop them.

</details>

**Stale claim, not an open item.**
[README_v2.md](/Users/eugenetsenter/Looker_clonedRepo/looker_personal/master_data_model/README_v2.md)
line 396 says a conversion dated after a package's delivery dates is kept with
nearest-package context and marked `conversion_date_without_delivery_row`. That string
appears **nowhere** in the builder and **0 rows** carry it. No nearest-package fallback
exists: an unmatched conversion becomes a `conversion_only` row with its own package/date
context and no delivery. The v2 doc is wrong; this page is authoritative.

---

## Refresh

Neither step is scheduled. Run the loader, then the whole builder:

```bash
bq query --project_id=looker-studio-pro-452620 --use_legacy_sql=false < /Users/eugenetsenter/Looker_clonedRepo/looker_personal/master_data_model/model/branches/digital/conversions/merge_rtl_cm360_direct_conversions_latest.sql
bq query --project_id=looker-studio-pro-452620 --use_legacy_sql=false < /Users/eugenetsenter/Looker_clonedRepo/looker_personal/master_data_model/model/final_model/create_master_stg_data_model_v3.sql
```

Loading without rebuilding changes nothing downstream. Rebuilding without loading
republishes yesterday's conversions against today's delivery, shifting the match counts.

## Verify current state yourself

```sql
-- Match health; proof matched rows keep delivery and conversion-only rows contribute zero
SELECT conv_model_detail_join_status, qa_v3_source_detail_type,
       COUNT(*) AS rows_,
       COUNTIF(_impressions IS NOT NULL) AS with_impressions,
       COUNTIF(_spend IS NOT NULL) AS with_spend,
       SUM(COALESCE(_spend,0) + COALESCE(_impressions,0) + COALESCE(_clicks,0)) AS delivery_total,
       SUM(conv_total_conversions) AS conversions
FROM `looker-studio-pro-452620.master_stg.data_model_v3`
WHERE conv_model_detail_key IS NOT NULL
GROUP BY 1,2 ORDER BY rows_ DESC;
```

```sql
-- Collapse ratio, freshness, and whether the seven empty columns are still empty
SELECT (SELECT COUNT(*) FROM `looker-studio-pro-452620.master_stg.rtl_cm360_direct_conversions`) AS source_activity_rows,
       (SELECT COUNT(DISTINCT model_detail_key) FROM `looker-studio-pro-452620.master_stg.rtl_cm360_direct_conversions`) AS detail_keys,
       COUNTIF(conv_activity IS NOT NULL) AS nn_conv_activity,
       COUNTIF(conv_source_impressions IS NOT NULL) AS nn_conv_source_impressions,
       COUNTIF(conv_source_clicks IS NOT NULL) AS nn_conv_source_clicks,
       COUNTIF(conv_campaign_id IS NOT NULL) AS nn_conv_campaign_id,
       MAX(conv_staged_at) AS last_staged
FROM `looker-studio-pro-452620.master_stg.data_model_v3`;
```

Freshness is `MAX(conv_staged_at)`, not `MAX(_date)` — conversion dates run 2026-04-30 to
2026-08-03 and lag the delivery window.

---

## Creative names are remapped after the conversion join

Verified 2026-08-25. Write statement 2 now carries a creative-name mapping layer that
did not exist when this pipeline was first documented. It adds a `_creative_name_raw`
column, joins `master_stg.creative_mapping`, and **replaces `_creative_name` on every
row of v3** — currently **2,955 rows** differ between the two.

<details><summary><b>Rebuild the conversion key from _creative_name_raw, not _creative_name</b></summary>

`conv_model_detail_key` is built from the **pre-mapping** creative name. The published
`_creative_name` is the mapped one. Anyone reproducing the key from the published
column will fail to match on those 2,955 rows, with nothing in the data to explain why.

Locate the layer with:

```bash
grep -n "creative_mapping\|_creative_name_raw" /Users/eugenetsenter/Looker_clonedRepo/looker_personal/master_data_model/model/final_model/create_master_stg_data_model_v3.sql
```

Two `ASSERT` statements guard mapping-key uniqueness and abort the build if violated.

</details>

## Definitions

[^1]: **Contract:** a promise about a table's shape that other work may depend on — which columns exist, what one row means, what will not change without warning. The word signals obligation: something elsewhere is relying on it.

[^2]: **Grain:** what a single row represents. Mixing grains in one aggregate is the most common cause of double-counted totals. This branch adds a second grain to v3, so read `qa_v3_metric_grain` before summing.

[^3]: **Attribution:** the ad platform's decision about which impression or click deserves credit for an outcome. A modeled opinion, not an observation — two platforms scoring the same conversion will disagree, and the numbers are not additive across platforms.

[^4]: **MERGE (upsert):** a load that updates rows it recognizes and inserts rows it does not, without clearing what was there. It preserves history, and it never removes anything — a row deleted upstream stays here forever.

---

- [Direct CM360 migration guide](/Users/eugenetsenter/Looker_clonedRepo/looker_personal/master_data_model/docs/rtl-direct-cm360-conversion-migration.md)
- [DCM delivery pipeline](/Users/eugenetsenter/Looker_clonedRepo/looker_personal/master_data_model/model/branches/dcm/README_dcm-pipeline.md)
- [Source branch index](/Users/eugenetsenter/Looker_clonedRepo/looker_personal/master_data_model/model/branches/README.md)
