---
pipeline: Social Delivery (cross-platform, Reddit, WP Apollo)
source_type: delivery — actuals; planned spend exists only where pacing does
output: looker-studio-pro-452620.master_stg.data_model_v3
output_grain: source_social_daily
source_tables:
  - looker-studio-pro-452620.repo_stg.stg__olipop__crossplatform_raw_tbl
  - looker-studio-pro-452620.landing.reddit-ads-email
  - looker-studio-pro-452620.repo_stg.stg__wp__search_data_template_daily
  - looker-studio-pro-452620.repo_int.crossplatform_pacing_tbl
refresh: scheduled queries, daily 10:00 UTC (shared staging and pacing mart/snapshot) and 10:15 UTC (master upstream snapshots); WP workbook loader is manual
loader_script: model/branches/social/wp_search/load_wp_search_data_template.R (WP workbook only; nothing else has a loader)
verified: 2026-09-15
verified_against:
  - model/stable_base/create_master_stg_data_model.sql
  - model/final_model/create_master_stg_data_model_v3.sql
  - model/branches/social/wp_search/sql/create_stg_crossplatform_wp_primary_production.sql
  - model/branches/social/reddit/stg__olipop_reddit_crossplatform.sql
  - ../sql/crossplatform/int__crossplatform_pacing.sql
  - ../sql/marts/olipop/mart__pacing__table.sql
  - live master_stg.data_model, master_stg.data_model_v3, repo_stg.stg__olipop__crossplatform_raw_tbl, repo_stg.stg__wp__search_data_template_daily, repo_int.crossplatform_pacing_tbl, repo_mart.fct_crossplatform_pacing_daily, master_stg.advertiser_mapping
reviewers:
  - gene <gene.tsenter@giantspoon.com>
---

# Social Delivery Pipeline

Daily ad-level social, paid-search, and online-video delivery — Meta, TikTok, LinkedIn, Pinterest, Google Ads, Reddit — entering [data_model_v3](https://console.cloud.google.com/bigquery?project=looker-studio-pro-452620&p=looker-studio-pro-452620&d=master_stg&t=data_model_v3&page=table) as source rows under synthetic keys[^1], because social has no Prisma package ID. **60,621 rows live as of 2026-08-25**, dated 2025-01-01 to 2026-08-23.

| To do this | Use |
|---|---|
| Isolate social rows | `qa_media_data_type = 'social'` (equivalently `_package_id LIKE 'social:%'`) |
| Isolate WP-sourced Apollo rows | `s_wp_row_key IS NOT NULL`, or `qa_v3_source_detail_type = 'wp_search_data_template'` |
| Read social's own evidence | `s_*` fields — unchanged source values |
| Read reporting values | `_spend`, `_impressions`, `_clicks` — see [Output Fields](#output-fields-and-precedence) |

## Table of Contents

- [Canonical files and protected surfaces](#canonical-files-and-protected-surfaces) · [How the Data Arrives](#how-the-data-arrives) · [Sources and Boundaries](#sources-and-boundaries)
- [Layer Responsibilities](#layer-responsibilities) · [WP Precedence for Apollo](#wp-precedence-for-apollo) · [Output Fields and Precedence](#output-fields-and-precedence)
- [Known Gaps and Gotchas](#known-gaps-and-gotchas) · [Refresh and Proof](#refresh-and-proof) · [Verify Current State Yourself](#verify-current-state-yourself)
- [Troubleshooting](#troubleshooting) · [Definitions](#definitions)

## Canonical files and protected surfaces

`model/**` is the only active workspace. Authority order for any disagreement: **live `data_model_v3` → semantic model documentation → the SQL below.**

| Purpose | File |
|---|---|
| Shared social staging builder — WP precedence, campaign exclusions, Reddit union | [create_stg_crossplatform_wp_primary_production.sql](/Users/eugenetsenter/Looker_clonedRepo/looker_personal/master_data_model/model/branches/social/wp_search/sql/create_stg_crossplatform_wp_primary_production.sql) |
| WP workbook loader, its business rules, and their tests | [load_wp_search_data_template.R](/Users/eugenetsenter/Looker_clonedRepo/looker_personal/master_data_model/model/branches/social/wp_search/load_wp_search_data_template.R), [wp_search_social_logic.R](/Users/eugenetsenter/Looker_clonedRepo/looker_personal/master_data_model/model/branches/social/wp_search/wp_search_social_logic.R), [tests](/Users/eugenetsenter/Looker_clonedRepo/looker_personal/master_data_model/model/branches/social/wp_search/tests/test_wp_search_social_logic.R) |
| Reddit standardization view and CSV loader | [stg__olipop_reddit_crossplatform.sql](/Users/eugenetsenter/Looker_clonedRepo/looker_personal/master_data_model/model/branches/social/reddit/stg__olipop_reddit_crossplatform.sql), [load_reddit_csv.r](/Users/eugenetsenter/Looker_clonedRepo/looker_personal/master_data_model/model/branches/social/reddit/load_reddit_csv.r) |
| Social normalization for all platforms | [stable base](/Users/eugenetsenter/Looker_clonedRepo/looker_personal/master_data_model/model/stable_base/README.md) → [create_master_stg_data_model.sql](/Users/eugenetsenter/Looker_clonedRepo/looker_personal/master_data_model/model/stable_base/create_master_stg_data_model.sql) |
| Final reporting shape | [final model](/Users/eugenetsenter/Looker_clonedRepo/looker_personal/master_data_model/model/final_model/README.md) → [create_master_stg_data_model_v3.sql](/Users/eugenetsenter/Looker_clonedRepo/looker_personal/master_data_model/model/final_model/create_master_stg_data_model_v3.sql) |

`wp_search/sql/*_qa.sql`, `wp_search/sql/validate_*.sql`, and `wp_search/archive_candidates/` are **not** canonical — they build review-only objects or restore pre-WP behavior, and none runs in production. **Never write to:** the two upstream `giant-spoon-299605.ad_reporting_*` delivery tables; `landing.reddit-ads-email`; the user-owned WP Google Sheet; the three rebuilt staging and snapshot tables (`stg__wp__search_data_template_daily`, `stg__olipop__crossplatform_raw_tbl`, `repo_int.crossplatform_pacing_tbl` — a direct write is overwritten at the next run); or the live BigQuery transfer configs.

## How the Data Arrives

```text
platform delivery (freshest of two ad_report tables) ─┐
Reddit email landing → standardization view ──────────┼─→ stg__olipop__crossplatform_raw_tbl
WP workbook → stg__wp__search_data_template_daily ────┘         (daily 10:00 UTC)
                                                                       │
repo_int.crossplatform_pacing_tbl (daily 10:00 UTC) ───────────────────┤
                                                                       ▼
                                      social_daily → … → social_final (stable base)
                                                                       ▼
                                                            master_stg.data_model_v3
```

The shared-staging, pacing, and master-upstream schedules are documented in [BigQuery Scheduled Queries](/Users/eugenetsenter/Looker_clonedRepo/looker_personal/docs/SCHEDULED_QUERIES.md). The pacing schedule `mart__pacing_table` is the live owner for the daily pacing mart and shared snapshot; it must keep its transfer-level destination dataset blank because its SQL writes fully qualified targets. The WP workbook load is **deliberately manual** while the cross-campaign ad-ID question below is open. Campaign names containing `1000heads` or `pros_dysrupt` are agency-side rows excluded case-insensitively from both source legs and again from the final union. Effective live: **0 rows of 97,751 in staging match either token, verified 2026-08-25.**

TikTok ADIF Smart+ delivery keeps its creative ID as the ad-level identity. When hierarchy fields are blank, the shared-social builder fills them from the latest matching `creative_history` row; it does not replace the creative ID with the parent Smart+ ad ID. Pacing is a separate match on platform, campaign, ad group, and date, but the unified pacing view now reads both standard TikTok and ADIF Smart+ history and admits both `_gs_` and `WP_` campaigns. Verified September 15, 2026: all 70 current delivery rows across seven Smart+ creatives had hierarchy and pacing, with unchanged actual spend, impressions, and clicks.

## Sources and Boundaries

| Source | Grain[^2] — one row is… | Role | May **not** feed |
|---|---|---|---|
| [stg__olipop__crossplatform_raw_tbl](https://console.cloud.google.com/bigquery?project=looker-studio-pro-452620&p=looker-studio-pro-452620&d=repo_stg&t=stg__olipop__crossplatform_raw_tbl&page=table) | One platform/campaign/ad-group/ad on one date | The stable base's **only** social input | It is the assembled product, not any platform's raw API table — query platform data upstream, not here. |
| [landing.reddit-ads-email](https://console.cloud.google.com/bigquery?project=looker-studio-pro-452620&p=looker-studio-pro-452620&d=landing&t=reddit-ads-email&page=table) | One Reddit campaign/ad-group/ad on one date | Reaches the model only via the Reddit standardization view | The model directly — it lacks the shared 38-column contract[^3]. |
| [stg__wp__search_data_template_daily](https://console.cloud.google.com/bigquery?project=looker-studio-pro-452620&p=looker-studio-pro-452620&d=repo_stg&t=stg__wp__search_data_template_daily&page=table) | One Apollo campaign/ad-group/ad on one date | Primary for matching Apollo rows | Non-Apollo clients. Apollo scope is `UPPER(account_name)` matching `APOLLO`. |
| [repo_int.crossplatform_pacing_tbl](https://console.cloud.google.com/bigquery?project=looker-studio-pro-452620&p=looker-studio-pro-452620&d=repo_int&t=crossplatform_pacing_tbl&page=table) | One campaign/ad-group flight with a budget | Planned spend only | Actuals — it carries no delivery, and covers only three platforms. |

## Layer Responsibilities

Locate any stage with `grep -n "^<cte> AS (" /Users/eugenetsenter/Looker_clonedRepo/looker_personal/master_data_model/model/stable_base/create_master_stg_data_model.sql`.

| Stage | Accountable for | Does **not** own |
|---|---|---|
| The staging builder | Choosing the fresher delivery table; WP precedence; campaign exclusions; unioning Reddit | Aggregation, pacing, package identity |
| `social_daily` | Platform-name normalization; summing metrics to a 9-field key; `ANY_VALUE` on every WP column | Pacing, keys, issue labels |
| `social_pacing_dedup` → `social_pacing_daily` | Flattening each flight budget evenly across its window | Any platform absent from the pacing table |
| `social_with_pacing` | Joining pacing on date/platform/campaign/ad-group; dropping post-flight zero-delivery rows | Metric values |
| `social_final` | Package and placement identity, channel and supplier labels, dividing planned spend across ads in the group | Final `qa_data_issues` — rebuilt[^4] later by `row_callouts` |
| `final_model_rows`, then the final model's `non_digital_source_rows` | Renaming to the `s_*` / `_*` contract; `qa_v3_metric_grain = 'source_social_daily'`; planned-carrier placement; manual-override suppression | Social values themselves |

Because `social_daily` uses `ANY_VALUE` for all thirteen WP columns, two staging rows sharing the 9-field key with different WP values resolve arbitrarily and silently.

### Keys and grain

Social invents its identity as plain concatenation — no hashing, so the key stays readable and reversible. `_package_id` is `social:<normalized platform>:<campaign_id>:<ad_group_id>`; `_placement_id` is the ad ID as text; one row is one ad on one date. Consequence for joins: a social row **never** joins to Prisma package metadata, so `_product_code`, `_cost_method`, `_payable_rate`, and the Prisma planned fields are always NULL, and no social row ever carries `missing_prisma_package` — its planned-data gap is labelled `missing_social_pacing` instead. Renaming a platform, campaign, or ad group upstream mints a **new** package; the old one simply stops receiving rows.

## WP Precedence for Apollo

WP wins field by field, only inside its matched scope. In `merged_apollo` (in the staging builder) the standard Apollo leg and the publishable WP leg are joined `FULL OUTER` on four keys: `date_day`, lower-cased `platform`, `ad_id` as text, and lower/trimmed `campaign_name`. Then:

- **Text fields:** `COALESCE(NULLIF(wp, ''), standard)` — a blank WP string yields to standard.
- **Numeric fields:** `COALESCE(wp, standard)` — a WP **zero is a real value** and wins.
- **Fields WP never supplies** (`conversions`, `conversions_value`, `video_play`, the four video percentiles, `hookrate_num`): taken from standard and listed in `s_fallback_fields`.
- Rows whose `wp_publication_status` is neither `publish` nor `publish_pending_source_owner_review` never enter the join at all.

Because `campaign_name` is a join key, a WP campaign-name edit splits one merged row into two unmatched rows — one WP-only, one standard-only — rather than updating in place.

`s_record_source` records which side won. Live 2026-08-25: `wp_primary_with_standard_fallback` 21,784 · `wp_only` 4,834 · `wp_only_pending_source_owner_review` 624 · `standard_only` 33,379. Channel classification (`s_channel_classification_source`): `sheet_channel_fallback` 21,910 · `campaign_marker_search` 4,732 · `campaign_marker_youtube` 600. A campaign carrying both `_Search_` and `_YT_` is excluded upstream as ambiguous.

## Output Fields and Precedence

Social's own values keep an `s_` prefix and stay unchanged; final reporting fields are prefixed with an underscore. Counts verified 2026-08-25.

| Final field | Filled from | Note |
|---|---|---|
| `_spend`, `_impressions`, `_clicks` | `spend`, `impressions`, `clicks` after `social_daily` summing | Identical to `s_spend` / `s_impressions` / `s_clicks` |
| `_video_plays` · `_video_views` · `_video_comps` | `video_play` (20,972 rows) · `COALESCE(s_video_views, …)`, social winning over Polaris and FPD (49,050) · `video_views_p_100` (21,014) | `_video_comps` is a completion **proxy**, not a platform completes metric |
| `_planned_spend` | Package/date plan, carried only on the row where `planned_carrier_rank = 1` so totals stay summable | 5,100 rows |
| `s_pacing_planned_spend` | This row's share: daily flight budget ÷ ads in that group that day | 23,753 rows; safe to sum across ads |
| `_planned_impressions` | Nothing — social pacing carries budget only | Always NULL |
| `_creative_name` | `COALESCE(fpd_orig_creative, amzn_ad_name, social_creative_name)` — social is the **last** fallback | 58,064 rows |
| `_creative_img` | `COALESCE(fpd_creative_img, man_creative_img)`; WP's thumbnail arrives in `man_creative_img` despite its manual-sounding name | **0 social rows** — see gaps |
| `s_wp_row_key` | WP's campaign/ad-group/ad grain key | Non-NULL on exactly the 27,242 WP-sourced rows; the cleanest WP test |
| `s_fallback_fields`, `s_source_sheet_url`, `s_loaded_at`, `s_publication_status` | WP provenance | NULL on standard and Reddit rows |
| `qa_data_issues` | Rebuilt[^4] downstream by `row_callouts` | Never read this from `social_final`; query the live values. A zero metric is not a missing one: 1,120 rows carry `planned_spend_no_actual` (a genuine zero inside a funded flight) and 168 carry `missing_final_metrics` (NULL from source) |

Advertiser normalization, verified live: LinkedIn's `Apollo Corporate` (and a trailing-space variant) resolves to canonical `Apollo` through an inline rule in `with_standardized_advertiser_base`, **not** through `master_stg.advertiser_mapping` — a mapping-table lookup will not find it. Reddit's `Olipop Reddit` account alias does resolve to `Olipop` through an active `advertiser_mapping` row. Manual package edits suppress social actuals exactly as they suppress TV's; that mechanism is documented once in the [final model guide](/Users/eugenetsenter/Looker_clonedRepo/looker_personal/master_data_model/model/final_model/README.md). No social package/date is currently overridden.

## Known Gaps and Gotchas

<details><summary><b>Pacing exists for only three platforms, so most social rows have no planned spend</b></summary>

`repo_int.crossplatform_pacing_tbl` holds `facebook` (8,465 flights), `tiktok` (296), and `google_ads` (63) — nothing else, verified 2026-08-25. LinkedIn, Pinterest, Snapchat, and **Reddit have no rows at all**, so every row on those platforms carries `missing_social_pacing` and zero planned spend. A source gap, not a join bug. Reddit's whole live footprint: **654 rows, 2026-04-17 through 2026-05-31, $175,000 of spend, 100% `missing_social_pacing`** — closing it needs a Reddit pacing plan upstream. All 27,242 WP-sourced Apollo rows are also `missing_social_pacing`, because WP supplies delivery and never budget.

</details>

<details><summary><b>Raw staging and the model legitimately disagree by thousands of rows</b></summary>

`social_with_pacing` drops rows dated after a matched flight's end date that delivered nothing. Verified 2026-08-25: raw staging holds 67,134 rows in the modelled window against the model's 60,621 — a gap of **6,513 rows carrying $0.00 of spend** (6,402 TikTok, 107 Meta, 4 LinkedIn). Reconciling raw counts to model counts will never balance, and should not.

Mechanism worth knowing: all 6,402 TikTok rows have a NULL `video_view`, so the all-zero test evaluates to NULL rather than TRUE and the row is filtered by three-valued logic, not by matching the written condition. The outcome still matches the intent — any row with real spend, impressions, or clicks fails on that conjunct and is kept — but the SQL does not read the way it behaves.

</details>

<details><summary><b>WP creative thumbnails are plumbed but have never carried a value</b></summary>

The loader converts supported YouTube links into thumbnail URLs in `wp_creative_img`, which the stable base maps to `man_creative_img` and thence to `_creative_img`. Live 2026-08-25 that column is NULL for **all 27,320 WP staging rows, all 97,751 shared staging rows, and all 60,621 social model rows.** Treat social creative imagery as unavailable, not as a broken join.

</details>

<details><summary><b>Cross-campaign Apollo ad IDs publish, flagged, pending a source-owner rule</b></summary>

WP rows whose ad ID appears under more than one campaign publish rather than drop, carrying `s_publication_status = 'publish_pending_source_owner_review'` and `pending_wp_source_owner_review` in `qa_data_issues`: 624 rows, 2025-12-15 to 2026-02-03, $18,337. Keep the flag visible; do not resolve it by picking a winner.

</details>

<details><summary><b>Open QA item — Google Ads rows arrive from two sources under one platform label</b></summary>

`s_platform = 'google_ads'` covers 8,663 rows split between `wp_search_data_template` (5,458) and standard delivery (3,205), indistinguishable by platform or channel alone. Of the standard rows, 1,134 have pacing (`no_issues`, 2025-05-22 to 2025-12-31) and 2,071 do not, leaving **$1,627,272 of spend unmatched to any plan**. Whether those 2,071 are genuinely unplanned or are failing to join pacing on `campaign_id`/`ad_group_id` **cannot be settled from either source** — the pacing table holds only 63 Google Ads flights, too few to be the whole account. A human with planning-system access must decide whether Google Ads pacing is intentionally partial. Do not loosen the pacing join to close it.

</details>

## Refresh and Proof

Refresh in dependency order — shared staging (or trigger its existing transfer), then the stable base, then the final model:

```bash
bq query --project_id=looker-studio-pro-452620 --use_legacy_sql=false < /Users/eugenetsenter/Looker_clonedRepo/looker_personal/master_data_model/model/branches/social/wp_search/sql/create_stg_crossplatform_wp_primary_production.sql
bq query --project_id=looker-studio-pro-452620 --use_legacy_sql=false < /Users/eugenetsenter/Looker_clonedRepo/looker_personal/master_data_model/model/stable_base/create_master_stg_data_model.sql
bq query --project_id=looker-studio-pro-452620 --use_legacy_sql=false < /Users/eugenetsenter/Looker_clonedRepo/looker_personal/master_data_model/model/final_model/create_master_stg_data_model_v3.sql

# Re-reading the WP workbook is a separate, deliberate step:
WP_SEARCH_TABLE=stg__wp__search_data_template_daily WP_SEARCH_UPLOAD=TRUE \
WP_SEARCH_ALLOW_PRODUCTION=TRUE \
Rscript /Users/eugenetsenter/Looker_clonedRepo/looker_personal/master_data_model/model/branches/social/wp_search/load_wp_search_data_template.R
```

**Proof per work type.** WP rule change: the unit tests, then the QA builders, then `validate_wp_primary_social_qa.sql` — before production. Staging or stable-base SQL change: the queries below, compared against the counts stamped in this doc. **Not proof:** a successful `bq query`, a matching schema, or an unchanged total row count — every gap documented here survives all three. If a local SQL edit is meant to persist, update the saved transfer configuration too; BigQuery does not copy file edits into a scheduled query.

## Verify Current State Yourself

```sql
-- Coverage by platform and source. Expect every LinkedIn/Pinterest/Reddit row to show
-- missing_social_pacing, and Reddit to span 2026-04-17..2026-05-31 only.
SELECT s_platform, qa_v3_source_detail_type, s_record_source, qa_data_issues,
       COUNT(*) AS rows_, MIN(_date) AS min_date, MAX(_date) AS max_date,
       COUNTIF(s_pacing_planned_spend IS NULL) AS no_pacing, ROUND(SUM(_spend), 0) AS spend
FROM `looker-studio-pro-452620.master_stg.data_model_v3`
WHERE qa_media_data_type = 'social' GROUP BY 1,2,3,4 ORDER BY rows_ DESC;
```

```sql
-- Which platforms can have planned spend at all. Expect exactly facebook, tiktok,
-- google_ads. A Reddit row here means the gap closed upstream and this doc is stale.
SELECT LOWER(platform) AS platform_raw, COUNT(*) AS flights,
       MIN(start_date) AS min_start, MAX(end_date) AS max_end
FROM `looker-studio-pro-452620.repo_int.crossplatform_pacing_tbl` GROUP BY 1 ORDER BY flights DESC;
```

```sql
-- Reconciliation and exclusion health. Expect model_rows < raw_rows_in_window, the
-- difference carrying ~$0 of spend, and excluded_leaks = 0.
WITH mdl AS (SELECT COUNT(*) AS model_rows, MAX(_date) AS d
  FROM `looker-studio-pro-452620.master_stg.data_model_v3` WHERE qa_media_data_type = 'social')
SELECT mdl.model_rows, mdl.d AS model_max_date,
  (SELECT COUNT(*) FROM `looker-studio-pro-452620.repo_stg.stg__olipop__crossplatform_raw_tbl`
   WHERE date_day BETWEEN DATE '2025-01-01' AND mdl.d) AS raw_rows_in_window,
  (SELECT COUNTIF(REGEXP_CONTAINS(LOWER(COALESCE(campaign_name,'')), r'(1000heads|pros_dysrupt)'))
   FROM `looker-studio-pro-452620.repo_stg.stg__olipop__crossplatform_raw_tbl`) AS excluded_leaks
FROM mdl;
```

Add `s_account_name, _advertiser` to the first query's grouping to check advertiser normalization: expect `Apollo Corporate` → `Apollo`, `Olipop Reddit` → `Olipop`, and no `Unknown`. Freshness is the two scheduled queries' last successful runs, not `MAX(_date)`; staging normally leads the model by a day.

## Troubleshooting

| Symptom | Check | Why that check |
|---|---|---|
| Social spend below expectation | `qa_data_issues` for `missing_final_metrics`, then the reconciliation query | Separates NULL source metrics from the deliberate zero-tail drop |
| A WP edit never appeared | `s_loaded_at` and `s_wp_row_key` on the affected rows | The WP loader is manual; a stale `s_loaded_at` means the workbook was never re-read |
| A campaign vanished after a rename | `_package_id` under both old and new names | Package identity is concatenated from platform/campaign/ad-group, so a rename forks the package |
| Planned spend looks doubled | Whether the query sums `_planned_spend` or `s_pacing_planned_spend` | The first is a package-level carrier on one row; the second is a per-ad share |

## Definitions

[^1]: **Synthetic key:** an ID this model invents because the source has none. Reproducible from source values, but it exists nowhere outside this model and cannot be looked up in the planning system — so a social row can never be reconciled against a Prisma package.

[^2]: **Grain:** what a single row represents. Combining tables of different grains without accounting for it is the most common cause of double-counted totals.

[^3]: **Contract:** a promise about a table's shape that other work depends on — which columns exist, what one row means, and what will not change without warning. Reddit must match the shared 38-column contract before it can be unioned in.

[^4]: **Rebuild:** the field is recomputed from scratch from a fixed allow-list rather than carried forward. Anything an earlier stage wrote that is not on the list is silently dropped, so a branch's own SQL is not evidence of what the field contains.

---

- [Master Data Model Pipeline v2](/Users/eugenetsenter/Looker_clonedRepo/looker_personal/master_data_model/README_v2.md) · [BigQuery Scheduled Queries](/Users/eugenetsenter/Looker_clonedRepo/looker_personal/docs/SCHEDULED_QUERIES.md) · [WP workbook workflow](/Users/eugenetsenter/Looker_clonedRepo/looker_personal/master_data_model/model/branches/social/wp_search/README.md) · [TV pipeline](/Users/eugenetsenter/Looker_clonedRepo/looker_personal/master_data_model/model/branches/tv/README_tv-pipeline.md)
