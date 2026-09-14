---
pipeline: DCM Delivery
source_type: delivery — platform-reported
output: looker-studio-pro-452620.master_stg.data_model_v3
output_grain: package/date/placement/ad/creative
source_tables:
  - looker-studio-pro-452620.DCM.20250505_costModel_v5
  - looker-studio-pro-452620.landing.apo_dcm_creative_image_asset_map
creative_image_refresh: Apollo DCM Creative Image Refresh (universal cron runner)
loader_script: model/branches/dcm/load_apo_dcm_creative_image_map.R (creative images only)
verified: 2026-08-21
verified_against:
  - model/stable_base/create_master_stg_data_model.sql
  - model/final_model/create_master_stg_data_model_v3.sql
  - model/branches/dcm/load_apo_dcm_creative_image_map.R
reviewers:
  - gene <gene.tsenter@giantspoon.com>
---

# DCM Delivery Pipeline

Campaign Manager delivery — impressions, clicks, media cost, and video — entering the
master data model at its natural placement/ad/creative grain[^1], plus the Apollo
creative-image pathway that attaches a published image URL to those rows.

Delivery is deliberately kept at two grains: a package/date rollup for totals, and a
creative-level sibling so one creative is never falsely chosen to represent a package.

| To do this | Use |
|---|---|
| Isolate DCM rows | `qa_v3_source_detail_type = 'dcm'` |
| Read creative-level detail | [Delivery detail v2](https://console.cloud.google.com/bigquery?project=looker-studio-pro-452620&p=looker-studio-pro-452620&d=master_stg&t=data_model_delivery_detail_v2&page=table) or v3 |
| Read the creative image | `_creative_img` — see [Field contract](#field-contract) |

## Table of Contents

- [Pipeline Overview](#pipeline-overview)
- [Source and Output Contract](#source-and-output-contract)
- [Metric and Field Lineage](#metric-and-field-lineage)
- [Custom Logic and Boundaries](#custom-logic-and-boundaries)
- [Creative Images: How It Works Today](#creative-images-how-it-works-today)
- [Multi-Client Creative Sources — Not Built](#multi-client-creative-sources--not-built)
- [Safe Debugging Route](#safe-debugging-route)
- [Definitions](#definitions)

---

## Pipeline Overview

```text
DCM cost model v5
     ├─→ stable package/date base → Master evidence model
     ├─→ delivery detail v2 → placement/ad/creative evidence
     └─→ Master evidence model v3 → natural delivery-detail rows
```

## Source and Output Contract

| Stage | Object or file | Grain | Responsibility |
|---|---|---|---|
| Delivery source | [DCM cost model v5](https://console.cloud.google.com/bigquery?project=looker-studio-pro-452620&p=looker-studio-pro-452620&d=DCM&t=20250505_costModel_v5&page=table) | Package/date plus delivery detail | DCM delivery, source impressions, media cost, clicks, and video metrics. |
| Package/date base | [Stable package/date base SQL](/Users/eugenetsenter/Looker_clonedRepo/looker_personal/master_data_model/model/stable_base/create_master_stg_data_model.sql) | Package/date | Aggregates DCM daily evidence and joins it to Prisma and FPD. |
| Detail sibling | [Delivery detail SQL](/Users/eugenetsenter/Looker_clonedRepo/looker_personal/master_data_model/model/branches/dcm/create_data_model_delivery_detail_v2.sql) | Package/date/placement/ad/creative | Keeps creative detail visible without falsely selecting one creative for a package/date. |
| Versioned evidence output | [Master evidence model v3](https://console.cloud.google.com/bigquery?project=looker-studio-pro-452620&p=looker-studio-pro-452620&d=master_stg&t=data_model_v3&page=table) | Package/date/placement/ad/creative | Preserves natural DCM detail while carrying planned metrics once per package/date. |

## Metric and Field Lineage

| DCM source field | Evidence field | Final-field rule |
|---|---|---|
| `daily_recalculated_cost` | `dcm_daily_recalculated_cost` | Supplies final spend only when FPD does not provide spend. |
| `impressions` | `dcm_impressions` | Supplies final impressions; recalculated impressions remain QA/source context. |
| `clicks` | `dcm_clicks` | Supplies final clicks only when original FPD does not provide clicks. |
| Rich-media video plays/completions | `dcm_video_plays`, `dcm_video_comps` | Supply final video metrics where no approved manual override applies. |
| Placement, ad, creative | Detail-view identity fields | Stay at detail grain in the [Delivery detail v2](https://console.cloud.google.com/bigquery?project=looker-studio-pro-452620&p=looker-studio-pro-452620&d=master_stg&t=data_model_delivery_detail_v2&page=table) and v3; they are not forced into the package/date creative label. |

## Custom Logic and Boundaries

- The active master-model source is DCM cost model v5, not the combined Basis/DCM reporting view.
- The stable base treats original or updated FPD spend and impressions as higher-priority partner evidence. DCM remains available in `dcm_*` fields for reconciliation.
- The v3 model groups DCM at package/date/placement/ad/creative and uses source `impressions` for final impressions. `daily_recalculated_imps` is retained as source and QA context.
- The delivery-detail builder chooses one context row per package/date, then joins that context to every natural detail row. Planned fields are deliberately named `doNotSum` when repeated.
- Basis is adjacent reporting lineage only. It does not feed the package/date master-model DCM branch.
- `qa_data_issues` is **not** set by this branch. A shared `row_callouts` step in the [stable base](/Users/eugenetsenter/Looker_clonedRepo/looker_personal/master_data_model/model/stable_base/create_master_stg_data_model.sql) *rebuilds* the field from a fixed allow-list of cause tokens and then adds computed metric symptoms — it does not preserve what a branch wrote. A token absent from that allow-list is silently dropped. Query the live values; never infer them from this branch's SQL.

## Creative Images: How It Works Today

Read this section before changing anything about DCM creative images. It answers
the questions that otherwise get re-derived from the loader and the V3 SQL every
time. Facts verified against the live objects on 2026-08-21.

### End-to-end path

```text
Apollo Creative Assignment workbook (READ-ONLY source)
  → tab "STEP 1 | INPUT - Creative Details", columns C:L
  → loader publishes each available image to GitHub via API
  → landing.apo_dcm_creative_image_asset_map (one row per creative name)
  → V3 joins it onto DCM delivery → dcm_creative_img → _creative_img
```

### Canonical entrypoint and safety gates

The only supported way to refresh production is the registered runner workload.
Do not call the loader by hand for a production load; the wrapper is what
supplies the production settings.

| Item | Value |
|---|---|
| Runner workload | [run_apollo_dcm_creative_image_refresh.sh](/Users/eugenetsenter/Docs/R_Studio_Projects/universal_cron_runner/automation_hub/workloads/ops/apollo_dcm_creative_images/run_apollo_dcm_creative_image_refresh.sh) |
| Registered as | `Apollo DCM Creative Image Refresh` in [universal_script_runner.R](/Users/eugenetsenter/Docs/R_Studio_Projects/universal_cron_runner/universal_script_runner.R) |
| Ordering | Registered immediately before `Master Data Model Clustered Advertiser Refresh`, so the map is current before V3 rebuilds. Keep that order. |
| Loader | [load_apo_dcm_creative_image_map.R](/Users/eugenetsenter/Looker_clonedRepo/looker_personal/master_data_model/model/branches/dcm/load_apo_dcm_creative_image_map.R) |
| Publisher | [publish_github_assets.R](/Users/eugenetsenter/Looker_clonedRepo/looker_personal/master_data_model/model/branches/dcm/publish_github_assets.R) — one atomic GitHub API commit, no local clone or checkout |

Running the loader directly is preview-only by default and writes nothing. A
write requires `APO_DCM_CREATIVE_UPLOAD=TRUE`; writing to a table whose name
does not end in `_qa` additionally requires
`APO_DCM_CREATIVE_ALLOW_PRODUCTION=TRUE`. The loader's own default target is the
QA table, while V3 reads the production table — so a hand-run loader does not
change what V3 sees unless the target is set explicitly.

| Surface | Rule |
|---|---|
| The Apollo workbook | **Read-only.** Nothing in this pipeline may write to it. |
| [landing.apo_dcm_creative_image_asset_map](https://console.cloud.google.com/bigquery?project=looker-studio-pro-452620&p=looker-studio-pro-452620&d=landing&t=apo_dcm_creative_image_asset_map&page=table) | **Live map** V3 reads (63 rows). Replaced whole (`WRITE_TRUNCATE`) on refresh. |
| `landing.apo_dcm_creative_image_asset_map_qa` | Loader default. Safe target for testing. |
| `landing.apo_dcm_creative_image_map` | **Superseded, do not use** (36 rows). Retained as historical evidence from the retired design that keyed on DCM *ad* name. |

Two naming traps sit next to each other here. The live table is the one ending
`_asset_map`; the shorter `_map` name is the retired design. Inside the V3 SQL
the CTE is still *named* `apo_dcm_creative_image_map` while correctly reading
`FROM ..._asset_map` — check the `FROM` clause, not the alias.

To rebuild V3 by hand, use the documented command. Improvised `bigrquery` calls
have silently failed to submit the script while appearing to succeed:

```bash
bq query --project_id=looker-studio-pro-452620 --use_legacy_sql=false < /Users/eugenetsenter/Looker_clonedRepo/looker_personal/master_data_model/model/final_model/create_master_stg_data_model_v3.sql
```

### The join, exactly

V3 matches on the **creative** field, not the ad name, scoped to Apollo:

```sql
LOWER(REGEXP_REPLACE(CAST(d.creative AS STRING),
      r'_(?:\d+\s*x\s*\d+|\d+x\d+|0x0|NA)$', ''))
  = m.normalized_dcm_creative_name
```

- `normalized_dcm_creative_name` is the workbook's `Asset Name`, trimmed and lowercased.
- The regex removes a size suffix **only at the very end of the name**.
- Only rows with `publication_status = 'published'` and a non-null URL are eligible; the newest `loaded_at` wins per creative name.
- The join is Apollo-scoped on both sides (`advertiser LIKE '%apollo%'` and `m.advertiser = 'Apollo'`). Creative names are not globally unique across clients, so never drop that scoping.

Matching on placement text, or on the DCM **ad** name, is not safe.

### Field contract

| Field | Meaning |
|---|---|
| `dcm_creative_img` | Published URL resolved through the join above. NULL for every non-DCM source branch. |
| `fpd_creative_img` | Creative link supplied by FPD. |
| `_creative_img` | What consumers read. Defined as `COALESCE(dcm_creative_img, fpd_creative_img)` — **DCM wins when both exist.** |

### Known exceptions — why some creatives have no image

This is expected behavior, not a bug, and it is the thing most likely to be
mistaken for one. As of 2026-08-21, of 52 distinct Apollo DCM creative names:

| Count | Situation | Meaning |
|---|---|---|
| 9 | Image published | `_creative_img` is populated |
| 39 | In the workbook, `no_final_img_path` | The workbook has no `Final_img_path` value; there is nothing to publish |
| 4 | Not in the workbook at all | The join misses — see below |

The workbook holds 63 assets, of which only 9 carry a usable image path. **Low
image coverage is a source-data condition. Report it; do not treat it as a
pipeline failure or attempt to infer a substitute image.**

The four join misses all fail for one reason — text appears *after* the size, so
the end-anchored regex cannot strip it:

| DCM creative name | Blocking text |
|---|---|
| `Apollo-FT_HPTO_970x250_V3_optimized` | `_V3_optimized` after the size |
| `Apollo-FT_HPTO_Video_ 970 x 250_r11 V3-optimized GIF` | `_r11 V3-optimized GIF` after the size |
| `FinancialWorldNYT_300 x 250 (OLD)` | `(OLD)` after the size |
| `InReadNikkei30_0 x 0 (OLD)` | `(OLD)` after the size |

Two are retired `(OLD)` creatives that need no image. The two `V3_optimized`
records are genuine misses; fixing them requires either an `Asset Name` in the
workbook that matches the full DCM creative name, or a deliberate, reviewed
change to the suffix rule. Loosening the regex to strip a size found anywhere in
the name would collapse distinct creatives onto one image — do not do it
casually.

### Verify current state yourself

Prefer running these over trusting the counts above, which age.

```sql
-- Publication coverage in the production map
SELECT publication_status, COUNT(*) AS creatives
FROM `looker-studio-pro-452620.landing.apo_dcm_creative_image_asset_map`
GROUP BY publication_status ORDER BY creatives DESC;
```

```sql
-- Apollo DCM creatives that do not resolve to an image, and why
WITH map AS (
  SELECT normalized_dcm_creative_name AS n, publication_status
  FROM `looker-studio-pro-452620.landing.apo_dcm_creative_image_asset_map`
),
dcm AS (
  SELECT DISTINCT CAST(creative AS STRING) AS raw,
    LOWER(REGEXP_REPLACE(CAST(creative AS STRING),
          r'_(?:\d+\s*x\s*\d+|\d+x\d+|0x0|NA)$','')) AS n
  FROM `looker-studio-pro-452620.DCM.20250505_costModel_v5`
  WHERE LOWER(TRIM(CAST(advertiser AS STRING))) LIKE '%apollo%'
    AND DATE(date) >= DATE '2025-01-01'
)
SELECT d.raw,
  IFNULL(m.publication_status, 'not_in_workbook') AS reason
FROM dcm d LEFT JOIN map m USING (n)
WHERE m.publication_status IS NULL OR m.publication_status != 'published'
ORDER BY reason, d.raw;
```

### Gotchas that have caused real errors

- **Published URL shape** is `https://raw.githubusercontent.com/genetsen/apo-db-creat/main/assets/dcm/apollo/<asset-slug>/<original-filename>`. The `<asset-slug>` folder is the Asset Name lowercased with every run of non-alphanumeric characters replaced by `-`. Two Asset Names differing only in punctuation therefore collide into one folder.
- **Workbook path cleaning is deliberate and fragile.** The Sheet stores a backslash before **every space** in a file path as an escape marker — these are not path separators, so the loader removes all literal backslashes (`fixed = TRUE`, not a regex). Separately, it removes an apostrophe **only at the very start or end** of the value. Those anchors matter: an earlier version stripped apostrophes anywhere, which broke real names such as `BARRON'S` and falsely reported 16 valid mappings as missing files. Do not "simplify" either rule.
- **Regex escaping differs between the two languages here.** BigQuery raw strings take `\d` as the digit token; R needs `\\d`. A rule copied between the SQL and the loader without adjusting this will silently match nothing.
- **A BigQuery script job returns before its statements finish.** Multi-statement rebuilds have been reported complete while still running. Confirm the child statements, not just the parent job.
- **`man_creative_img` is a different pathway.** Manual social/WP creative images are unrelated to this DCM path and are not part of the join described above.
- **Duplicate mappings hard-stop the load.** If one Asset Name points to two different source files, the loader aborts and the source owner must resolve it. BigQuery is left unchanged.
- **Publication is all-or-nothing.** If GitHub does not confirm every intended asset, the loader stops before touching BigQuery.
- **Missing files stay visible.** Unavailable sources are retained as explicit non-published statuses. Never substitute another creative or infer a match from placement text.

## Multi-Client Creative Sources — Not Built

The creative-image loader is **Apollo-only**. Do not point it at another client's
workbook or add a client by copying its configuration: creative names are not
guaranteed unique across clients, so an unscoped match would attach one client's
image to another's creative.

<details><summary><b>What a second client would require</b></summary>

A controlled source registry, one row per client workbook, carrying: `advertiser`,
`workbook_url`, `creative_details_tab`, `asset_name_field`,
`dcm_creative_suffix_rule`, `source_path_field`, `media_repository_prefix`, and
`active`. The shared mapping table must retain the workbook URL, source Asset Name,
normalized creative name, source path, published URL, publication status, and load
time, keyed uniquely on `advertiser + normalized_dcm_creative_name` — the same
client-safe key V3 must join on.

Before replacing the Apollo-only path: confirm the new workbook's exact tab names,
headers, and file-path field; run a small QA mapping table; and verify every mapped
V3 row has the expected client, normalized creative name, and published URL. Keep
unavailable source files as explicit failed-publication records — never substitute
another creative or infer a match from placement text.

This is a design sketch, not an implementation. Nothing below it exists yet.

</details>

## Safe Debugging Route

| Symptom | Trace this path | Why |
|---|---|---|
| DCM delivery is missing | [DCM cost model v5](https://console.cloud.google.com/bigquery?project=looker-studio-pro-452620&p=looker-studio-pro-452620&d=DCM&t=20250505_costModel_v5&page=table) → stable base → [Master evidence model](https://console.cloud.google.com/bigquery?project=looker-studio-pro-452620&p=looker-studio-pro-452620&d=master_stg&t=data_model&page=table) | Confirms source, package/date assembly, and output separately. |
| Creative-level row is absent | Source → [Delivery detail v2](https://console.cloud.google.com/bigquery?project=looker-studio-pro-452620&p=looker-studio-pro-452620&d=master_stg&t=data_model_delivery_detail_v2&page=table) or [v3](https://console.cloud.google.com/bigquery?project=looker-studio-pro-452620&p=looker-studio-pro-452620&d=master_stg&t=data_model_v3&page=table) | The package/date model intentionally is not the creative-detail surface. |
| Final amount differs from DCM | Compare `dcm_*`, `fpd_*`, manual evidence, and `qa_row_data_source_primary` | FPD or a valid manual edit may intentionally win. |

## Definitions

[^1]: **Grain:** what a single row represents. DCM's natural grain is one placement, ad, and creative on one date; the package/date rollup is a coarser grain over the same delivery. Mixing the two without accounting for it is the most common cause of double-counted totals.

---

## Related Guides

- [FPD pipeline](/Users/eugenetsenter/Looker_clonedRepo/looker_personal/master_data_model/model/branches/fpd/README_fpd-pipeline.md)
- [Prisma planning pipeline](/Users/eugenetsenter/Looker_clonedRepo/looker_personal/master_data_model/model/branches/prisma/README_prisma-pipeline.md)
- [Basis, DCM, and master-model handoff](/Users/eugenetsenter/Looker_clonedRepo/looker_personal/docs/BASIS_DCM_MASTER_DATA_MODEL_PIPELINE.md)
