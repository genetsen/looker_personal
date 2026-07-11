# DCM Delivery Pipeline

This guide documents Campaign Manager delivery lineage in the master data model, including the intentionally separate package/date and creative-safe detail outputs. It follows the structure of [Master Data Model Pipeline v2](/Users/eugenetsenter/Looker_clonedRepo/looker_personal/master_data_model/README_v2.md).

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

## Safe Debugging Route

| Symptom | Trace this path | Why |
|---|---|---|
| DCM delivery is missing | [DCM cost model v5](https://console.cloud.google.com/bigquery?project=looker-studio-pro-452620&p=looker-studio-pro-452620&d=DCM&t=20250505_costModel_v5&page=table) → stable base → [Master evidence model](https://console.cloud.google.com/bigquery?project=looker-studio-pro-452620&p=looker-studio-pro-452620&d=master_stg&t=data_model&page=table) | Confirms source, package/date assembly, and output separately. |
| Creative-level row is absent | Source → [Delivery detail v2](https://console.cloud.google.com/bigquery?project=looker-studio-pro-452620&p=looker-studio-pro-452620&d=master_stg&t=data_model_delivery_detail_v2&page=table) or [v3](https://console.cloud.google.com/bigquery?project=looker-studio-pro-452620&p=looker-studio-pro-452620&d=master_stg&t=data_model_v3&page=table) | The package/date model intentionally is not the creative-detail surface. |
| Final amount differs from DCM | Compare `dcm_*`, `fpd_*`, manual evidence, and `qa_row_data_source_primary` | FPD or a valid manual edit may intentionally win. |

## Related Guides

- [FPD pipeline](/Users/eugenetsenter/Looker_clonedRepo/looker_personal/master_data_model/model/branches/fpd/README_fpd-pipeline.md)
- [Prisma planning pipeline](/Users/eugenetsenter/Looker_clonedRepo/looker_personal/master_data_model/model/branches/prisma/README_prisma-pipeline.md)
- [Basis, DCM, and master-model handoff](/Users/eugenetsenter/Looker_clonedRepo/looker_personal/docs/BASIS_DCM_MASTER_DATA_MODEL_PIPELINE.md)
