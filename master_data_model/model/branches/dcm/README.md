# DCM Branch

Use this folder for DCM delivery, source/detail grain, creative-safe joins, and DCM-specific metric behavior.

DCM creative can be one-to-many at package/date grain, so detail handling belongs here rather than being silently collapsed into package/date fields.

## Live Pipeline Boundary

The live master data model reads DCM delivery from [DCM cost model v5](https://console.cloud.google.com/bigquery?project=looker-studio-pro-452620&p=looker-studio-pro-452620&d=DCM&t=20250505_costModel_v5&page=table), not from [Joined DCM and Basis view](https://console.cloud.google.com/bigquery?project=looker-studio-pro-452620&p=looker-studio-pro-452620&d=final_views&t=joined_dcmBasis&page=table).

Basis delivery has a neighboring reporting path:

| Surface | Role | Master-model relationship |
|---|---|---|
| [Basis Google Sheet external table](https://console.cloud.google.com/bigquery?project=looker-studio-pro-452620&p=looker-studio-pro-452620&d=repo_stg&t=basis_gsheet2&page=table) | Looker-owned external table over the Basis delivery Sheet. | Does not directly feed the master model. |
| [Basis delivery master](https://console.cloud.google.com/bigquery?project=looker-studio-pro-452620&p=looker-studio-pro-452620&d=repo_stg&t=basis_master2&page=table) | `basis_update` target refreshed daily at `10:00` UTC. | Feeds combined DCM/Basis reporting, not package/date master rows. |
| [Joined DCM and Basis view](https://console.cloud.google.com/bigquery?project=looker-studio-pro-452620&p=looker-studio-pro-452620&d=final_views&t=joined_dcmBasis&page=table) | Union of [Final DCM view](https://console.cloud.google.com/bigquery?project=looker-studio-pro-452620&p=looker-studio-pro-452620&d=final_views&t=dcm&page=table) and [Basis delivery master](https://console.cloud.google.com/bigquery?project=looker-studio-pro-452620&p=looker-studio-pro-452620&d=repo_stg&t=basis_master2&page=table). | Useful for DCM/Basis reporting checks, but not the master-model source. |
| [Master evidence model](https://console.cloud.google.com/bigquery?project=looker-studio-pro-452620&p=looker-studio-pro-452620&d=master_stg&t=data_model&page=table) | Package/date evidence model. | Reads DCM cost-model fields and maps them into `dcm_*` and final `_` metrics. |
| [Master delivery detail model](https://console.cloud.google.com/bigquery?project=looker-studio-pro-452620&p=looker-studio-pro-452620&d=master_stg&t=data_model_delivery_detail_v2&page=table) | Source/detail grain sibling model. | Preserves DCM package/date/placement/ad/creative rows and non-summable package context. |

## Files In This Branch

| File | Purpose |
|---|---|
| [Delivery detail SQL](/Users/eugenetsenter/Looker_clonedRepo/looker_personal/master_data_model/model/branches/dcm/create_data_model_delivery_detail_v2.sql) | Builds the lower-grain delivery detail model so DCM placement, ad, and creative rows stay visible. |

## Debugging Guide

| Symptom | First check | Reason |
|---|---|---|
| Basis delivery looks stale | [Basis delivery master](https://console.cloud.google.com/bigquery?project=looker-studio-pro-452620&p=looker-studio-pro-452620&d=repo_stg&t=basis_master2&page=table) and the `basis_update` transfer run | Basis is owned by the Basis scheduled query, not this DCM branch. |
| DCM rows are missing from the master model | [DCM cost model v5](https://console.cloud.google.com/bigquery?project=looker-studio-pro-452620&p=looker-studio-pro-452620&d=DCM&t=20250505_costModel_v5&page=table), then [Master evidence model](https://console.cloud.google.com/bigquery?project=looker-studio-pro-452620&p=looker-studio-pro-452620&d=master_stg&t=data_model&page=table) | The package/date model reads the DCM cost model directly. |
| Creative-level DCM rows are needed | [Master delivery detail model](https://console.cloud.google.com/bigquery?project=looker-studio-pro-452620&p=looker-studio-pro-452620&d=master_stg&t=data_model_delivery_detail_v2&page=table) | DCM creative is intentionally kept out of the package/date `_creative_name` field. |

See [Basis, DCM, and master data model pipeline](/Users/eugenetsenter/Looker_clonedRepo/looker_personal/docs/BASIS_DCM_MASTER_DATA_MODEL_PIPELINE.md) for the full end-to-end map.
