# Master Data Model Source Branches

This folder is the source-family map for the active master data model. Each guide traces source data through the stable package/date base, explains custom logic and grain, and links to the current output surfaces. The documentation mirrors the reader-first pipeline style of [Master Data Model Pipeline v2](/Users/eugenetsenter/Looker_clonedRepo/looker_personal/master_data_model/README_v2.md).

## Branch Guide Index

| Source family | Guide | Use it for |
|---|---|---|
| Planning | [Prisma planning pipeline](/Users/eugenetsenter/Looker_clonedRepo/looker_personal/master_data_model/model/branches/prisma/README_prisma-pipeline.md) | Planned package/date metrics, metadata, and digital context. |
| DCM delivery | [DCM delivery pipeline](/Users/eugenetsenter/Looker_clonedRepo/looker_personal/master_data_model/model/branches/dcm/README_dcm-pipeline.md) | DCM source lineage, detail grain, and the Basis boundary. |
| Partner delivery | [FPD pipeline](/Users/eugenetsenter/Looker_clonedRepo/looker_personal/master_data_model/model/branches/fpd/README_fpd-pipeline.md) | Original and updated FPD precedence, creative evidence, and partner requests. |
| Social | [Social delivery pipeline](/Users/eugenetsenter/Looker_clonedRepo/looker_personal/master_data_model/model/branches/social/README_social-pipeline.md) | Standard social, Reddit, WP workbook precedence, QA, and rollback. |
| Linear TV | [TV estimates pipeline](/Users/eugenetsenter/Looker_clonedRepo/looker_personal/master_data_model/model/branches/tv/README_tv-pipeline.md) | Local/national TV estimates, synthetic keys, and planned-value fallback. |
| Amazon Ads | [Amazon Ads pipeline](/Users/eugenetsenter/Looker_clonedRepo/looker_personal/master_data_model/model/branches/amazon/README_amazon-pipeline.md) | Ritual Amazon daily delivery, commercial outcomes, and email-file lineage. |

## Shared Model Surfaces

| Surface | Purpose |
|---|---|
| [Stable package/date base SQL](/Users/eugenetsenter/Looker_clonedRepo/looker_personal/master_data_model/model/stable_base/create_master_stg_data_model.sql) | Builds the shared package/date evidence layer and source-specific evidence fields. |
| [Final model v3 SQL](/Users/eugenetsenter/Looker_clonedRepo/looker_personal/master_data_model/model/final_model/create_master_stg_data_model_v3.sql) | Keeps natural source-detail rows while carrying planned metrics safely. |
| [Master evidence model](https://console.cloud.google.com/bigquery?project=looker-studio-pro-452620&p=looker-studio-pro-452620&d=master_stg&t=data_model&page=table) | Package/date compatibility evidence layer. |
| [Master evidence model v3](https://console.cloud.google.com/bigquery?project=looker-studio-pro-452620&p=looker-studio-pro-452620&d=master_stg&t=data_model_v3&page=table) | Current production model at the lowest available source grain. |

Digital conversion outcomes remain documented in [Final model](/Users/eugenetsenter/Looker_clonedRepo/looker_personal/master_data_model/model/final_model/README.md), because their pipeline is assembled directly by the v3 builder rather than by a sibling source folder.
