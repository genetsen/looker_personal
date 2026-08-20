# First-Party Data Pipeline

This guide documents partner-reported first-party delivery (FPD) from partner request through its two landing sources and into the master data model. It follows the source-to-output style in [Master Data Model Pipeline v2](/Users/eugenetsenter/Looker_clonedRepo/looker_personal/master_data_model/README_v2.md).

## Pipeline Overview

```text
MIQ Google Sheet → FPD loader → original/revised FPD landing
MIQ email reports → Polaris → GCS Meta/TikTok feeds
                                      ↓
                   guarded Polaris Email daily landing
                                      ↓
          package/date coverage chooses the ingestion path
                                      ↓
             compatibility base → V3 source-detail rows
```

## Source Inventory

| Source | Grain | What it supplies | Important boundary |
|---|---|---|---|
| [First-Party Partner Data Collection Template](/Users/eugenetsenter/Looker_clonedRepo/looker_personal/master_data_model/model/branches/fpd/partner-data-collection-template.md) | Partner request and entry rows | Requested package context, date grain, dimensions, and partner-entered metrics. | It is a request builder, not the master-model source table. |
| [Original FPD landing table](https://console.cloud.google.com/bigquery?project=looker-studio-pro-452620&p=looker-studio-pro-452620&d=landing&t=fpd_data_ranged_shortcutsFolder&page=table) | Partner/package/date, with creative evidence | Spend, impressions, clicks, sends, opens, benchmark, factor, creative, source-file lineage. | Original FPD can preserve placement/factor/creative detail in v3. |
| [Updated FPD daily table](https://console.cloud.google.com/bigquery?project=looker-studio-pro-452620&p=looker-studio-pro-452620&d=landing&t=adif_updated_fpd_daily&page=table) | Package/date | Daily spend and impressions, supplier, initiative, and Sheet freshness. | Updated FPD has no creative/detail expansion in v3. |
| [Polaris Email package mapping](https://console.cloud.google.com/bigquery?project=looker-studio-pro-452620&p=looker-studio-pro-452620&d=landing&t=polaris_email_package_mapping&page=table) | Feed/platform/campaign/ad group | Central owner for the five approved source-key-to-Prisma-package mappings. | MIQ is the partner; this table chooses packages for the Polaris Email ingestion path. |
| [Polaris Email daily delivery](https://console.cloud.google.com/bigquery?project=looker-studio-pro-452620&p=looker-studio-pro-452620&d=landing&t=polaris_email_delivery_daily&page=table) | Package/date/platform/campaign/ad group/ad | Normalized delivery metrics, raw source values, source object/row, package mapping, and load time. | The guarded loader replaces the full snapshot only after every source and warehouse check passes. |
| [Polaris Email reader and loader](polaris/README.md) | Source detail plus package/cumulative snapshot review | Stage 1 review artifacts and the production manual loader for the approved MIQ connection. | Automation remains a separate next stage. |

## Lineage and Precedence

| Stage | Behavior |
|---|---|
| [Stable package/date base SQL](/Users/eugenetsenter/Looker_clonedRepo/looker_personal/master_data_model/model/stable_base/create_master_stg_data_model.sql) | Derives each Polaris Email package's loaded minimum and maximum date. Inside that range it uses Polaris Email; outside it keeps the existing FPD path. |
| Package/date final metrics | Polaris Email replaces both FPD inputs only inside loaded coverage. Existing FPD precedence over DCM remains unchanged outside coverage. The original FPD landing rows are never deleted. |
| [Delivery detail v2](https://console.cloud.google.com/bigquery?project=looker-studio-pro-452620&p=looker-studio-pro-452620&d=master_stg&t=data_model_delivery_detail_v2&page=table) | Unions DCM, original FPD, and updated FPD. Original FPD is package/date/placement/creative; updated FPD remains package/date. |
| [Master evidence model v3](https://console.cloud.google.com/bigquery?project=looker-studio-pro-452620&p=looker-studio-pro-452620&d=master_stg&t=data_model_v3&page=table) | Adds `polaris_email` detail at package/date/platform/campaign/ad-group/ad grain, retains explicit `polaris_*` lineage, and assigns planned metrics to one ranked natural row. |

## Custom Logic

- Both FPD inputs are date-gated from `2025-01-01` and require a package ID.
- Original FPD preserves source file, source URL, content-modified time, creative image, factor, sends, opens, and benchmark evidence; these fields explain the final value rather than replace it.
- Updated FPD contributes only its available daily spend and impressions. It does not invent clicks or creative detail.
- A package/date can contain DCM and FPD evidence at the same time. `qa_data_issues` can surface an `actual_source_conflict`; this is evidence for review, not permission to combine metrics arbitrarily.
- Valid manual delivery edits apply after source assembly. They are a later override path and leave FPD evidence visible for audit.
- MIQ remains the partner for both ingestion paths. `Polaris Email` and `FPD` describe how the partner delivery reached the model; Facebook, Instagram, and TikTok remain platforms.

## Partner Template Boundary

The [partner template guide](/Users/eugenetsenter/Looker_clonedRepo/looker_personal/master_data_model/model/branches/fpd/partner-data-collection-template.md) owns the request workflow: select packages, configure date grain and extra columns, hand off a copy, and preserve its tab/column contract. It documents known template caveats, including duplicate short package names and typed-table dropdown restrictions. Do not treat template configuration as proof that FPD has loaded into BigQuery.

## Debugging Route

| Question | Start here | Then trace |
|---|---|---|
| Was partner data requested correctly? | [Partner template guide](/Users/eugenetsenter/Looker_clonedRepo/looker_personal/master_data_model/model/branches/fpd/partner-data-collection-template.md) | Partner copy and loader path. |
| Did delivery reach a landing source? | Original or updated [FPD table](https://console.cloud.google.com/bigquery?project=looker-studio-pro-452620&p=looker-studio-pro-452620&d=landing&t=fpd_data_ranged_shortcutsFolder&page=table) | Stable `fpd_*` fields, then final fields. |
| Why does final delivery differ from DCM? | `fpd_*` and `dcm_*` evidence fields | Package/date precedence and any manual override. |
| Where is creative detail? | [Delivery detail v2](https://console.cloud.google.com/bigquery?project=looker-studio-pro-452620&p=looker-studio-pro-452620&d=master_stg&t=data_model_delivery_detail_v2&page=table) or [v3](https://console.cloud.google.com/bigquery?project=looker-studio-pro-452620&p=looker-studio-pro-452620&d=master_stg&t=data_model_v3&page=table) | Original FPD supports it; updated FPD does not. |

## Related Guides

- [MIQ Polaris Email reader and loader](polaris/README.md)
- [Polaris Email V3 MVP plan](polaris-email-v3-mvp-plan.md)
- [DCM delivery pipeline](/Users/eugenetsenter/Looker_clonedRepo/looker_personal/master_data_model/model/branches/dcm/README_dcm-pipeline.md)
- [Prisma planning pipeline](/Users/eugenetsenter/Looker_clonedRepo/looker_personal/master_data_model/model/branches/prisma/README_prisma-pipeline.md)
