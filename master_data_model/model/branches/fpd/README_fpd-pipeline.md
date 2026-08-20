# First-Party Data Pipeline

This guide documents partner-reported first-party delivery (FPD) from partner request through its two landing sources and into the master data model. It follows the source-to-output style in [Master Data Model Pipeline v2](/Users/eugenetsenter/Looker_clonedRepo/looker_personal/master_data_model/README_v2.md).

## Pipeline Overview

```text
Partner request template → partner-supplied delivery
                         ↓
Original FPD landing table + updated FPD daily table
                         ↓
Stable package/date base and FPD evidence fields
                         ↓
Master evidence model / v3 detail rows

Polaris Meta/TikTok CSVs → local QA preview and overlap evidence only
```

## Source Inventory

| Source | Grain | What it supplies | Important boundary |
|---|---|---|---|
| [First-Party Partner Data Collection Template](/Users/eugenetsenter/Looker_clonedRepo/looker_personal/master_data_model/model/branches/fpd/partner-data-collection-template.md) | Partner request and entry rows | Requested package context, date grain, dimensions, and partner-entered metrics. | It is a request builder, not the master-model source table. |
| [Original FPD landing table](https://console.cloud.google.com/bigquery?project=looker-studio-pro-452620&p=looker-studio-pro-452620&d=landing&t=fpd_data_ranged_shortcutsFolder&page=table) | Partner/package/date, with creative evidence | Spend, impressions, clicks, sends, opens, benchmark, factor, creative, source-file lineage. | Original FPD can preserve placement/factor/creative detail in v3. |
| [Updated FPD daily table](https://console.cloud.google.com/bigquery?project=looker-studio-pro-452620&p=looker-studio-pro-452620&d=landing&t=adif_updated_fpd_daily&page=table) | Package/date | Daily spend and impressions, supplier, initiative, and Sheet freshness. | Updated FPD has no creative/detail expansion in v3. |
| [Polaris FPD preview](polaris/README.md) | Platform/date/campaign/ad group/ad; package/cumulative snapshot date for overlap review | Locally normalized Meta and TikTok delivery, approved MIQ package candidates, reconciliation, and cumulative current-FPD snapshot overlap evidence. | QA-only Stage 1 workflow. It preserves Polaris's daily grain and compares cumulative delivery through MIQ's native `date_final` snapshots. It does not publish a landing table, replace legacy FPD, edit a mapping Sheet, or feed the model. |

## Lineage and Precedence

| Stage | Behavior |
|---|---|
| [Stable package/date base SQL](/Users/eugenetsenter/Looker_clonedRepo/looker_personal/master_data_model/model/stable_base/create_master_stg_data_model.sql) | Aggregates original and updated FPD separately, then publishes their consolidated `fpd_*` evidence family. |
| Package/date final metrics | Original and updated spend/impressions are added together. Their nonzero combined values take precedence over DCM spend and impressions. Original FPD clicks take precedence over DCM clicks when present. |
| [Delivery detail v2](https://console.cloud.google.com/bigquery?project=looker-studio-pro-452620&p=looker-studio-pro-452620&d=master_stg&t=data_model_delivery_detail_v2&page=table) | Unions DCM, original FPD, and updated FPD. Original FPD is package/date/placement/creative; updated FPD remains package/date. |
| [Master evidence model v3](https://console.cloud.google.com/bigquery?project=looker-studio-pro-452620&p=looker-studio-pro-452620&d=master_stg&t=data_model_v3&page=table) | Keeps original FPD at package/date/placement/factor/creative grain and updated FPD at package/date grain. It assigns planned metrics to one ranked natural row, not every detail row. |

## Custom Logic

- Both FPD inputs are date-gated from `2025-01-01` and require a package ID.
- Original FPD preserves source file, source URL, content-modified time, creative image, factor, sends, opens, and benchmark evidence; these fields explain the final value rather than replace it.
- Updated FPD contributes only its available daily spend and impressions. It does not invent clicks or creative detail.
- A package/date can contain DCM and FPD evidence at the same time. `qa_data_issues` can surface an `actual_source_conflict`; this is evidence for review, not permission to combine metrics arbitrarily.
- Valid manual delivery edits apply after source assembly. They are a later override path and leave FPD evidence visible for audit.

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

- [Polaris FPD preview](polaris/README.md)
- [DCM delivery pipeline](/Users/eugenetsenter/Looker_clonedRepo/looker_personal/master_data_model/model/branches/dcm/README_dcm-pipeline.md)
- [Prisma planning pipeline](/Users/eugenetsenter/Looker_clonedRepo/looker_personal/master_data_model/model/branches/prisma/README_prisma-pipeline.md)
