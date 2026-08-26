---
pipeline: Creative Name Mapping V1
source_type: user-maintained Google Sheet
output: looker-studio-pro-452620.master_stg.creative_mapping
linked_source: looker-studio-pro-452620.master_stg.purely_elizabeth_creative_mapping_sheet
output_grain: one row per advertiser and approved creative match key
source_tables: [Purely Elizabeth Creative Mapping Google Sheet]
refresh: manual preview, approved mapping-table load, V3 rebuild, west-copy trigger
loader_script: model/mappings/load_purely_elizabeth_creative_mapping.R
verified: 2026-08-26
verified_against: [live source Sheet, live linked Sheet view, live creative_mapping table, live data_model_v3, live west copy, live PE reporting view]
reviewers:
  - gene <gene.tsenter@giantspoon.com>
---

# Creative Name Mapping V1

This workflow turns approved Purely Elizabeth friendly creative names into reporting labels without changing the
source Sheet or any delivery metric. The final endpoint is the
[Purely Elizabeth reporting view](https://console.cloud.google.com/bigquery?project=looker-studio-pro-452620&p=looker-studio-pro-452620&d=master_ext_west&t=mart_data_model_purelyElizabeth&page=table).

## Table of Contents

- [Terms](#terms)
- [Pipeline and ownership](#pipeline-and-ownership)
- [Live linked Sheet access](#live-linked-sheet-access)
- [Source and mapping contract](#source-and-mapping-contract)
- [Fields consumers should use](#fields-consumers-should-use)
- [Run the manual refresh](#run-the-manual-refresh)
- [Verify the result](#verify-the-result)
- [Known gaps and troubleshooting](#known-gaps-and-troubleshooting)

## Terms

- `creative` scope matches advertiser plus raw creative name.
- `tactic_creative` scope matches advertiser plus initiative plus raw creative name and wins when both scopes match.
- A blank creative is an exact value only for `tactic_creative`; it is never a wildcard.

## Pipeline and ownership

```text
Purely Elizabeth Sheet (read-only)
  -> raw BigQuery external table
  -> clean linked BigQuery view

Purely Elizabeth Sheet (read-only)
  -> preview and validation
  -> master_stg.creative_mapping
  -> master_stg.data_model_v3
  -> master_ext_west.data_model
  -> master_ext_west.mart_data_model_purelyElizabeth
```

| Stage | Owns | Does not own |
|---|---|---|
| [PE source Sheet](https://docs.google.com/spreadsheets/d/15LXvL0_DRY0GBDGN_omx2p5Ast4BCptelE463bvj8Bk/edit?gid=89900805#gid=89900805) | User-entered supplier, initiative, raw creative, and friendly name | The loader must never edit or format it. |
| [Clean linked Sheet view](https://console.cloud.google.com/bigquery?project=looker-studio-pro-452620&p=looker-studio-pro-452620&d=master_stg&t=purely_elizabeth_creative_mapping_sheet&page=table) | Live access to every populated mapping and benchmark row | Mapping validation, typed benchmark logic, or V3 refreshes |
| [Preview-first loader](/Users/eugenetsenter/Looker_clonedRepo/looker_personal/master_data_model/model/mappings/load_purely_elizabeth_creative_mapping.R) | Header checks, active-row filtering, key validation, and optional table replacement | V3 or west refreshes |
| [Mapping table](https://console.cloud.google.com/bigquery?project=looker-studio-pro-452620&p=looker-studio-pro-452620&d=master_stg&t=creative_mapping&page=table) | Canonical active mappings and source lineage | Delivery metrics or advertiser standardization |
| [V3 builder](/Users/eugenetsenter/Looker_clonedRepo/looker_personal/master_data_model/model/final_model/create_master_stg_data_model_v3.sql) | Raw preservation, priority matching, and displayed friendly name | Editing the mapping source |
| Existing west copy | Region-to-region table refresh | Mapping validation or scheduled V1 orchestration |

## Live linked Sheet access

The [external-table builder](/Users/eugenetsenter/Looker_clonedRepo/looker_personal/master_data_model/model/mappings/create_master_stg_purely_elizabeth_creative_mapping_sheet.sql)
creates two objects so future Sheet rows remain visible without exposing hundreds of unused blank grid rows:

| Object | Type | Use |
|---|---|---|
| [Raw Sheet link](https://console.cloud.google.com/bigquery?project=looker-studio-pro-452620&p=looker-studio-pro-452620&d=master_stg&t=purely_elizabeth_creative_mapping_sheet_raw&page=table) | External table | Faithful `Creative Mapping!A:M` connection, including blank Sheet-grid rows. |
| [Clean linked view](https://console.cloud.google.com/bigquery?project=looker-studio-pro-452620&p=looker-studio-pro-452620&d=master_stg&t=purely_elizabeth_creative_mapping_sheet&page=table) | View | Normal analyst surface; removes unused blank grid rows and converts measures and benchmark rates to usable numeric types. |

The raw link keeps all 13 fields as text so source values remain inspectable. The clean view exposes impressions as
`INT64`, spend as `NUMERIC`, and the five benchmark percentage fields as decimal `NUMERIC` rates—for example,
`0.04%` becomes `0.0004`. Text identifiers and the two explicitly marked `DO NOT USE` source columns remain text.
Blank numeric cells and benchmark `#N/A` values become `NULL`; any other malformed populated numeric value causes
the view query to fail instead of silently losing data.

Query users need both BigQuery access and Google Drive access to the source Sheet. A credential can inspect the
table metadata yet fail to read its rows when its OAuth token lacks Drive permission. The established R credential
for `gene.tsenter@giantspoon.com` has both permissions.

Recreate the linked source without editing the Sheet:

```bash
bq query --project_id=looker-studio-pro-452620 --use_legacy_sql=false < /Users/eugenetsenter/Looker_clonedRepo/looker_personal/master_data_model/model/mappings/create_master_stg_purely_elizabeth_creative_mapping_sheet.sql
```

This verification should return at least one populated row; the linked schema should contain 13 fields:

```sql
SELECT COUNT(*) AS source_record_count
FROM `looker-studio-pro-452620.master_stg.purely_elizabeth_creative_mapping_sheet`;
```

## Source and mapping contract

The PE adapter reads columns A:F but uses only the fields below. Rows with a blank friendly name are ignored.
All current PE rows load as `tactic_creative`.

| Sheet field | Mapping-table field | Rule |
|---|---|---|
| Supplier Code | `supplier_code_context` | Review context only; never part of a key. |
| Initiative | `initiative` | Required for `tactic_creative`. |
| Creative Name | `source_creative_name` | May be genuinely blank only for `tactic_creative`. |
| Friendly Creative Name | `mapped_creative_name` | Required for an active mapping. |

Matching is case-insensitive and trims outer whitespace. The loader fails before writing when required values are
invalid or two rows reduce to the same canonical key. It also refuses to replace production with zero mappings
unless the operator separately enables the empty-table override.

## Fields consumers should use

| Field | Meaning |
|---|---|
| `_creative_name` | Reporting label. Uses tactic-specific mapping first, creative-only mapping second, then the raw value. |
| `_creative_name_raw` | Original V3 creative value before friendly-name mapping. Use for source reconciliation and matching. |
| `initiative` / PE view `_tactic` | Tactic portion of a `tactic_creative` key. |
| `_supplier_code` | Context only; changing it does not change the creative match. |

V3 applies the mapping after direct CM360 conversion enrichment. This preserves raw creative keys for conversion
matching and prevents a friendly label from changing delivery or conversion joins.

## Run the manual refresh

Run from the project root with the approved R environment. The first command is read-only and must show the
intended rows plus `PREVIEW ONLY: BigQuery was not changed.`

```bash
/usr/local/bin/Rscript model/mappings/load_purely_elizabeth_creative_mapping.R
```

After reviewing the complete preview, explicitly enable the production replacement:

```bash
PE_CREATIVE_MAPPING_UPLOAD=TRUE /usr/local/bin/Rscript model/mappings/load_purely_elizabeth_creative_mapping.R
```

Then rebuild V3 with the complete multi-statement builder, trigger the existing `master_raw_CopyToWest` transfer,
wait for that exact run to report `SUCCEEDED`, and recreate the PE view from
[its builder](/Users/eugenetsenter/Looker_clonedRepo/looker_personal/master_data_model/model/reporting_outputs/create_master_ext_west_mart_data_model_purely_elizabeth.sql).

## Verify the result

This query must return zero duplicate keys. It should also return at least one active mapping.

```sql
SELECT
  COUNT(*) AS mapping_rows,
  COUNT(*) - COUNT(DISTINCT TO_JSON_STRING(STRUCT(
    LOWER(TRIM(advertiser)),
    match_scope,
    IF(match_scope = 'creative', '<not_applicable>', COALESCE(LOWER(TRIM(initiative)), '<null>')),
    COALESCE(NULLIF(LOWER(TRIM(source_creative_name)), ''), '<null>')
  ))) AS duplicate_keys
FROM `looker-studio-pro-452620.master_stg.creative_mapping`;
```

For the live PE view, every mapped row must match the priority rule and every unmapped row must keep
`_creative_name = _creative_name_raw`. SQL Change Guard owns the predeployment proof that row coverage, identity,
spend, impressions, clicks, video metrics, and planned metrics remain unchanged. A successful loader, SQL dry run,
or schema check alone is not completion proof.

## Known gaps and troubleshooting

- V1 has no scheduled universal-runner step; every refresh remains manual.
- The source adapter is Purely Elizabeth-specific. A standardized multi-advertiser Sheet is deferred.
- Compatibility models and the general reporting mart do not receive `_creative_name_raw` in V1.
- Automated unmapped-creative reporting is deferred; use the raw-versus-display fields for ad hoc review.
- If QUAN stops mapping, confirm `Columbus Circle DOOH` remains the initiative and the raw creative is genuinely
  blank. Do not replace the blank with a wildcard value.
- If row counts grow after a candidate join, stop and inspect duplicate canonical mapping keys before rebuilding
  any production table.
