# dbt And BigQuery Structure

This companion captures the broad direction: use dbt for transformation, tests, docs, and lineage, and organize BigQuery so object names reveal lifecycle and purpose.

## dbt Direction

Local context shows a working dbt sandbox at [fivetran dbt Learning Sandbox](/Users/eugenetsenter/Docs/dbt/fivetran/README.md), with existing table contracts in [Table Contracts](/Users/eugenetsenter/Docs/dbt/TABLE_CONTRACTS.md). Official dbt guidance reviewed through Context7 also supports a layered project structure with staging, intermediate, and marts models plus YAML documentation and tests.

## Recommended dbt Model Layers

| dbt Layer | Simple Job | Example Naming |
|---|---|---|
| Sources | Declare raw BigQuery objects dbt reads. | `source('master_dcm', 'src_dcm_cost_model')` |
| Base | Thin fixes for messy source shape when needed. | `base_dcm__cost_model` |
| Staging | Clean one source into predictable fields. | `stg_dcm__delivery_detail` |
| Mapping config | Approved field and grain mappings. | `seed_source_mappings` or `stg_mapping__source_fields` |
| Intermediate | Apply inference, allocation, metric safety, and source precedence. | `int_master__inferred_metadata`, `int_master__metric_safety` |
| Final fact | Universal final evidence table. | `fct_master__universal_evidence` |
| Marts | Shortcut summaries over the final evidence table. | `mart_master__package_daily`, `mart_master__creative_daily`, `mart_master__dma_daily` |
| Tests | Grain, totals, placeholder, inference, and conflict checks. | `assert_master_universal_no_metric_duplication` |

## Suggested BigQuery Organization

These are proposed zones, not existing objects.

| Zone | Suggested Project / Dataset Pattern | What Lives There | Why It Is Clean |
|---|---|---|---|
| Raw ingested sources | [mdm_raw](https://console.cloud.google.com/bigquery?project=looker-studio-pro-452620&ws=!1m4!1m3!3m2!1slooker-studio-pro-452620!2smdm_raw), plus existing source-owned datasets until migration | Third-party, loader-owned, or copied raw tables | Keeps the original receipt separate from modeled logic. |
| Mapping configuration | [mdm_config](https://console.cloud.google.com/bigquery?project=looker-studio-pro-452620&ws=!1m4!1m3!3m2!1slooker-studio-pro-452620!2smdm_config) | Approved source mappings, placeholder definitions, inference rules, metric rules | Keeps user-configured mapping decisions out of model SQL. |
| dbt staging | [mdm_stg](https://console.cloud.google.com/bigquery?project=looker-studio-pro-452620&ws=!1m4!1m3!3m2!1slooker-studio-pro-452620!2smdm_stg) | dbt staging and base models | Separates source cleaning from shared business logic. |
| dbt intermediate | [mdm_int](https://console.cloud.google.com/bigquery?project=looker-studio-pro-452620&ws=!1m4!1m3!3m2!1slooker-studio-pro-452620!2smdm_int) | Inference, allocation, precedence, metric safety, and universal-detail preparation | Makes business logic inspectable by layer. |
| QA outputs | [mdm_qa](https://console.cloud.google.com/bigquery?project=looker-studio-pro-452620&ws=!1m4!1m3!3m2!1slooker-studio-pro-452620!2smdm_qa) | Validation tables, diff views, migration proof | Prevents QA artifacts from blending into reporting surfaces. |
| Final modeled tables | [mdm_mart](https://console.cloud.google.com/bigquery?project=looker-studio-pro-452620&ws=!1m4!1m3!3m2!1slooker-studio-pro-452620!2smdm_mart) | Universal final evidence table and reporting marts | Gives the new design a clear home before replacement. |
| Published BI views | [mdm_publish](https://console.cloud.google.com/bigquery?project=looker-studio-pro-452620&ws=!1m4!1m3!3m2!1slooker-studio-pro-452620!2smdm_publish) | Dashboard-safe views with governed names | Keeps stable dashboard names separate from build internals. |
| Sandbox | [mdm_sandbox](https://console.cloud.google.com/bigquery?project=looker-studio-pro-452620&ws=!1m4!1m3!3m2!1slooker-studio-pro-452620!2smdm_sandbox) | Short-lived QA builds and prototypes | Gives experiments a clearly temporary home. |

## Optional Cleaner Project Split

If the redesign becomes large enough, use project-level separation instead of only dataset-level separation.

| Environment | Suggested Project Pattern | Purpose |
|---|---|---|
| Development | `looker-master-dev` | Agent/user experiments and dbt development. |
| QA | `looker-master-qa` | Validation, migration comparisons, and stakeholder review. |
| Production | `looker-master-prod` or existing `looker-studio-pro-452620` | Published final tables and marts. |

## Suggested Core Tables

| Object Type | Suggested Name | Grain |
|---|---|---|
| Mapping table | [mdm_config.source_field_mappings](https://console.cloud.google.com/bigquery?project=looker-studio-pro-452620&p=looker-studio-pro-452620&d=mdm_config&t=source_field_mappings&page=table) | One source field mapping per row. |
| Placeholder table | [mdm_config.dimension_placeholders](https://console.cloud.google.com/bigquery?project=looker-studio-pro-452620&p=looker-studio-pro-452620&d=mdm_config&t=dimension_placeholders&page=table) | One placeholder rule per dimension. |
| Inference rules | [mdm_config.metadata_inference_rules](https://console.cloud.google.com/bigquery?project=looker-studio-pro-452620&p=looker-studio-pro-452620&d=mdm_config&t=metadata_inference_rules&page=table) | One inference rule per field/source/grain. |
| Metric rules | [mdm_config.metric_rules](https://console.cloud.google.com/bigquery?project=looker-studio-pro-452620&p=looker-studio-pro-452620&d=mdm_config&t=metric_rules&page=table) | One value-status and summability decision per metric/source/grain. |
| Final evidence | [mdm_mart.fct_master__universal_evidence](https://console.cloud.google.com/bigquery?project=looker-studio-pro-452620&p=looker-studio-pro-452620&d=mdm_mart&t=fct_master__universal_evidence&page=table) | Declared by `univ_row_grain`, source row, and date. |
| Stable BI view | [mdm_publish.master_data_model](https://console.cloud.google.com/bigquery?project=looker-studio-pro-452620&p=looker-studio-pro-452620&d=mdm_publish&t=master_data_model&page=table) | Published, dashboard-safe view over the final evidence table. |
| Package shortcut | [mdm_mart.mart_master__package_daily](https://console.cloud.google.com/bigquery?project=looker-studio-pro-452620&p=looker-studio-pro-452620&d=mdm_mart&t=mart_master__package_daily&page=table) | Package/date. |
| Creative shortcut | [mdm_mart.mart_master__creative_daily](https://console.cloud.google.com/bigquery?project=looker-studio-pro-452620&p=looker-studio-pro-452620&d=mdm_mart&t=mart_master__creative_daily&page=table) | Creative/date. |
| DMA shortcut | [mdm_mart.mart_master__dma_daily](https://console.cloud.google.com/bigquery?project=looker-studio-pro-452620&p=looker-studio-pro-452620&d=mdm_mart&t=mart_master__dma_daily&page=table) | DMA/date. |

## Testing And Documentation Requirements

| Requirement | dbt Expression |
|---|---|
| Source declarations | YAML `sources` for every BigQuery input. |
| Column docs | YAML descriptions for final dimensions, metrics, placeholders, and source flags. |
| Grain tests | Unique or custom tests on source row keys and final grain keys. |
| Metric status checks | Custom tests proving every exposed metric has value status and summability. |
| Additive metric checks | Custom tests comparing source totals to final additive totals at approved grains. |
| Legacy column coverage checks | Custom tests or audit queries proving the candidate includes every live column from [master_stg.data_model](https://console.cloud.google.com/bigquery?project=looker-studio-pro-452620&p=looker-studio-pro-452620&d=master_stg&t=data_model&page=table). |
| Legacy total reconciliation checks | Custom tests or audit queries proving approved candidate totals match [master_stg.data_model](https://console.cloud.google.com/bigquery?project=looker-studio-pro-452620&p=looker-studio-pro-452620&d=master_stg&t=data_model&page=table). |
| Inference checks | Tests proving actual values are not overwritten by inferred values. |
| Placeholder checks | Tests proving required selected dimension fields do not disappear silently. |
| Relationship checks | Tests linking final rows to mapping config and source lineage where practical. |

## Naming Principles

| Principle | Rule |
|---|---|
| Lifecycle first | Names should reveal config, staging, intermediate, final, mart, or QA. |
| Domain second | Names should reveal master model and source/domain. |
| Grain visible | Final and mart objects should reveal package, creative, DMA, or universal evidence shape. |
| Legacy isolation | New dbt objects should not hide inside legacy datasets until replacement is approved. |
