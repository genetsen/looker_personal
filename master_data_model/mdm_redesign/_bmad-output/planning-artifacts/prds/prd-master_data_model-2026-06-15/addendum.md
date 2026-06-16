# Addendum: Universal Final Evidence Table Technical Outline

This addendum preserves implementation-level depth that should guide architecture and later stories but would make the PRD too heavy. It reads alongside [prd.md](/Users/eugenetsenter/Looker_clonedRepo/looker_personal/master_data_model/_bmad-output/planning-artifacts/prds/prd-master_data_model-2026-06-15/prd.md) and the [Universal Final Evidence Table SPEC](/Users/eugenetsenter/Looker_clonedRepo/looker_personal/master_data_model/_bmad-output/specs/spec-universal-final-evidence-table/SPEC.md).

## Source Mapper Outline

| Area | Broad Requirement |
|---|---|
| Agent skill | Walks the user through source discovery, mapping proposal, validation, and handoff. |
| HTML widget | Lets the user review and edit mappings for new and existing sources. |
| Configuration | Stores versioned mapping decisions in BigQuery config tables and dbt docs, outside core model SQL. |
| Validation | Runs grain, placeholder, inference, metric-safety, and source-total checks before approval. |
| Deferred choice | Runtime details remain open: local HTML, hosted widget, Codex/BMAD widget, or hybrid. |

## dbt Direction

The redesign should leverage the local dbt learning/refactor context in [Table Contracts](/Users/eugenetsenter/Docs/dbt/TABLE_CONTRACTS.md) and [fivetran README](/Users/eugenetsenter/Docs/dbt/fivetran/README.md). Official dbt guidance reviewed during spec work supports source declarations, staging models, intermediate models, marts, YAML docs, and tests as the main analytics-engineering structure.

| dbt Layer | Purpose | Example Naming |
|---|---|---|
| Sources | Declare raw BigQuery inputs. | `source('master_dcm', 'src_dcm_cost_model')` |
| Base | Thin source-shape fixes where needed. | `base_dcm__cost_model` |
| Staging | Clean one source into predictable fields. | `stg_dcm__delivery_detail` |
| Mapping config | Approved field and grain mapping inputs. | `seed_source_mappings` or `stg_mapping__source_fields` |
| Intermediate | Apply inference, allocation, metric safety, and precedence. | `int_master__inferred_metadata` |
| Final fact | Expose the universal final evidence table. | `fct_master__universal_evidence` |
| Marts | Shortcut summaries over the final table. | `mart_master__package_daily` |
| Tests | Prove grain, totals, placeholders, and inference safety. | `assert_master_universal_no_metric_duplication` |

## Proposed BigQuery Structure

These names are planning suggestions, not deployed objects.

| Dataset | Purpose | Example Contents |
|---|---|---|
| [mdm_raw](https://console.cloud.google.com/bigquery?project=looker-studio-pro-452620&ws=!1m4!1m3!3m2!1slooker-studio-pro-452620!2smdm_raw) | Raw ingested source tables. | DCM, FPD, social, TV, and future sources. Existing raw sources can stay where they are until migration is approved. |
| [mdm_config](https://console.cloud.google.com/bigquery?project=looker-studio-pro-452620&ws=!1m4!1m3!3m2!1slooker-studio-pro-452620!2smdm_config) | Manual and agent-managed mapping tables. | Source mapper rules, field mappings, placeholder rules, inference rules, metric rules. |
| [mdm_stg](https://console.cloud.google.com/bigquery?project=looker-studio-pro-452620&ws=!1m4!1m3!3m2!1slooker-studio-pro-452620!2smdm_stg) | dbt staging models. | Cleaned source-specific fields. |
| [mdm_int](https://console.cloud.google.com/bigquery?project=looker-studio-pro-452620&ws=!1m4!1m3!3m2!1slooker-studio-pro-452620!2smdm_int) | Shared business logic. | Universal detail, inferred metadata, allocation checks, metric safety checks. |
| [mdm_qa](https://console.cloud.google.com/bigquery?project=looker-studio-pro-452620&ws=!1m4!1m3!3m2!1slooker-studio-pro-452620!2smdm_qa) | Validation outputs. | Grain checks, duplicate checks, metric reconciliation, migration diffs. |
| [mdm_mart](https://console.cloud.google.com/bigquery?project=looker-studio-pro-452620&ws=!1m4!1m3!3m2!1slooker-studio-pro-452620!2smdm_mart) | Final modeled tables. | Master final table and reporting marts. |
| [mdm_publish](https://console.cloud.google.com/bigquery?project=looker-studio-pro-452620&ws=!1m4!1m3!3m2!1slooker-studio-pro-452620!2smdm_publish) | Stable BI-facing views. | Dashboard-safe views with governed names. |
| [mdm_sandbox](https://console.cloud.google.com/bigquery?project=looker-studio-pro-452620&ws=!1m4!1m3!3m2!1slooker-studio-pro-452620!2smdm_sandbox) | Temporary experiments. | Short-lived QA builds and prototypes. |

Optional project split, if the redesign grows: development in `looker-master-dev`, validation in `looker-master-qa`, and production in `looker-master-prod` or the existing production project.

## Suggested Core Tables

| Object Type | Suggested Name | Grain |
|---|---|---|
| Mapping table | [mdm_config.source_field_mappings](https://console.cloud.google.com/bigquery?project=looker-studio-pro-452620&p=looker-studio-pro-452620&d=mdm_config&t=source_field_mappings&page=table) | One source-field mapping per row. |
| Placeholder table | [mdm_config.dimension_placeholders](https://console.cloud.google.com/bigquery?project=looker-studio-pro-452620&p=looker-studio-pro-452620&d=mdm_config&t=dimension_placeholders&page=table) | One placeholder rule per dimension. |
| Inference rules | [mdm_config.metadata_inference_rules](https://console.cloud.google.com/bigquery?project=looker-studio-pro-452620&p=looker-studio-pro-452620&d=mdm_config&t=metadata_inference_rules&page=table) | One inference rule per field/source/grain. |
| Metric rules | [mdm_config.metric_rules](https://console.cloud.google.com/bigquery?project=looker-studio-pro-452620&p=looker-studio-pro-452620&d=mdm_config&t=metric_rules&page=table) | One value-status and summability decision per metric/source/grain. |
| Final evidence | [mdm_mart.fct_master__universal_evidence](https://console.cloud.google.com/bigquery?project=looker-studio-pro-452620&p=looker-studio-pro-452620&d=mdm_mart&t=fct_master__universal_evidence&page=table) | Declared by `univ_row_grain`, source row, and date. |
| Stable BI view | [mdm_publish.master_data_model](https://console.cloud.google.com/bigquery?project=looker-studio-pro-452620&p=looker-studio-pro-452620&d=mdm_publish&t=master_data_model&page=table) | Published, dashboard-safe view over the final evidence table. |
| Package shortcut | [mdm_mart.mart_master__package_daily](https://console.cloud.google.com/bigquery?project=looker-studio-pro-452620&p=looker-studio-pro-452620&d=mdm_mart&t=mart_master__package_daily&page=table) | Package/date. |
| Creative shortcut | [mdm_mart.mart_master__creative_daily](https://console.cloud.google.com/bigquery?project=looker-studio-pro-452620&p=looker-studio-pro-452620&d=mdm_mart&t=mart_master__creative_daily&page=table) | Creative/date. |
| DMA shortcut | [mdm_mart.mart_master__dma_daily](https://console.cloud.google.com/bigquery?project=looker-studio-pro-452620&p=looker-studio-pro-452620&d=mdm_mart&t=mart_master__dma_daily&page=table) | DMA/date. |

## Universal Field Naming

New universal fields should use a dedicated prefix so they do not blend into legacy columns or source-specific prefixes such as `dcm_`, `fpd_`, `s_`, `amzn_`, `man_`, and `qa_`.

| Naming Rule | Working Recommendation | Example Fields |
|---|---|---|
| Universal fields need a prefix. | Use `univ_`. | `univ_row_grain`, `univ_source_system`, `univ_source_row_id`, `univ_source_lineage`, `univ_record_date`. |
| Legacy fields keep their existing names. | Do not rename current master-table columns just to fit the new prefix. | Existing `_package_id`, `_date`, `_spend`, and source-specific fields remain available. |

## Metric Rule Shape

Every metric needs two simple labels.

| Label | Example Values | Plain-English Meaning |
|---|---|---|
| Value status | `direct`, `inferred`, `allocated`, `unavailable` | Where did this number come from? |
| Summability | `additive`, `doNotSum`, `blocked` | Can this number be added across rows? |

## Decisions To Lock Next

| Decision | Current Recommendation | Why It Matters |
|---|---|---|
| Placeholder names | Start with `not_available_at_source` for dimensions a source does not provide. Add `unknown_from_source` only when a source has the field but the value is blank or unknown. | Keeps blanks from hiding rows while avoiding fake detail. |
| Metric status field shape | Keep the meaning simple. In a wide final table, this may become per-metric status columns; in a narrow metric table, it can be one `metric_value_status` field. | The warehouse shape should match the final table grain. |
| Universal field prefix | Use `univ_`. | Prevents new universal fields from colliding with legacy or source-specific fields. |
| Final table name | Keep [mdm_mart.fct_master__universal_evidence](https://console.cloud.google.com/bigquery?project=looker-studio-pro-452620&p=looker-studio-pro-452620&d=mdm_mart&t=fct_master__universal_evidence&page=table) as the working dbt-table name and decide the final published BI view name before dashboard migration. | Stable names reduce dashboard churn. |
| Source mapper storage | Store approved rules in [mdm_config](https://console.cloud.google.com/bigquery?project=looker-studio-pro-452620&ws=!1m4!1m3!3m2!1slooker-studio-pro-452620!2smdm_config) and document them in dbt; do not rely on code-only mappings. | Mapping rules need to be editable, reviewable, and testable. |

## Recommended Migration Path

| Step | Goal | Proof Check |
|---|---|---|
| 1. Freeze current behavior | Document current live outputs before changing anything. | Row counts, totals, and known examples match today. |
| 2. Build adapters | Create source translators one at a time. | Each adapter passes schema and fill-rate checks. |
| 3. Build universal detail | Create the shared row shape. | No source rows vanish without an approved reason. |
| 4. Add inferred metadata | Fill only missing metadata. | Inferred flags are visible and totals reconcile. |
| 5. Publish final and views | Expose the master table plus convenience views. | Dashboards can read stable views while analysts can inspect detail. |

## Validation Themes

| Theme | Example Check |
|---|---|
| Source grain | Source row keys are unique at declared grain or duplicate behavior is documented. |
| Legacy column coverage | Pull the current live schema from [master_stg.data_model](https://console.cloud.google.com/bigquery?project=looker-studio-pro-452620&p=looker-studio-pro-452620&d=master_stg&t=data_model&page=table) and prove every current column exists in the new version. |
| Legacy total reconciliation | Pull fresh totals from [master_stg.data_model](https://console.cloud.google.com/bigquery?project=looker-studio-pro-452620&p=looker-studio-pro-452620&d=master_stg&t=data_model&page=table) and prove the new version matches approved legacy metrics. |
| Placeholder safety | Required selected dimensions use placeholders instead of disappearing. |
| Inference safety | Actual and manual metadata are not overwritten by inferred values. |
| Additive metric safety | Source totals match final additive totals at approved grains. |
| doNotSum safety | Repeated package/context totals are not exposed as normal additive fields. |
| Mapping approval | Only validated/approved mapping versions feed final outputs. |

## Legacy Parity Requirement

The redesign can add grain-flexible fields, status fields, and new source-adapter outputs, but it must not make the current master table harder to use.

| Requirement | How To Prove It |
|---|---|
| All current columns included | Compare `INFORMATION_SCHEMA.COLUMNS` for [master_stg.data_model](https://console.cloud.google.com/bigquery?project=looker-studio-pro-452620&p=looker-studio-pro-452620&d=master_stg&t=data_model&page=table) to the candidate version. Missing columns fail validation unless the user explicitly approved the omission or rename. |
| Final totals aligned | Compare fresh totals for approved legacy reporting metrics between [master_stg.data_model](https://console.cloud.google.com/bigquery?project=looker-studio-pro-452620&p=looker-studio-pro-452620&d=master_stg&t=data_model&page=table) and the candidate version. |
| No hidden metric inflation | doNotSum fields remain doNotSum or blocked in the candidate version, especially when lower-grain rows are added. |
| Fresh baseline only | Do not hardcode today's row counts or totals into the design. Re-pull the live baseline at validation time. |
