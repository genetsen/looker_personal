# master_data_model Redesign — dbt Workspace

## Purpose

This dbt project builds the versioned redesign of the `master_data_model`. It lives entirely in sibling/versioned BigQuery zones (`mdm_*`) and does not touch any `master_stg` production object.

## BigQuery Zone Map

| Zone | Dataset | Purpose | dbt Model Folder |
|------|---------|---------|------------------|
| Raw | `mdm_raw` | Mirrored vendor/ingested source declarations | `models/sources/` |
| Config | `mdm_config` | Versioned mapping configuration tables | `models/mapping_config/` |
| Staging | `mdm_stg` | Cleaned, typed, lightly-joined source data | `models/staging/` |
| Intermediate | `mdm_int` | Business rules, grain bridges, placeholder filling, inference | `models/intermediate/` |
| QA | `mdm_qa` | Parity checks, schema snapshots, reconciliation reports | `models/tests/` |
| Mart | `mdm_mart` | Shortcut marts for common reporting grains | `models/marts/` |
| Publish | `mdm_publish` | Stable BI-facing final output | `models/final_fact/` |
| Sandbox | `mdm_sandbox` | Ad-hoc prototyping and experiments | (not tracked in dbt) |

## Production Safety

- **No existing `master_stg` object is replaced.** This project targets only `mdm_*` datasets.
- The final candidate must pass column-coverage and total-reconciliation checks (see Epic 1 Story 1.3) before migration is considered.
- Approval is required before any production replacement (see Epic 5).

## Key Design Rules

1. Every new universal field uses the `univ_` prefix.
2. Missing dimensions use explicit placeholders (`not_available_at_source`, etc.).
3. Inferred metadata overwrites nothing — it only fills blanks.
4. Every metric exposes both value-source status and summability.
5. Lower-grain rows keep full identity; package-level totals are not exposed as additive metrics on detail rows.

## Related Documents

- [PRD](/Users/eugenetsenter/Looker_clonedRepo/looker_personal/master_data_model/_bmad-output/planning-artifacts/prds/prd-master_data_model-2026-06-15/prd.md)
- [Epics](/Users/eugenetsenter/Looker_clonedRepo/looker_personal/master_data_model/_bmad-output/planning-artifacts/epics.md)
- [SPEC](/Users/eugenetsenter/Looker_clonedRepo/looker_personal/master_data_model/_bmad-output/specs/spec-universal-final-evidence-table/SPEC.md)
