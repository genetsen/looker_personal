# Development Guide

**Date:** 2026-06-15  
**Audience:** Agents and humans making local docs, SQL, loader, or Sheet workflow changes.

## Safe Working Model

| Work Type | Start Here | Proof Needed Before Completion Claim |
|---|---|---|
| Current warehouse behavior | Live BigQuery inspection | Live schema/query results from the relevant object. |
| SQL definition changes | Local SQL file after live/source check | Dry run before deploy; live verification after approved deploy. |
| Manual editor loader changes | [load_manual_package_edits.R](/Users/eugenetsenter/Looker_clonedRepo/looker_personal/master_data_model/manual_package_edits/load_manual_package_edits.R) and tests | Local tests plus live Sheet/raw/daily/model/mart proof if claiming end-to-end behavior. |
| Sheet formatting changes | Read-only live formatting snapshot first | Rendered/pixel or visual proof for layout claims. |
| Documentation changes | Existing README/runbook/model map | Link validation and clear boundary between durable semantics and fluctuating counts. |

## Common Commands

### Manual Editor Tests

```bash
Rscript manual_package_edits/tests/test_loader_choice_logic.R
Rscript manual_package_edits/tests/test_daily_total_proof.R
```

### R Loader Parse Check

```bash
Rscript -e 'invisible(parse("manual_package_edits/load_manual_package_edits.R")); cat("R parse OK\n")'
```

### Manual Editor Loader

```bash
Rscript /Users/eugenetsenter/Looker_clonedRepo/looker_personal/master_data_model/manual_package_edits/load_manual_package_edits.R
```

### BigQuery Dry Run

```bash
bq query --project_id=looker-studio-pro-452620 --use_legacy_sql=false --dry_run \
  < master_data_model/create_master_stg_data_model.sql
```

## Deploy Order

| Step | Script | Notes |
|---|---|---|
| 1 | [create_manual_package_edit_tables.sql](/Users/eugenetsenter/Looker_clonedRepo/looker_personal/master_data_model/create_manual_package_edit_tables.sql) | Creates manual landing schemas when needed. |
| 2 | [create_master_data_model_upstream_tables_sched.sql](/Users/eugenetsenter/Looker_clonedRepo/looker_personal/master_data_model/create_master_data_model_upstream_tables_sched.sql) | Refreshes stored upstream table siblings. |
| 3 | [create_master_stg_data_model.sql](/Users/eugenetsenter/Looker_clonedRepo/looker_personal/master_data_model/create_master_stg_data_model.sql) | Replaces the main evidence view if deployed. |
| 4 | [create_master_stg_data_model_mart.sql](/Users/eugenetsenter/Looker_clonedRepo/looker_personal/master_data_model/create_master_stg_data_model_mart.sql) | Replaces the reporting mart after the evidence model is current. |
| 5 | v2/detail scripts | Refresh only the sibling objects needed for the task. |

## Testing Strategy

| Test | Protects |
|---|---|
| [test_loader_choice_logic.R](/Users/eugenetsenter/Looker_clonedRepo/looker_personal/master_data_model/manual_package_edits/tests/test_loader_choice_logic.R) | Undo/correction/stale-source logic for visible Sheet values. |
| [test_daily_total_proof.R](/Users/eugenetsenter/Looker_clonedRepo/looker_personal/master_data_model/manual_package_edits/tests/test_daily_total_proof.R) | Daily allocation totals and one-cent proof behavior. |
| BigQuery dry run | SQL syntax and static query validity. |
| Live QA queries | Current warehouse behavior, row labels, filters, and manual evidence reflection. |
| Rendered Sheet/browser proof | User-facing formatting, slicer order, visibility, and layout. |

## Development Guardrails

| Guardrail | Practical Meaning |
|---|---|
| Do not treat local SQL as live truth | Inspect BigQuery before current-state claims or production deploy decisions. |
| Do not silently add lower-grain fields to package/date rows | Use detail views, nested structures, or explicit user-approved summaries. |
| Do not run full Sheet formatting rebuilds casually | Use loader for routine refreshes; snapshot and compare formatting before UX edits. |
| Do not call request-refresh end-to-end proof | It can stamp audit cells and send notification, but it does not run the loader or publish BigQuery changes. |
| Do not preserve fluctuating counts in durable docs | Counts belong in run-specific handoffs unless the user asks for a snapshot. |
