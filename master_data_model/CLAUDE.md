# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

> **Read `AGENTS.md` first.** It holds the authoritative operating rules for this project (deployment workflow, missing-field lineage, versioned-object rule, freshness rule, documentation hygiene, Manual Editor formatting preservation). This file summarizes architecture and the rules most likely to trip up a new session; `AGENTS.md` is the source of truth where they conflict.

## What this is

The master data model for marketing analytics: SQL builders that combine planning (PRISMA), delivery (DCM, FPD, social, TV, Amazon), and package metadata into one package/date reporting layer, plus a Google Sheet **Manual Package Editor** and its R loader. BigQuery project: `looker-studio-pro-452620`, primary dataset `master_stg`.

## Canonical entrypoint: `model/`

`model/` is a function-organized re-projection of the older root-level SQL files. **Start work in `model/`; treat root-level `create_*.sql` and `README*.md` as compatibility/history unless the user names them explicitly** (AGENTS "Local Entrypoint Rule").

| Folder | What lives there |
|---|---|
| `model/final_model/` | Current production builder: `create_master_stg_data_model_v3.sql` → `master_stg.data_model_v3` (clustered by `_advertiser`, lowest source grain, creative-capable) |
| `model/stable_base/` | `create_master_stg_data_model.sql` → `master_stg.data_model` (package/date compatibility base that v3 reads from); upstream snapshot refresh |
| `model/reporting_outputs/` | Dashboard-facing marts (`..._mart.sql`), Ritual + Purely Elizabeth (PE) client views |
| `model/branches/` | Per-source lineage guides: `dcm`, `fpd`, `prisma`, `social`, `tv`, `amazon`, `digital` |
| `model/manual_editor/` | **Canonical** Manual Package Editor: loader, table schemas, Apps Script, repair tools, tests, `QA_RUNBOOK.md` |
| `model/mappings/` | Advertiser/client mapping SQL |
| `model/rollups_and_fields/` | Canonical field definitions, precedence, do-not-sum context |
| `model/reference_maps/` | Interactive HTML lineage maps (keep updated on model-shape changes) |
| `model/archive_candidates/` | Deprecated SQL/docs, out of the active path |

`mdm_redesign/` is a separate, production-isolated redesign candidate in sibling `mdm_*` datasets — it does **not** replace `master_stg` objects. See `mdm_redesign/MIGRATION_HANDOFF.md`.

## Deploying a SQL model change

Deployment is guard-gated, not direct-to-prod (AGENTS "Lean BigQuery Deployment Workflow"):

1. Inspect the live object; verify local parity once.
2. Edit the permanent SQL and create **one** isolated QA candidate (sibling `_qa` view/table).
3. Run **SQL Change Guard** as the sole broad pre-deploy comparison — it reads a `.qa.json` manifest (e.g. `model/reporting_outputs/ritual_data_model_v3_schema_expansion.qa.json`) and a read-only baseline query. Confirm proposed keys are truly unique at the live grain before configuring it.
4. Deploy only after the guard passes, then run one focused live check on the changed fields.
5. Update docs (`../CHANGELOG.md`, stable READMEs, `model/reference_maps/*.html`) and delete temp artifacts in one cleanup pass.

### How builders actually run

Every `create_*.sql` file is a `CREATE OR REPLACE VIEW/TABLE` script deployed by piping it into the `bq` CLI against project `looker-studio-pro-452620` (no console clicks, no `DECLARE` of a target — the target dataset/table is written into the DDL itself):

```bash
# Deploy any view or table builder (base, mart, v3, ritual, PE, advertiser mapping)
bq query --project_id=looker-studio-pro-452620 --use_legacy_sql=false \
  < model/final_model/create_master_stg_data_model_v3.sql
```

- The **v3 builder is idempotent and self-guarding**: it opens with a `DECLARE ... INFORMATION_SCHEMA.TABLES` check so `CREATE OR REPLACE TABLE data_model_v3` won't clobber the wrong object type. Safe to re-run.
- For an isolated QA candidate, point the same pattern at the sibling `_qa` file (e.g. `create_master_stg_data_model_v3_cm360_direct_qa.sql` → `data_model_v3_cm360_direct_qa`).

Two objects are **not** deployed by running a local `.sql` — they refresh through BigQuery **scheduled queries (Data Transfer configs)** that must be *triggered*, then polled with `bq show --transfer_run`:

- `master_stg.data_model_clustered_by_advertiser_qa` (clustered copy of `data_model`)
- `repo_int.crossplatform_pacing_tbl` and `landing.tv_combined_tbl` (upstream snapshots; their DDL lives in `model/stable_base/create_master_data_model_upstream_tables_sched.sql` for reference, but the transfer owns the refresh)

### Automated refresh (universal runner)

The scheduled-query trigger + v3 rebuild + all verification is wrapped by the universal runner (symlinked at `model/automation/universal_runner`). The authoritative wrapper is `automation_hub/workloads/ops/bq_trigger/run_master_data_model_clustered_advertiser_refresh.sh`, which: triggers the clustered-advertiser transfer → waits for `SUCCEEDED` → verifies clustering on `_advertiser` and source/target row-count parity → runs the v3 builder via `bq query` → verifies the v3 grain contract (no `package_plan` rows, no duplicate visible grain keys, ≤1 summable planned carrier per package/date). Prefer running the wrapper over hand-rolling `bq` calls when refreshing production, so the same verification runs.

```bash
# Trigger scheduled queries + rebuild & verify data_model_v3 (production refresh)
bash /Users/eugenetsenter/Docs/R_Studio_Projects/universal_cron_runner/automation_hub/workloads/ops/bq_trigger/run_master_data_model_clustered_advertiser_refresh.sh

# Or via the full runner (matches the cron step "Master Data Model Clustered Advertiser Refresh")
Rscript /Users/eugenetsenter/Docs/R_Studio_Projects/universal_cron_runner/universal_script_runner.R
```

**Versioned targets** (`v2`, `v3`): create sibling versioned files/objects; never overwrite the unversioned production object unless told to.

**Dependent-table freshness:** changing `master_stg.data_model` or a feeding base view means refreshing every stored dependent table in the *same session* — currently `master_stg.data_model_clustered_by_advertiser_qa` and `master_stg.data_model_v3`. If you defer, say so and name the stale table. (AGENTS has the refresh owners.)

## Manual Package Editor

The editor lets a media buyer correct dashboard values in a Google Sheet; the R loader turns edits into auditable backend `man_*` fields that take precedence in final reporting fields.

```bash
# Run the loader (production sheet is the default)
Rscript model/manual_editor/load_manual_package_edits.R

# Choice-logic regression test
Rscript manual_package_edits/tests/test_loader_choice_logic.R

# R parse check
Rscript -e 'invisible(parse("model/manual_editor/load_manual_package_edits.R")); cat("R parse OK\n")'
```

- `manual_package_edits/load_manual_package_edits.R` (root) is a thin **shim** that delegates to the canonical 89KB loader in `model/manual_editor/` — edit the canonical one, not the shim.
- **Never** run `setup_manual_package_editor_sheet.mjs` or any formatting rebuild against the live sheet unless the user explicitly asks — user formatting on the live sheet is the source of truth. Routine refreshes use the loader only.
- Debugging a marker/color bug: trace the cell through the full path (visible value → hidden baseline → hidden manual marker → raw manual row → daily manual row → `data_model` → `data_model_mart` → loader comparison source). Baselines are recalculated from raw delivery fields, **not** from manual-affected final `_` fields.
- Full QA queries, filters, and package-trace SQL: `model/manual_editor/QA_RUNBOOK.md`.

## Investigating a missing / blank field

Trace master-model lineage before answering (AGENTS "Missing-Field Lineage Rule"): identify the live source table+column, follow it through CTEs/joins/aggregations to the final projection, and state the exact step where it is renamed, aggregated, null-filled, filtered, or dropped. Use schema/fill-rate checks only as supporting evidence. The `model/branches/*/README_*.md` guides document each source's grain and custom logic.

## Conventions & gotchas

- **Grain matters:** `data_model` is package/date; `data_model_v3` keeps natural source-detail (incl. creative) rows while carrying planned metrics safely. Don't sum planned metrics across detail rows — see `model/rollups_and_fields/`.
- Reference-map HTML files (`model/reference_maps/*.html`) describe durable model shape/lineage — update them on structural changes, not to chase fluctuating row counts.
- `archive/` and `archive_candidates/` are never deleted (global preference); move retired files there instead.
- In docs, prefer short Markdown link labels over raw paths/URLs; show literal `project.dataset.table` names only when needed for copy/paste.
