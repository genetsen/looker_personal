# AGENTS.md

Operational notes for work inside `/Users/eugenetsenter/Looker_clonedRepo/looker_personal/adif`.

## Instruction Sources

- Monorepo rules file:
  - `/Users/eugenetsenter/Looker_clonedRepo/looker_personal/AGENTS.md`
- Local project rules file:
  - `/Users/eugenetsenter/Looker_clonedRepo/looker_personal/adif/AGENTS.md`
- Working rule:
  - Apply monorepo rules first, then apply these ADIF-specific rules for work inside this folder.

## ADIF Repo Map

- `projects/social_layering/` - social-layer workflow docs, scheduled-query validation assets, and archived notebook copies
- `projects/updated_fpd_integration/` - updated FPD deployment and validation workflow
- `projects/tv_digital_pipeline/` - TV and digital ingestion scripts and docs
- `scripts/use_sandbox_gcloud.sh` - helper path for sandbox-safe `gcloud` and BigQuery/Dataform access when needed

## Social Production Mode

The main live ADIF refresh schedule is the BigQuery scheduled query:

- Transfer config: `projects/671028410185/locations/us/transferConfigs/6a40bbfa-0000-2ee2-a61f-582429bc84e0`
- Display name: `ADIF_FullDataRefresh_2604`
- Schedule: `every 10 hours`
- Verified on: `2026-04-08`
- Current live target table: `looker-studio-pro-452620.repo_stg.adif__mainDataTable_notebook_v2_test`
- Current query shape: single-pass V2 `CREATE OR REPLACE TABLE` SQL that combines digital rows and social rows in one scheduled query; treat this scheduled query as the active builder for table changes

Older notebook copies still exist as historical artifacts, but they are not the active build path for this table.

- Older notebook target table: `looker-studio-pro-452620.repo_stg.adif__mainDataTable_notebook`
- Legacy scheduled SQL and duplicate notebook copies are archived under `projects/social_layering/archive/legacy_scheduled_sql/`

## BigQuery Notebook Access (Dataform-backed)

BigQuery notebooks in this project are exposed as Dataform repositories with `single-file-asset-type=notebook`.

Default access preference for this project:

1. Use BigQuery MCP first for dataset discovery, schema inspection, and read-only BigQuery query work.
2. Keep the local `zsh scripts/use_sandbox_gcloud.sh ...` helper as the repair and advanced access path.
3. Use the helper when authentication must be refreshed or when Dataform notebook file reads need a token for `queryDirectoryContents` / `readFile`.

Permanent workspace for notebook access:

- Project: `looker-studio-pro-452620`
- Location: `us-east1`
- Repository: `acfacedf-9d13-4beb-98d4-34f9a2afdba7`
- Workspace: `adif-bq-notebook-permanent`
- Full workspace name: `projects/looker-studio-pro-452620/locations/us-east1/repositories/acfacedf-9d13-4beb-98d4-34f9a2afdba7/workspaces/adif-bq-notebook-permanent`

Read flow (CLI + Dataform API):

```bash
TOKEN=$(gcloud auth print-access-token)
WS="projects/looker-studio-pro-452620/locations/us-east1/repositories/acfacedf-9d13-4beb-98d4-34f9a2afdba7/workspaces/adif-bq-notebook-permanent"

# List files at root (use empty path; "/" is invalid for this API)
curl -s -G \
  -H "Authorization: Bearer $TOKEN" \
  --data-urlencode "path=" \
  "https://dataform.googleapis.com/v1/${WS}:queryDirectoryContents"

# Read a specific file (replace FILE_PATH)
curl -s -G \
  -H "Authorization: Bearer $TOKEN" \
  --data-urlencode "path=FILE_PATH" \
  "https://dataform.googleapis.com/v1/${WS}:readFile"
```

## Production SQL Verification (Required)

Before approving or deploying any production SQL change, verify live BigQuery state with BigQuery MCP first, or with `bq` through the project helper when MCP is unavailable.

Required checks:
1. Confirm object exists and type is correct (`bq show project:dataset.object`).
2. Confirm live schema (`bq show --schema ...` or `INFORMATION_SCHEMA`).
3. Run a read-only sanity query (row counts/date range/null checks).
4. If live BigQuery and local SQL/docs differ, treat live BigQuery as source of truth, document drift, then update local files.
5. Do not deploy until verification evidence is captured in task notes/PR notes.

When referencing a BigQuery table or view in user-facing outputs, render it as a clickable deep link whenever the client supports one instead of plain text only.

## Validation Logic Defaults

For package-level validation:

- Always aggregate planned metrics from `planned_daily_spend_pk` and `planned_daily_impressions_pk`.
- Do not aggregate `planned_amount`, `planned_impressions`, or similar non-daily planned totals at package level.
- As a naming heuristic, fields containing `pkg` or `pk` are generally not safe to sum across dates unless the field name also includes `daily`.
- Treat non-daily `pkg` / `pk` fields as likely windowed or repeated totals unless verified otherwise in live BigQuery.

For DCM-level validation:

- Always use `d_impressions` as the canonical DCM impressions metric.
- Always use `d_daily_recalculated_cost` as the canonical DCM spend metric.
- Do not substitute other DCM impression or spend columns in validation summaries unless the user explicitly asks for a comparison.

For validation responses:

- Present findings in bite-sized chunks, not one large dump.
- Start with the specific scope and grain being used for that case.
- If a table is too wide, misleading, or mixes incompatible examples, say so explicitly before showing or continuing.
- Prefer package-level examples over campaign-level summaries when the user asks to validate package logic.
