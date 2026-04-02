# AGENTS.md

Global operational notes for this repo.

## Social Production Mode

The production social-layer pipeline is notebook-first:

- Active production notebook: `projects/social_layering/build__adif__prisma_expanded_plus_dcm_with_social_tbl.ipynb`
- Notebook target table: `looker-studio-pro-452620.repo_stg.adif__mainDataTable_notebook`
- Notebook section flow:
  - Section 1: `CREATE OR REPLACE TABLE` from `repo_stg.adif__prisma_expanded_plus_dcm_updated_fpd_view`
  - Section 2: `INSERT INTO` with social mapping from `repo_stg.stg__adif__social_crossplatform` and `repo_int.crossplatform_pacing`
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
