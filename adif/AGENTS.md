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
