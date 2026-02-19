# Current Work - Workspace (Resume Fast)

Updated: `2026-02-18 18:42:13 EST`
Workspace root: `/Users/eugenetsenter/Looker_clonedRepo/looker_personal`
Directories scanned this run: `13`

## Snapshot Source

- Skill: `current-work-skill` (/Users/eugenetsenter/Looker_clonedRepo/looker_personal/current-work-skill/SKILL.md)
- Automation: `manual-run` (`none`)

## What You Were Working On

1. **ADIF notebook production and docs alignment** - You were updating notebook-first ADIF production flow and syncing local docs to the live notebook behavior. Confidence: high. Next: Run notebook dependency and target-table sanity checks (about 8 minutes).
2. **Offline sheet daily sync pipeline** - You were maintaining the sheet-to-BigQuery sync used for `mft_offline` updates. Confidence: high. Next: Verify scheduled-query status and latest rows (about 7 minutes).
3. **DCM + UTM enrichment hardening** - You were tightening how DCM rows get UTM fields with a constrained fallback for specific campaigns. Confidence: medium. Next: Run a null-UTM health check for scoped campaigns (about 8 minutes).

## What You Should Do Next (In Order)

### Do First (5 Minutes)
Run notebook dependency and target-table sanity checks (estimated 8 minutes)

```bash
cd /Users/eugenetsenter/Looker_clonedRepo/looker_personal/adif
jq -r '.cells[] | select(.cell_type=="markdown") | (.source // []) | join("")' projects/social_layering/build__adif__prisma_expanded_plus_dcm_with_social_tbl.ipynb | head -n 60
rg -n "adif__mainDataTable_notebook|Section 1|Section 2" README.md AGENTS.md projects/social_layering/README.md
```

### 1) ADIF notebook production and docs alignment
What this means: You were updating notebook-first ADIF production flow and syncing local docs to the live notebook behavior.

Entry source: Source: `current-work-skill` skill via `manual-run` automation.

Confidence: high

Why this is prioritized now: Recent ADIF notebook, README, and AGENTS updates indicate active production documentation alignment.

Why it matters: This keeps production runbooks and dependency docs aligned to the real pipeline target table and steps.

Next step now: Run notebook dependency and target-table sanity checks (estimated 8 minutes)

How to do this: Run the command and confirm the key metric changed in the expected direction. Start with [CHANGELOG.md](/Users/eugenetsenter/Looker_clonedRepo/looker_personal/adif/CHANGELOG.md), [SKILL.md](/Users/eugenetsenter/Looker_clonedRepo/looker_personal/adif/skills/sql-change-guard/SKILL.md), [README.md](/Users/eugenetsenter/Looker_clonedRepo/looker_personal/adif/README.md).

```bash
cd /Users/eugenetsenter/Looker_clonedRepo/looker_personal/adif
jq -r '.cells[] | select(.cell_type=="markdown") | (.source // []) | join("")' projects/social_layering/build__adif__prisma_expanded_plus_dcm_with_social_tbl.ipynb | head -n 60
rg -n "adif__mainDataTable_notebook|Section 1|Section 2" README.md AGENTS.md projects/social_layering/README.md
```

<details><summary>Signals - ADIF notebook production and docs alignment</summary>

- `path hits=20`
- `table hits=3`

</details>

<details><summary>Paths - ADIF notebook production and docs alignment</summary>

- 2026-02-18 18:32:18 EST - [/Users/eugenetsenter/Looker_clonedRepo/looker_personal/adif/CHANGELOG.md](/Users/eugenetsenter/Looker_clonedRepo/looker_personal/adif/CHANGELOG.md)
- 2026-02-18 18:31:59 EST - [/Users/eugenetsenter/Looker_clonedRepo/looker_personal/adif/skills/sql-change-guard/SKILL.md](/Users/eugenetsenter/Looker_clonedRepo/looker_personal/adif/skills/sql-change-guard/SKILL.md)
- 2026-02-18 18:31:43 EST - [/Users/eugenetsenter/Looker_clonedRepo/looker_personal/adif/README.md](/Users/eugenetsenter/Looker_clonedRepo/looker_personal/adif/README.md)
- 2026-02-18 18:31:02 EST - [/Users/eugenetsenter/Looker_clonedRepo/looker_personal/adif/projects/tv_digital_pipeline/README - ADIF TV & Digital Data Pipeline.md](/Users/eugenetsenter/Looker_clonedRepo/looker_personal/adif/projects/tv_digital_pipeline/README - ADIF TV & Digital Data Pipeline.md)
- 2026-02-18 18:30:57 EST - [/Users/eugenetsenter/Looker_clonedRepo/looker_personal/adif/projects/updated_fpd_integration/README_Updated_FPD_Integration.md](/Users/eugenetsenter/Looker_clonedRepo/looker_personal/adif/projects/updated_fpd_integration/README_Updated_FPD_Integration.md)
- 2026-02-18 18:30:51 EST - [/Users/eugenetsenter/Looker_clonedRepo/looker_personal/adif/projects/social_layering/README.md](/Users/eugenetsenter/Looker_clonedRepo/looker_personal/adif/projects/social_layering/README.md)
- 2026-02-18 18:29:57 EST - [/Users/eugenetsenter/Looker_clonedRepo/looker_personal/adif/AGENTS.md](/Users/eugenetsenter/Looker_clonedRepo/looker_personal/adif/AGENTS.md)
- 2026-02-18 18:27:45 EST - [/Users/eugenetsenter/Looker_clonedRepo/looker_personal/adif/projects/social_layering/build__adif__prisma_expanded_plus_dcm_with_social_tbl.ipynb](/Users/eugenetsenter/Looker_clonedRepo/looker_personal/adif/projects/social_layering/build__adif__prisma_expanded_plus_dcm_with_social_tbl.ipynb)
- 2026-02-18 18:22:40 EST - [/Users/eugenetsenter/Looker_clonedRepo/looker_personal/adif/projects/social_layering/archive/legacy_scheduled_sql/README.md](/Users/eugenetsenter/Looker_clonedRepo/looker_personal/adif/projects/social_layering/archive/legacy_scheduled_sql/README.md)
- 2026-02-18 16:48:25 EST - [/Users/eugenetsenter/Looker_clonedRepo/looker_personal/adif/projects/social_layering/archive/legacy_scheduled_sql/build__adif__prisma_expanded_plus_dcm_with_social_tbl (1).ipynb](/Users/eugenetsenter/Looker_clonedRepo/looker_personal/adif/projects/social_layering/archive/legacy_scheduled_sql/build__adif__prisma_expanded_plus_dcm_with_social_tbl (1).ipynb)
- 2026-02-18 16:42:50 EST - [/Users/eugenetsenter/Looker_clonedRepo/looker_personal/adif/projects/updated_fpd_integration/sql/stg__adif__updated_fpd_integrated_v2.sql](/Users/eugenetsenter/Looker_clonedRepo/looker_personal/adif/projects/updated_fpd_integration/sql/stg__adif__updated_fpd_integrated_v2.sql)
- 2026-02-18 16:06:40 EST - [/Users/eugenetsenter/Looker_clonedRepo/looker_personal/adif/projects/social_layering/Getting_started_with_BigQuery_DataFrames.ipynb](/Users/eugenetsenter/Looker_clonedRepo/looker_personal/adif/projects/social_layering/Getting_started_with_BigQuery_DataFrames.ipynb)

</details>

<details><summary>Tables - ADIF notebook production and docs alignment</summary>

- `looker-studio-pro-452620.repo_stg.adif__prisma_expanded_plus_dcm_updated_fpd_view`
- `looker-studio-pro-452620.repo_stg.stg__adif__social_crossplatform`
- `looker-studio-pro-452620.repo_int.crossplatform_pacing`

</details>

### 2) Offline sheet daily sync pipeline
What this means: You were maintaining the sheet-to-BigQuery sync used for `mft_offline` updates.

Entry source: Source: `current-work-skill` skill via `manual-run` automation.

Confidence: high

Why this is prioritized now: Recent scheduler and sync SQL files are concentrated in this workflow.

Why it matters: If this schedule breaks, offline reporting can drift from source-sheet truth.

Next step now: Verify scheduled-query status and latest rows (estimated 7 minutes)

How to do this: Run the check, confirm expected status and recent rows, then capture what you saw. Start with [stg__mm__mft_offline_connected_gsheet.sql](/Users/eugenetsenter/Looker_clonedRepo/looker_personal/mft/scripts/sql/stg__mm__mft_offline_connected_gsheet.sql), [mft_offline_update_manual.sql](/Users/eugenetsenter/Looker_clonedRepo/looker_personal/mft/scripts/sql/mft_offline_update_manual.sql), [mft_offline_daily_sheet_sync.sql](/Users/eugenetsenter/Looker_clonedRepo/looker_personal/mft/scripts/sql/mft_offline_daily_sheet_sync.sql).

```bash
bq ls --transfer_config --transfer_location=US --project_id=looker-studio-pro-452620
bq show --transfer_config --transfer_location=US projects/671028410185/locations/us/transferConfigs/699421ab-0000-2129-a27e-883d24f0f1b8
bq head -n 5 looker-studio-pro-452620:mass_mutual_mft_ext.mft_offline
```

<details><summary>Signals - Offline sheet daily sync pipeline</summary>

- `path hits=4`
- `table hits=2`

</details>

<details><summary>Paths - Offline sheet daily sync pipeline</summary>

- 2026-02-17 16:09:33 EST - [/Users/eugenetsenter/Looker_clonedRepo/looker_personal/mft/scripts/sql/stg__mm__mft_offline_connected_gsheet.sql](/Users/eugenetsenter/Looker_clonedRepo/looker_personal/mft/scripts/sql/stg__mm__mft_offline_connected_gsheet.sql)
- 2026-02-17 16:09:33 EST - [/Users/eugenetsenter/Looker_clonedRepo/looker_personal/mft/scripts/sql/mft_offline_update_manual.sql](/Users/eugenetsenter/Looker_clonedRepo/looker_personal/mft/scripts/sql/mft_offline_update_manual.sql)
- 2026-02-17 16:09:33 EST - [/Users/eugenetsenter/Looker_clonedRepo/looker_personal/mft/scripts/sql/mft_offline_daily_sheet_sync.sql](/Users/eugenetsenter/Looker_clonedRepo/looker_personal/mft/scripts/sql/mft_offline_daily_sheet_sync.sql)
- 2026-02-17 16:09:33 EST - [/Users/eugenetsenter/Looker_clonedRepo/looker_personal/mft/scripts/setup-mft-offline-daily-sheet-sync.sh](/Users/eugenetsenter/Looker_clonedRepo/looker_personal/mft/scripts/setup-mft-offline-daily-sheet-sync.sh)

</details>

<details><summary>Tables - Offline sheet daily sync pipeline</summary>

- `looker-studio-pro-452620.mass_mutual_mft_ext.mft_offline`
- `looker-studio-pro-452620.repo_stg.stg__mm__mft_offline_connected_gsheet`

</details>

### 3) DCM + UTM enrichment hardening
What this means: You were tightening how DCM rows get UTM fields with a constrained fallback for specific campaigns.

Entry source: Source: `current-work-skill` skill via `manual-run` automation.

Confidence: medium

Why this is prioritized now: Recent SQL edits and key table hits point to active DCM-UTM alignment work.

Why it matters: This prevents null UTM rows and keeps attribution reporting stable.

Next step now: Run a null-UTM health check for scoped campaigns (estimated 8 minutes)

How to do this: Run the command and confirm the key metric changed in the expected direction. Start with [repo_stg__dcm_plus_utms.sql](/Users/eugenetsenter/Looker_clonedRepo/looker_personal/mft/scripts/sql/repo_stg__dcm_plus_utms.sql).

```bash
cd /Users/eugenetsenter/Looker_clonedRepo/looker_personal/mft
./scripts/bq-safe-query.sh --max-rows 25 --sql "SELECT campaign, COUNT(*) AS rows, COUNTIF(utm_content IS NULL) AS null_utm_content FROM `looker-studio-pro-452620.repo_stg.dcm_plus_utms` WHERE campaign IN ('MassMutual20252026Media','MassMutualLVGP2025') AND date >= '2026-01-01' GROUP BY 1 ORDER BY rows DESC"
```

<details><summary>Signals - DCM + UTM enrichment hardening</summary>

- `path hits=1`
- `table hits=3`

</details>

<details><summary>Paths - DCM + UTM enrichment hardening</summary>

- 2026-02-17 16:09:33 EST - [/Users/eugenetsenter/Looker_clonedRepo/looker_personal/mft/scripts/sql/repo_stg__dcm_plus_utms.sql](/Users/eugenetsenter/Looker_clonedRepo/looker_personal/mft/scripts/sql/repo_stg__dcm_plus_utms.sql)

</details>

<details><summary>Tables - DCM + UTM enrichment hardening</summary>

- `looker-studio-pro-452620.repo_stg.dcm_plus_utms`
- `looker-studio-pro-452620.final_views.dcm`
- `looker-studio-pro-452620.final_views.utms_view`

</details>

## Repo Status

### `Explorations`
- Branch[^3]: `dev...Omni_remote/dev [ahead 5]`
- Working tree[^1]: staged=0, modified=11, untracked=4
- Remote activity[^2]:
  - `Omni_remote` (main) => local HEAD differs from remote HEAD; url=`https://github.com/GiantSpoon-Tech/omni.git`
  - `origin` (main) => local HEAD differs from remote HEAD; url=`https://github.com/genetsen/looker_personal`

### `mft`
- Branch[^3]: `main...origin/main`
- Working tree[^1]: staged=0, modified=0, untracked=0
- Remote activity[^2]:
  - `origin` (main) => local HEAD matches remote HEAD; url=`https://github.com/genetsen/mft.git`

## Open Files Seen From VS Code Context[^4]

- No open-file list found in recent local session logs.

## Current Risks

- No high-risk signal found in this scan.

## Footnotes

[^1]: Working tree means your local file state before commit.
[^2]: Remote means the server-side git copy (for example GitHub).
[^3]: Branch means a named commit line in git.
[^4]: VS Code context here comes from local session log messages that include open tabs.
[^5]: PKM means personal knowledge management notes.
