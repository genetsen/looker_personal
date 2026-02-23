# Current Work - Workspace (Resume Fast)

Updated: `2026-02-20 15:23:56 EST`
Workspace root: `/Users/eugenetsenter/Looker_clonedRepo/looker_personal`
Directories scanned this run: `3`

## Snapshot Source

- Skill: `current-work-skill` (/Users/eugenetsenter/Looker_clonedRepo/looker_personal/current-work-skill/SKILL.md)
- Automation: `manual-run` (`none`)

## What You Were Working On

1. **Current-work skill accuracy tuning** - You were actively improving how the current-work skill detects true active work from logs and repo signals. Confidence: high. Next: Run a sample generation and verify the top task matches the last 24-hour activity (about 8 minutes).
2. **ADIF notebook production and docs alignment** - You were updating notebook-first ADIF production flow and syncing local docs to the live notebook behavior. Confidence: high. Next: Run notebook dependency and target-table sanity checks (about 8 minutes).
3. **Offline sheet daily sync pipeline** - You were maintaining the sheet-to-BigQuery sync used for `mft_offline` updates. Confidence: high. Next: Verify scheduled-query status and latest rows (about 7 minutes).

## What You Should Do Next (In Order)

### Do First (5 Minutes)
Run a sample generation and verify the top task matches the last 24-hour activity (estimated 8 minutes)

```bash
python /Users/eugenetsenter/Looker_clonedRepo/looker_personal/current-work-skill/scripts/generate_current_work.py \
  --workspace-root /Users/eugenetsenter/Looker_clonedRepo/looker_personal \
  --scan-dirs current-work-skill,mft,util \
  --workspace-out /Users/eugenetsenter/Looker_clonedRepo/looker_personal/current-work.md \
  --global-out /Users/eugenetsenter/.codex/current-work.md \
  --project-out /Users/eugenetsenter/Looker_clonedRepo/looker_personal/current-work-skill/output/current-work-workspace.md \
  --log-window-hours 24
```

### 1) Current-work skill accuracy tuning
What this means: You were actively improving how the current-work skill detects true active work from logs and repo signals.

Entry source: Source: `current-work-skill` skill via `manual-run` automation.

Confidence: high

Why this is prioritized now: Recent terminal activity and messages are centered on current-work-skill scoring improvements.

Why it matters: Better detection prevents false top-priority tasks and makes restart guidance trustworthy.

Next step now: Run a sample generation and verify the top task matches the last 24-hour activity (estimated 8 minutes)

How to do this: Run the check, confirm expected status and recent rows, then capture what you saw. Start with [generate_current_work.py](/Users/eugenetsenter/Looker_clonedRepo/looker_personal/current-work-skill/scripts/generate_current_work.py), [metadata.json](/Users/eugenetsenter/Looker_clonedRepo/looker_personal/current-work-skill/runs/verify-20260220-v2/metadata.json), [pkm-note.md](/Users/eugenetsenter/Looker_clonedRepo/looker_personal/current-work-skill/runs/verify-20260220-v2/pkm-note.md).

```bash
python /Users/eugenetsenter/Looker_clonedRepo/looker_personal/current-work-skill/scripts/generate_current_work.py \
  --workspace-root /Users/eugenetsenter/Looker_clonedRepo/looker_personal \
  --scan-dirs current-work-skill,mft,util \
  --workspace-out /Users/eugenetsenter/Looker_clonedRepo/looker_personal/current-work.md \
  --global-out /Users/eugenetsenter/.codex/current-work.md \
  --project-out /Users/eugenetsenter/Looker_clonedRepo/looker_personal/current-work-skill/output/current-work-workspace.md \
  --log-window-hours 24
```

<details><summary>Signals - Current-work skill accuracy tuning</summary>

- `git activity in current-work-skill: staged=0, modified=7, untracked=4`
- `path hits=72`
- `log command hits=52 (last 24h)`
- `log message hits=18 (last 24h)`

</details>

<details><summary>Paths - Current-work skill accuracy tuning</summary>

- 2026-02-20 15:23:40 EST - [/Users/eugenetsenter/Looker_clonedRepo/looker_personal/current-work-skill/scripts/generate_current_work.py](/Users/eugenetsenter/Looker_clonedRepo/looker_personal/current-work-skill/scripts/generate_current_work.py)
- 2026-02-20 15:23:08 EST - [/Users/eugenetsenter/Looker_clonedRepo/looker_personal/current-work-skill/runs/verify-20260220-v2/metadata.json](/Users/eugenetsenter/Looker_clonedRepo/looker_personal/current-work-skill/runs/verify-20260220-v2/metadata.json)
- 2026-02-20 15:23:08 EST - [/Users/eugenetsenter/Looker_clonedRepo/looker_personal/current-work-skill/runs/verify-20260220-v2/pkm-note.md](/Users/eugenetsenter/Looker_clonedRepo/looker_personal/current-work-skill/runs/verify-20260220-v2/pkm-note.md)
- 2026-02-20 15:23:08 EST - [/Users/eugenetsenter/Looker_clonedRepo/looker_personal/current-work-skill/runs/verify-20260220-v2/global-current-work.md](/Users/eugenetsenter/Looker_clonedRepo/looker_personal/current-work-skill/runs/verify-20260220-v2/global-current-work.md)
- 2026-02-20 15:23:08 EST - [/Users/eugenetsenter/Looker_clonedRepo/looker_personal/current-work-skill/runs/verify-20260220-v2/workspace-current-work.md](/Users/eugenetsenter/Looker_clonedRepo/looker_personal/current-work-skill/runs/verify-20260220-v2/workspace-current-work.md)
- 2026-02-20 15:23:08 EST - [/Users/eugenetsenter/Looker_clonedRepo/looker_personal/current-work-skill/runs/verify-20260220-v2/project-copy.md](/Users/eugenetsenter/Looker_clonedRepo/looker_personal/current-work-skill/runs/verify-20260220-v2/project-copy.md)
- 2026-02-20 15:22:33 EST - [/Users/eugenetsenter/Looker_clonedRepo/looker_personal/current-work-skill/runs/verify-20260220/metadata.json](/Users/eugenetsenter/Looker_clonedRepo/looker_personal/current-work-skill/runs/verify-20260220/metadata.json)
- 2026-02-20 15:22:33 EST - [/Users/eugenetsenter/Looker_clonedRepo/looker_personal/current-work-skill/runs/verify-20260220/pkm-note.md](/Users/eugenetsenter/Looker_clonedRepo/looker_personal/current-work-skill/runs/verify-20260220/pkm-note.md)
- 2026-02-20 15:22:33 EST - [/Users/eugenetsenter/Looker_clonedRepo/looker_personal/current-work-skill/runs/verify-20260220/global-current-work.md](/Users/eugenetsenter/Looker_clonedRepo/looker_personal/current-work-skill/runs/verify-20260220/global-current-work.md)
- 2026-02-20 15:22:33 EST - [/Users/eugenetsenter/Looker_clonedRepo/looker_personal/current-work-skill/runs/verify-20260220/workspace-current-work.md](/Users/eugenetsenter/Looker_clonedRepo/looker_personal/current-work-skill/runs/verify-20260220/workspace-current-work.md)
- 2026-02-20 15:22:33 EST - [/Users/eugenetsenter/Looker_clonedRepo/looker_personal/current-work-skill/runs/verify-20260220/project-copy.md](/Users/eugenetsenter/Looker_clonedRepo/looker_personal/current-work-skill/runs/verify-20260220/project-copy.md)
- 2026-02-20 15:22:17 EST - [/Users/eugenetsenter/Looker_clonedRepo/looker_personal/current-work-skill/CHANGELOG.md](/Users/eugenetsenter/Looker_clonedRepo/looker_personal/current-work-skill/CHANGELOG.md)

</details>

<details><summary>Tables - Current-work skill accuracy tuning</summary>

- None detected

</details>

### 2) ADIF notebook production and docs alignment
What this means: You were updating notebook-first ADIF production flow and syncing local docs to the live notebook behavior.

Entry source: Source: `current-work-skill` skill via `manual-run` automation.

Confidence: high

Why this is prioritized now: Recent ADIF notebook, README, and AGENTS updates indicate active production documentation alignment.

Why it matters: This keeps production runbooks and dependency docs aligned to the real pipeline target table and steps.

Next step now: Run notebook dependency and target-table sanity checks (estimated 8 minutes)

How to do this: Run the command and confirm the key metric changed in the expected direction. Start with [streamlit_ad_reporting](/Users/eugenetsenter/Looker_clonedRepo/looker_personal/adif/streamlit_ad_reporting), [data_loader.py](/Users/eugenetsenter/Looker_clonedRepo/looker_personal/adif/streamlit_ad_reporting/functions/data_loader.py), [requirements.txt](/Users/eugenetsenter/Looker_clonedRepo/looker_personal/adif/streamlit_ad_reporting/requirements.txt).

```bash
cd /Users/eugenetsenter/Looker_clonedRepo/looker_personal/adif
jq -r '.cells[] | select(.cell_type=="markdown") | (.source // []) | join("")' projects/social_layering/build__adif__prisma_expanded_plus_dcm_with_social_tbl.ipynb | head -n 60
rg -n "adif__mainDataTable_notebook|Section 1|Section 2" README.md AGENTS.md projects/social_layering/README.md
```

<details><summary>Signals - ADIF notebook production and docs alignment</summary>

- `path hits=6`

</details>

<details><summary>Paths - ADIF notebook production and docs alignment</summary>

- [/Users/eugenetsenter/Looker_clonedRepo/looker_personal/adif/streamlit_ad_reporting](/Users/eugenetsenter/Looker_clonedRepo/looker_personal/adif/streamlit_ad_reporting)
- [/Users/eugenetsenter/Looker_clonedRepo/looker_personal/adif/streamlit_ad_reporting/functions/data_loader.py](/Users/eugenetsenter/Looker_clonedRepo/looker_personal/adif/streamlit_ad_reporting/functions/data_loader.py)
- [/Users/eugenetsenter/Looker_clonedRepo/looker_personal/adif/streamlit_ad_reporting/requirements.txt](/Users/eugenetsenter/Looker_clonedRepo/looker_personal/adif/streamlit_ad_reporting/requirements.txt)
- [/Users/eugenetsenter/Looker_clonedRepo/looker_personal/adif/streamlit_ad_reporting/README.md](/Users/eugenetsenter/Looker_clonedRepo/looker_personal/adif/streamlit_ad_reporting/README.md)
- [/Users/eugenetsenter/Looker_clonedRepo/looker_personal/adif/streamlit_ad_reporting/CHANGELOG.md](/Users/eugenetsenter/Looker_clonedRepo/looker_personal/adif/streamlit_ad_reporting/CHANGELOG.md)
- [/Users/eugenetsenter/Looker_clonedRepo/looker_personal/adif/streamlit_ad_reporting/.stre](/Users/eugenetsenter/Looker_clonedRepo/looker_personal/adif/streamlit_ad_reporting/.stre)

</details>

<details><summary>Tables - ADIF notebook production and docs alignment</summary>

- None detected

</details>

### 3) Offline sheet daily sync pipeline
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

- `git activity in mft: staged=0, modified=0, untracked=1`
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

## Repo Status

### `current-work-skill`
- Branch[^3]: `dev...Omni_remote/dev [ahead 6]`
- Working tree[^1]: staged=0, modified=7, untracked=4
- Remote activity[^2]:
  - `Omni_remote` (main) => local HEAD differs from remote HEAD; url=`https://github.com/GiantSpoon-Tech/omni.git`
  - `origin` (main) => local HEAD differs from remote HEAD; url=`https://github.com/genetsen/looker_personal`

### `mft`
- Branch[^3]: `main...origin/main`
- Working tree[^1]: staged=0, modified=0, untracked=1
- Remote activity[^2]:
  - `origin` (main) => local HEAD matches remote HEAD; url=`https://github.com/genetsen/mft.git`

## Open Files Seen From VS Code Context[^4]

- No open-file list found in recent local session logs.

## Current Risks

- No high-risk signal found in this scan.

## Sample Scope Note

- This run used a limited directory set, so some workspace areas were intentionally skipped.
- Skipped top-level directories in this sample run:
  - `/Users/eugenetsenter/Looker_clonedRepo/looker_personal/dim_model`
  - `/Users/eugenetsenter/Looker_clonedRepo/looker_personal/Explorations`
  - `/Users/eugenetsenter/Looker_clonedRepo/looker_personal/linear`
  - `/Users/eugenetsenter/Looker_clonedRepo/looker_personal/Prisma`
  - `/Users/eugenetsenter/Looker_clonedRepo/looker_personal/FPD`
  - `/Users/eugenetsenter/Looker_clonedRepo/looker_personal/docs`
  - `/Users/eugenetsenter/Looker_clonedRepo/looker_personal/adif`
  - `/Users/eugenetsenter/Looker_clonedRepo/looker_personal/apollo`
  - `/Users/eugenetsenter/Looker_clonedRepo/looker_personal/omni`
  - `/Users/eugenetsenter/Looker_clonedRepo/looker_personal/olipop`
  - `/Users/eugenetsenter/Looker_clonedRepo/looker_personal/sql`

## Footnotes

[^1]: Working tree means your local file state before commit.
[^2]: Remote means the server-side git copy (for example GitHub).
[^3]: Branch means a named commit line in git.
[^4]: VS Code context here comes from local session log messages that include open tabs.
[^5]: PKM means personal knowledge management notes.
