# Current Work - Workspace (Resume Fast)

Updated: `2026-02-23 09:18:43 EST`
Workspace root: `/Users/eugenetsenter/Looker_clonedRepo/looker_personal`
Directories scanned this run: `2`

## Snapshot Source

- Skill: `current-work-skill` (/Users/eugenetsenter/Looker_clonedRepo/looker_personal/current-work-skill/SKILL.md)
- Automation: `manual-run` (`none`)

## What You Were Working On

1. **Current-work skill accuracy tuning** - You were actively improving how the current-work skill detects true active work from logs and repo signals. Confidence: high. Next: Run a sample generation and verify the top task matches the last 24-hour activity (about 8 minutes).
2. **Offline sheet daily sync pipeline** - You were maintaining the sheet-to-BigQuery sync used for `mft_offline` updates. Confidence: high. Next: Verify scheduled-query status and latest rows (about 7 minutes).
3. **Basis UTM merge and FY26 Q1 backfill** - You were working on Basis UTM union/backfill logic and a patched SQL flow in `/tmp`. Confidence: medium. Next: Validate patched SQL once, then decide whether to move it into repo (about 10 minutes).

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

How to do this: Run the check, confirm expected status and recent rows, then capture what you saw. Start with [README.md](/Users/eugenetsenter/Looker_clonedRepo/looker_personal/mft/README.md), [CHANGELOG.md](/Users/eugenetsenter/Looker_clonedRepo/looker_personal/mft/CHANGELOG.md), [current-work-skill](/Users/eugenetsenter/.codex/automations/current-work-skill).

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

- `path hits=14`
- `log command hits=15 (last 24h)`
- `log message hits=10 (last 24h)`

</details>

<details><summary>Paths - Current-work skill accuracy tuning</summary>

- 2026-02-17 16:09:33 EST - [/Users/eugenetsenter/Looker_clonedRepo/looker_personal/mft/README.md](/Users/eugenetsenter/Looker_clonedRepo/looker_personal/mft/README.md)
- 2026-02-17 16:09:33 EST - [/Users/eugenetsenter/Looker_clonedRepo/looker_personal/mft/CHANGELOG.md](/Users/eugenetsenter/Looker_clonedRepo/looker_personal/mft/CHANGELOG.md)
- [/Users/eugenetsenter/.codex/automations/current-work-skill](/Users/eugenetsenter/.codex/automations/current-work-skill)
- [/Users/eugenetsenter/.codex/automations/current-work-skill/memory.md](/Users/eugenetsenter/.codex/automations/current-work-skill/memory.md)
- [/Users/eugenetsenter/Looker_clonedRepo/looker_personal/current-work-skill/scripts/generate_current_work.py](/Users/eugenetsenter/Looker_clonedRepo/looker_personal/current-work-skill/scripts/generate_current_work.py)
- [/Users/eugenetsenter/Looker_clonedRepo/looker_personal/current-work.md](/Users/eugenetsenter/Looker_clonedRepo/looker_personal/current-work.md)
- [/Users/eugenetsenter/Looker_clonedRepo/looker_personal/current-work-skill/output/current-work-global.md](/Users/eugenetsenter/Looker_clonedRepo/looker_personal/current-work-skill/output/current-work-global.md)
- [/Users/eugenetsenter/Looker_clonedRepo/looker_personal/current-work-skill/output/current-work-workspace.md](/Users/eugenetsenter/Looker_clonedRepo/looker_personal/current-work-skill/output/current-work-workspace.md)
- [/Users/eugenetsenter/Looker_clonedRepo/looker_personal/current-work-skill/scrap](/Users/eugenetsenter/Looker_clonedRepo/looker_personal/current-work-skill/scrap)
- [/Users/eugenetsenter/Looker_clonedRepo/looker_personal/current-work-skill/SKILL.md](/Users/eugenetsenter/Looker_clonedRepo/looker_personal/current-work-skill/SKILL.md)
- [/Users/eugenetsenter/Looker_clonedRepo/looker_personal/current-work-skill/scrap.](/Users/eugenetsenter/Looker_clonedRepo/looker_personal/current-work-skill/scrap.)
- [/Users/eugenetsenter/.codex/current-work.md](/Users/eugenetsenter/.codex/current-work.md)

</details>

<details><summary>Tables - Current-work skill accuracy tuning</summary>

- None detected

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

### 3) Basis UTM merge and FY26 Q1 backfill
What this means: You were working on Basis UTM union/backfill logic and a patched SQL flow in `/tmp`.

Entry source: Source: `current-work-skill` skill via `manual-run` automation.

Confidence: medium

Why this is prioritized now: Open tabs and recent util file edits both point to Basis UTM backfill tasks right now.

Why it matters: This controls whether Basis delivery rows join into UTM layers without gaps or duplicate logic.

Next step now: Validate patched SQL once, then decide whether to move it into repo (estimated 10 minutes)

How to do this: Run the command one time and confirm the output looks sane; then choose whether to keep temporary logic or copy it into the repo file.

```bash
bq query --use_legacy_sql=false < /tmp/utm_updates_patched.sql
```

<details><summary>Signals - Basis UTM merge and FY26 Q1 backfill</summary>

- `git activity in util: staged=0, modified=7, untracked=6`

</details>

<details><summary>Paths - Basis UTM merge and FY26 Q1 backfill</summary>

- None detected

</details>

<details><summary>Tables - Basis UTM merge and FY26 Q1 backfill</summary>

- None detected

</details>

## Repo Status

### `mft`
- Branch[^3]: `main...origin/main`
- Working tree[^1]: staged=0, modified=0, untracked=1
- Remote activity[^2]:
  - `origin` (unknown) => remote HEAD not available; url=`https://github.com/genetsen/mft.git`

### `util`
- Branch[^3]: `dev...Omni_remote/dev [ahead 6]`
- Working tree[^1]: staged=0, modified=7, untracked=6
- Remote activity[^2]:
  - `Omni_remote` (unknown) => remote HEAD not available; url=`https://github.com/GiantSpoon-Tech/omni.git`
  - `origin` (unknown) => remote HEAD not available; url=`https://github.com/genetsen/looker_personal`

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
