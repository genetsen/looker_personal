# Current Work - Workspace (Resume Fast)

Updated: `2026-02-18 13:12:13 EST`
Workspace root: `/Users/eugenetsenter/Looker_clonedRepo/looker_personal`
Directories scanned this run: `2`

## Snapshot Source

- Skill: `current-work` (unknown)
- Automation: `unknown` (`unknown`)

## Headlines

1. **Offline sheet daily sync pipeline** - You were maintaining the sheet-to-BigQuery sync used for `mft_offline` updates. Confidence: high. Next: Verify scheduled-query status and latest rows (about 7 minutes).
2. **DCM + UTM enrichment hardening** - You were tightening how DCM rows get UTM fields with a constrained fallback for specific campaigns. Confidence: medium. Next: Run a null-UTM health check for scoped campaigns (about 8 minutes).

## Details

### Do First (5 Minutes)
Verify scheduled-query status and latest rows (estimated 7 minutes)

```bash
bq ls --transfer_config --transfer_location=US --project_id=looker-studio-pro-452620
bq show --transfer_config --transfer_location=US projects/671028410185/locations/us/transferConfigs/699421ab-0000-2129-a27e-883d24f0f1b8
bq head -n 5 looker-studio-pro-452620:mass_mutual_mft_ext.mft_offline
```

### 1) Offline sheet daily sync pipeline
What this means: You were maintaining the sheet-to-BigQuery sync used for `mft_offline` updates.

Entry source: Source: `current-work` skill via `unknown` automation.

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

### 2) DCM + UTM enrichment hardening
What this means: You were tightening how DCM rows get UTM fields with a constrained fallback for specific campaigns.

Entry source: Source: `current-work` skill via `unknown` automation.

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

### `mft`
- Branch[^3]: `main...origin/main`
- Working tree[^1]: staged=0, modified=0, untracked=0
- Remote activity[^2]:
  - `origin` (unknown) => remote HEAD not available; url=`https://github.com/genetsen/mft.git`

### `util`
- Branch[^3]: `dev...Omni_remote/dev [ahead 4]`
- Working tree[^1]: staged=0, modified=32, untracked=5
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
