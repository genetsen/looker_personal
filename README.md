# looker_personal

SQL- and BigQuery-first analytics workspace for campaign reporting pipelines across ADIF, Olipop, MFT, and shared utility workflows.

## Repository Boundary Model

This workspace uses a monorepo + project subrepo structure.

Default preference for long-term simplicity:
- Use a single repo with folders as the default operating model.
- Treat subrepos as exceptions, not the default.
- Keep project boundaries with folder structure and local instruction files before introducing nested Git repos.

Monorepo (`/Users/eugenetsenter/Looker_clonedRepo/looker_personal`):
- Purpose: data model platform at large.
- Scope: shared SQL, shared R/scripts, warehouse workflows, AI agent/skill assets, notes, and cross-project docs.
- Rule: if content is reusable across clients/projects, keep it here.

Project subrepos (for example `mft`, and future `adif`/`apollo` if promoted):
- Purpose: project- or client-specific implementation and operations.
- Scope: project-only transformations, runbooks, project docs, and project instructions.
- Rule: if content is specific to one project/client, keep it in that project subrepo.

When to allow a subrepo (all should be true):
- The project has an independent release cadence.
- The project needs separate access control or ownership boundaries.
- The project requires separate lifecycle/tooling from the root workspace.

Instruction files policy:
- Keep root-level instructions for monorepo rules (`AGENTS.md`, `CLAUDE.md`).
- Keep separate instruction files per project subrepo for local project behavior.
- Every subrepo `AGENTS.md` should explicitly reference both:
  - monorepo instruction source: `/Users/eugenetsenter/Looker_clonedRepo/looker_personal/AGENTS.md`
  - local subrepo instruction source: `<subrepo_path>/AGENTS.md`
- Use the starter template at `/Users/eugenetsenter/Looker_clonedRepo/looker_personal/docs/SUBREPO_AGENTS_TEMPLATE.md` when creating a new subrepo instruction file.

## Core Workflows

- ADIF TV and digital pipeline (`/Users/eugenetsenter/Looker_clonedRepo/looker_personal/adif`)
- ADIF updated FPD integration (`/Users/eugenetsenter/Looker_clonedRepo/looker_personal/adif`)
- Olipop cross-platform delivery + video joins (`/Users/eugenetsenter/Looker_clonedRepo/looker_personal/sql/marts/olipop`)
- MFT export and mart views (`/Users/eugenetsenter/Looker_clonedRepo/looker_personal/mft`)
- Basis UTM processing utilities (`/Users/eugenetsenter/Looker_clonedRepo/looker_personal/util/basis_utms`) with active scripts in `essential/` and legacy assets in `archive/`

## Cross-Brand Data Flow Diagram (Ingestion -> BigQuery -> dbt -> Dashboards)

- OLI (Olipop), MassMutual (MFT), and ADIF flow diagram:
  `/Users/eugenetsenter/Looker_clonedRepo/looker_personal/docs/DATA_FLOW_DIAGRAMS.md`
- This view maps each brand’s path from source ingestion to BigQuery datasets/tables, dbt/SQL model layer, and final BI dashboards.

## New Capability: ADIF Social Layer From Cross-Platform Raw

- Source table: `looker-studio-pro-452620.repo_stg.stg__olipop__crossplatform_raw_tbl`
- Layer SQL: `/Users/eugenetsenter/Looker_clonedRepo/looker_personal/adif/sql/stg__adif__social_crossplatform.sql`
- Default output view: `looker-studio-pro-452620.repo_stg.stg__adif__social_crossplatform`
- Current inclusion logic: keep rows where `account_name` is one of `ADIF USA`, `A Diamond is Forever - US`, `A Diamond is Forever`, or `De Beers Group`, and `campaign_name` contains literal `WP_`
- Social-layered ADIF table build SQL: `/Users/eugenetsenter/Looker_clonedRepo/looker_personal/adif/sql/build__adif__prisma_expanded_plus_dcm_with_social_tbl.sql`
- Scheduled-query payload SQL: `/Users/eugenetsenter/Looker_clonedRepo/looker_personal/adif/sql/query__adif__prisma_expanded_plus_dcm_with_social_tbl_sched.sql`
- Social-layered table target: `looker-studio-pro-452620.repo_stg.adif__prisma_expanded_plus_dcm_with_social_tbl`
- Base schema source for the layered table: `looker-studio-pro-452620.repo_stg.adif__prisma_expanded_plus_dcm_updated_fpd_view`

## Notes

- The ADIF social layer keeps original grain (daily ad-level rows) and all source metrics.
- If you want a mart/table target, materialize from the staging view to your selected dataset.
- SQL QA guardrail: validate in isolated `_qa` objects first, share proof results, and only then apply live SQL patches after explicit approval.

## Restore Instructions (Dev Cutover)

If you need to restore after the ADIF-to-dev cutover, use the commands below from:
`/Users/eugenetsenter/Looker_clonedRepo/looker_personal`

### 1) Confirm current references

```bash
git show-ref | rg 'refs/heads/dev$|refs/heads/ADIF$|refs/remotes/Omni_remote/dev$|backup/dev-before-adif-cutover-2026-02-13$'
```

### 2) Restore local uncommitted work from the safety stash

```bash
git stash list | head -n 5
git stash pop stash@{0}
```

If you want to keep the stash entry after applying files, use:

```bash
git stash apply stash@{0}
```

### 3) Roll back `dev` to the pre-cutover backup branch (local only)

```bash
git checkout dev
git reset --hard backup/dev-before-adif-cutover-2026-02-13
```

### 4) Roll back `dev` on the remote used by BigQuery Repositories

```bash
git push --force-with-lease Omni_remote dev:dev
```

### 5) Validate rollback state

```bash
git fetch Omni_remote
git rev-parse --short=12 refs/heads/dev refs/remotes/Omni_remote/dev refs/heads/backup/dev-before-adif-cutover-2026-02-13
git rev-list --left-right --count refs/heads/dev...refs/remotes/Omni_remote/dev
```

Expected validation result after rollback:
- The three SHAs are identical for `dev`, `Omni_remote/dev`, and `backup/dev-before-adif-cutover-2026-02-13`.
- Ahead/behind count is `0 0`.

## Git Beginner Cheat Sheet (Daily Safe Use)

Run all commands from:
`/Users/eugenetsenter/Looker_clonedRepo/looker_personal`

### 1) Start of day checks

```bash
git status --short --branch
git branch --list
```

### 2) Sync branch safely

```bash
git fetch --all --prune
git checkout dev
git pull --ff-only Omni_remote dev
```

### 3) Create a small work branch

```bash
git checkout -b work/<short-topic>
```

### 4) Save your changes

```bash
git add -A
git commit -m "short clear message"
```

### 5) Push your branch

```bash
git push -u Omni_remote HEAD
```

### 6) Return to stable branch

```bash
git checkout dev
git pull --ff-only Omni_remote dev
```

### 7) If you are blocked by local files

```bash
git stash push --all -m "temp-save"
git stash list | head -n 5
git stash pop stash@{0}
```

### 8) Emergency rollback pattern

```bash
git checkout dev
git branch backup/dev-before-risk-$(date +%Y-%m-%d)
# perform risky step
# if needed to roll back:
git reset --hard backup/dev-before-risk-$(date +%Y-%m-%d)
git push --force-with-lease Omni_remote dev:dev
```

### 9) Things to avoid

- Do not use `git reset --hard` unless you made a backup branch first.
- Do not delete branches until you confirm they are contained by another branch.
- Do not initialize `.git` inside snapshot/history folders.

## Deferred Task: Safe Main/Dev Simplification

Do this later (not now), before making `main` and `dev` identical:

1. Create a temporary trial branch from `main`.
2. In the trial branch, align content to `dev` (without touching `main`).
3. Run a practical smoke-test checklist for workflows used in this repo (ADIF scripts, key SQL paths, and BigQuery-connected repo behavior).
4. Record pass/fail outcomes in notes.
5. Only after tests pass, promote the same change to `main`.

Goal: reduce the risk of hidden breakage showing up months later.
