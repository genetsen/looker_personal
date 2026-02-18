# Subrepo AGENTS Template

Use this file as a starting point for project-level `AGENTS.md` files in nested repos (for example `mft`, `adif`, `apollo`).

## Instruction Sources (Required)

- Monorepo rules file:
  - `/Users/eugenetsenter/Looker_clonedRepo/looker_personal/AGENTS.md`
- Local subrepo rules file:
  - `<absolute_path_to_subrepo>/AGENTS.md`
- Working rule:
  - Apply monorepo rules first, then apply local subrepo rules for project-specific behavior.

## Project Scope

- Project name: `<project_name>`
- Repository path: `<absolute_path_to_subrepo>`
- Purpose: `<what_this_project_owns>`
- Out of scope: `<what_should_stay_in_monorepo>`

## Ownership Rules

- Keep only project/client-specific assets in this subrepo.
- Shared utilities, cross-client SQL, and shared docs stay in monorepo root.
- If a file could be reused across projects, move it to monorepo and reference it.

## Local Workflows

- Primary runbooks:
  - `<path_to_project_readme_or_runbook>`
- Primary scripts:
  - `<path_to_primary_script_or_sql>`
- Verification commands:
  - `<command_1>`
  - `<command_2>`

## Change Safety

- Validate in non-production targets first.
- Share proof (row counts, query output, test result, or diff summary) before live changes.
- Keep rollback notes for any workflow that modifies production-like assets.

## Documentation Rules

- Keep this file aligned with real project workflows.
- Update project `README.md` when behavior changes.
- Add concise entries to project `CHANGELOG.md` for Added/Changed/Fixed/Removed updates.
