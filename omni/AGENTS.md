# Omni Agent Rules

## Instruction Sources

- Monorepo rules file:
  - `/Users/eugenetsenter/Looker_clonedRepo/looker_personal/AGENTS.md`
- Local subfolder rules file:
  - `/Users/eugenetsenter/Looker_clonedRepo/looker_personal/omni/AGENTS.md`
- Working rule:
  - Apply monorepo rules first, then apply these Omni-specific rules for work inside this folder.

## Omni Repo Map

- `omni/AGENTS.md` - local Omni operating rules
- Local files in this folder are intentionally minimal right now; treat the live Omni platform as the main working surface unless the user explicitly asks for local-file work.

## Project Working Agreement

This project is Omni-first.

I will do the following in this project:

- Use Omni skills first whenever the task is related to Omni modeling, content, queries, embeds, admin work, or dashboard changes.
- Make edits in the Omni platform unless the user explicitly says to use another method.
- Treat the live Omni platform as the default working surface for this project.
- Do not fall back to local file edits unless the user explicitly asks for that path.

## Omni-First Workflow

When working in this folder:

1. Start with the relevant Omni skill first.
2. Inspect the live Omni asset first.
3. If the Omni asset depends on a live BigQuery table or view, inspect the live warehouse object before making changes.
4. Make the smallest possible change in Omni that solves the requested problem.
5. Verify the result in Omni after the change.

## Source Of Truth

- For Omni work, treat the live Omni platform as the default source of truth.
- For BigQuery-backed objects used by Omni, treat the live warehouse object as the default source of truth for schema and data behavior.
- If the user explicitly asks to work from a local file, say that this is an exception to the Omni-first project rule and then follow the user’s direction.

## Safety Rules

- Do not use deleted local Omni files as a default workflow reference.
- Do not recreate removed local modeling files unless the user explicitly asks for them.
- Keep changes surgical: every changed step should map back to the current request.

## Dashboard Change Default

For Omni dashboard or widget work in this project:

1. Inspect the live dashboard element first.
2. Build and show a before-and-after preview.
3. Call out whether the change is visual, behavioral, or both.
4. Wait for user confirmation before making the live dashboard change.
5. For single-widget edits, use the Omni UI or a scoped widget-level update. Do not replace the full dashboard document payload to change one widget because that can break tile IDs, query mappings, and layout bindings.

## Verification TODOs

- None currently.
