# MFT Agent Rules

Behavioral guidelines to reduce common LLM coding mistakes. Merge with project-specific instructions as needed.

**Tradeoff:** These guidelines bias toward caution over speed. For trivial tasks, use judgment.

## 1. Think Before Coding

**Don't assume. Don't hide confusion. Surface tradeoffs.**

Before implementing:
- State your assumptions explicitly. If uncertain, ask.
- If multiple interpretations exist, present them - don't pick silently.
- If a simpler approach exists, say so. Push back when warranted.
- If something is unclear, stop. Name what's confusing. Ask.

## 2. Simplicity First

**Minimum code that solves the problem. Nothing speculative.**

- No features beyond what was asked.
- No abstractions for single-use code.
- No "flexibility" or "configurability" that wasn't requested.
- No error handling for impossible scenarios.
- If you write 200 lines and it could be 50, rewrite it.

Ask yourself: "Would a senior engineer say this is overcomplicated?" If yes, simplify.

## 3. Surgical Changes

**Touch only what you must. Clean up only your own mess.**

When editing existing code:
- Don't "improve" adjacent code, comments, or formatting.
- Don't refactor things that aren't broken.
- Match existing style, even if you'd do it differently.
- If you notice unrelated dead code, mention it - don't delete it.

When your changes create orphans:
- Remove imports/variables/functions that YOUR changes made unused.
- Don't remove pre-existing dead code unless asked.

The test: Every changed line should trace directly to the user's request.

## 4. Goal-Driven Execution

**Define success criteria. Loop until verified.**

Transform tasks into verifiable goals:
- "Add validation" → "Write tests for invalid inputs, then make them pass"
- "Fix the bug" → "Write a test that reproduces it, then make it pass"
- "Refactor X" → "Ensure tests pass before and after"

For multi-step tasks, state a brief plan:
```
1. [Step] → verify: [check]
2. [Step] → verify: [check]
3. [Step] → verify: [check]
```

Strong success criteria let you loop independently. Weak criteria ("make it work") require constant clarification.

---

**These guidelines are working if:** fewer unnecessary changes in diffs, fewer rewrites due to overcomplication, and clarifying questions come before implementation rather than after mistakes.
## BigQuery Guardrails
- For BigQuery reads, use `/Users/eugenetsenter/Looker_clonedRepo/looker_personal/mft/scripts/bq-safe-query.sh` by default.
- Do not run raw `bq query` for data pulls unless the user explicitly asks to bypass guardrails.
- Apply the same guardrails when using MCP (`bigquery` server, `run_query` tool):
  - no `SELECT *` by default
  - default row ceiling `LIMIT <= 50`
  - prefer summary-first queries before detailed row pulls
- Prefer `--schema-only` when the user asks for table columns.
- Keep result outputs small by default:
  - `--max-rows 50`
  - `--max-bytes 176000000000` (about `$1` at `$6.25/TiB`)
- When larger output is requested, confirm and increase limits intentionally.
- If a query is blocked by guardrails, provide summary alternatives instead of returning raw rows:
  - row count and date span
  - top categories by volume
  - narrow explicit-column sample (`LIMIT 50`)
  - schema exploration via `INFORMATION_SCHEMA.COLUMNS`

## Changelog Consolidation
- Keep `/Users/eugenetsenter/Looker_clonedRepo/looker_personal/mft/CHANGELOG.md` concise and daily:
  - include only essential high-level outcomes for each day
  - keep formatting simple and preview-stable
  - avoid session-by-session and prompt by prompt implementation noise
- Keep detailed implementation/session history in `/Users/eugenetsenter/Looker_clonedRepo/looker_personal/mft/CHANGELOG_EXTENDED.md`.
- When adding new entries:
  - update both files in the same turn
  - do not remove or rewrite unrelated historical entries
  - update only the thread-related entry unless explicitly asked to refactor older entries
  - in `CHANGELOG.md`, use concise `What` + `Why` for `Added`/`Changed` and focus on outcomes, not process churn
  - in `CHANGELOG_EXTENDED.md`, keep full context and detailed breakdown
  - bold each main item title (for example `**Documentation and changelog standards**`)
  - keep path references in dedicated collapsible blocks:
    - use `<details><summary>Paths — [Title]</summary>` blocks
    - put each path on its own line as a clickable markdown link
    - keep long URLs on their own lines
- For `Fixed` entries in `CHANGELOG.md`:
  - include `Issue` and `Cause`
  - only include `Resolution` after verification
  - if unresolved/unverified, use `Verification: pending` and add/update `## Verification TODOs` below

## Verification TODOs
- None currently.
- Offline daily scheduled query fix was verified with successful transfer updates and successful runs for active schedule `ext_mm_mft_offlineData_sched`.
