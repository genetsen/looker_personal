# Rule File Audit Cleanup - 2026-06-26

This side conversation cleaned up duplicated BigQuery, SQL, and data-modeling rules across global and local instruction files.

What changed:

- Added remaining grain, lower-level field, lossy-compromise, durable-documentation, and local-runbook boundary rules to `/Users/eugenetsenter/.codex/BIGQUERY_SQL_DATA_MODELING_RULES.md`.
- Reduced duplicated global rule text in repo-level, master-data-model, ADIF, and MFT `AGENTS.md` files.
- Kept local files focused on project-specific helpers, proof ownership, QA isolation, Dataform notebook access, safe-query guardrails, validation slices, and business semantics.
- Added a pointer in the master-data-model README so operational field semantics defer global warehouse/modeling behavior to the global rule doc.
- Updated maintained changelogs for global `.codex`, `looker_personal`, `master_data_model`, `adif`, and `mft`.

Verification:

- Duplicate-rule scan no longer found the removed local headings or stale phrases.
- `git diff --check` passed for the `looker_personal` repository.
- Direct conflict-marker checks found no merge markers in touched files.
- Direct whitespace scan only found pre-existing trailing spaces in older MFT changelog history, not this change.

Remaining boundary:

- Operational READMEs and runbooks were audited for BigQuery/modeling rule language. They were mostly kept as runnable workflow documentation rather than converted into rule files.
