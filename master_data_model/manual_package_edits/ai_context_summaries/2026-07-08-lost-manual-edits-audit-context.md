# Lost Manual Edits Audit Context - 2026-07-08

The user reported that real human-made Google Sheets edits, including older edits, were lost during Manual Package Editor refresh work.

Current operating boundary:
- Do not run the live loader, sheet setup, formatting rebuilds, or any write path.
- Use read-only evidence from saved BigQuery snapshots, current landing tables, Drive/Sheets metadata, and local generated audit outputs.
- Separate rows that are confirmed missing from current manual state from rows whose human authorship is not provable because audit columns are blank.

Key known artifacts:
- `ai_context_summaries/2026-07-08-lost-manual-edit-candidates.csv` contains 60 rows present as active manual rows in historical raw snapshots but missing from the current active raw table.
- Current BigQuery raw state has 2,434 rows, 15 active valid package rows, and no active rows with `Manual Edit At` / `Manual Edit By`.
- Historical raw snapshots also have zero active rows with populated `Manual Edit At` / `Manual Edit By`, so BigQuery alone cannot prove editor identity or edit timestamp.
- Current production workbook Drive revisions only surfaced July 7-8, 2026 workbook-level revisions; the older workbook surfaced June 26-July 7, 2026 workbook-level revisions.

Next steps:
1. Parse the candidate CSV into a readable grouped list.
2. Re-read current and older workbook tabs read-only with compact CSV output.
3. Compare candidate rows against Sheet-visible active rows and report confirmed missing rows, likely generated artifacts, and unproven authorship boundaries.
