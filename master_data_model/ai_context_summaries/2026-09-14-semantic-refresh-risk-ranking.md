# Semantic Refresh Open-Risk Ranking — September 14, 2026

## Purpose

This note preserves the context needed to continue the user-facing summary of the September 12 Master Data Model semantic-layer refresh. It does not describe a new warehouse check or a data-model change.

## Reader context

- The user knows what the Master Data Model is but may not remember its implementation details or prior automation runs.
- Future summaries should identify every material update from each run, distinguish source changes from automation-made documentation changes, and remain concise.
- The current request is to rank the eight unresolved September 12 review items by risk and recommend an owner for each.

## Evidence boundary

- The ranking is based on the September 12 refresh evidence, not a fresh September 14 warehouse check.
- No unresolved item currently proves model-wide corruption.
- The strongest current reporting concern is the TikTok hierarchy and pacing gap: 42 delivered records reached the v3 model without planned spend.
- The strongest production-reliability concern is unverified scheduled-checkout and remote-branch provenance.
- The clustered support table has misleading warehouse metadata that could encourage deletion of an actively required table.

## Recommended order

1. Diagnose the TikTok hierarchy and pacing gap.
2. Verify which checkout and commit scheduled production jobs actually use.
3. Correct the clustered support table description after confirming ownership.
4. Resolve Reddit pacing coverage.
5. Assign a durable owner for creative-mapping refreshes.
6. Diagnose Manual Editor timestamp serialization.
7. Classify extreme or missing social-pacing date endpoints.
8. Reconcile stale local documentation after higher-priority behavior decisions settle.

