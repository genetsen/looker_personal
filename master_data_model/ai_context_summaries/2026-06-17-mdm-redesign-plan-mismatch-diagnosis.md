# MDM Redesign Plan Mismatch Diagnosis - 2026-06-17

This context summary captures the active diagnosis after thread compaction. It is a resume note only; it does not change the warehouse model.

## What The User Flagged

The user selected the BMad help path for: "I don't think what was built reflects the plan."

## Working Diagnosis

The built `mdm_redesign` objects partially reflect the plan, but the published output does not currently satisfy the agreed master-final-table contract.

Confirmed live findings:

- `master_stg.data_model` has 197 columns and 166,009 rows.
- `mdm_int.int_universal_compat_view` has 204 columns, preserves all 197 master columns, and matches master totals for spend, impressions, and clicks.
- `mdm_publish.v_master_evidence` has only 37 columns, misses 189 master columns, and returns 4,398,463 rows.
- One example package/date has 25 rows in the master and 15,625 rows in the published view, caused by joining non-unique package/date views together.

## Key Local Evidence

- `mdm_redesign/MIGRATION_HANDOFF.md` claims the redesign is ready for review and says "All 104 baseline columns preserved," which conflicts with the live 197-column master.
- `mdm_redesign/scripts/migration_readiness_report.sql` hardcodes a 165,394-row baseline and incorrectly uses `COUNT(*)` on the candidate rows as schema coverage evidence.
- `mdm_redesign/dbt/mdm/` has dbt folders and README guidance, but no real dbt model files yet.
- `mdm_redesign/scripts/source_mapper.py` can write directly to BigQuery config tables, while the plan expected an interactive/manual review-centered mapper flow.

## Recommended Next Move

Do not approve migration yet. Rework the published final table/view so it is built from the compatibility-preserving universal surface, keeps all current master columns, keeps `univ_` additions, and does not join non-unique package/date views in a way that multiplies rows.
