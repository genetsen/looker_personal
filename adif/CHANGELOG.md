## 2026-02-18

### Added
- **Persistent workspace runbook for BigQuery notebook-backed assets**
  What: Added a root `AGENTS.md` runbook and README section that document the permanent Dataform workspace and the exact read flow for notebook-backed files (`queryDirectoryContents` and `readFile`).
  Why: Provides a stable, repeatable way to access BigQuery notebook content through the correct Dataform workspace path.

<details><summary>Paths — Persistent workspace runbook for BigQuery notebook-backed assets</summary>

- [AGENTS.md](AGENTS.md)
- [README.md](README.md)

</details>

- **SQL change-guard skill scaffold with runnable validator**
  What: Added a new `skills/sql-change-guard` skill with a BigQuery runner script that validates SQL updates using baseline-vs-candidate checks, lineage discovery, downstream checks, and summary-first output.
  Why: Creates a reusable validation workflow that proves SQL changes are safe before touching live pipelines.

<details><summary>Paths — SQL change-guard skill scaffold with runnable validator</summary>

- [skills/sql-change-guard/SKILL.md](skills/sql-change-guard/SKILL.md)
- [skills/sql-change-guard/scripts/run_sql_change_guard.py](skills/sql-change-guard/scripts/run_sql_change_guard.py)
- [skills/sql-change-guard/agents/openai.yaml](skills/sql-change-guard/agents/openai.yaml)

</details>

- **Editable manifest template for custom checks**
  What: Added `assets/intended_change_manifest.template.json` with key definitions, metric tolerances, custom checks, and downstream check examples.
  Why: Lets reviewers add project-specific validation rules without changing runner code.

<details><summary>Paths — Editable manifest template for custom checks</summary>

- [skills/sql-change-guard/assets/intended_change_manifest.template.json](skills/sql-change-guard/assets/intended_change_manifest.template.json)

</details>

- **Check and report reference docs**
  What: Added check catalog and output-format references that define built-in checks, custom-check schema, and staged approval output behavior.
  Why: Makes review expectations explicit and lowers setup time for repeat validations.

<details><summary>Paths — Check and report reference docs</summary>

- [skills/sql-change-guard/references/check_catalog.md](skills/sql-change-guard/references/check_catalog.md)
- [skills/sql-change-guard/references/report_format.md](skills/sql-change-guard/references/report_format.md)

</details>

### Changed
- **Notebook-aligned documentation refresh**
  What: Updated active runbooks and lineage docs to match the production notebook exactly, including target table (`repo_stg.adif__mainDataTable_notebook`), section flow (rebuild + social insert), dependency graphs, and notebook verification checks.
  Why: Keeps all operational documentation aligned with the live notebook pipeline and removes stale references to retired scheduled-script outputs.

<details><summary>Paths — Notebook-aligned documentation refresh</summary>

- [AGENTS.md](AGENTS.md)
- [README.md](README.md)
- [projects/social_layering/README.md](projects/social_layering/README.md)
- [projects/updated_fpd_integration/README_Updated_FPD_Integration.md](projects/updated_fpd_integration/README_Updated_FPD_Integration.md)
- [projects/tv_digital_pipeline/README%20-%20ADIF%20TV%20%26%20Digital%20Data%20Pipeline.md](projects/tv_digital_pipeline/README%20-%20ADIF%20TV%20%26%20Digital%20Data%20Pipeline.md)
- [skills/sql-change-guard/SKILL.md](skills/sql-change-guard/SKILL.md)

</details>

- **Canonical social production notebook synced to remote**
  What: Replaced local `projects/social_layering/build__adif__prisma_expanded_plus_dcm_with_social_tbl.ipynb` with the current Dataform workspace file to enforce exact parity.
  Why: Keeps local production source aligned with the live notebook-backed pipeline and prevents drift during edits and reviews.

<details><summary>Paths — Canonical social production notebook synced to remote</summary>

- [projects/social_layering/build__adif__prisma_expanded_plus_dcm_with_social_tbl.ipynb](projects/social_layering/build__adif__prisma_expanded_plus_dcm_with_social_tbl.ipynb)

</details>

- **Social production pipeline is now notebook-first**
  What: Promoted `projects/social_layering/build__adif__prisma_expanded_plus_dcm_with_social_tbl.ipynb` as the active production path, moved conflicting scheduled SQL and duplicate notebook files into `projects/social_layering/archive/legacy_scheduled_sql/`, and updated lineage diagrams and runbooks to reflect notebook orchestration.
  Why: Removes ambiguity between script-based and notebook-based production execution and keeps operational docs aligned with current reality.

<details><summary>Paths — Social production pipeline is now notebook-first</summary>

- [projects/social_layering/build__adif__prisma_expanded_plus_dcm_with_social_tbl.ipynb](projects/social_layering/build__adif__prisma_expanded_plus_dcm_with_social_tbl.ipynb)
- [projects/social_layering/archive/legacy_scheduled_sql/build__adif__prisma_expanded_plus_dcm_with_social_tbl.sql](projects/social_layering/archive/legacy_scheduled_sql/build__adif__prisma_expanded_plus_dcm_with_social_tbl.sql)
- [projects/social_layering/archive/legacy_scheduled_sql/query__adif__prisma_expanded_plus_dcm_with_social_tbl_sched.sql](projects/social_layering/archive/legacy_scheduled_sql/query__adif__prisma_expanded_plus_dcm_with_social_tbl_sched.sql)
- [projects/social_layering/archive/legacy_scheduled_sql/build__adif__prisma_expanded_plus_dcm_with_social_tbl%20(1).ipynb](projects/social_layering/archive/legacy_scheduled_sql/build__adif__prisma_expanded_plus_dcm_with_social_tbl%20(1).ipynb)
- [projects/social_layering/archive/legacy_scheduled_sql/README.md](projects/social_layering/archive/legacy_scheduled_sql/README.md)
- [projects/social_layering/README.md](projects/social_layering/README.md)
- [README.md](README.md)
- [AGENTS.md](AGENTS.md)

</details>

- **Social notebook now sets platform-level social source labels**
  What: Updated the social layering notebook insert query to populate `data_source_primary` for social rows as `meta` or `tiktok` from normalized social platform values.
  Why: Makes social-source attribution explicit in appended rows while leaving non-social source labeling unchanged.

<details><summary>Paths — Social notebook now sets platform-level social source labels</summary>

- [projects/social_layering/archive/legacy_scheduled_sql/build__adif__prisma_expanded_plus_dcm_with_social_tbl%20(1).ipynb](projects/social_layering/archive/legacy_scheduled_sql/build__adif__prisma_expanded_plus_dcm_with_social_tbl%20(1).ipynb)
- [projects/social_layering/README.md](projects/social_layering/README.md)

</details>

- **SQL change-guard cutoff now uses current date**
  What: Updated SQL change-guard date-window cutoff logic to always use `CURRENT_DATE() - exclude_recent_days` for baseline and candidate filtering.
  Why: Enforces a fixed operational cutoff that is independent of future-dated or irregular source max dates.

<details><summary>Paths — SQL change-guard cutoff now uses current date</summary>

- [skills/sql-change-guard/scripts/run_sql_change_guard.py](skills/sql-change-guard/scripts/run_sql_change_guard.py)
- [skills/sql-change-guard/SKILL.md](skills/sql-change-guard/SKILL.md)

</details>

- **SQL change-guard now enforces shared date windows with recent-day exclusion**
  What: Updated the SQL change-guard runner to exclude at least the newest 5 days by default when `date_column` is present and to apply one shared baseline/candidate end date for filtered QA tables.
  Why: Ensures apples-to-apples comparisons and avoids validation drift from late-arriving recent data.

<details><summary>Paths — SQL change-guard now enforces shared date windows with recent-day exclusion</summary>

- [skills/sql-change-guard/scripts/run_sql_change_guard.py](skills/sql-change-guard/scripts/run_sql_change_guard.py)
- [skills/sql-change-guard/SKILL.md](skills/sql-change-guard/SKILL.md)
- [README.md](README.md)

</details>

- **Root README now documents SQL change-guard workflow**
  What: Added a `README.md` section with run commands for summary-only and comparison-view modes, plus direct links to skill docs and manifest template.
  Why: Keeps repository runbooks aligned with the new SQL validation capability.

<details><summary>Paths — Root README now documents SQL change-guard workflow</summary>

- [README.md](README.md)
- [skills/sql-change-guard/SKILL.md](skills/sql-change-guard/SKILL.md)
- [skills/sql-change-guard/assets/intended_change_manifest.template.json](skills/sql-change-guard/assets/intended_change_manifest.template.json)

</details>

- **SQL change-guard now prechecks baseline live tables**
  What: Updated the SQL change-guard runner to check for an existing live baseline table first and reuse it by default, added script-input guards for candidate query files, and surfaced baseline-source mode in output.
  Why: Avoids unnecessary heavy baseline query execution and makes validation runs safer and faster when baseline outputs already exist.

<details><summary>Paths — SQL change-guard now prechecks baseline live tables</summary>

- [skills/sql-change-guard/scripts/run_sql_change_guard.py](skills/sql-change-guard/scripts/run_sql_change_guard.py)
- [skills/sql-change-guard/SKILL.md](skills/sql-change-guard/SKILL.md)
- [skills/sql-change-guard/references/check_catalog.md](skills/sql-change-guard/references/check_catalog.md)
- [skills/sql-change-guard/references/report_format.md](skills/sql-change-guard/references/report_format.md)
- [README.md](README.md)

</details>

- **SQL change-guard now supports MCP query backend**
  What: Updated the SQL change-guard runner to support `--query-backend mcp` (default), route SQL execution through MCP, and print query backend in summary output; docs were updated with MCP-first run examples.
  Why: Aligns validation execution with MCP requirements while retaining `bq` fallback when needed.

<details><summary>Paths — SQL change-guard now supports MCP query backend</summary>

- [skills/sql-change-guard/scripts/run_sql_change_guard.py](skills/sql-change-guard/scripts/run_sql_change_guard.py)
- [skills/sql-change-guard/SKILL.md](skills/sql-change-guard/SKILL.md)
- [skills/sql-change-guard/references/check_catalog.md](skills/sql-change-guard/references/check_catalog.md)
- [skills/sql-change-guard/references/report_format.md](skills/sql-change-guard/references/report_format.md)
- [README.md](README.md)

</details>

## 2026-02-17

### Added
- **Folder guide for ADIF workflows**
  What: Added a top-level `README.md` that groups files by workflow purpose and adds quick-start commands.
  Why: Makes the folder easier to navigate and lowers setup time when returning to the project.

<details><summary>Paths — Folder guide for ADIF workflows</summary>

- [README.md](README.md)

</details>

- **Social-layering sub-project README**
  What: Added `projects/social_layering/README.md` with the social-append branch overview and lineage handoff to the final `stg` output table.
  Why: Gives the social sub-project a dedicated runbook entry point aligned to the new folder structure.

<details><summary>Paths — Social-layering sub-project README</summary>

- [projects/social_layering/README.md](projects/social_layering/README.md)

</details>

- **Editable social mapping matrix CSV**
  What: Added `projects/social_layering/social_mapping_matrix_editable.csv` with ad set/ad mapping rules and sample mapped values for direct editing.
  Why: Speeds up iteration on social-to-main dimension mapping by making assumptions explicit in one editable table.

<details><summary>Paths — Editable social mapping matrix CSV</summary>

- [projects/social_layering/social_mapping_matrix_editable.csv](projects/social_layering/social_mapping_matrix_editable.csv)
- [projects/social_layering/README.md](projects/social_layering/README.md)

</details>

- **Social mapping v2 QA test script**
  What: Added `projects/social_layering/sql/test__adif__social_mapping_v2_vs_current.sql` to validate proposed social mapping totals and pacing against raw social inputs and current social output.
  Why: Provides proof checks before changing live social append logic, reducing risk of spend/impression or pacing regressions.

<details><summary>Paths — Social mapping v2 QA test script</summary>

- [projects/social_layering/sql/test__adif__social_mapping_v2_vs_current.sql](projects/social_layering/sql/test__adif__social_mapping_v2_vs_current.sql)
- [projects/social_layering/README.md](projects/social_layering/README.md)

</details>

### Changed
- **Project now tracks local organization updates**
  What: Added a local `CHANGELOG.md` entry for this folder-organization improvement.
  Why: Preserves a dated record of changes so future updates are easier to follow.

<details><summary>Paths — Project now tracks local organization updates</summary>

- [CHANGELOG.md](CHANGELOG.md)

</details>

- **Top-level folder map now includes social QA assets**
  What: Updated root `README.md` Social Layering links to include the editable mapping CSV and new v2 validation SQL.
  Why: Keeps top-level navigation aligned with the latest social workflow artifacts.

<details><summary>Paths — Top-level folder map now includes social QA assets</summary>

- [README.md](README.md)
- [projects/social_layering/social_mapping_matrix_editable.csv](projects/social_layering/social_mapping_matrix_editable.csv)
- [projects/social_layering/sql/test__adif__social_mapping_v2_vs_current.sql](projects/social_layering/sql/test__adif__social_mapping_v2_vs_current.sql)

</details>

- **ADIF reorganized into sub-project folders**
  What: Moved ADIF assets from a flat root layout into `projects/updated_fpd_integration`, `projects/tv_digital_pipeline`, and `projects/social_layering`, and updated local docs/links.
  Why: Improves findability by grouping files around real project workflows instead of file type.

<details><summary>Paths — ADIF reorganized into sub-project folders</summary>

- [README.md](README.md)
- [projects/updated_fpd_integration/DEPLOYMENT_CHECKLIST.md](projects/updated_fpd_integration/DEPLOYMENT_CHECKLIST.md)
- [projects/updated_fpd_integration/README_Updated_FPD_Integration.md](projects/updated_fpd_integration/README_Updated_FPD_Integration.md)
- [projects/updated_fpd_integration/PROJECT_SUMMARY_Updated_FPD_Integration.md](projects/updated_fpd_integration/PROJECT_SUMMARY_Updated_FPD_Integration.md)
- [projects/tv_digital_pipeline/README - ADIF TV & Digital Data Pipeline.md](projects/tv_digital_pipeline/README%20-%20ADIF%20TV%20%26%20Digital%20Data%20Pipeline.md)

</details>

- **BigQuery lineage diagrams added across ADIF readmes**
  What: Updated root and sub-project READMEs with BigQuery MCP-derived pipeline diagrams that trace `raw -> models -> stg.adif__prisma_expanded_plus_dcm_with_social_tbl`.
  Why: Makes the full dependency chain and each sub-project's ownership boundary visible in one place.

<details><summary>Paths — BigQuery lineage diagrams added across ADIF readmes</summary>

- [README.md](README.md)
- [projects/updated_fpd_integration/README_Updated_FPD_Integration.md](projects/updated_fpd_integration/README_Updated_FPD_Integration.md)
- [projects/tv_digital_pipeline/README - ADIF TV & Digital Data Pipeline.md](projects/tv_digital_pipeline/README%20-%20ADIF%20TV%20%26%20Digital%20Data%20Pipeline.md)
- [projects/social_layering/README.md](projects/social_layering/README.md)

</details>

- **Social append now uses ad_set/package and ad/placement mapping**
  What: Updated both social append SQL scripts to map `ad_group` to package fields and `ad` to placement fields, with token-based dimension mapping and ad_set-level pacing allocation to ads.
  Why: Aligns social output grain and dimensions with the approved mapping model while preserving spend/impression totals and improving pacing accuracy.

<details><summary>Paths — Social append now uses ad_set/package and ad/placement mapping</summary>

- [projects/social_layering/sql/build__adif__prisma_expanded_plus_dcm_with_social_tbl.sql](projects/social_layering/sql/build__adif__prisma_expanded_plus_dcm_with_social_tbl.sql)
- [projects/social_layering/sql/query__adif__prisma_expanded_plus_dcm_with_social_tbl_sched.sql](projects/social_layering/sql/query__adif__prisma_expanded_plus_dcm_with_social_tbl_sched.sql)
- [projects/social_layering/README.md](projects/social_layering/README.md)

</details>

## 2026-02-09

### Changed
- **Social-layer build view source alignment**
  What: Updated the social-layer build SQL to use `repo_stg.adif__prisma_expanded_plus_dcm_updated_fpd_view` as the base source.
  Why: Preserved updated-FPD fields in the social-layered output table.

<details><summary>Paths — Social-layer build view source alignment</summary>

- [projects/social_layering/sql/build__adif__prisma_expanded_plus_dcm_with_social_tbl.sql](projects/social_layering/sql/build__adif__prisma_expanded_plus_dcm_with_social_tbl.sql)
- [../CHANGELOG.md](../CHANGELOG.md)

</details>

- **Social-layer scheduled query source alignment**
  What: Updated scheduled social-layer SQL to use the updated-FPD base view for schema cloning and final union output.
  Why: Kept scheduled query behavior consistent with the updated base view.

<details><summary>Paths — Social-layer scheduled query source alignment</summary>

- [projects/social_layering/sql/query__adif__prisma_expanded_plus_dcm_with_social_tbl_sched.sql](projects/social_layering/sql/query__adif__prisma_expanded_plus_dcm_with_social_tbl_sched.sql)
- [../CHANGELOG.md](../CHANGELOG.md)

</details>

## 2026-02-06

### Added
- **ADIF social cross-platform staging SQL**
  What: Added ADIF social layering SQL for cross-platform staging.
  Why: Established the initial ADIF social-layer input for downstream transformations.

<details><summary>Paths — ADIF social cross-platform staging SQL</summary>

- [projects/social_layering/sql/stg__adif__social_crossplatform.sql](projects/social_layering/sql/stg__adif__social_crossplatform.sql)
- [../CHANGELOG.md](../CHANGELOG.md)

</details>

### Changed
- **ADIF social staging filter hardening**
  What: Tightened social staging rules using an `account_name` allowlist and a literal `WP_` campaign-name check.
  Why: Reduced off-target social rows entering ADIF social staging.

<details><summary>Paths — ADIF social staging filter hardening</summary>

- [projects/social_layering/sql/stg__adif__social_crossplatform.sql](projects/social_layering/sql/stg__adif__social_crossplatform.sql)
- [../CHANGELOG.md](../CHANGELOG.md)

</details>
