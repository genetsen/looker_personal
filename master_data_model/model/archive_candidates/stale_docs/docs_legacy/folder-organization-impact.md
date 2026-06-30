# Folder Organization Impact

This note records which parts of the master data model folder can be organized safely, which parts are wired into local or external workflows, and what proof is needed before moving them. Use it before renaming files or moving folders.

## Current Safe Cleanup

| Area | Current location | Cleanup action | Breakage risk | Why |
|---|---|---|---|---|
| Audit reference docs | [Audit docs](/Users/eugenetsenter/Looker_clonedRepo/looker_personal/master_data_model/docs/audits) | Moved from the project root into `docs/audits/` | Low | These are Markdown reference files. The known project navigation links were updated. |
| Redesign report render files | [Redesign report output](/Users/eugenetsenter/Looker_clonedRepo/looker_personal/master_data_model/output/redesign-report-2026-06-16) | Grouped the PDF and page PNGs under one ignored local output folder | Low | These files are local render artifacts in ignored `output/` and `tmp/` areas. No active script reference was found for the old exact PDF/page paths. |

## Move Impact Matrix

| Area | Recommendation | Risk | Before moving, verify | Notes |
|---|---|---|---|---|
| Root SQL builders | Keep in place | High | Search project docs, universal runner scripts, and deployment commands for the exact filename | Files such as [create_master_stg_data_model.sql](/Users/eugenetsenter/Looker_clonedRepo/looker_personal/master_data_model/create_master_stg_data_model.sql) and [create_master_stg_data_model_v3.sql](/Users/eugenetsenter/Looker_clonedRepo/looker_personal/master_data_model/create_master_stg_data_model_v3.sql) are named directly by docs and runner workflows. |
| Manual Package Editor | Keep in place | High | Check Apps Script, universal runner entrypoints, loader helpers, and live Sheet workflow docs | [manual_package_edits](/Users/eugenetsenter/Looker_clonedRepo/looker_personal/master_data_model/manual_package_edits) contains the loader, repair scripts, Apps Script source, tests, and live workflow docs. |
| Interactive docs maps | Keep stable filenames | Medium | Search README, docs index, changelog, and map cross-links | Files such as [master-data-model-map.html](/Users/eugenetsenter/Looker_clonedRepo/looker_personal/master_data_model/docs/master-data-model-map.html) are documentation, but they are linked from several stable navigation surfaces. |
| Script-backed presentation outputs | Leave in current `outputs/` paths unless intentionally refactoring | Medium | Search for absolute and relative `outputs/...` paths inside the scripts | [manual-data-editor-intro](/Users/eugenetsenter/Looker_clonedRepo/looker_personal/master_data_model/outputs/manual-data-editor-intro) has scripts that write to its current folder and a Google Slides import helper with an absolute path. |
| BMAD planning output | Leave in place or archive as a planned project | Medium | Search for `_bmad-output` absolute paths and PRD/spec links | Redesign docs and readiness reports point to BMAD artifact paths. Moving them would require link updates across planning docs. |
| Ignored local output | Safe to organize inside ignored folders | Low | Search for exact filenames before moving | `output/`, `tmp/`, `exports/`, and CSV files are ignored by the parent repo. Keep them local unless the artifact should become durable project documentation. |
| Amazon report CSV snapshots | Leave ignored or move only within ignored storage | Low to Medium | Confirm whether the CSV is a disposable loader artifact or evidence for a specific proof | [amazon_reports](/Users/eugenetsenter/Looker_clonedRepo/looker_personal/master_data_model/amazon_reports) is ignored because CSVs are not tracked by default. |
| Context summaries | Keep in [ai_context_summaries](/Users/eugenetsenter/Looker_clonedRepo/looker_personal/master_data_model/ai_context_summaries) | Low | Search by filename if moving individual summaries | This folder is the project convention for compacted-context handoffs. |

## Practical Rule

If a file can run, deploy, refresh, repair, load, or import something, treat its path as workflow-critical until proven otherwise. If a file is only a Markdown or rendered reference artifact, it is usually safe to move after a targeted link search and index update.
