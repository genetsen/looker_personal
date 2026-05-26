# 2026-05-20 Manual Editor Docs Continuation

## Current Goal

Improve the Manual Data Editor documentation so it is easy to scan during QA and clearly names the scripts, tables, filters, sheet controls, validation rules, and troubleshooting paths involved in the workflow.

## Live Checks Already Performed

- Verified the live manual landing tables exist:
  - `looker-studio-pro-452620.landing.master_data_model_manual_package_edits_raw`
  - `looker-studio-pro-452620.landing.master_data_model_manual_package_daily`
- Verified the live output views exist:
  - `looker-studio-pro-452620.master_stg.data_model`
  - `looker-studio-pro-452620.master_stg.data_model_mart`
- Verified `data_model` joins the manual raw and daily tables.
- Verified `data_model_mart` filters out rows whose `qa_row_data_issue_category` contains `low_signal_dcm`.
- Verified live schema includes the relevant `man_*` evidence fields in both `data_model` and `data_model_mart`.

## Important Current Behavior

- Loader file: `manual_package_edits/load_manual_package_edits.R`.
- Universal runner wrapper: `/Users/eugenetsenter/Docs/R_Studio_Projects/universal_cron_runner/automation_hub/workloads/ops/master_manual_package_edits/load_master_manual_package_edits.R`.
- Request checkbox Apps Script: `manual_package_edits/apps_script/Code.js`.
- Formatting rebuild script: `manual_package_edits/setup_manual_package_editor_sheet.mjs`; do not run it against the live sheet unless the user explicitly asks for a full formatting rebuild.
- The loader compares sheet values to source-derived baselines:
  - planned values from PRISMA package totals
  - delivered values from recalculated raw delivery fields
  - not manual-affected final dashboard fields
- Metadata edits apply package-wide. Metric edits apply only to the delivery override date range.

## Files Being Updated

- `manual_package_edits/QA_RUNBOOK.md` will be the detailed QA runbook.
- `manual_package_edits/README.md` will stay the everyday workflow guide and link to the runbook.
- `README.md` will link to the detailed runbook from the quick reference.
- `../CHANGELOG.md` will record the documentation update.
