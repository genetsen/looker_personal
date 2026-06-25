# Manual Edit Audit Rollback Bundle

Created: 2026-06-23

Purpose: preserve the pre-deployment state for the Manual Data Editor audit-column change before any live Sheet, Apps Script, BigQuery table, or BigQuery view deployment.

## Saved Artifacts

| Surface | Rollback artifact | Verification |
|---|---|---|
| Local source changes | [manual_edit_audit_local_changes.patch](/Users/eugenetsenter/Looker_clonedRepo/looker_personal/master_data_model/manual_package_edits/rollback_bundles/manual_edit_audit_2026-06-23/manual_edit_audit_local_changes.patch) | Non-empty patch saved from `git diff --binary` |
| Master model view | [restore_master_stg_data_model.sql](/Users/eugenetsenter/Looker_clonedRepo/looker_personal/master_data_model/manual_package_edits/rollback_bundles/manual_edit_audit_2026-06-23/restore_master_stg_data_model.sql) | BigQuery dry run passed |
| Reporting mart view | [restore_master_stg_data_model_mart.sql](/Users/eugenetsenter/Looker_clonedRepo/looker_personal/master_data_model/manual_package_edits/rollback_bundles/manual_edit_audit_2026-06-23/restore_master_stg_data_model_mart.sql) | BigQuery dry run passed |
| View-definition JSON | [current_master_stg_view_definitions.json](/Users/eugenetsenter/Looker_clonedRepo/looker_personal/master_data_model/manual_package_edits/rollback_bundles/manual_edit_audit_2026-06-23/current_master_stg_view_definitions.json) | Contains current `data_model` and `data_model_mart` definitions |
| Apps Script live source | [live_apps_script_snapshot](/Users/eugenetsenter/Looker_clonedRepo/looker_personal/master_data_model/manual_package_edits/rollback_bundles/manual_edit_audit_2026-06-23/live_apps_script_snapshot) | Pulled with `clasp pull`; contains `Code.js` and `appsscript.json` |
| Google Sheet file | [Manual Data Editor rollback copy - before manual edit audit - 2026-06-23](https://docs.google.com/spreadsheets/d/1cfgRiVWRPAUfiQclz-FGO2kBm4gkW_vq14LyIJSQ4N8/edit) | Metadata checked: `Package Editor`, `Instructions`, and `Sheet3` tabs present |

## Rollback Commands

Reverse local source changes:

```bash
git apply -R rollback_bundles/manual_edit_audit_2026-06-23/manual_edit_audit_local_changes.patch
```

Restore the pre-change BigQuery master view:

```bash
bq query --project_id=looker-studio-pro-452620 --use_legacy_sql=false < rollback_bundles/manual_edit_audit_2026-06-23/restore_master_stg_data_model.sql
```

Restore the pre-change BigQuery reporting mart view:

```bash
bq query --project_id=looker-studio-pro-452620 --use_legacy_sql=false < rollback_bundles/manual_edit_audit_2026-06-23/restore_master_stg_data_model_mart.sql
```

Restore the pre-change Apps Script source after a live script deploy:

```bash
cd rollback_bundles/manual_edit_audit_2026-06-23/live_apps_script_snapshot
clasp push
```

## Notes

- The BigQuery manual landing-table columns are additive. If deployed and rollback is needed, the safest rollback is to restore the views and old loader/script behavior and leave the extra columns unused.
- Dropping the additive table columns is possible but is a separate live schema change and should only be done if exact cleanup is required.
- The Sheet copy is a full rollback reference. It does not overwrite the production spreadsheet ID by itself.
