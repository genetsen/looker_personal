# Changelog

## 2026-08-10

**WP loader restored to the shared Google authentication path** ([R](/Users/eugenetsenter/Looker_clonedRepo/looker_personal/wp/load_wp_search_data_template.R)) — 🟢 **Verified and committed**<br>The [WP workbook loader](/Users/eugenetsenter/Looker_clonedRepo/looker_personal/wp/README.md) now prefers the consolidated Google login for Sheets and BigQuery while retaining its established cached credentials as fallback. The canonical universal runner read the live workbook and uploaded 24,992 normalized rows successfully.

## 2026-07-07

- **FIXED** - Excluded `1000heads` campaigns in the shared-social production builder before they can enter shared-social staging or flow into the master data model.

### Pending Next Actions

- **Since Jul 7** - Update the saved `stg__olipop__crossplatform_raw_tbl_sched` transfer config with owner-account credentials so future scheduled runs keep excluding `1000heads` campaigns
