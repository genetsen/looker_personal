# Changelog

## 2026-07-27

**WP workbook loader reads the verified ad-group-ID tab** ([R](/Users/eugenetsenter/Looker_clonedRepo/looker_personal/wp/load_wp_search_data_template.R)) — 🟢 **Verified and committed**<br>The [WP delivery-workbook workflow](/Users/eugenetsenter/Looker_clonedRepo/looker_personal/wp/README.md) now reads `Import_blend`, the exact available worksheet title, alongside `Report`, so its preview no longer substitutes generated ad-group IDs because of a stale tab name. The production upload remains a separate manual action. [More details](/Users/eugenetsenter/Looker_clonedRepo/looker_personal/wp/README.md)

## 2026-07-07

- **FIXED** - Excluded `1000heads` campaigns in the shared-social production builder before they can enter shared-social staging or flow into the master data model.

### Pending Next Actions

- **Since Jul 7** - Update the saved `stg__olipop__crossplatform_raw_tbl_sched` transfer config with owner-account credentials so future scheduled runs keep excluding `1000heads` campaigns
