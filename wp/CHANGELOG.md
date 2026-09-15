# Changelog

## 2026-09-14

**TikTok Smart+ hierarchy enrichment is live across the master model** ([SQL](/Users/eugenetsenter/Looker_clonedRepo/looker_personal/wp/sql/create_stg_crossplatform_wp_primary_production.sql)) — 🟢 **Verified and committed**<br>The [shared-social workflow](/Users/eugenetsenter/Looker_clonedRepo/looker_personal/wp/README.md) now fills blank TikTok ADIF Smart+ hierarchy fields from current creative history while preserving creative-level delivery identity. SQL Change Guard passed, the saved scheduled query completed successfully, and all 70 current rows across seven ads carry campaign and ad-group IDs with unchanged delivery totals through shared staging and every master-model layer. Hierarchy and pacing were proven to be separate conditions; the pacing gap was subsequently closed by the unified social-pacing schedule on September 15.

### Pending Next Actions


## 2026-08-10

**WP loader uses shared authentication and the live workbook's current ID tab** ([R](/Users/eugenetsenter/Looker_clonedRepo/looker_personal/wp/load_wp_search_data_template.R)) — 🟡 **Partially verified**<br>The [WP workbook loader](/Users/eugenetsenter/Looker_clonedRepo/looker_personal/wp/README.md) prefers the consolidated Google login with cached credentials retained as fallback and now reads `Import_blend` rather than the nonexistent `Import` tab. A live no-write preview read both required tabs and normalized 24,992 rows without generating fallback IDs.
- **⚠️ Unverified** - The production upload has not been rerun with the corrected `Import_blend` code path.

## 2026-07-27

**WP workbook loader reads the verified ad-group-ID tab** ([R](/Users/eugenetsenter/Looker_clonedRepo/looker_personal/wp/load_wp_search_data_template.R)) — 🟢 **Verified and committed**<br>The [WP delivery-workbook workflow](/Users/eugenetsenter/Looker_clonedRepo/looker_personal/wp/README.md) now reads `Import_blend`, the exact available worksheet title, alongside `Report`, so its preview no longer substitutes generated ad-group IDs because of a stale tab name. The production upload remains a separate manual action. [More details](/Users/eugenetsenter/Looker_clonedRepo/looker_personal/wp/README.md)

## 2026-07-07

- **FIXED** - Excluded `1000heads` campaigns in the shared-social production builder before they can enter shared-social staging or flow into the master data model.

### Pending Next Actions

- **Since Jul 7** - Update the saved `stg__olipop__crossplatform_raw_tbl_sched` transfer config with owner-account credentials so future scheduled runs keep excluding `1000heads` campaigns
