ALTER TABLE `giant-spoon-299605.ALL_DCM_adswerve.combined_dcm_test1024_daily_delivery_pre2026`
SET OPTIONS (
  description = 'Combines 139 pre-2026 CM360 / DCMtoBigquery Test 1024 daily delivery source tables with the same 11-column schema. Source tables were created from 2025-03-11 through 2025-07-27 and contribute 32,171 rows. Adds trace and overlap columns: _source_table, _source_table_created_at, _row_fingerprint, _is_duplicate_row, _is_overlapping_row, and overlap source metadata.'
);

ALTER TABLE `giant-spoon-299605.ALL_DCM_adswerve.combined_dcm_planned_vs_actual_delivery_pre2026`
SET OPTIONS (
  description = 'Combines 89 pre-2026 CM360 planned-vs-actual delivery source tables with package, media cost, and video metrics using the same 15-column schema. Source tables were created from 2025-01-23 through 2025-12-31 and contribute 431,476 rows. Adds trace and overlap columns: _source_table, _source_table_created_at, _row_fingerprint, _is_duplicate_row, _is_overlapping_row, and overlap source metadata.'
);

ALTER TABLE `giant-spoon-299605.ALL_DCM_adswerve.combined_mmm_cm360_weekly_media_pre2026`
SET OPTIONS (
  description = 'Combines 6 pre-2026 MMM / CM360 weekly media source tables with the same 12-column schema. Source tables were created from 2023-08-16 through 2025-08-11 and contribute 870,328 rows. Adds trace and overlap columns: _source_table, _source_table_created_at, _row_fingerprint, _is_duplicate_row, _is_overlapping_row, and overlap source metadata.'
);

ALTER TABLE `giant-spoon-299605.ALL_DCM_adswerve.combined_empty_test_placeholder_pre2026`
SET OPTIONS (
  description = 'Combines 1 pre-2026 empty legacy test placeholder source table with a 1-column schema. Source table was created on 2022-01-19 and contributes 0 rows. Adds trace and overlap columns for consistency with the other combined pre-2026 cleanup tables.'
);

ALTER TABLE `giant-spoon-299605.ALL_DCM_adswerve.combined_empty_funnel_dcm_legacy_activity_pre2026`
SET OPTIONS (
  description = 'Combines 1 pre-2026 empty legacy Funnel + DoubleClick Campaign Manager activity source table with a 47-column schema. Source table was created on 2022-01-19 and contributes 0 rows. Adds trace and overlap columns for consistency with the other combined pre-2026 cleanup tables.'
);

ALTER TABLE `giant-spoon-299605.ALL_DCM_adswerve.combined_dcm_campaign_site_av_viewability_test_pre2026`
SET OPTIONS (
  description = 'Combines 1 pre-2026 CM360 test source table with campaign, site, date, delivery, and Active View metrics using a 12-column schema. Source table was created on 2023-08-16 and contributes 288 rows. Adds trace and overlap columns: _source_table, _source_table_created_at, _row_fingerprint, _is_duplicate_row, _is_overlapping_row, and overlap source metadata.'
);

ALTER TABLE `giant-spoon-299605.ALL_DCM_adswerve.combined_dcm_campaign_start_conversion_test_pre2026`
SET OPTIONS (
  description = 'Combines 1 pre-2026 CM360 test source table with advertiser, campaign start date, and click-through conversion metrics using a 5-column schema. Source table was created on 2024-02-05 and contributes 5 rows. Adds trace and overlap columns: _source_table, _source_table_created_at, _row_fingerprint, _is_duplicate_row, _is_overlapping_row, and overlap source metadata.'
);

ALTER TABLE `giant-spoon-299605.ALL_DCM_adswerve.combined_dcm_site_placement_delivery_nodate_test_pre2026`
SET OPTIONS (
  description = 'Combines 1 pre-2026 CM360 test source table with advertiser, campaign, site, placement, and delivery metrics but no date column, using a 10-column schema. Source table was created on 2024-09-17 and contributes 287 rows. Adds trace and overlap columns: _source_table, _source_table_created_at, _row_fingerprint, _is_duplicate_row, _is_overlapping_row, and overlap source metadata.'
);

ALTER TABLE `giant-spoon-299605.ALL_DCM_adswerve.combined_dcm_campaign_delivery_summary_test_pre2026`
SET OPTIONS (
  description = 'Combines 1 pre-2026 CM360 test source table summarized at advertiser and campaign delivery level using a 6-column schema. Source table was created on 2024-09-17 and contributes 10 rows. Adds trace and overlap columns: _source_table, _source_table_created_at, _row_fingerprint, _is_duplicate_row, _is_overlapping_row, and overlap source metadata.'
);

ALTER TABLE `giant-spoon-299605.ALL_DCM_adswerve.combined_dcm_joined_final_delivery_pre2026`
SET OPTIONS (
  description = 'Combines 1 pre-2026 joined final DCM delivery source table with package, media cost, and video metrics using a 15-column schema. Source table was created on 2025-01-23 and contributes 135,767 rows. Adds trace and overlap columns: _source_table, _source_table_created_at, _row_fingerprint, _is_duplicate_row, _is_overlapping_row, and overlap source metadata.'
);
