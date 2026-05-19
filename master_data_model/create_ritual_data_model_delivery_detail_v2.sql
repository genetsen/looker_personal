-- Build Ritual-only delivery detail view over the corrected v2 detail model.

CREATE OR REPLACE VIEW `looker-studio-pro-452620.master_stg.ritual_data_model_delivery_detail_v2` AS
SELECT
  *
FROM `looker-studio-pro-452620.master_stg.data_model_delivery_detail_v2`
WHERE `_advertiser_name` = 'Ritual'
   OR `_advertiser_short_name` = 'RTL';
