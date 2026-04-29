-- Build Ritual-only package/date data model view.
--
-- Purpose:
--   Provide the same columns as the generalized master data model, limited to
--   the Ritual advertiser/client slice.
--
-- Source of truth:
--   `looker-studio-pro-452620.master_stg.data_model`

CREATE OR REPLACE VIEW `looker-studio-pro-452620.master_stg.ritual_data_model` AS
SELECT
  *
FROM `looker-studio-pro-452620.master_stg.data_model`
WHERE advertiser_name = 'Ritual'
   OR advertiser_short_name = 'RTL';
