-- QA-only preview for the v3 offline planned-delivery fallback.
--
-- Purpose:
--   Compare the intended TV, Print, OOH, and dOOH planned-as-delivered fallback
--   with the current production v3 table before rebuilding production.
--
-- Reads:
--   master_stg.data_model_v3 (current production v3 table).
-- Produces:
--   master_stg.data_model_v3_offline_fallback_baseline_qa (temporary QA view).
--   master_stg.data_model_v3_offline_fallback_qa (temporary QA view).

CREATE OR REPLACE VIEW
  `looker-studio-pro-452620.master_stg.data_model_v3_offline_fallback_baseline_qa`
OPTIONS (
  description = 'Temporary null-safe baseline view used to compare the v3 offline planned-delivery fallback with current production.'
) AS
SELECT
  TO_JSON_STRING(STRUCT(
    _package_id AS package_id,
    _date AS date,
    _placement_id AS placement_id,
    qa_v3_ad_name AS ad_name,
    _creative_name AS creative_name,
    conv_activity AS conversion_activity,
    fpd_factor AS fpd_factor,
    qa_v3_row_type AS row_type,
    qa_v3_source_detail_type AS source_detail_type
  )) AS qa_comparison_key,
  source.*
FROM `looker-studio-pro-452620.master_stg.data_model_v3` AS source;

CREATE OR REPLACE VIEW
  `looker-studio-pro-452620.master_stg.data_model_v3_offline_fallback_qa`
OPTIONS (
  description = 'Temporary QA preview of the v3 offline planned-delivery fallback. It fills _spend and _impressions from planned values only for planned-only TV, Print, OOH, and dOOH rows with no delivery source.'
) AS
SELECT
  TO_JSON_STRING(STRUCT(
    _package_id AS package_id,
    _date AS date,
    _placement_id AS placement_id,
    qa_v3_ad_name AS ad_name,
    _creative_name AS creative_name,
    conv_activity AS conversion_activity,
    fpd_factor AS fpd_factor,
    qa_v3_row_type AS row_type,
    qa_v3_source_detail_type AS source_detail_type
  )) AS qa_comparison_key,
  * REPLACE (
    CASE
      WHEN qa_v3_row_type = 'planned_only_package_date'
        AND _planned_spend IS NOT NULL
        AND NULLIF(_planned_impressions, 0) IS NOT NULL
        AND (
          qa_media_data_type = 'tv'
          OR _channel_group IN ('linear', 'print', 'ooh', 'ooh_d')
          OR _channel IN ('linear_tv', 'print', 'ooh', 'ooh_d')
          OR LOWER(COALESCE(_media_name, '')) IN (
            'tv', 'print', 'magazine', 'newspaper', 'ooh'
          )
        )
        THEN _planned_spend
      ELSE _spend
    END AS _spend,
    CASE
      WHEN qa_v3_row_type = 'planned_only_package_date'
        AND NULLIF(_planned_impressions, 0) IS NOT NULL
        AND (
          qa_media_data_type = 'tv'
          OR _channel_group IN ('linear', 'print', 'ooh', 'ooh_d')
          OR _channel IN ('linear_tv', 'print', 'ooh', 'ooh_d')
          OR LOWER(COALESCE(_media_name, '')) IN (
            'tv', 'print', 'magazine', 'newspaper', 'ooh'
          )
        )
        THEN _planned_impressions
      ELSE _impressions
    END AS _impressions
  )
FROM `looker-studio-pro-452620.master_stg.data_model_v3`;
