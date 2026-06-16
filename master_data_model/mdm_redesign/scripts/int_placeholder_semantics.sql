-- ============================================================
-- int_placeholder_semantics.sql
--
-- Story 2.3: Add Explicit Placeholder Semantics
--
-- Wraps the compatibility view adding standardized placeholder
-- values for unavailable lower-grain dimensions.
-- ============================================================

CREATE OR REPLACE VIEW `looker-studio-pro-452620.mdm_int.int_placeholder_semantics`
OPTIONS(
  description='Compatibility view with explicit placeholder semantics for unavailable lower-grain dimensions. Uses standardized not_available_at_source, unknown_from_source, and not_applicable_to_source values. Story 2.3. Does not replace production objects.'
)
AS
SELECT
  base.*,

  -- Creative dimension: not available at package/date grain
  CASE
    WHEN _creative_img IS NULL AND fpd_orig_creative IS NULL
    THEN 'not_available_at_source'
    ELSE COALESCE(_creative_img, fpd_orig_creative, 'not_available_at_source')
  END AS univ_creative_placeholder,

  -- DMA dimension: not available at package/date grain
  'not_available_at_source' AS univ_dma_placeholder,

  -- Placement dimension
  CASE
    WHEN _placement_id IS NULL THEN 'not_available_at_source'
    ELSE _placement_id
  END AS univ_placement_placeholder,

  -- Campaign dimension
  CASE
    WHEN _campaign_name IS NULL THEN 'not_available_at_source'
    ELSE _campaign_name
  END AS univ_campaign_placeholder,

  -- Advertiser dimension
  CASE
    WHEN _advertiser IS NULL THEN 'not_available_at_source'
    ELSE _advertiser
  END AS univ_advertiser_placeholder,

  -- Supplier dimension
  CASE
    WHEN _supplier_code IS NULL THEN 'not_available_at_source'
    ELSE _supplier_code
  END AS univ_supplier_placeholder,

  -- Date dimension: always available at package/date grain
  CASE
    WHEN _date IS NULL THEN 'unknown_from_source'
    ELSE SAFE_CAST(_date AS STRING)
  END AS univ_date_placeholder

FROM `looker-studio-pro-452620.mdm_int.int_universal_compat_view` base;
