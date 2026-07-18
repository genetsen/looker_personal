-- Build the weekly Purely Elizabeth delivered-product-group media-and-sales table.
--
-- Sources:
--   master_ext_west.mart_data_model_purelyElizabeth for current dashboard media semantics.
--   PE.purely_elizabeth_campaign_product_mapping for explicit overrides.
--   PE.protein_granola_weekly_sales for combined MULO and Natural Expanded sales and TDP.
--
-- Output grain:
--   One row per Monday-through-Sunday week, labeled with the Sunday week-ending date.
--
-- Business rules:
--   Every valid exact mapping row is included.
--   Campaigns absent from the mapping table remain visible as UNMAPPED delivery.
--   Only mapped product groups can receive product-specific sales measures.
--   Sales-only and media-only weeks remain visible; missing measures remain NULL.

CREATE OR REPLACE TABLE
  `looker-studio-pro-452620.master_ext_west.mart_PE_delivery_plus_sales_weekly`
OPTIONS (
  description = 'One row per Sunday-ending week and product group with Purely Elizabeth media delivery. Campaigns without a valid product mapping remain visible as UNMAPPED delivery and do not receive product-specific sales. Currently available Protein Granola MULO plus Natural Expanded sales and TDP join only to PROTEIN GRANOLA. Retail sales are temporal comparison metrics rather than campaign-attributed outcomes.'
) AS
WITH classified_media_candidates AS (
  -- HOW TO ADD A PRODUCT GROUP
  --
  -- Add each exact campaign name to PE.purely_elizabeth_campaign_product_mapping.
  -- Use a year-free label such as PROTEIN GRANOLA for product_group.
  -- Mapped campaigns can receive sales for their product group.
  -- Campaigns without a valid mapping remain visible as UNMAPPED delivery.
  --
  -- No weekly-view change is needed for media. After the mapped product group has
  -- delivery, this view includes its complete weekly media timeline.
  --
  -- Sales require a separate product-specific weekly source. Until that source is
  -- added below, sales_dollars and sales_tdp remain NULL for the new product group.
  -- Never reuse Protein Granola sales for a different product group or UNMAPPED.
  SELECT
    media.*,
    COALESCE(NULLIF(TRIM(mapping.product_group), ''), 'UNMAPPED') AS product_group
  FROM
    `looker-studio-pro-452620.master_ext_west.mart_data_model_purelyElizabeth` AS media
  LEFT JOIN
    `looker-studio-pro-452620.PE.purely_elizabeth_campaign_product_mapping` AS mapping
    ON media._advertiser = mapping.advertiser_name
    AND media._campaign_name = mapping.campaign_name
),

delivered_product_groups AS (
  SELECT DISTINCT product_group
  FROM classified_media_candidates
  WHERE
    COALESCE(_spend, 0) != 0
    OR COALESCE(_impressions, 0) != 0
    OR COALESCE(_clicks, 0) != 0
    OR COALESCE(_video_plays, 0) != 0
    OR COALESCE(_video_views, 0) != 0
    OR COALESCE(_video_comps, 0) != 0
),

classified_media AS (
  SELECT candidate.*
  FROM classified_media_candidates AS candidate
  INNER JOIN delivered_product_groups
    USING (product_group)
),

media_weekly AS (
  SELECT
    DATE_ADD(DATE_TRUNC(_date, WEEK(MONDAY)), INTERVAL 6 DAY) AS week_end_date,
    product_group,
    SUM(_planned_spend) AS planned_spend,
    SUM(_planned_impressions) AS planned_impressions,
    SUM(_spend) AS delivered_spend,
    SUM(_impressions) AS delivered_impressions,
    SUM(_clicks) AS clicks,
    SUM(_video_plays) AS _video_plays,
    SUM(_video_views) AS _video_views,
    SUM(_video_comps) AS _video_comps
  FROM classified_media
  GROUP BY
    week_end_date,
    product_group
),

sales_weekly AS (
  SELECT
    time_period_end_date AS week_end_date,
    'PROTEIN GRANOLA' AS product_group,
    total_dollar_sales AS protein_granola_dollar_sales,
    total_tdp AS protein_granola_tdp
  FROM `looker-studio-pro-452620.PE.protein_granola_weekly_sales`
  WHERE EXISTS (
    SELECT 1
    FROM delivered_product_groups
    WHERE product_group = 'PROTEIN GRANOLA'
  )
)

SELECT
  -- REPORTING NOTE: Campaign fields classify the year-free product group internally.
  -- Brand-level retail sales are joined by week and are not campaign-attributed.
  COALESCE(media.week_end_date, sales.week_end_date) AS _date,
  COALESCE(media.product_group, sales.product_group) AS product_group,
  media.planned_spend AS _planned_spend,
  media.planned_impressions AS _planned_impressions,
  media.delivered_spend AS _spend,
  media.delivered_impressions AS _impressions,
  media.clicks AS _clicks,
  media._video_plays,
  media._video_views,
  media._video_comps,
  sales.protein_granola_dollar_sales AS sales_dollars,
  sales.protein_granola_tdp AS sales_tdp
FROM media_weekly AS media
FULL OUTER JOIN sales_weekly AS sales
  USING (week_end_date, product_group);
