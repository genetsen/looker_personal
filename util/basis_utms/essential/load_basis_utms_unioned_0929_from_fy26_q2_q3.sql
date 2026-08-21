-- FY26 Q2/Q3 Basis UTM production promotion.
-- Reads the newest normalized Q2/Q3 landing table, excludes blank mappings,
-- updates matching placement-and-creative keys, and inserts new keys.
-- Historical keys omitted from the newest workbook remain available.
-- Safe to rerun because the MERGE is idempotent and fails closed if the newest
-- source contains more than one complete URL for the same business key.

DECLARE target_rows_before INT64;
DECLARE target_rows_after INT64;
DECLARE source_rows INT64;

SET target_rows_before = (
  SELECT COUNT(*)
  FROM `looker-studio-pro-452620.landing.basis_utms_unioned-0929`
);

CREATE TEMP TABLE latest_rows AS
SELECT DISTINCT
  CAST(src.line_item AS STRING) AS line_item,
  CAST(src.tag_placement AS STRING) AS tag_placement,
  CAST(src.name AS STRING) AS name,
  SAFE_CAST(src.end_date AS DATE) AS end_date,
  SAFE_CAST(src.start_date AS DATE) AS start_date,
  LOWER(
    COALESCE(
      REGEXP_EXTRACT(CAST(src.name AS STRING), r'(?i)(\d{2,4}x\d{2,4})'),
      REGEXP_EXTRACT(CAST(src.tag_placement AS STRING), r'(?i)(\d{2,4}x\d{2,4})'),
      REGEXP_EXTRACT(CAST(src.line_item AS STRING), r'(?i)(\d{2,4}x\d{2,4})')
    )
  ) AS size,
  CAST(src.formats AS STRING) AS formats,
  CAST(src.url AS STRING) AS url
FROM `looker-studio-pro-452620.landing.basis_utms_pivoted_fy26_q2_q3_aug20` AS src
WHERE src.name IS NOT NULL
  AND TRIM(CAST(src.name AS STRING)) != ''
  AND src.url IS NOT NULL
  AND TRIM(CAST(src.url AS STRING)) != '';

ASSERT (
  SELECT COUNT(*)
  FROM (
    SELECT tag_placement, name
    FROM latest_rows
    GROUP BY tag_placement, name
    HAVING COUNT(*) > 1
  )
) = 0 AS 'Newest FY26 Q2/Q3 Basis source has more than one row for a placement-and-creative key';

SET source_rows = (SELECT COUNT(*) FROM latest_rows);

MERGE `looker-studio-pro-452620.landing.basis_utms_unioned-0929` AS target
USING latest_rows AS source
ON target.tag_placement = source.tag_placement
  AND target.name = source.name
WHEN MATCHED THEN
  UPDATE SET
    line_item = source.line_item,
    end_date = source.end_date,
    start_date = source.start_date,
    size = source.size,
    formats = source.formats,
    url = source.url
WHEN NOT MATCHED THEN
  INSERT (line_item, tag_placement, name, end_date, start_date, size, formats, url)
  VALUES (
    source.line_item,
    source.tag_placement,
    source.name,
    source.end_date,
    source.start_date,
    source.size,
    source.formats,
    source.url
  );

SET target_rows_after = (
  SELECT COUNT(*)
  FROM `looker-studio-pro-452620.landing.basis_utms_unioned-0929`
);

SELECT
  target_rows_before,
  source_rows,
  target_rows_after;
