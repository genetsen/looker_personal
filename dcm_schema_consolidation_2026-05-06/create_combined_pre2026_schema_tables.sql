DECLARE cutoff_ts TIMESTAMP DEFAULT TIMESTAMP('2026-01-01 00:00:00 UTC');
DECLARE project_id STRING DEFAULT 'giant-spoon-299605';
DECLARE dataset_id STRING DEFAULT 'ALL_DCM_adswerve';

CREATE TEMP TABLE schema_groups AS
WITH candidate_tables AS (
  SELECT table_name, creation_time
  FROM `giant-spoon-299605.ALL_DCM_adswerve.INFORMATION_SCHEMA.TABLES`
  WHERE creation_time < cutoff_ts
    AND table_type = 'BASE TABLE'
),
schema_fingerprints AS (
  SELECT
    c.table_name,
    c.creation_time,
    TO_HEX(SHA256(STRING_AGG(
      FORMAT(
        '%04d|%s|%s|%s|%s|%s',
        col.ordinal_position,
        col.column_name,
        col.data_type,
        col.is_nullable,
        IFNULL(col.collation_name, ''),
        IFNULL(col.rounding_mode, '')
      ),
      '\n'
      ORDER BY col.ordinal_position
    ))) AS schema_hash,
    COUNT(*) AS column_count
  FROM candidate_tables c
  JOIN `giant-spoon-299605.ALL_DCM_adswerve.INFORMATION_SCHEMA.COLUMNS` col
    ON col.table_name = c.table_name
  GROUP BY c.table_name, c.creation_time
),
group_summary AS (
  SELECT
    schema_hash,
    COUNT(*) AS table_count,
    MIN(creation_time) AS oldest_created,
    MAX(creation_time) AS newest_created,
    ANY_VALUE(column_count) AS column_count
  FROM schema_fingerprints
  GROUP BY schema_hash
)
SELECT
  schema_hash,
  FORMAT(
    'combined_pre2026_schema_%02d_%s',
    ROW_NUMBER() OVER (ORDER BY table_count DESC, oldest_created, schema_hash),
    SUBSTR(schema_hash, 1, 8)
  ) AS target_table_name,
  table_count,
  oldest_created,
  newest_created,
  column_count
FROM group_summary;

CREATE TEMP TABLE schema_tables AS
WITH candidate_tables AS (
  SELECT table_name, creation_time
  FROM `giant-spoon-299605.ALL_DCM_adswerve.INFORMATION_SCHEMA.TABLES`
  WHERE creation_time < cutoff_ts
    AND table_type = 'BASE TABLE'
),
schema_fingerprints AS (
  SELECT
    c.table_name,
    c.creation_time,
    TO_HEX(SHA256(STRING_AGG(
      FORMAT(
        '%04d|%s|%s|%s|%s|%s',
        col.ordinal_position,
        col.column_name,
        col.data_type,
        col.is_nullable,
        IFNULL(col.collation_name, ''),
        IFNULL(col.rounding_mode, '')
      ),
      '\n'
      ORDER BY col.ordinal_position
    ))) AS schema_hash
  FROM candidate_tables c
  JOIN `giant-spoon-299605.ALL_DCM_adswerve.INFORMATION_SCHEMA.COLUMNS` col
    ON col.table_name = c.table_name
  GROUP BY c.table_name, c.creation_time
)
SELECT
  sf.schema_hash,
  sg.target_table_name,
  sf.table_name,
  sf.creation_time
FROM schema_fingerprints sf
JOIN schema_groups sg
  USING (schema_hash);

CREATE TEMP TABLE schema_columns AS
SELECT
  st.schema_hash,
  col.ordinal_position,
  col.column_name,
  col.data_type
FROM (
  SELECT schema_hash, ARRAY_AGG(table_name ORDER BY creation_time, table_name LIMIT 1)[OFFSET(0)] AS representative_table
  FROM schema_tables
  GROUP BY schema_hash
) st
JOIN `giant-spoon-299605.ALL_DCM_adswerve.INFORMATION_SCHEMA.COLUMNS` col
  ON col.table_name = st.representative_table;

FOR schema_rec IN (
  SELECT *
  FROM schema_groups
  ORDER BY target_table_name
) DO
  EXECUTE IMMEDIATE (
    WITH column_sql AS (
      SELECT
        STRING_AGG(FORMAT('`%s`', column_name), ', ' ORDER BY ordinal_position) AS column_list
      FROM schema_columns
      WHERE schema_hash = schema_rec.schema_hash
    ),
    union_sql AS (
      SELECT
        STRING_AGG(
          FORMAT(
            '%T AS _source_table, %T AS _source_table_created_at, TO_HEX(SHA256(TO_JSON_STRING(STRUCT(%s)))) AS _row_fingerprint, %s FROM `%s.%s.%s`',
            table_name,
            creation_time,
            (SELECT column_list FROM column_sql),
            (SELECT column_list FROM column_sql),
            project_id,
            dataset_id,
            table_name
          ),
          '\nUNION ALL\nSELECT '
          ORDER BY creation_time, table_name
        ) AS union_body
      FROM schema_tables
      WHERE schema_hash = schema_rec.schema_hash
    )
    SELECT FORMAT(
      '''
      CREATE TABLE `%s.%s.%s`
      OPTIONS (
        description = "Combined pre-2026 DCM tables for one exact schema group. Preserves source rows and adds trace columns plus overlap flags. Created by Codex on 2026-05-06; source cutoff is creation_time before 2026-01-01 UTC."
      ) AS
      WITH unioned AS (
        SELECT %s
      ),
      overlap_groups AS (
        SELECT
          _row_fingerprint,
          COUNT(*) AS _row_occurrence_count,
          COUNT(DISTINCT _source_table) AS _source_table_count,
          ARRAY_AGG(DISTINCT _source_table ORDER BY _source_table) AS _overlap_source_tables,
          MIN(_source_table_created_at) AS _first_source_table_created_at,
          MAX(_source_table_created_at) AS _last_source_table_created_at
        FROM unioned
        GROUP BY _row_fingerprint
      )
      SELECT
        u.* EXCEPT(_row_fingerprint),
        u._row_fingerprint,
        og._row_occurrence_count,
        og._source_table_count,
        og._row_occurrence_count > 1 AS _is_duplicate_row,
        og._source_table_count > 1 AS _is_overlapping_row,
        IF(og._source_table_count > 1, og._overlap_source_tables, []) AS _overlap_source_tables,
        og._first_source_table_created_at,
        og._last_source_table_created_at
      FROM unioned u
      JOIN overlap_groups og
        USING (_row_fingerprint)
      ''',
      project_id,
      dataset_id,
      schema_rec.target_table_name,
      (SELECT union_body FROM union_sql)
    )
  );
END FOR;
