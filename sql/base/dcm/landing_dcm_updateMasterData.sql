DECLARE latest_table   STRING;
DECLARE merge_sql      STRING;
DECLARE column_list    STRING;      -- raw columns
DECLARE update_clause  STRING;      -- for WHEN MATCHED
DECLARE insert_columns STRING;      -- for WHEN NOT MATCHED
DECLARE insert_values  STRING;      -- for WHEN NOT MATCHED
DECLARE agg_select     STRING;      -- builds SELECT list with SUM()/ANY_VALUE()

-- 1️⃣  Get most-recent source table
SET latest_table = (
  SELECT table_name
  FROM   `giant-spoon-299605.ALL_DCM_adswerve`.INFORMATION_SCHEMA.TABLES
  ORDER BY creation_time DESC
  LIMIT 1
);

-- 2️⃣  Column lists -----------------------------------------------------------
SET column_list = (
  SELECT STRING_AGG(column_name, ', ')
  FROM   `giant-spoon-299605.ALL_DCM_adswerve`.INFORMATION_SCHEMA.COLUMNS
  WHERE  table_name = latest_table
);

-- a.  Build update set: target.col = source.col
SET update_clause = (
  SELECT STRING_AGG(FORMAT("target.%s = source.%s", column_name, column_name), ', ')
  FROM   `giant-spoon-299605.ALL_DCM_adswerve`.INFORMATION_SCHEMA.COLUMNS
  WHERE  table_name = latest_table
);

-- b.  Insert columns & values (raw copy)
SET insert_columns = column_list;
SET insert_values  = (
  SELECT STRING_AGG(FORMAT("source.%s", column_name), ', ')
  FROM   `giant-spoon-299605.ALL_DCM_adswerve`.INFORMATION_SCHEMA.COLUMNS
  WHERE  table_name = latest_table
);

-- Append computed columns
SET insert_columns = CONCAT(insert_columns, ', placement_id, package_id, key');
SET insert_values  = CONCAT(
  insert_values,
  ", source.placement_id",
  ", source.package_id",
  ", source.key"
);

-- 3️⃣  Dynamic SELECT list for aggregation -------------------------------
SET agg_select = (
  SELECT STRING_AGG(
           CASE
             WHEN data_type IN ('INT64','NUMERIC','FLOAT64','BIGNUMERIC')
               THEN FORMAT('SUM(%s) AS %s', column_name, column_name)
             ELSE FORMAT('ANY_VALUE(%s) AS %s', column_name, column_name)
           END,
           ', '
         )
  FROM   `giant-spoon-299605.ALL_DCM_adswerve`.INFORMATION_SCHEMA.COLUMNS
  WHERE  table_name = latest_table
);

-- 4️⃣  Build and run the MERGE ------------------------------------------------
SET merge_sql = FORMAT("""
MERGE `giant-spoon-299605.data_model_2025.new_md_test` AS target
USING (
  /* ---- Stage A: add derived IDs & key ---- */
  WITH src AS (
    SELECT
      %s,
      COALESCE(REGEXP_EXTRACT(placement, r'P3[^|_]+'),
               REGEXP_EXTRACT(placement, r'P2[^|_]+')) AS placement_id,
      COALESCE(REGEXP_EXTRACT(package_roadblock, r'P3[^|_]+'),
               REGEXP_EXTRACT(package_roadblock, r'P2[^|_]+')) AS package_id,
      CONCAT(date, ad) AS key
    FROM `giant-spoon-299605.ALL_DCM_adswerve.%s`
  )
  /* ---- Stage B: aggregate by key ---- */
  SELECT
    %s,
    placement_id,
    package_id,
    key
  FROM src
  GROUP BY
    key, placement_id, package_id
) AS source
ON target.key = source.key
WHEN MATCHED THEN
  UPDATE SET %s
WHEN NOT MATCHED THEN
  INSERT (%s)
  VALUES (%s);
""",             -- FORMAT() args
   column_list,  -- %s (Stage A select list)
   latest_table,
   agg_select,   -- %s (Stage B aggregation select list)
   update_clause,
   insert_columns,
   insert_values
);

-- 5️⃣  Execute
EXECUTE IMMEDIATE merge_sql;