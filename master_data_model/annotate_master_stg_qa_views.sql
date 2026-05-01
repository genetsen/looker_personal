-- Annotate master_data_model QA views in master_stg.
--
-- Purpose:
--   Republish QA views with warehouse-visible descriptions and SQL comments
--   directly under the SELECT statement so temporary validation objects are
--   not context-free.
--
-- Safe to rerun:
--   Yes. This script recreates the existing QA views from their current
--   INFORMATION_SCHEMA view definitions and adds metadata only.

DECLARE comment_prefix STRING DEFAULT 'master_data_model QA object. ';

CREATE TEMP TABLE qa_view_notes AS
SELECT * FROM UNNEST([
  STRUCT(
    'data_model_qa_rename_columns' AS table_name,
    'CURRENT QA VIEW - master_data_model rename-column candidate. Tests the approved 117-column prefixed schema before production replacement. Local SQL: create_master_stg_data_model.sql lines 887-1012 contain the renamed public SELECT. Safe to delete only after production data_model is replaced and validated.' AS description,
    'WHAT: Current master_data_model QA rename candidate.\nCHANGE: Applies approved column rename map to the public output schema.\nLOCAL LINES: create_master_stg_data_model.sql:887-1012 contains the renamed output SELECT.\nSAFE TO DELETE: Not yet. Delete after production data_model has been replaced and validated.' AS top_comment
  ),
  STRUCT(
    'data_model_mart_qa_rename_columns',
    'CURRENT QA VIEW - reporting mart over the renamed master QA view. Recalculates package rollups after low_signal_dcm filtering using the new prefixed names. Local SQL: create_master_stg_data_model_mart.sql lines 7-59. Safe to delete only after production data_model_mart is replaced and validated.',
    'WHAT: Current mart QA view for the renamed master schema.\nCHANGE: Filters low_signal_dcm and recalculates qa_*_doNotSum rollups using renamed columns.\nLOCAL LINES: create_master_stg_data_model_mart.sql:7-59 contains the mart logic.\nSAFE TO DELETE: Not yet. Delete after production data_model_mart has been replaced and validated.'
  ),
  STRUCT(
    'ritual_data_model_qa_rename_columns',
    'CURRENT QA VIEW - Ritual compatibility slice over the renamed master QA view. Exposes old column names for downstream compatibility. Local SQL: create_ritual_data_model_view.sql lines 10-131. Safe to delete only after production ritual_data_model is replaced and validated.',
    'WHAT: Current Ritual compatibility QA view.\nCHANGE: Reads renamed master columns and aliases them back to old Ritual-facing names.\nLOCAL LINES: create_ritual_data_model_view.sql:10-131 contains the compatibility aliases.\nSAFE TO DELETE: Not yet. Delete after production ritual_data_model has been replaced and validated.'
  ),
  STRUCT(
    'data_model_qa_source_issues',
    'SUPERSEDED QA VIEW - prior source/callout issue candidate. It added source availability, issue category, row callouts, unmatched DCM/FPD evidence, and low_signal_dcm tagging before the rename pass. Superseded by data_model_qa_rename_columns. Safe to delete after rename QA/prod validation no longer needs historical comparison.',
    'WHAT: Superseded source/callout QA candidate.\nCHANGE: Added source availability, issue category, row callouts, unmatched DCM/FPD evidence, and low_signal_dcm tagging before the rename pass.\nLOCAL LINES: No current local SQL file owns this exact published QA object; it is superseded by create_master_stg_data_model.sql current rename candidate.\nSAFE TO DELETE: Yes, after rename QA/prod validation no longer needs historical comparison.'
  ),
  STRUCT(
    'data_model_mart_qa_source_issues',
    'SUPERSEDED QA VIEW - prior mart candidate over data_model_qa_source_issues. It filtered low_signal_dcm and recalculated old-name package rollups. Superseded by data_model_mart_qa_rename_columns. Safe to delete after rename QA/prod validation no longer needs historical comparison.',
    'WHAT: Superseded mart QA view for the source/callout candidate.\nCHANGE: Filtered low_signal_dcm and recalculated old-name package rollups.\nLOCAL LINES: No current local SQL file owns this exact published QA object; current equivalent is create_master_stg_data_model_mart.sql:7-59.\nSAFE TO DELETE: Yes, after rename QA/prod validation no longer needs historical comparison.'
  ),
  STRUCT(
    'data_model_qa_p_package_friendly',
    'SUPERSEDED QA VIEW - earlier candidate used to validate p_package_friendly coverage. Superseded by later source/callout, TV, and rename QA views. Safe to delete after confirming no one is using it for historical p_package_friendly proof.',
    'WHAT: Superseded p_package_friendly validation QA view.\nCHANGE: Earlier candidate used to test p_package_friendly coverage in the master output.\nLOCAL LINES: No current local SQL file owns this exact published QA object; p_package_friendly is now mapped in create_master_stg_data_model.sql:904.\nSAFE TO DELETE: Yes, after confirming no one needs historical p_package_friendly proof.'
  ),
  STRUCT(
    'ritual_data_model_qa_p_package_friendly',
    'SUPERSEDED QA VIEW - earlier Ritual slice over data_model_qa_p_package_friendly. Superseded by ritual_data_model_qa_rename_columns. Safe to delete after confirming no one is using it for historical Ritual p_package_friendly proof.',
    'WHAT: Superseded Ritual p_package_friendly QA view.\nCHANGE: Earlier Ritual slice over the p_package_friendly QA candidate.\nLOCAL LINES: No current local SQL file owns this exact published QA object; current Ritual compatibility aliases are in create_ritual_data_model_view.sql:10-131.\nSAFE TO DELETE: Yes, after confirming no one needs historical Ritual p_package_friendly proof.'
  ),
  STRUCT(
    'data_model_qa_tv_layer',
    'SUPERSEDED QA VIEW - earlier candidate used to validate adding the TV branch. TV logic now lives in current master SQL. Safe to delete after confirming no one is using it for historical TV-layer proof.',
    'WHAT: Superseded TV-layer validation QA view.\nCHANGE: Earlier candidate used to validate adding TV rows and TV source fields.\nLOCAL LINES: Current TV branch is in create_master_stg_data_model.sql; public TV output aliases are lines 968-977.\nSAFE TO DELETE: Yes, after confirming no one needs historical TV-layer proof.'
  )
]);

FOR row IN (
  SELECT
    n.table_name,
    n.description,
    n.top_comment,
    v.view_definition
  FROM qa_view_notes AS n
  JOIN `looker-studio-pro-452620.master_stg.INFORMATION_SCHEMA.VIEWS` AS v
    ON v.table_name = n.table_name
)
DO
  EXECUTE IMMEDIATE FORMAT(
    """CREATE OR REPLACE VIEW `looker-studio-pro-452620.master_stg.%s`
OPTIONS(description=%T)
AS
SELECT
/*
%s
*/
  *
FROM (
%s
)""",
    row.table_name,
    row.description,
    row.top_comment,
    row.view_definition
  );
END FOR;
