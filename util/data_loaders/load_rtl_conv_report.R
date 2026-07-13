#!/usr/bin/env Rscript

# RTL conversion compatibility-table refresh
#
# Reads persistent direct CM360 conversion history from BigQuery and replaces
# the rows in landing.rtl_conv_report without dropping or recreating the target
# table. The legacy 18-column schema stays intact for compatibility. Fields
# that existed only in the retired Google Sheet source remain present and are
# set to NULL rather than being populated with misleading substitute values.

suppressPackageStartupMessages({
  library(bigrquery)
})

project_id <- "looker-studio-pro-452620"
source_dataset <- "master_stg"
source_table_name <- "rtl_cm360_direct_conversions"
target_dataset <- "landing"
target_table_name <- "rtl_conv_report"

source_table_path <- paste(project_id, source_dataset, source_table_name, sep = ".")
target_table_path <- paste(project_id, target_dataset, target_table_name, sep = ".")

expected_target_schema <- data.frame(
  column_name = c(
    "new_site",
    "unnamed_column_2",
    "date",
    "campaign",
    "campaign_id",
    "site_cm360",
    "package_roadblock",
    "creative",
    "activity",
    "impressions",
    "clicks",
    "click_rate",
    "total_conversions",
    "data_refresh_date",
    "loaded_at",
    "source_sheet_id",
    "source_sheet_tab",
    "source_sheet_gid"
  ),
  data_type = c(
    "STRING",
    "STRING",
    "DATE",
    "STRING",
    "STRING",
    "STRING",
    "STRING",
    "STRING",
    "STRING",
    "INT64",
    "INT64",
    "FLOAT64",
    "INT64",
    "DATE",
    "TIMESTAMP",
    "STRING",
    "STRING",
    "STRING"
  ),
  is_nullable = rep("YES", 18),
  stringsAsFactors = FALSE
)

download_query <- function(sql) {
  bigrquery::bq_table_download(
    bigrquery::bq_project_query(project_id, sql, quiet = TRUE),
    quiet = TRUE
  )
}

run_query <- function(sql) {
  query_job <- bigrquery::bq_perform_query(
    query = sql,
    billing = project_id
  )
  bigrquery::bq_job_wait(query_job, quiet = TRUE)
  invisible(query_job)
}

cat("RTL conversion compatibility refresh started at", format(Sys.time()), "\n")
cat("Direct CM360 source:", source_table_path, "\n")
cat("Existing-schema destination:", target_table_path, "\n\n")

# Stop before any row replacement if the live target no longer has the exact
# legacy schema this compatibility refresh was designed to preserve.
schema_sql <- sprintf(
  "
  SELECT
    column_name,
    data_type,
    is_nullable
  FROM `%s.%s.INFORMATION_SCHEMA.COLUMNS`
  WHERE table_name = '%s'
  ORDER BY ordinal_position
  ",
  project_id,
  target_dataset,
  target_table_name
)

actual_target_schema <- download_query(schema_sql)
schema_matches <-
  identical(as.character(actual_target_schema$column_name), expected_target_schema$column_name) &&
  identical(as.character(actual_target_schema$data_type), expected_target_schema$data_type) &&
  identical(as.character(actual_target_schema$is_nullable), expected_target_schema$is_nullable)

if (!schema_matches) {
  cat("The destination schema does not match the protected 18-column contract.\n")
  cat("Expected schema:\n")
  print(expected_target_schema)
  cat("Live schema:\n")
  print(actual_target_schema)
  stop("No rows were changed because the destination schema check failed.", call. = FALSE)
}

source_checks_sql <- sprintf(
  "
  SELECT
    COUNT(*) AS source_record_count,
    COUNTIF(total_conversions != TRUNC(total_conversions)) AS fractional_conversion_record_count,
    COUNTIF(conversion_row_key IS NULL) AS missing_source_key_record_count,
    COUNT(*) - COUNT(DISTINCT conversion_row_key) AS duplicate_source_key_record_count,
    SUM(total_conversions) AS source_total_conversions,
    MIN(date) AS min_date,
    MAX(date) AS max_date
  FROM `%s`
  ",
  source_table_path
)

source_checks <- download_query(source_checks_sql)

if (source_checks$source_record_count[[1]] == 0) {
  stop("The direct CM360 source has zero rows; the destination was not changed.", call. = FALSE)
}

if (source_checks$fractional_conversion_record_count[[1]] != 0) {
  stop(
    "Direct CM360 contains fractional conversion values that cannot safely fit the protected INTEGER column; the destination was not changed.",
    call. = FALSE
  )
}

if (
  source_checks$missing_source_key_record_count[[1]] != 0 ||
    source_checks$duplicate_source_key_record_count[[1]] != 0
) {
  stop(
    "Direct CM360 source keys are missing or duplicated; the destination was not changed.",
    call. = FALSE
  )
}

# The transaction preserves the table object and its schema while replacing all
# rows. Legacy-only delivery and Google Sheet lineage columns stay NULL because
# direct CM360 does not provide honest equivalents for them.
refresh_sql <- sprintf(
  "
  BEGIN TRANSACTION;

  TRUNCATE TABLE `%s`;

  INSERT INTO `%s` (
    new_site,
    unnamed_column_2,
    date,
    campaign,
    campaign_id,
    site_cm360,
    package_roadblock,
    creative,
    activity,
    impressions,
    clicks,
    click_rate,
    total_conversions,
    data_refresh_date,
    loaded_at,
    source_sheet_id,
    source_sheet_tab,
    source_sheet_gid
  )
  SELECT
    CAST(site AS STRING) AS new_site,
    CAST(NULL AS STRING) AS unnamed_column_2,
    DATE(date) AS date,
    CAST(campaign AS STRING) AS campaign,
    CAST(NULL AS STRING) AS campaign_id,
    CAST(site AS STRING) AS site_cm360,
    CAST(package_roadblock AS STRING) AS package_roadblock,
    CAST(creative AS STRING) AS creative,
    CAST(activity AS STRING) AS activity,
    CAST(NULL AS INT64) AS impressions,
    CAST(NULL AS INT64) AS clicks,
    CAST(NULL AS FLOAT64) AS click_rate,
    CAST(total_conversions AS INT64) AS total_conversions,
    DATE(data_refresh_date) AS data_refresh_date,
    TIMESTAMP(staged_at) AS loaded_at,
    CAST(NULL AS STRING) AS source_sheet_id,
    CAST(NULL AS STRING) AS source_sheet_tab,
    CAST(NULL AS STRING) AS source_sheet_gid
  FROM `%s`;

  COMMIT TRANSACTION;
  ",
  target_table_path,
  target_table_path,
  source_table_path
)

run_query(refresh_sql)

description_sql <- sprintf(
  "
  ALTER TABLE `%s`
  SET OPTIONS (
    description = 'Legacy 18-column RTL conversion compatibility mirror refreshed from persistent direct CM360 history. Google Sheet reads are retired. Unavailable legacy delivery and Sheet-lineage fields remain NULL. Created by load_rtl_conv_report.R in util/data_loaders.'
  )
  ",
  target_table_path
)

run_query(description_sql)

verification_sql <- sprintf(
  "
  SELECT
    COUNT(*) AS target_record_count,
    COUNTIF(
      impressions IS NOT NULL
      OR clicks IS NOT NULL
      OR click_rate IS NOT NULL
      OR source_sheet_id IS NOT NULL
      OR source_sheet_tab IS NOT NULL
      OR source_sheet_gid IS NOT NULL
    ) AS unexpected_legacy_value_record_count,
    SUM(total_conversions) AS target_total_conversions,
    MIN(date) AS min_date,
    MAX(date) AS max_date,
    MAX(data_refresh_date) AS latest_refresh_date
  FROM `%s`
  ",
  target_table_path
)

verification_result <- download_query(verification_sql)

counts_match <-
  verification_result$target_record_count[[1]] == source_checks$source_record_count[[1]] &&
  verification_result$target_total_conversions[[1]] == source_checks$source_total_conversions[[1]]

if (!counts_match || verification_result$unexpected_legacy_value_record_count[[1]] != 0) {
  cat("The live refresh completed, but final verification did not match the source contract.\n")
  cat("Source checks:\n")
  print(source_checks)
  cat("Destination checks:\n")
  print(verification_result)
  stop("The compatibility refresh is not verified end to end.", call. = FALSE)
}

cat("Direct CM360 source checks:\n")
print(source_checks)
cat("\nDestination verification:\n")
print(verification_result)
cat("\nRTL conversion compatibility refresh finished successfully.\n")
