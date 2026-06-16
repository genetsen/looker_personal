#!/usr/bin/env Rscript

# RTL conversion report raw-sales loader
#
# Reads the `raw sales` tab from the Ritual reporting Google Sheet and replaces
# the BigQuery landing table with the latest sheet contents. The loader keeps
# the source values mostly raw, but it cleans headers into BigQuery-safe names
# and adds load metadata so daily runner refreshes are traceable.

suppressPackageStartupMessages({
  library(bigrquery)
  library(dplyr)
  library(googlesheets4)
  library(janitor)
  library(lubridate)
  library(readr)
  library(stringr)
})

project_id <- "looker-studio-pro-452620"
bq_dataset <- "landing"
bq_table_name <- "rtl_conv_report"

sheet_id <- "1tFxj3IZ_oBxUO9VwoDpd42CXGPWMJL5M1QKO_WIhOxI"
sheet_tab <- "raw sales"
sheet_gid <- "2031245039"
sheet_url <- paste0(
  "https://docs.google.com/spreadsheets/d/",
  sheet_id,
  "/edit?gid=",
  sheet_gid,
  "#gid=",
  sheet_gid
)

rename_blank_headers <- function(original_names, clean_names) {
  blank_positions <- which(is.na(original_names) | trimws(original_names) == "")
  if (!length(blank_positions)) {
    return(clean_names)
  }

  clean_names[blank_positions] <- paste0("unnamed_column_", blank_positions)
  janitor::make_clean_names(clean_names, case = "snake")
}

parse_number_field <- function(value) {
  readr::parse_number(as.character(value), na = c("", "NA", "N/A", "Invalid Number"))
}

parse_percent_field <- function(value) {
  raw_value <- as.character(value)
  parsed_value <- readr::parse_number(raw_value, na = c("", "NA", "N/A", "Invalid Number"))
  ifelse(str_detect(raw_value, "%"), parsed_value / 100, parsed_value)
}

cat("RTL Conversion Report loader started at", format(Sys.time()), "\n")
cat("Source sheet:", sheet_url, "\n")
cat("Source tab:", sheet_tab, "\n")
cat("Destination:", paste(project_id, bq_dataset, bq_table_name, sep = "."), "\n\n")

raw_sales <- googlesheets4::read_sheet(
  sheet_url,
  sheet = sheet_tab,
  col_types = "c",
  .name_repair = "minimal"
)

original_names <- names(raw_sales)
clean_names <- janitor::make_clean_names(original_names, case = "snake")
names(raw_sales) <- rename_blank_headers(original_names, clean_names)

empty_row <- raw_sales %>%
  mutate(across(everything(), ~ na_if(str_squish(as.character(.x)), ""))) %>%
  transmute(.is_empty = if_all(everything(), is.na)) %>%
  pull(.is_empty)

raw_sales <- raw_sales[!empty_row, , drop = FALSE]

rtl_conv_report <- raw_sales %>%
  mutate(
    date = lubridate::mdy(date),
    impressions = parse_number_field(impressions),
    clicks = parse_number_field(clicks),
    click_rate = parse_percent_field(click_rate),
    total_conversions = parse_number_field(total_conversions),
    data_refresh_date = Sys.Date(),
    loaded_at = Sys.time(),
    source_sheet_id = sheet_id,
    source_sheet_tab = sheet_tab,
    source_sheet_gid = sheet_gid
  )

if (!nrow(rtl_conv_report)) {
  stop("The raw sales tab returned zero non-empty rows; refusing to overwrite BigQuery with an empty table.", call. = FALSE)
}

bq_target <- bigrquery::bq_table(
  project = project_id,
  dataset = bq_dataset,
  table = bq_table_name
)

target_fields <- bigrquery::bq_fields(list(
  bigrquery::bq_field("new_site", "STRING"),
  bigrquery::bq_field("unnamed_column_2", "STRING"),
  bigrquery::bq_field("date", "DATE"),
  bigrquery::bq_field("campaign", "STRING"),
  bigrquery::bq_field("campaign_id", "STRING"),
  bigrquery::bq_field("site_cm360", "STRING"),
  bigrquery::bq_field("package_roadblock", "STRING"),
  bigrquery::bq_field("creative", "STRING"),
  bigrquery::bq_field("activity", "STRING"),
  bigrquery::bq_field("impressions", "INTEGER"),
  bigrquery::bq_field("clicks", "INTEGER"),
  bigrquery::bq_field("click_rate", "FLOAT"),
  bigrquery::bq_field("total_conversions", "INTEGER"),
  bigrquery::bq_field("data_refresh_date", "DATE"),
  bigrquery::bq_field("loaded_at", "TIMESTAMP"),
  bigrquery::bq_field("source_sheet_id", "STRING"),
  bigrquery::bq_field("source_sheet_tab", "STRING"),
  bigrquery::bq_field("source_sheet_gid", "STRING")
))

prepare_target_table <- function() {
  if (bigrquery::bq_table_exists(bq_target)) {
    bigrquery::bq_table_delete(bq_target)
  }

  bigrquery::bq_table_create(
    bq_target,
    fields = target_fields,
    description = paste(
      "Daily raw-sales mirror from the Ritual reporting Google Sheet.",
      "Created by load_rtl_conv_report.R in util/data_loaders."
    )
  )
}

write_table <- function() {
  prepare_target_table()
  bigrquery::bq_table_upload(
    bq_target,
    rtl_conv_report,
    write_disposition = "WRITE_APPEND"
  )
}

write_ok <- tryCatch(
  {
    write_table()
    TRUE
  },
  error = function(err) {
    error_message <- conditionMessage(err)
    schema_error <- grepl(
      "schema|column|field|type mismatch|incompatible",
      error_message,
      ignore.case = TRUE
    )

    if (!schema_error) {
      stop(err)
    }

    cat("Schema mismatch detected after table recreation attempt.\n")
    cat("Original BigQuery error:", error_message, "\n")
    stop(err)
  }
)

if (!isTRUE(write_ok)) {
  stop("BigQuery upload did not report success.", call. = FALSE)
}

verification_sql <- sprintf(
  "
  SELECT
    COUNT(*) AS row_count,
    MIN(date) AS min_date,
    MAX(date) AS max_date,
    MAX(data_refresh_date) AS latest_refresh_date
  FROM `%s.%s.%s`
  ",
  project_id,
  bq_dataset,
  bq_table_name
)

verification_result <- bigrquery::bq_table_download(
  bigrquery::bq_project_query(project_id, verification_sql)
)

cat("\nBigQuery verification:\n")
print(verification_result)
cat("\nRTL Conversion Report loader finished successfully.\n")
