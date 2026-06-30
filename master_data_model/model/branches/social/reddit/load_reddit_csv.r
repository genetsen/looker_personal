#!/usr/bin/env Rscript
# load_reddit_csv.r
# Temporary script to clean and upload Reddit Ads daily data to BigQuery using clean snake_case names

library(bigrquery)
library(readr)
library(dplyr)
library(stringr)
library(lubridate)
library(janitor)

# Enable BigQuery authentication via local gcloud credentials
bq_auth()

csv_path <- "master_data_model/tmp/Reddit-Olipop-scheaduled (Copy) (2).csv"

if (!file.exists(csv_path)) {
  stop(paste("CSV file not found at path:", csv_path))
}

message("Reading raw CSV from: ", csv_path)
df <- read_csv(csv_path, show_col_types = FALSE)
message("Rows read: ", nrow(df), ", Columns read: ", ncol(df))

# 1. Clean up column names to snake_case using janitor to avoid invalid JSON keys in BigQuery loading
message("Cleaning column names using janitor...")
df <- df %>% clean_names()

# 2. Skip / Drop the cpc_usd column as requested by the user
if ("cpc_usd" %in% colnames(df)) {
  message("Dropping 'cpc_usd' column as requested.")
  df <- df %>% select(-cpc_usd)
} else {
  message("Warning: 'cpc_usd' column not found in cleaned CSV columns, proceeding.")
}

# 3. Clean up leading single quotes from IDs and other character columns
# Also handle "NA" strings to convert them to actual R NA
df <- df %>% mutate(across(where(is.character), ~ {
  val <- str_replace_all(.x, "^'", "")
  ifelse(val == "NA", NA_character_, val)
}))

# 4. Correct column data types for BigQuery ingestion
# Convert date (m/d/y format) to Date object
df$date <- mdy(df$date)

# Convert timestamp columns to POSIXct
timestamp_cols <- c("start_time", "end_time", "start_time_campaign", "end_time_campaign")
for (col in timestamp_cols) {
  if (col %in% colnames(df)) {
    # Convert to character first in case of logical/empty columns
    df[[col]] <- as.character(df[[col]])
    df[[col]] <- ymd_hms(df[[col]], quiet = TRUE)
  }
}

# 5. Drop the old table (to completely overwrite schema) and upload new schema + data
project <- "looker-studio-pro-452620"
dataset <- "landing"
table <- "reddit-ads-email"

table_ref <- bq_table(project, dataset, table)

if (bq_table_exists(table_ref)) {
  message("Table exists. Deleting table looker-studio-pro-452620.landing.reddit-ads-email to overwrite its schema...")
  bq_table_delete(table_ref)
  message("Table deleted successfully.")
}

message("Creating table and uploading data to BigQuery...")
# Since table is deleted, bq_table_upload will create the table fresh with the new dataframe schema
bq_table_upload(
  x = table_ref,
  values = df,
  write_disposition = "WRITE_TRUNCATE"
)

message("Success! Data successfully loaded into BigQuery table with clean snake_case columns.")
