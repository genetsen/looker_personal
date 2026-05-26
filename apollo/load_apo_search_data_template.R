################################################################################
#### LOAD APO SEARCH DATA TEMPLATE FOR SOCIAL QA
################################################################################
# Purpose:
#   Read the APO Search Data Template, normalize daily ad records, and stage
#   them for APO-first shared-social merge validation.
# Inputs:
#   Google Sheet tabs `Report` and `Import` from the configured workbook.
# Output and safe usage:
#   Defaults to a read/preview run. Set APO_SEARCH_UPLOAD=TRUE to replace only
#   the configured QA staging table. Production writes require the additional
#   APO_SEARCH_ALLOW_PRODUCTION=TRUE opt-in after QA review.
################################################################################

suppressPackageStartupMessages({
  library(googlesheets4)
  library(googledrive)
  library(bigrquery)
  library(dplyr)
  library(janitor)
})


# * SECTION [1]: CONFIGURATION

  # Description: Keep the production boundary explicit while QA is underway.

  # ? Resolve this script's sibling normalization helper safely
    file_arg <- grep("^--file=", commandArgs(trailingOnly = FALSE), value = TRUE)
    script_path <- if (length(file_arg) > 0) sub("^--file=", "", file_arg[[1]]) else "apollo/load_apo_search_data_template.R"
    script_dir <- dirname(normalizePath(script_path, mustWork = FALSE))
    source(file.path(script_dir, "apo_search_social_logic.R"))

  # ? Define source workbook and QA output controls
    PROJECT_ID <- Sys.getenv("APO_SEARCH_PROJECT", "looker-studio-pro-452620")
    DATASET_ID <- Sys.getenv("APO_SEARCH_DATASET", "repo_stg")
    TABLE_ID <- Sys.getenv("APO_SEARCH_TABLE", "stg__apo__search_data_template_daily_qa")
    SHEET_ID <- Sys.getenv("APO_SEARCH_SHEET_ID", "1fen46Ugxx12PYRzCDT88z8ENVcVl_MjlQhqkGDxZNbc")
    AUTH_EMAIL <- Sys.getenv("APO_SEARCH_AUTH_EMAIL", "gene.tsenter@giantspoon.com")
    UPLOAD_ENABLED <- tolower(Sys.getenv("APO_SEARCH_UPLOAD", "FALSE")) == "true"
    ALLOW_PRODUCTION <- tolower(Sys.getenv("APO_SEARCH_ALLOW_PRODUCTION", "FALSE")) == "true"

  # ? Refuse accidental writes outside a QA table during the evidence phase
    if (UPLOAD_ENABLED && !grepl("_qa$", TABLE_ID) && !ALLOW_PRODUCTION) {
      stop("Production upload blocked. Use a _qa table or explicitly set APO_SEARCH_ALLOW_PRODUCTION=TRUE after QA approval.")
    }

  # ? Declare the shared daily-ad contract instead of inferring IDs as numbers
    STAGING_TYPES <- c(
      source_relation = "STRING",
      date_day = "DATE",
      platform = "STRING",
      account_id = "STRING",
      account_name = "STRING",
      campaign_id = "STRING",
      campaign_name = "STRING",
      ad_group_id = "STRING",
      ad_group_name = "STRING",
      ad_id = "STRING",
      ad_name = "STRING",
      clicks = "INTEGER",
      impressions = "INTEGER",
      spend = "FLOAT",
      conversions = "FLOAT",
      conversions_value = "FLOAT",
      video_play = "FLOAT",
      video_view = "FLOAT",
      video_views_p_25 = "FLOAT",
      video_views_p_50 = "FLOAT",
      video_views_p_75 = "FLOAT",
      video_views_p_100 = "FLOAT",
      hookrate_num = "FLOAT",
      video_flag = "STRING",
      channel = "STRING",
      channel_group = "STRING",
      media_name = "STRING",
      ADIF_channel = "STRING",
      apo_classification_source = "STRING",
      apo_publication_status = "STRING",
      apo_creative_name = "STRING",
      apo_creative_box_link = "STRING",
      apo_source_sheet_url = "STRING",
      apo_loaded_at = "TIMESTAMP",
      apo_row_key = "STRING"
    )
    upload_fields <- bq_fields(unname(Map(
      function(field_name, field_type) bq_field(field_name, field_type),
      names(STAGING_TYPES),
      unname(STAGING_TYPES)
    )))


# * SECTION [2]: READ AND NORMALIZE WORKBOOK

  # Description: Use the visible report for facts and hidden import only for IDs.

  # ? Authenticate to the workbook using the established cached Sheets path
    gs4_auth(email = AUTH_EMAIL)
    drive_auth(email = AUTH_EMAIL)
    sheet_url <- paste0("https://docs.google.com/spreadsheets/d/", SHEET_ID)
    loaded_at <- Sys.time()

  # ? Read source tabs and normalize header names for the contract helper
    report_rows <- read_sheet(SHEET_ID, sheet = "Report", col_names = TRUE, .name_repair = "unique") %>%
      clean_names()
    import_rows <- read_sheet(SHEET_ID, sheet = "Import", col_names = TRUE, .name_repair = "unique") %>%
      clean_names()
    normalized_rows <- normalize_apo_report_rows(report_rows, import_rows, sheet_url, loaded_at)

  # ? Print compact source quality evidence before any optional write
    quality_summary <- normalized_rows %>%
      group_by(platform, media_name, apo_publication_status) %>%
      summarise(
        row_count = n(),
        spend = sum(spend, na.rm = TRUE),
        impressions = sum(impressions, na.rm = TRUE),
        clicks = sum(clicks, na.rm = TRUE),
        creative_links = sum(!is.na(apo_creative_box_link) & apo_creative_box_link != "-", na.rm = TRUE),
        .groups = "drop"
      )
    cat("\nAPO SEARCH DATA TEMPLATE NORMALIZATION\n")
    cat("Rows prepared:", nrow(normalized_rows), "\n")
    print(quality_summary)


# * SECTION [3]: OPTIONAL QA UPLOAD

  # Description: Publish only when explicitly requested for QA evidence.

  # ? Upload the normalized rows to the selected BigQuery table when enabled
    if (UPLOAD_ENABLED) {
      bq_auth(email = AUTH_EMAIL)
      target <- bq_table(PROJECT_ID, DATASET_ID, TABLE_ID)
      bq_table_upload(
        target,
        normalized_rows,
        fields = upload_fields,
        write_disposition = "WRITE_TRUNCATE"
      )
      description_sql <- sprintf(
        paste(
          "ALTER TABLE `%s.%s.%s` SET OPTIONS (description =",
          "'QA-only normalized source-row input loaded from the APO Search Data Template",
          "by apollo/load_apo_search_data_template.R. Rows with conflicting daily-ad",
          "keys are retained with an exclusion status for shared-social review;",
          "production shared staging is unchanged.",
          "Safe to delete after approval or rejection of the APO promotion candidate",
          "by the master data model owner.')"
        ),
        PROJECT_ID,
        DATASET_ID,
        TABLE_ID
      )
      bq_project_query(PROJECT_ID, description_sql, quiet = TRUE)
      cat("\nUploaded normalized APO rows to ", PROJECT_ID, ".", DATASET_ID, ".", TABLE_ID, "\n", sep = "")
    } else {
      cat("\nPreview only: set APO_SEARCH_UPLOAD=TRUE to write the QA staging table.\n")
    }
