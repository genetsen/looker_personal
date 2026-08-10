################################################################################
#### LOAD WP SEARCH DATA TEMPLATE FOR SHARED SOCIAL DELIVERY
################################################################################
# Purpose:
#   Read the WP delivery workbook, normalize daily ad records, and stage them
#   for WP-first shared-social delivery and validation. The source workbook
#   title is still APO Search Data Template.
# Inputs:
#   Google Sheet tabs `Report` and `Import` from the configured workbook.
# Output and safe usage:
#   Defaults to a read/preview run and a QA staging table. Set
#   WP_SEARCH_UPLOAD=TRUE to replace the configured output. Production writes
#   require WP_SEARCH_ALLOW_PRODUCTION=TRUE after approval.
################################################################################

suppressPackageStartupMessages({
  library(googlesheets4)
  library(googledrive)
  library(bigrquery)
  library(dplyr)
  library(janitor)
})


# * SECTION [1]: CONFIGURATION

  # Description: Keep the production boundary explicit for controlled refreshes.

  # ? Resolve this script's sibling normalization helper safely
    file_arg <- grep("^--file=", commandArgs(trailingOnly = FALSE), value = TRUE)
    script_path <- if (length(file_arg) > 0) sub("^--file=", "", file_arg[[1]]) else "wp/load_wp_search_data_template.R"
    script_dir <- dirname(normalizePath(script_path, mustWork = FALSE))
    source(file.path(script_dir, "wp_search_social_logic.R"))

  # ? Define source workbook and QA output controls
    PROJECT_ID <- Sys.getenv("WP_SEARCH_PROJECT", "looker-studio-pro-452620")
    DATASET_ID <- Sys.getenv("WP_SEARCH_DATASET", "repo_stg")
    TABLE_ID <- Sys.getenv("WP_SEARCH_TABLE", "stg__wp__search_data_template_daily_qa")
    SHEET_ID <- Sys.getenv("WP_SEARCH_SHEET_ID", "1fen46Ugxx12PYRzCDT88z8ENVcVl_MjlQhqkGDxZNbc")
    AUTH_EMAIL <- Sys.getenv("WP_SEARCH_AUTH_EMAIL", "gene.tsenter@giantspoon.com")
    UPLOAD_ENABLED <- tolower(Sys.getenv("WP_SEARCH_UPLOAD", "FALSE")) == "true"
    ALLOW_PRODUCTION <- tolower(Sys.getenv("WP_SEARCH_ALLOW_PRODUCTION", "FALSE")) == "true"
    IS_QA_TABLE <- grepl("_qa$", TABLE_ID)

  # ? Refuse accidental writes outside a QA table during the evidence phase
    if (UPLOAD_ENABLED && !IS_QA_TABLE && !ALLOW_PRODUCTION) {
      stop("Production upload blocked. Use a _qa table or explicitly set WP_SEARCH_ALLOW_PRODUCTION=TRUE after QA approval.")
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
      wp_classification_source = "STRING",
      wp_publication_status = "STRING",
      wp_creative_name = "STRING",
      wp_creative_img = "STRING",
      wp_source_sheet_url = "STRING",
      wp_loaded_at = "TIMESTAMP",
      wp_row_key = "STRING"
    )
    upload_fields <- bq_fields(unname(Map(
      function(field_name, field_type) bq_field(field_name, field_type),
      names(STAGING_TYPES),
      unname(STAGING_TYPES)
    )))


# * SECTION [2]: READ AND NORMALIZE WORKBOOK

  # Description: Use the visible report for facts and hidden import only for IDs.

  # ? Authenticate to the workbook using the established cached Sheets path
    gspoon_ok <- tryCatch({
      source("/Users/eugenetsenter/.config/gspoon_google_auth/google_auth.R")
      !is.null(gspoon_google_auth(
        packages = c("googledrive", "googlesheets4", "bigrquery"),
        fallback = TRUE
      ))
    }, error = function(e) {
      message("gspoon-auth fallback: ", conditionMessage(e))
      FALSE
    })
    if (!gspoon_ok) {
      gs4_auth(email = AUTH_EMAIL)
      drive_auth(email = AUTH_EMAIL)
    }
    sheet_url <- paste0("https://docs.google.com/spreadsheets/d/", SHEET_ID)
    loaded_at <- Sys.time()

  # ? Read source tabs and normalize header names for the contract helper
    report_rows <- read_sheet(SHEET_ID, sheet = "Report", col_names = TRUE, col_types = "c", .name_repair = "unique") %>%
      clean_names()
    import_rows <- tryCatch(
      read_sheet(SHEET_ID, sheet = "Import", col_names = TRUE, col_types = "c", .name_repair = "unique") %>%
        clean_names(),
      error = function(e) {
        message("Import tab unavailable; using deterministic ad-group ID fallback. Detail: ", conditionMessage(e))
        data.frame()
      }
    )
    normalized_rows <- normalize_wp_report_rows(report_rows, import_rows, sheet_url, loaded_at)

  # ? Print compact source quality evidence before any optional write
    quality_summary <- normalized_rows %>%
      group_by(platform, media_name, wp_publication_status) %>%
      summarise(
        row_count = n(),
        spend = sum(spend, na.rm = TRUE),
        impressions = sum(impressions, na.rm = TRUE),
        clicks = sum(clicks, na.rm = TRUE),
        video_views = sum(video_view, na.rm = TRUE),
        creative_images = sum(!is.na(wp_creative_img), na.rm = TRUE),
        .groups = "drop"
      )
    cat("\nWP SEARCH DATA TEMPLATE NORMALIZATION\n")
    cat("Rows prepared:", nrow(normalized_rows), "\n")
    print(quality_summary)


# * SECTION [3]: OPTIONAL CONTROLLED UPLOAD

  # Description: Publish only when explicitly requested for QA or production.

  # ? Upload the normalized rows to the selected BigQuery table when enabled
    if (UPLOAD_ENABLED) {
      if (!gspoon_ok) {
        bq_auth(email = AUTH_EMAIL)
      }
      target <- bq_table(PROJECT_ID, DATASET_ID, TABLE_ID)
      bq_table_upload(
        target,
        normalized_rows,
        fields = upload_fields,
        write_disposition = "WRITE_TRUNCATE"
      )
      table_description <- if (IS_QA_TABLE) {
        paste(
          "QA-only normalized source-row input loaded from the WP delivery workbook",
          "by wp/load_wp_search_data_template.R. Campaign-grain records with",
          "cross-campaign ad-ID conflicts are marked pending source-owner review;",
          "production shared staging is unchanged. Safe to delete after review",
          "by the master data model owner."
        )
      } else {
        paste(
          "Production normalized source-row input loaded from the WP delivery workbook",
          "by wp/load_wp_search_data_template.R. Campaign-grain records",
          "with cross-campaign ad-ID conflicts are included pending source-owner",
          "review and retain their publication-status provenance. Refresh is",
          "controlled manually until the source-owner rule is confirmed."
        )
      }
      description_sql <- sprintf(
        "ALTER TABLE `%s.%s.%s` SET OPTIONS (description = '%s')",
        PROJECT_ID,
        DATASET_ID,
        TABLE_ID,
        table_description
      )
      bq_project_query(PROJECT_ID, description_sql, quiet = TRUE)
      cat("\nUploaded normalized WP rows to ", PROJECT_ID, ".", DATASET_ID, ".", TABLE_ID, "\n", sep = "")
    } else {
      cat("\nPreview only: set WP_SEARCH_UPLOAD=TRUE to write the configured staging table.\n")
    }
