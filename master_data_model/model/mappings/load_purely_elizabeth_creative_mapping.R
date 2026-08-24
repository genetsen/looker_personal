#!/usr/bin/env Rscript
################################################################################
#### LOAD PURELY ELIZABETH CREATIVE NAME MAPPING
################################################################################
# Purpose:
#   Read the user-maintained Creative Mapping tab without changing it, validate
#   active friendly-name mappings, and optionally replace the canonical
#   master_stg.creative_mapping table.
# Safe usage:
#   Preview is the default. A production write requires
#   PE_CREATIVE_MAPPING_UPLOAD=TRUE. Empty replacements additionally require
#   PE_CREATIVE_MAPPING_ALLOW_EMPTY=TRUE.
################################################################################

suppressPackageStartupMessages({
  library(googlesheets4)
  library(bigrquery)
})

script_argument <- grep("^--file=", commandArgs(trailingOnly = FALSE), value = TRUE)
script_path <- sub("^--file=", "", script_argument[[1L]])
script_directory <- dirname(normalizePath(script_path))
source(file.path(script_directory, "creative_mapping_logic.R"))


# * SECTION [1]: CONFIGURATION

  # Description: Keep the current PE-specific adapter explicit and preview-first.

  PROJECT_ID <- Sys.getenv("PE_CREATIVE_MAPPING_PROJECT", "looker-studio-pro-452620")
  DATASET_ID <- Sys.getenv("PE_CREATIVE_MAPPING_DATASET", "master_stg")
  TABLE_ID <- Sys.getenv("PE_CREATIVE_MAPPING_TABLE", "creative_mapping")
  SHEET_ID <- Sys.getenv(
    "PE_CREATIVE_MAPPING_SHEET_ID",
    "15LXvL0_DRY0GBDGN_omx2p5Ast4BCptelE463bvj8Bk"
  )
  SHEET_TAB <- Sys.getenv("PE_CREATIVE_MAPPING_SHEET_TAB", "Creative Mapping")
  AUTH_EMAIL <- Sys.getenv("PE_CREATIVE_MAPPING_AUTH_EMAIL", "gene.tsenter@giantspoon.com")
  UPLOAD_ENABLED <- identical(tolower(Sys.getenv("PE_CREATIVE_MAPPING_UPLOAD", "FALSE")), "true")
  ALLOW_EMPTY <- identical(tolower(Sys.getenv("PE_CREATIVE_MAPPING_ALLOW_EMPTY", "FALSE")), "true")


# * SECTION [2]: READ AND VALIDATE

  # Description: Read only the six existing columns and translate PE rows to the canonical contract.

  gs4_auth(email = AUTH_EMAIL)
  source_values <- read_sheet(
    SHEET_ID,
    sheet = SHEET_TAB,
    range = "A:F",
    col_types = "c",
    .name_repair = "minimal"
  )

  required_headers <- c(
    "Supplier Code", "Initiative", "Creative Name", "Friendly Creative Name"
  )
  missing_headers <- setdiff(required_headers, names(source_values))
  if (length(missing_headers) > 0L) {
    stop(
      paste("Creative Mapping tab is missing headers:", paste(missing_headers, collapse = ", ")),
      call. = FALSE
    )
  }

  canonical_source_rows <- data.frame(
    supplier_code = source_values[["Supplier Code"]],
    initiative = source_values[["Initiative"]],
    source_creative_name = source_values[["Creative Name"]],
    mapped_creative_name = source_values[["Friendly Creative Name"]],
    source_row_number = seq_len(nrow(source_values)) + 1L,
    stringsAsFactors = FALSE
  )

  mapping_rows <- prepare_creative_mapping_rows(
    canonical_source_rows,
    advertiser = "Purely Elizabeth",
    source_sheet_id = SHEET_ID,
    source_sheet_tab = SHEET_TAB,
    default_match_scope = "tactic_creative",
    loaded_at = Sys.time()
  )

  if (nrow(mapping_rows) == 0L && !ALLOW_EMPTY) {
    stop(
      "No active friendly-name mappings were found. Set PE_CREATIVE_MAPPING_ALLOW_EMPTY=TRUE only to intentionally clear production.",
      call. = FALSE
    )
  }

  cat("Creative mapping source rows:", nrow(source_values), "\n")
  cat("Active validated mappings:", nrow(mapping_rows), "\n")
  print(mapping_rows[, c(
    "advertiser", "match_scope", "initiative", "source_creative_name",
    "mapped_creative_name", "supplier_code_context", "source_row_number"
  ), drop = FALSE])


# * SECTION [3]: OPTIONAL PRODUCTION REPLACEMENT

  # Description: Replace the mapping table only after the complete Sheet snapshot passes validation.

  if (!UPLOAD_ENABLED) {
    cat("PREVIEW ONLY: BigQuery was not changed.\n")
    quit(save = "no", status = 0L)
  }

  bq_auth(email = AUTH_EMAIL)
  target_table <- bq_table(PROJECT_ID, DATASET_ID, TABLE_ID)
  bq_table_upload(
    target_table,
    mapping_rows,
    write_disposition = "WRITE_TRUNCATE"
  )

  description_sql <- sprintf(
    paste0(
      "ALTER TABLE `%s.%s.%s` SET OPTIONS (description = ",
      "'Canonical creative-name mapping for data_model_v3. One row per advertiser and approved match scope/key; ",
      "tactic_creative overrides creative. Supplier code is review context only. ",
      "Loaded read-only from the source Sheet by model/mappings/load_purely_elizabeth_creative_mapping.R.')"
    ),
    PROJECT_ID,
    DATASET_ID,
    TABLE_ID
  )
  bq_project_query(PROJECT_ID, description_sql, quiet = TRUE)

  verification_sql <- sprintf(
    paste0(
      "SELECT COUNT(*) AS record_count, ",
      "COUNT(DISTINCT CONCAT(LOWER(TRIM(advertiser)), '|', match_scope, '|', ",
      "COALESCE(LOWER(TRIM(initiative)), '<null>'), '|', ",
      "COALESCE(LOWER(TRIM(source_creative_name)), '<null>'))) AS distinct_key_count ",
      "FROM `%s.%s.%s`"
    ),
    PROJECT_ID,
    DATASET_ID,
    TABLE_ID
  )
  verification_job <- bq_project_query(PROJECT_ID, verification_sql, quiet = TRUE)
  verification <- bq_table_download(verification_job, quiet = TRUE)
  if (
    nrow(verification) != 1L ||
      verification$record_count[[1L]] != nrow(mapping_rows) ||
      verification$distinct_key_count[[1L]] != nrow(mapping_rows)
  ) {
    stop("BigQuery creative mapping verification did not match the validated source snapshot.", call. = FALSE)
  }

  cat("PRODUCTION LOAD COMPLETE:", PROJECT_ID, DATASET_ID, TABLE_ID, "\n")
  cat("Verified mapping rows:", verification$record_count[[1L]], "\n")
