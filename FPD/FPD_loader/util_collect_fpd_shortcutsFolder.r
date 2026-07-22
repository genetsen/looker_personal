################################################################################
#### ! LATEST VERSION OF THIS SCRIPT
#### FPD INGESTION - VERSION 4 ALL FPD FILES from shortcuts folder...
#### /Users/eugenetsenter/Library/CloudStorage/GoogleDrive-gene.tsenter@giantspoon.com/.shortcut-targets-by-id/0B0U23i7iN3kZaHJ4NzMyeHp5NW8/Giant Spoon - SHARED (USE THIS ONE)/4. Department Folders/Analytics
#### https://drive.google.com/drive/folders/1d--Bc554eBaRCr8blt1LnUYiOMHQe7jF?usp=drive_link
################################################################################
# What changed: this shortcut-aware loader now resolves Drive shortcuts, supports
# a one-run pattern override, and stages BigQuery writes before replacing prod.
# How to undo: restore the previous script version from Git if the broader
# shortcut discovery or staged BigQuery replace behavior needs to be rolled back.
# Purpose: Rewrite of util_collect_fpd.r as a straightforward, linear script
# with minimal abstractions. Build step-by-step through phases:
# 1. Discover files
# 2. Detect header rows
# 3. Collect column metadata
# 4. Combine all data
#
# Output: Checkpoint CSVs after each phase for inspection
################################################################################

#### LOAD LIBRARIES ####
library(googledrive)
library(googlesheets4)
library(bigrquery)
library(dplyr)
library(stringr)
library(readr)
library(lubridate)
library(janitor)

cat ("\n-----------\n First party data pipeline started at:", format(Sys.time(), "%Y-%m-%d %H:%M:%S"), "\n-----------\n")

#### CLI ARGUMENTS ####
# Optional usage:
#   Rscript util_collect_fpd_shortcutsFolder.r --pattern="| Partner Data"
#   Rscript util_collect_fpd_shortcutsFolder.r --pattern "| Partner Data"
cli_args <- commandArgs(trailingOnly = TRUE)

if ("--help" %in% cli_args || "-h" %in% cli_args) {
  cat("\nUsage:\n")
  cat("  Rscript util_collect_fpd_shortcutsFolder.r [--pattern <text>] [--no-file-cache]\n\n")
  cat("Options:\n")
  cat("  --pattern <text>   Override sheet-name match pattern used in Phase 1\n")
  cat("  --no-file-cache    Disable default per-sheet cache reuse and force fresh Google Sheets reads\n")
  cat("  -h, --help         Show this help and exit\n\n")
  quit(save = "no", status = 0)
}

get_flag_value <- function(args, flag_name) {
  long_flag <- paste0("--", flag_name)
  eq_prefix <- paste0(long_flag, "=")

  eq_idx <- which(startsWith(args, eq_prefix))
  if (length(eq_idx) > 0) {
    return(sub(eq_prefix, "", args[eq_idx[length(eq_idx)]], fixed = TRUE))
  }

  spaced_idx <- which(args == long_flag)
  if (length(spaced_idx) > 0) {
    idx <- spaced_idx[length(spaced_idx)]
    if (idx < length(args)) {
      return(args[idx + 1])
    }
  }

  NULL
}

has_flag <- function(args, flag_name) {
  long_flag <- paste0("--", flag_name)
  any(args == long_flag | startsWith(args, paste0(long_flag, "=")))
}

#### CONFIGURATION ####
gdrive_folder_id <- "1d--Bc554eBaRCr8blt1LnUYiOMHQe7jF"
  # Analytics department folder (recursive scan, filtered by pattern):
  # https://drive.google.com/drive/folders/1d--Bc554eBaRCr8blt1LnUYiOMHQe7jF
pattern <- "| Partner Data"
  pattern_override <- get_flag_value(cli_args, "pattern")
    if (!is.null(pattern_override)) {
      pattern <- pattern_override
      cat("CLI override applied: pattern =", pattern, "\n")
    }
use_file_cache_reads <- !has_flag(cli_args, "no-file-cache")
use_file_cache_writes <- TRUE
  if (use_file_cache_reads) {
    cat("Per-file cache reuse is enabled\n")
  } else {
    cat("CLI override applied: per-file cache reads disabled; fresh results will still refresh cache files\n")
  }
output_dir <- "/Users/eugenetsenter/Looker_clonedRepo/looker_personal/FPD/FPD_loader/output"
  if (!dir.exists(output_dir)) dir.create(output_dir, recursive = TRUE)
cache_dir <- file.path(output_dir, "sheet_cache")
  if (!dir.exists(cache_dir)) dir.create(cache_dir, recursive = TRUE)
use_saved_phases <- FALSE
  # Default to a full fresh run. Turn this on only while debugging a saved checkpoint.
  # Set the phase you are actively working on (1..7). Saved results will be used for other phases.
  current_phase <- 1
#

known_kpi_metrics <- c(
  # Known KPI metric fields (normalized) to treat as numeric + split across days
  # NOTE: keep this list tight to avoid accidentally treating dimensions as metrics.
  "spend",
  "impressions",
  "sends",
  "opens",
  "views",
  "completed_views",
  "engagements",
  "clicks",
  "pageviews",
  "conversions",
  "benchmark_metric",
  "benchmark"
)

# Preserve these columns as numeric when checkpoint CSVs are read back in.
# Some sparse rate fields can be mis-inferred as logical by readr when most rows
# are blank, which later breaks the BigQuery sync schema.
checkpoint_numeric_fields <- c(known_kpi_metrics, "ctr", "ctr_vcr")

# Per-client week configuration: which day the reporting week starts on.
# Used to fill in missing `week` values in the final output.
# Clients are matched case-insensitively against the `client` column.
client_week_config <- data.frame(
  # Per-client week configuration: which day the reporting week starts on.
  # Used to fill in missing `week` values in the final output.
  # Clients are matched case-insensitively against the `client` column.

  client     = c("mass", "oli", "adif", "apollo"),
  week_start = c("Sun",  "Sun", "Sun",  "Mon"),
    stringsAsFactors = FALSE
)
default_week_start <- "Sun"

# Mapping from day abbreviation to lubridate floor_date week_start number
# (lubridate uses: 1=Mon, 2=Tue, ... 7=Sun)
day_to_wstart <- c(Mon = 1, Tue = 2, Wed = 3, Thu = 4, Fri = 5, Sat = 6, Sun = 7)

# Predefine checkpoint paths so they can be reused when loading saved results
  phase1_output <- file.path(output_dir, "phase1_discovered_files.csv")
  phase2_output <- file.path(output_dir, "phase2_header_detection.csv")
  phase3_output <- file.path(output_dir, "phase3_raw_headers.csv")
  phase4_output <- file.path(output_dir, "phase4_normalization_mapping.csv")
  phase5_output <- file.path(output_dir, "phase5_combined_master_data.csv")
  phase6_output <- file.path(output_dir, "phase6_cleaned_master_data.csv")
  phase7_output <- file.path(output_dir, "phase7_daily_master_data.csv")
  phase6_filter_audit_output <- file.path(output_dir, "phase6_filter_audit.csv")
  phase7_validation_output <- file.path(output_dir, "phase7_validation_table.csv")
#
# Keep validation results available for final end-of-run reporting
validation_table <- data.frame()
validation_diff_cols <- character(0)

project_id <- "looker-studio-pro-452620"
dataset_id <- "landing"
prod_table <- "fpd_data_ranged_shortcutsFolder"
staging_table <- paste0(prod_table, "__staging")
creative_refresh_title_prefix <- "APO | Partner Data Collection"
creative_source_fallback_header <- "Creative img PATH"
creative_repo_owner <- "genetsen"
creative_repo_name <- "apo-db-creat"
creative_repo_asset_root <- "assets/apo"
creative_repo_url <- paste0("https://github.com/", creative_repo_owner, "/", creative_repo_name, ".git")
creative_repo_state <- new.env(parent = emptyenv())
creative_repo_state$repo_dir <- NULL

map_bq_type <- function(x) {
  if (inherits(x, "POSIXct") || inherits(x, "POSIXt")) return("TIMESTAMP")
  if (inherits(x, "Date")) return("DATE")
  if (is.integer(x)) return("INT64")
  if (is.numeric(x)) return("FLOAT64")
  if (is.logical(x)) return("BOOL")
  "STRING"
}

build_bq_fields <- function(df) {
  lapply(names(df), function(nm) {
    list(name = nm, type = map_bq_type(df[[nm]]), mode = "NULLABLE")
  })
}

coerce_checkpoint_numeric_fields <- function(df) {
  numeric_cols <- intersect(checkpoint_numeric_fields, names(df))

  if (length(numeric_cols) == 0) {
    return(df)
  }

  for (col_name in numeric_cols) {
    df[[col_name]] <- suppressWarnings(as.numeric(as.character(df[[col_name]])))
  }

  df
}

drop_blank_generated_columns <- function(df) {
  if (ncol(df) == 0) return(df)

  generated_cols <- names(df)[
    grepl("^x\\d+$", names(df), ignore.case = TRUE) |
      grepl("^\\.\\.\\.\\d+$", names(df))
  ]
  if (length(generated_cols) == 0) return(df)

  keep_cols <- vapply(
    generated_cols,
    function(col_name) {
      values <- df[[col_name]]
      any(!is.na(values) & trimws(as.character(values)) != "")
    },
    logical(1)
  )

  drop_cols <- generated_cols[!keep_cols]
  if (length(drop_cols) == 0) return(df)

  df %>% select(-all_of(drop_cols))
}

sql_quote_string <- function(x) {
  if (is.na(x)) return("NULL")
  paste0("'", gsub("'", "''", as.character(x), fixed = TRUE), "'")
}

normalize_bq_sql_type <- function(type_name) {
  type_upper <- toupper(as.character(type_name))
  if (type_upper == "FLOAT") return("FLOAT64")
  if (type_upper == "INTEGER") return("INT64")
  if (type_upper == "BOOLEAN") return("BOOL")
  type_upper
}

apo_clean_text <- function(x) {
  if (length(x) == 0 || is.null(x) || all(is.na(x))) return("")
  trimws(as.character(x[[1]]))
}

apo_is_target_sheet <- function(sheet_name) {
  !is.na(sheet_name) && str_starts(as.character(sheet_name), fixed(creative_refresh_title_prefix))
}

apo_col_to_a1 <- function(col_index) {
  stopifnot(col_index >= 1)
  out <- ""
  n <- as.integer(col_index)
  while (n > 0) {
    rem <- (n - 1) %% 26
    out <- paste0(intToUtf8(65 + rem), out)
    n <- (n - 1) %/% 26
  }
  out
}

apo_raw_github_url <- function(rel_path) {
  encoded_parts <- vapply(strsplit(rel_path, "/", fixed = TRUE)[[1]], URLencode, character(1), reserved = TRUE)
  paste0(
    "https://raw.githubusercontent.com/",
    creative_repo_owner,
    "/",
    creative_repo_name,
    "/main/",
    paste(encoded_parts, collapse = "/")
  )
}

apo_git_run <- function(args) {
  out <- system2("git", shQuote(args), stdout = TRUE, stderr = TRUE)
  status <- attr(out, "status")
  if (!is.null(status) && status != 0) {
    stop(paste(c(out), collapse = "\n"))
  }
  out
}

apo_ensure_repo_clone <- function() {
  repo_dir <- creative_repo_state$repo_dir
  if (!is.null(repo_dir) && dir.exists(file.path(repo_dir, ".git"))) {
    return(repo_dir)
  }

  repo_dir <- file.path(tempdir(), paste0("apo-db-creat-", Sys.getpid()))
  if (dir.exists(repo_dir)) {
    unlink(repo_dir, recursive = TRUE, force = TRUE)
  }
  apo_git_run(c("clone", creative_repo_url, repo_dir))
  creative_repo_state$repo_dir <- repo_dir
  repo_dir
}

apo_normalize_local_path <- function(raw_path) {
  cleaned <- apo_clean_text(raw_path)
  cleaned <- sub("'$", "", cleaned)
  if (cleaned == "") return(NULL)

  candidates <- cleaned
  if (grepl("^/Users/[^/]+/", cleaned)) {
    remapped <- sub("^/Users/[^/]+/", "/Users/eugenetsenter/", cleaned)
    candidates <- unique(c(candidates, remapped))
  }

  for (candidate in candidates) {
    candidate_path <- path.expand(candidate)
    if (file.exists(candidate_path) && !dir.exists(candidate_path)) {
      return(normalizePath(candidate_path, winslash = "/", mustWork = TRUE))
    }
  }

  NULL
}

apo_sheet_needs_refresh <- function(raw_df, sheet_name) {
  if (!apo_is_target_sheet(sheet_name) || !any(c("Final_img_path", creative_source_fallback_header) %in% names(raw_df))) {
    return(FALSE)
  }

  source_vals <- if ("Final_img_path" %in% names(raw_df)) trimws(as.character(raw_df[["Final_img_path"]])) else rep("", nrow(raw_df))
  source_vals[is.na(source_vals)] <- ""
  if (creative_source_fallback_header %in% names(raw_df)) {
    fallback_vals <- trimws(as.character(raw_df[[creative_source_fallback_header]]))
    fallback_vals[is.na(fallback_vals)] <- ""
    source_vals[source_vals == "" & fallback_vals != ""] <- fallback_vals[source_vals == "" & fallback_vals != ""]
  }
  source_present <- !is.na(source_vals) & source_vals != ""
  if (!any(source_present)) {
    return(FALSE)
  }

  if (!"creative_git_link" %in% names(raw_df)) {
    return(TRUE)
  }

  link_vals <- trimws(as.character(raw_df[["creative_git_link"]]))
  any(is.na(link_vals[source_present]) | link_vals[source_present] == "")
}

apo_range_write_matrix <- function(sheet_id, cell_range, values_matrix) {
  write_df <- as.data.frame(values_matrix, stringsAsFactors = FALSE)
  suppressMessages(
    googlesheets4::range_write(
      ss = sheet_id,
      sheet = "data",
      range = cell_range,
      data = write_df,
      col_names = FALSE,
      reformat = FALSE
    )
  )
}

apo_commit_repo_changes <- function(repo_dir, rel_paths) {
  if (length(rel_paths) == 0) return(invisible(FALSE))

  apo_git_run(c("-C", repo_dir, "add", rel_paths))
  status_lines <- apo_git_run(c("-C", repo_dir, "status", "--short"))
  if (length(status_lines) == 0) {
    return(invisible(FALSE))
  }

  apo_git_run(c("-C", repo_dir, "commit", "-m", "Add APO creative image assets rerun"))
  apo_git_run(c("-C", repo_dir, "push"))
  invisible(TRUE)
}

refresh_apo_creative_links <- function(sheet_id, sheet_name, header_row, raw_df) {
  if (!apo_is_target_sheet(sheet_name) || !any(c("Final_img_path", creative_source_fallback_header) %in% names(raw_df))) {
    return(list(data = raw_df, refreshed = FALSE))
  }

  source_idx <- match("Final_img_path", names(raw_df))
  fallback_source_idx <- match(creative_source_fallback_header, names(raw_df))
  box_idx <- match("Creative box link", names(raw_df))
  link_idx <- match("creative_git_link", names(raw_df))
  track_path_idx <- match("creative_git_last_final_img_path", names(raw_df))
  track_box_idx <- match("creative_git_last_box_link", names(raw_df))

  next_col_idx <- ncol(raw_df) + 1
  missing_headers <- list()
  if (is.na(link_idx)) {
    link_idx <- next_col_idx
    missing_headers[[length(missing_headers) + 1]] <- list(name = "creative_git_link", col_index = link_idx)
    next_col_idx <- next_col_idx + 1
  }
  if (is.na(track_path_idx)) {
    track_path_idx <- next_col_idx
    missing_headers[[length(missing_headers) + 1]] <- list(name = "creative_git_last_final_img_path", col_index = track_path_idx)
    next_col_idx <- next_col_idx + 1
  }
  if (is.na(track_box_idx)) {
    track_box_idx <- next_col_idx
    missing_headers[[length(missing_headers) + 1]] <- list(name = "creative_git_last_box_link", col_index = track_box_idx)
    next_col_idx <- next_col_idx + 1
  }

  pending_rows <- list()
  asset_paths <- character(0)
  asset_targets <- character(0)
  site_slug <- tolower(trimws(tail(strsplit(sheet_name, "\\|")[[1]], 1)))

  for (row_idx in seq_len(nrow(raw_df))) {
    source_val <- if (!is.na(source_idx)) apo_clean_text(raw_df[row_idx, source_idx, drop = TRUE]) else ""
    if (source_val == "" && !is.na(fallback_source_idx)) {
      source_val <- apo_clean_text(raw_df[row_idx, fallback_source_idx, drop = TRUE])
    }
    if (source_val == "") next

    current_link <- if (link_idx <= ncol(raw_df)) apo_clean_text(raw_df[row_idx, link_idx, drop = TRUE]) else ""
    current_box <- if (!is.na(box_idx) && box_idx <= ncol(raw_df)) apo_clean_text(raw_df[row_idx, box_idx, drop = TRUE]) else ""
    last_synced_path <- if (track_path_idx <= ncol(raw_df)) apo_clean_text(raw_df[row_idx, track_path_idx, drop = TRUE]) else ""
    last_synced_box <- if (track_box_idx <= ncol(raw_df)) apo_clean_text(raw_df[row_idx, track_box_idx, drop = TRUE]) else ""

    resolved_path <- apo_normalize_local_path(source_val)
    if (is.null(resolved_path)) {
      cat("  ⚠ APO creative path not found locally for", sheet_name, "row", header_row + row_idx, ":", source_val, "\n")
      next
    }

    rel_path <- file.path(creative_repo_asset_root, site_slug, basename(resolved_path))
    rel_path <- gsub("\\\\", "/", rel_path)
    expected_url <- apo_raw_github_url(rel_path)

    reasons <- character(0)
    if (current_link == "") reasons <- c(reasons, "missing creative_git_link")
    if (current_link != "" && current_link != expected_url) reasons <- c(reasons, "Final_img_path changed")
    if (last_synced_path != "" && last_synced_path != source_val) reasons <- c(reasons, "Final_img_path changed")
    if (last_synced_box != "" && current_box != last_synced_box) reasons <- c(reasons, "Creative box link changed")
    if (current_link != "" && last_synced_path == "") reasons <- c(reasons, "missing sync snapshot")

    if (length(reasons) == 0) next

    asset_paths <- c(asset_paths, resolved_path)
    asset_targets <- c(asset_targets, rel_path)
    pending_rows[[length(pending_rows) + 1]] <- list(
      row_number = header_row + row_idx,
      write_url = expected_url,
      source_path_value = source_val,
      box_link_value = current_box
    )
    cat("  APO creative refresh pending for", sheet_name, "row", header_row + row_idx, "->", paste(unique(reasons), collapse = ", "), "\n")
  }

  if (length(pending_rows) == 0) {
    return(list(data = raw_df, refreshed = FALSE))
  }

  repo_dir <- apo_ensure_repo_clone()

  copied_rel_paths <- character(0)
  if (length(asset_targets) > 0) {
    unique_assets <- split(asset_paths, asset_targets)
    for (rel_path in names(unique_assets)) {
      src_path <- unique_assets[[rel_path]][[1]]
      dest_path <- file.path(repo_dir, rel_path)
      dir.create(dirname(dest_path), recursive = TRUE, showWarnings = FALSE)
      if (!file.exists(dest_path)) {
        ok <- file.copy(src_path, dest_path, overwrite = FALSE)
        if (!ok) {
          stop(paste("Failed to copy APO creative asset into repo clone:", src_path))
        }
        copied_rel_paths <- c(copied_rel_paths, rel_path)
      }
    }
  }

  if (length(copied_rel_paths) > 0) {
    apo_commit_repo_changes(repo_dir, copied_rel_paths)
  }

  if (length(missing_headers) > 0) {
    for (header_info in missing_headers) {
      header_range <- paste0(apo_col_to_a1(header_info$col_index), header_row)
      apo_range_write_matrix(sheet_id, header_range, matrix(header_info$name, nrow = 1, ncol = 1))
    }
  }

  for (pending in pending_rows) {
    apo_range_write_matrix(sheet_id, paste0(apo_col_to_a1(link_idx), pending$row_number), matrix(pending$write_url, nrow = 1, ncol = 1))
    apo_range_write_matrix(sheet_id, paste0(apo_col_to_a1(track_path_idx), pending$row_number), matrix(pending$source_path_value, nrow = 1, ncol = 1))
    apo_range_write_matrix(sheet_id, paste0(apo_col_to_a1(track_box_idx), pending$row_number), matrix(pending$box_link_value, nrow = 1, ncol = 1))
  }

  updated_df <- suppressMessages(read_sheet(
    ss = sheet_id,
    sheet = "data",
    range = paste0("A", header_row, ":Y"),
    col_names = TRUE
  ))

  list(data = updated_df, refreshed = TRUE)
}

est_tz <- "America/New_York"

normalize_to_est_posix <- function(x) {
  if (length(x) == 0 || is.null(x) || all(is.na(x))) {
    return(as.POSIXct(NA, tz = est_tz))
  }

  if (inherits(x, "POSIXct") || inherits(x, "POSIXt")) {
    return(as.POSIXct(format(x, tz = est_tz, usetz = FALSE), tz = est_tz))
  }

  if (inherits(x, "Date")) {
    return(as.POSIXct(x, tz = est_tz))
  }

  if (is.character(x)) {
    x_chr <- as.character(x[[1]])
    if (is.na(x_chr) || x_chr == "") {
      return(as.POSIXct(NA, tz = est_tz))
    }

    parsed <- suppressWarnings(as.POSIXct(x_chr, format = "%Y-%m-%dT%H:%M:%OS%z", tz = "UTC"))
    if (is.na(parsed)) {
      parsed <- suppressWarnings(as.POSIXct(x_chr, format = "%Y-%m-%dT%H:%M:%S%z", tz = "UTC"))
    }
    if (is.na(parsed)) {
      parsed <- suppressWarnings(as.POSIXct(x_chr, format = "%Y-%m-%dT%H:%M:%OSZ", tz = "UTC"))
    }
    if (is.na(parsed)) {
      parsed <- suppressWarnings(as.POSIXct(x_chr, format = "%Y-%m-%dT%H:%M:%SZ", tz = "UTC"))
    }
    if (is.na(parsed)) {
      parsed <- suppressWarnings(as.POSIXct(x_chr, format = "%Y-%m-%d %H:%M:%S", tz = est_tz))
    }
    if (is.na(parsed)) {
      parsed <- suppressWarnings(as.POSIXct(x_chr, format = "%Y-%m-%d", tz = est_tz))
    }
    if (is.na(parsed)) {
      return(as.POSIXct(NA, tz = est_tz))
    }

    return(as.POSIXct(format(parsed, tz = est_tz, usetz = FALSE), tz = est_tz))
  }

  parsed <- suppressWarnings(as.POSIXct(x, tz = "UTC"))
  if (is.na(parsed)) {
    return(as.POSIXct(NA, tz = est_tz))
  }

  as.POSIXct(format(parsed, tz = est_tz, usetz = FALSE), tz = est_tz)
}

convert_to_est <- function(x) {
  normalize_to_est_posix(x)
}

format_est_timestamp <- function(x) {
  if (length(x) == 0 || is.null(x) || all(is.na(x))) {
    return(NA_character_)
  }
  normalized <- normalize_to_est_posix(x)
  if (is.na(normalized)) {
    return(NA_character_)
  }
  format(normalized, "%Y-%m-%dT%H:%M:%S%z", tz = est_tz)
}

normalize_cache_timestamp <- function(x) {
  if (length(x) == 0 || is.null(x) || all(is.na(x))) {
    return(NA_character_)
  }
  format_est_timestamp(x)
}

extract_last_modified_time <- function(drive_resource) {
  if (is.null(drive_resource) || is.null(drive_resource$modifiedTime) || length(drive_resource$modifiedTime) == 0) {
    return(as.POSIXct(NA, tz = est_tz))
  }

  ts_char <- as.character(drive_resource$modifiedTime[[1]])
  if (is.na(ts_char) || ts_char == "") {
    return(as.POSIXct(NA, tz = est_tz))
  }

  parsed_utc <- suppressWarnings(as.POSIXct(ts_char, format = "%Y-%m-%dT%H:%M:%OSZ", tz = "UTC"))
  if (is.na(parsed_utc)) {
    parsed_utc <- suppressWarnings(as.POSIXct(ts_char, format = "%Y-%m-%dT%H:%M:%S", tz = "UTC"))
  }
  convert_to_est(parsed_utc)
}

extract_last_modified_by <- function(drive_resource) {
  if (is.null(drive_resource) || is.null(drive_resource$lastModifyingUser)) {
    return(NA_character_)
  }

  user <- drive_resource$lastModifyingUser
  display_name <- user$displayName
  email <- user$emailAddress

  if (!is.null(display_name) && length(display_name) > 0 && nzchar(display_name[[1]])) {
    return(as.character(display_name[[1]]))
  }
  if (!is.null(email) && length(email) > 0 && nzchar(email[[1]])) {
    return(as.character(email[[1]]))
  }

  NA_character_
}

fetch_sheet_metadata <- function(sheet_id) {
  metadata <- tryCatch(
    drive_get(as_id(sheet_id)),
    error = function(e) {
      cat("  ⚠ Could not fetch metadata for sheet", sheet_id, ":", e$message, "\n")
      NULL
    }
  )

  if (is.null(metadata) || nrow(metadata) == 0) {
    return(list(
      last_modified_time = NA_character_,
      last_modified_by = NA_character_
    ))
  }

  drive_resource <- metadata$drive_resource[[1]]
  list(
    last_modified_time = format_est_timestamp(extract_last_modified_time(drive_resource)),
    last_modified_by = extract_last_modified_by(drive_resource)
  )
}

sanitize_cache_name <- function(x) {
  clean <- gsub("[^A-Za-z0-9_-]", "_", as.character(x))
  clean <- gsub("_+", "_", clean)
  clean <- gsub("^_+|_+$", "", clean)
  ifelse(is.na(clean) | clean == "", "unknown_sheet", clean)
}

# Keep partner placement values bindable when Google Sheets returns mixed text
# and date cells as a list-column.
coerce_partner_placement_name <- function(x) {
  vapply(seq_along(x), function(i) {
    value <- if (is.list(x)) x[[i]] else x[i]
    if (length(value) == 0 || all(is.na(value))) return(NA_character_)
    if (inherits(value, "Date") || inherits(value, "POSIXt")) return(format(value[1], "%Y-%m-%d"))
    as.character(value[1])
  }, character(1))
}

get_sheet_cache_path <- function(cache_dir, sheet_name, sheet_id = NULL) {
  # New cache naming convention: sheet-name based filename.
  # Keep old sheet-id path as a fallback read path in read_sheet_cache.
  base_name <- sanitize_cache_name(sheet_name)
  file.path(cache_dir, paste0(base_name, ".rds"))
}

cache_status_columns <- c("cache_used", "cache_fields", "cache_last_modified_time")
cache_schema_version <- 2L

is_current_cache_schema <- function(cache_obj) {
  !is.null(cache_obj$cache_schema_version) &&
    !is.na(cache_obj$cache_schema_version) &&
    as.integer(cache_obj$cache_schema_version) == cache_schema_version
}

append_cache_status <- function(df, status_map, key_col = "sheet_id") {
  if (is.null(df) || nrow(df) == 0 || !key_col %in% names(df)) return(df)
  if (is.null(status_map) || nrow(status_map) == 0 || !"sheet_id" %in% names(status_map)) {
    df$cache_used <- FALSE
    df$cache_fields <- NA_character_
    df$cache_last_modified_time <- NA_character_
    return(df)
  }

  status_map <- status_map %>%
    mutate(
      cache_used = dplyr::coalesce(cache_used, FALSE),
      cache_fields = as.character(cache_fields),
      cache_last_modified_time = as.character(cache_last_modified_time)
    )

  df <- df %>%
    left_join(
      status_map %>% select(sheet_id, cache_used, cache_fields, cache_last_modified_time),
      by = setNames("sheet_id", key_col)
    )

  if (!"cache_used" %in% names(df)) df$cache_used <- FALSE
  if (!"cache_fields" %in% names(df)) df$cache_fields <- NA_character_
  if (!"cache_last_modified_time" %in% names(df)) df$cache_last_modified_time <- NA_character_

  df$cache_used[is.na(df$cache_used)] <- FALSE
  df
}

read_sheet_cache <- function(cache_dir, sheet_id, sheet_name, last_modified_time) {
  cache_path <- get_sheet_cache_path(cache_dir, sheet_name, sheet_id)
  legacy_cache_path <- file.path(cache_dir, paste0(gsub("[^A-Za-z0-9_-]", "_", sheet_id), ".rds"))

  if (!file.exists(cache_path) && file.exists(legacy_cache_path)) {
    cache_path <- legacy_cache_path
  }
  if (!file.exists(cache_path)) return(NULL)

  cache_obj <- tryCatch(readRDS(cache_path), error = function(e) NULL)
  if (is.null(cache_obj) || !is.list(cache_obj)) return(NULL)
  if (!is_current_cache_schema(cache_obj)) return(NULL)

  cache_ts <- normalize_cache_timestamp(cache_obj$last_modified_time)
  current_ts <- normalize_cache_timestamp(last_modified_time)
  if (is.na(cache_ts) || is.na(current_ts) || cache_ts != current_ts) return(NULL)

  cache_obj
}

write_sheet_cache <- function(cache_dir, sheet_id, sheet_name, last_modified_time, field_name, field_value) {
  cache_path <- get_sheet_cache_path(cache_dir, sheet_name, sheet_id)
  cache_obj <- if (file.exists(cache_path)) {
    tryCatch(readRDS(cache_path), error = function(e) list())
  } else {
    list()
  }

  if (!is.list(cache_obj)) cache_obj <- list()
  old_cache_ts <- normalize_cache_timestamp(cache_obj$last_modified_time)
  new_cache_ts <- normalize_cache_timestamp(last_modified_time)
  if (!is_current_cache_schema(cache_obj) || is.na(old_cache_ts) || old_cache_ts != new_cache_ts) {
    cache_obj <- list()
  }
  cache_obj$cache_schema_version <- cache_schema_version
  cache_obj$sheet_id <- sheet_id
  cache_obj$last_modified_time <- new_cache_ts
  cache_obj[[field_name]] <- field_value
  saveRDS(cache_obj, cache_path)
}

get_table_field_types <- function(project_id, dataset_id, table_name) {
  table_ref <- bq_table(project = project_id, dataset = dataset_id, table = table_name)

  exists_flag <- tryCatch(
    bq_table_exists(table_ref),
    error = function(e) FALSE
  )

  if (!isTRUE(exists_flag)) {
    return(NULL)
  }

  meta <- bq_table_meta(table_ref)
  field_info <- meta$schema$fields

  if (is.null(field_info) || length(field_info) == 0) {
    return(setNames(character(0), character(0)))
  }

  field_names <- vapply(field_info, function(f) as.character(f$name), character(1))
  field_types <- vapply(field_info, function(f) as.character(f$type), character(1))
  setNames(field_types, field_names)
}

ensure_prod_schema_matches <- function(data_upload, project_id, dataset_id, prod_table) {
  prod_field_types <- get_table_field_types(project_id, dataset_id, prod_table)

  if (is.null(prod_field_types)) {
    return(NULL)
  }

  incoming_types <- setNames(
    vapply(names(data_upload), function(nm) map_bq_type(data_upload[[nm]]), character(1)),
    names(data_upload)
  )

  new_cols <- setdiff(names(incoming_types), names(prod_field_types))
  if (length(new_cols) == 0) {
    return(prod_field_types)
  }

  blank_only_new_cols <- new_cols[vapply(
    new_cols,
    function(nm) {
      values <- data_upload[[nm]]
      all(is.na(values) | trimws(as.character(values)) == "")
    },
    logical(1)
  )]

  if (length(blank_only_new_cols) > 0) {
    cat(
      paste0(
        "  ⚠ Skipping new blank-only column(s) for prod schema: ",
        paste(blank_only_new_cols, collapse = ", "),
        ". These columns were present in staging but had no values in this run.\n"
      )
    )
    new_cols <- setdiff(new_cols, blank_only_new_cols)
  }

  if (length(new_cols) == 0) {
    return(prod_field_types)
  }

  for (nm in new_cols) {
    col_type <- incoming_types[[nm]]
    alter_sql <- paste0(
      "ALTER TABLE `", project_id, ".", dataset_id, ".", prod_table, "` ",
      "ADD COLUMN `", nm, "` ", col_type
    )

    tryCatch({
      bq_perform_query(
        query = alter_sql,
        billing = project_id
      )
      cat("  ✓ Added missing BigQuery column:", nm, "(", col_type, ")\n")
    }, error = function(e) {
      stop(paste0("Failed to add missing BigQuery column ", nm, ": ", e$message))
    })
  }

  get_table_field_types(project_id, dataset_id, prod_table)
}

#### GOOGLE AUTHENTICATION (consolidated login, new-primary / old-fallback) ####
# Primary: authenticate googledrive/googlesheets4/bigrquery via the durable
# consolidated Google login. If it fails for any reason, gspoon_ok stays FALSE
# and gargle's pre-existing implicit cached-token auth is used unchanged.
gspoon_ok <- tryCatch({
  source("/Users/eugenetsenter/.config/gspoon_google_auth/google_auth.R")
  !is.null(gspoon_google_auth(packages = c("googledrive", "googlesheets4", "bigrquery"), fallback = TRUE))
}, error = function(e) { message("gspoon-auth fallback: ", conditionMessage(e)); FALSE })

################################################################################
#### PHASE 1: GOOGLE DRIVE DISCOVERY ####
################################################################################
# Goal: Find all spreadsheets matching the configured pattern
# Output: Dataframe with sheet IDs, names, URLs
# Checkpoint: phase1_discovered_files.csv

cat("\n=== PHASE 1: GOOGLE DRIVE DISCOVERY ===\n")
# If requested, load saved Phase 1 results unless this is the active phase
if (use_saved_phases && current_phase != 1 && file.exists(phase1_output)) {
  cat("Loading saved Phase 1 results from:", phase1_output, "\n")
  discovered_files <- read_csv(phase1_output, show_col_types = FALSE)
  # ensure columns have expected names
  if (!"sheet_id" %in% names(discovered_files) && "id" %in% names(discovered_files)) {
    discovered_files <- discovered_files %>% rename(sheet_id = id)
  }
  if (!"sheet_name" %in% names(discovered_files) && "name" %in% names(discovered_files)) {
    discovered_files <- discovered_files %>% rename(sheet_name = name)
  }
  cat("✓ Loaded", nrow(discovered_files), "sheets from saved Phase 1 file\n")
} else {
  # Get folder reference and query Drive
  cat("Accessing Google Drive folder:", gdrive_folder_id, "\n")
  gdrive_folder <- drive_get(as_id(gdrive_folder_id))
  cat("Searching for sheets matching pattern:", pattern, "\n")
  discovered_files <- drive_ls(
    as_id(gdrive_folder_id),
    recursive = TRUE,
    n_max = Inf
  )

  # If we find shortcuts that point to folders, expand those folders recursively
  # so sheets nested under them can be discovered as well.
  expand_folder_shortcuts <- function(df) {
    if (nrow(df) == 0) return(df)

    folder_shortcut_rows <- df %>%
      filter(
        vapply(
          drive_resource,
          function(x) {
            mime <- if (!is.null(x) && !is.null(x$mimeType) && length(x$mimeType) > 0) as.character(x$mimeType[[1]]) else NA_character_
            tgt_mime <- if (!is.null(x) && !is.null(x$shortcutDetails) && !is.null(x$shortcutDetails$targetMimeType) && length(x$shortcutDetails$targetMimeType) > 0) as.character(x$shortcutDetails$targetMimeType[[1]]) else NA_character_
            identical(mime, "application/vnd.google-apps.shortcut") &&
              identical(tgt_mime, "application/vnd.google-apps.folder")
          },
          logical(1)
        )
      )

    if (nrow(folder_shortcut_rows) == 0) return(df)

    expanded_list <- list(df)

    for (i in seq_len(nrow(folder_shortcut_rows))) {
      dr <- folder_shortcut_rows$drive_resource[[i]]
      target_folder_id <- if (!is.null(dr$shortcutDetails) && !is.null(dr$shortcutDetails$targetId) && length(dr$shortcutDetails$targetId) > 0) {
        as.character(dr$shortcutDetails$targetId[[1]])
      } else {
        NA_character_
      }

      if (is.na(target_folder_id) || target_folder_id == "") next

      cat("Expanding folder shortcut target:", target_folder_id, "\n")
      nested <- tryCatch(
        drive_ls(as_id(target_folder_id), recursive = TRUE, n_max = Inf),
        error = function(e) {
          cat("  ⚠ Could not expand folder shortcut", target_folder_id, ":", e$message, "\n")
          tibble::tibble()
        }
      )

      if (nrow(nested) > 0) expanded_list[[length(expanded_list) + 1]] <- nested
    }

    bind_rows(expanded_list)
  }

  discovered_files <- expand_folder_shortcuts(discovered_files)

  # Include both direct Google Sheets and Drive shortcuts that point to Sheets.
  # For shortcuts, use the target sheet ID so downstream reads hit the real file.
  discovered_files <- discovered_files %>%
    mutate(
      source_item_id = as.character(id),
      file_mime_type = vapply(
        drive_resource,
        function(x) {
          if (is.null(x) || is.null(x$mimeType) || length(x$mimeType) == 0) return(NA_character_)
          as.character(x$mimeType[[1]])
        },
        character(1)
      ),
      shortcut_target_id = vapply(
        drive_resource,
        function(x) {
          if (is.null(x) || is.null(x$shortcutDetails) || is.null(x$shortcutDetails$targetId) || length(x$shortcutDetails$targetId) == 0) return(NA_character_)
          as.character(x$shortcutDetails$targetId[[1]])
        },
        character(1)
      ),
      shortcut_target_mime_type = vapply(
        drive_resource,
        function(x) {
          if (is.null(x) || is.null(x$shortcutDetails) || is.null(x$shortcutDetails$targetMimeType) || length(x$shortcutDetails$targetMimeType) == 0) return(NA_character_)
          as.character(x$shortcutDetails$targetMimeType[[1]])
        },
        character(1)
      )
    ) %>%
    rename(sheet_name = name) %>%
    filter(str_detect(sheet_name, fixed(pattern))) %>%
    filter(
      file_mime_type == "application/vnd.google-apps.spreadsheet" |
        (file_mime_type == "application/vnd.google-apps.shortcut" &
           shortcut_target_mime_type == "application/vnd.google-apps.spreadsheet")
    ) %>%
    mutate(
      sheet_id = if_else(
        file_mime_type == "application/vnd.google-apps.shortcut" & !is.na(shortcut_target_id),
        shortcut_target_id,
        source_item_id
      ),
      sheet_url = paste0("https://docs.google.com/spreadsheets/d/", sheet_id),
      .after = sheet_name
    ) %>%
    distinct(sheet_id, .keep_all = TRUE) %>%
    rowwise() %>%
    mutate(
      metadata = list(fetch_sheet_metadata(sheet_id)),
      last_modified_time = metadata$last_modified_time,
      last_modified_by = metadata$last_modified_by
    ) %>%
    select(-metadata) %>%
    ungroup()

  # Filter out ARCHIVE sheets upstream so they never enter later phases
  # (case-insensitive match on sheet name)
  before_archive_filter <- nrow(discovered_files)
  discovered_files <- discovered_files %>%
    filter(!str_detect(sheet_name, "(?i)archive"))
  cat("✓ Found", before_archive_filter, "matching sheets (", nrow(discovered_files), "after removing ARCHIVE)\n")

  # Write checkpoint
  write_csv(discovered_files, phase1_output)
  cat("✓ Phase 1 checkpoint saved to:", phase1_output, "\n")
}

print(discovered_files)

################################################################################
#### PHASE 2: HEADER ROW DETECTION (PER-FILE) ####
################################################################################
# Goal: For each discovered file, identify where the data table starts
# Method: Read columns A:G, find first row with any text = header row
# Output: Dataframe with sheet name, URL, detected header row number
# Checkpoint: phase2_header_detection.csv

cat("\n=== PHASE 2: HEADER ROW DETECTION ===\n")
phase2_cache_status <- list()

# If requested, load saved Phase 2 results unless this is the active phase
if (use_saved_phases && current_phase != 2 && file.exists(phase2_output)) {
  cat("Loading saved Phase 2 results from:", phase2_output, "\n")
  phase2_results <- read_csv(phase2_output, show_col_types = FALSE)
  cat("✓ Loaded", nrow(phase2_results), "phase2 rows from saved file\n")
} else {
  # Initialize results list
  header_detection_results <- list()

  # Loop through each discovered file
  for (i in seq_len(nrow(discovered_files))) {
    sheet_name <- discovered_files$sheet_name[i]
    sheet_id <- discovered_files$sheet_id[i]
    sheet_url <- discovered_files$sheet_url[i]
    last_mod_time <- discovered_files$last_modified_time[i]
    last_mod_by <- discovered_files$last_modified_by[i]
    
    cat("\nProcessing file", i, "of", nrow(discovered_files), ":", sheet_name, "\n")
    
    tryCatch({
      cached_sheet <- if (use_file_cache_reads) read_sheet_cache(cache_dir, sheet_id, sheet_name, last_mod_time) else NULL
      if (!is.null(cached_sheet) && !is.null(cached_sheet$header_row)) {
        cat("  ✓ Reused cached header row:", cached_sheet$header_row, "\n")
        phase2_cache_status[[length(phase2_cache_status) + 1]] <- data.frame(
          sheet_id = sheet_id,
          cache_used = TRUE,
          cache_fields = "header_row",
          cache_last_modified_time = normalize_cache_timestamp(last_mod_time),
          stringsAsFactors = FALSE
        )
        header_detection_results[[i]] <- data.frame(
          sheet_name = sheet_name,
          sheet_id = sheet_id,
          sheet_url = sheet_url,
          last_modified_time = last_mod_time,
          last_modified_by = last_mod_by,
          header_row = as.integer(cached_sheet$header_row),
          status = "success",
          stringsAsFactors = FALSE
        )
        next
      }

      # Read columns A:G from 'data' tab (use for scanning header row)
      # Use a large range (A1:G500) to ensure we capture all rows including those further down
      scan_range <- "A1:G500"
      
      cat("  Scanning columns A:G for header row...\n")
      sheet_data_scan <- suppressMessages(read_sheet(
        ss = sheet_id,
        sheet = "data",
        range = scan_range,
        col_names = FALSE
      ))
      
      if (nrow(sheet_data_scan) == 0) {
        cat("  ✗ ERROR: Sheet is empty\n")
        header_detection_results[[i]] <- data.frame(
          sheet_name = sheet_name,
          sheet_id = sheet_id,
          sheet_url = sheet_url,
          last_modified_time = last_mod_time,
          last_modified_by = last_mod_by,
          header_row = NA_integer_,
          status = "empty_sheet",
          stringsAsFactors = FALSE
        )
        next
      }
      
      # Find header row: look for row with most non-empty cells (likely the header)
      # Headers typically have 5+ columns filled
      header_row_detected <- NA_integer_
      max_non_empty <- 0
      
      for (row_num in seq_len(nrow(sheet_data_scan))) {
        row_values <- as.character(sheet_data_scan[row_num, ])
        non_empty_count <- sum(!is.na(row_values) & row_values != "" & trimws(row_values) != "")
        
        # Header should have at least 5 columns and more filled cells than previous rows
        if (non_empty_count >= 5 && non_empty_count > max_non_empty) {
          header_row_detected <- row_num
          max_non_empty <- non_empty_count
        }
      }
      
      if (is.na(header_row_detected)) {
        cat("  ✗ ERROR: Could not find header row (no row with 5+ columns)\n")
        header_detection_results[[i]] <- data.frame(
          sheet_name = sheet_name,
          sheet_id = sheet_id,
          sheet_url = sheet_url,
          last_modified_time = last_mod_time,
          last_modified_by = last_mod_by,
          header_row = NA_integer_,
          status = "no_header_found",
          stringsAsFactors = FALSE
        )
        next
      }
      
      cat("  ✓ Header row detected at row:", header_row_detected, "with", max_non_empty, "columns\n")
      header_detection_results[[i]] <- data.frame(
        sheet_name = sheet_name,
        sheet_id = sheet_id,
        sheet_url = sheet_url,
        last_modified_time = last_mod_time,
        last_modified_by = last_mod_by,
        header_row = header_row_detected,
        status = "success",
        stringsAsFactors = FALSE
      )
      if (use_file_cache_writes) {
        write_sheet_cache(cache_dir, sheet_id, sheet_name, last_mod_time, "header_row", as.integer(header_row_detected))
      }
      phase2_cache_status[[length(phase2_cache_status) + 1]] <- data.frame(
        sheet_id = sheet_id,
        cache_used = FALSE,
        cache_fields = NA_character_,
        cache_last_modified_time = normalize_cache_timestamp(last_mod_time),
        stringsAsFactors = FALSE
      )
      
    }, error = function(e) {
      cat("  ✗ ERROR:", e$message, "\n")
      header_detection_results[[i]] <<- data.frame(
        sheet_name = sheet_name,
        sheet_id = sheet_id,
        sheet_url = sheet_url,
        last_modified_time = last_mod_time,
        last_modified_by = last_mod_by,
        header_row = NA_integer_,
        status = paste("error:", e$message),
        stringsAsFactors = FALSE
      )
      phase2_cache_status[[length(phase2_cache_status) + 1]] <<- data.frame(
        sheet_id = sheet_id,
        cache_used = FALSE,
        cache_fields = NA_character_,
        cache_last_modified_time = normalize_cache_timestamp(last_mod_time),
        stringsAsFactors = FALSE
      )
    })
  }

  # Combine all detection results
  phase2_results <- bind_rows(header_detection_results)
  phase2_cache_df <- bind_rows(phase2_cache_status) %>% distinct(sheet_id, .keep_all = TRUE)
  phase2_results <- append_cache_status(phase2_results, phase2_cache_df, key_col = "sheet_id")

  cat("\n=== PHASE 2 SUMMARY ===\n")
  cat("Total sheets processed:", nrow(phase2_results), "\n")
  cat("Successful detections:", sum(phase2_results$status == "success"), "\n")
  cat("Failed/Empty sheets:", nrow(phase2_results) - sum(phase2_results$status == "success"), "\n")

  # Write checkpoint
  write_csv(phase2_results, phase2_output)
  cat("✓ Phase 2 checkpoint saved to:", phase2_output, "\n")
}

#view(phase2_results)

################################################################################
#### PHASE 3: COLUMN HEADER INGESTION & METADATA COLLECTION ####
################################################################################
    # Goal: Extract all column headers from the 'data' tab of each file
    # Method: Use detected header row from Phase 2, read columns A:Y to get full table width
    # Output: Dataframe with sheet name, URL, column name, position
    # Checkpoint: phase3_raw_headers.csv


    cat("\n=== PHASE 3: COLUMN HEADER INGESTION ===\n")
phase3_cache_status <- list()

    # Initialize results list
    header_metadata_results <- list()
    header_metadata_index <- 1

    # Filter to only successful detections from Phase 2
    compute_phase3 <- FALSE
    if (use_saved_phases && current_phase != 3 && file.exists(phase3_output)) {
    cat("Loading saved Phase 3 results from:", phase3_output, "\n")
    phase3_results <- read_csv(phase3_output, show_col_types = FALSE)
    # Validate loaded structure
    if (!"column_name_raw" %in% names(phase3_results)) {
        cat("  ⚠ Saved Phase 3 file missing expected columns; recomputing Phase 3\n")
        compute_phase3 <- TRUE
    } else {
        cat("✓ Loaded", nrow(phase3_results), "phase3 rows from saved file\n")
    }
    } else {
    compute_phase3 <- TRUE
    }

    if (compute_phase3) {
    successful_files <- phase2_results %>%
        filter(status == "success")

    cat("Processing", nrow(successful_files), "sheets with detected headers...\n")

  # Loop through each file with a successful header detection
  for (i in seq_len(nrow(successful_files))) {
    sheet_name <- successful_files$sheet_name[i]
    sheet_id <- successful_files$sheet_id[i]
    sheet_url <- successful_files$sheet_url[i]
    last_mod_time <- successful_files$last_modified_time[i]
    last_mod_by <- successful_files$last_modified_by[i]
    header_row <- successful_files$header_row[i]
    
    cat("\nProcessing file", i, "of", nrow(successful_files), ":", sheet_name, "\n")
        
        tryCatch({
        cached_sheet <- if (use_file_cache_reads) read_sheet_cache(cache_dir, sheet_id, sheet_name, last_mod_time) else NULL
        if (!is.null(cached_sheet) && !is.null(cached_sheet$raw_headers)) {
          col_names <- as.character(cached_sheet$raw_headers)
          cat("  ✓ Reused cached header list with", length(col_names), "columns\n")
          phase3_cache_status[[length(phase3_cache_status) + 1]] <- data.frame(
            sheet_id = sheet_id,
            cache_used = TRUE,
            cache_fields = "raw_headers",
            cache_last_modified_time = normalize_cache_timestamp(last_mod_time),
            stringsAsFactors = FALSE
          )
        } else {
        # Read the data starting from detected header row to column S (full table width)
        # Use this row as header (col_names = TRUE)
      cat("  Reading columns A:Y starting from row", header_row, "...\n")
      
      sheet_data <- suppressMessages(read_sheet(
        ss = sheet_id,
        sheet = "data",
        range = paste0("A", header_row, ":Y"),
        col_names = TRUE
      ))
      
      if (nrow(sheet_data) == 0 || ncol(sheet_data) == 0) {
        cat("  ✗ ERROR: No data found after header row\n")
        # Record an empty result row and continue to next file
        header_metadata_results[[header_metadata_index]] <- data.frame(
          sheet_name = sheet_name,
          sheet_id = sheet_id,
          sheet_url = sheet_url,
          last_modified_time = last_mod_time,
          last_modified_by = last_mod_by,
          column_position = NA_integer_,
          column_name_raw = NA_character_,
          stringsAsFactors = FALSE
        )
        header_metadata_index <- header_metadata_index + 1
        next
      }        # Extract column names (as read by googlesheets4)
        col_names <- names(sheet_data)
        if (use_file_cache_writes) {
          write_sheet_cache(cache_dir, sheet_id, sheet_name, last_mod_time, "raw_headers", col_names)
        }
          phase3_cache_status[[length(phase3_cache_status) + 1]] <- data.frame(
            sheet_id = sheet_id,
            cache_used = FALSE,
            cache_fields = NA_character_,
            cache_last_modified_time = normalize_cache_timestamp(last_mod_time),
            stringsAsFactors = FALSE
          )
        }
        
        cat("  ✓ Found", length(col_names), "columns\n")
        
      # Create metadata record for each column
      for (col_idx in seq_along(col_names)) {
        col_name <- col_names[col_idx]
        
        header_metadata_results[[header_metadata_index]] <- data.frame(
          sheet_name = sheet_name,
          sheet_id = sheet_id,
          sheet_url = sheet_url,
          last_modified_time = last_mod_time,
          last_modified_by = last_mod_by,
          column_position = col_idx,
          column_name_raw = col_name,
          stringsAsFactors = FALSE
        )
        header_metadata_index <- header_metadata_index + 1
      }        }, error = function(e) {
        cat("  ✗ ERROR:", e$message, "\n")
        phase3_cache_status[[length(phase3_cache_status) + 1]] <<- data.frame(
          sheet_id = sheet_id,
          cache_used = FALSE,
          cache_fields = NA_character_,
          cache_last_modified_time = normalize_cache_timestamp(last_mod_time),
          stringsAsFactors = FALSE
        )
        })
    }

    # Combine all header metadata
    phase3_results <- bind_rows(header_metadata_results)
    phase3_cache_df <- bind_rows(phase3_cache_status) %>% distinct(sheet_id, .keep_all = TRUE)
    phase3_results <- append_cache_status(phase3_results, phase3_cache_df, key_col = "sheet_id")

    # Add frequency column (count how many sheets have each column name)
    phase3_results <- phase3_results %>%
        group_by(column_name_raw) %>%
        mutate(
        frequency_across_sheets = n_distinct(sheet_name),
        .after = column_name_raw
        ) %>%
        ungroup() %>%
        arrange(column_name_raw, sheet_name)

    # Write checkpoint
    write_csv(phase3_results, phase3_output)
    cat("✓ Phase 3 checkpoint saved to:", phase3_output, "\n")

    }

    if (!compute_phase3) {
    # A saved Phase 3 was loaded earlier; ensure it has the expected helper column
    if (!"frequency_across_sheets" %in% names(phase3_results)) {
        phase3_results <- phase3_results %>%
        group_by(column_name_raw) %>%
        mutate(frequency_across_sheets = n_distinct(sheet_name)) %>%
        ungroup()
    }
    } else {
    # compute_phase3 was TRUE and we already wrote the checkpoint above; nothing more to do
    }

    cat("\n=== PHASE 3 SUMMARY ===\n")
    cat("Total sheets processed:", n_distinct(phase3_results$sheet_name), "\n")
    cat("Total columns extracted:", nrow(phase3_results), "\n")
    cat("Unique column names:", n_distinct(phase3_results$column_name_raw), "\n")

    # Ensure checkpoint path and write updated phase3_results
    phase3_output <- file.path(output_dir, "phase3_raw_headers.csv")
    write_csv(phase3_results, phase3_output)
    cat("✓ Phase 3 checkpoint saved to:", phase3_output, "\n")

    # Show most frequent column names (to help with normalization)
    cat("\nMost frequent column names across sheets:\n")
    col_frequency <- phase3_results %>%
    distinct(column_name_raw, frequency_across_sheets) %>%
    arrange(desc(frequency_across_sheets)) %>%
    head(20)

    #print(col_frequency)

    #view(phase3_results)
#
################################################################################
#### PHASE 4: COLUMN NORMALIZATION MAPPING ####
################################################################################
# Goal: Build a simple normalization mapping from raw column names -> normalized names
# Rules: a few heuristic string matches (package, package_id, dates, metrics). Drop auto-generated
# columns that start with "...". Output: phase4_normalization_mapping.csv

cat("\n=== PHASE 4: COLUMN NORMALIZATION MAPPING ===\n")

# Load saved Phase 4 map if available and not the active phase
if (use_saved_phases && current_phase != 4 && file.exists(phase4_output)) {
  cat("Loading saved Phase 4 mapping from:", phase4_output, "\n")
  phase4_map <- read_csv(phase4_output, show_col_types = FALSE)
  cat("✓ Loaded", nrow(phase4_map), "mappings from saved file\n")
} else {

# Work on distinct raw column names from phase3
unique_cols <- phase3_results %>%
  distinct(column_name_raw, frequency_across_sheets) %>%
  filter(!is.na(column_name_raw))

# Helper: fallback using janitor to produce a clean name
fallback_clean <- function(x) {
  x_char <- as.character(x)
  # make_clean_names returns lower_snake_case
  nm <- tryCatch(janitor::make_clean_names(x_char), error = function(e) NA_character_)
  if (is.na(nm) || nm == "") nm <- NA_character_
  nm
}

mapping_rows <- list()
mi <- 1

for (i in seq_len(nrow(unique_cols))) {
  raw <- unique_cols$column_name_raw[i]
  freq <- unique_cols$frequency_across_sheets[i]
  rule <- NA_character_
  norm <- NA_character_

  # Skip auto-generated blank columns
  if (!is.na(raw) && startsWith(raw, "...")) {
    rule <- "drop_autogenerated"
    norm <- NA_character_
  } else {
    rlow <- tolower(raw)

    # Exact or strong matches first
    if (str_detect(rlow, "package\\s*/\\s*placement|placement\\s*/\\s*package")) {
      norm <- "partner_packagePlacement_name"; rule <- "match_package_placement"
    } else if (str_detect(rlow, "package_id") || str_detect(rlow, "packageid") || str_detect(rlow, "package id")) {
      norm <- "package_id"; rule <- "match_package_id"
    } else if (str_detect(rlow, "\\bP[A-Za-z0-9]{6}\\b")) {
      norm <- "package_id"; rule <- "regex_package_code"
    } else if (str_detect(rlow, "\\bplacement\\b")) {
      norm <- "partner_placement_name"; rule <- "match_placement_name"
    } else if (str_detect(rlow, "\\bcreative\\b")) {
      norm <- "partner_creative_name"; rule <- "match_creative_name"
    } else if (str_detect(rlow, "\\bpackage\\b") || str_detect(rlow, "package_name")) {
      norm <- "package_name"; rule <- "match_package_name"
    } else if (str_detect(rlow, "prisma[_ ]?start|prisma[_ ]?start[_ ]?date")) {
      norm <- "prisma_start_date"; rule <- "match_prisma_start_date"
    } else if (str_detect(rlow, "start[_ ]?date") || str_detect(rlow, "^start")) {
      norm <- "start_date"; rule <- "match_start_date"
    } else if (str_detect(rlow, "prisma[_ ]?end|prisma[_ ]?end[_ ]?date")) {
      norm <- "prisma_end_date"; rule <- "match_prisma_end_date"
    } else if (str_detect(rlow, "end[_ ]?date") || str_detect(rlow, "^end")) {
      norm <- "end_date"; rule <- "match_end_date"
    } else if (str_detect(rlow, "week")) {
      norm <- "week"; rule <- "match_week"
    } else if (str_detect(rlow, "(^|[^a-z])date($|[^a-z])") || rlow == "date") {
      norm <- "date"; rule <- "match_date"
    } else if (str_detect(rlow, "spend|cost|budget")) {
      norm <- "spend"; rule <- "match_spend"
    } else if (str_detect(rlow, "impress")) {
      norm <- "impressions"; rule <- "match_impressions"
    } else if (str_detect(rlow, "click")) {
      norm <- "clicks"; rule <- "match_clicks"
    } else if (str_detect(rlow, "sends?\\b|send\\b") && !str_detect(rlow, "spend")) {
      norm <- "sends"; rule <- "match_sends"
    } else if (str_detect(rlow, "completed[_ ]?view|completedview")) {
      norm <- "completed_views"; rule <- "match_completed_views"
    } else if (str_detect(rlow, "page[_ ]?view")) {
      norm <- "pageviews"; rule <- "match_pageviews"
    } else if (str_detect(rlow, "\\bview\\b")) {
      norm <- "views"; rule <- "match_views"
    } else if (str_detect(rlow, "open")) {
      norm <- "opens"; rule <- "match_opens"
    } else {
      # Fallback: use janitor to create a clean name
      token <- fallback_clean(raw)
      norm <- token
      rule <- "fallback_janitor"
    }
  }

  mapping_rows[[mi]] <- data.frame(
    column_name_raw = raw,
    frequency_across_sheets = freq,
    normalized_name = norm,
    rule_applied = rule,
    stringsAsFactors = FALSE
  )
  mi <- mi + 1
}

phase4_map <- bind_rows(mapping_rows) %>%
  arrange(desc(frequency_across_sheets), column_name_raw)

# Write checkpoint
phase4_output <- file.path(output_dir, "phase4_normalization_mapping.csv")
write_csv(phase4_map, phase4_output)
cat("✓ Phase 4 checkpoint saved to:", phase4_output, "\n")

#cat("\nSample mappings (top rows):\n")
# view(head(phase4_map, 40))


cat("\nPhase 4 complete. Next: Phase 5 will ingest data using this mapping.\n")

} # end - compute phase4_map

################################################################################
#### PHASE 5: DATA INGESTION & COMBINATION ####
################################################################################
# Goal: Read each sheet starting at detected header row (A:Y), normalize columns using
# the Phase 4 mapping, drop auto-generated columns (those starting with "..."),
# clean numeric and date columns, add source metadata, and combine into master CSV.

cat("\n=== PHASE 5: DATA INGESTION & COMBINATION ===\n")
phase5_cache_status <- list()

if (use_saved_phases && current_phase != 5 && file.exists(phase5_output)) {
  cat("Loading saved Phase 5 output from:", phase5_output, "\n")
  master_df <- read_csv(phase5_output, show_col_types = FALSE)
  master_df <- coerce_checkpoint_numeric_fields(master_df)
  cat("✓ Loaded", nrow(master_df), "rows from saved Phase 5 file\n")
} else {

# Prepare mapping lookup: a named vector raw -> normalized
map_lookup <- phase4_map %>%
  filter(!is.na(normalized_name)) %>%
  select(column_name_raw, normalized_name)
map_vec <- setNames(map_lookup$normalized_name, map_lookup$column_name_raw)

# Metrics to coerce to numeric (use configured KPI list)
metrics_expected <- known_kpi_metrics

combined_list <- list()
ci <- 1

# Ensure list of files to ingest is available (from Phase 2 results)
successful_files <- phase2_results %>% filter(status == "success")

  for (i in seq_len(nrow(successful_files))) {
    sheet_name <- successful_files$sheet_name[i]
    sheet_id <- successful_files$sheet_id[i]
    sheet_url <- successful_files$sheet_url[i]
    last_mod_time <- successful_files$last_modified_time[i]
    last_mod_by <- successful_files$last_modified_by[i]
    header_row <- successful_files$header_row[i]

    cat("\nIngesting file", i, "of", nrow(successful_files), ":", sheet_name, "\n")

  tryCatch({
    cached_sheet <- if (use_file_cache_reads) read_sheet_cache(cache_dir, sheet_id, sheet_name, last_mod_time) else NULL
    force_live_read <- !is.null(cached_sheet) &&
      !is.null(cached_sheet$raw_data) &&
      apo_sheet_needs_refresh(cached_sheet$raw_data, sheet_name)

    if (!is.null(cached_sheet) && !is.null(cached_sheet$raw_data) && !force_live_read) {
      df <- cached_sheet$raw_data
      cat("  ✓ Reused cached raw sheet data\n")
      cache_used_this_sheet <- TRUE
    } else {
      if (force_live_read) {
        cat("  APO creative links need refresh; bypassing cached raw sheet data\n")
      }
      df <- suppressMessages(read_sheet(
        ss = sheet_id,
        sheet = "data",
        range = paste0("A", header_row, ":Y"),
        col_names = TRUE
      ))
      cache_used_this_sheet <- FALSE
    }

    # Drop columns that are auto-generated blanks (start with ...)
    df <- df %>% select(-starts_with("..."))
    df <- drop_blank_generated_columns(df)

    # If dataframe is empty after dropping, skip
    if (nrow(df) == 0 || ncol(df) == 0) {
      cat("  ✗ Skipping - no usable data after dropping autogenerated columns\n")
      next
    }

    creative_refresh_result <- list(data = df, refreshed = FALSE)
    if (!cache_used_this_sheet) {
      creative_refresh_result <- refresh_apo_creative_links(sheet_id, sheet_name, header_row, df)
      df <- creative_refresh_result$data
      df <- drop_blank_generated_columns(df)
      if (isTRUE(creative_refresh_result$refreshed)) {
        cat("  ✓ APO creative_git_link values refreshed in-sheet before ingest\n")
      }
    }

    if (use_file_cache_writes && (is.null(cached_sheet) || is.null(cached_sheet$raw_data) || !use_file_cache_reads) && !isTRUE(creative_refresh_result$refreshed)) {
      write_sheet_cache(cache_dir, sheet_id, sheet_name, last_mod_time, "raw_data", df)
    } else if (isTRUE(creative_refresh_result$refreshed)) {
      cat("  ⚠ Skipped raw-data cache write because the sheet was updated during this run\n")
    }

    # Current column names (raw) as read
    raw_names <- names(df)

    # Build rename mapping for this sheet
    rename_map <- list()
    for (cn in raw_names) {
      if (cn %in% names(map_vec)) {
        newn <- map_vec[[cn]]
      } else {
        # fallback: janitor clean name
        newn <- janitor::make_clean_names(cn)
      }
      # ensure non-empty
      if (is.na(newn) || newn == "") newn <- cn
      rename_map[[cn]] <- newn
    }

    # Apply renaming (resolve duplicates with make.unique)
    new_names <- unname(unlist(rename_map))
    new_names <- make.unique(new_names, sep = "__")
    names(df) <- new_names

    # --- Normalise common column name variants (undo __N suffixes for canonical fields) ---
    normalize_after_unique <- function(nm) {
      nlow <- tolower(as.character(nm))
      # drop trailing unique suffix like __1, __2
      nlow <- sub("__\\d+$", "", nlow)
      # replace dots/spaces/hyphens with underscore for matching
      # NOTE: In R string literals, "\\." is an invalid escape. Use a safe character class.
      nlow <- gsub("[.[:space:]-]+", "_", nlow)
      if (grepl("^(start[_ ]?date|^start)$", nlow)) return("start_date")
      if (grepl("^(end[_ ]?date|^end)$", nlow)) return("end_date")
      if (grepl("prisma[_ ]?start|prisma[_ ]?start[_ ]?date", nlow)) return("prisma_start_date")
      if (grepl("prisma[_ ]?end|prisma[_ ]?end[_ ]?date", nlow)) return("prisma_end_date")
      if (grepl("\\bweek\\b", nlow)) return("week")
      if (grepl("\\bmonth\\b", nlow)) return("month")
      if (grepl("\\bdate\\b", nlow)) return("date")
      if (grepl("spend|cost|budget", nlow)) return("spend")
      if (grepl("impress", nlow)) return("impressions")
      if (grepl("click", nlow)) return("clicks")
      return(nm)
    }

    canon_names <- vapply(names(df), normalize_after_unique, FUN.VALUE = character(1), USE.NAMES = FALSE)
    names(df) <- make.unique(canon_names, sep = "__")

    if ("partner_placement_name" %in% names(df)) {
      df$partner_placement_name <- coerce_partner_placement_name(df$partner_placement_name)
    }

    # Coerce numeric metric columns: remove $ and , then as.numeric
    library(readr)

    numeric_candidates <- intersect(metrics_expected, names(df))

    if (length(numeric_candidates) > 0) {
        # Convert known KPI columns to numeric while suppressing noisy parse warnings
        # from occasional header-like text values (e.g., "CTR", "VCR").
        df <- df %>%
            mutate(
            across(
                all_of(numeric_candidates),
                ~ suppressWarnings(parse_number(as.character(.x)))
            )
            )
    }

    # Parse date columns if present (include prisma_* variants)
    date_candidates <- intersect(c("start_date", "end_date", "prisma_start_date", "prisma_end_date", "week", "date"), names(df))
    for (dc in date_candidates) {
      df[[dc]] <- suppressWarnings(as.Date(as.character(df[[dc]]), tryFormats = c("%Y-%m-%d", "%m/%d/%Y", "%d/%m/%Y")))
    }

    # Add metadata columns
    df <- df %>% mutate(
      source_file = sheet_name, 
      source_url = sheet_url,
      last_modified_time = last_mod_time,
      last_modified_by = last_mod_by,
      partner_sheet = sheet_name
      #!partner_sheet = str_extract(sheet_name, "^[^|]+") %>% str_trim()
    )

    combined_list[[ci]] <- df
    phase5_cache_status[[length(phase5_cache_status) + 1]] <- data.frame(
      source_file = sheet_name,
      source_url = sheet_url,
      sheet_id = sheet_id,
      cache_used = cache_used_this_sheet,
      cache_fields = ifelse(cache_used_this_sheet, "raw_data", NA_character_),
      cache_last_modified_time = normalize_cache_timestamp(last_mod_time),
      stringsAsFactors = FALSE
    )
    ci <- ci + 1

    cat("  ✓ Ingested", nrow(df), "rows and", ncol(df), "cols\n")

  }, error = function(e) {
    cat("  ✗ ERROR ingesting:", e$message, "\n")
  })
}

# Combine all ingested data
if (length(combined_list) == 0) {
  cat("\nNo data ingested. Phase 5 ends with no output.\n")
} else {
  master_df <- bind_rows(combined_list)
  master_df <- drop_blank_generated_columns(master_df)
  phase5_cache_df <- bind_rows(phase5_cache_status) %>% distinct(sheet_id, .keep_all = TRUE)

  # Filter rows upstream (before output + before validation) to remove rows with no KPI signal.
  # This matches the intent of the Phase 6 filter but avoids Phase 5 vs Phase 7 mismatches.
  kpi_present <- intersect(known_kpi_metrics, names(master_df))
  if (length(kpi_present) > 0) {
    rows_before <- nrow(master_df)
    master_df <- master_df %>%
      mutate(
        row_kpi_sum = rowSums(across(all_of(kpi_present), ~ dplyr::coalesce(as.numeric(.), 0)), na.rm = TRUE)
      ) %>%
      filter(!(is.na(row_kpi_sum) | row_kpi_sum == 0)) %>%
      select(-row_kpi_sum)
    cat("✓ Phase 5 pre-filter removed", rows_before - nrow(master_df), "rows where KPI metric sum is NA or 0\n")
  } else {
    cat("⚠ Phase 5 pre-filter skipped: none of known_kpi_metrics found in master_df\n")
  }

  master_df <- append_cache_status(master_df, phase5_cache_df, key_col = "sheet_id")

  phase5_output <- file.path(output_dir, "phase5_combined_master_data.csv")
  write_csv(master_df, phase5_output)
  cat("\n✓ Phase 5 complete. Combined data written to:", phase5_output, "\n")
  cat("Total rows:", nrow(master_df), "Total cols:", ncol(master_df), "\n")
}

} # end - Phase 5 checkpoint conditional

# --- Phase 5 Summary ---
cat("\n=== PHASE 5 SUMMARY ===\n")

# Metric totals
numeric_cols <- names(master_df)[sapply(master_df, is.numeric)]
if (length(numeric_cols) > 0) {
  cat("Metric Totals:\n")
  metric_sums <- master_df %>%
    summarise(across(all_of(numeric_cols), ~ sum(.x, na.rm = TRUE)))
  print(metric_sums)
}

# By sheet summary
cat("\nSummary by Sheet:\n")

# Helper: safe min/max across multiple date columns
safe_min_date <- function(df, cols) {
  cols <- intersect(cols, names(df))
  if (length(cols) == 0) return(as.Date(NA))
  x <- do.call(c, lapply(cols, function(cn) as.Date(df[[cn]])))
  if (all(is.na(x))) as.Date(NA) else min(x, na.rm = TRUE)
}

safe_max_date <- function(df, cols) {
  cols <- intersect(cols, names(df))
  if (length(cols) == 0) return(as.Date(NA))
  x <- do.call(c, lapply(cols, function(cn) as.Date(df[[cn]])))
  if (all(is.na(x))) as.Date(NA) else max(x, na.rm = TRUE)
}

date_cols_any <- c(
  "start_date", "end_date", "prisma_start_date", "prisma_end_date",
  "week", "date", "month",
  "start_date_final", "end_date_final", "date_final"
)

# package count preference: package_id else package_name
package_key <- NA_character_
if ("package_id" %in% names(master_df)) {
  package_key <- "package_id"
} else if ("package_name" %in% names(master_df)) {
  package_key <- "package_name"
}

sheet_summary <- master_df %>%
  mutate(
    source_file = as.character(source_file),
    source_url = as.character(source_url)
  ) %>%
  group_by(source_file, last_modified_time, last_modified_by) %>%
  summarise(
    row_count = n(),
    package_count = if (!is.na(package_key)) n_distinct(.data[[package_key]], na.rm = TRUE) else NA_integer_,
    spend_sum = if ("spend" %in% names(pick(everything()))) sum(spend, na.rm = TRUE) else NA_real_,
    impressions_sum = if ("impressions" %in% names(pick(everything()))) sum(impressions, na.rm = TRUE) else NA_real_,
    min_date_any = safe_min_date(pick(everything()), date_cols_any),
    min_date_col = {
      min_val <- safe_min_date(pick(everything()), date_cols_any)
      cols <- intersect(date_cols_any, names(pick(everything())))
      if (is.na(min_val) || length(cols) == 0) {
        NA_character_
      } else {
        # Find which column(s) contain this min date
        cur_pick <- pick(everything())
        matches <- sapply(cols, function(cn) any(as.Date(cur_pick[[cn]]) == min_val, na.rm = TRUE))
        paste(names(matches)[matches], collapse = ", ")
      }
    },
    min_date_reported = safe_min_date(pick(everything()), c("date", "week", "month", "start_date")),
    # add column used for max_date_any
    max_date_reported = safe_max_date(pick(everything()), c("date", "week", "month", "end_date")),
    max_date_any = safe_max_date(pick(everything()), date_cols_any),
    across(all_of(setdiff(numeric_cols, c("spend", "impressions"))), ~ sum(.x, na.rm = TRUE)),
    .groups = "drop"
  ) %>%
  arrange(source_file)

# Update Phase 1 checkpoint with per-sheet summary metrics
if (exists("discovered_files") && nrow(discovered_files) > 0) {
  phase1_enriched <- discovered_files %>%
    left_join(sheet_summary %>% select(source_file, row_count, package_count, spend_sum, impressions_sum, min_date_any, max_date_any),
              by = c("sheet_name" = "source_file"))

  write_csv(phase1_enriched, phase1_output)
  cat("✓ Phase 1 checkpoint updated with per-sheet rollups:", phase1_output, "\n")
}

print(sheet_summary)

cat("\n=== END PHASE 5 SUMMARY ===\n")

################################################################################
#### PHASE 6: CLEANING & DATE FINALIZATION ####
################################################################################
# Goal: Post-process the combined master (`phase5_combined_master_data.csv`) to
# 1) extract `package_id` from `package_name` when missing
# 2) standardize date columns (`start_date`, `end_date`, `week`, `date`, `month`) to Date
# 3) compute `start_date_final` and `end_date_final` for every row

cat("\n=== PHASE 6: CLEANING & DATE FINALIZATION ===\n")

phase6_output <- file.path(output_dir, "phase6_cleaned_master_data.csv")
phase6_filter_audit <- data.frame(
  source_file = character(0),
  filter_reason = character(0),
  removed_rows = integer(0),
  stringsAsFactors = FALSE
)

if (use_saved_phases && current_phase != 6 && file.exists(phase6_output)) {
  cat("Loading saved Phase 6 output from:", phase6_output, "\n")
  phase6_df <- read_csv(phase6_output, show_col_types = FALSE)
  phase6_df <- coerce_checkpoint_numeric_fields(phase6_df)
  cat("✓ Loaded", nrow(phase6_df), "rows from saved Phase 6 file\n")

  if (file.exists(phase6_filter_audit_output)) {
    phase6_filter_audit <- read_csv(phase6_filter_audit_output, show_col_types = FALSE)
    cat("✓ Loaded Phase 6 filter audit from:", phase6_filter_audit_output, "\n")
  } else {
    cat("  ⚠ Phase 6 filter audit not found at:", phase6_filter_audit_output, "\n")
  }
} else {
  # Read Phase 5 combined master
  if (!file.exists(phase5_output)) stop("Phase 5 output not found: ", phase5_output)
  cat("Reading Phase 5 master from:", phase5_output, "\n")
  phase6_df <- read_csv(phase5_output, show_col_types = FALSE)
  phase6_df <- coerce_checkpoint_numeric_fields(phase6_df)

  # Normalize column name typos: pacakge_id -> package_id
  if ("pacakge_id" %in% names(phase6_df) && !"package_id" %in% names(phase6_df)) {
    phase6_df <- phase6_df %>% rename(package_id = pacakge_id)
  }

  # Ensure package_id exists
  if (!"package_id" %in% names(phase6_df)) phase6_df$package_id <- NA_character_

  # Attempt extraction of package_id from package_name when missing
  if ("package_name" %in% names(phase6_df)) {
    pk <- as.character(phase6_df$package_name)
    # primary pattern: 7 alnum after first '|' and before next '_'
    extracted <- stringr::str_match(pk, "\\|[^_]*?([A-Za-z0-9]{7})_")[,2]
    # fallback: 7 alnum directly after first |
    missing_idx <- which(is.na(extracted) | extracted == "")
    if (length(missing_idx) > 0) {
      extracted2 <- stringr::str_match(pk[missing_idx], "\\|\\s*([A-Za-z0-9]{7})")[,2]
      extracted[missing_idx] <- extracted2
    }
    fill_idx <- which(is.na(phase6_df$package_id) | phase6_df$package_id == "")
    if (length(fill_idx) > 0) phase6_df$package_id[fill_idx] <- extracted[fill_idx]
  }

  # Date parsing helper using lubridate for robustness
  parse_any_date <- function(x) {
    x_chr <- as.character(x)
    # try multiple orders
    dt <- suppressWarnings(lubridate::parse_date_time(x_chr, orders = c("Y-m-d", "m/d/Y", "d/m/Y", "Ymd", "Y-m", "b Y", "B Y"), tz = "UTC"))
    as.Date(dt)
  }

  # Parse possible date-like columns
  if ("start_date" %in% names(phase6_df)) phase6_df$start_date <- parse_any_date(phase6_df$start_date)
  if ("end_date" %in% names(phase6_df)) phase6_df$end_date <- parse_any_date(phase6_df$end_date)
  if ("prisma_start_date" %in% names(phase6_df)) phase6_df$prisma_start_date <- parse_any_date(phase6_df$prisma_start_date)
  if ("prisma_end_date" %in% names(phase6_df)) phase6_df$prisma_end_date <- parse_any_date(phase6_df$prisma_end_date)
  if ("week" %in% names(phase6_df)) phase6_df$week <- parse_any_date(phase6_df$week)
  if ("date" %in% names(phase6_df)) phase6_df$date <- parse_any_date(phase6_df$date)
  if ("month" %in% names(phase6_df)) {
    phase6_df$month <- parse_any_date(phase6_df$month)
    # Snap to the 1st of the month regardless of input day (e.g., Oct 15 -> Oct 1)
    phase6_df$month <- lubridate::floor_date(phase6_df$month, "month")
  }

  # Compute week_end and month_end
  week_end <- if ("week" %in% names(phase6_df)) phase6_df$week + 6 else as.Date(NA)
  month_end <- if ("month" %in% names(phase6_df)) lubridate::ceiling_date(phase6_df$month, "month") - lubridate::days(1) else as.Date(NA)

  # Compute final start/end dates with precedence rules
  # Prisma dates are planning-level (package-wide) — only use as last resort
  # when no granular date column (start_date, week, month, date) is available.

  # Helper: safely get a date column or NA vector of the right length
  safe_date_col <- function(df, col) {
    if (col %in% names(df)) df[[col]] else rep(as.Date(NA), nrow(df))
  }

  # --- start_date_final precedence ---
  # start_date > week > month > date > end_date > prisma_start_date > prisma_end_date
  start_candidates <- list(
    list(col = safe_date_col(phase6_df, "start_date"),        label = "start_date"),
    list(col = safe_date_col(phase6_df, "week"),              label = "week"),
    list(col = safe_date_col(phase6_df, "month"),             label = "month"),
    list(col = safe_date_col(phase6_df, "date"),              label = "date"),
    list(col = safe_date_col(phase6_df, "end_date"),          label = "end_date"),
    list(col = safe_date_col(phase6_df, "prisma_start_date"), label = "prisma_start_date"),
    list(col = safe_date_col(phase6_df, "prisma_end_date"),   label = "prisma_end_date")
  )

  # Build result + source label via first-non-NA walk
  n_rows <- nrow(phase6_df)
  sdf_val    <- rep(as.Date(NA), n_rows)
  sdf_source <- rep(NA_character_, n_rows)
  for (cand in start_candidates) {
    fill <- which(is.na(sdf_val) & !is.na(cand$col))
    if (length(fill) > 0) {
      sdf_val[fill]    <- cand$col[fill]
      sdf_source[fill] <- cand$label
    }
  }
  phase6_df$start_date_final  <- as.Date(sdf_val, origin = "1970-01-01")
  phase6_df$start_date_source <- sdf_source

  # --- end_date_final precedence ---
  # end_date > week_end > month_end > start_date > date > prisma_end_date > prisma_start_date
  end_candidates <- list(
    list(col = safe_date_col(phase6_df, "end_date"),          label = "end_date"),
    list(col = week_end,                                      label = "week_end"),
    list(col = month_end,                                     label = "month_end"),
    list(col = safe_date_col(phase6_df, "start_date"),        label = "start_date"),
    list(col = safe_date_col(phase6_df, "date"),              label = "date"),
    list(col = safe_date_col(phase6_df, "prisma_end_date"),   label = "prisma_end_date"),
    list(col = safe_date_col(phase6_df, "prisma_start_date"), label = "prisma_start_date")
  )

  edf_val    <- rep(as.Date(NA), n_rows)
  edf_source <- rep(NA_character_, n_rows)
  for (cand in end_candidates) {
    fill <- which(is.na(edf_val) & !is.na(cand$col))
    if (length(fill) > 0) {
      edf_val[fill]    <- cand$col[fill]
      edf_source[fill] <- cand$label
    }
  }
  phase6_df$end_date_final  <- as.Date(edf_val, origin = "1970-01-01")
  phase6_df$end_date_source <- edf_source

  cat("  Date source distribution (start_date_final):\n")
  print(table(phase6_df$start_date_source, useNA = "ifany"))
  cat("  Date source distribution (end_date_final):\n")
  print(table(phase6_df$end_date_source, useNA = "ifany"))

  # Track removals by stable row id for filter-level audit reporting
  phase6_df <- phase6_df %>% mutate(.phase6_row_id = row_number())
  validate_kpis_for_audit <- setdiff(known_kpi_metrics, c("benchmark", "benchmark_metric"))
  audit_metric_cols <- intersect(unique(c(validate_kpis_for_audit, "spend", "impressions", "clicks")), names(phase6_df))
  filter_audit_entries <- list()

  summarize_removed_rows <- function(before_df, after_df, reason_label, metric_cols) {
    removed_ids <- setdiff(before_df$.phase6_row_id, after_df$.phase6_row_id)
    if (length(removed_ids) == 0) return(data.frame())

    removed_df <- before_df %>% filter(.phase6_row_id %in% removed_ids)
    if (!"source_file" %in% names(removed_df)) removed_df$source_file <- NA_character_
    removed_df <- removed_df %>% mutate(source_file = as.character(source_file))

    if (length(metric_cols) > 0) {
      removed_df %>%
        group_by(source_file) %>%
        summarise(
          filter_reason = reason_label,
          removed_rows = n(),
          across(all_of(metric_cols), ~ sum(as.numeric(.x), na.rm = TRUE), .names = "removed_{.col}"),
          .groups = "drop"
        )
    } else {
      removed_df %>%
        group_by(source_file) %>%
        summarise(
          filter_reason = reason_label,
          removed_rows = n(),
          .groups = "drop"
        )
    }
  }

  # Filter 1: Remove rows where source_file contains "archive" (case-insensitive)
  if ("source_file" %in% names(phase6_df)) {
    before_filter <- phase6_df
    rows_before <- nrow(phase6_df)
    phase6_df <- phase6_df %>%
      filter(!str_detect(source_file, "(?i)archive"))
    filter_audit_entries[[length(filter_audit_entries) + 1]] <-
      summarize_removed_rows(before_filter, phase6_df, "archive_source_file", audit_metric_cols)
    cat("  ✓ Removed", rows_before - nrow(phase6_df), "rows with 'archive' in source_file\n")
  }

  # Filter 2: Remove rows where sum of all numeric columns is NA, 0, or blank
  numeric_cols <- setdiff(names(phase6_df)[sapply(phase6_df, is.numeric)], ".phase6_row_id")
  if (length(numeric_cols) > 0) {
    before_filter <- phase6_df
    rows_before <- nrow(phase6_df)
    phase6_df <- phase6_df %>%
      mutate(
        row_metric_sum = rowSums(across(all_of(numeric_cols), ~ coalesce(as.numeric(.), 0)), na.rm = TRUE)
      ) %>%
      filter(!(is.na(row_metric_sum) | row_metric_sum == 0)) %>%
      select(-row_metric_sum)
    filter_audit_entries[[length(filter_audit_entries) + 1]] <-
      summarize_removed_rows(before_filter, phase6_df, "numeric_row_metric_sum_zero_or_na", audit_metric_cols)
    cat("  ✓ Removed", rows_before - nrow(phase6_df), "rows where sum of numeric columns is NA or 0\n")
  }

  # Filter 3: Remove rows where package_name is NA
  if ("package_name" %in% names(phase6_df)) {
    before_filter <- phase6_df
    rows_before <- nrow(phase6_df)
    phase6_df <- phase6_df %>%
      filter(!is.na(package_name) & package_name != "")
    filter_audit_entries[[length(filter_audit_entries) + 1]] <-
      summarize_removed_rows(before_filter, phase6_df, "missing_package_name", audit_metric_cols)
    cat("  ✓ Removed", rows_before - nrow(phase6_df), "rows where package_name is NA or blank\n")
  }

  # Build and write filter-audit checkpoint
  phase6_filter_audit <- bind_rows(filter_audit_entries)
  if (nrow(phase6_filter_audit) > 0) {
    phase6_filter_audit <- phase6_filter_audit %>%
      mutate(source_file = as.character(source_file)) %>%
      arrange(source_file, filter_reason)
  } else {
    phase6_filter_audit <- data.frame(
      source_file = character(0),
      filter_reason = character(0),
      removed_rows = integer(0),
      stringsAsFactors = FALSE
    )
  }

  write_csv(phase6_filter_audit, phase6_filter_audit_output)
  cat("✓ Phase 6 filter audit saved to:", phase6_filter_audit_output, "\n")

  # Remove internal tracking id before writing Phase 6 output
  phase6_df <- phase6_df %>% select(-any_of(".phase6_row_id"))

  # Write Phase 6 checkpoint
  write_csv(phase6_df, phase6_output)
  cat("✓ Phase 6 checkpoint saved to:", phase6_output, "\n")
}

cat("Phase 6 complete. Rows:", if (exists('phase6_df')) nrow(phase6_df) else 0, "\n")

################################################################################
#### PHASE 6.1: SHEET-LEVEL SUMMARY TABLE ####
################################################################################
# Goal: Create a summary table with one row per sheet showing:
# - Partner name, sheet name, URL, last modified info
# - Row count, package count, date range
# - Totals for all KPI metrics

cat("\n=== PHASE 6.1: SHEET-LEVEL SUMMARY TABLE ===\n")

phase6_1_output <- file.path(output_dir, "phase6_1_sheet_summary.csv")

if (use_saved_phases && current_phase != 6 && file.exists(phase6_1_output)) {
  cat("Loading saved Phase 6.1 output from:", phase6_1_output, "\n")
  phase6_1_summary <- read_csv(phase6_1_output, show_col_types = FALSE)
  cat("✓ Loaded", nrow(phase6_1_summary), "sheet summaries from saved Phase 6.1 file\n")
} else {
  # Ensure phase6_df is available
  if (!exists('phase6_df') || nrow(phase6_df) == 0) {
    cat("⚠ Phase 6 data not available. Skipping Phase 6.1.\n")
  } else {
    # Helper functions for date aggregation
    safe_min_date <- function(df, cols) {
      cols <- intersect(cols, names(df))
      if (length(cols) == 0) return(as.Date(NA))
      x <- do.call(c, lapply(cols, function(cn) as.Date(df[[cn]])))
      if (all(is.na(x))) as.Date(NA) else min(x, na.rm = TRUE)
    }

    safe_max_date <- function(df, cols) {
      cols <- intersect(cols, names(df))
      if (length(cols) == 0) return(as.Date(NA))
      x <- do.call(c, lapply(cols, function(cn) as.Date(df[[cn]])))
      if (all(is.na(x))) as.Date(NA) else max(x, na.rm = TRUE)
    }

    # Date columns to consider for range
    date_cols_for_range <- c(
      "start_date", "end_date", "prisma_start_date", "prisma_end_date",
      "week", "date", "month", "start_date_final", "end_date_final"
    )

    # Determine package key (prefer package_id, fallback to package_name)
    package_key <- NA_character_
    if ("package_id" %in% names(phase6_df)) {
      package_key <- "package_id"
    } else if ("package_name" %in% names(phase6_df)) {
      package_key <- "package_name"
    }

    # Get all numeric columns for metrics
    numeric_cols <- names(phase6_df)[sapply(phase6_df, is.numeric)]

    # Build summary by source_file
    phase6_1_summary <- phase6_df %>%
      mutate(
        source_file = as.character(source_file),
        source_url = as.character(source_url),
        partner_name = as.character(site)
      ) %>%
      group_by(partner_name, source_file, source_url, last_modified_time, last_modified_by) %>%
      summarise(
        row_count = n(),
        package_count = if (!is.na(package_key)) n_distinct(.data[[package_key]], na.rm = TRUE) else NA_integer_,
        date_range_start = safe_min_date(pick(everything()), date_cols_for_range),
        date_range_end = safe_max_date(pick(everything()), date_cols_for_range),
        # Date source diagnostics: which column drives start/end dates for this sheet
        start_date_sources = paste(sort(unique(start_date_source[!is.na(start_date_source)])), collapse = ", "),
        end_date_sources   = paste(sort(unique(end_date_source[!is.na(end_date_source)])),     collapse = ", "),
        # If week is a source, show the distinct week start dates used
        week_start_dates_used = if (any(start_date_source == "week", na.rm = TRUE))
          paste(sort(unique(as.character(start_date_final[start_date_source == "week" & !is.na(start_date_source)]))), collapse = ", ")
        else NA_character_,
        # If week is a source, show the day-of-week range (e.g., "Sun-Sat", "Mon-Sun")
        week_day_range = if (any(start_date_source == "week", na.rm = TRUE)) {
          wk_dates <- start_date_final[start_date_source == "week" & !is.na(start_date_source)]
          start_dow <- unique(lubridate::wday(wk_dates, label = TRUE, abbr = TRUE))
          end_dow   <- unique(lubridate::wday(wk_dates + 6, label = TRUE, abbr = TRUE))
          paste(paste(start_dow, collapse = "/"), paste(end_dow, collapse = "/"), sep = "-")
        } else NA_character_,
        # Add totals for all KPI metrics
        across(
          all_of(intersect(known_kpi_metrics, numeric_cols)),
          ~ sum(as.numeric(.x), na.rm = TRUE),
          .names = "{.col}_sum"
        ),
        .groups = "drop"
      ) %>%
      arrange(partner_name, source_file)

    # Write Phase 6.1 checkpoint
    write_csv(phase6_1_summary, phase6_1_output)
    cat("✓ Phase 6.1 checkpoint saved to:", phase6_1_output, "\n")
    cat("✓ Summary created for", nrow(phase6_1_summary), "sheets\n")
  }
}

cat("Phase 6.1 complete.\n")

################################################################################
#### PHASE 7: EXPAND RANGED DATA TO DAILY ROWS ####
################################################################################
# Goal: For rows with a date range (start_date_final -> end_date_final), expand into
# daily rows. Numeric metric columns are divided evenly across days. Add `date_final`
# which records the original end date for each expanded row.

cat("\n=== PHASE 7: EXPAND RANGED DATA TO DAILY ROWS ===\n")

phase7_output <- file.path(output_dir, "phase7_daily_master_data.csv")

if (use_saved_phases && current_phase != 7 && file.exists(phase7_output)) {
  cat("Loading saved Phase 7 output from:", phase7_output, "\n")
  phase7_df <- read_csv(phase7_output, show_col_types = FALSE)
  cat("✓ Loaded", nrow(phase7_df), "rows from saved Phase 7 file\n")
} else {
  # Read Phase 6 cleaned master
  if (!file.exists(phase6_output)) stop("Phase 6 output not found: ", phase6_output)
  cat("Reading Phase 6 master from:", phase6_output, "\n")
  base_df <- read_csv(phase6_output, show_col_types = FALSE)
  base_df <- coerce_checkpoint_numeric_fields(base_df)

  # Ensure date columns are Date class
  date_cols <- c("start_date_final", "end_date_final")
  for (dc in date_cols) if (dc %in% names(base_df)) base_df[[dc]] <- as.Date(base_df[[dc]])

  # Determine metric columns to split using configured KPI list only
  metric_cols <- intersect(known_kpi_metrics, names(base_df))

  cat("Detected metric columns to split (known KPI list):", paste(metric_cols, collapse=", "), "\n")

  # Expand rows
  expanded_rows <- list()
  er <- 1

  for (r in seq_len(nrow(base_df))) {
    row <- base_df[r, , drop = FALSE]
    sdate <- row$start_date_final
    edate <- row$end_date_final

    if (is.na(sdate) || is.na(edate) || edate < sdate) {
      # no valid range -> keep as-is. Prefer an existing raw `date` column if present,
      # otherwise fall back to end_date_final or start_date_final.
      if ("date" %in% names(row) && !is.na(row$date)) {
        row$date_final <- as.Date(row$date)
      } else if (!is.na(row$end_date_final)) {
        row$date_final <- as.Date(row$end_date_final)
      } else {
        row$date_final <- as.Date(row$start_date_final)
      }
      expanded_rows[[er]] <- row
      er <- er + 1
      next
    }

    ndays <- as.integer(edate - sdate) + 1
    if (ndays <= 1) {
      # single-day range: prefer raw `date` column if present, else use end_date_final
      if ("date" %in% names(row) && !is.na(row$date)) {
        row$date_final <- as.Date(row$date)
      } else {
        row$date_final <- as.Date(edate)
      }
      expanded_rows[[er]] <- row
      er <- er + 1
      next
    }

    # For metric columns, divide evenly across ndays
    metrics_to_split <- intersect(metric_cols, names(row))
    if (length(metrics_to_split) == 0) metrics_to_split <- character(0)

    # Create daily rows
    for (d in 0:(ndays-1)) {
      newrow <- row
      this_date <- sdate + d
      # assign per-row exploded date into `date_final` (user preference)
      newrow$date_final <- this_date
      # divide metrics
      if (length(metrics_to_split) > 0) {
        for (mc in metrics_to_split) {
          val <- as.numeric(row[[mc]])
          newrow[[mc]] <- ifelse(is.na(val), NA_real_, val / ndays)
        }
      }
      expanded_rows[[er]] <- newrow
      er <- er + 1
    }
  }

  # Bind expanded rows
  phase7_df <- bind_rows(expanded_rows) %>% mutate(data_update_datetime = Sys.time())

  # --- Fill missing week values using per-client config ---
  # For rows where `week` is NA but `date_final` exists, compute the week start
  # using the client's configured week_start day (defaults to Sunday).
  if (!"week" %in% names(phase7_df)) phase7_df$week <- as.Date(NA)

  missing_week_idx <- which(is.na(phase7_df$week) & !is.na(phase7_df$date_final))
  if (length(missing_week_idx) > 0) {
    # Build lookup: lowercase client name -> week_start number
    config_lookup <- setNames(
      day_to_wstart[client_week_config$week_start],
      tolower(client_week_config$client)
    )
    default_ws_num <- day_to_wstart[[default_week_start]]

    # Get each row's client (fall back to default if client column missing or unrecognized)
    if ("client" %in% names(phase7_df)) {
      row_clients <- tolower(as.character(phase7_df$client[missing_week_idx]))
      row_ws_nums <- ifelse(
        row_clients %in% names(config_lookup),
        config_lookup[row_clients],
        default_ws_num
      )
    } else {
      row_ws_nums <- rep(default_ws_num, length(missing_week_idx))
    }

    # Vectorize by unique week_start values to avoid per-row loop
    for (ws in unique(row_ws_nums)) {
      ws_rows <- missing_week_idx[row_ws_nums == ws]
      phase7_df$week[ws_rows] <- lubridate::floor_date(
        phase7_df$date_final[ws_rows], "week", week_start = ws
      )
    }

    cat("  ✓ Filled", length(missing_week_idx), "missing week values using per-client config\n")
    # Show summary of which clients were filled
    if ("client" %in% names(phase7_df)) {
      filled_summary <- table(phase7_df$client[missing_week_idx])
      cat("    Rows filled per client:", paste(names(filled_summary), filled_summary, sep = "=", collapse = ", "), "\n")
    }
  } else {
    cat("  No missing week values to fill.\n")
  }

  # Write Phase 7 checkpoint
  write_csv(phase7_df, phase7_output)
  cat("✓ Phase 7 checkpoint saved to:", phase7_output, "\n")
}

# Enforce integer output values for count metrics in final Phase 7 output.
# This keeps downstream BigQuery typing stable for fields like `clicks`.
integer_output_cols <- intersect(c("impressions", "clicks"), names(phase7_df))
if (length(integer_output_cols) > 0) {
  for (col_name in integer_output_cols) {
    col_vals <- suppressWarnings(as.numeric(phase7_df[[col_name]]))
    phase7_df[[col_name]] <- ifelse(is.na(col_vals), NA_real_, round(col_vals, 0))
  }
  write_csv(phase7_df, phase7_output)
  cat("✓ Rounded final output columns to integers:", paste(integer_output_cols, collapse = ", "), "\n")
}

# --- Validation: compare per-sheet metric totals between Phase 5 and Phase 7 ---
# Build full validation table and enrich with Phase 6 filter-reason audit.
cat("\nBuilding KPI validation table between Phase 5 and Phase 7...\n")
if (file.exists(phase5_output)) {
  phase5_df <- read_csv(phase5_output, show_col_types = FALSE)
  phase5_df <- coerce_checkpoint_numeric_fields(phase5_df)

  # KPI metrics to validate (exclude benchmarks; those are often non-additive / metadata)
  validate_kpis <- setdiff(known_kpi_metrics, c("benchmark", "benchmark_metric"))
  metrics <- intersect(validate_kpis, intersect(names(phase5_df), names(phase7_df)))

  if (length(metrics) == 0) {
    cat("  No KPI metrics detected for validation.\n")
    validation_table <- data.frame(
      source_file = character(0),
      mismatch_any = logical(0),
      filter_reason = character(0),
      stringsAsFactors = FALSE
    )
    validation_diff_cols <- character(0)
    write_csv(validation_table, phase7_validation_output)
    cat("  ✓ Validation table saved to:", phase7_validation_output, "\n")
  } else {
    cat("  KPI metrics validated:", paste(metrics, collapse = ", "), "\n")

    s5 <- phase5_df %>%
      mutate(source_file = as.character(source_file)) %>%
      group_by(source_file) %>%
      summarise(across(all_of(metrics), ~ sum(as.numeric(.x), na.rm = TRUE)), .groups = "drop")

    s7 <- phase7_df %>%
      mutate(source_file = as.character(source_file)) %>%
      group_by(source_file) %>%
      summarise(across(all_of(metrics), ~ sum(as.numeric(.x), na.rm = TRUE)), .groups = "drop")

    comp <- full_join(s5, s7, by = "source_file", suffix = c(".p5", ".p7"))

    # Compute differences for each metric (round before diffing to avoid floating point accumulation noise)
    # Suggested rounding: 2 decimals is typical for currency; for simplicity we use 6 decimals for all.
    round_digits <- 6
    for (m in metrics) {
      c_p5 <- paste0(m, ".p5")
      c_p7 <- paste0(m, ".p7")
      diff_col <- paste0(m, "_diff")
      comp[[c_p5]][is.na(comp[[c_p5]])] <- 0
      comp[[c_p7]][is.na(comp[[c_p7]])] <- 0
      comp[[diff_col]] <- round(comp[[c_p7]], round_digits) - round(comp[[c_p5]], round_digits)
    }

    # Flag mismatches beyond a tiny tolerance
    tol <- 1e-6
    diff_cols <- grep("_diff$", names(comp), value = TRUE)
    comp$mismatch_any <- apply(abs(comp[, diff_cols, drop = FALSE]), 1, function(x) any(x > tol, na.rm = TRUE))

    # Load Phase 6 filter-audit checkpoint if needed and build aggregated reason text
    if ((!exists("phase6_filter_audit") || !is.data.frame(phase6_filter_audit) || nrow(phase6_filter_audit) == 0) &&
        file.exists(phase6_filter_audit_output)) {
      phase6_filter_audit <- read_csv(phase6_filter_audit_output, show_col_types = FALSE)
    }

    reason_summary <- data.frame(
      source_file = character(0),
      filter_reason = character(0),
      stringsAsFactors = FALSE
    )

    if (exists("phase6_filter_audit") && is.data.frame(phase6_filter_audit) && nrow(phase6_filter_audit) > 0) {
      audit_df <- phase6_filter_audit
      if (!"source_file" %in% names(audit_df)) audit_df$source_file <- NA_character_
      if (!"filter_reason" %in% names(audit_df)) audit_df$filter_reason <- NA_character_
      if (!"removed_rows" %in% names(audit_df)) audit_df$removed_rows <- NA_integer_
      removed_metric_cols <- grep("^removed_", names(audit_df), value = TRUE)

      format_reason_detail <- function(df_row) {
        reason_label <- as.character(df_row[["filter_reason"]])
        if (is.na(reason_label) || reason_label == "") reason_label <- "unknown_filter"

        detail_parts <- c()
        rows_removed <- suppressWarnings(as.numeric(df_row[["removed_rows"]]))
        if (!is.na(rows_removed)) detail_parts <- c(detail_parts, paste0("rows=", as.integer(rows_removed)))

        if (length(removed_metric_cols) > 0) {
          for (metric_col in removed_metric_cols) {
            val <- suppressWarnings(as.numeric(df_row[[metric_col]]))
            if (is.na(val) || abs(val) <= 1e-12) next
            metric_name <- sub("^removed_", "", metric_col)
            val_chr <- if (metric_name == "spend") {
              format(round(val, 2), nsmall = 2, trim = TRUE, scientific = FALSE)
            } else {
              format(round(val, 6), trim = TRUE, scientific = FALSE)
            }
            detail_parts <- c(detail_parts, paste0(metric_name, "=", val_chr))
          }
        }

        if (length(detail_parts) == 0) {
          reason_label
        } else {
          paste0(reason_label, "(", paste(detail_parts, collapse = ", "), ")")
        }
      }

      audit_df$reason_detail <- vapply(
        seq_len(nrow(audit_df)),
        function(i) format_reason_detail(audit_df[i, , drop = FALSE]),
        FUN.VALUE = character(1)
      )

      reason_summary <- audit_df %>%
        mutate(source_file = as.character(source_file)) %>%
        arrange(source_file, filter_reason) %>%
        group_by(source_file) %>%
        summarise(
          filter_reason = paste(reason_detail, collapse = "; "),
          .groups = "drop"
        )
    }

    validation_table <- comp %>%
      left_join(reason_summary, by = "source_file") %>%
      mutate(filter_reason = if_else(is.na(filter_reason), "", filter_reason)) %>%
      arrange(source_file)

    validation_diff_cols <- diff_cols
    write_csv(validation_table, phase7_validation_output)
    cat("  ✓ Validation table saved to:", phase7_validation_output, "\n")
  }
} else {
  cat("  ⚠ Phase 5 checkpoint not found; skipping validation table build.\n")
  validation_table <- data.frame(
    source_file = character(0),
    mismatch_any = logical(0),
    filter_reason = character(0),
    stringsAsFactors = FALSE
  )
  validation_diff_cols <- character(0)
  write_csv(validation_table, phase7_validation_output)
  cat("  ✓ Empty validation table saved to:", phase7_validation_output, "\n")
}

cat("Phase 7 complete. Rows:", if (exists('phase7_df')) nrow(phase7_df) else 0, "\n")


#### write to BQ --using write_to_bq  ####

  cat("\n=== Starting BigQuery upload process ===\n")
  # Before uploading: replace NA in numeric columns with 0 to avoid NULLs where zeros are expected
  if (exists('phase7_df')) {
    numeric_cols_bq <- names(phase7_df)[sapply(phase7_df, is.numeric)]
    if (length(numeric_cols_bq) > 0) {
      phase7_df[numeric_cols_bq] <- lapply(phase7_df[numeric_cols_bq], function(x) { x[is.na(x)] <- 0; x })
      # Update the Phase 7 checkpoint so it's consistent with upload-ready data
      write_csv(phase7_df, phase7_output)
      cat("  ✓ Replaced NA with 0 for numeric columns before BQ upload: ", paste(numeric_cols_bq, collapse=", "), "\n")
    }
  }

  # SAFE DEPLOY: do NOT delete prod first.
  # Upload to staging, validate, then replace only the affected prod rows.

  write_to_bq <- function(data, dataset, table) {
    cat("Writing", nrow(data), "rows to BigQuery...\n")
    
    # Wrap in tryCatch to handle errors
    tryCatch({
      # Write the data to BigQuery
      bq_table <- bq_table(project = project_id, dataset = dataset, table = table)
      
      # Normalize types before upload to avoid BigQuery autodetect edge-case failures.
      # Keep timestamps explicit and coerce non-standard/list/object columns to character.
      data_upload <- data

      # Ensure Date columns are plain Date and timestamp is POSIXct UTC.
      date_cols_upload <- intersect(c("date", "week", "month", "start_date", "end_date", "prisma_start_date", "prisma_end_date", "start_date_final", "end_date_final", "date_final"), names(data_upload))
      for (dc in date_cols_upload) data_upload[[dc]] <- as.Date(data_upload[[dc]])
      if ("data_update_datetime" %in% names(data_upload)) {
        data_upload$data_update_datetime <- as.POSIXct(data_upload$data_update_datetime, tz = "UTC")
      }

      # Coerce unsupported/object columns to character proactively.
      for (cn in names(data_upload)) {
        x <- data_upload[[cn]]
        if (is.list(x) || is.factor(x)) {
          data_upload[[cn]] <- as.character(x)
        }
      }

      # BigQuery upload can fail on all-NA logical columns (NULL type inference).
      # Convert logicals to character flags to keep schema explicit/stable.
      logical_cols_upload <- names(data_upload)[sapply(data_upload, is.logical)]
      if (length(logical_cols_upload) > 0) {
        for (lc in logical_cols_upload) {
          data_upload[[lc]] <- ifelse(is.na(data_upload[[lc]]), NA_character_, ifelse(data_upload[[lc]], "true", "false"))
        }
      }

      # Keep timestamp fields as POSIXct (UTC) so they load as TIMESTAMP in BigQuery.
      ts_cols_upload <- intersect(c("last_modified_time", "data_update_datetime"), names(data_upload))
      if (length(ts_cols_upload) > 0) {
        for (tc in ts_cols_upload) {
          data_upload[[tc]] <- as.POSIXct(data_upload[[tc]], tz = "UTC")
        }
      }

      fields <- build_bq_fields(data_upload)

      # Use bq_table_upload to write the data
      bq_table_upload(
        x = bq_table,
        values = data_upload,
        fields = fields,
        write_disposition = "WRITE_TRUNCATE"
      )
      
      cat("✓ Data written to BigQuery table:", table, "\n")
    },
    error = function(e) {
      cat("✗ ERROR writing to BigQuery:", e$message, "\n")
      cat("  Upload diagnostics:\n")
      cat("    rows:", nrow(data), " cols:", ncol(data), "\n")
      cat("    column names:", paste(names(data), collapse = ", "), "\n")
      if ("data_update_datetime" %in% names(data)) {
        cat("    data_update_datetime class:", paste(class(data$data_update_datetime), collapse = ", "), "\n")
      }
      cat("    column classes:\n")
      for (cn in names(data)) {
        cat("      -", cn, ":", paste(class(data[[cn]]), collapse = ", "), "\n")
      }
      stop("Failed to write to BigQuery")
    })
  }
  
  write_to_bq(phase7_df, "landing", staging_table)

  validate_staging <- function(df) {
    required_cols <- c("source_file", "date_final", "data_update_datetime")
    missing_cols <- setdiff(required_cols, names(df))

    if (nrow(df) == 0) {
      return(list(ok = FALSE, reason = "staging has 0 rows"))
    }
    if (length(missing_cols) > 0) {
      return(list(ok = FALSE, reason = paste("missing required cols:", paste(missing_cols, collapse = ", "))))
    }
    if (all(is.na(df$date_final))) {
      return(list(ok = FALSE, reason = "date_final is all NA"))
    }

    list(ok = TRUE, reason = "passed")
  }

  v <- validate_staging(phase7_df)
  if (!isTRUE(v$ok)) {
    stop(paste0("Staging validation failed (prod NOT modified): ", v$reason))
  }

  cat("Staging validation passed. Syncing affected prod rows from staging...\n")

  prod_field_types <- ensure_prod_schema_matches(
    data_upload = phase7_df,
    project_id = project_id,
    dataset_id = dataset_id,
    prod_table = prod_table
  )

  if (is.null(prod_field_types)) {
    create_sql <- paste0(
      "CREATE TABLE `", project_id, ".", dataset_id, ".", prod_table, "` AS ",
      "SELECT * FROM `", project_id, ".", dataset_id, ".", staging_table, "`"
    )

    tryCatch({
      create_job <- bq_perform_query(
        query = create_sql,
        billing = project_id
      )
      bq_job_wait(create_job)
      cat("✓ Prod table created from staging:", prod_table, "\n")
    }, error = function(e) {
      stop(paste0("Failed to create prod table from staging: ", e$message))
    })
  } else {
    staging_field_types <- get_table_field_types(project_id, dataset_id, staging_table)
    if (is.null(staging_field_types)) {
      stop("Staging table schema could not be read after upload.")
    }

    sync_source_urls <- unique(as.character(phase7_df$source_url))
    sync_source_urls <- sync_source_urls[!is.na(sync_source_urls) & sync_source_urls != ""]
    sync_source_files <- unique(as.character(phase7_df$source_file))
    sync_source_files <- sync_source_files[!is.na(sync_source_files) & sync_source_files != ""]

    if (length(sync_source_urls) == 0 && length(sync_source_files) == 0) {
      stop("Incremental BigQuery sync requires source_url or source_file values, but none were found in this run.")
    }

    delete_clauses <- character(0)
    if (length(sync_source_urls) > 0) {
      delete_clauses <- c(
        delete_clauses,
        paste0(
          "target.source_url IN (",
          paste(vapply(sync_source_urls, sql_quote_string, character(1)), collapse = ", "),
          ")"
        )
      )
    }
    if (length(sync_source_files) > 0) {
      delete_clauses <- c(
        delete_clauses,
        paste0(
          "target.source_file IN (",
          paste(vapply(sync_source_files, sql_quote_string, character(1)), collapse = ", "),
          ")"
        )
      )
    }

    prod_columns <- names(prod_field_types)
    insert_column_sql <- paste(paste0("`", prod_columns, "`"), collapse = ", ")

    select_exprs <- vapply(
      prod_columns,
      function(col_name) {
        if (col_name %in% names(staging_field_types)) {
          paste0("source.`", col_name, "`")
        } else {
          paste0("CAST(NULL AS ", normalize_bq_sql_type(prod_field_types[[col_name]]), ") AS `", col_name, "`")
        }
      },
      character(1)
    )
    select_sql <- paste(select_exprs, collapse = ", ")

    sync_sql <- paste0(
      "BEGIN TRANSACTION; ",
      "DELETE FROM `", project_id, ".", dataset_id, ".", prod_table, "` AS target ",
      "WHERE ", paste(delete_clauses, collapse = " OR "), "; ",
      "INSERT INTO `", project_id, ".", dataset_id, ".", prod_table, "` (", insert_column_sql, ") ",
      "SELECT ", select_sql, " FROM `", project_id, ".", dataset_id, ".", staging_table, "` AS source; ",
      "COMMIT TRANSACTION;"
    )

    tryCatch({
      sync_job <- bq_perform_query(
        query = sync_sql,
        billing = project_id
      )
      bq_job_wait(sync_job)
      cat("✓ Prod table updated only for sheets in this run:", prod_table, "\n")
    }, error = function(e) {
      stop(paste0("Failed incremental prod sync from staging: ", e$message))
    })
  }

  cat("\n-----------\n-----------\n First party data pipeline completed at:", format(Sys.time(), "%Y-%m-%d %H:%M:%S"), "\n")

  cat("\n=== FINAL VALIDATION SUMMARY (PHASE 5 vs PHASE 7) ===\n")
  if (exists("validation_table") && is.data.frame(validation_table)) {
    if (nrow(validation_table) == 0) {
      cat("No validation rows available (validation table is empty).\n")
    } else {
      final_mismatches <- validation_table %>% filter(mismatch_any)
      show_cols <- unique(c("source_file", validation_diff_cols, "filter_reason"))
      show_cols <- show_cols[show_cols %in% names(validation_table)]

      if (nrow(final_mismatches) == 0) {
        cat("✓ All per-sheet KPI metric totals match between Phase 5 and Phase 7 (within tolerance).\n")
      } else {
        cat("✗ Detected differences in per-sheet KPI totals for", nrow(final_mismatches), "sheets.\n")
        print(final_mismatches[, show_cols, drop = FALSE])
      }
    }

    cat("Validation table saved to:", phase7_validation_output, "\n")
  } else {
    cat("⚠ Validation table not generated.\n")
  }


# --- OPTIONAL: Run post-processing script ---
# this script layers on manually updated first party data [currently for ADIF] needs to be updated for general use]
# manually update data here: https://docs.google.com/spreadsheets/d/1kUD8gVrHAAaZbULtFgDZl1hgGU-7Ut8fSdNJDgsZwfE/edit?gid=1894007924#gid=1894007924

#source("/Users/eugenetsenter/Looker_clonedRepo/looker_personal/util/data_loaders/FPD_loader/manually_updated_data_loader.r")
