################################################################################
#### LOAD MASTER DATA MODEL MANUAL PACKAGE EDITS
################################################################################
# Purpose:
#   Maintain the Manual Data Editor's single Package Editor tab where users edit
#   dashboard values in-place. The loader compares those cells to source-derived
#   baselines and trusted prior manual evidence, writes valid manual BigQuery
#   rows, and refreshes the same visible sheet without using blank/stale reads as
#   evidence that a user deleted an edit.
################################################################################

suppressPackageStartupMessages({
  library(googledrive)
  library(googlesheets4)
  library(bigrquery)
  library(dplyr)
  library(tidyr)
  library(lubridate)
  library(stringr)
})

PROJECT_ID <- Sys.getenv("MASTER_MANUAL_EDIT_PROJECT", "looker-studio-pro-452620")
DATASET_ID <- Sys.getenv("MASTER_MANUAL_EDIT_DATASET", "landing")
RAW_TABLE <- Sys.getenv("MASTER_MANUAL_EDIT_RAW_TABLE", "master_data_model_manual_package_edits_raw")
DAILY_TABLE <- Sys.getenv("MASTER_MANUAL_EDIT_DAILY_TABLE", "master_data_model_manual_package_daily")
HISTORY_TABLE <- Sys.getenv("MASTER_MANUAL_EDIT_HISTORY_TABLE", "master_data_model_manual_package_edits_history")
MART_TABLE <- Sys.getenv("MASTER_MANUAL_EDIT_MART_TABLE", "looker-studio-pro-452620.master_stg.data_model_mart")
PRISMA_TABLE <- Sys.getenv("MASTER_MANUAL_EDIT_PRISMA_TABLE", "looker-studio-pro-452620.20250327_data_model.prisma_expanded_full")
LOOKUP_TABLE <- Sys.getenv("MASTER_MANUAL_EDIT_LOOKUP_TABLE", "looker-studio-pro-452620.master_stg.manual_package_editor_package_lookup")
DEFAULT_SHEET_ID <- "1p1aGAg8lMk7JvUKCJBKRj5rKQNNYL3iEKnl0kPHvZ7E"
SHEET_ID <- Sys.getenv("MASTER_MANUAL_EDIT_SHEET_ID", unset = DEFAULT_SHEET_ID)
AUTH_EMAIL <- Sys.getenv("MASTER_MANUAL_EDIT_AUTH_EMAIL", "gene.tsenter@giantspoon.com")
AUTH_CONFIG_BY_ACCOUNT <- c(
  "gene.tsenter@giantspoon.com" = "/Users/eugenetsenter/.config/gcloud-giantspoon",
  "gene.tsenter@old.giantspoon.com" = "/Users/eugenetsenter/.config/gcloud-old-giantspoon"
)
AUTH_CONFIG_PATH <- Sys.getenv(
  "MASTER_MANUAL_EDIT_GCLOUD_CONFIG",
  Sys.getenv(
    "CLOUDSDK_CONFIG",
    unname(AUTH_CONFIG_BY_ACCOUNT[[AUTH_EMAIL]])
  )
)
GOOGLE_AUTH_SCOPES <- c(
  "https://www.googleapis.com/auth/cloud-platform",
  "https://www.googleapis.com/auth/drive",
  "https://www.googleapis.com/auth/spreadsheets"
)
LOADER_FILENAME <- "load_manual_package_edits.R"
DEFAULT_SCRIPT_DIR <- "/Users/eugenetsenter/Looker_clonedRepo/looker_personal/master_data_model/manual_package_edits"

find_loader_script_dir <- function() {
  script_args <- commandArgs(trailingOnly = FALSE)
  file_matches <- sub("^--file=", "", script_args[startsWith(script_args, "--file=")])
  frame_files <- vapply(sys.frames(), function(frame) {
    ofile <- frame$ofile
    if (is.null(ofile) || length(ofile) == 0) {
      NA_character_
    } else {
      as.character(ofile[[1]])
    }
  }, character(1))

  candidates <- c(file_matches, frame_files)
  candidates <- candidates[!is.na(candidates) & candidates != ""]
  loader_candidates <- candidates[basename(candidates) == LOADER_FILENAME]
  if (length(loader_candidates) > 0) {
    return(dirname(normalizePath(loader_candidates[[length(loader_candidates)]], mustWork = FALSE)))
  }

  if (file.exists(file.path(getwd(), "repair_manual_package_editor_filters.mjs"))) {
    return(getwd())
  }

  if (file.exists(file.path(DEFAULT_SCRIPT_DIR, "repair_manual_package_editor_filters.mjs"))) {
    return(DEFAULT_SCRIPT_DIR)
  }

  if (length(candidates) > 0) {
    return(dirname(normalizePath(candidates[[1]], mustWork = FALSE)))
  }

  getwd()
}
SCRIPT_DIR <- find_loader_script_dir()
CONTRACTS_PATH <- file.path(SCRIPT_DIR, "manual_editor_contracts.R")
if (!file.exists(CONTRACTS_PATH)) {
  stop("Manual Data Editor contract helper not found: ", CONTRACTS_PATH, call. = FALSE)
}
source(CONTRACTS_PATH)
LOOKUP_REFRESH_SQL <- Sys.getenv(
  "MASTER_MANUAL_EDIT_LOOKUP_REFRESH_SQL",
  file.path(SCRIPT_DIR, "create_manual_package_editor_package_lookup.sql")
)
EXCLUDED_SOCIAL_CAMPAIGN_PATTERN <- Sys.getenv(
  "MASTER_MANUAL_EDIT_EXCLUDED_SOCIAL_CAMPAIGN_PATTERN",
  "1000heads"
)

TAB_EDITOR <- "Package Editor"
EDITOR_HEADER_ROW <- 4
EDITOR_HEADER_INDEX <- EDITOR_HEADER_ROW - 1
EDITOR_DATA_INDEX <- EDITOR_HEADER_ROW
REQUEST_STATUS_CELL <- "F2"
EDITOR_VISIBLE_LAST_COLUMN <- "BC"
MANUAL_MARKER_START_COLUMN <- "BD"
FILTER_HELPER_START_COLUMN <- "CA"
EDITOR_LAST_COLUMN <- "CA"
LEGACY_TABS <- c(
  "Sheet1", "Package Lookup", "Start Here", "Manual Package Edits", "Manual Edits",
  "Validation Preview", "Daily Proof", "Publish Status", "Change History"
)

if (SHEET_ID == "") {
  SHEET_ID <- DEFAULT_SHEET_ID
}

cat("\n========================================\n")
cat("MASTER DATA MODEL PACKAGE EDITOR\n")
cat("========================================\n\n")

auth_manual_editor <- function() {
  if (is.na(AUTH_CONFIG_PATH) || AUTH_CONFIG_PATH == "") {
    stop("No account-specific Google auth config is defined for ", AUTH_EMAIL, call. = FALSE)
  }

  auth_file <- file.path(AUTH_CONFIG_PATH, "application_default_credentials.json")
  if (!file.exists(auth_file)) {
    stop("Missing account-specific Google ADC file: ", auth_file, call. = FALSE)
  }

  token <- gargle::credentials_app_default(
    scopes = GOOGLE_AUTH_SCOPES,
    path = auth_file
  )
  gs4_auth(token = token)
  drive_auth(token = token)
  bq_auth(token = token)
  sheets_request <- googlesheets4:::gs4_token()
  sheets_access_token <- sheets_request$auth_token$credentials$access_token
  if (!is.null(sheets_access_token) && nzchar(sheets_access_token)) {
    # The Node geometry helper runs in this same loader process. Reuse the
    # already authenticated Sheets token instead of asking a separate gcloud
    # credential store to mint a second token.
    Sys.setenv(MASTER_MANUAL_EDIT_ACCESS_TOKEN = sheets_access_token)
  }
  cat("Authenticated with account-specific ADC: ", auth_file, "\n", sep = "")
  invisible(token)
}

auth_manual_editor()

loaded_at <- Sys.time()

display_columns <- c(
  "Package ID",
  "Site",
  "Package Friendly Name",
  "Flight Start Date",
  "Flight End Date",
  "Planned Spend",
  "Planned Impressions",
  "Spend",
  "Impressions",
  "Clicks",
  "Video Plays",
  "Video Completions",
  "Delivery Override Start Date",
  "Delivery Override End Date",
  "Advertiser",
  "Package Type",
  "Channel",
  "Campaign",
  "Initiative",
  "Supplier Code",
  "Supplier Name",
  "Package Name",
  "GS Channel",
  "Benchmark KPI",
  "Benchmark Value",
  "Manually Edited?",
  "Manual Edit At",
  "Manual Edit By",
  "Manual Edit Published At",
  "Primary Row Data Source",
  "Validation Status",
  "Validation Reason",
  "Baseline Flight Start Date",
  "Baseline Flight End Date",
  "Baseline Planned Spend",
  "Baseline Planned Impressions",
  "Baseline Spend",
  "Baseline Impressions",
  "Baseline Clicks",
  "Baseline Video Plays",
  "Baseline Video Completions",
  "Baseline Delivery Start Date",
  "Baseline Delivery End Date",
  "Baseline Advertiser",
  "Baseline Package Type",
  "Baseline Channel",
  "Baseline Campaign",
  "Baseline Initiative",
  "Baseline Supplier Code",
  "Baseline Supplier Name",
  "Baseline Package Name",
  "Baseline Package Friendly Name",
  "Baseline GS Channel",
  "Baseline Benchmark KPI",
  "Baseline Benchmark Value"
)

manual_marker_columns <- c(
  "Manual Marker Flight Start Date",
  "Manual Marker Flight End Date",
  "Manual Marker Planned Spend",
  "Manual Marker Planned Impressions",
  "Manual Marker Spend",
  "Manual Marker Impressions",
  "Manual Marker Clicks",
  "Manual Marker Video Plays",
  "Manual Marker Video Completions",
  "Manual Marker Delivery Start Date",
  "Manual Marker Delivery End Date",
  "Manual Marker Advertiser",
  "Manual Marker Package Type",
  "Manual Marker Channel",
  "Manual Marker Campaign",
  "Manual Marker Initiative",
  "Manual Marker Supplier Code",
  "Manual Marker Supplier Name",
  "Manual Marker Package Name",
  "Manual Marker Package Friendly Name",
  "Manual Marker GS Channel",
  "Manual Marker Benchmark KPI",
  "Manual Marker Benchmark Value"
)

filter_helper_columns <- c(
  "Edited Row Filter"
)

metric_specs <- tibble::tribble(
  ~display_col, ~value_key, ~current_col, ~replacement_col, ~delta_col, ~daily_col, ~total_col, ~metric_name,
  "Spend", "spend", "current_spend", "replacement_spend", "delta_spend", "man_daily_spend", "man_total_spend_doNotSum", "spend",
  "Impressions", "impressions", "current_impressions", "replacement_impressions", "delta_impressions", "man_daily_impressions", "man_total_impressions_doNotSum", "impressions",
  "Planned Spend", "planned_spend", "current_planned_spend", "replacement_planned_spend", "delta_planned_spend", "man_daily_planned_spend", "man_total_planned_spend_doNotSum", "planned_spend",
  "Planned Impressions", "planned_impressions", "current_planned_impressions", "replacement_planned_impressions", "delta_planned_impressions", "man_daily_planned_impressions", "man_total_planned_impressions_doNotSum", "planned_impressions",
  "Clicks", "clicks", "current_clicks", "replacement_clicks", "delta_clicks", "man_daily_clicks", "man_total_clicks_doNotSum", "clicks",
  "Video Plays", "video_plays", "current_video_plays", "replacement_video_plays", "delta_video_plays", "man_daily_video_plays", "man_total_video_plays_doNotSum", "video_plays",
  "Video Completions", "video_comps", "current_video_comps", "replacement_video_comps", "delta_video_comps", "man_daily_video_comps", "man_total_video_comps_doNotSum", "video_comps"
)

planned_metric_names <- c("planned_spend", "planned_impressions")
whole_number_metric_names <- c("impressions", "planned_impressions", "clicks", "video_plays", "video_comps")
daily_total_proof_tolerance <- 0.01

metadata_specs <- tibble::tribble(
  ~display_col, ~value_key, ~current_col, ~manual_col, ~value_type,
  "Advertiser", "advertiser_name", "current_advertiser_name", "man_advertiser_name", "text",
  "Package Type", "package_type", "current_package_type", "man_package_type", "text",
  "Channel", "channel", "current_channel", "man_channel", "text",
  "Campaign", "campaign_name", "current_campaign_name", "man_campaign_name", "text",
  "Initiative", "initiative", "current_initiative", "man_initiative", "text",
  "Supplier Code", "supplier_code", "current_supplier_code", "man_supplier_code", "text",
  "Supplier Name", "supplier_name", "current_supplier_name", "man_supplier_name", "text",
  "Package Name", "package_name", "current_package_name", "man_package_name", "text",
  "Package Friendly Name", "package_name_friendly", "current_package_name_friendly", "man_package_name_friendly", "text",
  "GS Channel", "ADIF_channel", "current_ADIF_channel", "man_ADIF_channel", "text",
  "Benchmark KPI", "benchmark_kpi", "current_benchmark_kpi", "man_benchmark_kpi", "text",
  "Benchmark Value", "benchmark_value", "current_benchmark_value", "man_benchmark_value", "numeric"
)

raw_columns <- c(
  "is_active",
  "edit_id",
  "package_id",
  "man_start_date",
  "man_end_date",
  "man_flight_start_date",
  "man_flight_end_date",
  "current_row_count",
  "current_first_date",
  "current_last_date",
  "current_flight_start_date",
  "current_flight_end_date",
  "replacement_flight_start_date",
  "replacement_flight_end_date",
  metric_specs$current_col,
  metric_specs$replacement_col,
  metric_specs$delta_col,
  metadata_specs$current_col,
  metadata_specs$manual_col,
  "advertiser_name",
  "advertiser_short_name",
  "campaign_name",
  "campaign_friendly",
  "product_code",
  "product_name",
  "package_type",
  "package_name",
  "package_name_friendly",
  "ADIF_channel",
  "placement_id",
  "placement_name",
  "supplier_code",
  "supplier_name",
  "supplier_logo",
  "p_buy_type",
  "p_buy_category",
  "channel",
  "channel_raw",
  "channel_group",
  "media_name",
  "p_cost_method",
  "p_planned_amount_doNotSum",
  "p_planned_impressions_doNotSum",
  "p_planned_units_doNotSum",
  "p_unit_type",
  "p_rate",
  "edit_reason",
  "editor_email",
  "manual_edit_at",
  "manual_edit_by",
  "manual_edit_published_at",
  "validation_status",
  "validation_messages"
)

`%pick%` <- function(x, y) {
  if (is.null(x) || length(x) == 0 || all(is.na(x))) y else x
}

as_trimmed_character <- function(x) {
  out <- str_trim(as.character(x))
  out[out == "" | str_to_lower(out) %in% c("na", "nan", "null")] <- NA_character_
  out
}

is_excluded_social_campaign <- function(campaign_name) {
  campaign_name <- coalesce(as_trimmed_character(campaign_name), "")
  str_detect(str_to_lower(campaign_name), fixed(str_to_lower(EXCLUDED_SOCIAL_CAMPAIGN_PATTERN)))
}

parse_num <- function(x) {
  y <- str_replace_all(as.character(x), "[,$% ]", "")
  y[y == "" | str_to_lower(y) %in% c("na", "nan", "null")] <- NA_character_
  suppressWarnings(as.numeric(y))
}

parse_date <- function(x) {
  if (inherits(x, "Date")) {
    return(x)
  }
  if (inherits(x, "POSIXt")) {
    return(as.Date(x))
  }
  choose_serial_date <- function(serial_value) {
    sheet_date <- as.Date(serial_value, origin = "1899-12-30")
    r_date <- as.Date(serial_value, origin = "1970-01-01")
    use_r_date <- !is.na(r_date) &
      r_date >= as.Date("2000-01-01") &
      r_date <= as.Date("2100-12-31") &
      (is.na(sheet_date) | sheet_date < as.Date("2000-01-01"))
    sheet_date[use_r_date] <- r_date[use_r_date]
    sheet_date
  }
  if (is.numeric(x)) {
    return(choose_serial_date(x))
  }
  y <- str_trim(as.character(x))
  y[y == "" | str_to_lower(y) %in% c("na", "nan", "null")] <- NA_character_
  parsed <- suppressWarnings(as.Date(
    y,
    tryFormats = c("%Y-%m-%d", "%m/%d/%Y", "%m/%d/%y", "%Y/%m/%d", "%b %d, %Y", "%B %d, %Y"),
    optional = TRUE
  ))
  serial_date <- suppressWarnings(as.numeric(y))
  serial_date[is.na(serial_date)] <- NA_real_
  out <- dplyr::coalesce(parsed, choose_serial_date(serial_date))
  out[!is.na(out) & (out < as.Date("1900-01-01") | out > as.Date("2100-12-31"))] <- as.Date(NA)
  out
}

parse_timestamp <- function(x) {
  if (inherits(x, "POSIXt")) {
    return(as.POSIXct(x, tz = "UTC"))
  }
  if (inherits(x, "Date")) {
    return(as.POSIXct(x, tz = "UTC"))
  }
  if (is.numeric(x)) {
    return(as.POSIXct((as.numeric(x) - 25569) * 86400, origin = "1970-01-01", tz = "UTC"))
  }
  y <- str_trim(as.character(x))
  y[y == "" | str_to_lower(y) %in% c("na", "nan", "null")] <- NA_character_
  y <- str_replace(y, "Z$", "+00:00")
  parsed <- suppressWarnings(ymd_hms(y, tz = "UTC", quiet = TRUE))
  fallback <- suppressWarnings(as.POSIXct(y, tz = "UTC"))
  dplyr::coalesce(as.POSIXct(parsed, tz = "UTC"), fallback)
}

same_num <- function(a, b, tol = 0.01) {
  if (is.na(a) && is.na(b)) return(TRUE)
  if (is.na(a) || is.na(b)) return(FALSE)
  abs(as.numeric(a) - as.numeric(b)) <= tol
}

same_date <- function(a, b) {
  a <- parse_date(a)
  b <- parse_date(b)
  if (is.na(a) && is.na(b)) return(TRUE)
  if (is.na(a) || is.na(b)) return(FALSE)
  a == b
}

stable_edit_id <- function(package_id, row_number) {
  id <- str_replace_all(str_to_lower(as.character(package_id)), "[^a-z0-9]+", "-")
  id <- str_replace_all(id, "^-|-$", "")
  if (is.na(id) || id == "") {
    id <- paste0("row-", row_number)
  }
  paste0("manual-", id, "-", row_number)
}

format_date_key <- function(x) {
  d <- parse_date(x)
  out <- rep("", length(d))
  out[!is.na(d)] <- format(d[!is.na(d)], "%Y-%m-%d")
  out
}

manual_row_key <- function(package_id, start_date, end_date) {
  paste(as.character(package_id), format_date_key(start_date), format_date_key(end_date), sep = "\001")
}

ensure_tab <- function(sheet_id, tab_name) {
  sheet_info <- gs4_get(sheet_id)
  if (!tab_name %in% sheet_info$sheets$name) {
    sheet_add(sheet_id, sheet = tab_name)
  }
}

write_tab <- function(sheet_id, tab_name, data) {
  ensure_tab(sheet_id, tab_name)
  range_clear(sheet_id, sheet = tab_name)
  range_write(sheet_id, data = data, sheet = tab_name, range = "A1", col_names = TRUE)
}

format_date_for_sheet <- function(x) {
  dates <- parse_date(x)
  out <- rep(NA_character_, length(dates))
  out[!is.na(dates)] <- format(dates[!is.na(dates)], "%Y-%m-%d")
  out
}

format_timestamp_for_sheet <- function(x) {
  timestamps <- parse_timestamp(x)
  out <- rep(NA_character_, length(timestamps))
  out[!is.na(timestamps)] <- format(timestamps[!is.na(timestamps)], "%Y-%m-%d %H:%M:%S")
  out
}

prepare_editor_write_data <- function(data) {
  date_cols <- c(
    "Flight Start Date", "Flight End Date",
    "Delivery Override Start Date", "Delivery Override End Date",
    "Baseline Flight Start Date", "Baseline Flight End Date",
    "Baseline Delivery Start Date", "Baseline Delivery End Date"
  )
  for (col in intersect(date_cols, names(data))) {
    data[[col]] <- format_date_for_sheet(data[[col]])
  }

  timestamp_cols <- c("Manual Edit At", "Manual Edit Published At")
  for (col in intersect(timestamp_cols, names(data))) {
    data[[col]] <- format_timestamp_for_sheet(data[[col]])
  }

  data
}

write_editor_tab <- function(sheet_id, tab_name, data) {
  ensure_tab(sheet_id, tab_name)
  sheet_metadata <- gs4_get(sheet_id)
  tab_grid_rows <- sheet_metadata$sheets$grid_rows[sheet_metadata$sheets$name == tab_name]
  if (length(tab_grid_rows) != 1 || is.na(tab_grid_rows[[1]])) {
    stop("Could not determine the existing row count for editor tab: ", tab_name, call. = FALSE)
  }
  sheet_resize(sheet_id, sheet = tab_name, ncol = 79)
  range_clear(
    sheet_id,
    range = editor_data_clear_range(tab_name, EDITOR_HEADER_ROW, EDITOR_LAST_COLUMN, tab_grid_rows[[1]]),
    reformat = FALSE
  )
  data <- prepare_editor_write_data(data)
  range_write(sheet_id, data = data, sheet = tab_name, range = paste0("A", EDITOR_HEADER_ROW), col_names = TRUE, reformat = FALSE)
}

write_manual_marker_columns <- function(sheet_id, tab_name, data) {
  ensure_tab(sheet_id, tab_name)
  range_clear(sheet_id, range = paste0("'", tab_name, "'!", MANUAL_MARKER_START_COLUMN, EDITOR_HEADER_ROW, ":", EDITOR_LAST_COLUMN), reformat = FALSE)
  range_write(sheet_id, data = data, sheet = tab_name, range = paste0(MANUAL_MARKER_START_COLUMN, EDITOR_HEADER_ROW), col_names = TRUE, reformat = FALSE)
}

write_filter_helper_columns <- function(sheet_id, tab_name, data) {
  ensure_tab(sheet_id, tab_name)
  sheet_resize(sheet_id, sheet = tab_name, ncol = 79)
  range_clear(sheet_id, range = paste0("'", tab_name, "'!", FILTER_HELPER_START_COLUMN, EDITOR_HEADER_ROW, ":", EDITOR_LAST_COLUMN), reformat = FALSE)
  range_write(sheet_id, data = data, sheet = tab_name, range = paste0(FILTER_HELPER_START_COLUMN, EDITOR_HEADER_ROW), col_names = TRUE, reformat = FALSE)
}

write_refresh_status <- function(sheet_id, tab_name, loaded_at) {
  loaded_at_local <- with_tz(loaded_at, tzone = "America/New_York")
  status_message <- paste0(
    "Data updated at ",
    format(loaded_at_local, "%Y-%m-%d %H:%M:%S %Z"),
    ". Manual changes included in BigQuery; dashboards/ext partner reporting update on their normal refresh cadence."
  )
  range_write(
    sheet_id,
    data = tibble::tibble(status_message),
    sheet = tab_name,
    range = REQUEST_STATUS_CELL,
    col_names = FALSE,
    reformat = FALSE
  )
}

find_node_binary <- function() {
  candidates <- c(
    Sys.getenv("MASTER_MANUAL_EDIT_NODE", unset = ""),
    Sys.which("node"),
    "/usr/local/bin/node",
    "/opt/homebrew/bin/node",
    "/usr/bin/node"
  )
  candidates <- unique(candidates[candidates != ""])

  for (candidate in candidates) {
    if (file.exists(candidate) && file.access(candidate, 1) == 0) {
      return(candidate)
    }
  }

  ""
}

repair_filter_ranges <- function(sheet_id, tab_name) {
  repair_script <- file.path(SCRIPT_DIR, "repair_manual_package_editor_filters.mjs")
  node_path <- find_node_binary()
  missing_requirements <- character()
  if (!file.exists(repair_script)) {
    missing_requirements <- c(missing_requirements, paste0("repair script not found at ", repair_script))
  }
  if (node_path == "") {
    missing_requirements <- c(
      missing_requirements,
      "node not found; checked MASTER_MANUAL_EDIT_NODE, PATH, /usr/local/bin/node, /opt/homebrew/bin/node, and /usr/bin/node"
    )
  }
  if (length(missing_requirements) > 0) {
    stop("Cannot repair sheet filters:\n", paste(missing_requirements, collapse = "\n"))
  }

  cat("Repairing filter ranges with ", node_path, " and ", repair_script, "\n", sep = "")

  # The Sheet write can refresh the R client's OAuth token. Refresh again at
  # this process boundary so the Node helper never receives the stale token
  # captured when the loader first authenticated.
  sheets_config <- googlesheets4:::gs4_token()
  sheets_token <- sheets_config$auth_token
  if (is.null(sheets_token) || !is.function(sheets_token$refresh)) {
    stop("Cannot refresh the Google Sheets token for filter repair.", call. = FALSE)
  }
  sheets_token$refresh()
  sheets_access_token <- sheets_token$credentials$access_token
  if (is.null(sheets_access_token) || !nzchar(sheets_access_token)) {
    stop("Google Sheets token refresh produced no access token for filter repair.", call. = FALSE)
  }

  old_env <- Sys.getenv(
    c("MASTER_MANUAL_EDIT_SHEET_ID", "MASTER_MANUAL_EDIT_TAB", "MASTER_MANUAL_EDIT_AUTH_EMAIL", "MASTER_MANUAL_EDIT_ACCESS_TOKEN"),
    unset = NA_character_
  )
  on.exit({
    restore_names <- names(old_env)
    for (name in restore_names) {
      if (is.na(old_env[[name]])) {
        Sys.unsetenv(name)
      } else {
        restore_value <- old_env[[name]]
        names(restore_value) <- name
        do.call(Sys.setenv, as.list(restore_value))
      }
    }
  }, add = TRUE)
  Sys.setenv(
    MASTER_MANUAL_EDIT_SHEET_ID = sheet_id,
    MASTER_MANUAL_EDIT_TAB = tab_name,
    MASTER_MANUAL_EDIT_AUTH_EMAIL = AUTH_EMAIL,
    MASTER_MANUAL_EDIT_ACCESS_TOKEN = sheets_access_token
  )

  output <- system2(node_path, repair_script, stdout = TRUE, stderr = TRUE)
  status <- attr(output, "status") %pick% 0
  if (!identical(as.integer(status), 0L)) {
    stop("Filter range repair failed:\n", paste(output, collapse = "\n"))
  }
  cat(paste(output, collapse = "\n"), "\n")
}

read_first_existing_tab <- function(sheet_id, candidates) {
  existing_names <- sheet_names(sheet_id)
  for (tab_name in candidates) {
    if (tab_name %in% existing_names) {
      if (tab_name == TAB_EDITOR) {
        return(tryCatch(
          read_sheet(sheet_id, range = paste0("'", tab_name, "'!A", EDITOR_HEADER_ROW, ":", EDITOR_VISIBLE_LAST_COLUMN), col_names = TRUE, col_types = "c"),
          error = function(e) tibble::tibble()
        ))
      }

      first_read <- tryCatch(
        read_sheet(sheet_id, sheet = tab_name, col_names = TRUE, col_types = "c"),
        error = function(e) tibble::tibble()
      )
      if ("Package ID" %in% names(first_read)) {
        return(first_read)
      }
      return(tryCatch(
        read_sheet(sheet_id, range = paste0("'", tab_name, "'!A", EDITOR_HEADER_ROW, ":ZZ"), col_names = TRUE, col_types = "c"),
        error = function(e) tibble::tibble()
      ))
    }
  }
  tibble::tibble()
}

pick_existing_col <- function(data, names, default = NA_character_) {
  for (name in names) {
    if (name %in% names(data)) {
      return(data[[name]])
    }
  }
  rep(default, nrow(data))
}

empty_existing_editor <- function() {
  tibble::tibble(
    package_id = character(),
    package_name = character(),
    package_name_friendly = character(),
    supplier_code = character(),
    supplier_name = character(),
    channel = character(),
    flight_start_date = as.Date(character()),
    flight_end_date = as.Date(character()),
    delivery_start_date = as.Date(character()),
    delivery_end_date = as.Date(character()),
    campaign_name = character(),
    initiative = character(),
    advertiser_name = character(),
    package_type = character(),
    media_name = character(),
    ADIF_channel = character(),
    benchmark_kpi = character(),
    benchmark_value = numeric(),
    channel_group = character(),
    manual_edit_at = as.POSIXct(character()),
    manual_edit_by = character(),
    manual_edit_published_at = as.POSIXct(character()),
    manually_edited = character(),
    validation_status = character(),
    validation_reason = character()
  )
}

coalesce_existing_metric <- function(data, display_name, replacement_name, current_name) {
  if (display_name %in% names(data)) {
    return(parse_num(data[[display_name]]))
  }
  replacement <- if (replacement_name %in% names(data)) parse_num(data[[replacement_name]]) else rep(NA_real_, nrow(data))
  current <- if (current_name %in% names(data)) parse_num(data[[current_name]]) else rep(NA_real_, nrow(data))
  dplyr::coalesce(replacement, current)
}

normalize_existing_editor <- function(data) {
  if (nrow(data) == 0) {
    return(empty_existing_editor())
  }

  out <- tibble::tibble(
    package_id = as_trimmed_character(pick_existing_col(data, c("Package ID", "package_id"))),
    package_name = as_trimmed_character(pick_existing_col(data, c("Package Name", "package_name", "package_name_friendly"))),
    package_name_friendly = as_trimmed_character(pick_existing_col(data, c("Package Friendly Name", "package_friendly_name", "package_name_friendly"))),
    supplier_code = as_trimmed_character(pick_existing_col(data, c("Supplier Code", "supplier_code"))),
    supplier_name = as_trimmed_character(pick_existing_col(data, c("Site", "Supplier Name", "Supplier", "supplier_name"))),
    channel = as_trimmed_character(pick_existing_col(data, c("Channel", "channel"))),
    flight_start_date = parse_date(pick_existing_col(data, c("Flight Start Date", "man_flight_start_date", "current_flight_start_date"))),
    flight_end_date = parse_date(pick_existing_col(data, c("Flight End Date", "man_flight_end_date", "current_flight_end_date"))),
    delivery_start_date = parse_date(pick_existing_col(data, c("Delivery Override Start Date", "Start Date", "man_start_date", "current_first_date"))),
    delivery_end_date = parse_date(pick_existing_col(data, c("Delivery Override End Date", "End Date", "man_end_date", "current_last_date"))),
    campaign_name = as_trimmed_character(pick_existing_col(data, c("Campaign", "campaign_name", "campaign_friendly"))),
    initiative = as_trimmed_character(pick_existing_col(data, c("Initiative", "initiative"))),
    advertiser_name = as_trimmed_character(pick_existing_col(data, c("Advertiser", "advertiser_name"))),
    package_type = as_trimmed_character(pick_existing_col(data, c("Package Type", "Media Type", "package_type"))),
    media_name = as_trimmed_character(pick_existing_col(data, c("Media", "media_name"))),
    ADIF_channel = as_trimmed_character(pick_existing_col(data, c("GS Channel", "ADIF Channel", "ADIF_channel"))),
    benchmark_kpi = as_trimmed_character(pick_existing_col(data, c("Benchmark KPI", "benchmark_kpi", "man_benchmark_kpi", "current_benchmark_kpi"))),
    benchmark_value = parse_num(pick_existing_col(data, c("Benchmark Value", "benchmark_value", "man_benchmark_value", "current_benchmark_value"))),
    channel_group = as_trimmed_character(pick_existing_col(data, c("Channel Group", "channel_group"))),
    manual_edit_at = parse_timestamp(pick_existing_col(data, c("Manual Edit At", "manual_edit_at"))),
    manual_edit_by = as_trimmed_character(pick_existing_col(data, c("Manual Edit By", "manual_edit_by"))),
    manual_edit_published_at = parse_timestamp(pick_existing_col(data, c("Manual Edit Published At", "manual_edit_published_at"))),
    manually_edited = as_trimmed_character(pick_existing_col(data, c("Manually Edited?", "manually_edited"))),
    validation_status = as_trimmed_character(pick_existing_col(data, c("Validation Status", "validation_status"))),
    validation_reason = as_trimmed_character(pick_existing_col(data, c("Validation Reason", "validation_messages")))
  )

  for (idx in seq_len(nrow(metric_specs))) {
    spec <- metric_specs[idx, ]
    out[[spec$value_key]] <- coalesce_existing_metric(data, spec$display_col, spec$replacement_col, spec$current_col)
  }

  out %>%
    filter(
      !is.na(package_id) |
        !is.na(package_name) |
        !is.na(supplier_name) |
        !if_all(all_of(metric_specs$value_key), is.na)
    )
}

download_previous_raw <- function() {
  raw_ref <- bq_table(PROJECT_ID, DATASET_ID, RAW_TABLE)
  out <- tryCatch(
    bq_table_download(raw_ref),
    error = function(e) {
      warning("Could not download previous raw manual edits: ", conditionMessage(e))
      tibble::tibble()
    }
  )

  if (nrow(out) == 0) {
    return(tibble::as_tibble(setNames(rep(list(logical()), length(raw_columns)), raw_columns))[0, ])
  }

  missing_cols <- setdiff(raw_columns, names(out))
  for (col in missing_cols) {
    out[[col]] <- NA
  }
  out <- out[, raw_columns, drop = FALSE]
  out$package_id <- as_trimmed_character(out$package_id)
  out$man_start_date <- parse_date(out$man_start_date)
  out$man_end_date <- parse_date(out$man_end_date)
  out$man_flight_start_date <- parse_date(out$man_flight_start_date)
  out$man_flight_end_date <- parse_date(out$man_flight_end_date)
  out$current_first_date <- parse_date(out$current_first_date)
  out$current_last_date <- parse_date(out$current_last_date)
  out$current_flight_start_date <- parse_date(out$current_flight_start_date)
  out$current_flight_end_date <- parse_date(out$current_flight_end_date)
  out$replacement_flight_start_date <- parse_date(out$replacement_flight_start_date)
  out$replacement_flight_end_date <- parse_date(out$replacement_flight_end_date)
  numeric_metadata_cols <- metadata_specs$current_col[metadata_specs$value_type == "numeric"]
  numeric_metadata_cols <- c(numeric_metadata_cols, metadata_specs$manual_col[metadata_specs$value_type == "numeric"])
  for (col in intersect(numeric_metadata_cols, names(out))) {
    out[[col]] <- parse_num(out[[col]])
  }
  out$manual_edit_at <- parse_timestamp(out$manual_edit_at)
  out$manual_edit_published_at <- parse_timestamp(out$manual_edit_published_at)
  out$is_active <- out$is_active %in% TRUE
  out %>%
    filter(!is.na(package_id))
}

is_trusted_previous_manual_row <- function(data) {
  if (nrow(data) == 0) {
    return(logical())
  }

  (data$is_active %in% TRUE) & (
    as_trimmed_character(data$validation_status) %in% "valid" |
      !is.na(parse_timestamp(data$manual_edit_at)) |
      !is.na(as_trimmed_character(data$manual_edit_by)) |
      !is.na(parse_timestamp(data$manual_edit_published_at))
  )
}

previous_raw_to_editor_rows <- function(previous_raw) {
  if (nrow(previous_raw) == 0) {
    return(tibble::tibble())
  }

  previous_raw %>%
    filter(is_trusted_previous_manual_row(.)) %>%
    transmute(
      package_id,
      package_name = coalesce(man_package_name, current_package_name, package_name),
      package_name_friendly = coalesce(man_package_name_friendly, current_package_name_friendly, package_name_friendly, man_package_name, current_package_name, package_name),
      supplier_code = coalesce(man_supplier_code, current_supplier_code, supplier_code),
      supplier_name = coalesce(man_supplier_name, current_supplier_name, supplier_name),
      channel = coalesce(man_channel, current_channel, channel),
      flight_start_date = coalesce(replacement_flight_start_date, man_flight_start_date, man_start_date),
      flight_end_date = coalesce(replacement_flight_end_date, man_flight_end_date, man_end_date),
      planned_spend = coalesce(replacement_planned_spend, current_planned_spend),
      planned_impressions = coalesce(replacement_planned_impressions, current_planned_impressions),
      spend = coalesce(replacement_spend, current_spend),
      impressions = coalesce(replacement_impressions, current_impressions),
      clicks = coalesce(replacement_clicks, current_clicks),
      video_plays = coalesce(replacement_video_plays, current_video_plays),
      video_comps = coalesce(replacement_video_comps, current_video_comps),
      delivery_start_date = man_start_date,
      delivery_end_date = man_end_date,
      campaign_name = coalesce(man_campaign_name, current_campaign_name, campaign_name),
      initiative = coalesce(man_initiative, current_initiative),
      advertiser_name = coalesce(man_advertiser_name, current_advertiser_name, advertiser_name),
      package_type = coalesce(man_package_type, current_package_type, package_type),
      media_name,
      ADIF_channel = coalesce(man_ADIF_channel, current_ADIF_channel, ADIF_channel),
      benchmark_kpi = coalesce(man_benchmark_kpi, current_benchmark_kpi),
      benchmark_value = coalesce(man_benchmark_value, current_benchmark_value),
      channel_group,
      manual_edit_at,
      manual_edit_by,
      manual_edit_published_at,
      manually_edited = "Yes",
      validation_status,
      validation_reason = validation_messages
    )
}

has_user_edit_evidence <- function(manual_edit_at, manual_edit_by) {
  !is.na(parse_timestamp(manual_edit_at)) |
    !is.na(as_trimmed_character(manual_edit_by))
}

has_explicit_editor_manual_evidence <- function(editor_rows) {
  if (nrow(editor_rows) == 0) {
    return(logical())
  }

  pick <- function(col) {
    if (col %in% names(editor_rows)) {
      editor_rows[[col]]
    } else {
      rep(NA_character_, nrow(editor_rows))
    }
  }

  has_user_edit_evidence(pick("manual_edit_at"), pick("manual_edit_by"))
}

merge_previous_manual_editor_rows <- function(editor_rows, previous_editor_rows) {
  if (nrow(previous_editor_rows) == 0) {
    return(editor_rows)
  }
  if (nrow(editor_rows) == 0) {
    return(previous_editor_rows)
  }

  explicit_editor_evidence <- has_explicit_editor_manual_evidence(editor_rows)
  previous_package_ids <- unique(as_trimmed_character(previous_editor_rows$package_id))
  editor_package_ids <- as_trimmed_character(editor_rows$package_id)

  generated_same_package <- !explicit_editor_evidence &
    !is.na(editor_package_ids) &
    editor_package_ids %in% previous_package_ids
  kept_editor_rows <- editor_rows[!generated_same_package, , drop = FALSE]

  kept_explicit_evidence <- has_explicit_editor_manual_evidence(kept_editor_rows)
  editor_keys_with_evidence <- manual_row_key(
    kept_editor_rows$package_id,
    kept_editor_rows$delivery_start_date,
    kept_editor_rows$delivery_end_date
  )[kept_explicit_evidence]
  previous_keys <- manual_row_key(previous_editor_rows$package_id, previous_editor_rows$delivery_start_date, previous_editor_rows$delivery_end_date)
  bind_rows(kept_editor_rows, previous_editor_rows[!(previous_keys %in% editor_keys_with_evidence), , drop = FALSE])
}

choose_metric_value <- function(sheet_value, live_value, prior_current, prior_replacement, prior_replacement_trusted = TRUE) {
  if (is.na(sheet_value)) {
    if (isTRUE(prior_replacement_trusted) && !is.na(prior_replacement) && is.na(live_value) && is.na(prior_current)) {
      return(list(value = prior_replacement, edited = TRUE))
    }
    return(list(value = live_value, edited = FALSE))
  }
  if (!is.na(prior_replacement) && same_num(sheet_value, prior_replacement)) {
    if (!isTRUE(prior_replacement_trusted)) {
      if (is.na(live_value) && is.na(prior_current)) {
        return(list(value = sheet_value, edited = TRUE))
      }
      return(list(value = live_value, edited = FALSE))
    }
    return(list(value = sheet_value, edited = TRUE))
  }
  if (is.na(live_value) && !is.na(prior_current) && same_num(sheet_value, prior_current)) {
    return(list(value = prior_current, edited = FALSE))
  }
  if (same_num(sheet_value, live_value)) {
    return(list(value = live_value, edited = FALSE))
  }
  if (!is.na(prior_current) && same_num(sheet_value, prior_current)) {
    return(list(value = live_value, edited = FALSE))
  }
  list(value = sheet_value, edited = TRUE)
}

same_text <- function(a, b) {
  a <- as_trimmed_character(a)
  b <- as_trimmed_character(b)
  if (length(a) == 0) a <- NA_character_
  if (length(b) == 0) b <- NA_character_
  if (is.na(a[[1]]) && is.na(b[[1]])) return(TRUE)
  if (is.na(a[[1]]) || is.na(b[[1]])) return(FALSE)
  a[[1]] == b[[1]]
}

choose_text_value <- function(sheet_value, live_value, prior_current, prior_replacement, prior_replacement_trusted = TRUE) {
  sheet_value <- as_trimmed_character(sheet_value)
  live_value <- as_trimmed_character(live_value)
  prior_current <- as_trimmed_character(prior_current)
  prior_replacement <- as_trimmed_character(prior_replacement)

  sheet_value <- if (length(sheet_value) == 0) NA_character_ else sheet_value[[1]]
  live_value <- if (length(live_value) == 0) NA_character_ else live_value[[1]]
  prior_current <- if (length(prior_current) == 0) NA_character_ else prior_current[[1]]
  prior_replacement <- if (length(prior_replacement) == 0) NA_character_ else prior_replacement[[1]]

  if (is.na(sheet_value)) {
    if (isTRUE(prior_replacement_trusted) && !is.na(prior_replacement) && is.na(live_value) && is.na(prior_current)) {
      return(list(value = prior_replacement, edited = TRUE))
    }
    return(list(value = live_value, edited = FALSE))
  }
  if (!is.na(prior_replacement) && same_text(sheet_value, prior_replacement)) {
    if (!isTRUE(prior_replacement_trusted)) {
      if (is.na(live_value) && is.na(prior_current)) {
        return(list(value = sheet_value, edited = TRUE))
      }
      return(list(value = live_value, edited = FALSE))
    }
    return(list(value = sheet_value, edited = TRUE))
  }
  if (is.na(live_value) && !is.na(prior_current) && same_text(sheet_value, prior_current)) {
    return(list(value = prior_current, edited = FALSE))
  }
  if (same_text(sheet_value, live_value)) {
    return(list(value = live_value, edited = FALSE))
  }
  if (!is.na(prior_current) && same_text(sheet_value, prior_current)) {
    return(list(value = live_value, edited = FALSE))
  }
  if (is.na(live_value) && is.na(prior_current) && is.na(prior_replacement)) {
    return(list(value = sheet_value, edited = TRUE))
  }
  list(value = sheet_value, edited = TRUE)
}

choose_date_value <- function(sheet_value, live_value, prior_current, prior_manual, prior_active, prior_replacement_trusted = TRUE) {
  if (is.na(sheet_value)) {
    if (isTRUE(prior_replacement_trusted) && prior_active && !is.na(prior_manual) && is.na(live_value) && is.na(prior_current)) {
      return(list(value = prior_manual, edited = TRUE))
    }
    return(list(value = live_value, edited = FALSE))
  }
  if (prior_active && !same_date(prior_manual, prior_current) && same_date(sheet_value, prior_manual)) {
    if (!isTRUE(prior_replacement_trusted)) {
      if (is.na(live_value) && is.na(prior_current)) {
        return(list(value = sheet_value, edited = TRUE))
      }
      return(list(value = live_value, edited = FALSE))
    }
    return(list(value = sheet_value, edited = TRUE))
  }
  if (is.na(live_value) && !is.na(prior_current) && same_date(sheet_value, prior_current)) {
    return(list(value = prior_current, edited = FALSE))
  }
  if (same_date(sheet_value, live_value)) {
    return(list(value = live_value, edited = FALSE))
  }
  if (!is.na(prior_current) && same_date(sheet_value, prior_current)) {
    return(list(value = live_value, edited = FALSE))
  }
  list(value = sheet_value, edited = TRUE)
}

has_manual_audit_evidence <- function(manual_edit_at, manual_edit_by, manual_edit_published_at) {
  has_user_edit_evidence(manual_edit_at, manual_edit_by) |
    !is.na(parse_timestamp(manual_edit_published_at))
}

is_unaudited_source_edit <- function(
    any_edited,
    manual_evidence,
    current_row_count) {
  source_backed <- dplyr::coalesce(parse_num(current_row_count), 0) > 0
  any_edited &&
    !manual_evidence &&
    source_backed
}

should_stop_manual_only_inactive <- function(
    is_manual_only,
    is_active,
    package_id,
    has_replacement,
    has_metadata_replacement,
    has_flight_replacement,
    manual_edit_at,
    manual_edit_by,
    manual_edit_published_at,
    prior_current_row_count,
    sheet_no_edit_status) {
  manual_evidence <- !is.na(parse_timestamp(manual_edit_at)) |
    !is.na(as_trimmed_character(manual_edit_by)) |
    !is.na(parse_timestamp(manual_edit_published_at))
  prior_count <- suppressWarnings(as.numeric(prior_current_row_count))
  prior_count[is.na(prior_count)] <- 0
  stale_source_no_edit <- (prior_count > 0 | sheet_no_edit_status) &
    !has_replacement &
    !has_metadata_replacement &
    !has_flight_replacement &
    !manual_evidence

  is_manual_only &
    !is_active &
    !is.na(as_trimmed_character(package_id)) &
    !stale_source_no_edit
}

manual_override_columns <- function() {
  c(metric_specs$replacement_col, metadata_specs$manual_col, "replacement_flight_start_date", "replacement_flight_end_date")
}

manual_override_field_labels <- function() {
  labels <- c(
    setNames(metric_specs$display_col, metric_specs$replacement_col),
    setNames(metadata_specs$display_col, metadata_specs$manual_col),
    replacement_flight_start_date = "Flight Start Date",
    replacement_flight_end_date = "Flight End Date"
  )
  labels[manual_override_columns()]
}

has_any_manual_override <- function(data) {
  if (nrow(data) == 0) {
    return(logical())
  }
  cols <- intersect(manual_override_columns(), names(data))
  if (length(cols) == 0) {
    return(rep(FALSE, nrow(data)))
  }
  apply(!is.na(data[, cols, drop = FALSE]), 1, any)
}

same_manual_override_value <- function(col, a, b) {
  numeric_manual_cols <- c(metric_specs$replacement_col, metadata_specs$manual_col[metadata_specs$value_type == "numeric"])
  date_manual_cols <- c("replacement_flight_start_date", "replacement_flight_end_date")

  if (col %in% date_manual_cols) {
    return(same_date(a, b))
  }
  if (col %in% numeric_manual_cols) {
    return(same_num(a, b))
  }
  same_text(a, b)
}

has_new_user_audit_evidence <- function(previous_row, proposed_row) {
  previous_edit_at <- parse_timestamp(previous_row$manual_edit_at)[[1]]
  proposed_edit_at <- parse_timestamp(proposed_row$manual_edit_at)[[1]]
  if (!is.na(proposed_edit_at) && (is.na(previous_edit_at) || proposed_edit_at > previous_edit_at)) {
    return(TRUE)
  }

  previous_editor <- as_trimmed_character(previous_row$manual_edit_by)[[1]]
  proposed_editor <- as_trimmed_character(proposed_row$manual_edit_by)[[1]]
  !is.na(proposed_editor) && (is.na(previous_editor) || proposed_editor != previous_editor)
}

lost_manual_override_fields <- function(previous_row, proposed_row) {
  if (has_new_user_audit_evidence(previous_row, proposed_row)) {
    return(character())
  }

  cols <- intersect(manual_override_columns(), intersect(names(previous_row), names(proposed_row)))
  cols <- cols[!is.na(unlist(previous_row[cols], use.names = FALSE))]
  lost_cols <- cols[!vapply(cols, function(col) {
    same_manual_override_value(col, previous_row[[col]][[1]], proposed_row[[col]][[1]])
  }, logical(1))]

  unname(manual_override_field_labels()[lost_cols])
}

detect_manual_edit_loss <- function(previous_raw, proposed_raw) {
  empty_result <- tibble::tibble(
    package_id = character(),
    package_name_friendly = character(),
    man_start_date = as.Date(character()),
    man_end_date = as.Date(character()),
    loss_reason = character(),
    lost_fields = character()
  )
  if (nrow(previous_raw) == 0) {
    return(empty_result)
  }

  previous_manual <- previous_raw[is_trusted_previous_manual_row(previous_raw) & has_any_manual_override(previous_raw), , drop = FALSE]
  if (nrow(previous_manual) == 0) {
    return(empty_result)
  }

  proposed <- proposed_raw
  previous_manual$manual_row_key <- manual_row_key(previous_manual$package_id, previous_manual$man_start_date, previous_manual$man_end_date)
  proposed$manual_row_key <- manual_row_key(proposed$package_id, proposed$man_start_date, proposed$man_end_date)

  loss_rows <- list()
  for (row_idx in seq_len(nrow(previous_manual))) {
    prior <- previous_manual[row_idx, , drop = FALSE]
    same_key_rows <- proposed[proposed$manual_row_key == prior$manual_row_key[[1]], , drop = FALSE]
    active_valid_rows <- same_key_rows[
      same_key_rows$is_active %in% TRUE &
        as_trimmed_character(same_key_rows$validation_status) %in% "valid",
      ,
      drop = FALSE
    ]

    reason <- NA_character_
    fields <- character()
    if (nrow(active_valid_rows) == 0) {
      if (nrow(same_key_rows) == 0) {
        reason <- "missing from proposed upload"
      } else {
        reason <- "would become inactive or blocked"
      }
      prior_override_cols <- intersect(manual_override_columns(), names(prior))
      fields <- unname(manual_override_field_labels()[prior_override_cols[!is.na(unlist(prior[prior_override_cols], use.names = FALSE))]])
    } else {
      lost_by_match <- lapply(seq_len(nrow(active_valid_rows)), function(match_idx) {
        lost_manual_override_fields(prior, active_valid_rows[match_idx, , drop = FALSE])
      })
      if (!any(vapply(lost_by_match, function(fields) length(fields) == 0, logical(1)))) {
        reason <- "would lose previously accepted manual field values"
        fields <- unique(unlist(lost_by_match, use.names = FALSE))
      }
    }

    if (!is.na(reason)) {
      loss_rows[[length(loss_rows) + 1]] <- tibble::tibble(
        package_id = prior$package_id,
        package_name_friendly = coalesce(
          prior$man_package_name_friendly,
          prior$current_package_name_friendly,
          prior$package_name_friendly,
          prior$man_package_name,
          prior$current_package_name,
          prior$package_name
        ),
        man_start_date = prior$man_start_date,
        man_end_date = prior$man_end_date,
        loss_reason = reason,
        lost_fields = paste(sort(unique(fields)), collapse = ", ")
      )
    }
  }

  if (length(loss_rows) == 0) {
    return(empty_result)
  }
  bind_rows(loss_rows) %>%
    arrange(package_name_friendly, package_id, man_start_date, man_end_date)
}

stop_if_manual_edits_would_be_lost <- function(previous_raw, proposed_raw) {
  loss_rows <- detect_manual_edit_loss(previous_raw, proposed_raw)
  if (nrow(loss_rows) == 0) {
    return(invisible(loss_rows))
  }

  preview <- loss_rows %>%
    mutate(
      package_label = if_else(
        is.na(package_name_friendly) | package_name_friendly == "",
        package_id,
        paste0(package_name_friendly, " (", package_id, ")")
      ),
      date_label = paste0(format(man_start_date, "%Y-%m-%d"), " to ", format(man_end_date, "%Y-%m-%d")),
      detail = paste0(package_label, " [", date_label, "]: ", loss_reason, " - ", lost_fields)
    ) %>%
    pull(detail)
  preview <- head(preview, 20)
  more_count <- nrow(loss_rows) - length(preview)
  if (more_count > 0) {
    preview <- c(preview, paste0("...and ", more_count, " more previously accepted manual row(s)."))
  }

  stop(
    "Refusing to publish because this refresh would remove previously accepted manual edits before upload:\n",
    paste(preview, collapse = "\n"),
    "\nRestore or explicitly delete these user-owned edits before rerunning the loader.",
    call. = FALSE
  )
}

apply_manual_only_flight_date_fallback <- function(raw_upload, display_data) {
  manual_only_rows <- coalesce(raw_upload$current_row_count, 0) == 0
  fill_start <- manual_only_rows & is.na(raw_upload$man_flight_start_date) & !is.na(raw_upload$man_start_date)
  fill_end <- manual_only_rows & is.na(raw_upload$man_flight_end_date) & !is.na(raw_upload$man_end_date)

  raw_upload$man_flight_start_date[fill_start] <- raw_upload$man_start_date[fill_start]
  raw_upload$replacement_flight_start_date[fill_start & is.na(raw_upload$replacement_flight_start_date)] <-
    raw_upload$man_start_date[fill_start & is.na(raw_upload$replacement_flight_start_date)]
  raw_upload$man_flight_end_date[fill_end] <- raw_upload$man_end_date[fill_end]
  raw_upload$replacement_flight_end_date[fill_end & is.na(raw_upload$replacement_flight_end_date)] <-
    raw_upload$man_end_date[fill_end & is.na(raw_upload$replacement_flight_end_date)]

  if (!is.null(display_data) && nrow(display_data) == nrow(raw_upload)) {
    display_data$`Flight Start Date`[fill_start] <- raw_upload$man_flight_start_date[fill_start]
    display_data$`Flight End Date`[fill_end] <- raw_upload$man_flight_end_date[fill_end]
  }

  list(raw_upload = raw_upload, display_data = display_data)
}

build_daily_total_proof <- function(daily_data) {
  empty_pass <- tibble::tibble(
    edit_id = NA_character_,
    package_id = NA_character_,
    man_start_date = as.Date(NA),
    man_end_date = as.Date(NA),
    metric_name = "all",
    daily_total = 0,
    replacement_total = 0,
    delta = 0,
    proof_status = "passed",
    proof_message = "No valid manual daily rows to upload."
  )

  if (nrow(daily_data) == 0) {
    return(empty_pass)
  }

  proof_rows <- list()
  for (idx in seq_len(nrow(metric_specs))) {
    daily_col <- metric_specs$daily_col[[idx]]
    total_col <- metric_specs$total_col[[idx]]
    metric_name <- metric_specs$metric_name[[idx]]

    metric_data <- daily_data %>%
      filter(!is.na(.data[[total_col]]))

    if (nrow(metric_data) == 0) {
      next
    }

    proof_rows[[length(proof_rows) + 1]] <- metric_data %>%
      group_by(edit_id, package_id, man_start_date, man_end_date) %>%
      summarise(
        daily_total = sum(.data[[daily_col]], na.rm = TRUE),
        replacement_total = dplyr::first(.data[[total_col]]),
        .groups = "drop"
      ) %>%
      mutate(
        metric_name = metric_name,
        delta = daily_total - replacement_total,
        proof_status = if_else(abs(delta) <= daily_total_proof_tolerance, "passed", "blocked"),
        proof_message = if_else(
          proof_status == "passed",
          "Daily total matches replacement total.",
          "Daily total does not match replacement total."
        )
      ) %>%
      select(
        edit_id, package_id, man_start_date, man_end_date, metric_name,
        daily_total, replacement_total, delta, proof_status, proof_message
      )
  }

  if (length(proof_rows) == 0) {
    return(empty_pass)
  }

  bind_rows(proof_rows) %>%
    arrange(proof_status, edit_id, package_id, metric_name)
}

allocate_daily_total <- function(total, day_count, metric_name) {
  total <- as.numeric(total)
  if (is.na(total)) {
    return(rep(NA_real_, day_count))
  }

  if (day_count <= 0) {
    stop("Cannot allocate a manual edit over zero days.")
  }

  if (metric_name %in% whole_number_metric_names && abs(total - round(total)) <= 0.000001) {
    total_units <- as.integer(round(total))
    base_units <- floor(total_units / day_count)
    remainder_units <- total_units - (base_units * day_count)
    daily_units <- rep(base_units, day_count)
    if (remainder_units > 0) {
      daily_units[seq_len(remainder_units)] <- daily_units[seq_len(remainder_units)] + 1
    }
    return(as.numeric(daily_units))
  }

  base_value <- total / day_count
  daily_values <- rep(base_value, day_count)
  if (day_count > 1) {
    daily_values[[day_count]] <- total - sum(daily_values[-day_count], na.rm = TRUE)
  }
  daily_values
}

refresh_package_lookup <- function(sql_path) {
  if (!file.exists(sql_path)) {
    stop("Manual package lookup SQL not found at ", sql_path, call. = FALSE)
  }

  cat("Refreshing package lookup table from ", sql_path, "...\n", sep = "")
  sql <- paste(readLines(sql_path, warn = FALSE), collapse = "\n")
  invisible(bq_project_query(PROJECT_ID, sql))
  cat("Manual package lookup refresh finished.\n")
}

package_lookup_query <- sprintf("
SELECT *
FROM `%s`
WHERE NOT REGEXP_CONTAINS(LOWER(COALESCE(campaign_name, '')), r'1000heads')
ORDER BY advertiser_name, package_type, channel, campaign_name, initiative, supplier_code, supplier_name, package_name, package_id
", LOOKUP_TABLE)

refresh_package_lookup(LOOKUP_REFRESH_SQL)
cat("Refreshing package editor from ", LOOKUP_TABLE, "...\n", sep = "")
live_packages <- bq_table_download(bq_project_query(PROJECT_ID, package_lookup_query))
live_packages$package_id <- as_trimmed_character(live_packages$package_id)
live_packages <- live_packages %>%
  filter(!is_excluded_social_campaign(campaign_name)) %>%
  mutate(
    current_advertiser_name = advertiser_name,
    current_package_type = package_type,
    current_channel = channel,
    current_campaign_name = campaign_name,
    current_initiative = initiative,
    current_supplier_code = supplier_code,
    current_supplier_name = supplier_name,
    current_package_name = coalesce(package_name, package_name_friendly),
    current_package_name_friendly = coalesce(package_name_friendly, package_name),
    current_ADIF_channel = ADIF_channel,
    current_benchmark_kpi = benchmark_kpi,
    current_benchmark_value = benchmark_value
  )

existing_editor <- read_first_existing_tab(SHEET_ID, c(TAB_EDITOR, "Manual Package Edits"))
existing_editor <- normalize_existing_editor(existing_editor)
previous_raw <- download_previous_raw()

excluded_package_ids <- unique(c(
  live_packages$package_id[is_excluded_social_campaign(live_packages$campaign_name)],
  existing_editor$package_id[is_excluded_social_campaign(existing_editor$campaign_name)],
  previous_raw$package_id[
    is_excluded_social_campaign(previous_raw$campaign_name) |
      is_excluded_social_campaign(previous_raw$current_campaign_name) |
      is_excluded_social_campaign(previous_raw$man_campaign_name)
  ]
))
excluded_package_ids <- excluded_package_ids[!is.na(excluded_package_ids)]

existing_editor <- existing_editor %>%
  filter(!package_id %in% excluded_package_ids, !is_excluded_social_campaign(campaign_name))
previous_raw <- previous_raw %>%
  filter(!package_id %in% excluded_package_ids, !is_excluded_social_campaign(campaign_name))
if (length(excluded_package_ids) > 0) {
  cat(
    "Excluded ", length(excluded_package_ids),
    " manual-editor package id(s) from the 1000heads social source guard.\n",
    sep = ""
  )
}

live_by_package <- split(live_packages, live_packages$package_id)
previous_by_row_key <- if (nrow(previous_raw) > 0) {
  split(previous_raw, manual_row_key(previous_raw$package_id, previous_raw$man_start_date, previous_raw$man_end_date))
} else {
  list()
}

trusted_manual_only_ids <- previous_raw %>%
  filter(is_active, validation_status == "valid") %>%
  pull(package_id) %>%
  unique()
row_sets <- build_source_editor_rows(live_packages, existing_editor, trusted_manual_only_ids)
source_editor_rows <- row_sets$source_editor_rows
previous_editor_rows <- previous_raw_to_editor_rows(previous_raw)
source_editor_rows <- merge_previous_manual_editor_rows(
  source_editor_rows,
  previous_editor_rows %>% filter(package_id %in% source_editor_rows$package_id)
)
previous_manual_only_rows <- previous_editor_rows %>%
  filter(!package_id %in% source_editor_rows$package_id)
manual_only_rows <- merge_previous_manual_editor_rows(
  row_sets$audited_sheet_only_rows,
  previous_manual_only_rows
)
editor_rows <- bind_rows(source_editor_rows, manual_only_rows)

display_rows <- list()
raw_rows <- list()
edited_cells <- list()
raw_prior_current_row_counts <- list()
raw_sheet_no_edit_statuses <- list()

for (row_idx in seq_len(nrow(editor_rows))) {
  sheet <- editor_rows[row_idx, ]
  pkg <- sheet$package_id[[1]]
  live <- live_by_package[[pkg]]
  if (is.null(live)) {
    live <- tibble::tibble()
  } else if (nrow(live) > 1) {
    live <- live[1, , drop = FALSE]
  }

  has_live <- nrow(live) > 0
  has_sheet <- nrow(sheet) > 0

  live_value <- function(col) {
    if (has_live && col %in% names(live)) live[[col]][[1]] else NA
  }
  sheet_value <- function(col) {
    if (has_sheet && col %in% names(sheet)) sheet[[col]][[1]] else NA
  }
  sheet_delivery_start_date <- sheet_value("delivery_start_date")
  sheet_delivery_end_date <- sheet_value("delivery_end_date")
  prior <- previous_by_row_key[[manual_row_key(pkg, sheet_delivery_start_date, sheet_delivery_end_date)]]
  if (is.null(prior)) {
    prior <- tibble::tibble()
  } else if (nrow(prior) > 1) {
    prior <- prior[1, , drop = FALSE]
  }
  has_prior <- nrow(prior) > 0
  prior_value <- function(col) {
    if (has_prior && col %in% names(prior)) prior[[col]][[1]] else NA
  }
  manual_edit_at <- sheet_value("manual_edit_at") %pick% prior_value("manual_edit_at")
  manual_edit_by <- sheet_value("manual_edit_by") %pick% prior_value("manual_edit_by")
  sheet_no_edit_status <- same_text(sheet_value("manually_edited"), "No") &&
    same_text(sheet_value("validation_status"), "inactive") &&
    same_text(sheet_value("validation_reason"), "not edited")
  prior_replacement_trusted <- has_prior && (
    same_text(prior_value("validation_status"), "valid") ||
      !is.na(parse_timestamp(prior_value("manual_edit_at"))) ||
      !is.na(as_trimmed_character(prior_value("manual_edit_by"))[[1]]) ||
      !is.na(parse_timestamp(prior_value("manual_edit_published_at")))
  )

  flight_start_choice <- choose_date_value(
    sheet_value("flight_start_date"),
    live_value("current_flight_start_date"),
    prior_value("current_flight_start_date"),
    prior_value("replacement_flight_start_date"),
    has_prior && prior_value("is_active") %in% TRUE,
    prior_replacement_trusted
  )
  flight_end_choice <- choose_date_value(
    sheet_value("flight_end_date"),
    live_value("current_flight_end_date"),
    prior_value("current_flight_end_date"),
    prior_value("replacement_flight_end_date"),
    has_prior && prior_value("is_active") %in% TRUE,
    prior_replacement_trusted
  )
  delivery_start_choice <- choose_date_value(
    sheet_value("delivery_start_date"),
    live_value("current_first_date"),
    prior_value("current_first_date"),
    prior_value("man_start_date"),
    has_prior && prior_value("is_active") %in% TRUE,
    prior_replacement_trusted
  )
  delivery_end_choice <- choose_date_value(
    sheet_value("delivery_end_date"),
    live_value("current_last_date"),
    prior_value("current_last_date"),
    prior_value("man_end_date"),
    has_prior && prior_value("is_active") %in% TRUE,
    prior_replacement_trusted
  )

  metadata_choices <- list()
  for (metadata_idx in seq_len(nrow(metadata_specs))) {
    spec <- metadata_specs[metadata_idx, ]
    if (identical(spec$value_type[[1]], "numeric")) {
      metadata_choices[[spec$value_key]] <- choose_metric_value(
        sheet_value(spec$value_key),
        live_value(spec$current_col),
        prior_value(spec$current_col),
        prior_value(spec$manual_col),
        prior_replacement_trusted
      )
    } else {
      metadata_choices[[spec$value_key]] <- choose_text_value(
        sheet_value(spec$value_key),
        live_value(spec$current_col),
        prior_value(spec$current_col),
        prior_value(spec$manual_col),
        prior_replacement_trusted
      )
    }
  }

  metric_choices <- list()
  for (metric_idx in seq_len(nrow(metric_specs))) {
    spec <- metric_specs[metric_idx, ]
    metric_choices[[spec$value_key]] <- choose_metric_value(
      sheet_value(spec$value_key),
      live_value(spec$current_col),
      prior_value(spec$current_col),
      prior_value(spec$replacement_col),
      prior_replacement_trusted
    )
  }

  metadata_choice_edited <- any(vapply(metadata_choices, function(choice) isTRUE(choice$edited), logical(1)))
  metric_choice_edited <- any(vapply(metric_choices, function(choice) isTRUE(choice$edited), logical(1)))
  row_has_manual_evidence <- isTRUE(has_user_edit_evidence(
    sheet_value("manual_edit_at"),
    sheet_value("manual_edit_by")
  )[[1]]) || prior_replacement_trusted

  if (is_unaudited_source_edit(
    flight_start_choice$edited || flight_end_choice$edited || metadata_choice_edited || metric_choice_edited,
    row_has_manual_evidence,
    live_value("current_row_count")
  )) {
    flight_start_choice <- list(value = live_value("current_flight_start_date"), edited = FALSE)
    flight_end_choice <- list(value = live_value("current_flight_end_date"), edited = FALSE)
    for (metadata_idx in seq_len(nrow(metadata_specs))) {
      spec <- metadata_specs[metadata_idx, ]
      metadata_choices[[spec$value_key]] <- list(value = live_value(spec$current_col), edited = FALSE)
    }
    for (metric_idx in seq_len(nrow(metric_specs))) {
      spec <- metric_specs[metric_idx, ]
      metric_choices[[spec$value_key]] <- list(value = live_value(spec$current_col), edited = FALSE)
    }
  }

  display <- tibble::tibble(
      `Package ID` = pkg,
      Site = metadata_choices$supplier_name$value,
      `Package Friendly Name` = metadata_choices$package_name_friendly$value %pick% metadata_choices$package_name$value,
      `Flight Start Date` = flight_start_choice$value,
      `Flight End Date` = flight_end_choice$value,
      `Planned Spend` = metric_choices$planned_spend$value,
      `Planned Impressions` = metric_choices$planned_impressions$value,
      Spend = metric_choices$spend$value,
      Impressions = metric_choices$impressions$value,
      Clicks = metric_choices$clicks$value,
      `Video Plays` = metric_choices$video_plays$value,
      `Video Completions` = metric_choices$video_comps$value,
      `Delivery Override Start Date` = delivery_start_choice$value,
      `Delivery Override End Date` = delivery_end_choice$value,
      Advertiser = metadata_choices$advertiser_name$value,
      `Package Type` = metadata_choices$package_type$value,
      Channel = metadata_choices$channel$value,
      Campaign = metadata_choices$campaign_name$value,
      Initiative = metadata_choices$initiative$value,
      `Supplier Code` = metadata_choices$supplier_code$value,
      `Supplier Name` = metadata_choices$supplier_name$value,
      `Package Name` = metadata_choices$package_name$value,
      `GS Channel` = metadata_choices$ADIF_channel$value,
      `Benchmark KPI` = metadata_choices$benchmark_kpi$value,
      `Benchmark Value` = metadata_choices$benchmark_value$value,
      `Manually Edited?` = "",
      `Manual Edit At` = manual_edit_at,
      `Manual Edit By` = manual_edit_by,
      `Manual Edit Published At` = "",
      `Primary Row Data Source` = live_value("qa_row_data_source_primary"),
      `Validation Status` = "",
      `Validation Reason` = "",
      `Baseline Flight Start Date` = live_value("current_flight_start_date"),
      `Baseline Flight End Date` = live_value("current_flight_end_date"),
      `Baseline Planned Spend` = live_value("current_planned_spend"),
      `Baseline Planned Impressions` = live_value("current_planned_impressions"),
      `Baseline Spend` = live_value("current_spend"),
      `Baseline Impressions` = live_value("current_impressions"),
      `Baseline Clicks` = live_value("current_clicks"),
      `Baseline Video Plays` = live_value("current_video_plays"),
      `Baseline Video Completions` = live_value("current_video_comps"),
      `Baseline Delivery Start Date` = live_value("current_first_date"),
      `Baseline Delivery End Date` = live_value("current_last_date"),
      # Text baselines must use the same canonical form as visible text. Raw
      # source values remain preserved in the manual raw audit record below.
      `Baseline Advertiser` = as_trimmed_character(live_value("current_advertiser_name")),
      `Baseline Package Type` = as_trimmed_character(live_value("current_package_type")),
      `Baseline Channel` = as_trimmed_character(live_value("current_channel")),
      `Baseline Campaign` = as_trimmed_character(live_value("current_campaign_name")),
      `Baseline Initiative` = as_trimmed_character(live_value("current_initiative")),
      `Baseline Supplier Code` = as_trimmed_character(live_value("current_supplier_code")),
      `Baseline Supplier Name` = as_trimmed_character(live_value("current_supplier_name")),
      `Baseline Package Name` = as_trimmed_character(live_value("current_package_name")),
      `Baseline Package Friendly Name` = as_trimmed_character(live_value("current_package_name_friendly")),
      `Baseline GS Channel` = as_trimmed_character(live_value("current_ADIF_channel")),
      `Baseline Benchmark KPI` = as_trimmed_character(live_value("current_benchmark_kpi")),
      `Baseline Benchmark Value` = live_value("current_benchmark_value")
    )
  display_rows[[length(display_rows) + 1]] <- display

  raw <- as.list(rep(NA, length(raw_columns)))
  names(raw) <- raw_columns
  raw$edit_id <- stable_edit_id(pkg, row_idx)
  raw$package_id <- pkg
  raw$man_start_date <- delivery_start_choice$value
  raw$man_end_date <- delivery_end_choice$value
  raw$man_flight_start_date <- flight_start_choice$value
  raw$man_flight_end_date <- flight_end_choice$value
  raw$current_row_count <- live_value("current_row_count")
  raw$current_first_date <- live_value("current_first_date")
  raw$current_last_date <- live_value("current_last_date")
  raw$current_flight_start_date <- live_value("current_flight_start_date")
  raw$current_flight_end_date <- live_value("current_flight_end_date")

  raw$advertiser_name <- display$Advertiser
  raw$advertiser_short_name <- live_value("advertiser_short_name")
  raw$campaign_name <- display$Campaign
  raw$campaign_friendly <- live_value("campaign_friendly")
  raw$product_code <- live_value("product_code")
  raw$product_name <- live_value("product_name")
  raw$package_type <- display$`Package Type`
  raw$package_name <- display$`Package Name`
  raw$package_name_friendly <- display$`Package Friendly Name`
  raw$ADIF_channel <- display$`GS Channel`
  raw$placement_id <- live_value("placement_id")
  raw$placement_name <- live_value("placement_name")
  raw$supplier_code <- display$`Supplier Code`
  raw$supplier_name <- display$Site
  raw$supplier_logo <- live_value("supplier_logo")
  raw$p_buy_type <- live_value("p_buy_type")
  raw$p_buy_category <- live_value("p_buy_category")
  raw$channel <- display$Channel
  raw$channel_raw <- live_value("channel_raw")
  raw$channel_group <- as.character(sheet_value("channel_group") %pick% live_value("channel_group") %pick% display$Channel)
  raw$media_name <- as.character(sheet_value("media_name") %pick% live_value("media_name") %pick% display$`Package Type`)
  raw$p_cost_method <- live_value("p_cost_method")
  raw$p_planned_amount_doNotSum <- live_value("p_planned_amount_doNotSum")
  raw$p_planned_impressions_doNotSum <- live_value("p_planned_impressions_doNotSum")
  raw$p_planned_units_doNotSum <- live_value("p_planned_units_doNotSum")
  raw$p_unit_type <- live_value("p_unit_type")
  raw$p_rate <- live_value("p_rate")
  raw$editor_email <- AUTH_EMAIL
  raw$manual_edit_at <- manual_edit_at
  raw$manual_edit_by <- manual_edit_by

  flight_date_edited <- flight_start_choice$edited || flight_end_choice$edited
  delivery_date_edited <- delivery_start_choice$edited || delivery_end_choice$edited
  metadata_edited <- FALSE
  metric_edited <- FALSE

  if (flight_start_choice$edited) {
    raw$replacement_flight_start_date <- flight_start_choice$value
  }
  if (flight_end_choice$edited) {
    raw$replacement_flight_end_date <- flight_end_choice$value
  }

  for (metadata_idx in seq_len(nrow(metadata_specs))) {
    spec <- metadata_specs[metadata_idx, ]
    raw[[spec$current_col]] <- live_value(spec$current_col)
    if (metadata_choices[[spec$value_key]]$edited) {
      raw[[spec$manual_col]] <- metadata_choices[[spec$value_key]]$value
      metadata_edited <- TRUE
      edited_cells[[length(edited_cells) + 1]] <- tibble::tibble(
        row_number = row_idx,
        col_number = match(spec$display_col, display_columns) - 1,
        validation_status = "pending",
        validation_messages = "Manual package metadata edit captured."
      )
    }
  }

  if (flight_date_edited) {
    edited_cells[[length(edited_cells) + 1]] <- tibble::tibble(
      row_number = row_idx,
      col_number = match("Flight Start Date", display_columns) - 1,
      validation_status = "pending",
      validation_messages = "Manual package flight date edit captured."
    )
    edited_cells[[length(edited_cells) + 1]] <- tibble::tibble(
      row_number = row_idx,
      col_number = match("Flight End Date", display_columns) - 1,
      validation_status = "pending",
      validation_messages = "Manual package flight date edit captured."
    )
  }

  for (metric_idx in seq_len(nrow(metric_specs))) {
    spec <- metric_specs[metric_idx, ]
    raw[[spec$current_col]] <- live_value(spec$current_col)
    if (metric_choices[[spec$value_key]]$edited) {
      raw[[spec$replacement_col]] <- metric_choices[[spec$value_key]]$value
      metric_edited <- TRUE
      edited_cells[[length(edited_cells) + 1]] <- tibble::tibble(
        row_number = row_idx,
        col_number = match(spec$display_col, display_columns) - 1,
        validation_status = "pending",
        validation_messages = "Manual metric edit captured."
      )
    }
  }

  raw$is_active <- flight_date_edited || metadata_edited || metric_edited
  raw_rows[[length(raw_rows) + 1]] <- tibble::as_tibble(raw)
  raw_prior_current_row_counts[[length(raw_rows)]] <- parse_num(prior_value("current_row_count"))[[1]]
  raw_sheet_no_edit_statuses[[length(raw_rows)]] <- sheet_no_edit_status
}

if (length(display_rows) == 0) {
  display_data <- tibble::as_tibble(setNames(rep(list(character()), length(display_columns)), display_columns))
  raw_upload <- tibble::as_tibble(setNames(rep(list(character()), length(raw_columns)), raw_columns))
  prior_current_row_count <- numeric()
  sheet_no_edit_status <- logical()
} else {
  display_data <- bind_rows(display_rows)
  raw_upload <- bind_rows(raw_rows)
  prior_current_row_count <- unlist(raw_prior_current_row_counts, use.names = FALSE)
  sheet_no_edit_status <- unlist(raw_sheet_no_edit_statuses, use.names = FALSE)
}

raw_ref <- bq_table(PROJECT_ID, DATASET_ID, RAW_TABLE)
daily_ref <- bq_table(PROJECT_ID, DATASET_ID, DAILY_TABLE)
history_ref <- bq_table(PROJECT_ID, DATASET_ID, HISTORY_TABLE)

for (col in metric_specs$current_col) {
  raw_upload[[col]] <- parse_num(raw_upload[[col]])
}
for (col in metric_specs$replacement_col) {
  raw_upload[[col]] <- parse_num(raw_upload[[col]])
}
numeric_metadata_cols <- metadata_specs$current_col[metadata_specs$value_type == "numeric"]
numeric_metadata_cols <- c(numeric_metadata_cols, metadata_specs$manual_col[metadata_specs$value_type == "numeric"])
for (col in intersect(numeric_metadata_cols, names(raw_upload))) {
  raw_upload[[col]] <- parse_num(raw_upload[[col]])
}
raw_upload$man_start_date <- parse_date(raw_upload$man_start_date)
raw_upload$man_end_date <- parse_date(raw_upload$man_end_date)
raw_upload$man_flight_start_date <- parse_date(raw_upload$man_flight_start_date)
raw_upload$man_flight_end_date <- parse_date(raw_upload$man_flight_end_date)
raw_upload$current_first_date <- parse_date(raw_upload$current_first_date)
raw_upload$current_last_date <- parse_date(raw_upload$current_last_date)
raw_upload$current_flight_start_date <- parse_date(raw_upload$current_flight_start_date)
raw_upload$current_flight_end_date <- parse_date(raw_upload$current_flight_end_date)
raw_upload$replacement_flight_start_date <- parse_date(raw_upload$replacement_flight_start_date)
raw_upload$replacement_flight_end_date <- parse_date(raw_upload$replacement_flight_end_date)
raw_upload$current_row_count <- parse_num(raw_upload$current_row_count)
raw_upload$manual_edit_at <- parse_timestamp(raw_upload$manual_edit_at)
raw_upload$manual_edit_by <- as_trimmed_character(raw_upload$manual_edit_by)

manual_only_fallback <- apply_manual_only_flight_date_fallback(raw_upload, display_data)
raw_upload <- manual_only_fallback$raw_upload
display_data <- manual_only_fallback$display_data

has_replacement <- apply(!is.na(raw_upload[, metric_specs$replacement_col, drop = FALSE]), 1, any)
has_metadata_replacement <- apply(!is.na(raw_upload[, metadata_specs$manual_col, drop = FALSE]), 1, any)
has_flight_replacement <- !is.na(raw_upload$replacement_flight_start_date) | !is.na(raw_upload$replacement_flight_end_date)
planned_replacement_cols <- metric_specs$replacement_col[metric_specs$value_key %in% planned_metric_names]
has_planned_replacement <- apply(!is.na(raw_upload[, planned_replacement_cols, drop = FALSE]), 1, any)
actual_replacement_cols <- metric_specs$replacement_col[!(metric_specs$value_key %in% planned_metric_names)]
has_actual_replacement <- apply(!is.na(raw_upload[, actual_replacement_cols, drop = FALSE]), 1, any)
delivery_date_ok <- !is.na(raw_upload$man_start_date) & !is.na(raw_upload$man_end_date) & raw_upload$man_end_date >= raw_upload$man_start_date
flight_date_ok <- (!has_flight_replacement) | (
  !is.na(raw_upload$man_flight_start_date) &
    !is.na(raw_upload$man_flight_end_date) &
    raw_upload$man_flight_end_date >= raw_upload$man_flight_start_date
)
delivery_date_edited <- raw_upload$is_active & has_replacement & (
  !mapply(same_date, raw_upload$man_start_date, raw_upload$current_first_date) |
    !mapply(same_date, raw_upload$man_end_date, raw_upload$current_last_date)
)
manual_only <- coalesce(raw_upload$current_row_count, 0) == 0
effective_planned_start <- dplyr::coalesce(raw_upload$man_flight_start_date, raw_upload$current_flight_start_date)
effective_planned_end <- dplyr::coalesce(raw_upload$man_flight_end_date, raw_upload$current_flight_end_date)
planned_flight_range_ok <-
  !is.na(effective_planned_start) &
  !is.na(effective_planned_end) &
  effective_planned_start <= effective_planned_end
planned_range_ok <- !has_planned_replacement | planned_flight_range_ok
base_ok <- !is.na(raw_upload$package_id) &
  flight_date_ok &
  ((!has_actual_replacement) | delivery_date_ok) &
  planned_range_ok &
  (has_replacement | has_metadata_replacement | has_flight_replacement)
required_for_new <- c("advertiser_name", "campaign_name", "package_type", "package_name", "ADIF_channel", "supplier_name", "channel", "channel_group", "media_name")
metadata_ok <- apply(!is.na(raw_upload[, required_for_new, drop = FALSE]), 1, all)

duplicate_messages <- rep(NA_character_, nrow(raw_upload))
active_metric_days <- list()
for (i in seq_len(nrow(raw_upload))) {
  if (!raw_upload$is_active[[i]] || !has_replacement[[i]] || is.na(raw_upload$package_id[[i]])) next
  for (metric_idx in seq_len(nrow(metric_specs))) {
    spec <- metric_specs[metric_idx, ]
    if (is.na(raw_upload[[spec$replacement_col]][[i]])) next
    days <- metric_publish_dates(
      spec$metric_name,
      effective_planned_start[[i]], effective_planned_end[[i]],
      raw_upload$man_start_date[[i]], raw_upload$man_end_date[[i]]
    )
    if (length(days) == 0) next
    active_metric_days[[length(active_metric_days) + 1]] <- tibble::tibble(
      edit_row = i,
      package_id = raw_upload$package_id[[i]],
      date = days,
      metric_name = spec$metric_name
    )
  }
}

if (length(active_metric_days) > 0) {
  duplicate_map <- bind_rows(active_metric_days) %>%
    add_count(package_id, date, metric_name, name = "dupe_count") %>%
    filter(dupe_count > 1) %>%
    group_by(edit_row) %>%
    summarise(
      duplicate_detail = paste(sort(unique(metric_name)), collapse = ", "),
      .groups = "drop"
    )
  duplicate_messages[duplicate_map$edit_row] <- paste0("duplicate active package/date metric: ", duplicate_map$duplicate_detail)
}

validation_messages <- vector("list", nrow(raw_upload))
for (i in seq_len(nrow(raw_upload))) {
  msg <- character()
  if (!raw_upload$is_active[[i]]) msg <- c(msg, "not edited")
  if (is.na(raw_upload$package_id[[i]])) msg <- c(msg, "missing package ID")
  if (has_actual_replacement[[i]] && !delivery_date_ok[[i]]) msg <- c(msg, "invalid or missing delivery override date range")
  if (!flight_date_ok[[i]]) msg <- c(msg, "invalid package flight date range")
  if (!planned_range_ok[[i]]) msg <- c(msg, "planned metric edits require valid planned flight start and end dates")
  if (!has_replacement[[i]] && !has_metadata_replacement[[i]] && !has_flight_replacement[[i]] && raw_upload$is_active[[i]]) msg <- c(msg, "no changed values")
  if (manual_only[[i]] && raw_upload$is_active[[i]] && !metadata_ok[[i]]) msg <- c(msg, "new package missing required metadata")
  if (!is.na(duplicate_messages[[i]])) msg <- c(msg, duplicate_messages[[i]])
  validation_messages[[i]] <- paste(msg, collapse = " | ")
}

raw_upload$validation_status <- dplyr::case_when(
  !raw_upload$is_active ~ "inactive",
  base_ok & (!manual_only | metadata_ok) & is.na(duplicate_messages) ~ "valid",
  TRUE ~ "blocked"
)
raw_upload$validation_messages <- unlist(validation_messages)

# A manual-only package has no live model baseline to rebuild from. If a row
# like this is read as blank, rewriting the editor would erase the user's draft.
manual_only_inactive <- should_stop_manual_only_inactive(
  is_manual_only = manual_only,
  is_active = raw_upload$is_active,
  package_id = raw_upload$package_id,
  has_replacement = has_replacement,
  has_metadata_replacement = has_metadata_replacement,
  has_flight_replacement = has_flight_replacement,
  manual_edit_at = raw_upload$manual_edit_at,
  manual_edit_by = raw_upload$manual_edit_by,
  manual_edit_published_at = raw_upload$manual_edit_published_at,
  prior_current_row_count = prior_current_row_count,
  sheet_no_edit_status = sheet_no_edit_status
)
if (any(manual_only_inactive, na.rm = TRUE)) {
  stop(
    "Refusing to rewrite manual-only package rows as inactive/not edited: ",
    paste(raw_upload$package_id[manual_only_inactive], collapse = ", "),
    ". Restore or delete these draft rows before rerunning the loader.",
    call. = FALSE
  )
}

# Existing accepted manual edits are user-owned state. Stop before destructive
# table replacement or Sheet rewrite if the proposed refresh would drop them.
stop_if_manual_edits_would_be_lost(previous_raw, raw_upload)

raw_upload$manual_edit_published_at <- dplyr::if_else(
  raw_upload$is_active & raw_upload$validation_status == "valid",
  loaded_at,
  as.POSIXct(NA)
)

display_data$`Manually Edited?` <- dplyr::case_when(
  raw_upload$is_active & raw_upload$validation_status == "valid" ~ "Yes",
  raw_upload$is_active & raw_upload$validation_status == "blocked" ~ "Blocked",
  TRUE ~ "No"
)
display_data$`Manual Edit At` <- raw_upload$manual_edit_at
display_data$`Manual Edit By` <- raw_upload$manual_edit_by
display_data$`Manual Edit Published At` <- raw_upload$manual_edit_published_at
display_data$`Validation Status` <- raw_upload$validation_status
display_data$`Validation Reason` <- raw_upload$validation_messages

for (metric_idx in seq_len(nrow(metric_specs))) {
  spec <- metric_specs[metric_idx, ]
  raw_upload[[spec$delta_col]] <- ifelse(
    is.na(raw_upload[[spec$replacement_col]]),
    NA_real_,
    raw_upload[[spec$replacement_col]] - coalesce(raw_upload[[spec$current_col]], 0)
  )
}

sheet_info <- gs4_get(SHEET_ID)
source_sheet_url <- sheet_info$spreadsheet_url
source_modified <- tryCatch({
  sheet_drive <- drive_get(as_id(SHEET_ID))
  modified_raw <- sheet_drive$drive_resource[[1]]$modifiedTime
  as.POSIXct(str_replace(modified_raw, "Z$", ""), format = "%Y-%m-%dT%H:%M:%OS", tz = "UTC")
}, error = function(e) as.POSIXct(NA))

raw_upload <- raw_upload %>%
  mutate(
    source_sheet_url = source_sheet_url,
    source_sheet_modified_time = source_modified,
    loaded_at = loaded_at
  )

history_upload <- build_manual_edit_history_rows(raw_upload, loaded_at)
if (nrow(history_upload) > 0) {
  cat("Appending ", nrow(history_upload), " accepted manual edit record(s) to permanent history...\n", sep = "")
  bq_table_upload(history_ref, history_upload, write_disposition = "WRITE_APPEND")
}

cat("Uploading raw package editor rows to ", DATASET_ID, ".", RAW_TABLE, "...\n", sep = "")
bq_table_upload(raw_ref, raw_upload, write_disposition = "WRITE_TRUNCATE")

valid_edits <- raw_upload %>%
  filter(is_active, validation_status == "valid")
valid_publish_edits <- valid_edits %>%
  filter(
    apply(
      !is.na(.[, c(metric_specs$replacement_col, metadata_specs$manual_col, "replacement_flight_start_date", "replacement_flight_end_date"), drop = FALSE]),
      1,
      any
    )
  )

daily_rows <- list()
if (nrow(valid_publish_edits) > 0) {
  for (i in seq_len(nrow(valid_publish_edits))) {
    row <- valid_publish_edits[i, ]
    planned_start_date <- row$man_flight_start_date %pick% row$current_flight_start_date
    planned_end_date <- row$man_flight_end_date %pick% row$current_flight_end_date
    delivery_start_date <- row$man_start_date %pick% row$current_first_date %pick% row$man_flight_start_date
    delivery_end_date <- row$man_end_date %pick% row$current_last_date %pick% row$man_flight_end_date
    metric_ranges <- lapply(seq_len(nrow(metric_specs)), function(metric_idx) {
      spec <- metric_specs[metric_idx, ]
      repl <- row[[spec$replacement_col]]
      if (is.na(repl)) return(NULL)
      metric_publish_dates(
        spec$metric_name,
        planned_start_date, planned_end_date,
        delivery_start_date, delivery_end_date
      )
    })
    metric_ranges <- Filter(function(days) !is.null(days) && length(days) > 0, metric_ranges)
    if (length(metric_ranges) == 0) next
    all_days <- sort(unique(do.call(c, metric_ranges)))
    daily <- tibble::tibble(
      is_active = TRUE,
      validation_status = "valid",
      edit_id = row$edit_id,
      package_id = row$package_id,
      date = all_days,
      man_start_date = delivery_start_date,
      man_end_date = delivery_end_date
    )
    for (metric_idx in seq_len(nrow(metric_specs))) {
      spec <- metric_specs[metric_idx, ]
      repl <- row[[spec$replacement_col]]
      daily[[spec$daily_col]] <- NA_real_
      daily[[spec$total_col]] <- NA_real_
      if (is.na(repl)) next
      metric_days <- metric_publish_dates(
        spec$metric_name,
        planned_start_date, planned_end_date,
        delivery_start_date, delivery_end_date
      )
      metric_day_index <- match(metric_days, daily$date)
      daily[[spec$daily_col]][metric_day_index] <- allocate_daily_total(repl, length(metric_days), spec$metric_name)
      daily[[spec$total_col]] <- repl
    }
    passthrough_cols <- c(
      "advertiser_name", "advertiser_short_name", "campaign_name", "campaign_friendly",
      "product_code", "product_name", "package_type", "package_name", "package_name_friendly",
      "ADIF_channel", "placement_id", "placement_name", "supplier_code", "supplier_name",
      "supplier_logo", "p_buy_type", "p_buy_category", "channel", "channel_raw",
      "channel_group", "media_name", "p_cost_method", "p_planned_amount_doNotSum",
      "p_planned_impressions_doNotSum", "p_planned_units_doNotSum", "p_unit_type",
      "p_rate", "edit_reason", "editor_email", "manual_edit_at", "manual_edit_by",
      "manual_edit_published_at", "man_benchmark_kpi", "man_benchmark_value"
    )
    for (col in passthrough_cols) {
      daily[[col]] <- row[[col]]
    }
    daily$source_sheet_url <- source_sheet_url
    daily$source_sheet_modified_time <- source_modified
    daily$loaded_at <- loaded_at
    daily_rows[[length(daily_rows) + 1]] <- daily
  }
}

if (length(daily_rows) == 0) {
  daily_upload <- tibble::tibble(
    is_active = logical(),
    validation_status = character(),
    edit_id = character(),
    package_id = character(),
    date = as.Date(character()),
    man_start_date = as.Date(character()),
    man_end_date = as.Date(character()),
    man_daily_spend = numeric(),
    man_daily_impressions = numeric(),
    man_daily_planned_spend = numeric(),
    man_daily_planned_impressions = numeric(),
    man_daily_clicks = numeric(),
    man_daily_video_plays = numeric(),
    man_daily_video_comps = numeric(),
    man_total_spend_doNotSum = numeric(),
    man_total_impressions_doNotSum = numeric(),
    man_total_planned_spend_doNotSum = numeric(),
    man_total_planned_impressions_doNotSum = numeric(),
    man_total_clicks_doNotSum = numeric(),
    man_total_video_plays_doNotSum = numeric(),
    man_total_video_comps_doNotSum = numeric(),
    man_benchmark_kpi = character(),
    man_benchmark_value = numeric(),
    advertiser_name = character(),
    advertiser_short_name = character(),
    campaign_name = character(),
    campaign_friendly = character(),
    product_code = character(),
    product_name = character(),
    package_type = character(),
    package_name = character(),
    package_name_friendly = character(),
    ADIF_channel = character(),
    placement_id = character(),
    placement_name = character(),
    supplier_code = character(),
    supplier_name = character(),
    supplier_logo = character(),
    p_buy_type = character(),
    p_buy_category = character(),
    channel = character(),
    channel_raw = character(),
    channel_group = character(),
    media_name = character(),
    p_cost_method = character(),
    p_planned_amount_doNotSum = numeric(),
    p_planned_impressions_doNotSum = numeric(),
    p_planned_units_doNotSum = numeric(),
    p_unit_type = character(),
    p_rate = numeric(),
    edit_reason = character(),
    editor_email = character(),
    manual_edit_at = as.POSIXct(character()),
    manual_edit_by = character(),
    manual_edit_published_at = as.POSIXct(character()),
    source_sheet_url = character(),
    source_sheet_modified_time = as.POSIXct(character()),
    loaded_at = as.POSIXct(character())
  )
} else {
  daily_upload <- bind_rows(daily_rows)
}

daily_total_proof <- build_daily_total_proof(daily_upload)
if (any(daily_total_proof$proof_status == "blocked")) {
  stop("Daily proof failed: one or more daily metric totals do not match edited totals.")
}

cat("Uploading ", nrow(daily_upload), " daily manual rows to ", DATASET_ID, ".", DAILY_TABLE, "...\n", sep = "")
bq_table_upload(daily_ref, daily_upload, write_disposition = "WRITE_TRUNCATE")

edited_cells_df <- if (length(edited_cells) == 0) {
  tibble::tibble(row_number = integer(), col_number = integer(), validation_status = character(), validation_messages = character())
} else {
  bind_rows(edited_cells)
}

if (nrow(edited_cells_df) > 0) {
  edited_cells_df <- edited_cells_df %>%
    mutate(
      package_id = display_data$`Package ID`[row_number],
      validation_status = raw_upload$validation_status[row_number],
      validation_messages = raw_upload$validation_messages[row_number]
    )
}

write_editor_tab(SHEET_ID, TAB_EDITOR, display_data)

manual_marker_data <- tibble::tibble(
  `Manual Marker Flight Start Date` = raw_upload$is_active & raw_upload$validation_status == "valid" & !is.na(raw_upload$replacement_flight_start_date),
  `Manual Marker Flight End Date` = raw_upload$is_active & raw_upload$validation_status == "valid" & !is.na(raw_upload$replacement_flight_end_date),
  `Manual Marker Delivery Start Date` = raw_upload$is_active & raw_upload$validation_status == "valid" & has_replacement & delivery_date_edited,
  `Manual Marker Delivery End Date` = raw_upload$is_active & raw_upload$validation_status == "valid" & has_replacement & delivery_date_edited
)
for (metric_idx in seq_len(nrow(metric_specs))) {
  spec <- metric_specs[metric_idx, ]
  marker_col <- paste("Manual Marker", spec$display_col)
  manual_marker_data[[marker_col]] <- raw_upload$is_active &
    raw_upload$validation_status == "valid" &
    !is.na(raw_upload[[spec$replacement_col]])
}
for (metadata_idx in seq_len(nrow(metadata_specs))) {
  spec <- metadata_specs[metadata_idx, ]
  marker_col <- paste("Manual Marker", spec$display_col)
  manual_marker_data[[marker_col]] <- raw_upload$is_active &
    raw_upload$validation_status == "valid" &
    !is.na(raw_upload[[spec$manual_col]])
}
manual_marker_data <- manual_marker_data[, manual_marker_columns, drop = FALSE]
write_manual_marker_columns(SHEET_ID, TAB_EDITOR, manual_marker_data)
filter_helper_data <- tibble::tibble(`Edited Row Filter` = raw_upload$is_active)
filter_helper_data <- filter_helper_data[, filter_helper_columns, drop = FALSE]
write_filter_helper_columns(SHEET_ID, TAB_EDITOR, filter_helper_data)
write_refresh_status(SHEET_ID, TAB_EDITOR, loaded_at)
repair_filter_ranges(SHEET_ID, TAB_EDITOR)

current_tabs <- tryCatch(
  sheet_names(SHEET_ID),
  error = function(e) {
    warning("Skipping legacy tab cleanup because sheet metadata could not be read after data refresh: ", conditionMessage(e))
    character()
  }
)
if (length(current_tabs) > 0) {
  for (tab_name in LEGACY_TABS) {
    if (tab_name %in% current_tabs && tab_name != TAB_EDITOR) {
      try(sheet_delete(SHEET_ID, sheet = tab_name), silent = TRUE)
    }
  }
  remaining_tabs <- tryCatch(
    sheet_names(SHEET_ID),
    error = function(e) {
      warning("Skipping tab relocation check because sheet metadata could not be read after legacy cleanup: ", conditionMessage(e))
      character()
    }
  )
  if (length(remaining_tabs) > 0 && remaining_tabs[[1]] != TAB_EDITOR) {
    try(sheet_relocate(SHEET_ID, sheet = TAB_EDITOR, .before = 1), silent = TRUE)
  }
}

status <- tibble::tibble(
  loaded_at = loaded_at,
  package_rows_visible = nrow(display_data),
  edited_package_rows = sum(raw_upload$is_active, na.rm = TRUE),
  valid_package_rows = nrow(valid_edits),
  blocked_package_rows = sum(raw_upload$validation_status == "blocked", na.rm = TRUE),
  daily_rows_written = nrow(daily_upload),
  daily_total_proof_status = "passed",
  daily_total_proof_checks = nrow(daily_total_proof),
  raw_table = paste(PROJECT_ID, DATASET_ID, RAW_TABLE, sep = "."),
  history_table = paste(PROJECT_ID, DATASET_ID, HISTORY_TABLE, sep = "."),
  daily_table = paste(PROJECT_ID, DATASET_ID, DAILY_TABLE, sep = ".")
)

cat("\nCompleted manual package editor load.\n")
print(status)
