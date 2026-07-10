# Reads the rollback Manual Data Editor copy and writes local inspection CSVs.
# This is a read-only Google Sheets snapshot helper. It does not edit Sheets,
# Drive, BigQuery, or any production workflow.

suppressPackageStartupMessages({
  library(googlesheets4)
  library(gargle)
  library(dplyr)
  library(readr)
  library(stringr)
})

out_dir <- "/Users/eugenetsenter/Looker_clonedRepo/looker_personal/master_data_model/manual_package_edits/ai_context_summaries"
adc_path <- "/Users/eugenetsenter/.config/gcloud-giantspoon/application_default_credentials.json"

token <- gargle::credentials_app_default(
  scopes = c(
    "https://www.googleapis.com/auth/spreadsheets.readonly",
    "https://www.googleapis.com/auth/drive.readonly"
  ),
  path = adc_path
)

gs4_auth(token = token)

workbooks <- list(
  rollback_before_manual_edit_audit_2026_06_23 = "1JwXEUVsfEhvb_HpIm0qoxBpM7yIJhkvonAfTyDX8IRE",
  archived_manual_data_editor = "1WerhrbBMggzCwIUCOsOCV33aHygV96jt1HgqiYcUHZo",
  current_manual_data_editor = "1p1aGAg8lMk7JvUKCJBKRj5rKQNNYL3iEKnl0kPHvZ7E"
)

safe_col <- function(data, name) {
  if (name %in% names(data)) {
    return(as.character(data[[name]]))
  }
  rep(NA_character_, nrow(data))
}

coalesce_text <- function(...) {
  values <- list(...)
  out <- values[[1]]
  for (value in values[-1]) {
    out[is.na(out) | !nzchar(trimws(out))] <- value[is.na(out) | !nzchar(trimws(out))]
  }
  out
}

for (workbook_name in names(workbooks)) {
  sheet_id <- workbooks[[workbook_name]]
  tabs <- sheet_names(sheet_id)
  write_csv(
    tibble(tab_name = tabs),
    file.path(out_dir, paste0("2026-07-09-", workbook_name, "-tabs.csv"))
  )

  data <- read_sheet(
    sheet_id,
    sheet = "Package Editor",
    range = "A4:ZZ",
    col_names = TRUE,
    .name_repair = "minimal",
    guess_max = 10000
  )

  names(data) <- trimws(names(data))
  data <- data |>
    mutate(sheet_row = row_number() + 4, .before = 1) |>
    mutate(across(everything(), as.character))

  headers <- tibble(column_index = seq_along(names(data)), header = names(data))
  write_csv(
    headers,
    file.path(out_dir, paste0("2026-07-09-", workbook_name, "-headers.csv"))
  )

  package_id <- trimws(safe_col(data, "Package ID"))
  package_friendly_name <- trimws(safe_col(data, "Package Friendly Name"))
  package_name <- trimws(safe_col(data, "Package Name"))
  campaign_name <- trimws(safe_col(data, "Campaign Name"))
  validation_status <- tolower(trimws(safe_col(data, "Validation Status")))
  manually_edited <- tolower(trimws(safe_col(data, "Manually Edited?")))
  manual_edit_at <- trimws(safe_col(data, "Manual Edit At"))
  manual_edit_by <- trimws(safe_col(data, "Manual Edit By"))
  manual_published_at <- trimws(safe_col(data, "Manual Edit Published At"))

  marker_cols <- names(data)[str_detect(tolower(names(data)), "manual marker|manual_marker|^man_")]
  marker_cols <- setdiff(marker_cols, c("Manually Edited?", "Manual Edit At", "Manual Edit By", "Manual Edit Published At"))
  marker_values <- if (length(marker_cols) == 0) {
    rep(FALSE, nrow(data))
  } else {
    apply(data[, marker_cols, drop = FALSE], 1, function(row) {
      any(tolower(trimws(as.character(row))) %in% c("true", "yes", "1"))
    })
  }

  looks_manual <- !is.na(package_id) & nzchar(package_id) & (
    validation_status %in% c("valid", "blocked") |
      manually_edited %in% c("yes", "blocked", "true") |
      (!is.na(manual_edit_at) & nzchar(manual_edit_at)) |
      (!is.na(manual_edit_by) & nzchar(manual_edit_by)) |
      (!is.na(manual_published_at) & nzchar(manual_published_at)) |
      marker_values
  )

  key_match <- str_detect(tolower(coalesce_text(package_id, "")), "ccdooh") |
    str_detect(tolower(coalesce_text(package_friendly_name, package_name, campaign_name, "")), "columbus circle|purelyelizabeth|purely elizabeth")

  compact_cols <- unique(c(
    "sheet_row",
    "Package ID",
    "Advertiser",
    "Site",
    "Package Friendly Name",
    "Package Name",
    "Campaign Name",
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
    "Benchmark KPI",
    "Benchmark Value",
    "Manually Edited?",
    "Manual Edit At",
    "Manual Edit By",
    "Manual Edit Published At",
    "Validation Status",
    "Validation Reason",
    marker_cols
  ))
  compact_cols <- intersect(compact_cols, names(data))

  manual_rows <- data[looks_manual, compact_cols, drop = FALSE]
  key_rows <- data[key_match, compact_cols, drop = FALSE]

  write_csv(
    manual_rows,
    file.path(out_dir, paste0("2026-07-09-", workbook_name, "-manual-like-rows.csv")),
    na = ""
  )
  write_csv(
    key_rows,
    file.path(out_dir, paste0("2026-07-09-", workbook_name, "-ccdooh-columbus-rows.csv")),
    na = ""
  )

  cat(
    workbook_name,
    "tabs=", length(tabs),
    "rows=", nrow(data),
    "cols=", ncol(data),
    "manual_like=", nrow(manual_rows),
    "key_rows=", nrow(key_rows),
    "\n"
  )
}
