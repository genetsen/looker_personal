# Read-only Manual Package Editor sheet snapshot helper.
#
# This script reads the visible Package Editor tab from one or more Google Sheets
# workbooks and writes compact local CSV snapshots of rows that look active,
# edited, valid, or blocked. It does not write to Google Sheets or BigQuery.

suppressPackageStartupMessages({
  library(googlesheets4)
  library(gargle)
  library(dplyr)
  library(readr)
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
  current_production = "1p1aGAg8lMk7JvUKCJBKRj5rKQNNYL3iEKnl0kPHvZ7E",
  older_editor = "1WerhrbBMggzCwIUCOsOCV33aHygV96jt1HgqiYcUHZo"
)

wanted_names <- c(
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
  "Validation Reason"
)

normalize_names <- function(data) {
  names(data) <- trimws(names(data))
  data
}

safe_col <- function(data, name) {
  if (name %in% names(data)) {
    return(as.character(data[[name]]))
  }
  rep(NA_character_, nrow(data))
}

for (workbook_name in names(workbooks)) {
  sheet_id <- workbooks[[workbook_name]]
  tabs <- sheet_names(sheet_id)
  tabs_path <- file.path(out_dir, paste0("2026-07-08-", workbook_name, "-tabs.csv"))
  write_csv(tibble(tab_name = tabs), tabs_path)

  data <- read_sheet(
    sheet_id,
    sheet = "Package Editor",
    range = "A4:AF",
    col_names = TRUE,
    .name_repair = "minimal",
    guess_max = 10000
  ) |>
    normalize_names()

  package_id <- trimws(safe_col(data, "Package ID"))
  status <- tolower(trimws(safe_col(data, "Validation Status")))
  edited <- trimws(safe_col(data, "Manually Edited?"))
  published_at <- trimws(safe_col(data, "Manual Edit Published At"))
  manual_at <- trimws(safe_col(data, "Manual Edit At"))
  manual_by <- trimws(safe_col(data, "Manual Edit By"))

  active <- !is.na(package_id) & package_id != "" &
    (
      status %in% c("valid", "blocked") |
        edited %in% c("Yes", "Blocked") |
        (!is.na(published_at) & published_at != "") |
        (!is.na(manual_at) & manual_at != "") |
        (!is.na(manual_by) & manual_by != "")
    )

  compact <- data[active, intersect(wanted_names, names(data)), drop = FALSE]
  compact <- compact |>
    mutate(across(everything(), as.character))

  out_path <- file.path(out_dir, paste0("2026-07-08-", workbook_name, "-active-like-rows.csv"))
  write_csv(compact, out_path, na = "")

  cat(workbook_name, "tabs=", length(tabs), "rows=", nrow(data), "active_like=", nrow(compact), "output=", out_path, "\n")
}
