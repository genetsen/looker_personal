################################################################################
#### TEST MANUAL DATA EDITOR ROW AND METRIC-DATE CONTRACTS
################################################################################
# Purpose:
#   Prove the row-authority and planned-versus-delivery rules without opening
#   the live Google Sheet or writing any BigQuery data.
################################################################################

suppressPackageStartupMessages({
  library(dplyr)
  library(tibble)
})

test_file <- sub("^--file=", "", commandArgs(trailingOnly = FALSE)[startsWith(commandArgs(trailingOnly = FALSE), "--file=")])
source(file.path(dirname(normalizePath(test_file[[1]])), "..", "manual_editor_contracts.R"))

expect_true <- function(label, value) {
  if (!isTRUE(value)) stop("FAILED: ", label, call. = FALSE)
  cat("PASS: ", label, "\n", sep = "")
}

expect_error <- function(label, code) {
  failed <- inherits(try(force(code), silent = TRUE), "try-error")
  expect_true(label, failed)
}

source_rows <- tibble(
  package_id = "SOURCE-1"
)
fake_leftover_sheet <- tibble(
  package_id = c("SOURCE-1", "P3HN485"),
  manually_edited = c("No", "No"),
  validation_status = c("inactive", "inactive"),
  validation_reason = c("not edited", "not edited"),
  package_name = c(NA_character_, NA_character_)
)
row_set <- build_source_editor_rows(source_rows, fake_leftover_sheet)
expect_true("stale inactive Sheet-only row cannot create a visible row", identical(row_set$source_editor_rows$package_id, "SOURCE-1"))
expect_true("stale inactive Sheet-only row is not admitted as manual-only", nrow(row_set$audited_sheet_only_rows) == 0)

audited_manual_only_sheet <- tibble(
  package_id = "MANUAL-1",
  manually_edited = "Yes",
  validation_status = "blocked",
  validation_reason = "missing metadata",
  manual_edit_at = "2026-07-17 10:00:00",
  package_name = "Manual package"
)
row_set <- build_source_editor_rows(source_rows, audited_manual_only_sheet)
expect_true("audited Sheet-only package is retained for manual-only validation", identical(row_set$audited_sheet_only_rows$package_id, "MANUAL-1"))

prior_accepted_manual_only_sheet <- audited_manual_only_sheet
prior_accepted_manual_only_sheet$manual_edit_at <- NA_character_
prior_accepted_manual_only_sheet$manually_edited <- "Yes"
prior_accepted_manual_only_sheet$validation_status <- "valid"
row_set <- build_source_editor_rows(source_rows, prior_accepted_manual_only_sheet, trusted_manual_only_ids = "MANUAL-1")
expect_true("prior accepted Sheet-only package is retained without a loose audit stamp", identical(row_set$audited_sheet_only_rows$package_id, "MANUAL-1"))

duplicate_manual_only_sheet <- prior_accepted_manual_only_sheet %>% bind_rows(prior_accepted_manual_only_sheet)
duplicate_manual_only_sheet$manual_edit_at <- c("2026-07-17 10:00:00", "2026-07-17 10:01:00")
duplicate_manual_only_sheet$validation_status <- c("valid", "inactive")
duplicate_manual_only_sheet$manually_edited <- c("Yes", "No")
row_set <- build_source_editor_rows(source_rows, duplicate_manual_only_sheet, trusted_manual_only_ids = "MANUAL-1")
expect_true("one accepted manual-only row wins over inactive audit leftovers", nrow(row_set$audited_sheet_only_rows) == 1 && row_set$audited_sheet_only_rows$validation_status[[1]] == "valid")

multiple_accepted_manual_only_sheet <- duplicate_manual_only_sheet
multiple_accepted_manual_only_sheet$validation_status <- "valid"
multiple_accepted_manual_only_sheet$manually_edited <- "Yes"
row_set <- build_source_editor_rows(source_rows, multiple_accepted_manual_only_sheet, trusted_manual_only_ids = "MANUAL-1")
expect_true("multiple accepted-looking trusted Sheet-only rows defer to the raw record", nrow(row_set$audited_sheet_only_rows) == 0)

unaudited_values_sheet <- audited_manual_only_sheet
unaudited_values_sheet$manual_edit_at <- NA_character_
unaudited_values_sheet$manually_edited <- ""
expect_error("unaudited Sheet-only values stop before a clear/write", build_source_editor_rows(source_rows, unaudited_values_sheet))

duplicate_source_sheet <- fake_leftover_sheet[1, , drop = FALSE] %>% bind_rows(fake_leftover_sheet[1, , drop = FALSE])
row_set <- build_source_editor_rows(source_rows, duplicate_source_sheet)
expect_true("identical generated Sheet duplicates collapse to one source row", nrow(row_set$source_editor_rows) == 1 && row_set$source_editor_rows$package_id[[1]] == "SOURCE-1")

conflicting_duplicate_source_sheet <- duplicate_source_sheet
conflicting_duplicate_source_sheet$package_name[[2]] <- "conflicting value"
row_set <- build_source_editor_rows(source_rows, conflicting_duplicate_source_sheet)
expect_true("conflicting unaudited generated duplicates still collapse to one source row", nrow(row_set$source_editor_rows) == 1 && row_set$source_editor_rows$package_id[[1]] == "SOURCE-1")

source_with_dates <- tibble(
  package_id = "SOURCE-DATE-1",
  current_first_date = as.Date("2026-06-08"),
  current_last_date = as.Date("2026-06-30")
)
stale_date_duplicate_sheet <- tibble(
  package_id = c("SOURCE-DATE-1", "SOURCE-DATE-1"),
  delivery_start_date = c("2026-06-08", "2026-06-08"),
  delivery_end_date = c("2026-06-14", "2026-06-30"),
  manually_edited = c("No", "No"),
  validation_status = c("inactive", "inactive"),
  validation_reason = c("not edited", "not edited")
)
row_set <- build_source_editor_rows(source_with_dates, stale_date_duplicate_sheet)
expect_true("conflicting generated duplicates select the live-source baseline row", row_set$source_editor_rows$delivery_end_date[[1]] == "2026-06-30")

planned_range <- metric_publish_range("planned_spend", as.Date("2026-01-01"), as.Date("2026-01-31"), as.Date("2026-01-07"), as.Date("2026-01-28"))
actual_range <- metric_publish_range("spend", as.Date("2026-01-01"), as.Date("2026-01-31"), as.Date("2026-01-07"), as.Date("2026-01-28"))
expect_true("planned spend uses the planned flight start", planned_range$start_date == as.Date("2026-01-01"))
expect_true("planned spend uses the planned flight end", planned_range$end_date == as.Date("2026-01-31"))
expect_true("actual spend uses the delivery override start", actual_range$start_date == as.Date("2026-01-07"))
expect_true("actual spend uses the delivery override end", actual_range$end_date == as.Date("2026-01-28"))

planned_dates <- metric_publish_dates("planned_spend", as.Date("2026-01-01"), as.Date("2026-01-31"), as.Date("2026-01-07"), as.Date("2026-01-28"))
actual_dates <- metric_publish_dates("spend", as.Date("2026-01-01"), as.Date("2026-01-31"), as.Date("2026-01-07"), as.Date("2026-01-28"))
expect_true("planned allocation covers all 31 planned-flight days", length(planned_dates) == 31 && planned_dates[[1]] == as.Date("2026-01-01") && planned_dates[[31]] == as.Date("2026-01-31"))
expect_true("actual allocation covers only the 22 delivery-override days", length(actual_dates) == 22 && actual_dates[[1]] == as.Date("2026-01-07") && actual_dates[[22]] == as.Date("2026-01-28"))

history_input <- tibble(
  is_active = c(TRUE, TRUE, FALSE),
  edit_id = c("EDIT 1", "EDIT-2", "EDIT-3"),
  package_id = c("PACKAGE-1", "PACKAGE-2", "PACKAGE-3"),
  validation_status = c("valid", "blocked", "valid")
)
history_rows <- build_manual_edit_history_rows(history_input, as.POSIXct("2026-07-17 15:30:00", tz = "UTC"))
expect_true("history records only accepted edits", nrow(history_rows) == 1 && history_rows$edit_id[[1]] == "EDIT 1")
expect_true("history identifies the accepted loader state", history_rows$history_event_type[[1]] == "accepted_snapshot" && history_rows$history_source[[1]] == "manual_package_editor_loader")
expect_true("history gives each record a stable run-specific ID", grepl("manual-edit-history-EDIT_1-20260717T153000", history_rows$history_event_id[[1]], fixed = TRUE))

expect_true("editor rewrite clear range includes every existing grid row", identical(
  editor_data_clear_range("Package Editor", 4, "CA", 2466),
  "'Package Editor'!A4:CA2466"
))
