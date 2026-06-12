################################################################################
#### TEST MANUAL PACKAGE EDIT DAILY TOTAL PROOF
################################################################################
# Purpose:
#   Protect the daily-allocation proof used by the Manual Data Editor loader.
#   The loader must split edited metric totals into daily rows while preserving
#   the replacement total that will later flow into BigQuery.
################################################################################

suppressPackageStartupMessages({
  library(dplyr)
  library(tibble)
})

whole_number_metric_names <- c("impressions", "planned_impressions", "clicks", "video_plays", "video_comps")
daily_total_proof_tolerance <- 0.01

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

expect_total_preserved <- function(label, total, day_count, metric_name) {
  daily_values <- allocate_daily_total(total, day_count, metric_name)
  delta <- sum(daily_values, na.rm = TRUE) - total
  if (abs(delta) > 0.000001) {
    stop(
      label,
      " failed. Daily total=",
      as.character(sum(daily_values, na.rm = TRUE)),
      "; replacement total=",
      as.character(total),
      "; delta=",
      as.character(delta),
      call. = FALSE
    )
  }
  cat("PASS:", label, "\n")
}

expect_proof_status <- function(label, delta, expected_status) {
  actual_status <- ifelse(abs(delta) <= daily_total_proof_tolerance, "passed", "blocked")
  if (!identical(actual_status, expected_status)) {
    stop(
      label,
      " failed. Got status=",
      actual_status,
      "; expected status=",
      expected_status,
      "; delta=",
      as.character(delta),
      call. = FALSE
    )
  }
  cat("PASS:", label, "\n")
}

expect_total_preserved(
  "fractional-cent spend total is preserved",
  819.621,
  1,
  "spend"
)

expect_total_preserved(
  "whole-number impressions allocate as whole units",
  3841456,
  29,
  "impressions"
)

expect_proof_status(
  "sub-cent daily proof delta is allowed",
  -0.001,
  "passed"
)

expect_proof_status(
  "material daily proof delta is blocked",
  0.02,
  "blocked"
)

cat("All manual edit daily total proof tests passed.\n")
