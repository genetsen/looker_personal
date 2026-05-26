################################################################################
#### TEST MANUAL PACKAGE EDIT CHOICE LOGIC
################################################################################
# These tests cover the small but critical decision rules that decide whether a
# visible sheet value is a real manual override or just the current baseline.
################################################################################

as_trimmed_character <- function(x) {
  out <- trimws(as.character(x))
  out[out == "" | tolower(out) %in% c("na", "nan", "null")] <- NA_character_
  out
}

parse_date <- function(x) {
  if (inherits(x, "Date")) {
    return(x)
  }
  if (inherits(x, "POSIXt")) {
    return(as.Date(x))
  }
  if (is.numeric(x)) {
    return(as.Date(x, origin = "1899-12-30"))
  }
  y <- trimws(as.character(x))
  y[y == "" | tolower(y) %in% c("na", "nan", "null")] <- NA_character_
  parsed <- suppressWarnings(as.Date(y))
  serial_date <- suppressWarnings(as.numeric(y))
  serial_date[is.na(serial_date)] <- NA_real_
  out <- parsed
  fill_serial <- is.na(out) & !is.na(serial_date)
  out[fill_serial] <- as.Date(serial_date[fill_serial], origin = "1899-12-30")
  out
}

same_num <- function(a, b, tol = 0.01) {
  if (is.na(a) && is.na(b)) return(TRUE)
  if (is.na(a) || is.na(b)) return(FALSE)
  abs(as.numeric(a) - as.numeric(b)) <= tol
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

same_date <- function(a, b) {
  a <- parse_date(a)
  b <- parse_date(b)
  if (is.na(a) && is.na(b)) return(TRUE)
  if (is.na(a) || is.na(b)) return(FALSE)
  a == b
}

choose_metric_value <- function(sheet_value, live_value, prior_current, prior_replacement) {
  if (is.na(sheet_value)) {
    return(list(value = live_value, edited = FALSE))
  }
  if (same_num(sheet_value, live_value)) {
    return(list(value = live_value, edited = FALSE))
  }
  if (!is.na(prior_current) && same_num(sheet_value, prior_current)) {
    return(list(value = live_value, edited = FALSE))
  }
  if (!is.na(prior_replacement) && same_num(sheet_value, prior_replacement)) {
    return(list(value = sheet_value, edited = TRUE))
  }
  list(value = sheet_value, edited = TRUE)
}

choose_text_value <- function(sheet_value, live_value, prior_current, prior_replacement) {
  sheet_value <- as_trimmed_character(sheet_value)
  live_value <- as_trimmed_character(live_value)
  prior_current <- as_trimmed_character(prior_current)
  prior_replacement <- as_trimmed_character(prior_replacement)

  sheet_value <- if (length(sheet_value) == 0) NA_character_ else sheet_value[[1]]
  live_value <- if (length(live_value) == 0) NA_character_ else live_value[[1]]
  prior_current <- if (length(prior_current) == 0) NA_character_ else prior_current[[1]]
  prior_replacement <- if (length(prior_replacement) == 0) NA_character_ else prior_replacement[[1]]

  if (is.na(sheet_value)) {
    return(list(value = live_value, edited = FALSE))
  }
  if (same_text(sheet_value, live_value)) {
    return(list(value = live_value, edited = FALSE))
  }
  if (!is.na(prior_current) && same_text(sheet_value, prior_current)) {
    return(list(value = live_value, edited = FALSE))
  }
  if (!is.na(prior_replacement) && !is.na(prior_current) && same_text(sheet_value, prior_replacement)) {
    return(list(value = sheet_value, edited = TRUE))
  }
  if (is.na(live_value) && is.na(prior_current) && is.na(prior_replacement)) {
    return(list(value = sheet_value, edited = FALSE))
  }
  list(value = sheet_value, edited = TRUE)
}

choose_date_value <- function(sheet_value, live_value, prior_current, prior_manual, prior_active) {
  if (is.na(sheet_value)) {
    return(list(value = live_value, edited = FALSE))
  }
  if (same_date(sheet_value, live_value)) {
    return(list(value = live_value, edited = FALSE))
  }
  if (!is.na(prior_current) && same_date(sheet_value, prior_current)) {
    return(list(value = live_value, edited = FALSE))
  }
  if (prior_active && !same_date(prior_manual, prior_current) && same_date(sheet_value, prior_manual)) {
    return(list(value = sheet_value, edited = TRUE))
  }
  list(value = sheet_value, edited = TRUE)
}

expect_choice <- function(label, actual, expected_value, expected_edited) {
  value_ok <- if (inherits(expected_value, "Date")) {
    same_date(actual$value, expected_value)
  } else if (is.numeric(expected_value)) {
    same_num(actual$value, expected_value)
  } else {
    same_text(actual$value, expected_value)
  }
  edited_ok <- identical(actual$edited, expected_edited)
  if (!value_ok || !edited_ok) {
    stop(
      label,
      " failed. Got value=",
      as.character(actual$value),
      " edited=",
      as.character(actual$edited),
      "; expected value=",
      as.character(expected_value),
      " edited=",
      as.character(expected_edited),
      call. = FALSE
    )
  }
  cat("PASS:", label, "\n")
}

expect_choice(
  "false planned-impressions override equal to new PRISMA baseline is undone",
  choose_metric_value(19233094, 19233094, 19024038, 19233094),
  19233094,
  FALSE
)
expect_choice(
  "real existing metric override is preserved when it still differs from current baseline",
  choose_metric_value(20000000, 19233094, 19024038, 20000000),
  20000000,
  TRUE
)
expect_choice(
  "manual metric clears when source baseline catches up to the replacement",
  choose_metric_value(20000000, 20000000, 19024038, 20000000),
  20000000,
  FALSE
)
expect_choice(
  "correction of an existing metric correction captures the new value",
  choose_metric_value(20100000, 19233094, 19024038, 20000000),
  20100000,
  TRUE
)
expect_choice(
  "metric reverted to old current value is treated as undo",
  choose_metric_value(19024038, 19233094, 19024038, 20000000),
  19233094,
  FALSE
)
expect_choice(
  "blank metric cell reverts to current baseline instead of zero",
  choose_metric_value(NA_real_, 123, 100, 999),
  123,
  FALSE
)
expect_choice(
  "zero metric is captured as a real override when current baseline is nonzero",
  choose_metric_value(0, 123, 100, NA_real_),
  0,
  TRUE
)
expect_choice(
  "text override equal to new live baseline is undone",
  choose_text_value("New Channel", "New Channel", "Old Channel", "New Channel"),
  "New Channel",
  FALSE
)
expect_choice(
  "text override persists when live baseline still differs",
  choose_text_value("Corrected Channel", "Old Channel", "Old Channel", "Corrected Channel"),
  "Corrected Channel",
  TRUE
)
expect_choice(
  "date override equal to new live baseline is undone",
  choose_date_value(as.Date("2025-10-01"), as.Date("2025-10-01"), as.Date("2025-09-30"), as.Date("2025-10-01"), TRUE),
  as.Date("2025-10-01"),
  FALSE
)
expect_choice(
  "date override persists when live baseline still differs",
  choose_date_value(as.Date("2025-10-02"), as.Date("2025-10-01"), as.Date("2025-10-01"), as.Date("2025-10-02"), TRUE),
  as.Date("2025-10-02"),
  TRUE
)
expect_choice(
  "blank date cell reverts to current baseline",
  choose_date_value(as.Date(NA), as.Date("2025-10-01"), as.Date("2025-09-30"), as.Date("2025-10-02"), TRUE),
  as.Date("2025-10-01"),
  FALSE
)

cat("All manual edit choice logic tests passed.\n")
