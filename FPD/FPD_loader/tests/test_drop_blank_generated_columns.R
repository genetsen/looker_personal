# Regression check for generated blank spreadsheet columns.
#
# This test parses only the drop_blank_generated_columns() helper from the loader
# script, instead of sourcing the full script. Sourcing the full loader would run
# Google Drive, Sheets, and BigQuery side effects, which is not safe for a small
# unit check.

library(dplyr)
library(tibble)

`%||%` <- function(x, y) {
  if (is.null(x) || is.na(x)) y else x
}

file_arg <- sub("^--file=", "", commandArgs(FALSE)[grepl("^--file=", commandArgs(FALSE))][1])
test_path <- normalizePath(file_arg)
script_path <- file.path(dirname(dirname(test_path)), "util_collect_fpd_shortcutsFolder.r")

exprs <- parse(script_path)
helper_idx <- which(vapply(
  exprs,
  function(expr) {
    is.call(expr) &&
      identical(expr[[1]], as.name("<-")) &&
      identical(expr[[2]], as.name("drop_blank_generated_columns"))
  },
  logical(1)
))

if (length(helper_idx) != 1) {
  stop("Expected exactly one drop_blank_generated_columns() definition.")
}

eval(exprs[[helper_idx]], envir = globalenv())

sample_df <- tibble(
  keep_dimension = c("a", "b"),
  x24 = c(NA, NA),
  x25 = c(NA, ""),
  x26 = c(NA, "keep")
)
sample_df[["...24"]] <- c(NA, NA)
sample_df[["...25"]] <- c("", NA)

cleaned_df <- drop_blank_generated_columns(sample_df)

expected_present <- c("keep_dimension", "x26")
expected_absent <- c("x24", "x25", "...24", "...25")

missing_expected <- setdiff(expected_present, names(cleaned_df))
unexpected_present <- intersect(expected_absent, names(cleaned_df))

if (length(missing_expected) > 0 || length(unexpected_present) > 0) {
  stop(
    paste(
      "Generated blank column cleanup mismatch.",
      "Missing expected:",
      paste(missing_expected, collapse = ", "),
      "Unexpected present:",
      paste(unexpected_present, collapse = ", ")
    )
  )
}

cat("drop_blank_generated_columns regression check passed\n")
