# Regression check for mixed-type partner placement values.
#
# Google Sheets can return a list-column when one placement column contains both
# text and dates. This test parses only the coercion helper from the loader so it
# does not trigger Google Drive, Sheets, or BigQuery side effects.

file_arg <- sub("^--file=", "", commandArgs(FALSE)[grepl("^--file=", commandArgs(FALSE))][1])
test_path <- normalizePath(file_arg)
script_path <- file.path(dirname(dirname(test_path)), "util_collect_fpd_shortcutsFolder.r")

exprs <- parse(script_path)
helper_idx <- which(vapply(
  exprs,
  function(expr) {
    is.call(expr) &&
      identical(expr[[1]], as.name("<-")) &&
      identical(expr[[2]], as.name("coerce_partner_placement_name"))
  },
  logical(1)
))

if (length(helper_idx) != 1) {
  stop("Expected exactly one coerce_partner_placement_name() definition.")
}

eval(exprs[[helper_idx]], envir = globalenv())

mixed_values <- list(
  "FriendsKeepSecretsHostReadsQ1",
  as.POSIXct("2026-03-08", tz = "UTC"),
  NULL
)

actual <- coerce_partner_placement_name(mixed_values)
expected <- c("FriendsKeepSecretsHostReadsQ1", "2026-03-08", NA_character_)

if (!is.character(actual) || !identical(actual, expected)) {
  stop(
    paste(
      "partner_placement_name coercion mismatch.",
      "Expected:",
      paste(expected, collapse = ", "),
      "Actual:",
      paste(actual, collapse = ", ")
    )
  )
}

cat("partner_placement_name character coercion regression check passed\n")
