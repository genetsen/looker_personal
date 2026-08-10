# Offline regression tests for FPD BigQuery publication safeguards.
#
# These checks reproduce the blank-numeric-column and completed-failed-job
# conditions without authenticating, submitting jobs, or writing live data.

file_arg <- sub(
  "^--file=",
  "",
  commandArgs(FALSE)[grepl("^--file=", commandArgs(FALSE))][1]
)
test_path <- normalizePath(file_arg)
loader_dir <- dirname(dirname(test_path))
source(file.path(loader_dir, "fpd_bigquery_safety.R"))

blank_metric <- data.frame(
  video_watch_time = rep(NA, 3),
  label = c("a", "b", "c"),
  stringsAsFactors = FALSE
)
coerced <- coerce_blank_logicals_to_existing_numeric_types(
  blank_metric,
  c(video_watch_time = "FLOAT64", label = "STRING")
)
if (!is.numeric(coerced$video_watch_time) || !all(is.na(coerced$video_watch_time))) {
  stop("Blank metrics must preserve an existing numeric production type.")
}

actual_error_message <- paste(
  "Query column 53 has type STRING which cannot be inserted into",
  "column video_watch_time, which has type FLOAT64"
)
failed_status <- list(
  state = "DONE",
  errorResult = list(reason = "invalidQuery", message = actual_error_message)
)
failed_job_id <- "job_recorded_fpd_failure"
captured_error <- tryCatch(
  {
    wait_for_fpd_bq_job_success(
      job = list(job = failed_job_id),
      wait_fn = function(job) invisible(job),
      status_fn = function(job) failed_status
    )
    NULL
  },
  error = identity
)
if (is.null(captured_error)) {
  stop("A completed BigQuery job with errorResult must fail the loader.")
}
required_text <- c("DATA LOAD FAILURE", "invalidQuery", actual_error_message, failed_job_id)
if (any(!vapply(
  required_text,
  grepl,
  logical(1),
  x = conditionMessage(captured_error),
  fixed = TRUE
))) {
  stop("The BigQuery failure message must preserve its job ID, reason, and error text.")
}

success_job <- wait_for_fpd_bq_job_success(
  job = list(job = "job_success"),
  wait_fn = function(job) invisible(job),
  status_fn = function(job) list(state = "DONE")
)
if (!identical(success_job$job, "job_success")) {
  stop("A confirmed successful BigQuery job must return normally.")
}

uncertain_error <- tryCatch(
  {
    wait_for_fpd_bq_job_success(
      job = list(job = "job_unknown"),
      wait_fn = function(job) invisible(job),
      status_fn = function(job) list(state = "RUNNING")
    )
    NULL
  },
  error = identity
)
if (is.null(uncertain_error) ||
    !grepl("could not be verified", conditionMessage(uncertain_error), fixed = TRUE)) {
  stop("An unverified final BigQuery state must fail closed.")
}

cat("FPD BigQuery publication safety tests passed\n")
