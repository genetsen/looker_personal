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

errors_only_failure <- tryCatch(
  {
    wait_for_fpd_bq_job_success(
      job = list(job = "job_errors_only"),
      wait_fn = function(job) invisible(job),
      status_fn = function(job) list(
        state = "DONE",
        errors = list(list(
          reason = "backendError",
          message = "BigQuery could not finish the write."
        ))
      )
    )
    NULL
  },
  error = identity
)
if (is.null(errors_only_failure) ||
    !grepl("backendError", conditionMessage(errors_only_failure), fixed = TRUE) ||
    !grepl("job_errors_only", conditionMessage(errors_only_failure), fixed = TRUE)) {
  stop("A completed status containing only status$errors must fail visibly.")
}

status_called_after_wait_failure <- FALSE
wait_failure <- tryCatch(
  {
    wait_for_fpd_bq_job_success(
      job = list(job = "job_wait_reported_error"),
      wait_fn = function(job) {
        stop("Job failed: BigQuery could not finish the write. [backendError]")
      },
      status_fn = function(job) {
        status_called_after_wait_failure <<- TRUE
        list(state = "DONE")
      }
    )
    NULL
  },
  error = identity
)
if (is.null(wait_failure) ||
    status_called_after_wait_failure ||
    !grepl("DATA LOAD FAILURE", conditionMessage(wait_failure), fixed = TRUE) ||
    !grepl("job_wait_reported_error", conditionMessage(wait_failure), fixed = TRUE) ||
    !grepl("backendError", conditionMessage(wait_failure), fixed = TRUE)) {
  stop("A wait failure must stop before status lookup and preserve its job details.")
}

wait_calls <- 0L
status_calls <- 0L
success_job <- wait_for_fpd_bq_job_success(
  job = list(job = "job_success"),
  wait_fn = function(job) {
    wait_calls <<- wait_calls + 1L
    invisible(job)
  },
  status_fn = function(job) {
    status_calls <<- status_calls + 1L
    list(state = "DONE")
  }
)
if (wait_calls != 1L || status_calls != 1L ||
    !identical(success_job$job, "job_success")) {
  stop("A confirmed successful job must wait and verify exactly once.")
}

status_lookup_failure <- tryCatch(
  {
    wait_for_fpd_bq_job_success(
      job = list(job = "job_unknown_outcome"),
      wait_fn = function(job) invisible(job),
      status_fn = function(job) stop("status service unavailable")
    )
    NULL
  },
  error = identity
)
if (is.null(status_lookup_failure) ||
    !grepl("job_unknown_outcome", conditionMessage(status_lookup_failure), fixed = TRUE) ||
    !grepl("could not be verified", conditionMessage(status_lookup_failure), fixed = TRUE) ||
    !grepl("status service unavailable", conditionMessage(status_lookup_failure), fixed = TRUE)) {
  stop("An unavailable final status must fail closed with its job and lookup error.")
}

for (uncertain_status in list(
  list(),
  list(state = "RUNNING")
)) {
  uncertain_error <- tryCatch(
    {
      wait_for_fpd_bq_job_success(
        job = list(job = "job_not_final"),
        wait_fn = function(job) invisible(job),
        status_fn = function(job) uncertain_status
      )
      NULL
    },
    error = identity
  )
  if (is.null(uncertain_error) ||
      !grepl("job_not_final", conditionMessage(uncertain_error), fixed = TRUE) ||
      !grepl("could not be verified", conditionMessage(uncertain_error), fixed = TRUE)) {
    stop("An empty or non-final BigQuery status must fail closed.")
  }
}

loader_text <- readLines(
  file.path(loader_dir, "util_collect_fpd_shortcutsFolder.r"),
  warn = FALSE
)
if (sum(grepl("wait_for_fpd_bq_job_success(", loader_text, fixed = TRUE)) != 2L ||
    sum(grepl("rethrow_fpd_bq_error(e,", loader_text, fixed = TRUE)) != 2L) {
  stop("Both production query sites must use the checked wait and error-preserving rethrow.")
}

cat("FPD BigQuery publication safety tests passed\n")
