# FPD BigQuery publication safeguards.
#
# This file preserves established production column types for blank incoming
# metrics and requires a final, structured BigQuery success result after every
# production query. It does not submit or retry jobs by itself.

coerce_blank_logicals_to_existing_numeric_types <- function(data_upload, prod_field_types) {
  if (is.null(prod_field_types) || length(prod_field_types) == 0) {
    return(data_upload)
  }

  numeric_bq_types <- c(
    "FLOAT", "FLOAT64", "NUMERIC", "BIGNUMERIC",
    "INTEGER", "INT64", "DECIMAL", "BIGDECIMAL"
  )
  candidate_cols <- intersect(names(data_upload), names(prod_field_types))
  coerced_cols <- candidate_cols[vapply(
    candidate_cols,
    function(column_name) {
      is.logical(data_upload[[column_name]]) &&
        all(is.na(data_upload[[column_name]])) &&
        toupper(prod_field_types[[column_name]]) %in% numeric_bq_types
    },
    logical(1)
  )]

  for (column_name in coerced_cols) {
    data_upload[[column_name]] <- rep(NA_real_, nrow(data_upload))
  }

  if (length(coerced_cols) > 0) {
    cat(
      "  Preserved production numeric type for blank column(s):",
      paste(coerced_cols, collapse = ", "),
      "\n"
    )
  }

  data_upload
}

read_fpd_bq_job_id <- function(job) {
  candidate <- tryCatch(job$job, error = function(error_condition) NULL)
  if (is.null(candidate) || !length(candidate) || !nzchar(as.character(candidate[[1]]))) {
    return("unknown")
  }
  as.character(candidate[[1]])
}

stop_for_fpd_bq_job_error <- function(job_id, error_record) {
  read_error_text <- function(field_name) {
    value <- tryCatch(error_record[[field_name]], error = function(error_condition) NULL)
    if (is.null(value) || !length(value)) {
      return("")
    }
    value <- as.character(value[[1]])
    if (is.na(value) || !nzchar(value)) {
      return("")
    }
    value
  }

  reason <- read_error_text("reason")
  error_message <- read_error_text("message")
  stop(
    "DATA LOAD FAILURE: BigQuery job ",
    job_id,
    " failed",
    if (nzchar(reason)) paste0(" [", reason, "]") else "",
    if (nzchar(error_message)) paste0(": ", error_message) else "",
    call. = FALSE
  )
}

wait_for_fpd_bq_job_success <- function(
  job,
  wait_fn = bigrquery::bq_job_wait,
  status_fn = bigrquery::bq_job_status
) {
  job_id <- read_fpd_bq_job_id(job)
  wait_failure <- tryCatch(
    {
      wait_fn(job)
      NULL
    },
    error = identity
  )
  if (!is.null(wait_failure)) {
    stop(
      "DATA LOAD FAILURE: BigQuery job ",
      job_id,
      " failed while waiting: ",
      conditionMessage(wait_failure),
      call. = FALSE
    )
  }

  final_status <- tryCatch(
    status_fn(job),
    error = function(error_condition) {
      stop(
        "DATA LOAD FAILURE: BigQuery job ",
        job_id,
        " finished waiting, but its final outcome could not be verified. ",
        conditionMessage(error_condition),
        call. = FALSE
      )
    }
  )

  reported_state <- if (is.list(final_status) &&
                        !is.null(final_status$state) &&
                        length(final_status$state)) {
    as.character(final_status$state[[1]])
  } else {
    "missing"
  }
  if (is.na(reported_state) || !identical(reported_state, "DONE")) {
    stop(
      "DATA LOAD FAILURE: BigQuery job ",
      job_id,
      " finished waiting, but its final outcome could not be verified. Reported state: ",
      if (is.na(reported_state)) "missing" else reported_state,
      call. = FALSE
    )
  }

  fatal_error <- final_status$errorResult
  if (!is.null(fatal_error) && length(fatal_error)) {
    stop_for_fpd_bq_job_error(job_id, fatal_error)
  }

  other_errors <- final_status$errors
  if (!is.null(other_errors) && length(other_errors)) {
    first_error <- if (is.data.frame(other_errors)) {
      as.list(other_errors[1, , drop = FALSE])
    } else if (!is.null(other_errors$reason) || !is.null(other_errors$message)) {
      other_errors
    } else {
      other_errors[[1]]
    }
    stop_for_fpd_bq_job_error(job_id, first_error)
  }

  invisible(job)
}

rethrow_fpd_bq_error <- function(error_condition, context) {
  error_message <- conditionMessage(error_condition)
  if (startsWith(error_message, "DATA LOAD FAILURE")) {
    stop(error_message, call. = FALSE)
  }
  stop(context, ": ", error_message, call. = FALSE)
}
