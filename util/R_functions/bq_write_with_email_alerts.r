# Shared BigQuery write helper for R scripts that want one retry path,
# one failure email format, and one verification query.

ALERT_HELPER_SCRIPT_PATH <- normalizePath(
  "/Users/eugenetsenter/Looker_clonedRepo/looker_personal/util/R_functions/bq_write_with_email_alerts.r",
  winslash = "/",
  mustWork = FALSE
)
ALERT_HELPER_FOLDER_PATH <- dirname(ALERT_HELPER_SCRIPT_PATH)
ALERT_HELPER_DETAILS_PATH <- normalizePath(
  "/Users/eugenetsenter/Looker_clonedRepo/looker_personal/util/R_functions/README.md",
  winslash = "/",
  mustWork = FALSE
)

format_alert_timestamp <- function(x = Sys.time()) {
  format(x, "%Y-%m-%d %I:%M %p")
}

encode_mime_subject <- function(subject_line) {
  subject_utf8 <- enc2utf8(subject_line)
  subject_raw <- charToRaw(subject_utf8)

  if (all(as.integer(subject_raw) < 128L)) {
    return(subject_utf8)
  }

  encode_word <- function(text) {
    paste0(
      "=?UTF-8?B?",
      base64enc::base64encode(charToRaw(enc2utf8(text)), linewidth = 0L),
      "?="
    )
  }

  chars <- strsplit(subject_utf8, "", fixed = TRUE)[[1]]
  encoded_words <- character(0)
  current_chunk <- ""

  for (current_char in chars) {
    candidate_chunk <- paste0(current_chunk, current_char)
    if (nchar(encode_word(candidate_chunk), type = "bytes") <= 75L) {
      current_chunk <- candidate_chunk
    } else {
      encoded_words <- c(encoded_words, encode_word(current_chunk))
      current_chunk <- current_char
    }
  }

  if (nzchar(current_chunk)) {
    encoded_words <- c(encoded_words, encode_word(current_chunk))
  }

  paste(encoded_words, collapse = " ")
}

build_bigquery_table_deeplink <- function(project_id, dataset, table) {
  encoded_project <- utils::URLencode(project_id, reserved = TRUE)
  encoded_dataset <- utils::URLencode(dataset, reserved = TRUE)
  encoded_table <- utils::URLencode(table, reserved = TRUE)

  paste0(
    "https://console.cloud.google.com/bigquery?project=", encoded_project,
    "&p=", encoded_project,
    "&d=", encoded_dataset,
    "&t=", encoded_table,
    "&page=table"
  )
}

send_bigquery_failure_email <- function(project_id,
                                        dataset,
                                        table,
                                        loader_name,
                                        error_type,
                                        error_details,
                                        script_path = "unknown_script") {
  tryCatch({
    user_email <- gmailr::gm_profile()$emailAddress
    script_name <- if (nzchar(script_path) && script_path != "unknown_script") {
      basename(script_path)
    } else {
      loader_name
    }
    script_folder <- if (nzchar(script_path) && script_path != "unknown_script") {
      normalizePath(dirname(script_path), winslash = "/", mustWork = FALSE)
    } else {
      "unknown_script_folder"
    }
    short_table_name <- paste(dataset, table, sep = ".")
    table_identifier <- paste(project_id, dataset, table, sep = ".")
    table_deeplink <- build_bigquery_table_deeplink(project_id, dataset, table)
    subject_line <- sprintf(
      "\U0001F6A8 Error in (R) Script | %s failed to update %s",
      script_name,
      short_table_name
    )

    email_body <- paste0(
      "BigQuery Write Failure Alert\n\n",
      "Timestamp: ", format_alert_timestamp(), "\n",
      "Project: ", project_id, "\n",
      "Dataset: ", dataset, "\n",
      "Table: ", table, "\n",
      "BigQuery Table: ", table_identifier, "\n",
      "BigQuery Link: ", table_deeplink, "\n",
      "Script Path: ", script_path, "\n",
      "Script Folder: ", script_folder, "\n",
      "Alert Helper Script: ", ALERT_HELPER_SCRIPT_PATH, "\n",
      "Alert Helper Folder: ", ALERT_HELPER_FOLDER_PATH, "\n",
      "Alert Details / Update Instructions: ", ALERT_HELPER_DETAILS_PATH, "\n",
      "Error Type: ", error_type, "\n\n",
      "Error Details:\n", error_details, "\n\n",
      "---\n",
      "This is an automated alert from the ", loader_name, "."
    )

    email <- gmailr::gm_mime() %>%
      gmailr::gm_to(user_email) %>%
      gmailr::gm_from(user_email) %>%
      gmailr::gm_text_body(email_body)
    email$header$Subject <- encode_mime_subject(subject_line)

    gmailr::gm_send_message(email)
    cat("Failure notification email sent to:", user_email, "\n")
    cat("BigQuery table link included in alert:", table_deeplink, "\n")
  }, error = function(email_err) {
    cat("Failed to send email notification:", conditionMessage(email_err), "\n")
  })
}

write_to_bq_with_email_alerts <- function(data,
                                          project_id,
                                          dataset,
                                          table,
                                          loader_name,
                                          script_path = "unknown_script") {
  bq_tbl <- bigrquery::bq_table(
    project = project_id,
    dataset = dataset,
    table = table
  )

  data$data_refresh_date <- lubridate::today()

  tryCatch(
    {
      bigrquery::bq_table_upload(
        bq_tbl,
        data,
        write_disposition = "WRITE_TRUNCATE"
      )
      cat("Data written successfully to BigQuery table:", table, "\n")
    },
    error = function(e) {
      error_msg <- conditionMessage(e)
      is_schema_error <- grepl(
        "schema|column|field|type mismatch|incompatible",
        error_msg,
        ignore.case = TRUE
      )

      if (is_schema_error) {
        cat("Schema incompatibility detected. Deleting existing table and retrying...\n")
        cat("Error details:", error_msg, "\n")

        tryCatch(
          {
            bigrquery::bq_table_delete(bq_tbl)
            cat("Table deleted successfully\n")
          },
          error = function(del_err) {
            cat(
              "Note: Table may not exist or couldn't be deleted:",
              conditionMessage(del_err),
              "\n"
            )
          }
        )

        tryCatch(
          {
            bigrquery::bq_table_upload(
              bq_tbl,
              data,
              write_disposition = "WRITE_TRUNCATE"
            )
            cat("Data written successfully after table recreation\n")
          },
          error = function(retry_err) {
            retry_msg <- conditionMessage(retry_err)
            cat("Failed to write data after table deletion:", retry_msg, "\n")

            send_bigquery_failure_email(
              project_id = project_id,
              dataset = dataset,
              table = table,
              loader_name = loader_name,
              error_type = "Schema Error - Retry Failed",
              error_details = paste0(
                "Initial Error:\n", error_msg, "\n\n",
                "Retry Error:\n", retry_msg
              ),
              script_path = script_path
            )

            stop(retry_err)
          }
        )
      } else {
        cat("Write failed with non-schema error:", error_msg, "\n")

        send_bigquery_failure_email(
          project_id = project_id,
          dataset = dataset,
          table = table,
          loader_name = loader_name,
          error_type = "Non-Schema Error",
          error_details = error_msg,
          script_path = script_path
        )

        stop(e)
      }
    }
  )

  query <- sprintf(
    "SELECT data_refresh_date, COUNT(*) as row_count FROM `%s.%s.%s` GROUP BY data_refresh_date ORDER BY data_refresh_date DESC LIMIT 5",
    project_id,
    dataset,
    table
  )
  result <- bigrquery::bq_table_download(
    bigrquery::bq_project_query(project_id, query)
  )

  cat(
    "Latest data refresh date:", as.character(max(result$data_refresh_date)), "\n",
    "Total rows:", sum(result$row_count), "\n"
  )

  invisible(list(
    script_path = script_path,
    bigquery_table_link = build_bigquery_table_deeplink(project_id, dataset, table)
  ))
}
