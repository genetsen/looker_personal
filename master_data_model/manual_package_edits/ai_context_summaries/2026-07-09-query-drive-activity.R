# Queries Google Drive Activity for the Manual Data Editor workbooks.
# Reads only Google Drive Activity metadata and writes local JSON/CSV evidence files
# under ai_context_summaries/. It does not edit Google Sheets, Drive files, or BigQuery.

suppressPackageStartupMessages({
  library(gargle)
  library(httr)
  library(jsonlite)
})

out_dir <- "ai_context_summaries"
dir.create(out_dir, showWarnings = FALSE, recursive = TRUE)

workbooks <- data.frame(
  label = c("current-production", "archived-2026-07-26"),
  file_id = c(
    "1p1aGAg8lMk7JvUKCJBKRj5rKQNNYL3iEKnl0kPHvZ7E",
    "1WerhrbBMggzCwIUCOsOCV33aHygV96jt1HgqiYcUHZo"
  ),
  stringsAsFactors = FALSE
)

scopes <- c(
  "https://www.googleapis.com/auth/drive.activity.readonly",
  "https://www.googleapis.com/auth/drive.metadata.readonly"
)

activity_filter <- 'time >= "2026-06-01T00:00:00Z" AND time <= "2026-07-10T04:00:00Z"'
token_value <- Sys.getenv("DRIVE_ACTIVITY_ACCESS_TOKEN")

if (!nzchar(token_value)) {
  token <- token_fetch(scopes = scopes)
  if (!is.null(token) && !is.null(token$credentials$access_token)) {
    token_value <- token$credentials$access_token
  }
}

if (!nzchar(token_value)) {
  token_value <- paste(system2(
    "gcloud",
    c("auth", "application-default", "print-access-token"),
    stdout = TRUE
  ), collapse = "")
}

if (!nzchar(token_value)) {
  stop("No OAuth access token was available for Drive Activity.")
}

token_info_response <- GET(paste0("https://oauth2.googleapis.com/tokeninfo?access_token=", token_value))
writeLines(
  content(token_info_response, as = "text", encoding = "UTF-8"),
  file.path(out_dir, "2026-07-09-drive-activity-tokeninfo.json")
)

user_project <- Sys.getenv("DRIVE_ACTIVITY_USER_PROJECT", unset = "")
if (nzchar(user_project)) {
  auth_header <- add_headers(
    Authorization = paste("Bearer", token_value),
    `X-Goog-User-Project` = user_project
  )
} else {
  auth_header <- add_headers(
    Authorization = paste("Bearer", token_value)
  )
}

extract_names <- function(x) {
  if (is.null(x) || length(x) == 0) {
    return(character())
  }
  names(x)
}

actor_label <- function(actor) {
  if (is.null(actor$user$knownUser$personName)) {
    if (!is.null(actor$user$deletedUser)) {
      return("deleted-user")
    }
    if (!is.null(actor$administrator)) {
      return("administrator")
    }
    if (!is.null(actor$anonymous)) {
      return("anonymous")
    }
    return(NA_character_)
  }
  actor$user$knownUser$personName
}

flatten_activity <- function(activity, workbook_label, workbook_file_id) {
  action_names <- extract_names(activity$primaryActionDetail)
  if (length(action_names) == 0 && !is.null(activity$actions)) {
    action_names <- unique(unlist(lapply(activity$actions, function(action) {
      extract_names(action$detail)
    })))
  }

  actor_names <- character()
  if (!is.null(activity$actors)) {
    actor_names <- unique(vapply(activity$actors, actor_label, character(1)))
  }
  actor_names <- actor_names[!is.na(actor_names) & nzchar(actor_names)]

  target_titles <- character()
  target_names <- character()
  if (!is.null(activity$targets)) {
    target_titles <- unique(unlist(lapply(activity$targets, function(target) {
      target$driveItem$title
    })))
    target_names <- unique(unlist(lapply(activity$targets, function(target) {
      target$driveItem$name
    })))
  }
  target_titles <- target_titles[!is.na(target_titles) & nzchar(target_titles)]
  target_names <- target_names[!is.na(target_names) & nzchar(target_names)]

  data.frame(
    workbook_label = workbook_label,
    workbook_file_id = workbook_file_id,
    timestamp = ifelse(is.null(activity$timestamp), NA_character_, activity$timestamp),
    time_range_start = ifelse(is.null(activity$timeRange$startTime), NA_character_, activity$timeRange$startTime),
    time_range_end = ifelse(is.null(activity$timeRange$endTime), NA_character_, activity$timeRange$endTime),
    actions = paste(action_names, collapse = ";"),
    actors = paste(actor_names, collapse = ";"),
    target_titles = paste(target_titles, collapse = ";"),
    target_names = paste(target_names, collapse = ";"),
    stringsAsFactors = FALSE
  )
}

query_workbook <- function(label, file_id) {
  all_activities <- list()
  rows <- list()
  page_token <- NULL
  page_index <- 1

  repeat {
    body <- list(
      itemName = paste0("items/", file_id),
      pageSize = 100,
      filter = activity_filter
    )
    if (!is.null(page_token)) {
      body$pageToken <- page_token
    }

    response <- POST(
      "https://driveactivity.googleapis.com/v2/activity:query",
      auth_header,
      body = body,
      encode = "json"
    )

    response_text <- content(response, as = "text", encoding = "UTF-8")
    response_path <- file.path(
      out_dir,
      sprintf("2026-07-09-%s-drive-activity-page-%02d.json", label, page_index)
    )
    writeLines(response_text, response_path)

    cat(label, "page", page_index, "status", status_code(response), "\n")
    if (status_code(response) >= 300) {
      stop(sprintf("Drive Activity query failed for %s with HTTP %s", label, status_code(response)))
    }

    parsed <- fromJSON(response_text, simplifyVector = FALSE)
    if (!is.null(parsed$activities)) {
      all_activities <- c(all_activities, parsed$activities)
      rows <- c(rows, lapply(parsed$activities, flatten_activity, label, file_id))
    }

    if (is.null(parsed$nextPageToken) || !nzchar(parsed$nextPageToken)) {
      break
    }
    page_token <- parsed$nextPageToken
    page_index <- page_index + 1
  }

  combined_path <- file.path(out_dir, sprintf("2026-07-09-%s-drive-activity-combined.json", label))
  write_json(list(activities = all_activities), combined_path, auto_unbox = TRUE, pretty = TRUE)

  if (length(rows) == 0) {
    return(data.frame(
      workbook_label = label,
      workbook_file_id = file_id,
      timestamp = NA_character_,
      time_range_start = NA_character_,
      time_range_end = NA_character_,
      actions = NA_character_,
      actors = NA_character_,
      target_titles = NA_character_,
      target_names = NA_character_,
      stringsAsFactors = FALSE
    ))
  }

  do.call(rbind, rows)
}

all_rows <- do.call(rbind, Map(query_workbook, workbooks$label, workbooks$file_id))
summary_path <- file.path(out_dir, "2026-07-09-drive-activity-summary.csv")
write.csv(all_rows, summary_path, row.names = FALSE, na = "")
cat("Wrote", summary_path, "with", nrow(all_rows), "rows\n")
