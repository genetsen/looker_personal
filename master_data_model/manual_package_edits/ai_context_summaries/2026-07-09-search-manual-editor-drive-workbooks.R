# Searches Google Drive for older Manual Data Editor workbook candidates.
# Reads Drive file metadata and revision metadata only, then writes local CSV/JSON
# evidence files under ai_context_summaries/. It does not edit Drive, Sheets, or BigQuery.

suppressPackageStartupMessages({
  library(httr)
  library(jsonlite)
})

out_dir <- "ai_context_summaries"
dir.create(out_dir, showWarnings = FALSE, recursive = TRUE)

token_value <- paste(system2(
  "gcloud",
  c("auth", "application-default", "print-access-token"),
  stdout = TRUE
), collapse = "")

if (!nzchar(token_value)) {
  stop("No application-default OAuth token was available.")
}

auth_header <- add_headers(
  Authorization = paste("Bearer", token_value),
  `X-Goog-User-Project` = "looker-studio-pro-452620"
)

token_info <- GET(paste0("https://oauth2.googleapis.com/tokeninfo?access_token=", token_value))
writeLines(
  content(token_info, as = "text", encoding = "UTF-8"),
  file.path(out_dir, "2026-07-09-drive-workbook-search-tokeninfo.json")
)

queries <- data.frame(
  query_label = c(
    "name-manual-data-editor",
    "name-manual-package-editor",
    "name-package-editor",
    "name-manual-editor",
    "name-archived-manual",
    "fulltext-ccdooh",
    "fulltext-columbus-circle-dooh",
    "fulltext-purely-elizabeth-campaign"
  ),
  q = c(
    "mimeType = 'application/vnd.google-apps.spreadsheet' and trashed = false and name contains 'Manual Data Editor'",
    "mimeType = 'application/vnd.google-apps.spreadsheet' and trashed = false and name contains 'Manual Package Editor'",
    "mimeType = 'application/vnd.google-apps.spreadsheet' and trashed = false and name contains 'Package Editor'",
    "mimeType = 'application/vnd.google-apps.spreadsheet' and trashed = false and name contains 'Manual Editor'",
    "mimeType = 'application/vnd.google-apps.spreadsheet' and trashed = false and name contains 'ARCHIVED' and name contains 'Manual'",
    "mimeType = 'application/vnd.google-apps.spreadsheet' and trashed = false and fullText contains 'ccdooh'",
    "mimeType = 'application/vnd.google-apps.spreadsheet' and trashed = false and fullText contains 'Columbus Circle DOOH'",
    "mimeType = 'application/vnd.google-apps.spreadsheet' and trashed = false and fullText contains 'PurelyElizabethProteinGranola2025'"
  ),
  stringsAsFactors = FALSE
)

drive_file_fields <- paste0(
  "nextPageToken,files(",
  "id,name,mimeType,createdTime,modifiedTime,viewedByMeTime,webViewLink,",
  "owners(displayName,emailAddress),lastModifyingUser(displayName,emailAddress),",
  "parents,driveId,shared,trashed",
  ")"
)

flatten_user <- function(user) {
  if (is.null(user)) {
    return(NA_character_)
  }
  pieces <- c(user$emailAddress, user$displayName)
  pieces <- pieces[!is.na(pieces) & nzchar(pieces)]
  paste(unique(pieces), collapse = " / ")
}

flatten_file <- function(file, query_label) {
  owners <- NA_character_
  if (!is.null(file$owners)) {
    owners <- paste(unique(vapply(file$owners, flatten_user, character(1))), collapse = "; ")
  }

  data.frame(
    query_label = query_label,
    file_id = ifelse(is.null(file$id), NA_character_, file$id),
    name = ifelse(is.null(file$name), NA_character_, file$name),
    mime_type = ifelse(is.null(file$mimeType), NA_character_, file$mimeType),
    created_time = ifelse(is.null(file$createdTime), NA_character_, file$createdTime),
    modified_time = ifelse(is.null(file$modifiedTime), NA_character_, file$modifiedTime),
    viewed_by_me_time = ifelse(is.null(file$viewedByMeTime), NA_character_, file$viewedByMeTime),
    owners = owners,
    last_modifying_user = flatten_user(file$lastModifyingUser),
    web_view_link = ifelse(is.null(file$webViewLink), NA_character_, file$webViewLink),
    drive_id = ifelse(is.null(file$driveId), NA_character_, file$driveId),
    parents = ifelse(is.null(file$parents), NA_character_, paste(file$parents, collapse = ";")),
    shared = ifelse(is.null(file$shared), NA, file$shared),
    trashed = ifelse(is.null(file$trashed), NA, file$trashed),
    stringsAsFactors = FALSE
  )
}

drive_list <- function(query_label, query_text) {
  rows <- list()
  raw_pages <- list()
  page_token <- NULL
  page_index <- 1

  repeat {
    query_args <- list(
      q = query_text,
      fields = drive_file_fields,
      pageSize = 1000,
      supportsAllDrives = "true",
      includeItemsFromAllDrives = "true",
      corpora = "allDrives"
    )
    if (!is.null(page_token)) {
      query_args$pageToken <- page_token
    }

    response <- GET(
      "https://www.googleapis.com/drive/v3/files",
      auth_header,
      query = query_args
    )

    response_text <- content(response, as = "text", encoding = "UTF-8")
    raw_pages[[page_index]] <- fromJSON(response_text, simplifyVector = FALSE)
    if (status_code(response) >= 300) {
      warning(sprintf("Drive files.list failed for %s with HTTP %s: %s", query_label, status_code(response), response_text))
      break
    }

    parsed <- raw_pages[[page_index]]
    if (!is.null(parsed$files)) {
      rows <- c(rows, lapply(parsed$files, flatten_file, query_label))
    }

    if (is.null(parsed$nextPageToken) || !nzchar(parsed$nextPageToken)) {
      break
    }
    page_token <- parsed$nextPageToken
    page_index <- page_index + 1
  }

  raw_path <- file.path(out_dir, sprintf("2026-07-09-drive-search-%s.json", query_label))
  write_json(raw_pages, raw_path, auto_unbox = TRUE, pretty = TRUE)

  if (length(rows) == 0) {
    return(data.frame())
  }
  do.call(rbind, rows)
}

file_rows <- do.call(rbind, Map(drive_list, queries$query_label, queries$q))
if (is.null(file_rows) || nrow(file_rows) == 0) {
  stop("No Drive workbook candidates were found.")
}

deduped_files <- file_rows[!duplicated(file_rows$file_id), ]
file_rows_path <- file.path(out_dir, "2026-07-09-drive-manual-editor-workbook-search-results.csv")
deduped_path <- file.path(out_dir, "2026-07-09-drive-manual-editor-workbook-candidates.csv")
write.csv(file_rows, file_rows_path, row.names = FALSE, na = "")
write.csv(deduped_files, deduped_path, row.names = FALSE, na = "")

revision_fields <- paste0(
  "nextPageToken,revisions(",
  "id,mimeType,modifiedTime,keepForever,published,",
  "lastModifyingUser(displayName,emailAddress)",
  ")"
)

flatten_revision <- function(revision, file_id, file_name) {
  data.frame(
    file_id = file_id,
    name = file_name,
    revision_id = ifelse(is.null(revision$id), NA_character_, revision$id),
    modified_time = ifelse(is.null(revision$modifiedTime), NA_character_, revision$modifiedTime),
    last_modifying_user = flatten_user(revision$lastModifyingUser),
    mime_type = ifelse(is.null(revision$mimeType), NA_character_, revision$mimeType),
    keep_forever = ifelse(is.null(revision$keepForever), NA, revision$keepForever),
    published = ifelse(is.null(revision$published), NA, revision$published),
    stringsAsFactors = FALSE
  )
}

revision_list <- function(file_id, file_name) {
  rows <- list()
  page_token <- NULL

  repeat {
    query_args <- list(
      fields = revision_fields,
      pageSize = 1000,
      supportsAllDrives = "true"
    )
    if (!is.null(page_token)) {
      query_args$pageToken <- page_token
    }

    response <- GET(
      sprintf("https://www.googleapis.com/drive/v3/files/%s/revisions", file_id),
      auth_header,
      query = query_args
    )
    response_text <- content(response, as = "text", encoding = "UTF-8")
    raw_path <- file.path(out_dir, sprintf("2026-07-09-drive-revisions-%s.json", file_id))
    writeLines(response_text, raw_path)

    if (status_code(response) >= 300) {
      warning(sprintf("Drive revisions.list failed for %s with HTTP %s: %s", file_name, status_code(response), response_text))
      break
    }

    parsed <- fromJSON(response_text, simplifyVector = FALSE)
    if (!is.null(parsed$revisions)) {
      rows <- c(rows, lapply(parsed$revisions, flatten_revision, file_id, file_name))
    }

    if (is.null(parsed$nextPageToken) || !nzchar(parsed$nextPageToken)) {
      break
    }
    page_token <- parsed$nextPageToken
  }

  if (length(rows) == 0) {
    return(data.frame(
      file_id = file_id,
      name = file_name,
      revision_id = NA_character_,
      modified_time = NA_character_,
      last_modifying_user = NA_character_,
      mime_type = NA_character_,
      keep_forever = NA,
      published = NA,
      stringsAsFactors = FALSE
    ))
  }
  do.call(rbind, rows)
}

revision_rows <- do.call(rbind, Map(revision_list, deduped_files$file_id, deduped_files$name))
revisions_path <- file.path(out_dir, "2026-07-09-drive-manual-editor-workbook-revisions.csv")
write.csv(revision_rows, revisions_path, row.names = FALSE, na = "")

revision_summary <- aggregate(
  modified_time ~ file_id + name,
  data = revision_rows[!is.na(revision_rows$modified_time) & nzchar(revision_rows$modified_time), ],
  FUN = function(x) paste(min(x), max(x), length(x), sep = " | ")
)
names(revision_summary)[names(revision_summary) == "modified_time"] <- "revision_min_max_count"
summary_path <- file.path(out_dir, "2026-07-09-drive-manual-editor-workbook-revision-summary.csv")
write.csv(revision_summary, summary_path, row.names = FALSE, na = "")

cat("Candidates:", nrow(deduped_files), "\n")
cat("Candidate file CSV:", deduped_path, "\n")
cat("Revision CSV:", revisions_path, "\n")
cat("Revision summary CSV:", summary_path, "\n")
