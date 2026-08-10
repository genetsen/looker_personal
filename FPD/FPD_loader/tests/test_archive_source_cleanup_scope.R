# Offline regression tests for archive-driven FPD source cleanup.
#
# The test extracts only pure helper functions from the production loader. It
# does not authenticate, read Drive or Sheets, or submit BigQuery work. It proves
# that archive intent must be explicit and that archived identities can enter a
# cleanup transaction only as exact source_url values.

file_arg <- sub("^--file=", "", commandArgs(FALSE)[grepl("^--file=", commandArgs(FALSE))][1])
test_path <- normalizePath(file_arg)
script_path <- file.path(dirname(dirname(test_path)), "util_collect_fpd_shortcutsFolder.r")

exprs <- parse(script_path)
helper_names <- c(
  "sql_quote_string",
  "is_archive_named",
  "mark_archived_discovered_sources",
  "normalize_source_identities",
  "build_fpd_sync_scope",
  "build_fpd_delete_clauses"
)

for (helper_name in helper_names) {
  helper_idx <- which(vapply(
    exprs,
    function(expr) {
      is.call(expr) &&
        identical(expr[[1]], as.name("<-")) &&
        identical(expr[[2]], as.name(helper_name))
    },
    logical(1)
  ))

  if (length(helper_idx) != 1) {
    stop(paste0("Expected exactly one ", helper_name, "() definition."))
  }

  eval(exprs[[helper_idx]], envir = globalenv())
}

archived_url <- "https://docs.google.com/spreadsheets/d/archived-id"
active_url <- "https://docs.google.com/spreadsheets/d/current-id"
missing_url <- "https://docs.google.com/spreadsheets/d/missing-id"

discovered_sources <- data.frame(
  sheet_name = c(
    "APO | Partner Data Collection | NYTIME",
    "ARCHIVE | APO | Partner Data Collection | DIRECT",
    "APO | Partner Data Collection | NYTIME || v2 USE THIS"
  ),
  resolved_sheet_name = c(
    "ARCHIVE | APO | Partner Data Collection | NYTIME",
    "APO | Partner Data Collection | DIRECT",
    "APO | Partner Data Collection | NYTIME || v2 USE THIS"
  ),
  sheet_url = c(
    archived_url,
    "https://docs.google.com/spreadsheets/d/direct-archived-id",
    active_url
  ),
  stringsAsFactors = FALSE
)

classified <- mark_archived_discovered_sources(discovered_sources)
archived_urls <- classified$sheet_url[classified$is_archived_source]
active_urls <- classified$sheet_url[!classified$is_archived_source]

expected_archived_urls <- c(
  archived_url,
  "https://docs.google.com/spreadsheets/d/direct-archived-id"
)
if (!identical(archived_urls, expected_archived_urls)) {
  stop("Both archived shortcut targets and directly named archived Sheets must be excluded.")
}

if (!identical(active_urls, active_url)) {
  stop("The current v2 source must remain active.")
}

if (missing_url %in% archived_urls) {
  stop("A source absent from fresh Drive discovery must never be queued for cleanup.")
}

sync_scope <- build_fpd_sync_scope(
  active_source_urls = c(active_url, active_url, NA_character_, ""),
  active_source_files = c("APO | Partner Data Collection | NYTIME || v2 USE THIS", ""),
  archived_source_urls = c(archived_url, archived_url, NA_character_)
)

if (!identical(sync_scope$source_urls, c(active_url, archived_url))) {
  stop("The sync scope must preserve only unique exact active and archived source URLs.")
}

if (!identical(
  sync_scope$source_files,
  "APO | Partner Data Collection | NYTIME || v2 USE THIS"
)) {
  stop("Archive discovery must not add a display-name cleanup scope.")
}

delete_clauses <- build_fpd_delete_clauses(sync_scope)
expected_url_clause <- paste0(
  "target.source_url IN ('", active_url, "', '", archived_url, "')"
)
expected_file_clause <- paste0(
  "target.source_file IN ('APO | Partner Data Collection | NYTIME || v2 USE THIS')"
)

if (!identical(delete_clauses, c(expected_url_clause, expected_file_clause))) {
  stop("The transaction must delete archived warehouse rows through exact source_url matching only.")
}

if (any(grepl(missing_url, delete_clauses, fixed = TRUE))) {
  stop("A merely missing source URL must not appear in the generated delete scope.")
}

cat("archive source cleanup scope regression tests passed\n")
