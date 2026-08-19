################################################################################
#### PUBLISH CREATIVE ASSETS TO GITHUB WITHOUT A LOCAL CHECKOUT
################################################################################
# Purpose:
#   Publish a set of local files to one GitHub branch in a single atomic commit.
#   Existing files with identical content are left unchanged.
#
# Safety:
#   The branch is updated only after every blob and tree is created. A concurrent
#   branch update makes the final non-forced ref update fail instead of merging
#   or overwriting another publisher's work.
################################################################################

suppressPackageStartupMessages({
  library(httr2)
  library(jsonlite)
})

github_token <- function() {
  token <- Sys.getenv("GITHUB_TOKEN", Sys.getenv("GH_TOKEN", ""))
  if (nzchar(token)) return(token)

  result <- system2("gh", c("auth", "token"), stdout = TRUE, stderr = TRUE)
  status <- attr(result, "status")
  if (!is.null(status) && status != 0) {
    stop("GitHub authentication is unavailable. Run `gh auth login` for the scheduled user or set GITHUB_TOKEN.")
  }
  token <- paste(result, collapse = "")
  if (!nzchar(token)) stop("GitHub authentication returned no usable token.")
  token
}

github_request <- function(method, url, token, body = NULL) {
  request <- request(url) %>%
    req_method(method) %>%
    req_headers(
      Authorization = paste("Bearer", token),
      Accept = "application/vnd.github+json",
      `X-GitHub-Api-Version` = "2022-11-28",
      `User-Agent` = "looker-personal-creative-asset-publisher"
    )
  if (!is.null(body)) request <- request %>% req_body_json(body, auto_unbox = TRUE)

  response <- tryCatch(
    req_perform(request),
    error = function(error) {
      stop("GitHub publication request failed: ", conditionMessage(error), call. = FALSE)
    }
  )
  resp_body_json(response, simplifyVector = FALSE)
}

publish_github_assets <- function(
  assets,
  repository = "genetsen/apo-db-creat",
  branch = "main",
  commit_message = "Publish Apollo DCM creative media"
) {
  required_columns <- c("source_file_path", "relative_media_path")
  if (!all(required_columns %in% names(assets))) {
    stop("Asset publication requires source_file_path and relative_media_path columns.")
  }
  if (nrow(assets) == 0) {
    return(data.frame(relative_media_path = character(), blob_sha = character(), action = character()))
  }

  assets <- unique(assets[required_columns])
  invalid_paths <- grepl("^/|(^|/)\\.\\.(/|$)", assets$relative_media_path) |
    !startsWith(assets$relative_media_path, "assets/")
  if (any(invalid_paths)) stop("Every GitHub asset path must be a safe relative path under assets/.")
  if (any(!file.exists(assets$source_file_path))) stop("At least one source image disappeared before publication.")
  if (anyDuplicated(assets$relative_media_path)) stop("More than one source file targets the same GitHub path.")

  token <- github_token()
  api <- paste0("https://api.github.com/repos/", repository)
  ref <- github_request("GET", paste0(api, "/git/ref/heads/", branch), token)
  base_commit_sha <- ref$object$sha
  base_commit <- github_request("GET", paste0(api, "/git/commits/", base_commit_sha), token)
  base_tree_sha <- base_commit$tree$sha
  base_tree <- github_request("GET", paste0(api, "/git/trees/", base_tree_sha, "?recursive=1"), token)
  if (isTRUE(base_tree$truncated)) stop("GitHub returned a truncated repository tree; publication stopped safely.")

  existing_sha <- setNames(
    vapply(base_tree$tree, function(item) item$sha, character(1)),
    vapply(base_tree$tree, function(item) item$path, character(1))
  )
  results <- vector("list", nrow(assets))
  changed_entries <- list()

  for (index in seq_len(nrow(assets))) {
    file_size <- file.info(assets$source_file_path[index])$size
    connection <- file(assets$source_file_path[index], "rb")
    content <- readBin(connection, what = "raw", n = file_size)
    close(connection)
    encoded <- jsonlite::base64_enc(content)
    blob <- github_request(
      "POST",
      paste0(api, "/git/blobs"),
      token,
      list(content = encoded, encoding = "base64")
    )
    path <- assets$relative_media_path[index]
    unchanged <- !is.na(existing_sha[path]) && identical(unname(existing_sha[path]), blob$sha)
    if (!unchanged) {
      changed_entries[[length(changed_entries) + 1]] <- list(
        path = path,
        mode = "100644",
        type = "blob",
        sha = blob$sha
      )
    }
    results[[index]] <- data.frame(
      relative_media_path = path,
      blob_sha = blob$sha,
      action = if (unchanged) "unchanged" else "published",
      stringsAsFactors = FALSE
    )
  }

  if (length(changed_entries) > 0) {
    new_tree <- github_request(
      "POST",
      paste0(api, "/git/trees"),
      token,
      list(base_tree = base_tree_sha, tree = changed_entries)
    )
    new_commit <- github_request(
      "POST",
      paste0(api, "/git/commits"),
      token,
      list(message = commit_message, tree = new_tree$sha, parents = list(base_commit_sha))
    )
    github_request(
      "PATCH",
      paste0(api, "/git/refs/heads/", branch),
      token,
      list(sha = new_commit$sha, force = FALSE)
    )
    published_commit_sha <- new_commit$sha
  } else {
    published_commit_sha <- base_commit_sha
  }

  published_commit <- github_request("GET", paste0(api, "/git/commits/", published_commit_sha), token)
  published_tree <- github_request(
    "GET",
    paste0(api, "/git/trees/", published_commit$tree$sha, "?recursive=1"),
    token
  )
  published_sha <- setNames(
    vapply(published_tree$tree, function(item) item$sha, character(1)),
    vapply(published_tree$tree, function(item) item$path, character(1))
  )
  result <- do.call(rbind, results)
  verified <- unname(published_sha[result$relative_media_path]) == result$blob_sha
  if (any(is.na(verified)) || !all(verified)) {
    stop("GitHub did not confirm every intended asset on the published branch; BigQuery was not changed.")
  }
  attr(result, "commit_sha") <- published_commit_sha
  result
}
