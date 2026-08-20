################################################################################
#### POLARIS FIRST-PARTY DATA PREVIEW LOGIC
################################################################################
# Purpose:
#   Normalize Polaris Meta and TikTok CSV exports into one review-only FPD
#   shape while preserving source-row identity and approved package mappings.
# Inputs and outputs:
#   Functions accept in-memory CSV-shaped data frames and return normalized
#   rows plus reconciliation and mapping summaries.
# Safe usage:
#   This file contains pure functions only. It does not read cloud storage,
#   query BigQuery, or write local or live data.
################################################################################


# * SECTION [1]: SHARED VALUES

  # Description: Define the approved Stage 1 package mappings and value parsers.

  # ? Return the three explicitly approved platform-to-package preview mappings
    polaris_preview_mappings <- function() {
      data.frame(
        platform = c("Facebook", "Instagram", "TikTok"),
        package_friendly_label = c("Facebook Awareness", "Instagram", "TikTok"),
        package_id = c("P3HF7QB", "P3HF7T8", "P3HF88Q"),
        stringsAsFactors = FALSE
      )
    }

  # ? Convert source text blanks and placeholder null strings to missing values
    as_polaris_text <- function(value) {
      output <- trimws(as.character(value))
      output[output == "" | tolower(output) %in% c("na", "nan", "null")] <- NA_character_
      output
    }

  # ? Parse source numeric fields without changing observed zeros
    parse_polaris_numeric <- function(value) {
      cleaned <- gsub("[$,]", "", as.character(value))
      cleaned <- as_polaris_text(cleaned)
      suppressWarnings(as.numeric(cleaned))
    }

  # ? Parse ISO source dates and leave invalid values missing for visible review
    parse_polaris_date <- function(value) {
      if (inherits(value, "Date")) return(value)
      suppressWarnings(as.Date(as_polaris_text(value), format = "%Y-%m-%d"))
    }

  # ? Read one named source column or return an equally sized missing vector
    polaris_column <- function(data, name) {
      if (name %in% names(data)) data[[name]] else rep(NA_character_, nrow(data))
    }

  # ? Remove a UTF-8 byte-order mark that may be attached to the first header
    clean_polaris_headers <- function(data) {
      names(data) <- sub("^\ufeff", "", names(data))
      data
    }


# * SECTION [2]: SOURCE NORMALIZATION

  # Description: Map each supported source schema into the shared review shape.

  # ? Describe the exact fields required for each current Polaris export
    polaris_required_headers <- function(source_feed) {
      switch(
        source_feed,
        meta = c(
          "campaign_name", "adset_name", "Platform", "ad_name", "date",
          "Billable Spend", "impressions", "Link_click", "video_view",
          "Video View to 100%"
        ),
        tiktok = c(
          "Campaign Name", "Ad Group Name", "Ad Name", "Date Start",
          "Billable Spend", "Impressions", "Clicks (Destination)",
          "Video Views", "Video Views at 100%"
        ),
        stop(paste("Unsupported Polaris source feed:", source_feed), call. = FALSE)
      )
    }

  # ? Stop when a source file no longer satisfies its documented schema contract
    validate_polaris_headers <- function(data, source_feed) {
      required <- polaris_required_headers(source_feed)
      missing <- setdiff(required, names(data))
      if (length(missing) > 0) {
        stop(
          paste0(
            "Polaris ", source_feed, " source is missing required columns: ",
            paste(missing, collapse = ", ")
          ),
          call. = FALSE
        )
      }
      invisible(TRUE)
    }

  # ? Normalize a Meta export while retaining its campaign, ad-set, and ad detail
    normalize_polaris_meta <- function(data, source_object_uri) {
      data <- clean_polaris_headers(as.data.frame(data, stringsAsFactors = FALSE))
      validate_polaris_headers(data, "meta")

      data.frame(
        source_object_uri = rep(source_object_uri, nrow(data)),
        source_feed = rep("meta", nrow(data)),
        source_row_number = seq_len(nrow(data)) + 1L,
        platform = as_polaris_text(data[["Platform"]]),
        campaign_name = as_polaris_text(data[["campaign_name"]]),
        ad_group_name = as_polaris_text(data[["adset_name"]]),
        ad_name = as_polaris_text(data[["ad_name"]]),
        date = parse_polaris_date(data[["date"]]),
        spend = parse_polaris_numeric(data[["Billable Spend"]]),
        impressions = parse_polaris_numeric(data[["impressions"]]),
        clicks = parse_polaris_numeric(data[["Link_click"]]),
        video_views = parse_polaris_numeric(data[["video_view"]]),
        video_completions = parse_polaris_numeric(data[["Video View to 100%"]]),
        raw_date = as_polaris_text(data[["date"]]),
        raw_spend = as_polaris_text(data[["Billable Spend"]]),
        raw_impressions = as_polaris_text(data[["impressions"]]),
        raw_clicks = as_polaris_text(data[["Link_click"]]),
        raw_video_views = as_polaris_text(data[["video_view"]]),
        raw_video_completions = as_polaris_text(data[["Video View to 100%"]]),
        stringsAsFactors = FALSE
      )
    }

  # ? Normalize a TikTok export while retaining its campaign, ad-group, and ad detail
    normalize_polaris_tiktok <- function(data, source_object_uri) {
      data <- clean_polaris_headers(as.data.frame(data, stringsAsFactors = FALSE))
      validate_polaris_headers(data, "tiktok")

      data.frame(
        source_object_uri = rep(source_object_uri, nrow(data)),
        source_feed = rep("tiktok", nrow(data)),
        source_row_number = seq_len(nrow(data)) + 1L,
        platform = rep("TikTok", nrow(data)),
        campaign_name = as_polaris_text(data[["Campaign Name"]]),
        ad_group_name = as_polaris_text(data[["Ad Group Name"]]),
        ad_name = as_polaris_text(data[["Ad Name"]]),
        date = parse_polaris_date(data[["Date Start"]]),
        spend = parse_polaris_numeric(data[["Billable Spend"]]),
        impressions = parse_polaris_numeric(data[["Impressions"]]),
        clicks = parse_polaris_numeric(data[["Clicks (Destination)"]]),
        video_views = parse_polaris_numeric(data[["Video Views"]]),
        video_completions = parse_polaris_numeric(data[["Video Views at 100%"]]),
        raw_date = as_polaris_text(data[["Date Start"]]),
        raw_spend = as_polaris_text(data[["Billable Spend"]]),
        raw_impressions = as_polaris_text(data[["Impressions"]]),
        raw_clicks = as_polaris_text(data[["Clicks (Destination)"]]),
        raw_video_views = as_polaris_text(data[["Video Views"]]),
        raw_video_completions = as_polaris_text(data[["Video Views at 100%"]]),
        stringsAsFactors = FALSE
      )
    }


# * SECTION [3]: MAPPING AND REVIEW STATUS

  # Description: Attach approved package candidates and surface unsafe rows.

  # ? Add mappings, validation results, and a visible natural-row duplicate count
    classify_polaris_rows <- function(normalized_rows, mappings = polaris_preview_mappings()) {
      normalized_rows <- as.data.frame(normalized_rows, stringsAsFactors = FALSE)
      mapping_index <- match(tolower(normalized_rows$platform), tolower(mappings$platform))
      normalized_rows$package_friendly_label <- mappings$package_friendly_label[mapping_index]
      normalized_rows$package_id <- mappings$package_id[mapping_index]
      normalized_rows$mapping_status <- ifelse(is.na(mapping_index), "unmapped_platform", "mapped")

      key_parts <- list(
        normalized_rows$source_feed,
        normalized_rows$platform,
        normalized_rows$date,
        normalized_rows$campaign_name,
        normalized_rows$ad_group_name,
        normalized_rows$ad_name
      )
      key_parts <- lapply(key_parts, function(value) ifelse(is.na(value), "<missing>", as.character(value)))
      normalized_rows$natural_row_key <- do.call(paste, c(key_parts, sep = "|"))
      normalized_rows$natural_row_occurrences <- ave(
        rep(1L, nrow(normalized_rows)),
        normalized_rows$natural_row_key,
        FUN = length
      )

      required_missing <- is.na(normalized_rows$platform) |
        is.na(normalized_rows$campaign_name) |
        is.na(normalized_rows$ad_group_name) |
        is.na(normalized_rows$ad_name) |
        is.na(normalized_rows$raw_date)
      invalid_date <- !is.na(normalized_rows$raw_date) & is.na(normalized_rows$date)
      raw_metric_names <- c(
        "raw_spend", "raw_impressions", "raw_clicks", "raw_video_views",
        "raw_video_completions"
      )
      metric_names <- c("spend", "impressions", "clicks", "video_views", "video_completions")
      invalid_metric <- rep(FALSE, nrow(normalized_rows))
      for (index in seq_along(metric_names)) {
        invalid_metric <- invalid_metric |
          (!is.na(normalized_rows[[raw_metric_names[[index]]]]) &
            is.na(normalized_rows[[metric_names[[index]]]]))
      }

      normalized_rows$validation_status <- "valid"
      normalized_rows$validation_status[normalized_rows$natural_row_occurrences > 1] <-
        "duplicate_natural_key"
      normalized_rows$validation_status[invalid_metric] <- "invalid_metric"
      normalized_rows$validation_status[invalid_date] <- "invalid_date"
      normalized_rows$validation_status[required_missing] <- "missing_required_value"
      normalized_rows$review_status <- ifelse(
        normalized_rows$mapping_status == "mapped" & normalized_rows$validation_status == "valid",
        "ready_for_review",
        "needs_review"
      )
      normalized_rows
    }


# * SECTION [4]: REVIEW SUMMARIES

  # Description: Build compact mapping and metric reconciliations without dropping rows.

  # ? Aggregate metrics by platform with stable behavior for missing values
    summarize_polaris_metrics <- function(rows, prefix) {
      platforms <- sort(unique(ifelse(is.na(rows$platform), "<missing>", rows$platform)))
      output <- lapply(platforms, function(platform_name) {
        selected <- rows[ifelse(is.na(rows$platform), "<missing>", rows$platform) == platform_name, ]
        data.frame(
          platform = platform_name,
          row_count = nrow(selected),
          spend = sum(selected$spend, na.rm = TRUE),
          impressions = sum(selected$impressions, na.rm = TRUE),
          clicks = sum(selected$clicks, na.rm = TRUE),
          video_views = sum(selected$video_views, na.rm = TRUE),
          video_completions = sum(selected$video_completions, na.rm = TRUE),
          stringsAsFactors = FALSE
        )
      })
      output <- do.call(rbind, output)
      names(output)[-1] <- paste0(prefix, "_", names(output)[-1])
      output
    }

  # ? Reconcile parsed source values to the normalized output by platform
    build_polaris_reconciliation <- function(normalized_rows) {
      raw_summary <- summarize_polaris_metrics(normalized_rows, "raw")
      normalized_summary <- summarize_polaris_metrics(normalized_rows, "normalized")
      output <- merge(raw_summary, normalized_summary, by = "platform", all = TRUE, sort = TRUE)
      for (metric in c("row_count", "spend", "impressions", "clicks", "video_views", "video_completions")) {
        output[[paste0(metric, "_difference")]] <-
          output[[paste0("normalized_", metric)]] - output[[paste0("raw_", metric)]]
      }
      output
    }

  # ? Count rows for every visible platform, package candidate, and review result
    build_polaris_mapping_review <- function(normalized_rows) {
      review <- data.frame(
        platform = ifelse(is.na(normalized_rows$platform), "<missing>", normalized_rows$platform),
        package_friendly_label = ifelse(
          is.na(normalized_rows$package_friendly_label),
          "<unmapped>",
          normalized_rows$package_friendly_label
        ),
        package_id = ifelse(is.na(normalized_rows$package_id), "<unmapped>", normalized_rows$package_id),
        mapping_status = normalized_rows$mapping_status,
        validation_status = normalized_rows$validation_status,
        stringsAsFactors = FALSE
      )
      aggregate(
        list(row_count = rep(1L, nrow(review))),
        by = review,
        FUN = sum
      )
    }

  # ? Convert daily dates to the established Sunday FPD week-start boundary
    polaris_week_start <- function(date) {
      date <- as.Date(date)
      date - as.POSIXlt(date)$wday
    }

  # ? Aggregate approved daily Polaris metrics to package and Sunday-start week
    build_polaris_weekly_package_summary <- function(normalized_rows) {
      ready <- normalized_rows[
        normalized_rows$mapping_status == "mapped" &
          normalized_rows$validation_status == "valid",
      ]
      if (nrow(ready) == 0) {
        return(data.frame(
          package_id = character(0), week_start = as.Date(character(0)),
          week_end = as.Date(character(0)), latest_polaris_date = as.Date(character(0)),
          polaris_spend = numeric(0), polaris_impressions = numeric(0),
          polaris_clicks = numeric(0), polaris_video_views = numeric(0),
          polaris_video_completions = numeric(0), stringsAsFactors = FALSE
        ))
      }
      ready$week_start <- polaris_week_start(ready$date)
      metric_summary <- aggregate(
        cbind(spend, impressions, clicks, video_views, video_completions) ~ package_id + date,
        data = transform(ready, date = week_start),
        FUN = sum,
        na.rm = TRUE
      )
      names(metric_summary) <- c(
        "package_id", "week_start", "polaris_spend", "polaris_impressions",
        "polaris_clicks", "polaris_video_views", "polaris_video_completions"
      )
      latest_date <- aggregate(date ~ package_id + week_start, data = ready, FUN = max)
      names(latest_date)[[3]] <- "latest_polaris_date"
      output <- merge(metric_summary, latest_date, by = c("package_id", "week_start"), sort = TRUE)
      output$week_end <- output$week_start + 6L
      output[, c(
        "package_id", "week_start", "week_end", "latest_polaris_date",
        "polaris_spend", "polaris_impressions", "polaris_clicks",
        "polaris_video_views", "polaris_video_completions"
      )]
    }

  # ? Sum daily Polaris delivery through each native FPD snapshot date
    build_polaris_cumulative_package_summary <- function(normalized_rows, snapshots) {
      ready <- normalized_rows[
        normalized_rows$mapping_status == "mapped" &
          normalized_rows$validation_status == "valid",
      ]
      if (is.data.frame(snapshots)) {
        grid <- unique(snapshots[, c("package_id", "snapshot_date")])
        grid$snapshot_date <- as.Date(grid$snapshot_date)
        grid <- grid[order(grid$package_id, grid$snapshot_date), ]
      } else {
        snapshot_dates <- sort(unique(as.Date(snapshots)))
        grid <- expand.grid(
          package_id = sort(unique(ready$package_id)), snapshot_date = snapshot_dates,
          stringsAsFactors = FALSE
        )
      }
      if (nrow(ready) == 0 || nrow(grid) == 0) {
        return(data.frame(
          package_id = character(0), snapshot_date = as.Date(character(0)),
          latest_polaris_date = as.Date(character(0)),
          polaris_spend = numeric(0), polaris_impressions = numeric(0),
          polaris_clicks = numeric(0), polaris_video_views = numeric(0),
          polaris_video_completions = numeric(0), stringsAsFactors = FALSE
        ))
      }
      output <- lapply(seq_len(nrow(grid)), function(i) {
        package_rows <- ready[
          ready$package_id == grid$package_id[[i]] & ready$date <= grid$snapshot_date[[i]],
        ]
        if (nrow(package_rows) == 0) {
          return(data.frame(
            package_id = grid$package_id[[i]], snapshot_date = grid$snapshot_date[[i]],
            latest_polaris_date = as.Date(NA), polaris_spend = 0,
            polaris_impressions = 0, polaris_clicks = 0, polaris_video_views = 0,
            polaris_video_completions = 0, stringsAsFactors = FALSE
          ))
        }
        data.frame(
          package_id = grid$package_id[[i]], snapshot_date = grid$snapshot_date[[i]],
          latest_polaris_date = max(package_rows$date),
          polaris_spend = sum(package_rows$spend, na.rm = TRUE),
          polaris_impressions = sum(package_rows$impressions, na.rm = TRUE),
          polaris_clicks = sum(package_rows$clicks, na.rm = TRUE),
          polaris_video_views = sum(package_rows$video_views, na.rm = TRUE),
          polaris_video_completions = sum(package_rows$video_completions, na.rm = TRUE),
          stringsAsFactors = FALSE
        )
      })
      do.call(rbind, output)
    }
