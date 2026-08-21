################################################################################
#### TEST POLARIS EMAIL DELIVERY LOGIC
################################################################################
# Purpose:
#   Lock content-based Meta/TikTok source selection, approved mappings,
#   validation statuses, duplicate reporting, and metric reconciliation.
# Safe usage:
#   This test uses only in-memory fixtures. It does not access GCS or BigQuery
#   and does not write files.
################################################################################

test_arg <- grep("^--file=", commandArgs(trailingOnly = FALSE), value = TRUE)
test_path <- sub("^--file=", "", test_arg[[1]])
logic_path <- file.path(dirname(dirname(normalizePath(test_path))), "polaris_email_delivery_logic.R")
source(logic_path)


# * SECTION [1]: TEST HELPERS

  # Description: Provide readable assertions without adding a test framework.

  # ? Stop with the behavior label when an expectation does not hold
    expect_true <- function(condition, label) {
      if (!isTRUE(condition)) stop(paste("FAIL:", label), call. = FALSE)
      cat("PASS:", label, "\n")
    }


# * SECTION [2]: SOURCE SCHEMAS AND MAPPINGS

  # Description: Prove both source shapes normalize to the approved common fields.

  # ? Meta BOM-style headers and observed zeros retain their intended meaning
    meta_fixture <- data.frame(
      campaign_name = c("Awareness Campaign", "Awareness Campaign"),
      adset_name = c("Interests - Facebook", "ACR - Instagram"),
      Platform = c("Facebook", "Instagram"),
      ad_name = c("Creative A", "Creative B"),
      date = c("2026-07-20", "2026-07-21"),
      `Billable Spend` = c("0", "10.25"),
      impressions = c("0", "100"),
      Link_click = c("0", "2"),
      video_view = c("0", "50"),
      `Video View to 100%` = c("0", "5"),
      check.names = FALSE,
      stringsAsFactors = FALSE
    )
    names(meta_fixture)[[1]] <- paste0("\ufeff", names(meta_fixture)[[1]])
    meta_normalized <- classify_polaris_rows(normalize_polaris_meta(meta_fixture, "gs://fixture/meta.csv"))
    expect_true(
      nrow(meta_normalized) == 2 &&
        identical(meta_normalized$package_id, c("P3HF7QB", "P3HF7T8")) &&
        meta_normalized$spend[[1]] == 0 &&
        inherits(meta_normalized$raw_date, "Date") &&
        is.numeric(meta_normalized$raw_spend) &&
        is.integer(meta_normalized$raw_impressions) &&
        is.integer(meta_normalized$raw_clicks) &&
        is.integer(meta_normalized$raw_video_views) &&
        is.integer(meta_normalized$raw_video_completions) &&
        all(meta_normalized$validation_status == "valid"),
      "Meta dates and metrics, including upstream audit fields, remain typed"
    )

  # ? TikTok alternate headers normalize into the same daily creative-level fields
    tiktok_fixture <- data.frame(
      `Campaign Name` = "Purely Elizabeth - Awareness Q3",
      `Ad Group Name` = "Interests",
      `Ad Name` = "Creative C",
      `Date Start` = "2026-07-20",
      `Billable Spend` = "20.50",
      Impressions = "200",
      `Clicks (Destination)` = "3",
      `Video Views` = "190",
      `Video Views at 100%` = "9",
      check.names = FALSE,
      stringsAsFactors = FALSE
    )
    tiktok_normalized <- classify_polaris_rows(
      normalize_polaris_tiktok(tiktok_fixture, "gs://fixture/tiktok.csv")
    )
    expect_true(
      tiktok_normalized$platform[[1]] == "TikTok" &&
        tiktok_normalized$package_id[[1]] == "P3HF88Q" &&
        tiktok_normalized$campaign_name[[1]] == "Purely Elizabeth - Awareness Q3" &&
        tiktok_normalized$video_completions[[1]] == 9 &&
        inherits(tiktok_normalized$raw_date, "Date") &&
        is.numeric(tiktok_normalized$raw_spend) &&
        is.integer(tiktok_normalized$raw_impressions) &&
        is.integer(tiktok_normalized$raw_clicks) &&
        is.integer(tiktok_normalized$raw_video_views) &&
        is.integer(tiktok_normalized$raw_video_completions),
      "TikTok dates and metrics, including upstream audit fields, remain typed"
    )

  # ? Arbitrary paths are classified only from the documented source schemas
    expect_true(
      classify_polaris_source_schema(meta_fixture) == "meta" &&
        classify_polaris_source_schema(tiktok_fixture) == "tiktok" &&
        classify_polaris_source_schema(data.frame(unrelated = "value")) == "unsupported",
      "source schemas classify independently of filenames and folder names"
    )

  # ? The uniquely newest business-date snapshot is selected for each feed
    selected_inventory <- validate_polaris_source_inventory(data.frame(
      source_object_uri = c(
        "gs://fixture/arbitrary-a.csv", "gs://fixture/arbitrary-b.csv",
        "gs://fixture/arbitrary-c.csv"
      ),
      source_feed = c("meta", "meta", "tiktok"),
      source_snapshot_max_date = as.Date(c("2026-08-17", "2026-08-20", "2026-08-17")),
      stringsAsFactors = FALSE
    ))
    expect_true(
      identical(
        selected_inventory$selection_status,
        c("superseded_snapshot", "selected_current", "selected_current")
      ),
      "newest source dates select current snapshots without using paths"
    )

  # ? Unsupported schemas remain a hard stop even when both feeds are present
    unsupported_inventory_error <- tryCatch(
      {
        validate_polaris_source_inventory(data.frame(
          source_object_uri = c("gs://fixture/a.csv", "gs://fixture/b.csv", "gs://fixture/c.csv"),
          source_feed = c("meta", "tiktok", "unsupported"),
          source_snapshot_max_date = as.Date(c("2026-08-20", "2026-08-17", NA)),
          stringsAsFactors = FALSE
        ))
        NULL
      },
      error = function(error) conditionMessage(error)
    )
    expect_true(
      !is.null(unsupported_inventory_error) && grepl("unsupported CSV object", unsupported_inventory_error),
      "unsupported source schemas stop before normalization"
    )

  # ? Tied newest snapshots stop rather than relying on filename or upload order
    tied_inventory_error <- tryCatch(
      {
        validate_polaris_source_inventory(data.frame(
          source_object_uri = c("gs://fixture/a.csv", "gs://fixture/b.csv", "gs://fixture/c.csv"),
          source_feed = c("meta", "meta", "tiktok"),
          source_snapshot_max_date = as.Date(c("2026-08-20", "2026-08-20", "2026-08-17")),
          stringsAsFactors = FALSE
        ))
        NULL
      },
      error = function(error) conditionMessage(error)
    )
    expect_true(
      !is.null(tied_inventory_error) && grepl("tied for newest source date", tied_inventory_error),
      "tied newest snapshots stop before normalization"
    )

  # ? Object checkpoints select every newly arrived object per feed
    source_state_fixture <- data.frame(
      source_feed = c("meta", "tiktok"),
      source_object_uri = c(
        "gs://fixture/accepted-meta.csv", "gs://fixture/accepted-tiktok.csv"
      ),
      source_object_generation = c("1787245252374637", "1787245228232875"),
      object_created_at = as.POSIXct(
        c("2026-08-20 17:00:52", "2026-08-20 17:00:28"),
        tz = "UTC"
      ),
      source_max_date = as.Date(c("2026-08-17", "2026-08-17")),
      stringsAsFactors = FALSE
    )
    metadata_inventory <- data.frame(
      source_object_uri = paste0("gs://fixture/", c(
        "accepted-meta.csv", "new-meta-a.csv", "new-meta-b.csv", "accepted-tiktok.csv"
      )),
      source_object_generation = c(
        "1787245252374637", "1787340500000000", "1787340573113455", "1787245228232875"
      ),
      object_created_at = as.POSIXct(c(
        "2026-08-20 17:00:52", "2026-08-21 19:28:00",
        "2026-08-21 19:29:33", "2026-08-20 17:00:28"
      ), tz = "UTC"),
      source_feed = c("meta", "meta", "meta", "tiktok"),
      stringsAsFactors = FALSE
    )
    incremental_inventory <- select_new_polaris_objects(
      metadata_inventory,
      source_state_fixture
    )
    expect_true(
      identical(
        incremental_inventory$selection_status,
        c("not_new", "selected_new", "selected_new", "not_new")
      ),
      "source checkpoints select every newly arrived object per feed"
    )

  # ? Multiple objects are processed oldest first and checkpoint only the newest
    ordered_incremental <- order_polaris_objects(incremental_inventory[
      incremental_inventory$selection_status == "selected_new", , drop = FALSE
    ])
    latest_incremental <- latest_polaris_objects_by_feed(ordered_incremental)
    expect_true(
      identical(ordered_incremental$source_object_uri, paste0(
        "gs://fixture/", c("new-meta-a.csv", "new-meta-b.csv")
      )) && latest_incremental$source_object_uri[[1]] == "gs://fixture/new-meta-b.csv",
      "multiple arrivals run oldest to newest and checkpoint the newest success"
    )

  # ? An accepted generation stays processed when timestamp precision differs
    accepted_with_time_drift <- metadata_inventory[4, , drop = FALSE]
    accepted_with_time_drift$object_created_at <-
      source_state_fixture$object_created_at[source_state_fixture$source_feed == "tiktok"] + 0.5
    generation_match <- select_new_polaris_objects(
      accepted_with_time_drift,
      source_state_fixture
    )
    expect_true(
      generation_match$selection_status[[1]] == "not_new",
      "an accepted object generation is not reprocessed because of timestamp precision"
    )

  # ? Equal business dates allow corrected object generations but older dates stop
    selected_progress <- incremental_inventory[
      incremental_inventory$selection_status == "selected_new", , drop = FALSE
    ]
    selected_progress$source_snapshot_max_date <- as.Date("2026-08-17")
    expect_true(
      isTRUE(validate_polaris_source_progress(selected_progress, source_state_fixture)),
      "a new object generation may correct an already loaded maximum business date"
    )
    selected_progress$source_snapshot_max_date <- as.Date("2026-08-16")
    regression_error <- tryCatch(
      {
        validate_polaris_source_progress(selected_progress, source_state_fixture)
        NULL
      },
      error = function(error) conditionMessage(error)
    )
    expect_true(
      !is.null(regression_error) && grepl("moved backward", regression_error),
      "a newly arrived snapshot cannot move a feed backward"
    )

  # ? Production mappings use the full feed/platform/campaign/ad-group key
    production_mappings <- data.frame(
      source_feed = c("meta", "meta", "meta", "meta", "tiktok"),
      platform = c("Facebook", "Facebook", "Instagram", "Instagram", "TikTok"),
      campaign_name = c(
        rep("Awareness Campaign", 4),
        "Purely Elizabeth - Awareness Q3"
      ),
      ad_group_name = c(
        "ACR - Facebook", "Interests - Facebook",
        "ACR - Instagram", "Interests - Instagram", "Interests"
      ),
      package_id = c("P3HF7QB", "P3HF7QB", "P3HF7T8", "P3HF7T8", "P3HF88Q"),
      package_friendly_label = c(
        "Facebook Awareness", "Facebook Awareness", "Instagram", "Instagram", "TikTok"
      ),
      is_active = TRUE,
      stringsAsFactors = FALSE
    )
    production_meta <- apply_polaris_package_mappings(
      normalize_polaris_meta(meta_fixture, "gs://fixture/meta.csv"),
      production_mappings
    )
    production_tiktok <- apply_polaris_package_mappings(
      normalize_polaris_tiktok(tiktok_fixture, "gs://fixture/tiktok.csv"),
      production_mappings
    )
    expect_true(
      identical(production_meta$package_id, c("P3HF7QB", "P3HF7T8")) &&
        production_tiktok$package_id[[1]] == "P3HF88Q" &&
        all(c(production_meta$mapping_status, production_tiktok$mapping_status) == "mapped"),
      "all five approved production source keys map through the composite contract"
    )


# * SECTION [3]: REVIEW FAILURES AND DUPLICATES

  # Description: Prove unsafe values remain visible and never receive inferred mappings.

  # ? Unknown platforms remain unmapped instead of borrowing a nearby package
    unknown_platform <- meta_fixture[1, ]
    names(unknown_platform)[[1]] <- sub("^\ufeff", "", names(unknown_platform)[[1]])
    unknown_platform$Platform <- "Reddit"
    unknown_normalized <- classify_polaris_rows(
      normalize_polaris_meta(unknown_platform, "gs://fixture/unknown.csv")
    )
    expect_true(
      unknown_normalized$mapping_status[[1]] == "unmapped_platform" &&
        is.na(unknown_normalized$package_id[[1]]) &&
        unknown_normalized$review_status[[1]] == "needs_review",
      "unknown platforms remain unmapped and visible for review"
    )

  # ? A known platform with an unknown campaign/ad-group key still stops mapping
    unknown_key_fixture <- meta_fixture[1, ]
    names(unknown_key_fixture)[[1]] <- sub("^\ufeff", "", names(unknown_key_fixture)[[1]])
    unknown_key_fixture$adset_name <- "Unapproved Audience"
    unknown_key <- apply_polaris_package_mappings(
      normalize_polaris_meta(unknown_key_fixture, "gs://fixture/unknown-key.csv"),
      production_mappings
    )
    expect_true(
      unknown_key$mapping_status[[1]] == "unmapped_source_key" &&
        is.na(unknown_key$package_id[[1]]),
      "production mapping does not infer a package from platform alone"
    )

  # ? Duplicate active mapping owners are rejected before source classification
    ambiguous_mapping_error <- tryCatch(
      {
        apply_polaris_package_mappings(
          normalize_polaris_meta(meta_fixture[1, ], "gs://fixture/ambiguous-map.csv"),
          rbind(production_mappings, production_mappings[1, ])
        )
        NULL
      },
      error = function(error) conditionMessage(error)
    )
    expect_true(
      !is.null(ambiguous_mapping_error) && grepl("ambiguous active source keys", ambiguous_mapping_error),
      "duplicate active production mappings fail before a package is selected"
    )

  # ? Missing names, invalid dates, and invalid metrics receive specific statuses
    invalid_rows <- meta_fixture[c(1, 1, 1), ]
    names(invalid_rows)[[1]] <- sub("^\ufeff", "", names(invalid_rows)[[1]])
    invalid_rows$ad_name[[1]] <- ""
    invalid_rows$date[[2]] <- "07/21/2026"
    invalid_rows$`Billable Spend`[[3]] <- "not-a-number"
    invalid_normalized <- classify_polaris_rows(
      normalize_polaris_meta(invalid_rows, "gs://fixture/invalid.csv")
    )
    expect_true(
      identical(
        invalid_normalized$validation_status,
        c("missing_required_value", "invalid_date", "invalid_metric")
      ),
      "missing required values, invalid dates, and invalid metrics stay visible"
    )

  # ? Exact natural-key duplicates are labeled but not removed
    duplicate_fixture <- rbind(tiktok_fixture, tiktok_fixture)
    duplicate_normalized <- classify_polaris_rows(
      normalize_polaris_tiktok(duplicate_fixture, "gs://fixture/duplicate.csv")
    )
    expect_true(
      nrow(duplicate_normalized) == 2 &&
        all(duplicate_normalized$natural_row_occurrences == 2) &&
        all(duplicate_normalized$validation_status == "duplicate_natural_key"),
      "duplicate natural rows are retained and explicitly labeled"
    )


# * SECTION [4]: RECONCILIATION

  # Description: Prove the review summaries reconcile without changing detail grain.

  # ? Raw and normalized platform totals match exactly for valid fixture values
    combined <- rbind(meta_normalized, tiktok_normalized)
    reconciliation <- build_polaris_reconciliation(combined)
    difference_columns <- grep("_difference$", names(reconciliation), value = TRUE)
    expect_true(
      nrow(combined) == 3 &&
        all(vapply(reconciliation[difference_columns], function(value) all(value == 0), logical(1))),
      "row and metric totals reconcile exactly without collapsing source detail"
    )

  # ? Daily rows roll into the established Sunday-start FPD comparison week
    weekly_fixture <- rbind(tiktok_fixture, tiktok_fixture)
    weekly_fixture$`Date Start` <- c("2026-07-25", "2026-07-26")
    weekly_rows <- classify_polaris_rows(
      normalize_polaris_tiktok(weekly_fixture, "gs://fixture/weekly.csv")
    )
    weekly_summary <- build_polaris_weekly_package_summary(weekly_rows)
    expect_true(
      nrow(weekly_summary) == 2 &&
        identical(as.character(weekly_summary$week_start), c("2026-07-19", "2026-07-26")) &&
        identical(as.character(weekly_summary$week_end), c("2026-07-25", "2026-08-01")) &&
        all(weekly_summary$polaris_spend == 20.5),
      "Saturday and Sunday fall into separate Sunday-start FPD weeks"
    )

  # ? Cumulative snapshots use all Polaris delivery through each native date
    cumulative_summary <- build_polaris_cumulative_package_summary(
      weekly_rows,
      c("2026-07-25", "2026-07-26")
    )
    expect_true(
      nrow(cumulative_summary) == 2 &&
        identical(as.character(cumulative_summary$snapshot_date), c("2026-07-25", "2026-07-26")) &&
        all(cumulative_summary$polaris_spend == c(20.5, 41)) &&
        identical(as.character(cumulative_summary$latest_polaris_date), c("2026-07-25", "2026-07-26")),
      "cumulative snapshot summaries include Polaris delivery through each snapshot date"
    )

  # ? Package-specific snapshot input does not invent cross-package dates
    package_snapshot_summary <- build_polaris_cumulative_package_summary(
      weekly_rows,
      data.frame(package_id = "P3HF88Q", snapshot_date = as.Date("2026-07-26"))
    )
    expect_true(
      nrow(package_snapshot_summary) == 1 &&
        package_snapshot_summary$package_id == "P3HF88Q" &&
        package_snapshot_summary$polaris_spend == 41,
      "package-specific snapshots create only real comparison pairs"
    )

# * SECTION [7]: PRODUCTION UPSERT CONTRACT

  # Description: Prevent a future loader edit from deleting an entire feed again.

  # ? Confirm production deletion is limited to the staged natural keys
    loader_path <- file.path(dirname(dirname(normalizePath(test_path))), "load_polaris_email_delivery.R")
    loader_text <- paste(readLines(loader_path, warn = FALSE), collapse = "\n")
    expect_true(
      grepl(
        "production.natural_row_key = candidate.natural_row_key",
        loader_text,
        fixed = TRUE
      ),
      "production upsert deletes only overlapping natural keys and preserves older history"
    )
    expect_true(
      grepl(
        "production_id, delivery_staging_id",
        loader_text,
        fixed = TRUE
      ),
      "production upsert compares row keys against delivery staging"
    )

  # ? Overlapping files keep the later row while same-file duplicates remain errors
    older_overlap <- production_meta[1, , drop = FALSE]
    newer_overlap <- older_overlap
    older_overlap$source_object_uri <- "gs://fixture/older.csv"
    newer_overlap$source_object_uri <- "gs://fixture/newer.csv"
    newer_overlap$spend <- 99
    collapsed_overlap <- collapse_polaris_object_overlaps(rbind(older_overlap, newer_overlap))
    expect_true(
      nrow(collapsed_overlap) == 1L &&
        collapsed_overlap$source_object_uri[[1]] == "gs://fixture/newer.csv" &&
        collapsed_overlap$spend[[1]] == 99 &&
        collapsed_overlap$natural_row_occurrences[[1]] == 1L,
      "later delivery windows replace only their overlapping natural rows"
    )
    same_file_duplicate_error <- tryCatch(
      {
        collapse_polaris_object_overlaps(rbind(older_overlap, older_overlap))
        NULL
      },
      error = function(error) conditionMessage(error)
    )
    expect_true(
      !is.null(same_file_duplicate_error) && grepl("duplicate natural row keys", same_file_duplicate_error),
      "duplicates inside one source object still stop the load"
    )

cat("All Polaris Email delivery logic tests passed.\n")
