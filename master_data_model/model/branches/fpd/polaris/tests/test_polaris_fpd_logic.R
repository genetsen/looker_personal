################################################################################
#### TEST POLARIS FIRST-PARTY DATA PREVIEW LOGIC
################################################################################
# Purpose:
#   Lock the Stage 1 Meta/TikTok schema, approved mappings, validation statuses,
#   duplicate reporting, and metric reconciliation.
# Safe usage:
#   This test uses only in-memory fixtures. It does not access GCS or BigQuery
#   and does not write files.
################################################################################

test_arg <- grep("^--file=", commandArgs(trailingOnly = FALSE), value = TRUE)
test_path <- sub("^--file=", "", test_arg[[1]])
logic_path <- file.path(dirname(dirname(normalizePath(test_path))), "polaris_fpd_logic.R")
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

  # ? Multiple current snapshots stop before a source file is selected
    ambiguous_inventory_error <- tryCatch(
      {
        validate_polaris_source_inventory(data.frame(
          source_object_uri = c("gs://fixture/meta-1.csv", "gs://fixture/meta-2.csv", "gs://fixture/tiktok.csv"),
          source_feed = c("meta", "meta", "tiktok"),
          stringsAsFactors = FALSE
        ))
        NULL
      },
      error = function(error) conditionMessage(error)
    )
    expect_true(
      !is.null(ambiguous_inventory_error) && grepl("expected 1 Meta CSV; found 2", ambiguous_inventory_error),
      "ambiguous source snapshots stop before normalization"
    )

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
        all(meta_normalized$validation_status == "valid"),
      "Meta BOM headers, zero metrics, and Facebook/Instagram mappings normalize safely"
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
        tiktok_normalized$video_completions[[1]] == 9,
      "TikTok alternate headers retain campaign, ad-group, ad, and video detail"
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

cat("All Polaris FPD preview logic tests passed.\n")
