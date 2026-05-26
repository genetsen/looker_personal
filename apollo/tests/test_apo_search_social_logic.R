################################################################################
#### TEST APO SEARCH DATA TEMPLATE SOCIAL LOGIC
################################################################################
# Purpose:
#   Lock the business rules for normalizing APO Search Data Template records
#   before they are merged into shared social staging.
# Safe usage:
#   This file is read-only with respect to Sheets and BigQuery. Run with
#   Rscript apollo/tests/test_apo_search_social_logic.R from the repo root.
################################################################################

source("apollo/apo_search_social_logic.R")


# * SECTION [1]: TEST HELPERS

  # Description: Keep assertions readable without requiring a test framework.

  # ? Fail with the behavior label when an assertion is false
    expect_true <- function(condition, label) {
      if (!isTRUE(condition)) {
        stop(paste("FAIL:", label), call. = FALSE)
      }
      cat("PASS:", label, "\n")
    }


# * SECTION [2]: PLATFORM AND CHANNEL CLASSIFICATION

  # Description: Prove the source naming rules agreed for LinkedIn and Google.

  # ? Platform display names normalize to shared staging vocabulary
    expect_true(
      normalize_apo_platform("LinkedIn") == "linkedin_ads" &&
        normalize_apo_platform("Google") == "google_ads",
      "LinkedIn and Google normalize to existing shared platform values"
    )

  # ? Campaign markers override the displayed sheet channel
    search_channel <- classify_apo_channel("G_Search_Brand_Apollo", "Paid Social")
    expect_true(
      search_channel$publication_status == "publish" &&
        search_channel$channel == "paid_search" &&
        search_channel$channel_group == "search" &&
        search_channel$media_name == "Paid Search",
      "_Search_ campaign rows are classified as Paid Search"
    )

    youtube_channel <- classify_apo_channel("G_YT_Video_Apollo", "Paid Search")
    expect_true(
      youtube_channel$publication_status == "publish" &&
        youtube_channel$channel == "video_online" &&
        youtube_channel$channel_group == "video" &&
        youtube_channel$media_name == "Online Video",
      "_YT_ campaign rows are classified as Online Video"
    )

  # ? The sheet channel is fallback-only and ambiguous markers do not publish
    fallback_channel <- classify_apo_channel("Apollo_Brand_Awareness", "Paid Social")
    ambiguous_channel <- classify_apo_channel("G_Search_YT_Apollo", "Paid Search")
    expect_true(
      fallback_channel$classification_source == "sheet_channel_fallback" &&
        fallback_channel$media_name == "Paid Social" &&
        ambiguous_channel$publication_status == "exclude_ambiguous_channel",
      "sheet channel is fallback-only and ambiguous marker rows are excluded"
    )


# * SECTION [3]: IDENTIFIERS AND SOURCE PRECEDENCE

  # Description: Verify deterministic APO-only keys and column-level fallback.

  # ? New APO rows can be keyed when the sheet has no platform campaign ID
    expect_true(
      synthetic_apo_id("campaign", "google_ads", "G Search Brand Apollo") ==
        "apo:campaign:google_ads:g_search_brand_apollo",
      "name-based synthetic IDs are stable and readable"
    )

  # ? APO zero metrics win; only blank or missing APO values fall back
    expect_true(
      apo_first_numeric(0, 27) == 0 &&
        apo_first_numeric(NA_real_, 27) == 27 &&
        apo_first_text("", "standard") == "standard" &&
        apo_first_text("APO value", "standard") == "APO value",
      "APO nonblank values including zero win at column level"
    )


# * SECTION [4]: REPORT NORMALIZATION

  # Description: Prove the staging-table row shape without contacting Sheets.

  # ? Normalization recovers ad-group ID and ignores placeholder flight dates
    report_fixture <- data.frame(
      client = c("Apollo Global Management, Inc", "Apollo - Giant Spoon"),
      channel = c("Paid Social", "Paid Search"),
      platform = c("Google", "Google"),
      ad_group = c("G_Search_Brand_Apollo", "G_YT_Video_Apollo"),
      campaign = c("G_Search_Brand_Apollo_Modifier", "G_YT_Video_Apollo"),
      flight_start_date = c("Flight Start Date", "Flight Start Date"),
      flight_end_date = c("Flight End Date", "Flight End Date"),
      report_start_date = as.Date(c("2026-05-07", "2026-05-08")),
      ad_name = c("Apollo", "YT Apollo"),
      spend = c("$0.00", "$8.50"),
      impressions = c("0", "100"),
      clicks = c("0", "3"),
      creative_name = c("Search Creative", "YT Creative"),
      ad_id = c("791206330446", "807281496954"),
      creative_box_link = c("-", "https://example.com/youtube"),
      stringsAsFactors = FALSE
    )
    import_fixture <- data.frame(
      date_polaris = as.Date(c("2026-05-07", "2026-05-08")),
      ad_id_polaris = c("791206330446", "807281496954"),
      ad_group_id_polaris = c("991", "992"),
      stringsAsFactors = FALSE
    )
    normalized <- normalize_apo_report_rows(
      report_fixture,
      import_fixture,
      "https://docs.google.com/spreadsheets/d/test",
      as.POSIXct("2026-05-26 12:00:00", tz = "UTC")
    )
    expect_true(
      nrow(normalized) == 2 &&
        normalized$ad_group_id[[1]] == "991" &&
        normalized$channel[[1]] == "paid_search" &&
        normalized$channel[[2]] == "video_online" &&
        normalized$spend[[1]] == 0 &&
        is.character(normalized$ad_id) &&
        is.character(normalized$ad_group_id) &&
        is.numeric(normalized$conversions) &&
        !("flight_start_date" %in% names(normalized)),
      "report rows normalize with shared-source types and without placeholder flight dates"
    )

  # ? Conflicting duplicate daily-ad keys are visible but held out of publishing
    duplicate_report <- rbind(report_fixture[1, ], report_fixture[1, ])
    duplicate_report$campaign <- c(
      "G_Search_NOB_Apollo_Private_Credit",
      "G_Search_NOB_Apollo_Private_Credit Search | Private Credit | tIS"
    )
    duplicate_report$spend <- c("$64.61", "$78.85")
    duplicate_import <- rbind(import_fixture[1, ], import_fixture[1, ])
    duplicate_normalized <- normalize_apo_report_rows(
      duplicate_report,
      duplicate_import,
      "https://docs.google.com/spreadsheets/d/test",
      as.POSIXct("2026-05-26 12:00:00", tz = "UTC")
    )
    expect_true(
      all(duplicate_normalized$apo_publication_status == "exclude_duplicate_daily_ad_key"),
      "conflicting duplicate daily-ad keys are held out for QA review"
    )

cat("All APO search social logic tests passed.\n")
