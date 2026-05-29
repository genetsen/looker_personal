################################################################################
#### APO SEARCH DATA TEMPLATE SOCIAL RULES
################################################################################
# Purpose:
#   Normalize APO Search Data Template values before loading or merging them
#   into daily cross-platform social data.
# Inputs and outputs:
#   Functions accept sheet values or fallback-source values and return
#   normalized platform/channel identifiers and APO-first resolved values.
# Safe usage:
#   This file contains pure functions only; it does not read Google Sheets or
#   write BigQuery objects.
################################################################################


# * SECTION [1]: VALUE NORMALIZATION

  # Description: Normalize blank text, platforms, and readable synthetic keys.

  # ? Convert sheet text blanks and placeholder null strings to missing values
    as_apo_text <- function(x) {
      out <- trimws(as.character(x))
      out[out == "" | tolower(out) %in% c("na", "nan", "null")] <- NA_character_
      out
    }

  # ? Convert displayed platform names to the existing shared-source vocabulary
    normalize_apo_platform <- function(platform) {
      normalized <- tolower(as_apo_text(platform))
      normalized <- gsub("[^a-z0-9]+", "_", normalized)
      normalized <- gsub("^_+|_+$", "", normalized)
      normalized[normalized %in% c("linkedin", "linkedin_ads")] <- "linkedin_ads"
      normalized[normalized %in% c("google", "google_ads")] <- "google_ads"
      normalized
    }

  # ? Turn a source name into a stable human-readable synthetic identifier part
    apo_slug <- function(value) {
      value <- tolower(as_apo_text(value))
      value <- gsub("[^a-z0-9]+", "_", value)
      gsub("^_+|_+$", "", value)
    }

  # ? Build IDs for APO-only rows where the sheet does not provide source IDs
    synthetic_apo_id <- function(entity, platform, value) {
      paste("apo", apo_slug(entity), apo_slug(platform), apo_slug(value), sep = ":")
    }


# * SECTION [2]: CHANNEL CLASSIFICATION

  # Description: Keep Paid Search and YouTube meaning from campaign naming.

  # ? Package channel fields consistently for downstream reporting
    apo_channel_result <- function(channel, channel_group, media_name, source, status = "publish") {
      list(
        channel = channel,
        channel_group = channel_group,
        media_name = media_name,
        ADIF_channel = media_name,
        classification_source = source,
        publication_status = status
      )
    }

  # ? Prefer trusted campaign markers; use the displayed channel only as fallback
    classify_apo_channel <- function(campaign_name, sheet_channel) {
      campaign <- as_apo_text(campaign_name)
      campaign <- if (length(campaign) == 0 || is.na(campaign[[1]])) "" else campaign[[1]]
      has_search <- grepl("_search_", campaign, ignore.case = TRUE)
      has_youtube <- grepl("_yt_", campaign, ignore.case = TRUE)

      if (has_search && has_youtube) {
        return(apo_channel_result(
          NA_character_, NA_character_, NA_character_,
          "ambiguous_campaign_markers", "exclude_ambiguous_channel"
        ))
      }
      if (has_search) {
        return(apo_channel_result("paid_search", "search", "Paid Search", "campaign_marker_search"))
      }
      if (has_youtube) {
        return(apo_channel_result("video_yt", "video", "Online Video", "campaign_marker_youtube"))
      }

      displayed <- as_apo_text(sheet_channel)
      displayed <- if (length(displayed) == 0 || is.na(displayed[[1]])) NA_character_ else displayed[[1]]
      displayed_slug <- apo_slug(displayed)
      group <- switch(
        displayed_slug,
        "paid_search" = "search",
        "paid_social" = "social",
        "online_video" = "video",
        displayed_slug
      )
      apo_channel_result(displayed_slug, group, displayed, "sheet_channel_fallback")
    }


# * SECTION [3]: COLUMN-LEVEL SOURCE PRECEDENCE

  # Description: Apply APO-first behavior without treating real zeros as blanks.

  # ? Prefer an APO text cell only when it has a nonblank value
    apo_first_text <- function(apo_value, standard_value) {
      apo <- as_apo_text(apo_value)
      standard <- as_apo_text(standard_value)
      if (length(apo) > 0 && !is.na(apo[[1]])) apo[[1]] else standard[[1]]
    }

  # ? Prefer any present APO metric, including zero; otherwise use the fallback
    apo_first_numeric <- function(apo_value, standard_value) {
      if (!is.na(apo_value)) apo_value else standard_value
    }

  # ? Convert supported YouTube Box links to thumbnail image URLs
    transform_apo_creative_box_link <- function(link) {
      link <- as_apo_text(link)
      link[link == "-"] <- NA_character_

      match_source <- ifelse(is.na(link), "", link)
      match_result <- regmatches(
        match_source,
        regexec(
          "(?:youtu\\.be/|youtube\\.com/(?:.*[?&]v=|embed/|shorts/|live/))([A-Za-z0-9_-]{11})",
          match_source,
          ignore.case = TRUE
        )
      )
      video_id <- vapply(
        match_result,
        function(x) if (length(x) >= 2) x[[2]] else NA_character_,
        character(1)
      )

      ifelse(
        !is.na(video_id),
        paste0("https://img.youtube.com/vi/", video_id, "/0.jpg"),
        NA_character_
      )
    }


# * SECTION [4]: REPORT-TO-STAGING NORMALIZATION

  # Description: Convert the visible report plus import IDs to daily ad records.

  # ? Read a named input column or provide missing values with the right length
    apo_column <- function(data, name) {
      if (name %in% names(data)) data[[name]] else rep(NA_character_, nrow(data))
    }

  # ? Parse dates emitted by Google Sheets without using flight-date placeholders
    parse_apo_date <- function(value) {
      if (inherits(value, "Date")) return(value)
      if (inherits(value, "POSIXt")) return(as.Date(value))
      if (is.numeric(value)) return(as.Date(value, origin = "1899-12-30"))
      as.Date(as_apo_text(value))
    }

  # ? Parse monetary and count values while retaining zero as an observed metric
    parse_apo_numeric <- function(value) {
      value <- gsub("[,$]", "", as.character(value))
      value <- as_apo_text(value)
      suppressWarnings(as.numeric(value))
    }

  # ? Normalize one workbook extraction for QA staging and APO-first merging
    normalize_apo_report_rows <- function(report_rows, import_rows, source_sheet_url, loaded_at) {
      report_rows <- as.data.frame(report_rows, stringsAsFactors = FALSE)
      import_rows <- as.data.frame(import_rows, stringsAsFactors = FALSE)
      date_day <- parse_apo_date(apo_column(report_rows, "report_start_date"))
      ad_id <- as_apo_text(apo_column(report_rows, "ad_id"))
      platform <- normalize_apo_platform(apo_column(report_rows, "platform"))
      campaign_name <- as_apo_text(apo_column(report_rows, "campaign"))
      ad_group_name <- as_apo_text(apo_column(report_rows, "ad_group"))

      import_date <- parse_apo_date(apo_column(import_rows, "date_polaris"))
      import_ad_id <- as_apo_text(apo_column(import_rows, "ad_id_polaris"))
      import_ad_group_id <- as_apo_text(apo_column(import_rows, "ad_group_id_polaris"))
      import_key <- paste(import_date, import_ad_id, sep = "|")
      report_key <- paste(date_day, ad_id, sep = "|")
      matched_import <- match(report_key, import_key)
      imported_group_id <- import_ad_group_id[matched_import]

      campaign_id <- mapply(
        function(p, c) synthetic_apo_id("campaign", p, c),
        platform, campaign_name,
        USE.NAMES = FALSE
      )
      fallback_ad_group_id <- mapply(
        function(p, g) synthetic_apo_id("ad_group", p, g),
        platform, ad_group_name,
        USE.NAMES = FALSE
      )
      ad_group_id <- ifelse(
        !is.na(imported_group_id) & imported_group_id != "",
        imported_group_id,
        fallback_ad_group_id
      )
      classified <- lapply(
        seq_len(nrow(report_rows)),
        function(i) classify_apo_channel(campaign_name[[i]], apo_column(report_rows, "channel")[[i]])
      )
      class_value <- function(name) vapply(classified, function(x) x[[name]], character(1))

      result <- data.frame(
        source_relation = platform,
        date_day = date_day,
        platform = platform,
        account_id = paste0("apo:", apo_slug(apo_column(report_rows, "client"))),
        account_name = as_apo_text(apo_column(report_rows, "client")),
        campaign_id = campaign_id,
        campaign_name = campaign_name,
        ad_group_id = ad_group_id,
        ad_group_name = ad_group_name,
        ad_id = ad_id,
        ad_name = as_apo_text(apo_column(report_rows, "ad_name")),
        clicks = parse_apo_numeric(apo_column(report_rows, "clicks")),
        impressions = parse_apo_numeric(apo_column(report_rows, "impressions")),
        spend = parse_apo_numeric(apo_column(report_rows, "spend")),
        conversions = NA_real_,
        conversions_value = NA_real_,
        video_play = NA_real_,
        video_view = NA_real_,
        video_views_p_25 = NA_real_,
        video_views_p_50 = NA_real_,
        video_views_p_75 = NA_real_,
        video_views_p_100 = NA_real_,
        hookrate_num = NA_real_,
        video_flag = NA_character_,
        channel = class_value("channel"),
        channel_group = class_value("channel_group"),
        media_name = class_value("media_name"),
        ADIF_channel = class_value("ADIF_channel"),
        apo_classification_source = class_value("classification_source"),
        apo_publication_status = class_value("publication_status"),
        apo_creative_name = as_apo_text(apo_column(report_rows, "creative_name")),
        apo_creative_img = transform_apo_creative_box_link(apo_column(report_rows, "creative_box_link")),
        apo_source_sheet_url = source_sheet_url,
        apo_loaded_at = as.POSIXct(loaded_at, tz = "UTC"),
        apo_row_key = paste(
          "Apollo",
          platform,
          date_day,
          campaign_id,
          ad_group_id,
          ad_id,
          sep = "|"
        ),
        stringsAsFactors = FALSE
      )
      result <- result[!is.na(result$date_day) & !is.na(result$ad_id), , drop = FALSE]

      # Keep cross-campaign rows visible as separate records while a source
      # owner reviews the reused ad identity; exact record copies stay out.
      publishable <- result$apo_publication_status == "publish"
      duplicate_record_key <- publishable & (
        duplicated(result$apo_row_key) |
          duplicated(result$apo_row_key, fromLast = TRUE)
      )
      ad_identity_key <- paste(
        result$account_name,
        result$platform,
        result$date_day,
        result$ad_id,
        sep = "|"
      )
      cross_campaign_ad_identity <- publishable & !duplicate_record_key & (
        duplicated(ad_identity_key) |
          duplicated(ad_identity_key, fromLast = TRUE)
      )
      result$apo_publication_status[cross_campaign_ad_identity] <- "publish_pending_source_owner_review"
      result$apo_publication_status[duplicate_record_key] <- "exclude_duplicate_record_key"
      result
    }
