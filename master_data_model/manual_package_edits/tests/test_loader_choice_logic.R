################################################################################
#### TEST MANUAL PACKAGE EDIT CHOICE LOGIC
################################################################################
# These tests cover the small but critical decision rules that decide whether a
# visible sheet value is a real manual override or just the current baseline.
################################################################################

as_trimmed_character <- function(x) {
  out <- trimws(as.character(x))
  out[out == "" | tolower(out) %in% c("na", "nan", "null")] <- NA_character_
  out
}

parse_date <- function(x) {
  if (inherits(x, "Date")) {
    return(x)
  }
  if (inherits(x, "POSIXt")) {
    return(as.Date(x))
  }
  choose_serial_date <- function(serial_value) {
    sheet_date <- as.Date(serial_value, origin = "1899-12-30")
    r_date <- as.Date(serial_value, origin = "1970-01-01")
    use_r_date <- !is.na(r_date) &
      r_date >= as.Date("2000-01-01") &
      r_date <= as.Date("2100-12-31") &
      (is.na(sheet_date) | sheet_date < as.Date("2000-01-01"))
    sheet_date[use_r_date] <- r_date[use_r_date]
    sheet_date
  }
  if (is.numeric(x)) {
    return(choose_serial_date(x))
  }
  y <- trimws(as.character(x))
  y[y == "" | tolower(y) %in% c("na", "nan", "null")] <- NA_character_
  parsed <- suppressWarnings(as.Date(y))
  serial_date <- suppressWarnings(as.numeric(y))
  serial_date[is.na(serial_date)] <- NA_real_
  out <- parsed
  fill_serial <- is.na(out) & !is.na(serial_date)
  out[fill_serial] <- choose_serial_date(serial_date[fill_serial])
  out
}

parse_timestamp <- function(x) {
  if (inherits(x, "POSIXt")) {
    return(as.POSIXct(x, tz = "UTC"))
  }
  if (inherits(x, "Date")) {
    return(as.POSIXct(x, tz = "UTC"))
  }
  y <- trimws(as.character(x))
  y[y == "" | tolower(y) %in% c("na", "nan", "null")] <- NA_character_
  suppressWarnings(as.POSIXct(y, tz = "UTC"))
}

format_date_for_sheet <- function(x) {
  dates <- parse_date(x)
  out <- rep(NA_character_, length(dates))
  out[!is.na(dates)] <- format(dates[!is.na(dates)], "%Y-%m-%d")
  out
}

format_timestamp_for_sheet <- function(x) {
  timestamps <- parse_timestamp(x)
  out <- rep(NA_character_, length(timestamps))
  out[!is.na(timestamps)] <- format(timestamps[!is.na(timestamps)], "%Y-%m-%d %H:%M:%S")
  out
}

prepare_editor_write_data <- function(data) {
  date_cols <- c(
    "Flight Start Date", "Flight End Date",
    "Delivery Override Start Date", "Delivery Override End Date",
    "Baseline Flight Start Date", "Baseline Flight End Date",
    "Baseline Delivery Start Date", "Baseline Delivery End Date"
  )
  for (col in intersect(date_cols, names(data))) {
    data[[col]] <- format_date_for_sheet(data[[col]])
  }

  timestamp_cols <- c("Manual Edit At", "Manual Edit Published At")
  for (col in intersect(timestamp_cols, names(data))) {
    data[[col]] <- format_timestamp_for_sheet(data[[col]])
  }

  data
}

is_publishable_manual_row <- function(data) {
  publish_cols <- c(
    "replacement_spend", "replacement_impressions",
    "replacement_planned_spend", "replacement_planned_impressions",
    "replacement_clicks", "replacement_video_plays", "replacement_video_comps",
    "man_campaign_name", "man_package_name",
    "man_benchmark_kpi", "man_benchmark_value",
    "replacement_flight_start_date", "replacement_flight_end_date"
  )
  apply(!is.na(data[, publish_cols, drop = FALSE]), 1, any)
}

same_num <- function(a, b, tol = 0.01) {
  if (is.na(a) && is.na(b)) return(TRUE)
  if (is.na(a) || is.na(b)) return(FALSE)
  abs(as.numeric(a) - as.numeric(b)) <= tol
}

same_text <- function(a, b) {
  a <- as_trimmed_character(a)
  b <- as_trimmed_character(b)
  if (length(a) == 0) a <- NA_character_
  if (length(b) == 0) b <- NA_character_
  if (is.na(a[[1]]) && is.na(b[[1]])) return(TRUE)
  if (is.na(a[[1]]) || is.na(b[[1]])) return(FALSE)
  a[[1]] == b[[1]]
}

same_date <- function(a, b) {
  a <- parse_date(a)
  b <- parse_date(b)
  if (is.na(a) && is.na(b)) return(TRUE)
  if (is.na(a) || is.na(b)) return(FALSE)
  a == b
}

choose_metric_value <- function(sheet_value, live_value, prior_current, prior_replacement, prior_replacement_trusted = TRUE) {
  if (is.na(sheet_value)) {
    if (isTRUE(prior_replacement_trusted) && !is.na(prior_replacement) && is.na(live_value) && is.na(prior_current)) {
      return(list(value = prior_replacement, edited = TRUE))
    }
    return(list(value = live_value, edited = FALSE))
  }
  if (!is.na(prior_replacement) && same_num(sheet_value, prior_replacement)) {
    if (!isTRUE(prior_replacement_trusted)) {
      if (is.na(live_value) && is.na(prior_current)) {
        return(list(value = sheet_value, edited = TRUE))
      }
      return(list(value = live_value, edited = FALSE))
    }
    return(list(value = sheet_value, edited = TRUE))
  }
  if (is.na(live_value) && !is.na(prior_current) && same_num(sheet_value, prior_current)) {
    return(list(value = prior_current, edited = FALSE))
  }
  if (same_num(sheet_value, live_value)) {
    return(list(value = live_value, edited = FALSE))
  }
  if (!is.na(prior_current) && same_num(sheet_value, prior_current)) {
    return(list(value = live_value, edited = FALSE))
  }
  list(value = sheet_value, edited = TRUE)
}

choose_text_value <- function(sheet_value, live_value, prior_current, prior_replacement, prior_replacement_trusted = TRUE) {
  sheet_value <- as_trimmed_character(sheet_value)
  live_value <- as_trimmed_character(live_value)
  prior_current <- as_trimmed_character(prior_current)
  prior_replacement <- as_trimmed_character(prior_replacement)

  sheet_value <- if (length(sheet_value) == 0) NA_character_ else sheet_value[[1]]
  live_value <- if (length(live_value) == 0) NA_character_ else live_value[[1]]
  prior_current <- if (length(prior_current) == 0) NA_character_ else prior_current[[1]]
  prior_replacement <- if (length(prior_replacement) == 0) NA_character_ else prior_replacement[[1]]

  if (is.na(sheet_value)) {
    if (isTRUE(prior_replacement_trusted) && !is.na(prior_replacement) && is.na(live_value) && is.na(prior_current)) {
      return(list(value = prior_replacement, edited = TRUE))
    }
    return(list(value = live_value, edited = FALSE))
  }
  if (!is.na(prior_replacement) && same_text(sheet_value, prior_replacement)) {
    if (!isTRUE(prior_replacement_trusted)) {
      if (is.na(live_value) && is.na(prior_current)) {
        return(list(value = sheet_value, edited = TRUE))
      }
      return(list(value = live_value, edited = FALSE))
    }
    return(list(value = sheet_value, edited = TRUE))
  }
  if (is.na(live_value) && !is.na(prior_current) && same_text(sheet_value, prior_current)) {
    return(list(value = prior_current, edited = FALSE))
  }
  if (same_text(sheet_value, live_value)) {
    return(list(value = live_value, edited = FALSE))
  }
  if (!is.na(prior_current) && same_text(sheet_value, prior_current)) {
    return(list(value = live_value, edited = FALSE))
  }
  if (is.na(live_value) && is.na(prior_current) && is.na(prior_replacement)) {
    return(list(value = sheet_value, edited = TRUE))
  }
  list(value = sheet_value, edited = TRUE)
}

choose_date_value <- function(sheet_value, live_value, prior_current, prior_manual, prior_active, prior_replacement_trusted = TRUE) {
  if (is.na(sheet_value)) {
    if (isTRUE(prior_replacement_trusted) && prior_active && !is.na(prior_manual) && is.na(live_value) && is.na(prior_current)) {
      return(list(value = prior_manual, edited = TRUE))
    }
    return(list(value = live_value, edited = FALSE))
  }
  if (prior_active && !same_date(prior_manual, prior_current) && same_date(sheet_value, prior_manual)) {
    if (!isTRUE(prior_replacement_trusted)) {
      if (is.na(live_value) && is.na(prior_current)) {
        return(list(value = sheet_value, edited = TRUE))
      }
      return(list(value = live_value, edited = FALSE))
    }
    return(list(value = sheet_value, edited = TRUE))
  }
  if (is.na(live_value) && !is.na(prior_current) && same_date(sheet_value, prior_current)) {
    return(list(value = prior_current, edited = FALSE))
  }
  if (same_date(sheet_value, live_value)) {
    return(list(value = live_value, edited = FALSE))
  }
  if (!is.na(prior_current) && same_date(sheet_value, prior_current)) {
    return(list(value = live_value, edited = FALSE))
  }
  list(value = sheet_value, edited = TRUE)
}

should_stop_manual_only_inactive <- function(
    is_manual_only,
    is_active,
    package_id,
    has_replacement,
    has_metadata_replacement,
    has_flight_replacement,
    manual_edit_at,
    manual_edit_by,
    manual_edit_published_at,
    prior_current_row_count,
    sheet_no_edit_status) {
  manual_evidence <- !is.na(parse_timestamp(manual_edit_at)) |
    !is.na(as_trimmed_character(manual_edit_by)) |
    !is.na(parse_timestamp(manual_edit_published_at))
  prior_count <- suppressWarnings(as.numeric(prior_current_row_count))
  prior_count[is.na(prior_count)] <- 0
  stale_source_no_edit <- (prior_count > 0 | sheet_no_edit_status) &
    !has_replacement &
    !has_metadata_replacement &
    !has_flight_replacement &
    !manual_evidence

  is_manual_only &
    !is_active &
    !is.na(as_trimmed_character(package_id)) &
    !stale_source_no_edit
}

apply_manual_only_flight_date_fallback <- function(raw_upload, display_data) {
  manual_only_rows <- ifelse(is.na(raw_upload$current_row_count), 0, raw_upload$current_row_count) == 0
  fill_start <- manual_only_rows & is.na(raw_upload$man_flight_start_date) & !is.na(raw_upload$man_start_date)
  fill_end <- manual_only_rows & is.na(raw_upload$man_flight_end_date) & !is.na(raw_upload$man_end_date)

  raw_upload$man_flight_start_date[fill_start] <- raw_upload$man_start_date[fill_start]
  raw_upload$replacement_flight_start_date[fill_start & is.na(raw_upload$replacement_flight_start_date)] <-
    raw_upload$man_start_date[fill_start & is.na(raw_upload$replacement_flight_start_date)]
  raw_upload$man_flight_end_date[fill_end] <- raw_upload$man_end_date[fill_end]
  raw_upload$replacement_flight_end_date[fill_end & is.na(raw_upload$replacement_flight_end_date)] <-
    raw_upload$man_end_date[fill_end & is.na(raw_upload$replacement_flight_end_date)]

  if (!is.null(display_data) && nrow(display_data) == nrow(raw_upload)) {
    display_data$`Flight Start Date`[fill_start] <- raw_upload$man_flight_start_date[fill_start]
    display_data$`Flight End Date`[fill_end] <- raw_upload$man_flight_end_date[fill_end]
  }

  list(raw_upload = raw_upload, display_data = display_data)
}

is_trusted_previous_manual_row <- function(data) {
  if (nrow(data) == 0) {
    return(logical())
  }

  (data$is_active %in% TRUE) & (
    as_trimmed_character(data$validation_status) %in% "valid" |
      !is.na(parse_timestamp(data$manual_edit_at)) |
      !is.na(as_trimmed_character(data$manual_edit_by)) |
      !is.na(parse_timestamp(data$manual_edit_published_at))
  )
}

format_date_key <- function(x) {
  dates <- parse_date(x)
  out <- rep("", length(dates))
  out[!is.na(dates)] <- format(dates[!is.na(dates)], "%Y-%m-%d")
  out
}

manual_row_key <- function(package_id, start_date, end_date) {
  paste(as.character(package_id), format_date_key(start_date), format_date_key(end_date), sep = "\001")
}

previous_raw_to_editor_rows <- function(previous_raw) {
  if (nrow(previous_raw) == 0) {
    return(tibble::tibble())
  }

  dplyr::transmute(
    dplyr::filter(previous_raw, is_trusted_previous_manual_row(previous_raw)),
      package_id,
      package_name = dplyr::coalesce(man_package_name, current_package_name, package_name),
      package_name_friendly = dplyr::coalesce(man_package_name_friendly, current_package_name_friendly, package_name_friendly, man_package_name, current_package_name, package_name),
      supplier_code = dplyr::coalesce(man_supplier_code, current_supplier_code, supplier_code),
      supplier_name = dplyr::coalesce(man_supplier_name, current_supplier_name, supplier_name),
      channel = dplyr::coalesce(man_channel, current_channel, channel),
      flight_start_date = dplyr::coalesce(replacement_flight_start_date, man_flight_start_date, man_start_date),
      flight_end_date = dplyr::coalesce(replacement_flight_end_date, man_flight_end_date, man_end_date),
      planned_spend = dplyr::coalesce(replacement_planned_spend, current_planned_spend),
      planned_impressions = dplyr::coalesce(replacement_planned_impressions, current_planned_impressions),
      spend = dplyr::coalesce(replacement_spend, current_spend),
      impressions = dplyr::coalesce(replacement_impressions, current_impressions),
      clicks = dplyr::coalesce(replacement_clicks, current_clicks),
      video_plays = dplyr::coalesce(replacement_video_plays, current_video_plays),
      video_comps = dplyr::coalesce(replacement_video_comps, current_video_comps),
      delivery_start_date = man_start_date,
      delivery_end_date = man_end_date,
      campaign_name = dplyr::coalesce(man_campaign_name, current_campaign_name, campaign_name),
      initiative = dplyr::coalesce(man_initiative, current_initiative),
      advertiser_name = dplyr::coalesce(man_advertiser_name, current_advertiser_name, advertiser_name),
      package_type = dplyr::coalesce(man_package_type, current_package_type, package_type),
      media_name,
      ADIF_channel = dplyr::coalesce(man_ADIF_channel, current_ADIF_channel, ADIF_channel),
      benchmark_kpi = dplyr::coalesce(man_benchmark_kpi, current_benchmark_kpi),
      benchmark_value = dplyr::coalesce(man_benchmark_value, current_benchmark_value),
      channel_group,
      manual_edit_at,
      manual_edit_by,
      manual_edit_published_at,
      manually_edited = "Yes",
      validation_status,
      validation_reason = validation_messages
  )
}

has_editor_manual_evidence <- function(editor_rows) {
  if (nrow(editor_rows) == 0) {
    return(logical())
  }

  pick <- function(col) {
    if (col %in% names(editor_rows)) {
      editor_rows[[col]]
    } else {
      rep(NA_character_, nrow(editor_rows))
    }
  }

  edited_label <- as_trimmed_character(pick("manually_edited"))
  validation_status <- as_trimmed_character(pick("validation_status"))
  manual_edit_by <- as_trimmed_character(pick("manual_edit_by"))

  edited_label %in% c("Yes", "Blocked") |
    validation_status %in% c("valid", "blocked") |
    !is.na(parse_timestamp(pick("manual_edit_at"))) |
    !is.na(manual_edit_by) |
    !is.na(parse_timestamp(pick("manual_edit_published_at")))
}

merge_previous_manual_editor_rows <- function(editor_rows, previous_editor_rows) {
  if (nrow(previous_editor_rows) == 0) {
    return(editor_rows)
  }
  if (nrow(editor_rows) == 0) {
    return(previous_editor_rows)
  }

  editor_keys <- manual_row_key(editor_rows$package_id, editor_rows$delivery_start_date, editor_rows$delivery_end_date)
  editor_keys_with_evidence <- editor_keys[has_editor_manual_evidence(editor_rows)]
  previous_keys <- manual_row_key(previous_editor_rows$package_id, previous_editor_rows$delivery_start_date, previous_editor_rows$delivery_end_date)
  dplyr::bind_rows(editor_rows, previous_editor_rows[!(previous_keys %in% editor_keys_with_evidence), , drop = FALSE])
}

expect_choice <- function(label, actual, expected_value, expected_edited) {
  value_ok <- if (inherits(expected_value, "Date")) {
    same_date(actual$value, expected_value)
  } else if (is.numeric(expected_value)) {
    same_num(actual$value, expected_value)
  } else {
    same_text(actual$value, expected_value)
  }
  edited_ok <- identical(actual$edited, expected_edited)
  if (!value_ok || !edited_ok) {
    stop(
      label,
      " failed. Got value=",
      as.character(actual$value),
      " edited=",
      as.character(actual$edited),
      "; expected value=",
      as.character(expected_value),
      " edited=",
      as.character(expected_edited),
      call. = FALSE
    )
  }
  cat("PASS:", label, "\n")
}

expect_date_parse <- function(label, actual, expected) {
  if (!same_date(actual, expected)) {
    stop(label, " failed. Got ", as.character(actual), "; expected ", as.character(expected), call. = FALSE)
  }
  cat("PASS:", label, "\n")
}

expect_date_parse(
  "Google Sheets date serial parses as the intended modern date",
  parse_date(as.numeric(as.Date("2026-06-08") - as.Date("1899-12-30"))),
  as.Date("2026-06-08")
)

expect_date_parse(
  "R Date internal serial parses as the intended modern date",
  parse_date(as.numeric(as.Date("2026-06-08"))),
  as.Date("2026-06-08")
)

expect_date_parse(
  "list-wrapped sheet date text parses as the intended modern date",
  parse_date(list("2026-08-02")),
  as.Date("2026-08-02")
)

expect_sheet_value <- function(label, actual, expected) {
  if (!identical(actual, expected)) {
    stop(label, " failed. Got ", as.character(actual), "; expected ", as.character(expected), call. = FALSE)
  }
  cat("PASS:", label, "\n")
}

sheet_write_probe <- prepare_editor_write_data(tibble::tibble(
  `Flight Start Date` = as.Date("2026-06-08"),
  `Delivery Override End Date` = as.numeric(as.Date("2026-07-06")),
  `Manual Edit Published At` = as.POSIXct("2026-07-08 15:57:28", tz = "UTC")
))
expect_sheet_value(
  "sheet writeback emits ISO date text for Date values",
  sheet_write_probe$`Flight Start Date`[[1]],
  "2026-06-08"
)
expect_sheet_value(
  "sheet writeback emits ISO date text for R Date serials",
  sheet_write_probe$`Delivery Override End Date`[[1]],
  "2026-07-06"
)

publish_probe <- tibble::tibble(
  replacement_spend = NA_real_,
  replacement_impressions = NA_real_,
  replacement_planned_spend = NA_real_,
  replacement_planned_impressions = NA_real_,
  replacement_clicks = NA_real_,
  replacement_video_plays = NA_real_,
  replacement_video_comps = NA_real_,
  man_campaign_name = NA_character_,
  man_package_name = NA_character_,
  man_benchmark_kpi = NA_character_,
  man_benchmark_value = NA_real_,
  replacement_flight_start_date = as.Date(NA),
  replacement_flight_end_date = as.Date("2026-08-02")
)
expect_sheet_value(
  "valid flight-only manual rows remain publishable",
  as.logical(is_publishable_manual_row(publish_probe)[[1]]),
  TRUE
)

publish_probe_benchmark <- tibble::tibble(
  replacement_spend = NA_real_,
  replacement_impressions = NA_real_,
  replacement_planned_spend = NA_real_,
  replacement_planned_impressions = NA_real_,
  replacement_clicks = NA_real_,
  replacement_video_plays = NA_real_,
  replacement_video_comps = NA_real_,
  man_campaign_name = NA_character_,
  man_package_name = NA_character_,
  man_benchmark_kpi = "CTR",
  man_benchmark_value = 0.0025,
  replacement_flight_start_date = as.Date(NA),
  replacement_flight_end_date = as.Date(NA)
)
expect_sheet_value(
  "valid benchmark-only manual metadata rows remain publishable",
  as.logical(is_publishable_manual_row(publish_probe_benchmark)[[1]]),
  TRUE
)

fallback_probe <- apply_manual_only_flight_date_fallback(
  tibble::tibble(
    current_row_count = NA_real_,
    man_start_date = as.Date("2026-06-08"),
    man_end_date = as.Date("2026-07-06"),
    man_flight_start_date = as.Date(NA),
    man_flight_end_date = as.Date("2026-08-02"),
    replacement_flight_start_date = as.Date(NA),
    replacement_flight_end_date = as.Date("2026-08-02")
  ),
  tibble::tibble(`Flight Start Date` = as.Date(NA), `Flight End Date` = as.Date("2026-08-02"))
)
expect_date_parse(
  "manual-only missing flight start falls back to delivery start",
  fallback_probe$raw_upload$man_flight_start_date[[1]],
  as.Date("2026-06-08")
)
expect_date_parse(
  "manual-only fallback creates replacement flight start evidence",
  fallback_probe$raw_upload$replacement_flight_start_date[[1]],
  as.Date("2026-06-08")
)

prior_editor_probe <- merge_previous_manual_editor_rows(
  tibble::tibble(
    package_id = "P37S8VV",
    delivery_start_date = as.Date(NA),
    delivery_end_date = as.Date(NA)
  ),
  previous_raw_to_editor_rows(tibble::tibble(
    is_active = TRUE,
    validation_status = "valid",
    package_id = "P37S8VV",
    man_start_date = as.Date("2025-10-20"),
    man_end_date = as.Date("2025-12-31"),
    man_flight_start_date = as.Date(NA),
    man_flight_end_date = as.Date(NA),
    replacement_flight_start_date = as.Date(NA),
    replacement_flight_end_date = as.Date(NA),
    replacement_planned_spend = 100,
    current_planned_spend = NA_real_,
    replacement_planned_impressions = NA_real_,
    current_planned_impressions = NA_real_,
    replacement_spend = NA_real_,
    current_spend = NA_real_,
    replacement_impressions = NA_real_,
    current_impressions = NA_real_,
    replacement_clicks = NA_real_,
    current_clicks = NA_real_,
    replacement_video_plays = NA_real_,
    current_video_plays = NA_real_,
    replacement_video_comps = NA_real_,
    current_video_comps = NA_real_,
    man_package_name = "Manual Package",
    current_package_name = NA_character_,
    package_name = NA_character_,
    man_package_name_friendly = NA_character_,
    current_package_name_friendly = NA_character_,
    package_name_friendly = NA_character_,
    man_supplier_code = NA_character_,
    current_supplier_code = NA_character_,
    supplier_code = NA_character_,
    man_supplier_name = "Publisher",
    current_supplier_name = NA_character_,
    supplier_name = NA_character_,
    man_channel = "Digital",
    current_channel = NA_character_,
    channel = NA_character_,
    man_campaign_name = "Campaign",
    current_campaign_name = NA_character_,
    campaign_name = NA_character_,
    man_initiative = NA_character_,
    current_initiative = NA_character_,
    man_advertiser_name = "Advertiser",
    current_advertiser_name = NA_character_,
    advertiser_name = NA_character_,
    man_package_type = "Display",
    current_package_type = NA_character_,
    package_type = NA_character_,
    media_name = "Display",
    man_ADIF_channel = "Display",
    current_ADIF_channel = NA_character_,
    ADIF_channel = NA_character_,
    man_benchmark_kpi = "CTR",
    current_benchmark_kpi = NA_character_,
    man_benchmark_value = 0.0025,
    current_benchmark_value = NA_real_,
    channel_group = "Digital",
    manual_edit_at = as.POSIXct(NA),
    manual_edit_by = NA_character_,
    manual_edit_published_at = as.POSIXct("2026-07-08 15:00:00", tz = "UTC"),
    validation_messages = ""
  ))
)
expect_sheet_value(
  "previous valid manual row is appended beside blank same-package sheet row",
  nrow(prior_editor_probe),
  2L
)
prior_restored_row <- prior_editor_probe[nrow(prior_editor_probe), , drop = FALSE]
expect_sheet_value(
  "previous valid manual row restores one publishable prior row",
  nrow(prior_restored_row),
  1L
)
expect_date_parse(
  "previous valid manual row restores delivery start date",
  prior_restored_row$delivery_start_date[[1]],
  as.Date("2025-10-20")
)
expect_sheet_value(
  "previous valid manual row restores metric edits",
  prior_restored_row$planned_spend[[1]],
  100
)
expect_sheet_value(
  "previous valid manual row restores benchmark KPI",
  prior_restored_row$benchmark_kpi[[1]],
  "CTR"
)

same_key_no_evidence_probe <- merge_previous_manual_editor_rows(
  tibble::tibble(
    package_id = "P37S8VV",
    delivery_start_date = as.Date("2025-10-20"),
    delivery_end_date = as.Date("2025-12-31"),
    manually_edited = "No",
    validation_status = "inactive",
    validation_reason = "not edited"
  ),
  prior_restored_row
)
expect_sheet_value(
  "previous valid manual row is appended beside same-key no-evidence sheet row",
  nrow(same_key_no_evidence_probe),
  2L
)
expect_sheet_value(
  "same-key no-evidence append restores metric edits",
  same_key_no_evidence_probe$planned_spend[[2]],
  100
)

expect_choice(
  "trusted planned-impressions override is preserved even when baseline catches up",
  choose_metric_value(19233094, 19233094, 19024038, 19233094),
  19233094,
  TRUE
)
expect_choice(
  "real existing metric override is preserved when it still differs from current baseline",
  choose_metric_value(20000000, 19233094, 19024038, 20000000),
  20000000,
  TRUE
)
expect_choice(
  "untrusted blocked metric replacement is treated as stale baseline display",
  choose_metric_value(20000000, 19233094, 19024038, 20000000, prior_replacement_trusted = FALSE),
  19233094,
  FALSE
)
expect_choice(
  "untrusted manual-only metric has no live fallback and is preserved",
  choose_metric_value(210000, NA_real_, NA_real_, 210000, prior_replacement_trusted = FALSE),
  210000,
  TRUE
)
expect_choice(
  "manual metric is preserved when source baseline catches up to the replacement",
  choose_metric_value(20000000, 20000000, 19024038, 20000000),
  20000000,
  TRUE
)
expect_choice(
  "manual-only planned metric survives manual feedback baseline",
  choose_metric_value(210000, 210000, NA_real_, 210000),
  210000,
  TRUE
)
expect_choice(
  "correction of an existing metric correction captures the new value",
  choose_metric_value(20100000, 19233094, 19024038, 20000000),
  20100000,
  TRUE
)
expect_choice(
  "metric reverted to old current value is treated as undo",
  choose_metric_value(19024038, 19233094, 19024038, 20000000),
  19233094,
  FALSE
)
expect_choice(
  "blank metric cell reverts to current baseline instead of zero",
  choose_metric_value(NA_real_, 123, 100, 999),
  123,
  FALSE
)
expect_choice(
  "blank manual-only metric preserves trusted prior value when no live baseline exists",
  choose_metric_value(NA_real_, NA_real_, NA_real_, 999),
  999,
  TRUE
)
expect_choice(
  "zero metric is captured as a real override when current baseline is nonzero",
  choose_metric_value(0, 123, 100, NA_real_),
  0,
  TRUE
)
expect_choice(
  "text override is preserved even when new live baseline matches it",
  choose_text_value("New Channel", "New Channel", "Old Channel", "New Channel"),
  "New Channel",
  TRUE
)
expect_choice(
  "manual-only text override survives manual feedback baseline",
  choose_text_value("QUAN", "QUAN", NA_character_, "QUAN"),
  "QUAN",
  TRUE
)
expect_choice(
  "text override persists when live baseline still differs",
  choose_text_value("Corrected Channel", "Old Channel", "Old Channel", "Corrected Channel"),
  "Corrected Channel",
  TRUE
)
expect_choice(
  "text entered into blank baseline is captured as a new edit",
  choose_text_value("CTR", NA_character_, NA_character_, NA_character_),
  "CTR",
  TRUE
)
expect_choice(
  "blank manual-only text preserves trusted prior value when no live baseline exists",
  choose_text_value(NA_character_, NA_character_, NA_character_, "Purely Elizabeth"),
  "Purely Elizabeth",
  TRUE
)
expect_choice(
  "untrusted blocked text replacement is treated as stale baseline display",
  choose_text_value("Corrected Channel", "Old Channel", "Old Channel", "Corrected Channel", prior_replacement_trusted = FALSE),
  "Old Channel",
  FALSE
)
expect_choice(
  "untrusted manual-only text has no live fallback and is preserved",
  choose_text_value("Purely Elizabeth", NA_character_, NA_character_, "Purely Elizabeth", prior_replacement_trusted = FALSE),
  "Purely Elizabeth",
  TRUE
)
expect_choice(
  "date override is preserved even when new live baseline matches it",
  choose_date_value(as.Date("2025-10-01"), as.Date("2025-10-01"), as.Date("2025-09-30"), as.Date("2025-10-01"), TRUE),
  as.Date("2025-10-01"),
  TRUE
)
expect_choice(
  "manual-only date survives manual feedback baseline",
  choose_date_value(as.Date("2026-06-08"), as.Date("2026-06-08"), as.Date(NA), as.Date("2026-06-08"), TRUE),
  as.Date("2026-06-08"),
  TRUE
)
expect_choice(
  "date override persists when live baseline still differs",
  choose_date_value(as.Date("2025-10-02"), as.Date("2025-10-01"), as.Date("2025-10-01"), as.Date("2025-10-02"), TRUE),
  as.Date("2025-10-02"),
  TRUE
)
expect_choice(
  "date entered into blank baseline is captured as a new edit",
  choose_date_value(as.Date("2025-10-02"), as.Date(NA), as.Date(NA), as.Date(NA), FALSE),
  as.Date("2025-10-02"),
  TRUE
)
expect_choice(
  "untrusted blocked date replacement is treated as stale baseline display",
  choose_date_value(as.Date("2025-10-02"), as.Date("2025-10-01"), as.Date("2025-10-01"), as.Date("2025-10-02"), TRUE, prior_replacement_trusted = FALSE),
  as.Date("2025-10-01"),
  FALSE
)
expect_choice(
  "untrusted manual-only date has no live fallback and is preserved",
  choose_date_value(as.Date("2026-06-08"), as.Date(NA), as.Date(NA), as.Date("2026-06-08"), TRUE, prior_replacement_trusted = FALSE),
  as.Date("2026-06-08"),
  TRUE
)
expect_choice(
  "blank date cell reverts to current baseline",
  choose_date_value(as.Date(NA), as.Date("2025-10-01"), as.Date("2025-09-30"), as.Date("2025-10-02"), TRUE),
  as.Date("2025-10-01"),
  FALSE
)
expect_choice(
  "blank manual-only date preserves trusted prior value when no live baseline exists",
  choose_date_value(as.Date(NA), as.Date(NA), as.Date(NA), as.Date("2026-06-08"), TRUE),
  as.Date("2026-06-08"),
  TRUE
)

expect_guard <- function(label, actual, expected) {
  if (!identical(as.logical(actual), as.logical(expected))) {
    stop(label, " failed. Got ", as.character(actual), "; expected ", as.character(expected), call. = FALSE)
  }
  cat("PASS:", label, "\n")
}

expect_guard(
  "stale no-edit source rows that disappear from lookup do not block refresh",
  should_stop_manual_only_inactive(
    is_manual_only = TRUE,
    is_active = FALSE,
    package_id = "P3HN485",
    has_replacement = FALSE,
    has_metadata_replacement = FALSE,
    has_flight_replacement = FALSE,
    manual_edit_at = as.POSIXct(NA),
    manual_edit_by = NA_character_,
    manual_edit_published_at = as.POSIXct(NA),
    prior_current_row_count = 104,
    sheet_no_edit_status = FALSE
  ),
  FALSE
)

expect_guard(
  "stale sheet no-edit rows still do not block after source count evidence is gone",
  should_stop_manual_only_inactive(
    is_manual_only = TRUE,
    is_active = FALSE,
    package_id = "P3HN485",
    has_replacement = FALSE,
    has_metadata_replacement = FALSE,
    has_flight_replacement = FALSE,
    manual_edit_at = as.POSIXct(NA),
    manual_edit_by = NA_character_,
    manual_edit_published_at = as.POSIXct(NA),
    prior_current_row_count = NA_real_,
    sheet_no_edit_status = TRUE
  ),
  FALSE
)

expect_guard(
  "manual-only inactive row with no source fallback still blocks refresh",
  should_stop_manual_only_inactive(
    is_manual_only = TRUE,
    is_active = FALSE,
    package_id = "ccdooh",
    has_replacement = FALSE,
    has_metadata_replacement = FALSE,
    has_flight_replacement = FALSE,
    manual_edit_at = as.POSIXct(NA),
    manual_edit_by = NA_character_,
    manual_edit_published_at = as.POSIXct(NA),
    prior_current_row_count = 0,
    sheet_no_edit_status = FALSE
  ),
  TRUE
)

expect_guard(
  "manual evidence blocks refresh even when prior source row existed",
  should_stop_manual_only_inactive(
    is_manual_only = TRUE,
    is_active = FALSE,
    package_id = "manual-row",
    has_replacement = FALSE,
    has_metadata_replacement = FALSE,
    has_flight_replacement = FALSE,
    manual_edit_at = as.POSIXct(NA),
    manual_edit_by = "gene@example.com",
    manual_edit_published_at = as.POSIXct(NA),
    prior_current_row_count = 104,
    sheet_no_edit_status = TRUE
  ),
  TRUE
)

cat("All manual edit choice logic tests passed.\n")
