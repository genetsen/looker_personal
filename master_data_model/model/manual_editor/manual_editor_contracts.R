################################################################################
#### MANUAL DATA EDITOR ROW AND METRIC-DATE CONTRACTS
################################################################################
# Purpose:
#   Keep the Package Editor's two core business rules pure and testable:
#   source lookup rows determine normal visible packages, while planned and
#   delivered metrics use their respective date ranges. The loader imports this
#   file before it reads or writes Google Sheets and BigQuery.
################################################################################

manual_editor_text <- function(x) {
  out <- trimws(as.character(x))
  out[out == "" | tolower(out) %in% c("na", "nan", "null")] <- NA_character_
  out
}

manual_editor_has_audit <- function(data) {
  pick <- function(name) {
    if (name %in% names(data)) data[[name]] else rep(NA_character_, nrow(data))
  }
  !is.na(manual_editor_text(pick("manual_edit_at"))) |
    !is.na(manual_editor_text(pick("manual_edit_by")))
}

manual_editor_has_values <- function(data) {
  candidate_columns <- intersect(c(
    "package_name", "package_name_friendly", "supplier_code", "supplier_name",
    "channel", "flight_start_date", "flight_end_date", "delivery_start_date",
    "delivery_end_date", "planned_spend", "planned_impressions", "spend",
    "impressions", "clicks", "video_plays", "video_comps", "campaign_name",
    "initiative", "advertiser_name", "package_type", "media_name", "ADIF_channel",
    "benchmark_kpi", "benchmark_value"
  ), names(data))
  if (length(candidate_columns) == 0) return(rep(FALSE, nrow(data)))
  apply(data[, candidate_columns, drop = FALSE], 1, function(row) {
    any(!is.na(manual_editor_text(row)))
  })
}

manual_editor_same_baseline_value <- function(sheet_value, source_value) {
  sheet_text <- manual_editor_text(sheet_value)
  source_text <- manual_editor_text(source_value)
  if (is.na(sheet_text) || is.na(source_text)) return(is.na(sheet_text) && is.na(source_text))

  sheet_number <- suppressWarnings(as.numeric(gsub("[,$% ]", "", sheet_text)))
  source_number <- suppressWarnings(as.numeric(gsub("[,$% ]", "", source_text)))
  if (!is.na(sheet_number) && !is.na(source_number)) {
    return(abs(sheet_number - source_number) < 0.000001)
  }
  identical(sheet_text, source_text)
}

manual_editor_matches_live_baseline <- function(candidate_rows, live_row) {
  source_map <- c(
    flight_start_date = "current_flight_start_date",
    flight_end_date = "current_flight_end_date",
    planned_spend = "current_planned_spend",
    planned_impressions = "current_planned_impressions",
    spend = "current_spend",
    impressions = "current_impressions",
    clicks = "current_clicks",
    video_plays = "current_video_plays",
    video_comps = "current_video_comps",
    delivery_start_date = "current_first_date",
    delivery_end_date = "current_last_date"
  )
  available_fields <- names(source_map)[source_map %in% names(live_row) & names(source_map) %in% names(candidate_rows)]
  if (length(available_fields) == 0) return(rep(FALSE, nrow(candidate_rows)))

  vapply(seq_len(nrow(candidate_rows)), function(row_index) {
    all(vapply(available_fields, function(sheet_field) {
      manual_editor_same_baseline_value(
        candidate_rows[[sheet_field]][[row_index]],
        live_row[[source_map[[sheet_field]]]][[1]]
      )
    }, logical(1)))
  }, logical(1))
}

collapse_manual_only_sheet_rows <- function(sheet_only_rows, trusted_manual_only_ids) {
  if (nrow(sheet_only_rows) == 0) return(sheet_only_rows)

  candidates <- sheet_only_rows[
    manual_editor_has_audit(sheet_only_rows) |
      manual_editor_text(sheet_only_rows$package_id) %in% manual_editor_text(trusted_manual_only_ids),
    , drop = FALSE
  ]
  if (nrow(candidates) == 0) return(candidates)

  candidate_groups <- split(candidates, candidates$package_id)
  selected_rows <- lapply(candidate_groups, function(package_rows) {
    accepted_rows <-
      manual_editor_text(package_rows$manually_edited) %in% "Yes" &
      manual_editor_text(package_rows$validation_status) %in% "valid"
    if (sum(accepted_rows, na.rm = TRUE) == 1) {
      return(package_rows[which(accepted_rows)[[1]], , drop = FALSE])
    }
    if (sum(accepted_rows, na.rm = TRUE) > 1) {
      if (package_rows$package_id[[1]] %in% manual_editor_text(trusted_manual_only_ids)) {
        # The prior raw table contains the one accepted record for this package.
        # Do not replay multiple stale Sheet renderings of that same record.
        return(package_rows[0, , drop = FALSE])
      }
      stop(
        "Refusing to choose between multiple accepted Sheet-only rows for package ID: ",
        package_rows$package_id[[1]],
        call. = FALSE
      )
    }

    audited_rows <- manual_editor_has_audit(package_rows)
    if (sum(audited_rows, na.rm = TRUE) == 1) {
      return(package_rows[which(audited_rows)[[1]], , drop = FALSE])
    }
    if (package_rows$package_id[[1]] %in% manual_editor_text(trusted_manual_only_ids)) {
      # Let the prior accepted raw row restore this package. The Sheet copies
      # below are stale inactive audit trails, not competing manual edits.
      return(package_rows[0, , drop = FALSE])
    }
    stop(
      "Refusing to choose between multiple audited Sheet-only rows for package ID: ",
      package_rows$package_id[[1]],
      call. = FALSE
    )
  })
  dplyr::bind_rows(selected_rows)
}

# Build the only allowed Sheet-derived inputs. A Sheet row can patch a current
# source package, or it can be an audited manual-only candidate. It can never
# create a normal source row merely because it still exists in the Sheet.
build_source_editor_rows <- function(live_packages, existing_editor, trusted_manual_only_ids = character()) {
  live_ids <- unique(manual_editor_text(live_packages$package_id))
  live_ids <- live_ids[!is.na(live_ids)]

  if (nrow(existing_editor) == 0) {
    return(list(
      source_editor_rows = tibble::tibble(package_id = live_ids),
      audited_sheet_only_rows = existing_editor
    ))
  }

  existing_editor$package_id <- manual_editor_text(existing_editor$package_id)
  source_sheet_rows <- existing_editor[existing_editor$package_id %in% live_ids, , drop = FALSE]

  # The legacy Sheet can contain duplicate generated rows. They are not manual
  # evidence merely because they remain visible. Retain the one audited copy
  # when there is one; otherwise source authority decides the generated row.
  source_groups <- split(source_sheet_rows, source_sheet_rows$package_id)
  selected_source_rows <- lapply(source_groups, function(candidate_rows) {
    if (nrow(candidate_rows) == 1) return(candidate_rows)

    audited_rows <- manual_editor_has_audit(candidate_rows)
    if (sum(audited_rows, na.rm = TRUE) > 1) {
      stop(
        "Refusing to choose between multiple audited Sheet rows for current source package ID: ",
        candidate_rows$package_id[[1]],
        call. = FALSE
      )
    }
    if (sum(audited_rows, na.rm = TRUE) == 1) {
      return(candidate_rows[which(audited_rows)[[1]], , drop = FALSE])
    }

    live_row <- live_packages[live_packages$package_id == candidate_rows$package_id[[1]], , drop = FALSE]
    baseline_rows <- manual_editor_matches_live_baseline(candidate_rows, live_row[1, , drop = FALSE])
    if (any(baseline_rows)) {
      return(candidate_rows[which(baseline_rows)[[1]], , drop = FALSE])
    }

    # An unaudited source-backed row is display state, not saved manual work.
    # If no duplicate matches the current source exactly (for example, a source
    # date changed since the Sheet was last refreshed), keep one deterministically;
    # the source baseline rebuild below replaces its stale generated values.
    candidate_rows[1, , drop = FALSE]
  })
  if (length(selected_source_rows) > 0) {
    source_sheet_rows <- dplyr::bind_rows(selected_source_rows)
  }

  source_editor_rows <- tibble::tibble(package_id = live_ids) %>%
    dplyr::left_join(source_sheet_rows, by = "package_id")

  sheet_only_rows <- existing_editor[!(existing_editor$package_id %in% live_ids), , drop = FALSE]
  if (nrow(sheet_only_rows) == 0) {
    return(list(
      source_editor_rows = source_editor_rows,
      audited_sheet_only_rows = sheet_only_rows
    ))
  }

  has_audit <- manual_editor_has_audit(sheet_only_rows)
  has_trusted_prior <- manual_editor_text(sheet_only_rows$package_id) %in% manual_editor_text(trusted_manual_only_ids)
  stale_leftover <-
    manual_editor_text(sheet_only_rows$manually_edited) %in% "No" &
    manual_editor_text(sheet_only_rows$validation_status) %in% "inactive" &
    manual_editor_text(sheet_only_rows$validation_reason) %in% "not edited"
  untrusted_user_values <- !has_audit & !has_trusted_prior & !stale_leftover & manual_editor_has_values(sheet_only_rows)
  if (any(untrusted_user_values, na.rm = TRUE)) {
    stop(
      "Refusing to clear unaudited Sheet-only package row(s): ",
      paste(sheet_only_rows$package_id[untrusted_user_values], collapse = ", "),
      ". Add audit evidence or restore these rows through the approved manual-only workflow.",
      call. = FALSE
    )
  }

  list(
    source_editor_rows = source_editor_rows,
    audited_sheet_only_rows = collapse_manual_only_sheet_rows(sheet_only_rows, trusted_manual_only_ids)
  )
}

# Planned totals belong to the planned flight; actual metrics belong to their
# delivery override. The caller validates that the selected range is complete.
metric_publish_range <- function(metric_name, planned_start, planned_end, delivery_start, delivery_end) {
  if (metric_name %in% c("planned_spend", "planned_impressions")) {
    return(list(start_date = planned_start, end_date = planned_end))
  }
  list(start_date = delivery_start, end_date = delivery_end)
}

metric_publish_dates <- function(metric_name, planned_start, planned_end, delivery_start, delivery_end) {
  range <- metric_publish_range(
    metric_name,
    planned_start, planned_end,
    delivery_start, delivery_end
  )
  if (is.na(range$start_date) || is.na(range$end_date) || range$end_date < range$start_date) {
    return(as.Date(character()))
  }
  seq(range$start_date, range$end_date, by = "day")
}

# History is a ledger, not a replacement snapshot. Keep one immutable-looking
# record of each accepted loader state before the current raw snapshot is
# deliberately replaced. Database access control is the separate boundary that
# prevents a privileged user from deleting this table.
build_manual_edit_history_rows <- function(raw_upload, recorded_at) {
  history_columns <- c(
    "history_event_id",
    "history_event_type",
    "history_recorded_at",
    "history_loader_run_id",
    "history_source"
  )
  empty_history <- function() {
    tibble::as_tibble(setNames(
      c(rep(list(character()), 2), rep(list(as.POSIXct(character())), 1), rep(list(character()), 2)),
      history_columns
    ))
  }

  if (nrow(raw_upload) == 0) return(empty_history())
  accepted <- raw_upload[
    isTRUE(raw_upload$is_active) | (!is.na(raw_upload$is_active) & raw_upload$is_active),
    , drop = FALSE
  ]
  accepted <- accepted[manual_editor_text(accepted$validation_status) == "valid", , drop = FALSE]
  if (nrow(accepted) == 0) return(empty_history())

  recorded_at <- as.POSIXct(recorded_at, tz = "UTC")
  run_key <- format(recorded_at, "%Y%m%dT%H%M%OS3Z", tz = "UTC")
  safe_edit_id <- gsub("[^A-Za-z0-9_-]", "_", coalesce(manual_editor_text(accepted$edit_id), "no-edit-id"))

  accepted$history_event_id <- paste0("manual-edit-history-", safe_edit_id, "-", run_key, "-", seq_len(nrow(accepted)))
  accepted$history_event_type <- "accepted_snapshot"
  accepted$history_recorded_at <- recorded_at
  accepted$history_loader_run_id <- run_key
  accepted$history_source <- "manual_package_editor_loader"
  accepted[, c(history_columns, names(raw_upload)), drop = FALSE]
}

# Clear through the current grid's final row before a rewrite. Omitting the
# final row from an A1 clear range leaves trailing values from the prior load
# visible below the newly written data.
editor_data_clear_range <- function(tab_name, header_row, last_column, grid_row_count) {
  end_row <- max(as.integer(header_row), as.integer(grid_row_count))
  paste0("'", tab_name, "'!A", header_row, ":", last_column, end_row)
}
