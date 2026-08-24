################################################################################
#### CREATIVE NAME MAPPING LOGIC
################################################################################
# Purpose:
#   Normalize and validate advertiser creative-name mappings before they reach
#   BigQuery. This file has no network access or side effects and is safe to
#   source from tests and preview workflows.
################################################################################


# * SECTION [1]: NORMALIZATION HELPERS

  # Description: Normalize user-entered values without erasing meaningful punctuation.

  # ? Convert blanks to missing values while preserving the entered display text
    normalize_creative_mapping_text <- function(values) {
      normalized <- trimws(as.character(values))
      normalized[is.na(values) | normalized == ""] <- NA_character_
      normalized
    }

  # ? Build case-insensitive matching keys from the preserved source values
    creative_mapping_key_text <- function(values) {
      tolower(normalize_creative_mapping_text(values))
    }


# * SECTION [2]: MAPPING PREPARATION

  # Description: Produce the one-row-per-match-key contract consumed by V3.

  # ? Validate active mappings and return their canonical BigQuery shape
    prepare_creative_mapping_rows <- function(
      source_rows,
      advertiser,
      source_sheet_id,
      source_sheet_tab,
      default_match_scope = "tactic_creative",
      loaded_at = Sys.time()
    ) {
      required_columns <- c(
        "supplier_code", "initiative", "source_creative_name",
        "mapped_creative_name", "source_row_number"
      )
      missing_columns <- setdiff(required_columns, names(source_rows))
      if (length(missing_columns) > 0L) {
        stop(
          paste("Creative mapping input is missing columns:", paste(missing_columns, collapse = ", ")),
          call. = FALSE
        )
      }

      advertiser_value <- normalize_creative_mapping_text(advertiser)
      if (length(advertiser_value) != 1L || is.na(advertiser_value)) {
        stop("Creative mapping advertiser must contain one nonblank value.", call. = FALSE)
      }

      if ("match_scope" %in% names(source_rows)) {
        match_scope <- creative_mapping_key_text(source_rows$match_scope)
        match_scope[is.na(match_scope)] <- default_match_scope
      } else {
        match_scope <- rep(default_match_scope, nrow(source_rows))
      }

      prepared <- data.frame(
        advertiser = rep(advertiser_value, nrow(source_rows)),
        match_scope = match_scope,
        initiative = normalize_creative_mapping_text(source_rows$initiative),
        source_creative_name = normalize_creative_mapping_text(source_rows$source_creative_name),
        mapped_creative_name = normalize_creative_mapping_text(source_rows$mapped_creative_name),
        supplier_code_context = normalize_creative_mapping_text(source_rows$supplier_code),
        source_sheet_id = rep(source_sheet_id, nrow(source_rows)),
        source_sheet_tab = rep(source_sheet_tab, nrow(source_rows)),
        source_row_number = as.integer(source_rows$source_row_number),
        loaded_at = rep(as.POSIXct(loaded_at, tz = "UTC"), nrow(source_rows)),
        stringsAsFactors = FALSE
      )

      # Blank friendly-name cells are review candidates, not active mappings.
      prepared <- prepared[!is.na(prepared$mapped_creative_name), , drop = FALSE]
      if (nrow(prepared) == 0L) return(prepared)

      supported_scopes <- c("creative", "tactic_creative")
      unsupported_scopes <- setdiff(unique(prepared$match_scope), supported_scopes)
      if (length(unsupported_scopes) > 0L) {
        stop(
          paste("Unsupported creative mapping scope:", paste(unsupported_scopes, collapse = ", ")),
          call. = FALSE
        )
      }

      missing_tactic <- prepared$match_scope == "tactic_creative" & is.na(prepared$initiative)
      if (any(missing_tactic)) {
        stop("tactic_creative mappings require a nonblank initiative.", call. = FALSE)
      }

      missing_creative <- prepared$match_scope == "creative" & is.na(prepared$source_creative_name)
      if (any(missing_creative)) {
        stop("creative mappings require a nonblank source creative name.", call. = FALSE)
      }

      advertiser_key <- creative_mapping_key_text(prepared$advertiser)
      initiative_key <- creative_mapping_key_text(prepared$initiative)
      creative_key <- creative_mapping_key_text(prepared$source_creative_name)
      initiative_key[is.na(initiative_key)] <- "<null>"
      initiative_key[prepared$match_scope == "creative"] <- "<not_applicable>"
      creative_key[is.na(creative_key)] <- "<null>"
      canonical_key <- paste(
        advertiser_key,
        prepared$match_scope,
        initiative_key,
        creative_key,
        sep = "|"
      )
      duplicate_keys <- unique(canonical_key[duplicated(canonical_key) | duplicated(canonical_key, fromLast = TRUE)])
      if (length(duplicate_keys) > 0L) {
        stop(
          paste("Creative mapping input contains duplicate match keys:", paste(duplicate_keys, collapse = ", ")),
          call. = FALSE
        )
      }

      prepared[order(prepared$match_scope, prepared$initiative, prepared$source_creative_name), , drop = FALSE]
    }
