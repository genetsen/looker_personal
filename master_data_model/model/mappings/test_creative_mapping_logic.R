################################################################################
#### TEST CREATIVE NAME MAPPING LOGIC
################################################################################
# Purpose:
#   Prove active-row filtering, blank-creative handling, supported match scopes,
#   and duplicate-key rejection using in-memory fixtures only.
# Safe usage:
#   This test does not access Google Sheets or BigQuery and writes no files.
################################################################################

test_argument <- grep("^--file=", commandArgs(trailingOnly = FALSE), value = TRUE)
test_path <- sub("^--file=", "", test_argument[[1L]])
source(file.path(dirname(normalizePath(test_path)), "creative_mapping_logic.R"))


# * SECTION [1]: TEST HELPERS

  # Description: Keep each contract check readable without adding a test framework.

  # ? Stop with the behavior label when an expectation fails
    expect_true <- function(condition, label) {
      if (!isTRUE(condition)) stop(paste("FAIL:", label), call. = FALSE)
      cat("PASS:", label, "\n")
    }

  # ? Capture a validation error for one intentionally invalid fixture
    capture_error <- function(expression) {
      tryCatch(
        {
          force(expression)
          NULL
        },
        error = function(error) conditionMessage(error)
      )
    }


# * SECTION [2]: VALID MAPPINGS

  # Description: Prove the PE adapter keeps active rows and preserves a truly blank creative key.

  valid_fixture <- data.frame(
    supplier_code = c("QUAN", "MIQ", "MIQ"),
    initiative = c("Columbus Circle DOOH", "FacebookAwareness", "Instagram"),
    source_creative_name = c(NA, "Protein Product Madness :15s", "Unused Creative"),
    mapped_creative_name = c("Protein Maddness", "Protein Madness 15", ""),
    source_row_number = c(20L, 27L, 32L),
    stringsAsFactors = FALSE
  )
  valid_rows <- prepare_creative_mapping_rows(
    valid_fixture,
    advertiser = "Purely Elizabeth",
    source_sheet_id = "fixture-sheet",
    source_sheet_tab = "Creative Mapping",
    loaded_at = as.POSIXct("2026-08-24 12:00:00", tz = "UTC")
  )

  expect_true(
    nrow(valid_rows) == 2L &&
      all(valid_rows$match_scope == "tactic_creative") &&
      any(is.na(valid_rows$source_creative_name)) &&
      !"Unused Creative" %in% valid_rows$source_creative_name,
    "PE rows use tactic_creative, retain a blank source creative, and ignore blank friendly names"
  )

  creative_scope_fixture <- valid_fixture[2L, , drop = FALSE]
  creative_scope_fixture$match_scope <- "creative"
  creative_scope_rows <- prepare_creative_mapping_rows(
    creative_scope_fixture,
    advertiser = "Purely Elizabeth",
    source_sheet_id = "fixture-sheet",
    source_sheet_tab = "Creative Mapping"
  )
  expect_true(
    creative_scope_rows$match_scope[[1L]] == "creative" &&
      creative_scope_rows$initiative[[1L]] == "FacebookAwareness",
    "creative scope remains available while supplier and initiative stay audit context"
  )


# * SECTION [3]: FAIL-CLOSED VALIDATION

  # Description: Prove ambiguous or structurally unsafe mapping rows cannot be loaded.

  duplicate_fixture <- rbind(valid_fixture[2L, ], valid_fixture[2L, ])
  duplicate_fixture$mapped_creative_name <- c("Name A", "Name B")
  duplicate_error <- capture_error(prepare_creative_mapping_rows(
    duplicate_fixture,
    advertiser = "Purely Elizabeth",
    source_sheet_id = "fixture-sheet",
    source_sheet_tab = "Creative Mapping"
  ))
  expect_true(
    !is.null(duplicate_error) && grepl("duplicate match keys", duplicate_error),
    "duplicate canonical match keys stop before loading"
  )

  duplicate_creative_scope_fixture <- rbind(valid_fixture[2L, ], valid_fixture[2L, ])
  duplicate_creative_scope_fixture$match_scope <- "creative"
  duplicate_creative_scope_fixture$initiative <- c("FacebookAwareness", "Instagram")
  duplicate_creative_scope_error <- capture_error(prepare_creative_mapping_rows(
    duplicate_creative_scope_fixture,
    advertiser = "Purely Elizabeth",
    source_sheet_id = "fixture-sheet",
    source_sheet_tab = "Creative Mapping"
  ))
  expect_true(
    !is.null(duplicate_creative_scope_error) && grepl("duplicate match keys", duplicate_creative_scope_error),
    "creative-wide duplicate detection ignores tactic review context"
  )

  missing_tactic_fixture <- valid_fixture[2L, , drop = FALSE]
  missing_tactic_fixture$initiative <- NA_character_
  missing_tactic_error <- capture_error(prepare_creative_mapping_rows(
    missing_tactic_fixture,
    advertiser = "Purely Elizabeth",
    source_sheet_id = "fixture-sheet",
    source_sheet_tab = "Creative Mapping"
  ))
  expect_true(
    !is.null(missing_tactic_error) && grepl("require a nonblank initiative", missing_tactic_error),
    "tactic_creative scope requires a tactic"
  )

  missing_creative_fixture <- valid_fixture[1L, , drop = FALSE]
  missing_creative_fixture$match_scope <- "creative"
  missing_creative_error <- capture_error(prepare_creative_mapping_rows(
    missing_creative_fixture,
    advertiser = "Purely Elizabeth",
    source_sheet_id = "fixture-sheet",
    source_sheet_tab = "Creative Mapping"
  ))
  expect_true(
    !is.null(missing_creative_error) && grepl("require a nonblank source creative", missing_creative_error),
    "creative scope cannot turn a blank creative into a wildcard"
  )

  cat("All creative mapping logic tests passed.\n")
