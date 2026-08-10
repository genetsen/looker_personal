################################################################################
#### TEST APOLLO LINKEDIN ADVERTISER NORMALIZATION CONTRACT
################################################################################
# Purpose:
#   Prevent the stable-base SQL from losing the LinkedIn-only Apollo Corporate
#   advertiser normalization while preserving the original source account.
# Safe usage:
#   This test reads the canonical SQL file only. It does not contact BigQuery or
#   modify any local or live data.
################################################################################


# * SECTION [1]: LOAD THE CANONICAL SQL

  test_file_arg <- commandArgs(trailingOnly = FALSE)
  test_file_arg <- test_file_arg[startsWith(test_file_arg, "--file=")]
  test_file <- normalizePath(sub("^--file=", "", test_file_arg[[1]]))
  sql_file <- normalizePath(file.path(dirname(test_file), "..", "create_master_stg_data_model.sql"))
  sql_text <- paste(readLines(sql_file, warn = FALSE), collapse = "\n")

  expect_true <- function(condition, label) {
    if (!isTRUE(condition)) {
      stop(paste("FAIL:", label), call. = FALSE)
    }
    cat("PASS:", label, "\n")
  }


# * SECTION [2]: LOCK THE NORMALIZATION SCOPE AND PRECEDENCE

  platform_guard <- "LOWER(TRIM(wi.social_platform)) IN ('linkedin', 'linkedin_ads')"
  account_guard <- "UPPER(TRIM(wi.advertiser_name)) = 'APOLLO CORPORATE'"
  canonical_result <- "THEN 'Apollo'"

  expect_true(
    grepl(platform_guard, sql_text, fixed = TRUE),
    "Apollo Corporate normalization remains limited to LinkedIn sources"
  )
  expect_true(
    grepl(account_guard, sql_text, fixed = TRUE),
    "Apollo Corporate matching remains case-insensitive and whitespace-safe"
  )
  expect_true(
    grepl(canonical_result, sql_text, fixed = TRUE),
    "matching LinkedIn rows retain the canonical Apollo advertiser"
  )

  mapping_position <- regexpr("name_map.standardized_advertiser_name", sql_text, fixed = TRUE)[[1]]
  apollo_position <- regexpr(account_guard, sql_text, fixed = TRUE)[[1]]
  legal_suffix_position <- regexpr("NULLIF(TRIM(REGEXP_REPLACE(", sql_text, fixed = TRUE)[[1]]

  expect_true(
    mapping_position < apollo_position && apollo_position < legal_suffix_position,
    "mapping-table matches still win and Apollo normalization precedes the generic fallback"
  )


# * SECTION [3]: LOCK SOURCE-ACCOUNT LINEAGE

  expect_true(
    grepl("social_account_name AS `s_account_name`", sql_text, fixed = TRUE),
    "the original social account remains published as s_account_name"
  )

cat("All Apollo advertiser normalization contract tests passed.\n")
