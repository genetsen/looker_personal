################################################################################
#### TEST DXC ADVERTISER MAPPING CONTRACT
################################################################################
# Purpose:
#   Prevent DXC source-name and short-code variants from splitting into separate
#   advertiser groups in the master model.
# Safe usage:
#   This test reads the canonical mapping SQL only. It does not contact BigQuery
#   or modify local or live data.
################################################################################


# * SECTION [1]: LOAD THE CANONICAL MAPPING SQL

  test_file_arg <- commandArgs(trailingOnly = FALSE)
  test_file_arg <- test_file_arg[startsWith(test_file_arg, "--file=")]
  test_file <- normalizePath(sub("^--file=", "", test_file_arg[[1]]))
  sql_file <- normalizePath(file.path(dirname(test_file), "create_advertiser_mapping.sql"))
  sql_text <- paste(readLines(sql_file, warn = FALSE), collapse = "\n")

  expect_true <- function(condition, label) {
    if (!isTRUE(condition)) {
      stop(paste("FAIL:", label), call. = FALSE)
    }
    cat("PASS:", label, "\n")
  }


# * SECTION [2]: LOCK BOTH DXC ALIAS PATHS TO ONE REPORTING LABEL

  dxc_short_mapping <- paste0(
    "STRUCT('advertiser_short_name', 'DXC', 'DXC Technology Services', ",
    "'DXC', 'Approved canonical advertiser.')"
  )
  dxc_name_mapping <- paste0(
    "STRUCT('advertiser_name', 'DXC Technology Services', ",
    "'DXC Technology Services', 'DXC', 'Prisma source name.')"
  )

  expect_true(
    grepl(dxc_short_mapping, sql_text, fixed = TRUE),
    "the DXC short code resolves to the DXC reporting group"
  )
  expect_true(
    grepl(dxc_name_mapping, sql_text, fixed = TRUE),
    "the DXC Technology Services source name resolves to the DXC reporting group"
  )

  dxc_standardized_rows <- gregexpr(
    "'DXC', '[^']*'\\)",
    sql_text,
    perl = TRUE
  )[[1]]

  expect_true(
    length(dxc_standardized_rows[dxc_standardized_rows > 0]) >= 2,
    "the mapping contains both required DXC aliases"
  )

cat("All DXC advertiser mapping contract tests passed.\n")
