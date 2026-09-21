# TV impressions contract tests
#
# These tests protect the shared source-file-to-net-impressions rules used by
# the TV Gmail loaders and by the Universal Runner verification checks. If the
# loaders and verifiers disagree here, scheduled runs can look like data-quality
# failures even when the loader used the intended source-file logic.

source("/Users/eugenetsenter/Looker_clonedRepo/looker_personal/util/data_loaders/tv_impressions_contract.R")

assert_equal <- function(actual, expected, label) {
  if (!identical(as.numeric(actual), as.numeric(expected))) {
    stop(
      sprintf(
        "%s: expected %s, got %s",
        label,
        paste(expected, collapse = ", "),
        paste(actual, collapse = ", ")
      ),
      call. = FALSE
    )
  }
}

local_source <- data.frame(
  total_planned_impressions_all_demos = c(1, 2.5, NA_real_),
  total_planned_impressions = c(99, 99, 99)
)

assert_equal(
  f_tv_local_impressions_values(local_source),
  c(1, 2.5, NA_real_),
  "local uses total_planned_impressions_all_demos before older fallback columns"
)

national_source <- data.frame(
  total_planned_impressions_all_demos = c(10, 0, NA_real_, -1),
  total_objective_impressions = c(100, 20, 30, 40)
)

assert_equal(
  f_tv_national_impressions_values(national_source),
  c(10, 20, 30, 40),
  "national uses planned first and objective when planned is missing or not positive"
)

local_date_only_source <- data.frame(
  date = as.Date(c("2026-02-02", "2026-04-15", NA_character_)),
  advertiser = c("AUTD", "GE", NA_character_)
)
local_date_dimensions <- f_tv_calendar_dimensions(local_date_only_source)

stopifnot(
  nrow(local_date_dimensions) == 2L,
  identical(local_date_dimensions$year, c(2026L, 2026L)),
  identical(local_date_dimensions$quarter, c("Q1", "Q2")),
  identical(local_date_dimensions$month, c(2L, 4L))
)

cat("TV impressions contract tests passed\n")
