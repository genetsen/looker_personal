# TV impressions contract
#
# Shared source-file-to-impressions rules for the TV Gmail loaders and the
# Universal Runner verification checks. Source CSVs are expected to be cleaned
# with janitor::clean_names() before these helpers are called. The returned
# values are still in source units; callers apply the loader's x1000 scale.

f_tv_first_present_column <- function(df, candidates) {
  matches <- candidates[candidates %in% names(df)]
  if (!length(matches)) {
    return(NULL)
  }

  matches[[1]]
}

f_tv_numeric_vector <- function(values, n = NULL) {
  if (is.null(values)) {
    if (is.null(n)) {
      n <- 0
    }

    return(rep(NA_real_, n))
  }

  if (is.numeric(values)) {
    return(as.numeric(values))
  }

  suppressWarnings(as.numeric(gsub("[,$%]", "", as.character(values))))
}

f_tv_local_impressions_column <- function(df) {
  f_tv_first_present_column(
    df,
    c(
      "total_total_impressions_buyers_estimate",
      "total_planned_impressions_all_demos_000",
      "total_planned_impressions_all_demos",
      "total_planned_impressions"
    )
  )
}

f_tv_local_impressions_values <- function(df) {
  column_name <- f_tv_local_impressions_column(df)
  if (is.null(column_name)) {
    return(rep(NA_real_, nrow(df)))
  }

  f_tv_numeric_vector(df[[column_name]])
}

f_tv_national_impressions_columns <- function(df) {
  list(
    planned = f_tv_first_present_column(
      df,
      c(
        "total_impressions_buyers_estimate",
        "total_planned_impressions_all_demos",
        "total_planned_impressions_000",
        "total_planned_impressions"
      )
    ),
    objective = f_tv_first_present_column(
      df,
      c(
        "total_objective_impressions",
        "total_objective_impressions_000"
      )
    )
  )
}

f_tv_national_impressions_values <- function(df) {
  columns <- f_tv_national_impressions_columns(df)
  planned <- if (is.null(columns$planned)) {
    rep(NA_real_, nrow(df))
  } else {
    f_tv_numeric_vector(df[[columns$planned]])
  }
  objective <- if (is.null(columns$objective)) {
    rep(NA_real_, nrow(df))
  } else {
    f_tv_numeric_vector(df[[columns$objective]])
  }

  ifelse(
    !is.na(planned) & planned > 0,
    planned,
    ifelse(
      !is.na(objective) & objective > 0,
      objective,
      ifelse(!is.na(planned), planned, objective)
    )
  )
}
