
# Gmail to Drive TV Local Data Loader Script ###
#
# What changed: this loader now looks for the newer local impressions field
# `total_planned_impressions_all_demos_000` before falling back to older names.
# How to undo: restore the previous Git version if the incoming local CSV format
# returns to the older impressions column layout.
#
# ---------------------------------------------------------------------------- #
#                                    HEADER                                    #
# ---------------------------------------------------------------------------- #
# clean environment ----
    # rm(list = ls())
# load .rprofile
    # source("/Users/eugenetsenter/.Rprofile")

# load libraries ----
  library("gmailr")
  library(lubridate)
  library(googlesheets4)
  library(tidyverse)
  library(stringr)
# ---------------------------------------------------------------------------- #
#                              Configuration                                   #
# ---------------------------------------------------------------------------- #
# ------------------------ search criteria for gmailr ------------------------ #
search_criteria <- 'subject:"TV | Local | daily scheadule" -National'
 
# ------------------------------- BQ table info ------------------------------ #
project_id <- "looker-studio-pro-452620"
dataset <- "landing"
table <- "tv_local_estimates"

# --------------------------- shared helper loading -------------------------- #
current_script_path <- local({
  frames <- sys.frames()
  for (i in rev(seq_along(frames))) {
    if (!is.null(frames[[i]]$ofile)) {
      return(normalizePath(
        frames[[i]]$ofile,
        winslash = "/",
        mustWork = FALSE
      ))
    }
  }

  if (requireNamespace("rstudioapi", quietly = TRUE) &&
      rstudioapi::isAvailable()) {
    context <- rstudioapi::getActiveDocumentContext()
    if (!is.null(context$path) && context$path != "") {
      return(normalizePath(
        context$path,
        winslash = "/",
        mustWork = FALSE
      ))
    }
  }

  args <- commandArgs(trailingOnly = FALSE)
  file_arg <- grep("^--file=", args, value = TRUE)
  if (length(file_arg) > 0) {
    return(normalizePath(
      sub("^--file=", "", file_arg),
      winslash = "/",
      mustWork = FALSE
    ))
  }

  "unknown_script"
})

shared_helper_path <- local({
  helper_name <- "bq_write_with_email_alerts.r"
  candidate_paths <- unique(c(
    file.path(dirname(current_script_path), "..", "R_functions", helper_name),
    file.path(getwd(), "..", "R_functions", helper_name),
    file.path(getwd(), "util", "R_functions", helper_name),
    file.path(getwd(), "R_functions", helper_name),
    file.path(getwd(), "shared_bq_write_with_alerts.r"),
    file.path(getwd(), "util", "data_loaders", "shared_bq_write_with_alerts.r")
  ))
  helper_match <- candidate_paths[file.exists(candidate_paths)][1]

  if (is.na(helper_match)) {
    stop(
      "Shared BigQuery alert helper not found. Checked: ",
      paste(candidate_paths, collapse = ", ")
    )
  }

  normalizePath(helper_match, winslash = "/", mustWork = TRUE)
})

source(shared_helper_path)


# ---------------------------------------------------------------------------- #
#                                   Main Code                                  #
# ---------------------------------------------------------------------------- #

# ------------------------------ pull gmail data ----------------------------- #

  #retrieve threads matching search --------------------- #
  my_threads <- gm_threads(search = search_criteria,
                          num_results = 10)



  # my_threads <- gm_threads(search = 'subject:"TV | Local | daily scheadule" -National',
  #                          num_results = 10)
  my_threads


  # retrieve the latest thread by retrieving the first ID
  latest_thread <- gm_thread(gm_id(my_threads)[[1]])
  # messages in the thread will now be in a list

  # retrieve parts of a specific message
  (my_msg <- latest_thread$messages[[1]])
  (msg_id <- (my_msg[[1]]))

  # retrieve attachments
  (att <- gm_attachments(my_msg) )
  typeof(att)
  arrange(att)
  (att_id <- att[[1,4]])
  (att_name <- att[[1,1]])

  ## 2nd attachment

  # (att_id_2 <- att[[3,4]])
  # (att_name_2 <- att[[3,1]])

  att2 <- gm_attachment(att_id, msg_id)

  #Test Date of Email vs today
  (r_date <- str_sub(gm_date(my_msg), 6, 16))
  (r_date <- as.Date(r_date, format = "%d %b %Y"))

  today()
  #log to console

  cat(paste0("latest data date: ", r_date, " --  matches script run date: ", r_date == today()))



  # ----------------- write the attachement to local temp file ----------------- #
  temp_file <- tempfile(fileext = paste0("_", att_name))
  gm_save_attachment(att2, filename = temp_file)


  library(janitor)

#
# ----------------------------- load & clean data ---------------------------- #
  numeric_cols <- c(
    "net_cost",
    "net_impressions",
    "total_units"
  )

  raw_df <- readr::read_csv(
    temp_file,
    guess_max = 100,
    na = c("", "NA", "N/A", "Invalid Number"),
    #col_types = readr::cols('Weeks in WEEK_BEGIN_DATE' = readr::col_date(), 'Date' = readr::col_date())
    "%m/%d/%y"
    #col_types = readr::cols(.default = readr::col_character())
    ) %>%
    janitor::clean_names()
  str(raw_df)

  raw_df <- readr::read_csv(
    temp_file,
    guess_max = 100,
    na = c("", "NA", "N/A", "Invalid Number"),
    col_types = readr::cols(
      Date = readr::col_date(format = "%m/%d/%y")
    )
  ) %>%
    janitor::clean_names()
  str(raw_df)
  glimpse(raw_df)
  #type_convert(raw_df)
  # list the column names seperated by commas for easier reference
  colnames(raw_df) %>%  
    paste(collapse = ", ")

  if ("month" %in% names(raw_df)) {
    raw_df <- raw_df %>% dplyr::filter(!is.na(month))
  }
  unique(raw_df$date)
  str(raw_df)
  #lubridate::as_date(raw_df$date, format = "%m-%d-%Y")
  # Fix: After janitor::clean_names(), check for the actual column name
  # Check which column name exists before mutate
  # Map old column names to new column names after clean_names()
  net_cost_col <- if("total_net_cost" %in% names(raw_df)) {
    "total_net_cost"
  } else if("total_net" %in% names(raw_df)) {
    "total_net"
  } else if("total_planned_net" %in% names(raw_df)) {
    "total_planned_net"
  } else {
    NULL
  }
  
  # Check which impressions column exists
  impressions_col <- if("total_total_impressions_buyers_estimate" %in% names(raw_df)) {
    "total_total_impressions_buyers_estimate"
  } else if("total_planned_impressions_all_demos_000" %in% names(raw_df)) {
    "total_planned_impressions_all_demos_000"
  } else if("total_planned_impressions" %in% names(raw_df)) {
    "total_planned_impressions"
  } else {
    NULL
  }
  
  # Check which units column exists
  units_col <- if("total_units" %in% names(raw_df)) {
    "total_units"
  } else if("total_planned_spots" %in% names(raw_df)) {
    "total_planned_spots"
  } else {
    NULL
  }
  
  # If column not found, print available columns for debugging
  if(is.null(net_cost_col)) {
    cat("Warning: total_net_cost column not found. Available columns:", paste(names(raw_df), collapse = ", "), "\n")
  }
  
  df <- raw_df %>% mutate(
    year = as.integer(year),
    quarter = as.character(quarter),
    month = as.integer(month),
    week = (weeks_in_week_begin_date),
    #date = as.Date(Date),
    advertiser = as.character(advertiser),
    campaign_name = as.character(campaign_name),
    market = as.character(market),
    media_outlet = as.character(media_outlet),
    # Use the correct column name that was found above
    net_cost = if(!is.null(net_cost_col)) {
      .data[[net_cost_col]]
    } else {
      NA_real_
    },
    # Fix: Use the correct impressions column name
    net_impressions = if(!is.null(impressions_col)) {
      .data[[impressions_col]]
    } else {
      NA_real_
    },
    # Fix: Use the correct units column name
    total_units = if(!is.null(units_col)) {
      .data[[units_col]]
    } else {
      NA_real_
    }) %>%
    mutate(
      type = "Local"
      ) %>%
    select(
      year,
      quarter,
      month,
      #week,
      date,
      advertiser,
      type,
      campaign_name,
      program_name,
      market,
      media_outlet,
      net_cost,
      net_impressions,
      total_units
    ) %>%
    # dplyr::mutate(
    #   dplyr::across(
    #     dplyr::all_of(numeric_cols),
    #     ~ readr::parse_number(.x, na = c("", "NA", "N/A", "Invalid Number"))
    #   )
    # )  
    mutate(
      net_impressions = ifelse(is.na(net_impressions), 0, net_impressions * 1000)
    )
#
  # ---------------------------------------------------------------------------- #
  #                              write to big query                              #
  # ---------------------------------------------------------------------------- #
    #### write to BQ with shared schema retry and email alerts ####
    write_to_bq_with_email_alerts(
      data = df,
      project_id = project_id,
      dataset = dataset,
      table = table,
      loader_name = "TV Local data loader script",
      script_path = current_script_path
    )
    #
    #
    #
    #



#
#
#
# --------------------------------------END----------------------------------- #
# ---------------------------------------------------------------------------- #
