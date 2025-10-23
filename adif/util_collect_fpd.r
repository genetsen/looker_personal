#### Header ####
  library(googledrive)
  library(dplyr)
  library(stringr)
  library(lubridate)
  library(googlesheets4)
  library(janitor)



#### Configuration ####
  gdrive_folder_id <- "1EyN93JE7v4OXjMMQREVuZ4ZN7xEed5WB"
  pattern <- "De Beers | Partner Data"
  range <- "e61:Q"
  dataset <- "landing"
  table <- "adif_fpd_data"
#

#### Main Code - 1 - Combine all google sheets into one DF ####
  #go to the folder in Google Drive
    gdrive_folder <- drive_get(as_id(gdrive_folder_id))
    2
  #

  # Find all files in the gDrive folder and subfolders

    gdrive_files <- drive_ls(
      as_id(gdrive_folder_id),
      pattern = pattern,
      recursive = TRUE,
      type = "spreadsheet",
      n_max = Inf
    )
  #

  # Import and clean data from Google Sheets
    # Description:
      # •	Reads all Google Sheets listed in gdrive_files (each with an id and name).
      # •	Adds a source_file column to track origin.
      # •	Skips empty or NA-only sheets.
      # •	Detects and converts date-like columns to proper Date objects.
      # •	Returns a named list of cleaned data frames, one per sheet.
    #

    # Create a named list of data frames by reading multiple Google Sheets
      gdrive_dfs <- setNames(
      # Iterate over each row in gdrive_files (each row represents a file)
        lapply(seq_len(nrow(gdrive_files)), function(i) {
      #
      # Extract the file name for labeling later
          name <- gdrive_files$name[i]
        
      # Read the Google Sheet by its file ID
        # 'range' defines the cell range to read (can be NULL for entire sheet)
        # col_types = NULL lets read_sheet guess data types
        # col_names = TRUE means the first row is used as column names
        df <- read_sheet(
          gdrive_files$id[i],
          range = range,
          col_types = NULL,
          col_names = TRUE
        ) %>%
      # Add a column indicating which file the data came from
          mutate(source_file = name)
        
      # Skip completely empty or all-NA sheets
        if (nrow(df) == 0 || all(is.na(df))) {
          return(NULL)
        }
        
      # Identify any columns whose names contain "date" (case-insensitive)
        date_cols <- grep("date", names(df), ignore.case = TRUE, value = TRUE)
        
      # Convert each detected date column from character to Date format
        for (col in date_cols) {
          df[[col]] <- as.Date(df[[col]], format = "%m/%d/%Y")
          # Adjust format string above if your sheets use another date format
        }
        
      # Return the cleaned data frame for this sheet
        return(df)
      }),
      
      # Name each element in the output list with the file name
        gdrive_files$name
    )

  #

  # See which files are empty
  names(gdrive_dfs)[sapply(gdrive_dfs, function(x) is.null(x) || nrow(x) == 0)]


  # Remove NULL elements (blank sheets)
  gdrive_dfs <- gdrive_dfs[!sapply(gdrive_dfs, is.null)]

  # Combine all data frames into one
  gdrive_data <- bind_rows(gdrive_dfs, .id = "source_file")

  # Remove unwanted columns (e.g., those starting with "...")
  gdrive_data <- gdrive_data %>%
    select(-starts_with("...")) %>%
    clean_names() %>%
    rename(package_name = package_placement_please_use_gs_placement_package_names_see_col_y,
           week = week_first_sunday_of_week)

  colnames(gdrive_data)
#
#### Main Code - 2 - fix dates ####
str(gdrive_data)
# remove rows with NA in date columns
gdrive_data_filtered <- gdrive_data %>%
  filter(!(is.na(start_date) & is.na(end_date) & is.na(week)))
# expand date ranges into individual dates
ranged_data <- gdrive_data_filtered %>%
  rowwise() %>%
  mutate(
    start_date = case_when(
      !is.na(start_date) ~ (start_date),
      !is.na(week) ~ (week),
      #!is.na(date) ~ (date),
    ),
    end_date = case_when(
      !is.na(end_date) ~ (end_date),
      !is.na(week) ~ week + 6,
      #!is.na(date) ~ (date),
    ),
    date_final = case_when(
      #!is.na(date) ~ (date),
      !is.na(start_date) & !is.na(end_date) ~ list(seq(start_date, end_date, by = "day")),
    )) %>%
  unnest(date_final) %>%
  group_by(start_date,end_date,week) %>%
  mutate(
    days_in_range = as.numeric(end_date - start_date + 1),
    spend = spend / days_in_range,
    impressions = impressions / days_in_range,
    clicks = clicks / days_in_range,
    sends = sends / days_in_range,
    opens = opens / days_in_range,
    pageviews = pageviews / days_in_range,
    views = views / days_in_range,
    completed_views = completed_views / days_in_range
  ) %>%
  ungroup()


#### write to BQ --using write_to_bq  ####
  write_to_bq <- function(data, dataset, table) {
    library(bigrquery)
    
    # Define the BigQuery project and dataset
    project_id <- "looker-studio-pro-452620"
    
    # Write the data to BigQuery
    bq_table <- bq_table(project = project_id, dataset = dataset, table = table)
    
    # Use bq_perform_upload to write the data
    bq_perform_upload(bq_table, data, write_disposition = "WRITE_TRUNCATE")
    
    cat("Data written to BigQuery table:", table, "\n")
  }
  write_to_bq(gdrive_data, dataset, table)
