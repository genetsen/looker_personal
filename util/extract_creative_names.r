# =============================================================================
# EXTRACT AND COMPARE CREATIVE NAMES FROM ALL 3 UTM FILES
# =============================================================================
# Purpose: Show original vs cleaned creative names from all 3 Excel files
# =============================================================================

library(readxl)
library(dplyr)
library(janitor)
library(stringr)
library(DBI)
library(bigrquery)

# Function to clean creative names (same logic as main script)
clean_creative_name <- function(name) {
  if (is.na(name) || name == "") return(name)
  
  cleaned <- name %>%
    str_replace_all("1x1", "") %>%               # Remove "1x1" size indicators
    str_replace_all("0x0", "") %>%               # Remove "0x0" size indicators  
    gsub("(?:_?\\d+x\\d+.*)?$", "", .) %>%       # Remove "NxN" size patterns
    str_to_lower() %>%                            # Convert to lower case
    str_replace_all("[^A-Za-z0-9]", "") %>%      # Remove special characters
    str_replace_all("\\s+", "_")                 # Replace spaces with underscores
  
  return(cleaned)
}

# File configurations
files_config <- data.frame(
  file_id = c("Flight 1", "Flight 2", "Flight 3"),
  excel_file_path = c(
    '/Users/eugenetsenter/Downloads/Flight 1_Trafficking Sheet_MASSMUTUAL003CP_DSP.xlsx',
    '/Users/eugenetsenter/Downloads/Flight 2_Trafficking Sheet_MASSMUTUAL003CP_DSP.xlsx',
    '/Users/eugenetsenter/Downloads/Flight 3 Trafficking Sheet_MASSMUTUAL003CP_DSP.xlsx'
  ),
  sheet_name = c(
    "Q1 Tsheet",
    "Flight 2_Updated",
    "Flight 3"
  ),
  stringsAsFactors = FALSE
)

# Function to extract creative names from a single file
extract_creative_names <- function(file_config) {
  
  file_id <- file_config$file_id
  excel_file_path <- file_config$excel_file_path
  sheet_name <- file_config$sheet_name
  
  cat("\n", rep("=", 60), "\n")
  cat("Processing:", file_id, "\n")
  cat("File:", basename(excel_file_path), "\n")
  cat("Sheet:", sheet_name, "\n")
  cat(rep("=", 60), "\n")
  
  tryCatch({
    
    # Check if file exists
    if (!file.exists(excel_file_path)) {
      cat("ERROR: File not found -", excel_file_path, "\n")
      return(NULL)
    }
    
    # Auto-detect header row position
    temp_data <- read_excel(
      excel_file_path,
      sheet = sheet_name,
      n_max = 20,
      col_names = FALSE
    )
    
    # Find the row that contains "Property" in the first column
    header_row_position <- which(temp_data[[1]] == "Property")
    if(length(header_row_position) == 0) {
      cat("ERROR: Could not find header row with 'Property' in first column\n")
      return(NULL)
    }
    
    # Calculate how many rows to skip
    rows_to_skip <- header_row_position[1] - 1
    cat("Found header at row", header_row_position[1], "- skipping", rows_to_skip, "rows\n")
    
    # Load the data
    data <- read_excel(
      excel_file_path,
      sheet = sheet_name,
      skip = rows_to_skip
    ) %>%
      clean_names() %>%
      slice(-1) %>%
      mutate(across(everything(), as.character))
    
    # Find creative name columns
    name_columns <- grep("creative.*name", names(data), value = TRUE, ignore.case = TRUE)
    cat("Found", length(name_columns), "creative name columns\n")
    
    # Find placement/tag columns
    placement_columns <- grep("^(tag|line_item|placement)$", names(data), value = TRUE, ignore.case = TRUE)
    cat("Found", length(placement_columns), "placement columns:", paste(placement_columns, collapse = ", "), "\n")
    
    if (length(name_columns) == 0) {
      cat("No creative name columns found\n")
      return(NULL)
    }
    
    # Extract creative names with their corresponding placements
    creative_placement_pairs <- data.frame()
    
    # Get all rows with creative names and their placements
    for (i in 1:nrow(data)) {
      row_placements <- c()
      row_names <- c()
      
      # Get placement for this row
      if (length(placement_columns) > 0) {
        for (p_col in placement_columns) {
          if (!is.na(data[i, p_col]) && data[i, p_col] != "") {
            row_placements <- c(row_placements, data[i, p_col])
          }
        }
      }
      
      # Get creative names for this row
      for (n_col in name_columns) {
        if (!is.na(data[i, n_col]) && data[i, n_col] != "") {
          row_names <- c(row_names, data[i, n_col])
        }
      }
      
      # Create pairs for this row
      if (length(row_placements) > 0 && length(row_names) > 0) {
        for (placement in row_placements) {
          for (name in row_names) {
            creative_placement_pairs <- rbind(creative_placement_pairs, data.frame(
              placement = placement,
              original_name = name,
              cleaned_name = clean_creative_name(name),
              utm_key = paste(placement, clean_creative_name(name), sep = " || "),
              stringsAsFactors = FALSE
            ))
          }
        }
      }
    }
    
    # Remove duplicates
    creative_placement_pairs <- unique(creative_placement_pairs)
    
    # Create comparison dataframe
    comparison_df <- data.frame(
      file_source = file_id,
      placement = creative_placement_pairs$placement,
      original_name = creative_placement_pairs$original_name,
      cleaned_name = creative_placement_pairs$cleaned_name,
      utm_key = creative_placement_pairs$utm_key,
      stringsAsFactors = FALSE
    )
    
    cat("Total unique creative-placement pairs found:", nrow(comparison_df), "\n\n")
    
    # Display the comparison table with UTM keys
    cat("PLACEMENT + CREATIVE NAMES + UTM KEYS:\n")
    cat(rep("-", 150), "\n")
    cat(sprintf("%-12s | %-25s | %-30s | %-30s | %-40s\n", "FILE SOURCE", "PLACEMENT", "ORIGINAL NAME", "CLEANED NAME", "UTM KEY"))
    cat(rep("-", 150), "\n")
    
    for (i in 1:min(nrow(comparison_df), 20)) {  # Show first 20 rows
      cat(sprintf("%-12s | %-25s | %-30s | %-30s | %-40s\n", 
                  comparison_df$file_source[i],
                  substr(comparison_df$placement[i], 1, 25),
                  substr(comparison_df$original_name[i], 1, 30),
                  substr(comparison_df$cleaned_name[i], 1, 30),
                  substr(comparison_df$utm_key[i], 1, 40)))
    }
    
    if (nrow(comparison_df) > 20) {
      cat("... (showing first 20 of", nrow(comparison_df), "pairs)\n")
    }
    
    return(list(
      file_id = file_id,
      creative_names = unique(comparison_df$original_name),
      comparison_df = comparison_df,
      count = length(unique(comparison_df$original_name))
    ))
    
  }, error = function(e) {
    cat("ERROR processing", file_id, ":", e$message, "\n")
    return(NULL)
  })
}

# Process all files
cat("EXTRACTING CREATIVE NAMES FROM ALL FILES")
cat("\n", rep("=", 80), "\n")

all_results <- list()

for (i in 1:nrow(files_config)) {
  result <- extract_creative_names(files_config[i, ])
  if (!is.null(result)) {
    all_results[[result$file_id]] <- result
  }
}

# Create consolidated comparison table
cat("\n\n")
cat(rep("=", 120), "\n")
cat("CONSOLIDATED COMPARISON TABLE - ALL FILES")
cat("\n", rep("=", 120), "\n")

# Combine all comparison dataframes
all_comparisons <- data.frame()
for (file_id in names(all_results)) {
  result <- all_results[[file_id]]
  if (!is.null(result$comparison_df)) {
    all_comparisons <- rbind(all_comparisons, result$comparison_df)
  }
}

# Display consolidated table
if (nrow(all_comparisons) > 0) {
  cat(sprintf("%-12s | %-40s | %-40s\n", "FILE SOURCE", "ORIGINAL NAME", "CLEANED NAME"))
  cat(rep("-", 120), "\n")
  
  for (i in 1:nrow(all_comparisons)) {
    cat(sprintf("%-12s | %-40s | %-40s\n", 
                all_comparisons$file_source[i],
                substr(all_comparisons$original_name[i], 1, 40),
                substr(all_comparisons$cleaned_name[i], 1, 40)))
  }
}

# Summary statistics
cat("\n", rep("=", 80), "\n")
cat("SUMMARY STATISTICS")
cat("\n", rep("=", 80), "\n")

total_unique_across_all <- c()

for (file_id in names(all_results)) {
  result <- all_results[[file_id]]
  cat(sprintf("%-10s: %d creative names\n", file_id, result$count))
  total_unique_across_all <- c(total_unique_across_all, result$creative_names)
}

# Overall unique names across all files
overall_unique <- unique(total_unique_across_all)
cat(sprintf("%-10s: %d total unique names across all files\n", "OVERALL", length(overall_unique)))

# Show unique cleaned names
overall_cleaned <- unique(all_comparisons$cleaned_name)
cat(sprintf("%-10s: %d unique cleaned names across all files\n", "CLEANED", length(overall_cleaned)))

# =============================================================================
# PULL DATA FROM BIGQUERY TABLE
# =============================================================================

cat("\n", rep("=", 80), "\n")
cat("PULLING DATA FROM BIGQUERY TABLE")
cat("\n", rep("=", 80), "\n")

# Connect to BigQuery
tryCatch({
  bq_connection <- dbConnect(
    bigrquery::bigquery(),
    project = "looker-studio-pro-452620",
    dataset = "repo_stg",
    billing = "looker-studio-pro-452620"
  )
  
  cat("Connected to BigQuery successfully\n")
  
  # Query to get creative names data
  query <- "
  SELECT DISTINCT
    creative_name,
    cleaned_creative_name,
    del_key,
    placement
  FROM `looker-studio-pro-452620.repo_stg.basis_delivery`
  WHERE creative_name IS NOT NULL 
    AND creative_name != ''
    AND date >= '2025-01-01'  -- Adjust date filter as needed
  ORDER BY creative_name
  "
  
  cat("Executing query to fetch creative names data...\n")
  
  # Execute query
  bq_data <- dbGetQuery(bq_connection, query)
  
  cat("Query completed successfully\n")
  cat("Records retrieved:", nrow(bq_data), "\n\n")
  
  # Display BigQuery data
  cat("BIGQUERY DATA - CREATIVE NAMES:\n")
  cat(rep("-", 120), "\n")
  cat(sprintf("%-40s | %-40s | %-30s\n", "creative_name", "CLEANED_CREATIVE_NAME", "DEL_KEY"))
  cat(rep("-", 120), "\n")
  
  for (i in 1:min(nrow(bq_data), 50)) {  # Show first 50 records
    cat(sprintf("%-40s | %-40s | %-30s\n", 
                substr(bq_data$creative_name[i], 1, 40),
                substr(bq_data$cleaned_creative_name[i], 1, 40),
                substr(bq_data$del_key[i], 1, 30)))
  }
  
  if (nrow(bq_data) > 50) {
    cat("... (showing first 50 of", nrow(bq_data), "records)\n")
  }
  
  # =============================================================================
  # COMPARE EXCEL DATA WITH BIGQUERY DATA
  # =============================================================================
  
  cat("\n", rep("=", 80), "\n")
  cat("COMPARISON: EXCEL vs BIGQUERY DATA")
  cat("\n", rep("=", 80), "\n")
  
  # Get unique creative names from Excel files
  excel_original_names <- unique(all_comparisons$original_name)
  excel_cleaned_names <- unique(all_comparisons$cleaned_name)
  
  # Get unique creative names from BigQuery
  bq_original_names <- unique(bq_data$creative_name)
  bq_cleaned_names <- unique(bq_data$cleaned_creative_name)
  
  # Find matches and differences using CLEANED names
  excel_cleaned_in_bq <- excel_cleaned_names[excel_cleaned_names %in% bq_cleaned_names]
  excel_cleaned_not_in_bq <- excel_cleaned_names[!excel_cleaned_names %in% bq_cleaned_names]
  bq_cleaned_not_in_excel <- bq_cleaned_names[!bq_cleaned_names %in% excel_cleaned_names]
  
  # Summary statistics
  cat("COMPARISON SUMMARY (CLEANED NAMES):\n")
  cat(rep("-", 60), "\n")
  cat(sprintf("Excel files - Total unique original names: %d\n", length(excel_original_names)))
  cat(sprintf("Excel files - Total unique cleaned names:  %d\n", length(excel_cleaned_names)))
  cat(sprintf("BigQuery    - Total unique original names: %d\n", length(bq_original_names)))
  cat(sprintf("BigQuery    - Total unique cleaned names:  %d\n", length(bq_cleaned_names)))
  cat(sprintf("Cleaned names found in both Excel and BigQuery:    %d\n", length(excel_cleaned_in_bq)))
  cat(sprintf("Cleaned names in Excel but NOT in BigQuery:       %d\n", length(excel_cleaned_not_in_bq)))
  cat(sprintf("Cleaned names in BigQuery but NOT in Excel:       %d\n", length(bq_cleaned_not_in_excel)))
  
  # Show cleaned names that are in Excel but not in BigQuery
  if (length(excel_cleaned_not_in_bq) > 0) {
    cat("\nCLEANED NAMES IN EXCEL BUT NOT IN BIGQUERY:\n")
    cat(rep("-", 60), "\n")
    for (i in 1:min(length(excel_cleaned_not_in_bq), 20)) {
      cat(sprintf("%2d. %s\n", i, excel_cleaned_not_in_bq[i]))
    }
    if (length(excel_cleaned_not_in_bq) > 20) {
      cat("... (showing first 20 of", length(excel_cleaned_not_in_bq), "names)\n")
    }
  }
  
  # Show cleaned names that are in BigQuery but not in Excel
  if (length(bq_cleaned_not_in_excel) > 0) {
    cat("\nCLEANED NAMES IN BIGQUERY BUT NOT IN EXCEL:\n")
    cat(rep("-", 60), "\n")
    for (i in 1:min(length(bq_cleaned_not_in_excel), 20)) {
      cat(sprintf("%2d. %s\n", i, bq_cleaned_not_in_excel[i]))
    }
    if (length(bq_cleaned_not_in_excel) > 20) {
      cat("... (showing first 20 of", length(bq_cleaned_not_in_excel), "names)\n")
    }
  }
  
}, error = function(e) {
  cat("ERROR connecting to BigQuery or executing query:", e$message, "\n")
})

cat("\nExtraction and comparison completed!\n")
