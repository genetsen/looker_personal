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
    str_to_title() %>%                           # Convert to title case
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
    
    if (length(name_columns) == 0) {
      cat("No creative name columns found\n")
      return(NULL)
    }
    
    # Extract unique creative names and create comparison data
    all_names <- c()
    for (col in name_columns) {
      names_in_col <- data[[col]][!is.na(data[[col]]) & data[[col]] != ""]
      all_names <- c(all_names, names_in_col)
    }
    
    # Get unique names and clean them
    unique_names <- unique(all_names)
    unique_names <- unique_names[!is.na(unique_names) & unique_names != ""]
    
    # Create comparison dataframe
    comparison_df <- data.frame(
      file_source = file_id,
      original_name = unique_names,
      cleaned_name = sapply(unique_names, clean_creative_name),
      stringsAsFactors = FALSE
    )
    
    cat("Total unique creative names found:", length(unique_names), "\n\n")
    
    # Display the comparison table
    cat("ORIGINAL vs CLEANED CREATIVE NAMES:\n")
    cat(rep("-", 100), "\n")
    cat(sprintf("%-12s | %-40s | %-40s\n", "FILE SOURCE", "ORIGINAL NAME", "CLEANED NAME"))
    cat(rep("-", 100), "\n")
    
    for (i in 1:nrow(comparison_df)) {
      cat(sprintf("%-12s | %-40s | %-40s\n", 
                  comparison_df$file_source[i],
                  substr(comparison_df$original_name[i], 1, 40),
                  substr(comparison_df$cleaned_name[i], 1, 40)))
    }
    
    return(list(
      file_id = file_id,
      creative_names = unique_names,
      comparison_df = comparison_df,
      count = length(unique_names)
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

cat("\nExtraction completed!\n")
