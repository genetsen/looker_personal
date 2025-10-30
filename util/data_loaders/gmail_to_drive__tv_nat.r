

library("gmailr")
library(lubridate)
library(googlesheets4)
library(tidyverse)
library(httpuv)
library(tidyverse)
library(stringr)
library(xfun)
my_threads <- gm_threads(search = "TV | National | daily scheadule -Local",
                         num_results = 10)
2
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

print(paste0("latest data date: ", r_date, " --  matches script run date: ", r_date == today()))



#write the attachement to local temp file
temp_file <- tempfile(fileext = paste0("_", att_name))
gm_save_attachment(att2, filename = temp_file)


library(janitor)

numeric_cols <- c(
  "net_cost",
  "net_impressions",
  "total_units"
)

raw_df <- readr::read_csv(
  temp_file,
  guess_max = 10,
  na = c("", "NA", "N/A", "Invalid Number"),
  col_types = readr::cols(.default = readr::col_character())
) %>%
  janitor::clean_names()
str(raw_df)
type_convert(raw_df)
problems(raw_df)
# list the column names seperated by commas for easier reference
colnames(raw_df) %>%  
  paste(collapse = ", ")

if ("month" %in% names(raw_df)) {
  raw_df <- raw_df %>% dplyr::filter(!is.na(month))
}
unique(raw_df$week)
df <- raw_df %>% 
mutate(
  year = as.integer(year),
  quarter = as.character(quarter),
  month = as.integer(month),
  week = as.Date(week, format = "%m/%d/%y"),
  advertiser = as.character(advertiser),
  campaign_name = as.character(campaign_name),
  market = as.character(market),
  mediaoutlet = as.character(mediaoutlet),
  net_cost = total_cost,
  net_impressions = total_impressions_buyers_estimate,
  total_units = total_units,
  type = case_when(
    str_detect(market, "National") ~ "National",
    TRUE ~ "Other"
  )
  ) %>%
  select(
    # all columns: 
    everything(),
    # year,
    # quarter,
    # month,
    # week,
    # advertiser,
    # type,
    # campaign_name,
    # market,
    # mediaoutlet,
    # net_cost,
    # net_impressions,
    # total_units
    ) %>%
  dplyr::mutate(
    dplyr::across(
      dplyr::all_of(numeric_cols),
      ~ readr::parse_number(.x, na = c("", "NA", "N/A", "Invalid Number"))
    
    )
  ) %>% mutate(
    date = as.Date(week, format = "%Y-%m-%d"),
    net_impressions = ifelse(is.na(net_impressions), 0, net_impressions * 1000)

  )
str(df)


# # remove previous bq table
# bq_table <- bq_table(project = "looker-studio-pro-452620", dataset = "landing", table = "tv_national_estimates")
# bq_table_delete(bq_table)


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
  write_to_bq(df, "landing", "tv_national_estimates")

