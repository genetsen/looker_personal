

library("gmailr")
library(lubridate)
library(googlesheets4)
library(tidyverse)
library(httpuv)
library(tidyverse)
library(stringr)
library(xfun)
my_threads <- gm_threads(search = "TV | National | daily scheadule",
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
df <- read_csv(temp_file) %>% clean_names() 
str(df)

df <- df %>%
  #fix na values
    mutate(across(everything(), ~na_if(., "NA"))) %>%
  #fix date formats
  mutate(across(where(is.character), ~ str_replace(., "([0-9]{1,2})/([0-9]{1,2})/([0-9]{2,4})", "\\3-\\1-\\2"))) %>%
  mutate(across(where(is.character), ~ as.Date(., format = "%Y-%m-%d")))

gm_save_attachment(att2, filename = temp_file)

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
