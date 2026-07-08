library(googlesheets4)
library(dplyr)

gs4_auth(email = Sys.getenv("MASTER_MANUAL_EDIT_AUTH_EMAIL", "looker-studio-pro-service-account@looker-studio-pro-452620.iam.gserviceaccount.com"))

sheet_id <- "1Sks7-aZqI8B12k-cZ-t9v-aX3Q8jI0J4bL4s6_p7F5M"

data <- read_sheet(sheet_id, range = "'Manual Package Edits'!A4:ZZ", col_types = "c")
ccdooh <- data %>% filter(`Package ID` == "ccdooh")
print(glimpse(ccdooh))
