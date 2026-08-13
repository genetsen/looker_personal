################################################################################
#### LOAD APOLLO DCM CREATIVE IMAGE MAP
################################################################################
# Purpose:
#   Read Apollo's Creative Assignment workbook, publish each available creative
#   file to the shared creative-media repository, and load the exact
#   `Ad Name` -> `Creative Assignment` mapping used by data_model_v3.
#
# Inputs:
#   `STEP 1 | INPUT - Creative Details` supplies Asset Name and Final_img_path.
#   `OUTPUT | Adswerve Doc | v1` supplies DCM Ad Name and Creative Assignment.
#
# Safe use:
#   Preview is the default. QA writes require APO_DCM_CREATIVE_UPLOAD=TRUE;
#   production replacement also requires APO_DCM_CREATIVE_ALLOW_PRODUCTION=TRUE.
################################################################################

suppressPackageStartupMessages({
  library(googlesheets4)
  library(googledrive)
  library(bigrquery)
  library(dplyr)
  library(janitor)
  library(stringr)
})

# * SECTION [1]: CONFIGURATION

PROJECT_ID <- Sys.getenv("APO_DCM_CREATIVE_PROJECT", "looker-studio-pro-452620")
DATASET_ID <- Sys.getenv("APO_DCM_CREATIVE_DATASET", "landing")
TABLE_ID <- Sys.getenv("APO_DCM_CREATIVE_TABLE", "apo_dcm_creative_image_map_qa")
SHEET_ID <- Sys.getenv("APO_DCM_CREATIVE_SHEET_ID", "1U46ZJ4U6XCLTXqtNXlun7uY0L1iyG88RZ6HMiOeACyU")
AUTH_EMAIL <- Sys.getenv("APO_DCM_CREATIVE_AUTH_EMAIL", "gene.tsenter@giantspoon.com")
MEDIA_REPO <- Sys.getenv("APO_DCM_CREATIVE_MEDIA_REPO", "")
UPLOAD_ENABLED <- tolower(Sys.getenv("APO_DCM_CREATIVE_UPLOAD", "FALSE")) == "true"
ALLOW_PRODUCTION <- tolower(Sys.getenv("APO_DCM_CREATIVE_ALLOW_PRODUCTION", "FALSE")) == "true"
IS_QA_TABLE <- grepl("_qa$", TABLE_ID)

if (UPLOAD_ENABLED && !IS_QA_TABLE && !ALLOW_PRODUCTION) {
  stop("Production upload blocked. Use a _qa table or explicitly set APO_DCM_CREATIVE_ALLOW_PRODUCTION=TRUE after QA review.")
}
if (UPLOAD_ENABLED && !nzchar(MEDIA_REPO)) {
  stop("Set APO_DCM_CREATIVE_MEDIA_REPO to a clean clone of the shared creative-media repository before publishing files.")
}

# * SECTION [2]: READ AND VALIDATE THE AUTHORITATIVE MAPPING

gs4_auth(email = AUTH_EMAIL)
drive_auth(email = AUTH_EMAIL)
loaded_at <- Sys.time()

asset_rows <- read_sheet(
  SHEET_ID,
  sheet = "STEP 1 | INPUT - Creative Details",
  range = "C2:L",
  col_types = "c",
  .name_repair = "unique"
) %>%
  clean_names() %>%
  transmute(
    creative_assignment_asset = str_trim(asset_name),
    creative_type = str_trim(creative_type),
    source_box_url = str_trim(creative_asset_url),
    source_file_path = final_img_path %>%
      str_trim() %>%
      str_remove("^'") %>%
      str_remove("'$") %>%
      gsub(intToUtf8(92), "", ., fixed = TRUE)
  ) %>%
  filter(!is.na(creative_assignment_asset), creative_assignment_asset != "") %>%
  filter(!is.na(source_file_path), source_file_path != "") %>%
  distinct(creative_assignment_asset, .keep_all = TRUE)

assignment_rows <- read_sheet(
  SHEET_ID,
  sheet = "OUTPUT | Adswerve Doc | v1",
  range = "A1:Q",
  col_types = "c",
  .name_repair = "unique"
) %>%
  clean_names() %>%
  transmute(
    dcm_ad_name = str_trim(ad_name),
    creative_assignment = str_trim(creative_assignment),
    creative_assignment_asset = str_remove(creative_assignment, "_[^_]+$")
  ) %>%
  filter(!is.na(dcm_ad_name), dcm_ad_name != "", !is.na(creative_assignment), creative_assignment != "")

mapping_rows <- assignment_rows %>%
  inner_join(asset_rows, by = "creative_assignment_asset") %>%
  arrange(dcm_ad_name, creative_assignment)

conflicting_ads <- mapping_rows %>%
  distinct(dcm_ad_name, creative_assignment) %>%
  count(dcm_ad_name, name = "creative_assignment_count") %>%
  filter(creative_assignment_count > 1)
if (nrow(conflicting_ads) > 0) {
  stop("Workbook maps at least one DCM Ad Name to more than one Creative Assignment; source owner must resolve it before loading.")
}

# * SECTION [3]: PUBLISH AVAILABLE MEDIA AND LOAD THE MAP

mapping_rows <- mapping_rows %>%
  mutate(
    source_file_exists = file.exists(source_file_path),
    published_image_url = NA_character_,
    publication_status = if_else(source_file_exists, "pending_publish", "source_file_not_found")
  )

if (UPLOAD_ENABLED) {
  media_prefix <- "assets/dcm/apollo"
  unique_assets <- mapping_rows %>%
    filter(source_file_exists) %>%
    distinct(creative_assignment_asset, source_file_path)

  for (i in seq_len(nrow(unique_assets))) {
    asset <- unique_assets[i, ]
    file_name <- basename(asset$source_file_path)
    safe_asset <- str_replace_all(tolower(asset$creative_assignment_asset), "[^a-z0-9]+", "-") %>% str_remove("-$")
    relative_media_path <- file.path(media_prefix, safe_asset, file_name)
    destination <- file.path(MEDIA_REPO, relative_media_path)
    dir.create(dirname(destination), recursive = TRUE, showWarnings = FALSE)
    if (!file.exists(destination)) file.copy(asset$source_file_path, destination, overwrite = FALSE)
    mapping_rows$published_image_url[mapping_rows$creative_assignment_asset == asset$creative_assignment_asset] <- paste0(
      "https://raw.githubusercontent.com/genetsen/apo-db-creat/main/", relative_media_path
    )
  }

  git_status <- system2("git", c("-C", MEDIA_REPO, "status", "--short"), stdout = TRUE, stderr = TRUE)
  if (length(git_status) > 0) {
    system2("git", c("-C", MEDIA_REPO, "add", "assets/dcm/apollo"))
    system2("git", c("-C", MEDIA_REPO, "commit", "-m", "Publish Apollo DCM creative media"))
    system2("git", c("-C", MEDIA_REPO, "push", "origin", "main"))
  }
  mapping_rows <- mapping_rows %>%
    mutate(publication_status = if_else(source_file_exists, "published", publication_status))

  target <- bq_table(PROJECT_ID, DATASET_ID, TABLE_ID)
  bq_auth(email = AUTH_EMAIL)
  bq_table_upload(target, mapping_rows %>% mutate(loaded_at = loaded_at), write_disposition = "WRITE_TRUNCATE")
  description_sql <- sprintf(
    "ALTER TABLE `%s.%s.%s` SET OPTIONS (description = 'Apollo DCM creative-image map from Creative Assignment workbook. Exact one-row-per-Dcm-Ad-Name map used by data_model_v3; source files are published only when available locally.')",
    PROJECT_ID, DATASET_ID, TABLE_ID
  )
  bq_project_query(PROJECT_ID, description_sql, quiet = TRUE)
}

cat("Apollo DCM creative-image mapping rows:", nrow(mapping_rows), "\n")
print(mapping_rows %>% count(publication_status, name = "mapping_count"))
