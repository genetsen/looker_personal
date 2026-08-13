################################################################################
#### LOAD APOLLO DCM CREATIVE IMAGE MAP
################################################################################
# Purpose:
#   Read Apollo's Creative Assignment workbook, publish each available creative
#   file to the shared creative-media repository, and load the exact
#   Asset Name -> normalized DCM creative-name mapping used by data_model_v3.
#
# Inputs:
#   `STEP 1 | INPUT - Creative Details` supplies Asset Name and Final_img_path.
#   The DCM source's `creative` field supplies the matching creative name.
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
TABLE_ID <- Sys.getenv("APO_DCM_CREATIVE_TABLE", "apo_dcm_creative_image_asset_map_qa")
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

# * SECTION [2]: READ AND VALIDATE THE AUTHORITATIVE ASSET MAP

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
    advertiser = "Apollo",
    source_asset_name = str_trim(asset_name),
    normalized_dcm_creative_name = str_to_lower(str_trim(asset_name)),
    creative_type = str_trim(creative_type),
    source_box_url = str_trim(creative_asset_url),
    source_file_path = final_img_path %>%
      str_trim() %>%
      str_remove("^'") %>%
      str_remove("'$") %>%
      gsub(intToUtf8(92), "", ., fixed = TRUE)
  ) %>%
  filter(!is.na(source_asset_name), source_asset_name != "")

conflicting_assets <- asset_rows %>%
  mutate(source_file_path_for_check = na_if(source_file_path, "")) %>%
  group_by(advertiser, normalized_dcm_creative_name) %>%
  summarise(
    distinct_source_path_count = n_distinct(source_file_path_for_check, na.rm = TRUE),
    .groups = "drop"
  ) %>%
  filter(distinct_source_path_count > 1)
if (nrow(conflicting_assets) > 0) {
  stop("Workbook maps an Asset Name to more than one source file; source owner must resolve it before loading.")
}

mapping_rows <- asset_rows %>%
  group_by(advertiser, normalized_dcm_creative_name) %>%
  summarise(
    source_asset_name = first(source_asset_name),
    creative_type = first(creative_type),
    source_box_url = first(source_box_url),
    source_file_path = first(na_if(source_file_path, "")),
    source_asset_record_count = n(),
    .groups = "drop"
  ) %>%
  arrange(normalized_dcm_creative_name)

# * SECTION [3]: PUBLISH AVAILABLE MEDIA AND LOAD THE MAP

mapping_rows <- mapping_rows %>%
  mutate(
    source_file_extension = str_to_lower(str_extract(source_file_path, "\\.[^.]+$")),
    source_file_exists = !is.na(source_file_path) & file.exists(source_file_path),
    is_supported_image = source_file_extension %in% c(".jpg", ".jpeg", ".png", ".gif", ".webp"),
    published_image_url = NA_character_,
    publication_status = case_when(
      is.na(source_file_path) ~ "no_final_img_path",
      !source_file_exists ~ "source_file_not_found",
      !is_supported_image ~ "non_image_source_file",
      TRUE ~ "pending_publish"
    )
  )

if (UPLOAD_ENABLED) {
  media_prefix <- "assets/dcm/apollo"
  unique_assets <- mapping_rows %>%
    filter(publication_status == "pending_publish") %>%
    distinct(source_asset_name, source_file_path)

  for (i in seq_len(nrow(unique_assets))) {
    asset <- unique_assets[i, ]
    file_name <- basename(asset$source_file_path)
    safe_asset <- str_replace_all(tolower(asset$source_asset_name), "[^a-z0-9]+", "-") %>% str_remove("-$")
    relative_media_path <- file.path(media_prefix, safe_asset, file_name)
    destination <- file.path(MEDIA_REPO, relative_media_path)
    dir.create(dirname(destination), recursive = TRUE, showWarnings = FALSE)
    if (!file.exists(destination)) file.copy(asset$source_file_path, destination, overwrite = FALSE)
    mapping_rows$published_image_url[mapping_rows$source_asset_name == asset$source_asset_name] <- paste0(
      "https://raw.githubusercontent.com/genetsen/apo-db-creat/main/", relative_media_path
    )
  }

  git_status <- system2("git", c("-C", MEDIA_REPO, "status", "--short"), stdout = TRUE, stderr = TRUE)
  if (length(git_status) > 0) {
    publish_git <- function(args) {
      exit_code <- system2("git", c("-C", MEDIA_REPO, args))
      if (exit_code != 0) stop("Creative-media publication failed: git ", paste(args, collapse = " "))
    }
    publish_git(c("add", "assets/dcm/apollo"))
    publish_git(c("commit", "-m", shQuote("Publish Apollo DCM creative media")))
    publish_git(c("push", "origin", "main"))
  }
  mapping_rows <- mapping_rows %>%
    mutate(publication_status = if_else(publication_status == "pending_publish", "published", publication_status))

  target <- bq_table(PROJECT_ID, DATASET_ID, TABLE_ID)
  bq_auth(email = AUTH_EMAIL)
  bq_table_upload(target, mapping_rows %>% mutate(loaded_at = loaded_at), write_disposition = "WRITE_TRUNCATE")
  description_sql <- sprintf(
    "ALTER TABLE `%s.%s.%s` SET OPTIONS (description = 'Apollo DCM creative-image map from the Creative Assignment workbook. One row per normalized DCM creative name; V3 joins Apollo DCM creative names after final size-suffix removal. Every Step 1 asset is retained with a publication status; only supported image files receive a published URL.')",
    PROJECT_ID, DATASET_ID, TABLE_ID
  )
  bq_project_query(PROJECT_ID, description_sql, quiet = TRUE)
}

cat("Apollo DCM creative-image asset rows:", nrow(mapping_rows), "\n")
print(mapping_rows %>% count(publication_status, name = "mapping_count"))
