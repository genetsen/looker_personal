#!/usr/bin/env Rscript
################################################################################
#### LOAD MIQ POLARIS EMAIL DELIVERY
################################################################################
# Purpose:
#   Normalize the current MIQ Polaris Email Meta and TikTok snapshots, apply
#   warehouse-owned package mappings, and atomically replace the validated
#   daily delivery snapshot in BigQuery.
# Safe usage:
#   The loader fails before replacement on ambiguous files, unknown mappings,
#   invalid rows, or duplicate natural keys. Use --dry-run to exercise every
#   read and validation step without creating or changing BigQuery tables.
################################################################################


# * SECTION [1]: CONFIGURATION

  # ? Locate adjacent shared normalization logic
    script_arg <- grep("^--file=", commandArgs(trailingOnly = FALSE), value = TRUE)
    script_path <- if (length(script_arg) == 1) sub("^--file=", "", script_arg) else normalizePath(".")
    script_dir <- dirname(normalizePath(script_path, mustWork = TRUE))
    source(file.path(script_dir, "polaris_fpd_logic.R"))

  # ? Parse the deliberately small manual-run interface
    parse_loader_args <- function(arguments) {
      config <- list(
        project = "looker-studio-pro-452620",
        source_prefix = paste0(
          "gs://bkt-plrs-prd-data-imports-7nqd/data/",
          "client_id=C70545844/connection_id=11694"
        ),
        client_id = "C70545844",
        connection_id = "11694",
        dry_run = FALSE
      )
      index <- 1L
      while (index <= length(arguments)) {
        argument <- arguments[[index]]
        if (argument == "--dry-run") {
          config$dry_run <- TRUE
          index <- index + 1L
        } else if (argument %in% c("--project", "--source-prefix", "--client-id", "--connection-id")) {
          if (index == length(arguments)) stop(paste("Missing value for", argument), call. = FALSE)
          key <- gsub("-", "_", sub("^--", "", argument))
          config[[key]] <- arguments[[index + 1L]]
          index <- index + 2L
        } else {
          stop(paste("Unsupported argument:", argument), call. = FALSE)
        }
      }
      config
    }

  # ? Load only the packages required by this production path
    require_loader_packages <- function() {
      required <- c("bigrquery")
      missing <- required[!vapply(required, requireNamespace, logical(1), quietly = TRUE)]
      if (length(missing) > 0) {
        stop(paste("Install required R package(s):", paste(missing, collapse = ", ")), call. = FALSE)
      }
    }


# * SECTION [2]: SOURCE DISCOVERY AND NORMALIZATION

  # ? Run a local command and retain readable error output
    run_loader_command <- function(command, arguments) {
      output <- suppressWarnings(system2(command, arguments, stdout = TRUE, stderr = TRUE))
      status <- attr(output, "status")
      if (!is.null(status) && status != 0) {
        stop(paste(c(paste("Command failed:", command), output), collapse = "\n"), call. = FALSE)
      }
      output
    }

  # ? Require one and only one current object for each supported feed
    discover_current_objects <- function(source_prefix) {
      objects <- run_loader_command(
        "gcloud",
        c("storage", "ls", shQuote(paste0(sub("/$", "", source_prefix), "/**")))
      )
      objects <- trimws(objects[grepl("\\.csv$", objects, ignore.case = TRUE)])
      feeds <- ifelse(
        grepl("meta", objects, ignore.case = TRUE),
        "meta",
        ifelse(grepl("tiktok", objects, ignore.case = TRUE), "tiktok", "unsupported")
      )
      inventory <- data.frame(source_object_uri = objects, source_feed = feeds, stringsAsFactors = FALSE)
      validate_polaris_source_inventory(inventory)
      inventory
    }

  # ? Copy exact source bytes into an isolated temporary run directory
    normalize_source_inventory <- function(inventory, run_dir) {
      rows <- lapply(seq_len(nrow(inventory)), function(index) {
        feed <- inventory$source_feed[[index]]
        uri <- inventory$source_object_uri[[index]]
        destination <- file.path(run_dir, paste0(feed, "_", basename(uri)))
        run_loader_command("gcloud", c("storage", "cp", shQuote(uri), shQuote(destination)))
        source_data <- read.csv(
          destination,
          check.names = FALSE,
          stringsAsFactors = FALSE,
          fileEncoding = "UTF-8-BOM"
        )
        if (feed == "meta") {
          normalize_polaris_meta(source_data, uri)
        } else {
          normalize_polaris_tiktok(source_data, uri)
        }
      })
      do.call(rbind, rows)
    }


# * SECTION [3]: WAREHOUSE MAPPING AND GATES

  # ? Run a BigQuery statement and wait for its terminal result
    run_bq_statement <- function(project, sql) {
      query_path <- tempfile("polaris_email_statement_", fileext = ".sql")
      on.exit(unlink(query_path), add = TRUE)
      writeLines(sql, query_path)
      output <- suppressWarnings(system2(
        "bq",
        c(
          "query", paste0("--project_id=", project),
          "--use_legacy_sql=false", "--quiet"
        ),
        stdin = query_path,
        stdout = TRUE,
        stderr = TRUE
      ))
      status <- attr(output, "status")
      if (!is.null(status) && status != 0) {
        stop(paste(c("BigQuery statement failed:", output), collapse = "\n"), call. = FALSE)
      }
      invisible(output)
    }

  # ? Run a SELECT and return its downloadable destination table
    run_bq_query <- function(project, sql) {
      bigrquery::bq_project_query(project, sql, use_legacy_sql = FALSE, quiet = TRUE)
    }

  # ? Read active mappings for only this client and connection
    read_active_mappings <- function(config) {
      sql <- sprintf(
        paste0(
          "SELECT source_feed, platform, campaign_name, ad_group_name, package_id, ",
          "package_friendly_label, is_active ",
          "FROM `%s.landing.polaris_email_package_mapping` ",
          "WHERE client_id = '%s' AND connection_id = '%s' AND is_active"
        ),
        config$project,
        gsub("'", "''", config$client_id),
        gsub("'", "''", config$connection_id)
      )
      bigrquery::bq_table_download(run_bq_query(config$project, sql), quiet = TRUE)
    }

  # ? Stop unless every source row is mapped, valid, and naturally unique
    validate_ready_snapshot <- function(rows) {
      problems <- character(0)
      if (nrow(rows) == 0) problems <- c(problems, "no normalized source rows")
      unmapped <- sum(rows$mapping_status != "mapped" | is.na(rows$package_id))
      invalid <- sum(rows$validation_status != "valid")
      duplicated <- sum(rows$natural_row_occurrences > 1)
      if (unmapped > 0) problems <- c(problems, paste(unmapped, "unmapped row(s)"))
      if (invalid > 0) problems <- c(problems, paste(invalid, "invalid row(s)"))
      if (duplicated > 0) problems <- c(problems, paste(duplicated, "row(s) with duplicate natural keys"))
      if (length(problems) > 0) {
        sample_bad <- unique(rows$natural_row_key[
          rows$mapping_status != "mapped" |
            rows$validation_status != "valid" |
            rows$natural_row_occurrences > 1
        ])
        stop(
          paste(
            c("Polaris Email snapshot failed before upload:", problems, head(sample_bad, 10)),
            collapse = "\n- "
          ),
          call. = FALSE
        )
      }
      invisible(TRUE)
    }

  # ? Select and type the exact production table contract
    prepare_delivery_rows <- function(rows, config, loaded_at) {
      output <- data.frame(
        partner = "MIQ",
        ingestion_path = "Polaris Email",
        client_id = config$client_id,
        connection_id = config$connection_id,
        source_object_uri = rows$source_object_uri,
        source_feed = rows$source_feed,
        source_row_number = as.integer(rows$source_row_number),
        platform = rows$platform,
        campaign_name = rows$campaign_name,
        ad_group_name = rows$ad_group_name,
        ad_name = rows$ad_name,
        date = as.Date(rows$date),
        spend = as.numeric(rows$spend),
        impressions = as.integer(rows$impressions),
        clicks = as.integer(rows$clicks),
        video_views = as.integer(rows$video_views),
        video_completions = as.integer(rows$video_completions),
        raw_date = rows$raw_date,
        raw_spend = rows$raw_spend,
        raw_impressions = rows$raw_impressions,
        raw_clicks = rows$raw_clicks,
        raw_video_views = rows$raw_video_views,
        raw_video_completions = rows$raw_video_completions,
        package_id = rows$package_id,
        package_friendly_label = rows$package_friendly_label,
        mapping_status = rows$mapping_status,
        natural_row_key = rows$natural_row_key,
        loaded_at = as.POSIXct(loaded_at, tz = "UTC"),
        stringsAsFactors = FALSE
      )
      output
    }


# * SECTION [4]: GUARDED SNAPSHOT REPLACEMENT

  # ? Stage, validate again in BigQuery, then replace the prior snapshot atomically
    replace_delivery_snapshot <- function(rows, config) {
      suffix <- format(Sys.time(), "%Y%m%d_%H%M%S", tz = "UTC")
      staging_name <- paste0("polaris_email_delivery_daily_staging_", suffix, "_", Sys.getpid())
      staging_id <- paste(config$project, "landing", staging_name, sep = ".")
      production_id <- paste(config$project, "landing", "polaris_email_delivery_daily", sep = ".")

      create_staging <- sprintf(
        paste0(
          "CREATE TABLE `%s` LIKE `%s` OPTIONS (description = ",
          "'Temporary guarded MIQ Polaris Email load candidate. Safe to delete after the load completes; the loader owns cleanup.')"
        ),
        staging_id,
        production_id
      )
      run_bq_statement(config$project, create_staging)
      staging_table <- bigrquery::bq_table(config$project, "landing", staging_name)
      bigrquery::bq_table_upload(
        staging_table,
        rows,
        create_disposition = "CREATE_NEVER",
        write_disposition = "WRITE_APPEND",
        quiet = TRUE
      )

      validation_sql <- sprintf(
        paste0(
          "SELECT COUNT(*) AS row_count, COUNTIF(mapping_status != 'mapped') AS unmapped_rows, ",
          "COUNTIF(package_id IS NULL OR date IS NULL OR platform IS NULL OR campaign_name IS NULL ",
          "OR ad_group_name IS NULL OR ad_name IS NULL) AS invalid_rows, ",
          "COUNT(*) - COUNT(DISTINCT natural_row_key) AS duplicate_rows ",
          "FROM `%s`"
        ),
        staging_id
      )
      gate <- bigrquery::bq_table_download(run_bq_query(config$project, validation_sql), quiet = TRUE)
      if (
        gate$row_count[[1]] != nrow(rows) ||
          gate$unmapped_rows[[1]] != 0 || gate$invalid_rows[[1]] != 0 || gate$duplicate_rows[[1]] != 0
      ) {
        stop(
          paste("Warehouse validation failed; prior production snapshot is unchanged. Staging table retained:", staging_id),
          call. = FALSE
        )
      }

      replacement_sql <- sprintf(
        paste0(
          "BEGIN TRANSACTION; ",
          "DELETE FROM `%s` WHERE TRUE; ",
          "INSERT INTO `%s` SELECT * FROM `%s`; ",
          "COMMIT TRANSACTION;"
        ),
        production_id, production_id, staging_id
      )
      run_bq_statement(config$project, replacement_sql)
      run_bq_statement(config$project, sprintf("DROP TABLE `%s`", staging_id))
      invisible(gate)
    }


# * SECTION [5]: MANUAL ENTRYPOINT

    config <- parse_loader_args(commandArgs(trailingOnly = TRUE))
    require_loader_packages()
    run_dir <- tempfile("polaris_email_load_")
    dir.create(run_dir, recursive = TRUE)
    on.exit(unlink(run_dir, recursive = TRUE, force = TRUE), add = TRUE)

    inventory <- discover_current_objects(config$source_prefix)
    normalized <- normalize_source_inventory(inventory, run_dir)
    mappings <- read_active_mappings(config)
    classified <- apply_polaris_package_mappings(normalized, mappings)
    validate_ready_snapshot(classified)
    loaded_at <- Sys.time()
    delivery <- prepare_delivery_rows(classified, config, loaded_at)

    reconciliation <- build_polaris_reconciliation(classified)
    difference_columns <- grep("_difference$", names(reconciliation), value = TRUE)
    if (!all(vapply(reconciliation[difference_columns], function(value) all(value == 0), logical(1)))) {
      stop("Polaris Email raw-to-normalized reconciliation failed", call. = FALSE)
    }

    if (!config$dry_run) replace_delivery_snapshot(delivery, config)

    package_summary <- aggregate(
      cbind(spend, impressions, clicks, video_views, video_completions) ~ package_id,
      data = delivery,
      FUN = sum,
      na.rm = TRUE
    )
    cat(
      if (config$dry_run) "DRY RUN PASSED\n" else "PRODUCTION SNAPSHOT REPLACED\n",
      "Rows:", nrow(delivery), "\n",
      "Source objects:", nrow(inventory), "\n",
      "Mappings used: ", length(unique(classified$natural_row_key)), " validated source rows across ",
      length(unique(polaris_mapping_key(
        classified$source_feed, classified$platform,
        classified$campaign_name, classified$ad_group_name
      ))), " source keys\n",
      sep = ""
    )
    print(package_summary, row.names = FALSE)
