#!/usr/bin/env Rscript
################################################################################
#### LOAD MIQ POLARIS EMAIL DELIVERY
################################################################################
# Purpose:
#   Find newly arrived MIQ Polaris Email objects from Cloud Storage metadata,
#   normalize every object added after the last successful feed checkpoint, apply
#   warehouse-owned package mappings, and atomically upsert those source rows.
# Safe usage:
#   The loader classifies new files from header bytes, never path names. It
#   fails before upsert on ambiguous files, backward source dates, unknown
#   mappings, invalid rows, or duplicate natural keys. Use --dry-run to exercise
#   every read and validation step without writes.
################################################################################


# * SECTION [1]: CONFIGURATION

  # ? Locate adjacent shared normalization logic
    script_arg <- grep("^--file=", commandArgs(trailingOnly = FALSE), value = TRUE)
    command_path <- if (length(script_arg) == 1L) sub("^--file=", "", script_arg) else NA_character_
    source_path <- tryCatch(sys.frame(1)$ofile, error = function(error) NULL)
    script_path <- if (!is.na(command_path) && file.exists(command_path)) {
      command_path
    } else if (!is.null(source_path) && file.exists(source_path)) {
      source_path
    } else {
      normalizePath(".")
    }
    script_dir <- dirname(normalizePath(script_path, mustWork = TRUE))
    source(file.path(script_dir, "polaris_email_delivery_logic.R"))

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
        state_table = "polaris_email_source_state",
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
      required <- c("bigrquery", "jsonlite")
      missing <- required[!vapply(required, requireNamespace, logical(1), quietly = TRUE)]
      if (length(missing) > 0) {
        stop(paste("Install required R package(s):", paste(missing, collapse = ", ")), call. = FALSE)
      }
    }

  # ? Name and time one BigQuery boundary without exposing SQL or credentials
    run_polaris_bq_stage <- function(stage, operation) {
      started_at <- proc.time()[["elapsed"]]
      cat("POLARIS_BQ_STAGE|stage=", stage, "|status=started\n", sep = "")
      tryCatch({
        result <- operation()
        elapsed_seconds <- proc.time()[["elapsed"]] - started_at
        cat(
          "POLARIS_BQ_STAGE|stage=", stage,
          "|status=completed|elapsed_seconds=", sprintf("%.2f", elapsed_seconds),
          "\n",
          sep = ""
        )
        result
      }, error = function(error) {
        elapsed_seconds <- proc.time()[["elapsed"]] - started_at
        error_text <- gsub("[\r\n|]+", " ", conditionMessage(error))
        stop(
          "POLARIS_BQ_FAILURE|stage=", stage,
          "|elapsed_seconds=", sprintf("%.2f", elapsed_seconds),
          "|error=", error_text,
          call. = FALSE
        )
      })
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

  # ? Parse Cloud Storage timestamps returned by gcloud JSON
    parse_storage_time <- function(value) {
      parsed <- vapply(as.character(value), function(item) {
        fraction_text <- regmatches(item, regexpr("\\.[0-9]+", item))
        fraction <- if (length(fraction_text) == 0L) 0 else as.numeric(paste0("0", fraction_text))
        whole_seconds <- sub("\\.[0-9]+", "", item)
        normalized <- sub("([+-][0-9]{2}):([0-9]{2})$", "\\1\\2", whole_seconds)
        as.numeric(as.POSIXct(
          normalized,
          format = "%Y-%m-%dT%H:%M:%S%z",
          tz = "UTC"
        )) + fraction
      }, numeric(1))
      as.POSIXct(parsed, origin = "1970-01-01", tz = "UTC")
    }

  # ? List object metadata without downloading CSV contents
    list_source_object_metadata <- function(source_prefix) {
      output <- run_loader_command(
        "gcloud",
        c(
          "storage", "ls", "--recursive", "--json",
          shQuote(paste0(sub("/$", "", source_prefix), "/**"))
        )
      )
      parsed <- jsonlite::fromJSON(paste(output, collapse = "\n"))
      if (nrow(parsed) == 0L) {
        return(data.frame(
          source_object_uri = character(0), source_object_generation = character(0),
          object_created_at = as.POSIXct(character(0), tz = "UTC"),
          object_size_bytes = numeric(0),
          stringsAsFactors = FALSE
        ))
      }
      metadata <- parsed$metadata
      inventory <- data.frame(
        source_object_uri = paste0("gs://", metadata$bucket, "/", metadata$name),
        source_object_generation = as.character(metadata$generation),
        object_created_at = parse_storage_time(metadata$timeCreated),
        object_size_bytes = as.numeric(metadata$size),
        stringsAsFactors = FALSE
      )
      inventory[grepl("\\.csv$", inventory$source_object_uri, ignore.case = TRUE), , drop = FALSE]
    }

  # ? Read only the first object bytes needed to classify its CSV header
    read_source_object_header <- function(
      source_object_uri,
      source_object_generation,
      object_size_bytes
    ) {
      versioned_uri <- paste0(source_object_uri, "#", source_object_generation)
      range_end <- max(0, min(65535, as.numeric(object_size_bytes) - 1))
      output <- run_loader_command(
        "gcloud",
        c("storage", "cat", shQuote(versioned_uri), paste0("--range=0-", range_end))
      )
      if (length(output) == 0L) stop("Polaris source object has no readable header", call. = FALSE)
      read.csv(
        text = paste0(output[[1]], "\n"),
        nrows = 0,
        check.names = FALSE,
        stringsAsFactors = FALSE,
        fileEncoding = "UTF-8-BOM"
      )
    }

  # ? Limit header reads to objects that could be newer than either feed checkpoint
    prefilter_source_metadata <- function(metadata, source_state) {
      if (nrow(metadata) == 0L || nrow(source_state) == 0L) return(metadata)
      earliest_checkpoint <- min(as.POSIXct(source_state$object_created_at, tz = "UTC"))
      accepted_versions <- paste(
        source_state$source_object_uri,
        source_state$source_object_generation,
        sep = "#"
      )
      metadata_versions <- paste(
        metadata$source_object_uri,
        metadata$source_object_generation,
        sep = "#"
      )
      metadata[
        metadata$object_created_at >= earliest_checkpoint &
          !metadata_versions %in% accepted_versions,
        ,
        drop = FALSE
      ]
    }

  # ? Classify only new object headers and download every selected delivery window
    discover_new_objects <- function(source_prefix, source_state, run_dir) {
      metadata <- list_source_object_metadata(source_prefix)
      candidates <- prefilter_source_metadata(metadata, source_state)
      if (nrow(candidates) == 0L) {
        candidates$source_feed <- character(0)
        candidates$selection_status <- character(0)
        candidates$source_snapshot_max_date <- as.Date(character(0))
        candidates$local_path <- character(0)
        return(list(total_objects = nrow(metadata), header_objects = 0L, inventory = candidates))
      }

      candidates$source_feed <- vapply(
        seq_len(nrow(candidates)),
        function(index) {
          header <- read_source_object_header(
            candidates$source_object_uri[[index]],
            candidates$source_object_generation[[index]],
            candidates$object_size_bytes[[index]]
          )
          classify_polaris_source_schema(header)
        },
        character(1)
      )
      inventory <- select_new_polaris_objects(candidates, source_state)
      inventory$source_snapshot_max_date <- as.Date(NA)
      inventory$local_path <- NA_character_
      selected_indexes <- which(inventory$selection_status == "selected_new")
      if (length(selected_indexes) > 0L) {
        selected_order <- order_polaris_objects(inventory[selected_indexes, , drop = FALSE])
        selected_versions <- paste(
          selected_order$source_object_uri,
          selected_order$source_object_generation,
          sep = "#"
        )
        inventory_versions <- paste(
          inventory$source_object_uri,
          inventory$source_object_generation,
          sep = "#"
        )
        selected_indexes <- match(selected_versions, inventory_versions)
      }
      for (index in selected_indexes) {
        versioned_uri <- paste0(
          inventory$source_object_uri[[index]], "#",
          inventory$source_object_generation[[index]]
        )
        destination <- file.path(
          run_dir,
          sprintf(
            "selected_%s_%s.csv",
            inventory$source_feed[[index]],
            inventory$source_object_generation[[index]]
          )
        )
        run_loader_command(
          "gcloud",
          c("storage", "cp", shQuote(versioned_uri), shQuote(destination))
        )
        source_data <- read.csv(
          destination,
          check.names = FALSE,
          stringsAsFactors = FALSE,
          fileEncoding = "UTF-8-BOM"
        )
        inventory$source_snapshot_max_date[[index]] <- polaris_snapshot_max_date(
          source_data,
          inventory$source_feed[[index]]
        )
        inventory$local_path[[index]] <- destination
      }
      selected <- inventory[selected_indexes, , drop = FALSE]
      validate_polaris_source_progress(selected, source_state)
      list(total_objects = nrow(metadata), header_objects = nrow(candidates), inventory = inventory)
    }

  # ? Copy exact source bytes into an isolated temporary run directory
    normalize_source_inventory <- function(inventory, run_dir) {
      rows <- lapply(seq_len(nrow(inventory)), function(index) {
        feed <- inventory$source_feed[[index]]
        uri <- inventory$source_object_uri[[index]]
        source_data <- read.csv(
          inventory$local_path[[index]],
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

  # ? Run and download a SELECT inside one named, timed diagnostic boundary
    run_bq_download <- function(project, sql, stage) {
      run_polaris_bq_stage(stage, function() {
        destination <- bigrquery::bq_project_query(
          project,
          sql,
          use_legacy_sql = FALSE,
          quiet = TRUE
        )
        bigrquery::bq_table_download(destination, quiet = TRUE)
      })
    }

  # ? Identify whether the current per-feed checkpoint table has been deployed
    source_state_table_exists <- function(config) {
      run_polaris_bq_stage("source_state_table_check", function() {
        bigrquery::bq_table_exists(
          bigrquery::bq_table(config$project, "landing", config$state_table)
        )
      })
    }

  # ? Read one object's exact generation and creation time for state bootstrap
    describe_source_object <- function(source_object_uri) {
      output <- run_loader_command(
        "gcloud",
        c("storage", "objects", "describe", shQuote(source_object_uri), "--format=json")
      )
      metadata <- jsonlite::fromJSON(paste(output, collapse = "\n"))
      data.frame(
        source_object_generation = as.character(metadata$generation),
        object_created_at = parse_storage_time(metadata$creation_time),
        stringsAsFactors = FALSE
      )
    }

  # ? Derive the initial checkpoints from the exact objects already in landing
    derive_source_state_from_landing <- function(config) {
      sql <- sprintf(
        paste0(
          "WITH source_objects AS (",
          "SELECT source_feed, source_object_uri, MAX(date) AS source_max_date, ",
          "MAX(loaded_at) AS successful_load_at ",
          "FROM `%s.landing.polaris_email_delivery_daily` ",
          "WHERE client_id = '%s' AND connection_id = '%s' ",
          "GROUP BY source_feed, source_object_uri) ",
          "SELECT * FROM source_objects ",
          "QUALIFY ROW_NUMBER() OVER (PARTITION BY source_feed ",
          "ORDER BY successful_load_at DESC, source_object_uri DESC) = 1"
        ),
        config$project,
        gsub("'", "''", config$client_id),
        gsub("'", "''", config$connection_id)
      )
      state <- run_bq_download(config$project, sql, "source_state_bootstrap")
      if (!setequal(state$source_feed, c("meta", "tiktok"))) {
        stop("Cannot bootstrap Polaris source state: landing does not contain both feeds", call. = FALSE)
      }
      metadata <- lapply(state$source_object_uri, describe_source_object)
      state$source_object_generation <- vapply(
        metadata, function(item) item$source_object_generation[[1]], character(1)
      )
      state$object_created_at <- as.POSIXct(
        vapply(metadata, function(item) as.numeric(item$object_created_at[[1]]), numeric(1)),
        origin = "1970-01-01",
        tz = "UTC"
      )
      state
    }

  # ? Read the deployed checkpoints or derive them read-only for a dry run
    read_source_state <- function(config) {
      state <- data.frame()
      if (source_state_table_exists(config)) {
        sql <- sprintf(
          paste0(
            "SELECT source_feed, source_object_uri, source_object_generation, ",
            "object_created_at, source_max_date, successful_load_at ",
            "FROM `%s.landing.%s` ",
            "WHERE client_id = '%s' AND connection_id = '%s'"
          ),
          config$project,
          config$state_table,
          gsub("'", "''", config$client_id),
          gsub("'", "''", config$connection_id)
        )
        state <- run_bq_download(config$project, sql, "source_state_read")
      }
      if (setequal(state$source_feed, c("meta", "tiktok")) && nrow(state) == 2L) return(state)
      if (!config$dry_run) {
        stop(
          paste0(
            "Polaris source state is not initialized. Run ",
            file.path(script_dir, "create_polaris_email_source_state.sql"),
            " before a production load."
          ),
          call. = FALSE
        )
      }
      message("Polaris source state is not initialized; deriving read-only checkpoints from landing.")
      derive_source_state_from_landing(config)
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
      run_bq_download(config$project, sql, "active_mapping_read")
    }

  # ? Read the small package-level production summary used in the run comparison
    read_production_package_summary <- function(config) {
      sql <- sprintf(
        paste0(
          "SELECT package_id, ",
          "ARRAY_AGG(package_friendly_label IGNORE NULLS ORDER BY loaded_at DESC LIMIT 1)",
          "[SAFE_OFFSET(0)] AS package_friendly_label, ",
          "COUNT(*) AS record_count, MIN(date) AS start_date, MAX(date) AS end_date, ",
          "SUM(spend) AS spend, SUM(impressions) AS impressions ",
          "FROM `%s.landing.polaris_email_delivery_daily` ",
          "WHERE client_id = '%s' AND connection_id = '%s' ",
          "GROUP BY package_id"
        ),
        config$project,
        gsub("'", "''", config$client_id),
        gsub("'", "''", config$connection_id)
      )
      summary <- run_bq_download(config$project, sql, "production_summary_read")
      summary$start_date <- as.Date(summary$start_date)
      summary$end_date <- as.Date(summary$end_date)
      summary
    }

  # ? Keep the reader-facing totals optional because they do not protect the data
    read_optional_production_package_summary <- function(config) {
      tryCatch(
        read_production_package_summary(config),
        error = function(error) {
          cat(conditionMessage(error), "\n", sep = "")
          cat(
            "⚠️ Production totals are temporarily unavailable; ",
            "the validated data update will continue.\n",
            sep = ""
          )
          NULL
        }
      )
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

  # ? Build one checkpoint row from the newest successfully processed object per feed
    prepare_source_state_rows <- function(selected_inventory, config, successful_load_at) {
      # A late backfill owns arrival progress, but not the batch's newest data date.
      newest_dates <- tapply(
        as.numeric(selected_inventory$source_snapshot_max_date),
        selected_inventory$source_feed, max
      )
      selected_inventory <- latest_polaris_objects_by_feed(selected_inventory)
      data.frame(
        client_id = config$client_id,
        connection_id = config$connection_id,
        source_feed = selected_inventory$source_feed,
        source_object_uri = selected_inventory$source_object_uri,
        source_object_generation = selected_inventory$source_object_generation,
        object_created_at = as.POSIXct(selected_inventory$object_created_at, tz = "UTC"),
        source_max_date = as.Date(
          as.numeric(newest_dates[selected_inventory$source_feed]), origin = "1970-01-01"
        ),
        successful_load_at = as.POSIXct(successful_load_at, tz = "UTC"),
        stringsAsFactors = FALSE
      )
    }


# * SECTION [4]: GUARDED SNAPSHOT REPLACEMENT

  # ? Stage, validate, upsert new rows, and advance affected feed state atomically
    upsert_delivery_feeds <- function(rows, state_rows, config) {
      suffix <- format(Sys.time(), "%Y%m%d_%H%M%S", tz = "UTC")
      delivery_staging_name <- paste0(
        "polaris_email_delivery_daily_staging_", suffix, "_", Sys.getpid()
      )
      state_staging_name <- paste0(
        "polaris_email_source_state_staging_", suffix, "_", Sys.getpid()
      )
      delivery_staging_id <- paste(config$project, "landing", delivery_staging_name, sep = ".")
      state_staging_id <- paste(config$project, "landing", state_staging_name, sep = ".")
      production_id <- paste(config$project, "landing", "polaris_email_delivery_daily", sep = ".")
      state_id <- paste(config$project, "landing", config$state_table, sep = ".")

      create_delivery_staging <- sprintf(
        paste0(
          "CREATE TABLE `%s` LIKE `%s` OPTIONS (description = ",
          "'Temporary guarded Polaris Email feed candidate. Safe to delete after the load completes; the loader owns cleanup.')"
        ),
        delivery_staging_id,
        production_id
      )
      create_state_staging <- sprintf(
        paste0(
          "CREATE TABLE `%s` LIKE `%s` OPTIONS (description = ",
          "'Temporary Polaris Email source checkpoint candidate. Safe to delete after the load completes; the loader owns cleanup.')"
        ),
        state_staging_id,
        state_id
      )
      run_bq_statement(config$project, create_delivery_staging)
      run_bq_statement(config$project, create_state_staging)
      bigrquery::bq_table_upload(
        bigrquery::bq_table(config$project, "landing", delivery_staging_name),
        rows,
        create_disposition = "CREATE_NEVER",
        write_disposition = "WRITE_APPEND",
        quiet = TRUE
      )
      bigrquery::bq_table_upload(
        bigrquery::bq_table(config$project, "landing", state_staging_name),
        state_rows,
        create_disposition = "CREATE_NEVER",
        write_disposition = "WRITE_APPEND",
        quiet = TRUE
      )

      validation_sql <- sprintf(
        paste0(
          "SELECT ",
          "(SELECT COUNT(*) FROM `%s`) AS row_count, ",
          "(SELECT COUNTIF(mapping_status != 'mapped') FROM `%s`) AS unmapped_rows, ",
          "(SELECT COUNTIF(package_id IS NULL OR date IS NULL OR platform IS NULL ",
          "OR campaign_name IS NULL OR ad_group_name IS NULL OR ad_name IS NULL) FROM `%s`) AS invalid_rows, ",
          "(SELECT COUNT(*) - COUNT(DISTINCT natural_row_key) FROM `%s`) AS duplicate_rows, ",
          "(SELECT COUNT(*) FROM `%s`) AS state_rows, ",
          "(SELECT COUNT(DISTINCT source_feed) FROM `%s`) AS state_feeds, ",
          "(SELECT COUNT(*) FROM `%s` d LEFT JOIN `%s` s ",
          "USING (client_id, connection_id, source_feed) WHERE s.source_feed IS NULL) AS rows_without_state, ",
          "(SELECT COUNT(*) FROM `%s` candidate JOIN `%s` production ",
          "ON production.client_id = candidate.client_id ",
          "AND production.connection_id = candidate.connection_id ",
          "AND production.source_feed = candidate.source_feed ",
          "AND production.natural_row_key = candidate.natural_row_key) AS replaced_rows"
        ),
        delivery_staging_id, delivery_staging_id, delivery_staging_id, delivery_staging_id,
        state_staging_id, state_staging_id, delivery_staging_id, state_staging_id,
        delivery_staging_id, production_id
      )
      gate <- run_bq_download(config$project, validation_sql, "validation_gate")
      if (
        gate$row_count[[1]] != nrow(rows) ||
          gate$unmapped_rows[[1]] != 0 || gate$invalid_rows[[1]] != 0 || gate$duplicate_rows[[1]] != 0 ||
          gate$state_rows[[1]] != nrow(state_rows) ||
          gate$state_feeds[[1]] != nrow(state_rows) || gate$rows_without_state[[1]] != 0
      ) {
        stop(
          paste(
            "Warehouse validation failed; production is unchanged. Staging tables retained:",
            delivery_staging_id, state_staging_id
          ),
          call. = FALSE
        )
      }

      upsert_sql <- sprintf(
        paste0(
          "BEGIN TRANSACTION; ",
          "DELETE FROM `%s` AS production WHERE EXISTS (",
          "SELECT 1 FROM `%s` AS candidate ",
          "WHERE production.client_id = candidate.client_id ",
          "AND production.connection_id = candidate.connection_id ",
          "AND production.source_feed = candidate.source_feed ",
          "AND production.natural_row_key = candidate.natural_row_key); ",
          "INSERT INTO `%s` SELECT * FROM `%s`; ",
          "MERGE `%s` AS target USING `%s` AS source ",
          "ON target.client_id = source.client_id ",
          "AND target.connection_id = source.connection_id ",
          "AND target.source_feed = source.source_feed ",
          "WHEN MATCHED THEN UPDATE SET ",
          "source_object_uri = source.source_object_uri, ",
          "source_object_generation = source.source_object_generation, ",
          "object_created_at = source.object_created_at, ",
          "source_max_date = source.source_max_date, ",
          "successful_load_at = source.successful_load_at ",
          "WHEN NOT MATCHED THEN INSERT (",
          "client_id, connection_id, source_feed, source_object_uri, ",
          "source_object_generation, object_created_at, source_max_date, successful_load_at",
          ") VALUES (source.client_id, source.connection_id, source.source_feed, ",
          "source.source_object_uri, source.source_object_generation, source.object_created_at, ",
          "source.source_max_date, source.successful_load_at); ",
          "COMMIT TRANSACTION;"
        ),
        production_id, delivery_staging_id,
        production_id, delivery_staging_id,
        state_id, state_staging_id
      )
      run_bq_statement(config$project, upsert_sql)
      run_bq_statement(
        config$project,
        sprintf("DROP TABLE `%s`; DROP TABLE `%s`", delivery_staging_id, state_staging_id)
      )
      invisible(gate)
    }


# * SECTION [5]: READER-FACING RUN REPORT

  # ? Summarize every exact source object downloaded during this run
    build_source_file_report <- function(source_rows) {
      source_objects <- unique(source_rows$source_object_uri)
      output <- lapply(source_objects, function(source_object_uri) {
        selected <- source_rows[
          source_rows$source_object_uri == source_object_uri,
          ,
          drop = FALSE
        ]
        data.frame(
          source_feed = selected$source_feed[[1]],
          source_object_uri = source_object_uri,
          start_date = min(selected$date),
          end_date = max(selected$date),
          record_count = nrow(selected),
          stringsAsFactors = FALSE
        )
      })
      output <- do.call(rbind, output)
      output <- output[order(output$source_feed, output$source_object_uri), , drop = FALSE]
      rownames(output) <- NULL
      output
    }

  # ? Print exact files, row effects, package coverage, totals, and assignments
    print_production_run_report <- function(
      source_rows,
      delivery,
      classified,
      comparison,
      replaced_rows,
      elapsed_seconds
    ) {
      source_files <- build_source_file_report(source_rows)
      new_rows <- nrow(delivery) - replaced_rows
      assignments <- unique(classified[, c(
        "source_feed", "platform", "ad_group_name", "package_friendly_label"
      )])
      assignments <- assignments[
        order(assignments$source_feed, assignments$platform, assignments$ad_group_name),
        ,
        drop = FALSE
      ]

      cat("\n✅ POLARIS EMAIL LOAD COMPLETED\n\n")
      cat("SOURCE FILES INGESTED\n\n")
      for (index in seq_len(nrow(source_files))) {
        source_name <- if (source_files$source_feed[[index]] == "meta") {
          "Meta daily report"
        } else {
          "TikTok daily report"
        }
        cat("  ", source_name, "\n", sep = "")
        cat("    File:  ", basename(source_files$source_object_uri[[index]]), "\n", sep = "")
        cat("    GCS:   ", source_files$source_object_uri[[index]], "\n", sep = "")
        cat(
          "    Dates: ",
          format_polaris_date_range(
            source_files$start_date[[index]], source_files$end_date[[index]]
          ),
          ", ", format(source_files$end_date[[index]], "%Y"), "\n",
          sep = ""
        )
        cat("    Rows:  ", format(source_files$record_count[[index]], big.mark = ","), "\n\n", sep = "")
      }
      cat("  Total incoming rows: ", format(nrow(delivery), big.mark = ","), "\n\n\n", sep = "")

      cat("WHAT CHANGED\n\n")
      cat(
        "  ", format(nrow(delivery), big.mark = ","),
        " incoming rows validated and written:\n",
        "    • ", format(replaced_rows, big.mark = ","),
        " existing production rows replaced\n",
        "    • ", format(new_rows, big.mark = ","),
        " new production rows added\n\n\n",
        sep = ""
      )

      if (is.null(comparison)) {
        cat("PRODUCTION TOTALS\n\n")
        cat("  Temporarily unavailable; the validated data update succeeded.\n")
      } else {
        cat("DATE COVERAGE\n\n")
        cat("  Package                Before             After\n")
        for (index in seq_len(nrow(comparison))) {
          cat(sprintf(
            "  %-22s %-18s %s\n",
            comparison$package_friendly_label[[index]],
            format_polaris_date_range(
              comparison$start_date_before[[index]], comparison$end_date_before[[index]]
            ),
            format_polaris_date_range(
              comparison$start_date_after[[index]], comparison$end_date_after[[index]]
            )
          ))
        }

        cat("\n\nPRODUCTION TOTALS — Before → After (Change)\n\n")
        cat(paste(render_polaris_production_totals(comparison), collapse = "\n"), "\n")
      }

      cat("\n\nSOURCE AD GROUP ASSIGNMENTS\n\n")
      for (source_feed in unique(assignments$source_feed)) {
        cat("  ", if (source_feed == "meta") "Meta" else "TikTok", "\n", sep = "")
        feed_assignments <- assignments[assignments$source_feed == source_feed, , drop = FALSE]
        for (index in seq_len(nrow(feed_assignments))) {
          cat(sprintf(
            "    %-24s → %s\n",
            feed_assignments$ad_group_name[[index]],
            feed_assignments$package_friendly_label[[index]]
          ))
        }
        cat("\n")
      }

      cat("\nRESULT\n\n")
      cat("  ✓ Production load succeeded\n")
      cat(
        "  ✓ Meta and TikTok updated through ",
        format(max(delivery$date), "%b %d, %Y"), "\n",
        sep = ""
      )
      cat("  ✓ Loader completed in ", round(elapsed_seconds), " seconds\n", sep = "")
    }

  # ? Keep dry-run output explicit without pretending production was changed
    print_dry_run_report <- function(source_rows, delivery, classified) {
      source_files <- build_source_file_report(source_rows)
      assignments <- unique(classified[, c(
        "source_feed", "ad_group_name", "package_friendly_label"
      )])
      assignments <- assignments[order(assignments$source_feed, assignments$ad_group_name), , drop = FALSE]
      cat("\nDRY RUN PASSED — PRODUCTION UNCHANGED\n\n")
      cat("SOURCE FILES VALIDATED\n")
      for (index in seq_len(nrow(source_files))) {
        cat(
          "  ", source_files$source_feed[[index]], ": ",
          source_files$source_object_uri[[index]], " (",
          format(source_files$record_count[[index]], big.mark = ","), " rows, ",
          format_polaris_date_range(
            source_files$start_date[[index]], source_files$end_date[[index]]
          ), ")\n",
          sep = ""
        )
      }
      cat("\nSOURCE AD GROUP ASSIGNMENTS\n")
      for (index in seq_len(nrow(assignments))) {
        cat(
          "  ", assignments$ad_group_name[[index]], " → ",
          assignments$package_friendly_label[[index]], "\n",
          sep = ""
        )
      }
      cat("\n  ✓ ", format(nrow(delivery), big.mark = ","), " incoming rows validated\n", sep = "")
    }


# * SECTION [6]: MANUAL ENTRYPOINT

  # ? Run metadata-first discovery and update only feeds with new objects
    run_polaris_email_loader <- function(arguments = commandArgs(trailingOnly = TRUE)) {
      run_started_at <- Sys.time()
      config <- parse_loader_args(arguments)
      require_loader_packages()
      run_dir <- tempfile("polaris_email_load_")
      dir.create(run_dir, recursive = TRUE)
      on.exit(unlink(run_dir, recursive = TRUE, force = TRUE), add = TRUE)

      source_state <- read_source_state(config)
      discovery <- discover_new_objects(config$source_prefix, source_state, run_dir)
      inventory <- discovery$inventory
      selected_inventory <- inventory[
        inventory$selection_status == "selected_new", , drop = FALSE
      ]
      if (nrow(selected_inventory) == 0L) {
        cat(
          if (config$dry_run) {
            "DRY RUN PASSED — NO NEW SOURCE FILES\n"
          } else {
            "✅ POLARIS EMAIL LOAD COMPLETED — NO NEW SOURCE FILES\n"
          },
          "Production data changed: no\n",
          sep = ""
        )
        return(invisible(NULL))
      }

      normalized <- normalize_source_inventory(selected_inventory, run_dir)
      mappings <- read_active_mappings(config)
      classified <- apply_polaris_package_mappings(normalized, mappings)
      classified <- collapse_polaris_object_overlaps(classified)
      validate_ready_snapshot(classified)
      loaded_at <- Sys.time()
      delivery <- prepare_delivery_rows(classified, config, loaded_at)
      state_rows <- prepare_source_state_rows(selected_inventory, config, loaded_at)

      reconciliation <- build_polaris_reconciliation(classified)
      difference_columns <- grep("_difference$", names(reconciliation), value = TRUE)
      if (!all(vapply(reconciliation[difference_columns], function(value) all(value == 0), logical(1)))) {
        stop("Polaris Email raw-to-normalized reconciliation failed", call. = FALSE)
      }

      if (config$dry_run) {
        print_dry_run_report(normalized, delivery, classified)
        return(invisible(list(
          inventory = inventory, state_rows = state_rows, delivery = delivery
        )))
      }

      before_summary <- read_optional_production_package_summary(config)
      load_gate <- upsert_delivery_feeds(delivery, state_rows, config)
      after_summary <- read_optional_production_package_summary(config)
      comparison <- if (is.null(before_summary) || is.null(after_summary)) {
        NULL
      } else {
        build_polaris_production_comparison(before_summary, after_summary)
      }
      print_production_run_report(
        source_rows = normalized,
        delivery = delivery,
        classified = classified,
        comparison = comparison,
        replaced_rows = as.integer(load_gate$replaced_rows[[1]]),
        elapsed_seconds = as.numeric(difftime(Sys.time(), run_started_at, units = "secs"))
      )
      invisible(list(
        inventory = inventory,
        state_rows = state_rows,
        delivery = delivery,
        before_summary = before_summary,
        after_summary = after_summary,
        comparison = comparison
      ))
    }

    if (sys.nframe() == 0L) run_polaris_email_loader()
