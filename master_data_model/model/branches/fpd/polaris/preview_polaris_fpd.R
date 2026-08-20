#!/usr/bin/env Rscript
################################################################################
#### PREVIEW POLARIS FIRST-PARTY DATA
################################################################################
# Purpose:
#   Read exactly one Meta and one TikTok Polaris CSV from the configured GCS
#   prefix and produce local review artifacts without changing live systems.
# Inputs and outputs:
#   Requires --output-dir. Optional --source-prefix and --compare-live-model
#   control the read-only inputs. All generated artifacts stay in output-dir.
# Safe usage:
#   This script reads GCS and may issue a read-only BigQuery SELECT. It never
#   creates, replaces, updates, or deletes cloud objects or production data.
################################################################################


# * SECTION [1]: COMMAND-LINE CONFIGURATION

  # Description: Parse explicit arguments and enforce a clean local output boundary.

  # ? Resolve this script's directory so it can source its adjacent pure logic
    script_arg <- grep("^--file=", commandArgs(trailingOnly = FALSE), value = TRUE)
    script_path <- if (length(script_arg) == 1) sub("^--file=", "", script_arg) else normalizePath(".")
    script_dir <- dirname(normalizePath(script_path, mustWork = TRUE))
    source(file.path(script_dir, "polaris_fpd_logic.R"))

  # ? Parse supported value and Boolean command-line options
    parse_preview_args <- function(arguments) {
      config <- list(
        source_prefix = paste0(
          "gs://bkt-plrs-prd-data-imports-7nqd/data/",
          "client_id=C70545844/connection_id=11694"
        ),
        output_dir = NULL,
        compare_live_model = FALSE
      )
      index <- 1L
      while (index <= length(arguments)) {
        argument <- arguments[[index]]
        if (argument == "--compare-live-model") {
          config$compare_live_model <- TRUE
          index <- index + 1L
        } else if (argument %in% c("--source-prefix", "--output-dir")) {
          if (index == length(arguments)) {
            stop(paste("Missing value for", argument), call. = FALSE)
          }
          key <- sub("^--", "", argument)
          key <- gsub("-", "_", key)
          config[[key]] <- arguments[[index + 1L]]
          index <- index + 2L
        } else {
          stop(paste("Unsupported argument:", argument), call. = FALSE)
        }
      }
      if (is.null(config$output_dir) || trimws(config$output_dir) == "") {
        stop("--output-dir is required", call. = FALSE)
      }
      config
    }

  # ? Refuse to mix a new preview with existing files from another run
    prepare_output_dir <- function(output_dir) {
      if (dir.exists(output_dir) && length(list.files(output_dir, all.files = TRUE, no.. = TRUE)) > 0) {
        stop(paste("Output directory must be new or empty:", output_dir), call. = FALSE)
      }
      dir.create(output_dir, recursive = TRUE, showWarnings = FALSE)
      dir.create(file.path(output_dir, "raw", "meta"), recursive = TRUE, showWarnings = FALSE)
      dir.create(file.path(output_dir, "raw", "tiktok"), recursive = TRUE, showWarnings = FALSE)
      normalizePath(output_dir, mustWork = TRUE)
    }


# * SECTION [2]: READ-ONLY SOURCE DISCOVERY

  # Description: Discover and copy source objects while failing on snapshot ambiguity.

  # ? Run a local command and preserve its readable failure output
    run_preview_command <- function(command, arguments) {
      output <- suppressWarnings(system2(command, arguments, stdout = TRUE, stderr = TRUE))
      status <- attr(output, "status")
      if (!is.null(status) && status != 0) {
        stop(paste(c(paste("Command failed:", command), output), collapse = "\n"), call. = FALSE)
      }
      output
    }

  # ? List CSV objects and classify only the two approved Stage 1 feeds
    discover_polaris_objects <- function(source_prefix) {
      objects <- run_preview_command(
        "gcloud",
        c("storage", "ls", shQuote(paste0(sub("/$", "", source_prefix), "/**")))
      )
      objects <- trimws(objects)
      objects <- objects[grepl("\\.csv$", objects, ignore.case = TRUE)]
      feed <- ifelse(
        grepl("meta", objects, ignore.case = TRUE),
        "meta",
        ifelse(grepl("tiktok", objects, ignore.case = TRUE), "tiktok", "unsupported")
      )
      data.frame(source_object_uri = objects, source_feed = feed, stringsAsFactors = FALSE)
    }

  # ? Record discovered objects and stop instead of guessing across snapshots
    validate_source_inventory <- function(inventory, output_dir) {
      write.csv(inventory, file.path(output_dir, "source_inventory.csv"), row.names = FALSE, na = "")
      counts <- table(factor(inventory$source_feed, levels = c("meta", "tiktok", "unsupported")))
      problems <- character(0)
      if (counts[["meta"]] != 1L) problems <- c(problems, paste("Expected 1 Meta CSV; found", counts[["meta"]]))
      if (counts[["tiktok"]] != 1L) problems <- c(problems, paste("Expected 1 TikTok CSV; found", counts[["tiktok"]]))
      if (counts[["unsupported"]] > 0L) {
        problems <- c(problems, paste("Found", counts[["unsupported"]], "unsupported CSV object(s)"))
      }
      if (length(problems) > 0) {
        writeLines(
          c(
            "# Polaris FPD Preview — Source Selection Stopped",
            "",
            "No source rows were normalized because the GCS snapshot was ambiguous.",
            "",
            paste0("- ", problems),
            "",
            "Review `source_inventory.csv`; no cloud or production data was changed."
          ),
          file.path(output_dir, "run_summary.md")
        )
        stop(paste(problems, collapse = "; "), call. = FALSE)
      }
      invisible(TRUE)
    }

  # ? Copy the exact source bytes into the explicit local review directory
    copy_polaris_source <- function(source_object_uri, source_feed, output_dir) {
      destination <- file.path(output_dir, "raw", source_feed, basename(source_object_uri))
      run_preview_command(
        "gcloud",
        c("storage", "cp", shQuote(source_object_uri), shQuote(destination))
      )
      destination
    }

  # ? Read a copied CSV while safely handling Meta's UTF-8 byte-order mark
    read_polaris_csv <- function(path) {
      read.csv(
        path,
        check.names = FALSE,
        stringsAsFactors = FALSE,
        fileEncoding = "UTF-8-BOM"
      )
    }


# * SECTION [3]: READ-ONLY WEEKLY OVERLAP

  # Description: Compare Sunday-start Polaris weeks to native weekly MIQ FPD evidence.

  # ? Query the native FPD week field and merge it with weekly Polaris totals
    build_legacy_overlap <- function(normalized_rows, output_dir) {
      polaris_weekly <- build_polaris_weekly_package_summary(normalized_rows)
      if (nrow(polaris_weekly) == 0) return(NULL)

      package_values <- paste(sprintf("'%s'", unique(polaris_weekly$package_id)), collapse = ", ")
      minimum_week <- min(polaris_weekly$week_start)
      maximum_week <- max(polaris_weekly$week_start)
      query <- paste0(
        "SELECT package_id, week AS week_start, ",
        "MAX(date_final) AS latest_legacy_date, ",
        "SUM(spend) AS legacy_fpd_spend, ",
        "SUM(impressions) AS legacy_fpd_impressions, ",
        "SUM(clicks) AS legacy_fpd_clicks, ",
        "SUM(views) AS legacy_fpd_video_views, ",
        "SUM(completed_views) AS legacy_fpd_video_completions ",
        "FROM `looker-studio-pro-452620.landing.fpd_data_ranged_shortcutsFolder` ",
        "WHERE package_id IN (", package_values, ") ",
        "AND week BETWEEN DATE '", minimum_week, "' AND DATE '", maximum_week, "' ",
        "GROUP BY package_id, week_start ORDER BY package_id, week_start"
      )
      query_path <- tempfile("polaris_overlap_", fileext = ".sql")
      error_path <- tempfile("polaris_overlap_", fileext = ".err")
      result_path <- tempfile("polaris_overlap_", fileext = ".csv")
      on.exit(unlink(c(query_path, error_path, result_path)), add = TRUE)
      writeLines(query, query_path)
      status <- system2(
        "bq",
        c(
          "query", "--project_id=looker-studio-pro-452620",
          "--use_legacy_sql=false", "--format=csv"
        ),
        stdin = query_path,
        stdout = result_path,
        stderr = error_path
      )
      if (status != 0) {
        stop(
          paste(c("Read-only BigQuery overlap query failed:", readLines(error_path, warn = FALSE)), collapse = "\n"),
          call. = FALSE
        )
      }
      legacy <- read.csv(result_path, stringsAsFactors = FALSE)
      legacy$week_start <- as.Date(legacy$week_start)
      legacy$latest_legacy_date <- as.Date(legacy$latest_legacy_date)
      overlap <- merge(
        polaris_weekly,
        legacy,
        by = c("package_id", "week_start"),
        all = TRUE,
        sort = TRUE
      )
      overlap$week_end <- overlap$week_start + 6L
      overlap$comparison_grain <- "package_sunday_start_week"
      overlap$coverage_status <- ifelse(
        is.na(overlap$latest_polaris_date),
        "legacy_only",
        ifelse(
          is.na(overlap$latest_legacy_date),
          "polaris_only",
          ifelse(
            overlap$latest_polaris_date == overlap$latest_legacy_date,
            "matched_period_end",
            "different_period_end"
          )
        )
      )
      overlap$difference_interpretation <- ifelse(
        overlap$coverage_status == "matched_period_end",
        "like_for_like_period",
        "review_coverage_before_comparing"
      )
      metric_names <- c("spend", "impressions", "clicks", "video_views", "video_completions")
      for (metric in metric_names) {
        polaris_name <- paste0("polaris_", metric)
        legacy_name <- paste0("legacy_fpd_", metric)
        overlap[[paste0(metric, "_difference")]] <- overlap[[polaris_name]] - overlap[[legacy_name]]
      }
      overlap <- overlap[, c(
        "comparison_grain", "coverage_status", "difference_interpretation",
        "package_id", "week_start", "week_end",
        "latest_polaris_date", "latest_legacy_date",
        "polaris_spend", "legacy_fpd_spend", "spend_difference",
        "polaris_impressions", "legacy_fpd_impressions", "impressions_difference",
        "polaris_clicks", "legacy_fpd_clicks", "clicks_difference",
        "polaris_video_views", "legacy_fpd_video_views", "video_views_difference",
        "polaris_video_completions", "legacy_fpd_video_completions",
        "video_completions_difference"
      )]
      write.csv(overlap, file.path(output_dir, "legacy_overlap.csv"), row.names = FALSE, na = "")
      overlap
    }


# * SECTION [4]: REVIEW ARTIFACTS

  # Description: Write local evidence and a reader-friendly run result.

  # ? Format a metric for the Markdown summary without hiding decimals
    summary_number <- function(value, digits = 0L) {
      format(round(value, digits), big.mark = ",", nsmall = digits, scientific = FALSE, trim = TRUE)
    }

  # ? Write the approved Stage 1 artifacts and unresolved-item counts
    write_polaris_artifacts <- function(normalized_rows, inventory, output_dir, compared_live_model) {
      mapping_review <- build_polaris_mapping_review(normalized_rows)
      reconciliation <- build_polaris_reconciliation(normalized_rows)
      write.csv(normalized_rows, file.path(output_dir, "normalized_rows.csv"), row.names = FALSE, na = "")
      write.csv(mapping_review, file.path(output_dir, "mapping_review.csv"), row.names = FALSE, na = "")
      write.csv(reconciliation, file.path(output_dir, "source_reconciliation.csv"), row.names = FALSE, na = "")

      platform_summary <- summarize_polaris_metrics(normalized_rows, "preview")
      platform_lines <- vapply(seq_len(nrow(platform_summary)), function(index) {
        row <- platform_summary[index, ]
        paste0(
          "- ", row$platform, ": ", summary_number(row$preview_row_count),
          " rows; $", summary_number(row$preview_spend, 4L), " spend; ",
          summary_number(row$preview_impressions), " impressions; ",
          summary_number(row$preview_clicks), " clicks"
        )
      }, character(1))
      unresolved <- sum(normalized_rows$review_status == "needs_review")
      duplicate_rows <- sum(normalized_rows$natural_row_occurrences > 1)
      overlap_line <- if (compared_live_model) {
        "- `legacy_overlap.csv` compares Sunday-start Polaris weeks with native weekly MIQ FPD."
      } else {
        "- Live-model comparison was not requested; `legacy_overlap.csv` was not created."
      }
      writeLines(
        c(
          "# Polaris FPD Preview — Completed",
          "",
          "The preview read two GCS CSVs and wrote local review files. No cloud or production data was changed.",
          "",
          "## Source Results",
          "",
          platform_lines,
          "",
          "## Review Status",
          "",
          paste0("- Total normalized rows: ", summary_number(nrow(normalized_rows))),
          paste0("- Rows needing review: ", summary_number(unresolved)),
          paste0("- Rows participating in duplicate natural keys: ", summary_number(duplicate_rows)),
          paste0("- Source CSVs preserved unchanged: ", summary_number(nrow(inventory))),
          overlap_line,
          "",
          "## Safety Boundary",
          "",
          "This Stage 1 run did not write BigQuery, Google Sheets, the Manual Data Editor, the production model, or automation."
        ),
        file.path(output_dir, "run_summary.md")
      )
    }


# * SECTION [5]: PREVIEW RUN

  # Description: Execute the complete local preview and stop on unsafe ambiguity.

  # ? Discover, preserve, normalize, compare when requested, and summarize
    run_polaris_preview <- function(config) {
      output_dir <- prepare_output_dir(config$output_dir)
      inventory <- discover_polaris_objects(config$source_prefix)
      validate_source_inventory(inventory, output_dir)

      normalized_parts <- lapply(seq_len(nrow(inventory)), function(index) {
        source_uri <- inventory$source_object_uri[[index]]
        source_feed <- inventory$source_feed[[index]]
        local_path <- copy_polaris_source(source_uri, source_feed, output_dir)
        source_rows <- read_polaris_csv(local_path)
        if (source_feed == "meta") {
          normalize_polaris_meta(source_rows, source_uri)
        } else {
          normalize_polaris_tiktok(source_rows, source_uri)
        }
      })
      normalized_rows <- classify_polaris_rows(do.call(rbind, normalized_parts))
      if (config$compare_live_model) build_legacy_overlap(normalized_rows, output_dir)
      write_polaris_artifacts(
        normalized_rows,
        inventory,
        output_dir,
        compared_live_model = config$compare_live_model
      )
      cat("Polaris FPD preview completed:", output_dir, "\n")
      invisible(output_dir)
    }

    preview_config <- parse_preview_args(commandArgs(trailingOnly = TRUE))
    run_polaris_preview(preview_config)
