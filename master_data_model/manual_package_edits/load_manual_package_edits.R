#!/usr/bin/env Rscript

################################################################################
#### RUN CANONICAL MASTER DATA MODEL MANUAL PACKAGE EDITS LOADER
################################################################################
# Purpose:
#   Preserve the historical manual_package_edits/load_manual_package_edits.R
#   entrypoint while routing all real loader behavior through the organized
#   canonical implementation in model/manual_editor. Keeping this file small
#   prevents two loader copies from drifting apart.
################################################################################

canonical_loader <- "/Users/eugenetsenter/Looker_clonedRepo/looker_personal/master_data_model/model/manual_editor/load_manual_package_edits.R"

if (!file.exists(canonical_loader)) {
  stop("Canonical Manual Data Editor loader not found: ", canonical_loader, call. = FALSE)
}

loader_output <- system2("Rscript", canonical_loader, stdout = TRUE, stderr = TRUE)
if (length(loader_output)) {
  cat(paste(loader_output, collapse = "\n"), "\n")
}

loader_status <- attr(loader_output, "status")
if (is.null(loader_status)) {
  loader_status <- 0L
}
if (!identical(as.integer(loader_status), 0L)) {
  stop("Canonical Manual Data Editor loader failed with status ", loader_status, call. = FALSE)
}
