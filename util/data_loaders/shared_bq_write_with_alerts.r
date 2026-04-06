# Compatibility shim so older loader paths still work after the helper moved
# into the shared R helpers folder.

shim_dir <- if (!is.null(sys.frame(1)$ofile)) {
  dirname(sys.frame(1)$ofile)
} else {
  getwd()
}

source(
  normalizePath(
    file.path(shim_dir, "..", "R_functions", "bq_write_with_email_alerts.r"),
    winslash = "/",
    mustWork = TRUE
  )
)
