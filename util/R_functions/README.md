# Shared R Helpers

This folder holds reusable R helper files that multiple scripts in this repo can source.

## BigQuery Failure Alert Helper

**Helper script:** `bq_write_with_email_alerts.r`

Use this helper when an R script writes to BigQuery and you want:

- one shared write path
- one retry path for schema-like failures
- one shared failure email format

### What the alert email includes

- subject line in the format `🚨 Error in (R) Script | [script_name] failed to update [table_name]`
- local timestamp in 12-hour format without seconds
- failed script path
- failed script folder path
- BigQuery table identifier
- BigQuery table link
- path to this helper script
- path to this folder
- path to this README for update notes

### How to use it

Source the helper:

```r
source("/Users/eugenetsenter/Looker_clonedRepo/looker_personal/util/R_functions/bq_write_with_email_alerts.r")
```

Then call:

```r
write_to_bq_with_email_alerts(
  data = df,
  project_id = "looker-studio-pro-452620",
  dataset = "landing",
  table = "example_table",
  loader_name = "Example loader",
  script_path = "/absolute/path/to/your_script.R"
)
```

### Where to update the alert format

If you want to change the alert email subject or body, update:

- `/Users/eugenetsenter/Looker_clonedRepo/looker_personal/util/R_functions/bq_write_with_email_alerts.r`

If you want the shared instructions for this helper, update:

- `/Users/eugenetsenter/Looker_clonedRepo/looker_personal/util/R_functions/README.md`
