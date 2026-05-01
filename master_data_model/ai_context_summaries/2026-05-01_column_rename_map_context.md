# 2026-05-01 Column Rename Map Context

## What Changed

- Built `master_data_model_column_rename_map.csv` from the live BigQuery QA candidate `master_stg.data_model_qa_source_issues`.
- The CSV maps 117 current columns to proposed renamed columns, including source family, rename status, `do_not_sum`, notes, and user comments.
- User reviewed the CSV and added comments. Those comments were folded into the proposed mapping where clear.

## Current Decisions

- `planned_daily_spend_pk` maps to `_planned_spend`.
- `planned_daily_impressions_pk` maps to `_planned_impressions`.
- `planned_amount`, `planned_impressions`, and `planned_units` are package-level repeated planned totals and now use `_doNotSum`.
- `p_package_friendly` maps to `_package_name_friendly`.
- `gsMediaTeam_channel` maps to `ADIF_channel`, with a future task to design a reusable mapping system.
- `buy_type` and `buy_category` use `p_` names.
- `channel_raw` uses `qa_channel_raw`.

## Still Needs Attention

- `unit_type -> p_unit_type` is confirmed.
- Future tasks were noted for supplier-name cleanup and supplier-logo update-path documentation.
- SQL has not been rewritten yet.
