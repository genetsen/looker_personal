# TikTok Social Pacing Deployment Context

## Learned

ADIF Smart+ creatives live in TikTok `creative_history`, not the standard manual-ad history. Their hierarchy and pacing are separate joins: hierarchy comes from Smart+ metadata, while pacing requires a matching platform, campaign, ad group, and date flight.

## Changed

The shared pacing view now reads standard TikTok and ADIF Smart+ history, accepts `_gs_` and `WP_` campaigns, preserves creative IDs, and converts dynamic daily budget into the flight total expected by downstream allocation. The daily pacing mart normalizes platform aliases and fills missing delivery hierarchy from pacing metadata.

## Verified

BigQuery scheduled query `mart__pacing_table` run `6ac3563b-0000-2e3f-8d11-089e08265e58` succeeded on September 15, 2026. Seven pacing rows and 70 delivery rows had complete hierarchy and pacing; actual spend, impressions, and clicks were unchanged. Three temporary QA objects were deleted and confirmed absent.

## Still Needed

Review the targeted repository diff, run final formatting checks, update the changelog status after commit, and commit only the TikTok pacing and documentation files.
