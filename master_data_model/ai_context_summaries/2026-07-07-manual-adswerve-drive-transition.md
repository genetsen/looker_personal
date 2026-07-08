# 2026-07-07 Manual And Adswerve Drive Transition

This summary was saved after context compaction during the live verification phase.

## Learned

- New Adswerve workbook: `1sKX783MxKr9QP91WcG9X6eGgMLPsN_Dgc8yVnwC-m_A`.
- New Manual Data Editor workbook: `1p1aGAg8lMk7JvUKCJBKRj5rKQNNYL3iEKnl0kPHvZ7E`.
- Old Adswerve and Manual workbooks were synced to the new versions, verified for workbook structure/content, and then renamed with `ARCHIVED 07/26 |`.
- The active `gcloud` account is `gene.tsenter@giantspoon.com`, but its token does not include Sheets scope. Sheets and Drive refreshes need cached R OAuth tokens; BigQuery can use the active `gcloud` token.

## Changed So Far

- Manual loader defaults now point to the new Manual Data Editor workbook.
- Adswerve compiler defaults now point to the new Adswerve workbook.
- Manual and Adswerve refresh auth is being split so Sheets/Drive use the new-account R OAuth cache while BigQuery uses the active `gcloud` token.

## Still Needed

- Rerun the Manual Data Editor runner-managed live refresh after the auth split.
- Verify the Manual live sheet, conditional-format/protection metadata, and BigQuery landing tables.
- Commit and push only the verified task changes, leaving unrelated dirty worktree files out.
