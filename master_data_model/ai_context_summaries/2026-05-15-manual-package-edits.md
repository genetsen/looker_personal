# Manual Package Edits Handoff - 2026-05-15

The manual package editor is being rebuilt as a single-tab, media-buyer-friendly Google Sheet. Users should filter to a package, edit the visible value directly, and see edited cells marked with formatting only; backend `man_*` fields remain hidden from the sheet.

Key requirements still active:

- Do not edit non-fee lines during live tests.
- Planned Spend and Planned Impressions are flight-level only and should display final flight totals, not prorated values.
- Delivered metrics can be edited for any date range by adding or duplicating package rows.
- Conditional formatting must mark changed values in the sheet frontend.
- R should load and validate data only; sheet layout, formatting, filters, and instructions belong in the setup script and bound Apps Script.
- Verify loader ingestion, warehouse writes, sheet behavior, and final model behavior against live Google Sheets and BigQuery before claiming completion.

Current implementation direction:

- `load_manual_package_edits.R` preserves duplicate package rows, adds hidden baseline columns, blocks partial-range planned edits, and writes raw plus daily manual tables.
- `setup_manual_package_editor_sheet.mjs` owns the sheet UX, including hidden helper/baseline columns and conditional formatting for changed cells.
- `apps_script/Code.js` owns faceted filter controls for Advertiser, Channel, Campaign, and Site.
- `manual_package_edits/README.md` documents the media-buyer workflow and validation rules.
