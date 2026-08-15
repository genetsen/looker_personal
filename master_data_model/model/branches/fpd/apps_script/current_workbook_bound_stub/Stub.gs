/**
 * FPD PARTNER-TEMPLATE BOUND STUB
 * ---------------------------------------------------------------------------
 * Purpose: the small container-bound script that lives INSIDE the ACTUAL in-use
 * template "GS | Partner Data Collection | Template 2026 v3" (spreadsheet id
 * 1BYqrQrjL4_rf5-LKTlGkR94CkqSW6QOsYzAPLHxucfY). It wires the in-sheet "Publish"
 * action to the corrected duplicator library. All heavy logic (duplicate, folder
 * routing, hide tabs, membership control, log) lives in the library; this stub
 * only passes the active spreadsheet in.
 * NOTE: install this on 1BYq (the file operators duplicate), NOT the older
 * "*2025 Template* v3 ... Dupe before using" (1pc9...).
 *
 * WHY THIS EXISTS: after Gene's account migration (old.giantspoon.com ->
 * giantspoon.com) the workbook button was left bound to the OLD-account
 * library (publish_fpd_template), which still reads the retired v2 master
 * workbook. This stub re-binds it to the corrected v3 library.
 *
 * LIBRARY DEPENDENCY to add in the Apps Script editor
 * (Editor > Libraries > +):
 *   Identifier: FPDLib
 *   Script ID : 139EYkXK6w08HSN5wbWfdsoB3keQIEHSqFoEHqbckiYRWDIvvvIfz08BF
 *   Version   : 11  (opens the canonical shortcut folder from confirmation)
 *   Shortcut destination:
 *     https://drive.google.com/drive/folders/1pqQVdROIhOkfuBLwexH00uW4eiqkb0GY
 *     (owned by FPDLib CONFIG.SHORTCUT_FOLDER_ID, not by this wrapper)
 *
 * SAFETY: never run duplicateAndSetup from the Apps Script editor. It requires
 * the ACTIVE spreadsheet and drives its own confirmation/progress dialogs, so
 * it only works when triggered from inside the open workbook.
 */

/**
 * The single wrapper the button / menu calls. Passes the active spreadsheet
 * into the library exactly as the library's docs require.
 */
function publishFpdTemplate() {
  FPDLib.duplicateAndSetup(SpreadsheetApp.getActiveSpreadsheet());
}

/**
 * Owner-run, idempotent setup action for the two unpublished warning images.
 * Normal template copies inherit the images; published copies remove them.
 */
function installUnpublishedWarningImages() {
  const insertedCount = FPDLib.installUnpublishedWarnings();
  SpreadsheetApp.getActiveSpreadsheet().toast(
    insertedCount === 0
      ? "Unpublished warning images are already installed."
      : `Installed ${insertedCount} unpublished warning image(s).`,
    "FPD template status",
    8
  );
}

/**
 * Runs automatically when the workbook opens. Adds the operator-facing entry
 * point that calls publishFpdTemplate(). This is the in-sheet trigger that
 * replaces / backs up the drawing button.
 */
function onOpen() {
  SpreadsheetApp.getUi().createMenu("Publish New FPD Sheet")
    .addItem("Publish", "publishFpdTemplate")
    .addSeparator()
    .addItem("Install Unpublished Warning Images", "installUnpublishedWarningImages")
    .addToUi();
}
