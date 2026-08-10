/**
 * FPD PARTNER-TEMPLATE BOUND STUB
 * ---------------------------------------------------------------------------
 * Purpose: the small container-bound script that lives INSIDE the current
 * "Partner Data Collection | Dupe before using" workbook (spreadsheet id
 * 1pc9gXkMhWZ0dFNeagZWjUqsKUnWebIvB3xd5IGht4w4). It wires the in-sheet
 * "Publish" action to the corrected v3 duplicator library. All heavy logic
 * (duplicate, folder routing, hide tabs, ownership transfer, log) lives in the
 * library; this stub only passes the active spreadsheet in.
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
 *   Version   : 3   (fpdLib v3.1 -> current master 1pc9 + migrated CLIENTS root)
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
 * Runs automatically when the workbook opens. Adds the operator-facing entry
 * point that calls publishFpdTemplate(). This is the in-sheet trigger that
 * replaces / backs up the drawing button.
 */
function onOpen() {
  SpreadsheetApp.getUi().createMenu("Publish New FPD Sheet")
    .addItem("Publish", "publishFpdTemplate")
    .addToUi();
}
