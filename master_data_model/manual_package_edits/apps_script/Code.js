const EDITOR_SHEET_NAME = "Package Editor";
const HEADER_ROW = 4;
const DATA_START_ROW = 5;
const TABLE_COL_COUNT = 30;
const REQUEST_UPDATE_CELL = "B3";
const REQUEST_STATUS_CELL = "C3";
const DEFAULT_NOTIFY_EMAIL = "gene.tsenter@giantspoon.com";
const LOADER_COMMAND = "MASTER_MANUAL_EDIT_SHEET_ID=1WerhrbBMggzCwIUCOsOCV33aHygV96jt1HgqiYcUHZo Rscript /Users/eugenetsenter/Looker_clonedRepo/looker_personal/master_data_model/manual_package_edits/load_manual_package_edits.R";

function onOpen() {
  SpreadsheetApp.getUi()
    .createMenu("Manual Editor")
    .addItem("Notify Gene to run update", "requestManualEditorUpdate")
    .addItem("Authorize request button", "installManualEditorAutomation")
    .addItem("Reset request checkbox", "setupManualEditorControls")
    .addToUi();
}

function onEdit(event) {
  handleManualEditorEdit_(event, false);
}

function handleManualEditorEdit(event) {
  handleManualEditorEdit_(event, true);
}

function handleManualEditorEdit_(event, canSendNotification) {
  if (!event || !event.range) return;

  const sheet = event.range.getSheet();
  if (sheet.getName() !== EDITOR_SHEET_NAME) return;

  const editedCell = event.range.getA1Notation();
  if (editedCell === REQUEST_UPDATE_CELL && event.value === "TRUE") {
    if (canSendNotification) {
      requestManualEditorUpdate(event);
    } else {
      SpreadsheetApp.getActive().toast("Sending refresh request...", "Manual Editor", 5);
    }
    return;
  }
}

function setupManualEditorControls() {
  const sheet = getEditorSheet_();

  sheet.getRange("A1:J1").clearContent();
  sheet.getRange("A2").setValue("Filter above. Find the package, then edit the visible value that needs correction. Metadata and flight dates apply to the whole package; delivered metrics apply only to Delivery Override Start/End. Orange means changed, purple means already manual, red means fix before load.");
  sheet.getRange("A3").setValue("Request refresh");
  sheet.getRange(REQUEST_UPDATE_CELL).setValue(false);
  sheet.getRange("C3").setValue("Notification only. Check this when edits are ready; Gene still needs to review or run the loader before dashboards update.");
  sheet.getRange(REQUEST_UPDATE_CELL).insertCheckboxes();
}

function requestManualEditorUpdate(event) {
  const sheet = getEditorSheet_();
  const spreadsheet = SpreadsheetApp.getActive();
  const scriptProperties = PropertiesService.getScriptProperties();
  const notifyEmail = scriptProperties.getProperty("MANUAL_EDITOR_NOTIFY_EMAIL") || DEFAULT_NOTIFY_EMAIL;
  const slackWebhookUrl = scriptProperties.getProperty("MANUAL_EDITOR_SLACK_WEBHOOK_URL");
  const requestedBy = getRequesterEmail_(event);
  const requestedAt = Utilities.formatDate(new Date(), Session.getScriptTimeZone(), "yyyy-MM-dd HH:mm:ss z");
  const subject = "Manual package editor refresh requested";
  const sheetUrl = spreadsheet.getUrl();
  const body = [
    "Manual package editor refresh requested.",
    "",
    `Requested by: ${requestedBy}`,
    `Requested at: ${requestedAt}`,
    `Sheet: ${sheetUrl}`,
    "",
    "Run this terminal command to update the data:",
    LOADER_COMMAND,
    "",
    "The sheet uses native slicers for browsing. Open the sheet link to review the current filtered view.",
  ].join("\n");

  if (slackWebhookUrl) {
    UrlFetchApp.fetch(slackWebhookUrl, {
      method: "post",
      contentType: "application/json",
      payload: JSON.stringify({ text: body }),
      muteHttpExceptions: true,
    });
  }

  MailApp.sendEmail({
    to: notifyEmail,
    subject,
    body,
  });

  sheet.getRange(REQUEST_UPDATE_CELL).setValue(false);
  sheet.getRange(REQUEST_STATUS_CELL).setValue(`Notification sent ${requestedAt}. Loader still needs to run before dashboards update.`);
  spreadsheet.toast("Refresh request sent. This notification does not run the loader by itself.", "Manual Editor", 5);
}

function getRequesterEmail_(event) {
  if (event && event.user && typeof event.user.getEmail === "function") {
    const eventUserEmail = event.user.getEmail();
    if (eventUserEmail) return eventUserEmail;
  }

  const activeUserEmail = Session.getActiveUser().getEmail();
  if (activeUserEmail) return activeUserEmail;

  return "Requester could not be identified by Google Apps Script";
}

function installManualEditorAutomation() {
  const spreadsheet = SpreadsheetApp.getActive();
  ScriptApp.getProjectTriggers().forEach((trigger) => {
    if (trigger.getHandlerFunction() === "handleManualEditorEdit") {
      ScriptApp.deleteTrigger(trigger);
    }
  });

  ScriptApp
    .newTrigger("handleManualEditorEdit")
    .forSpreadsheet(spreadsheet)
    .onEdit()
    .create();

  setupManualEditorControls();
  spreadsheet.toast("Request refresh checkbox is ready.", "Manual Editor", 5);
}

function getManualEditorAutomationStatus() {
  const spreadsheet = SpreadsheetApp.getActive();
  const triggers = ScriptApp.getProjectTriggers()
    .filter((trigger) => trigger.getHandlerFunction() === "handleManualEditorEdit")
    .map((trigger) => ({
      handlerFunction: trigger.getHandlerFunction(),
      eventType: String(trigger.getEventType()),
      sourceId: trigger.getTriggerSourceId(),
      sourceType: String(trigger.getTriggerSource()),
    }));

  return JSON.stringify({
    spreadsheetId: spreadsheet.getId(),
    matchingTriggerCount: triggers.length,
    triggers,
  });
}

function getEditorSheet_() {
  const sheet = SpreadsheetApp.getActive().getSheetByName(EDITOR_SHEET_NAME);
  if (!sheet) {
    throw new Error(`Missing sheet tab: ${EDITOR_SHEET_NAME}`);
  }
  return sheet;
}
