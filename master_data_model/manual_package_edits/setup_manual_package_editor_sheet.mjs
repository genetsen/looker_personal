#!/usr/bin/env node

import { execFileSync } from "node:child_process";

const SHEET_ID = process.env.MASTER_MANUAL_EDIT_SHEET_ID;
const SPREADSHEET_TITLE = process.env.MASTER_MANUAL_EDIT_SPREADSHEET_TITLE || "Manual Data Editor | GS Internal";
const TAB_NAME = process.env.MASTER_MANUAL_EDIT_TAB || "Package Editor";
const INSTRUCTIONS_TAB_NAME = process.env.MASTER_MANUAL_EDIT_INSTRUCTIONS_TAB || "Instructions";
const AUTH_ACCOUNT = process.env.MASTER_MANUAL_EDIT_AUTH_EMAIL || "gene.tsenter@giantspoon.com";
const ALLOW_FORMAT_REBUILD = process.env.MASTER_MANUAL_EDIT_ALLOW_FORMAT_REBUILD === "YES";

if (!SHEET_ID) {
  throw new Error("Set MASTER_MANUAL_EDIT_SHEET_ID before running sheet setup.");
}

if (!ALLOW_FORMAT_REBUILD) {
  throw new Error(
    "Refusing to rebuild live Sheet formatting. User-made manual formatting edits may exist. " +
    "Set MASTER_MANUAL_EDIT_ALLOW_FORMAT_REBUILD=YES only after explicit approval for a full formatting rebuild.",
  );
}

const columns = [
  "Package ID",
  "Site",
  "Package Friendly Name",
  "Flight Start Date",
  "Flight End Date",
  "Planned Spend",
  "Planned Impressions",
  "Spend",
  "Impressions",
  "Clicks",
  "Video Plays",
  "Video Completions",
  "Delivery Override Start Date",
  "Delivery Override End Date",
  "Advertiser",
  "Package Type",
  "Channel",
  "Campaign",
  "Initiative",
  "Supplier Code",
  "Supplier Name",
  "Package Name",
  "GS Channel",
  "Baseline Flight Start Date",
  "Baseline Flight End Date",
  "Baseline Planned Spend",
  "Baseline Planned Impressions",
  "Baseline Spend",
  "Baseline Impressions",
  "Baseline Clicks",
  "Baseline Video Plays",
  "Baseline Video Completions",
  "Baseline Delivery Start Date",
  "Baseline Delivery End Date",
  "Baseline Advertiser",
  "Baseline Package Type",
  "Baseline Channel",
  "Baseline Campaign",
  "Baseline Initiative",
  "Baseline Supplier Code",
  "Baseline Supplier Name",
  "Baseline Package Name",
  "Baseline Package Friendly Name",
  "Baseline GS Channel",
  "Manual Marker Flight Start Date",
  "Manual Marker Flight End Date",
  "Manual Marker Planned Spend",
  "Manual Marker Planned Impressions",
  "Manual Marker Spend",
  "Manual Marker Impressions",
  "Manual Marker Clicks",
  "Manual Marker Video Plays",
  "Manual Marker Video Completions",
  "Manual Marker Delivery Start Date",
  "Manual Marker Delivery End Date",
  "Manual Marker Advertiser",
  "Manual Marker Package Type",
  "Manual Marker Channel",
  "Manual Marker Campaign",
  "Manual Marker Initiative",
  "Manual Marker Supplier Code",
  "Manual Marker Supplier Name",
  "Manual Marker Package Name",
  "Manual Marker Package Friendly Name",
  "Manual Marker GS Channel",
];

const headerRowIndex = 3;
const dataStartRowIndex = 4;
const colCount = columns.length;
const blankManualEntryRows = 250;
const colIndex = Object.fromEntries(columns.map((name, index) => [name, index]));
const visibleColumnCount = colIndex["Baseline Flight Start Date"];
const markerStartIndex = colIndex["Manual Marker Flight Start Date"];
const editableColumnIndexes = new Set([
  "Flight Start Date", "Flight End Date", "Planned Spend", "Planned Impressions",
  "Spend", "Impressions", "Clicks", "Video Plays", "Video Completions", "Delivery Override Start Date", "Delivery Override End Date",
  "Package Friendly Name", "Advertiser", "Package Type", "Channel", "Campaign", "Initiative", "Supplier Code", "Supplier Name", "Package Name", "GS Channel",
].map((name) => colIndex[name]));
const deliveredMetricColumnIndexes = new Set(["Spend", "Impressions", "Clicks", "Video Plays", "Video Completions"].map((name) => colIndex[name]));
const plannedMetricColumnIndexes = new Set(["Flight Start Date", "Flight End Date", "Planned Spend", "Planned Impressions"].map((name) => colIndex[name]));
const packageFriendlyNameColumnIndexes = new Set(["Package Friendly Name"].map((name) => colIndex[name]));
const metadataColumnIndexes = new Set(["Advertiser", "Package Type", "Channel", "Campaign", "Initiative", "Supplier Code", "Supplier Name", "Package Name", "GS Channel"].map((name) => colIndex[name]));
const hiddenColumnIndexes = new Set(columns.map((_, index) => index).filter((index) => index >= visibleColumnCount));
const markerNames = [
  "Flight Start Date", "Flight End Date", "Planned Spend", "Planned Impressions",
  "Spend", "Impressions", "Clicks", "Video Plays", "Video Completions", "Delivery Start Date", "Delivery End Date",
  "Advertiser", "Package Type", "Channel", "Campaign", "Initiative", "Supplier Code", "Supplier Name", "Package Name", "Package Friendly Name", "GS Channel",
];
const editMarkerPairs = [
  { editedName: "Flight Start Date", baselineName: "Baseline Flight Start Date" },
  { editedName: "Flight End Date", baselineName: "Baseline Flight End Date" },
  { editedName: "Delivery Override Start Date", baselineName: "Baseline Delivery Start Date" },
  { editedName: "Delivery Override End Date", baselineName: "Baseline Delivery End Date" },
  { editedName: "Spend", baselineName: "Baseline Spend" },
  { editedName: "Impressions", baselineName: "Baseline Impressions" },
  { editedName: "Planned Spend", baselineName: "Baseline Planned Spend" },
  { editedName: "Planned Impressions", baselineName: "Baseline Planned Impressions" },
  { editedName: "Clicks", baselineName: "Baseline Clicks" },
  { editedName: "Video Plays", baselineName: "Baseline Video Plays" },
  { editedName: "Video Completions", baselineName: "Baseline Video Completions" },
  { editedName: "Advertiser", baselineName: "Baseline Advertiser" },
  { editedName: "Package Type", baselineName: "Baseline Package Type" },
  { editedName: "Channel", baselineName: "Baseline Channel" },
  { editedName: "Campaign", baselineName: "Baseline Campaign" },
  { editedName: "Initiative", baselineName: "Baseline Initiative" },
  { editedName: "Supplier Code", baselineName: "Baseline Supplier Code" },
  { editedName: "Supplier Name", baselineName: "Baseline Supplier Name" },
  { editedName: "Package Name", baselineName: "Baseline Package Name" },
  { editedName: "Package Friendly Name", baselineName: "Baseline Package Friendly Name" },
  { editedName: "GS Channel", baselineName: "Baseline GS Channel" },
].map((pair) => ({
  editedIndex: colIndex[pair.editedName],
  baselineIndex: colIndex[pair.baselineName],
}));
const manualMarkerPairs = markerNames.map((name) => ({
  editedIndex: colIndex[name === "Delivery Start Date" ? "Delivery Override Start Date" : name === "Delivery End Date" ? "Delivery Override End Date" : name],
  markerIndex: colIndex[`Manual Marker ${name}`],
})).filter((pair) => pair.editedIndex !== undefined && pair.markerIndex !== undefined);
const slicers = [
  { title: "Advertiser", columnIndex: colIndex["Advertiser"], offsetXPixels: 0, widthPixels: 210, heightPixels: 58 },
  { title: "Package ID", columnIndex: colIndex["Package ID"], offsetXPixels: 220, widthPixels: 210, heightPixels: 58 },
  { title: "Initiative", columnIndex: colIndex["Initiative"], offsetXPixels: 440, widthPixels: 210, heightPixels: 58 },
  { title: "Channel", columnIndex: colIndex["Channel"], offsetXPixels: 660, widthPixels: 210, heightPixels: 58 },
  { title: "Campaign", columnIndex: colIndex["Campaign"], offsetXPixels: 880, widthPixels: 210, heightPixels: 58 },
  { title: "Site", columnIndex: colIndex["Site"], offsetXPixels: 1100, widthPixels: 210, heightPixels: 58 },
];
const widths = [
  105, 135, 720, 112, 112, 125, 140, 105, 120, 90,
  110, 130, 132, 132, 140, 110, 105, 150, 115, 90,
  140, 220, 120,
  ...Array(colCount - 23).fill(100),
];

function color(red, green, blue) {
  return { red, green, blue };
}

function gridRange(sheetId, startRowIndex, endRowIndex, startColumnIndex, endColumnIndex) {
  return { sheetId, startRowIndex, endRowIndex, startColumnIndex, endColumnIndex };
}

function columnLetter(index) {
  let value = index + 1;
  let out = "";
  while (value > 0) {
    const remainder = (value - 1) % 26;
    out = String.fromCharCode(65 + remainder) + out;
    value = Math.floor((value - remainder - 1) / 26);
  }
  return out;
}

function token() {
  return execFileSync(
    "gcloud",
    ["auth", "print-access-token", "--account", AUTH_ACCOUNT],
    { encoding: "utf8" },
  ).trim();
}

async function sheetsFetch(path, options = {}) {
  const response = await fetch(`https://sheets.googleapis.com/v4/spreadsheets/${SHEET_ID}${path}`, {
    ...options,
    headers: {
      Authorization: `Bearer ${token()}`,
      "Content-Type": "application/json",
      ...(options.headers || {}),
    },
  });

  if (!response.ok) {
    throw new Error(`${response.status} ${response.statusText}: ${await response.text()}`);
  }

  return response.json();
}

async function getSheet() {
  const spreadsheet = await sheetsFetch("?fields=sheets(properties(sheetId,title,index,gridProperties),merges,bandedRanges(bandedRangeId),protectedRanges(protectedRangeId,description),slicers(slicerId),conditionalFormats)");
  const sheet = spreadsheet.sheets.find((candidate) => candidate.properties.title === TAB_NAME);
  if (!sheet) {
    throw new Error(`Could not find tab: ${TAB_NAME}`);
  }
  return sheet;
}

async function getSpreadsheet() {
  return sheetsFetch("?fields=sheets(properties(sheetId,title,index,gridProperties),merges,bandedRanges(bandedRangeId),protectedRanges(protectedRangeId,description),slicers(slicerId),conditionalFormats)");
}

async function getExistingDataEndRowIndex() {
  const response = await sheetsFetch(
    `/values/${encodeURIComponent(`${TAB_NAME}!A${dataStartRowIndex + 1}:U`)}?valueRenderOption=UNFORMATTED_VALUE`,
  );
  const rows = response.values || [];
  let lastNonEmptyRowOffset = -1;

  rows.forEach((row, index) => {
    if (row.some((value) => value !== null && value !== undefined && String(value).trim() !== "")) {
      lastNonEmptyRowOffset = index;
    }
  });

  return lastNonEmptyRowOffset >= 0
    ? dataStartRowIndex + lastNonEmptyRowOffset + 1
    : dataStartRowIndex;
}

async function ensureInstructionsSheet(spreadsheet) {
  const existing = spreadsheet.sheets.find((candidate) => candidate.properties.title === INSTRUCTIONS_TAB_NAME);
  if (existing) {
    return existing;
  }

  const response = await sheetsFetch(":batchUpdate", {
    method: "POST",
    body: JSON.stringify({
      requests: [
        {
          addSheet: {
            properties: {
              title: INSTRUCTIONS_TAB_NAME,
              index: 0,
              gridProperties: {
                rowCount: 80,
                columnCount: 8,
                frozenRowCount: 1,
                hideGridlines: true,
              },
            },
          },
        },
      ],
    }),
  });

  return response.replies[0].addSheet;
}

async function writeInstructions() {
  const values = Array.from({ length: 3 }, () => Array(visibleColumnCount).fill(""));
  values[1][0] = "Use slicers above to find the package, then edit the visible value that needs correction.";
  values[1][3] = "Request refresh";
  values[1][4] = false;
  values[1][5] = "Notification only. Check this when edits are ready; Gene still needs to review or run the loader before dashboards update.";
  values[2][0] = "Locked IDs: Package ID + Site. Existing rows are locked; new rows can fill these.";
  values[2][2] = "Editable name: Package Friendly Name.";
  values[2][3] = "Editable planned values: Flight Start, Flight End, Planned Spend, Planned Impressions.";
  values[2][7] = "Editable delivered metrics: Spend, Impressions, Clicks, Video Plays, Video Completions.";
  values[2][12] = "Editable delivery window: Delivery Override Start/End controls metric edit dates.";
  values[2][14] = "Editable metadata: Advertiser, Package Type, Channel, Campaign, Initiative, Supplier, Package Name, GS Channel.";

  const lastVisibleColumn = columnLetter(visibleColumnCount - 1);
  await sheetsFetch(`/values/${encodeURIComponent(`${TAB_NAME}!A1:${lastVisibleColumn}3`)}?valueInputOption=USER_ENTERED`, {
    method: "PUT",
    body: JSON.stringify({ values }),
  });
}

async function batchUpdate(requests) {
  for (let i = 0; i < requests.length; i += 25) {
    await sheetsFetch(":batchUpdate", {
      method: "POST",
      body: JSON.stringify({ requests: requests.slice(i, i + 25) }),
    });
  }
}

function buildRequests(sheet, existingDataEndRowIndex) {
  const sheetId = sheet.properties.sheetId;
  const rowCount = Math.max(
    sheet.properties.gridProperties.rowCount || 2095,
    existingDataEndRowIndex + blankManualEntryRows,
    dataStartRowIndex + 1,
  );
  const requests = [];

  for (const band of sheet.bandedRanges || []) {
    requests.push({ deleteBanding: { bandedRangeId: band.bandedRangeId } });
  }

  for (const merge of sheet.merges || []) {
    if (
      merge.startRowIndex >= 0 &&
      merge.endRowIndex <= 3 &&
      merge.startColumnIndex >= 0 &&
      merge.endColumnIndex <= colCount
    ) {
      requests.push({ unmergeCells: { range: merge } });
    }
  }

  for (const protectedRange of sheet.protectedRanges || []) {
    if ((protectedRange.description || "").startsWith("Manual editor UX:")) {
      requests.push({ deleteProtectedRange: { protectedRangeId: protectedRange.protectedRangeId } });
    }
  }

  for (const slicer of sheet.slicers || []) {
    requests.push({ deleteEmbeddedObject: { objectId: slicer.slicerId } });
  }

  for (const _rule of sheet.conditionalFormats || []) {
    requests.push({
      deleteConditionalFormatRule: {
        sheetId,
        index: 0,
      },
    });
  }

  requests.push(
    {
      updateSpreadsheetProperties: {
        properties: { title: SPREADSHEET_TITLE },
        fields: "title",
      },
    },
    { clearBasicFilter: { sheetId } },
    {
      updateSheetProperties: {
        properties: {
          sheetId,
          gridProperties: {
            frozenRowCount: 4,
            frozenColumnCount: 0,
            columnCount: colCount,
            rowCount,
            hideGridlines: true,
          },
        },
        fields: "gridProperties.frozenRowCount,gridProperties.frozenColumnCount,gridProperties.columnCount,gridProperties.rowCount,gridProperties.hideGridlines",
      },
    },
    {
      repeatCell: {
        range: gridRange(sheetId, 0, rowCount, 0, colCount),
        cell: { note: null },
        fields: "note",
      },
    },
    {
      repeatCell: {
        range: gridRange(sheetId, 0, 3, 0, colCount),
        cell: {
          userEnteredFormat: {
            backgroundColor: color(1, 1, 1),
            textFormat: { foregroundColor: color(0.05, 0.13, 0.23), fontSize: 10 },
            horizontalAlignment: "LEFT",
            verticalAlignment: "MIDDLE",
            wrapStrategy: "WRAP",
          },
          dataValidation: null,
        },
        fields: "userEnteredFormat.backgroundColor,userEnteredFormat.textFormat,userEnteredFormat.horizontalAlignment,userEnteredFormat.verticalAlignment,userEnteredFormat.wrapStrategy,dataValidation",
      },
    },
    {
      mergeCells: {
        range: gridRange(sheetId, 1, 2, 0, 3),
        mergeType: "MERGE_ALL",
      },
    },
    {
      mergeCells: {
        range: gridRange(sheetId, 1, 2, 5, visibleColumnCount),
        mergeType: "MERGE_ALL",
      },
    },
    {
      mergeCells: {
        range: gridRange(sheetId, 2, 3, 0, 2),
        mergeType: "MERGE_ALL",
      },
    },
    {
      mergeCells: {
        range: gridRange(sheetId, 2, 3, 3, 7),
        mergeType: "MERGE_ALL",
      },
    },
    {
      mergeCells: {
        range: gridRange(sheetId, 2, 3, 7, 12),
        mergeType: "MERGE_ALL",
      },
    },
    {
      mergeCells: {
        range: gridRange(sheetId, 2, 3, 12, 14),
        mergeType: "MERGE_ALL",
      },
    },
    {
      mergeCells: {
        range: gridRange(sheetId, 2, 3, 14, visibleColumnCount),
        mergeType: "MERGE_ALL",
      },
    },
    {
      addBanding: {
        bandedRange: {
          range: gridRange(sheetId, headerRowIndex, rowCount, 0, colCount),
          rowProperties: {
            headerColor: color(0.09, 0.20, 0.34),
            firstBandColor: color(1.00, 1.00, 1.00),
            secondBandColor: color(0.95, 0.97, 0.99),
          },
        },
      },
    },
    {
      repeatCell: {
        range: gridRange(sheetId, headerRowIndex, headerRowIndex + 1, 0, colCount),
        cell: {
          userEnteredFormat: {
            backgroundColor: color(0.09, 0.20, 0.34),
            textFormat: { foregroundColor: color(1, 1, 1), bold: true },
            horizontalAlignment: "CENTER",
            verticalAlignment: "MIDDLE",
            wrapStrategy: "WRAP",
          },
        },
        fields: "userEnteredFormat.backgroundColor,userEnteredFormat.textFormat,userEnteredFormat.horizontalAlignment,userEnteredFormat.verticalAlignment,userEnteredFormat.wrapStrategy",
      },
    },
    {
      repeatCell: {
        range: gridRange(sheetId, dataStartRowIndex, rowCount, 0, colCount),
        cell: {
          userEnteredFormat: {
            verticalAlignment: "MIDDLE",
            wrapStrategy: "CLIP",
          },
        },
        fields: "userEnteredFormat.verticalAlignment,userEnteredFormat.wrapStrategy",
      },
    },
    {
      updateBorders: {
        range: gridRange(sheetId, headerRowIndex, rowCount, 0, colCount),
        top: { style: "SOLID", width: 1, color: color(0.72, 0.78, 0.86) },
        bottom: { style: "SOLID", width: 1, color: color(0.72, 0.78, 0.86) },
        left: { style: "SOLID", width: 1, color: color(0.72, 0.78, 0.86) },
        right: { style: "SOLID", width: 1, color: color(0.72, 0.78, 0.86) },
        innerHorizontal: { style: "SOLID", width: 1, color: color(0.86, 0.90, 0.94) },
        innerVertical: { style: "SOLID", width: 1, color: color(0.86, 0.90, 0.94) },
      },
    },
    {
      setBasicFilter: {
        filter: { range: gridRange(sheetId, headerRowIndex, rowCount, 0, colCount) },
      },
    },
  );

  for (const slicer of slicers) {
    requests.push({
      addSlicer: {
        slicer: {
          spec: {
            dataRange: gridRange(sheetId, headerRowIndex, rowCount, 0, colCount),
            columnIndex: slicer.columnIndex,
            title: slicer.title,
            textFormat: {
              foregroundColor: color(1, 1, 1),
              bold: true,
            },
            backgroundColor: color(0.09, 0.20, 0.34),
            horizontalAlignment: "LEFT",
          },
          position: {
            overlayPosition: {
              anchorCell: {
                sheetId,
                rowIndex: 0,
                columnIndex: 0,
              },
              offsetXPixels: slicer.offsetXPixels,
              offsetYPixels: 0,
              widthPixels: slicer.widthPixels,
              heightPixels: slicer.heightPixels,
            },
          },
        },
      },
    });
  }

  for (let index = 0; index < colCount; index += 1) {
    requests.push({
      updateDimensionProperties: {
        range: { sheetId, dimension: "COLUMNS", startIndex: index, endIndex: index + 1 },
        properties: { pixelSize: widths[index], hiddenByUser: hiddenColumnIndexes.has(index) },
        fields: "pixelSize,hiddenByUser",
      },
    });
  }

  requests.push(
    {
      updateDimensionProperties: {
        range: { sheetId, dimension: "COLUMNS", startIndex: 0, endIndex: visibleColumnCount },
        properties: { hiddenByUser: false },
        fields: "hiddenByUser",
      },
    },
    {
      updateDimensionProperties: {
        range: { sheetId, dimension: "COLUMNS", startIndex: visibleColumnCount, endIndex: colCount },
        properties: { hiddenByUser: true },
        fields: "hiddenByUser",
      },
    },
  );

  [64, 52, 66, 48].forEach((height, index) => {
    requests.push({
      updateDimensionProperties: {
        range: { sheetId, dimension: "ROWS", startIndex: index, endIndex: index + 1 },
        properties: { pixelSize: height },
        fields: "pixelSize",
      },
    });
  });

  requests.push({
    updateDimensionProperties: {
      range: { sheetId, dimension: "ROWS", startIndex: dataStartRowIndex, endIndex: rowCount },
      properties: { pixelSize: 58 },
      fields: "pixelSize",
    },
  });

  requests.push({
    repeatCell: {
      range: gridRange(sheetId, dataStartRowIndex, rowCount, 2, 3),
      cell: {
        userEnteredFormat: {
          textFormat: { fontSize: 9 },
          verticalAlignment: "MIDDLE",
          wrapStrategy: "WRAP",
        },
      },
      fields: "userEnteredFormat.textFormat.fontSize,userEnteredFormat.verticalAlignment,userEnteredFormat.wrapStrategy",
    },
  });

  for (const index of editableColumnIndexes) {
    requests.push(
      {
        repeatCell: {
          range: gridRange(sheetId, headerRowIndex, headerRowIndex + 1, index, index + 1),
          cell: {
            userEnteredFormat: {
              backgroundColor: color(0.98, 0.74, 0.20),
              textFormat: { foregroundColor: color(0.10, 0.08, 0.02), bold: true },
              horizontalAlignment: "CENTER",
              verticalAlignment: "MIDDLE",
              wrapStrategy: "WRAP",
            },
          },
          fields: "userEnteredFormat.backgroundColor,userEnteredFormat.textFormat,userEnteredFormat.horizontalAlignment,userEnteredFormat.verticalAlignment,userEnteredFormat.wrapStrategy",
        },
      },
      {
        repeatCell: {
          range: gridRange(sheetId, dataStartRowIndex, rowCount, index, index + 1),
          cell: { userEnteredFormat: { backgroundColor: color(1.00, 0.98, 0.84) } },
          fields: "userEnteredFormat.backgroundColor",
        },
      },
    );
  }

  for (const index of deliveredMetricColumnIndexes) {
    requests.push({
      repeatCell: {
        range: gridRange(sheetId, dataStartRowIndex, rowCount, index, index + 1),
        cell: { userEnteredFormat: { backgroundColor: color(1.00, 0.98, 0.84) } },
        fields: "userEnteredFormat.backgroundColor",
      },
    });
  }

  for (const index of plannedMetricColumnIndexes) {
    requests.push({
      repeatCell: {
        range: gridRange(sheetId, headerRowIndex, headerRowIndex + 1, index, index + 1),
        cell: {
          userEnteredFormat: {
            backgroundColor: color(0.51, 0.74, 0.49),
            textFormat: { foregroundColor: color(1, 1, 1), bold: true },
            horizontalAlignment: "CENTER",
            verticalAlignment: "MIDDLE",
            wrapStrategy: "WRAP",
          },
        },
        fields: "userEnteredFormat.backgroundColor,userEnteredFormat.textFormat,userEnteredFormat.horizontalAlignment,userEnteredFormat.verticalAlignment,userEnteredFormat.wrapStrategy",
      },
    });
    requests.push({
      repeatCell: {
        range: gridRange(sheetId, dataStartRowIndex, rowCount, index, index + 1),
        cell: { userEnteredFormat: { backgroundColor: color(0.92, 0.98, 0.90) } },
        fields: "userEnteredFormat.backgroundColor",
      },
    });
  }

  for (const index of packageFriendlyNameColumnIndexes) {
    requests.push(
      {
        repeatCell: {
          range: gridRange(sheetId, headerRowIndex, headerRowIndex + 1, index, index + 1),
          cell: {
            userEnteredFormat: {
              backgroundColor: color(0.29, 0.36, 0.45),
              textFormat: { foregroundColor: color(1, 1, 1), bold: true },
              horizontalAlignment: "CENTER",
              verticalAlignment: "MIDDLE",
              wrapStrategy: "WRAP",
            },
          },
          fields: "userEnteredFormat.backgroundColor,userEnteredFormat.textFormat,userEnteredFormat.horizontalAlignment,userEnteredFormat.verticalAlignment,userEnteredFormat.wrapStrategy",
        },
      },
      {
        repeatCell: {
          range: gridRange(sheetId, dataStartRowIndex, rowCount, index, index + 1),
          cell: {
            userEnteredFormat: {
              backgroundColor: color(0.96, 0.97, 0.98),
              textFormat: { fontSize: 9 },
              wrapStrategy: "WRAP",
            },
          },
          fields: "userEnteredFormat.backgroundColor,userEnteredFormat.textFormat.fontSize,userEnteredFormat.wrapStrategy",
        },
      },
    );
  }

  for (const index of metadataColumnIndexes) {
    requests.push(
      {
        repeatCell: {
          range: gridRange(sheetId, headerRowIndex, headerRowIndex + 1, index, index + 1),
          cell: {
            userEnteredFormat: {
              backgroundColor: color(0.11, 0.42, 0.48),
              textFormat: { foregroundColor: color(1, 1, 1), bold: true },
              horizontalAlignment: "CENTER",
              verticalAlignment: "MIDDLE",
              wrapStrategy: "WRAP",
            },
          },
          fields: "userEnteredFormat.backgroundColor,userEnteredFormat.textFormat,userEnteredFormat.horizontalAlignment,userEnteredFormat.verticalAlignment,userEnteredFormat.wrapStrategy",
        },
      },
      {
        repeatCell: {
          range: gridRange(sheetId, dataStartRowIndex, rowCount, index, index + 1),
          cell: {
            userEnteredFormat: {
              backgroundColor: color(0.90, 0.97, 0.98),
              wrapStrategy: "CLIP",
            },
          },
          fields: "userEnteredFormat.backgroundColor,userEnteredFormat.wrapStrategy",
        },
      },
    );
  }

  for (const pair of manualMarkerPairs) {
    const editedColumn = columnLetter(pair.editedIndex);
    const markerColumn = columnLetter(pair.markerIndex);
    const firstDataRow = dataStartRowIndex + 1;
    requests.push({
      addConditionalFormatRule: {
        rule: {
          ranges: [
            gridRange(sheetId, dataStartRowIndex, rowCount, pair.editedIndex, pair.editedIndex + 1),
          ],
          booleanRule: {
            condition: {
              type: "CUSTOM_FORMULA",
              values: [
                {
                  userEnteredValue: `=AND($${markerColumn}${firstDataRow}=TRUE,NOT(ISBLANK(${editedColumn}${firstDataRow})))`,
                },
              ],
            },
            format: {
              backgroundColor: color(0.88, 0.83, 0.97),
              textFormat: {
                foregroundColor: color(0.17, 0.10, 0.42),
                bold: true,
              },
            },
          },
        },
        index: 0,
      },
    });
  }

  for (const pair of editMarkerPairs) {
    const editedColumn = columnLetter(pair.editedIndex);
    const baselineColumn = columnLetter(pair.baselineIndex);
    const firstDataRow = dataStartRowIndex + 1;
    const manualMarkerPair = manualMarkerPairs.find((markerPair) => markerPair.editedIndex === pair.editedIndex);
    const manualMarkerGuard = manualMarkerPair
      ? `$${columnLetter(manualMarkerPair.markerIndex)}${firstDataRow}<>TRUE,`
      : "";
    requests.push({
      addConditionalFormatRule: {
        rule: {
          ranges: [
            gridRange(sheetId, dataStartRowIndex, rowCount, pair.editedIndex, pair.editedIndex + 1),
          ],
          booleanRule: {
            condition: {
              type: "CUSTOM_FORMULA",
              values: [
                {
                  userEnteredValue: `=AND(${manualMarkerGuard}NOT(ISBLANK(${editedColumn}${firstDataRow})),${editedColumn}${firstDataRow}<>$${baselineColumn}${firstDataRow})`,
                },
              ],
            },
            format: {
              backgroundColor: color(0.99, 0.84, 0.64),
              textFormat: {
                foregroundColor: color(0.30, 0.16, 0.02),
                bold: true,
              },
            },
          },
        },
        index: 0,
      },
    });
  }

  const firstBlankRow = existingDataEndRowIndex;
  const firstBlankSheetRow = firstBlankRow + 1;
  const firstDataSheetRow = dataStartRowIndex + 1;
  requests.push({
    addConditionalFormatRule: {
      rule: {
        ranges: [gridRange(sheetId, dataStartRowIndex, rowCount, 0, visibleColumnCount)],
        booleanRule: {
          condition: {
            type: "CUSTOM_FORMULA",
            values: [
              {
                userEnteredValue: `=AND($A${firstDataSheetRow}<>"",$X${firstDataSheetRow}="",COUNTA($A${firstDataSheetRow}:$W${firstDataSheetRow})>0,OR($A${firstDataSheetRow}="",$B${firstDataSheetRow}="",$C${firstDataSheetRow}="",$M${firstDataSheetRow}="",$N${firstDataSheetRow}="",COUNTA($F${firstDataSheetRow}:$L${firstDataSheetRow})=0,$O${firstDataSheetRow}="",$P${firstDataSheetRow}="",$Q${firstDataSheetRow}="",$R${firstDataSheetRow}="",$V${firstDataSheetRow}="",$W${firstDataSheetRow}="",$M${firstDataSheetRow}>$N${firstDataSheetRow},AND($D${firstDataSheetRow}<>"",$E${firstDataSheetRow}<>"",$D${firstDataSheetRow}>$E${firstDataSheetRow})))`,
              },
            ],
          },
          format: {
            backgroundColor: color(1.00, 0.86, 0.84),
            textFormat: {
              foregroundColor: color(0.43, 0.06, 0.04),
              bold: true,
            },
          },
        },
      },
      index: 0,
    },
  });
  if (firstBlankRow < rowCount) {
    requests.push(
      {
        addConditionalFormatRule: {
          rule: {
            ranges: [gridRange(sheetId, firstBlankRow, rowCount, 0, visibleColumnCount)],
            booleanRule: {
              condition: {
                type: "CUSTOM_FORMULA",
                values: [
                  {
                    userEnteredValue: `=AND(COUNTA($A${firstBlankSheetRow}:$W${firstBlankSheetRow})>0,OR($A${firstBlankSheetRow}="",$B${firstBlankSheetRow}="",$C${firstBlankSheetRow}="",$M${firstBlankSheetRow}="",$N${firstBlankSheetRow}="",COUNTA($F${firstBlankSheetRow}:$L${firstBlankSheetRow})=0,$O${firstBlankSheetRow}="",$P${firstBlankSheetRow}="",$Q${firstBlankSheetRow}="",$R${firstBlankSheetRow}="",$V${firstBlankSheetRow}="",$W${firstBlankSheetRow}=""))`,
                  },
                ],
              },
              format: {
                backgroundColor: color(1.00, 0.86, 0.84),
                textFormat: {
                  foregroundColor: color(0.43, 0.06, 0.04),
                  bold: true,
                },
              },
            },
          },
          index: 0,
        },
      },
      {
        addConditionalFormatRule: {
          rule: {
            ranges: [gridRange(sheetId, firstBlankRow, rowCount, colIndex["Delivery Override Start Date"], colIndex["Delivery Override End Date"] + 1)],
            booleanRule: {
              condition: {
                type: "CUSTOM_FORMULA",
                values: [
                  { userEnteredValue: `=AND($M${firstBlankSheetRow}<>"",$N${firstBlankSheetRow}<>"",$M${firstBlankSheetRow}>$N${firstBlankSheetRow})` },
                ],
              },
              format: {
                backgroundColor: color(1.00, 0.74, 0.70),
                textFormat: {
                  foregroundColor: color(0.43, 0.06, 0.04),
                  bold: true,
                },
              },
            },
          },
          index: 0,
        },
      },
    );
  }

  if (existingDataEndRowIndex > dataStartRowIndex) {
    const firstDataRow = dataStartRowIndex + 1;
    requests.push({
      addConditionalFormatRule: {
        rule: {
          ranges: [gridRange(sheetId, dataStartRowIndex, existingDataEndRowIndex, colIndex["Planned Spend"], colIndex["Planned Impressions"] + 1)],
          booleanRule: {
            condition: {
              type: "CUSTOM_FORMULA",
              values: [
                {
                  userEnteredValue: `=AND(OR($F${firstDataRow}<>$Z${firstDataRow},$G${firstDataRow}<>$AA${firstDataRow}),OR($M${firstDataRow}<>$AG${firstDataRow},$N${firstDataRow}<>$AH${firstDataRow}))`,
                },
              ],
            },
            format: {
              backgroundColor: color(1.00, 0.74, 0.70),
              textFormat: {
                foregroundColor: color(0.43, 0.06, 0.04),
                bold: true,
              },
            },
          },
        },
        index: 0,
      },
    });
  }

  requests.push(
    {
      repeatCell: {
        range: gridRange(sheetId, 0, 1, 0, visibleColumnCount),
        cell: {
          userEnteredFormat: {
            backgroundColor: color(0.90, 0.95, 1.00),
            verticalAlignment: "MIDDLE",
          },
          dataValidation: null,
        },
        fields: "userEnteredFormat.backgroundColor,userEnteredFormat.verticalAlignment,dataValidation",
      },
    },
    {
      repeatCell: {
        range: gridRange(sheetId, 1, 2, 0, 3),
        cell: {
          userEnteredFormat: {
            backgroundColor: color(0.86, 0.93, 1.00),
            textFormat: { foregroundColor: color(0.05, 0.13, 0.23), bold: true, fontSize: 10 },
            horizontalAlignment: "LEFT",
            verticalAlignment: "MIDDLE",
            wrapStrategy: "WRAP",
          },
          dataValidation: null,
        },
        fields: "userEnteredFormat.backgroundColor,userEnteredFormat.textFormat,userEnteredFormat.horizontalAlignment,userEnteredFormat.verticalAlignment,userEnteredFormat.wrapStrategy,dataValidation",
      },
    },
    {
      repeatCell: {
        range: gridRange(sheetId, 1, 2, 3, 4),
        cell: {
          userEnteredFormat: {
            backgroundColor: color(0.07, 0.28, 0.19),
            textFormat: { foregroundColor: color(1, 1, 1), bold: true, fontSize: 10 },
            horizontalAlignment: "CENTER",
            verticalAlignment: "MIDDLE",
            wrapStrategy: "WRAP",
          },
          dataValidation: null,
        },
        fields: "userEnteredFormat.backgroundColor,userEnteredFormat.textFormat,userEnteredFormat.horizontalAlignment,userEnteredFormat.verticalAlignment,userEnteredFormat.wrapStrategy,dataValidation",
      },
    },
    {
      repeatCell: {
        range: gridRange(sheetId, 1, 2, 4, 5),
        cell: {
          dataValidation: {
            condition: { type: "BOOLEAN" },
            strict: true,
            showCustomUi: true,
          },
          userEnteredFormat: {
            backgroundColor: color(0.83, 0.95, 0.86),
            horizontalAlignment: "CENTER",
            verticalAlignment: "MIDDLE",
          },
        },
        fields: "dataValidation,userEnteredFormat.backgroundColor,userEnteredFormat.horizontalAlignment,userEnteredFormat.verticalAlignment",
      },
    },
    {
      repeatCell: {
        range: gridRange(sheetId, 1, 2, 5, visibleColumnCount),
        cell: {
          userEnteredFormat: {
            backgroundColor: color(0.92, 0.98, 0.94),
            textFormat: { foregroundColor: color(0.06, 0.23, 0.16), fontSize: 10 },
            horizontalAlignment: "LEFT",
            verticalAlignment: "MIDDLE",
            wrapStrategy: "WRAP",
          },
          dataValidation: null,
        },
        fields: "userEnteredFormat.backgroundColor,userEnteredFormat.textFormat,userEnteredFormat.horizontalAlignment,userEnteredFormat.verticalAlignment,userEnteredFormat.wrapStrategy,dataValidation",
      },
    },
    {
      repeatCell: {
        range: gridRange(sheetId, 2, 3, 0, 2),
        cell: {
          userEnteredFormat: {
            backgroundColor: color(0.09, 0.20, 0.34),
            textFormat: { foregroundColor: color(1, 1, 1), bold: true, fontSize: 9 },
            horizontalAlignment: "CENTER",
            verticalAlignment: "MIDDLE",
            wrapStrategy: "WRAP",
          },
          dataValidation: null,
        },
        fields: "userEnteredFormat.backgroundColor,userEnteredFormat.textFormat,userEnteredFormat.horizontalAlignment,userEnteredFormat.verticalAlignment,userEnteredFormat.wrapStrategy,dataValidation",
      },
    },
    {
      repeatCell: {
        range: gridRange(sheetId, 2, 3, 2, 3),
        cell: {
          userEnteredFormat: {
            backgroundColor: color(0.29, 0.36, 0.45),
            textFormat: { foregroundColor: color(1, 1, 1), bold: true, fontSize: 9 },
            horizontalAlignment: "CENTER",
            verticalAlignment: "MIDDLE",
            wrapStrategy: "WRAP",
          },
          dataValidation: null,
        },
        fields: "userEnteredFormat.backgroundColor,userEnteredFormat.textFormat,userEnteredFormat.horizontalAlignment,userEnteredFormat.verticalAlignment,userEnteredFormat.wrapStrategy,dataValidation",
      },
    },
    {
      repeatCell: {
        range: gridRange(sheetId, 2, 3, 3, 7),
        cell: {
          userEnteredFormat: {
            backgroundColor: color(0.51, 0.74, 0.49),
            textFormat: { foregroundColor: color(1, 1, 1), bold: true, fontSize: 9 },
            horizontalAlignment: "CENTER",
            verticalAlignment: "MIDDLE",
            wrapStrategy: "WRAP",
          },
          dataValidation: null,
        },
        fields: "userEnteredFormat.backgroundColor,userEnteredFormat.textFormat,userEnteredFormat.horizontalAlignment,userEnteredFormat.verticalAlignment,userEnteredFormat.wrapStrategy,dataValidation",
      },
    },
    {
      repeatCell: {
        range: gridRange(sheetId, 2, 3, 7, 12),
        cell: {
          userEnteredFormat: {
            backgroundColor: color(0.98, 0.74, 0.20),
            textFormat: { foregroundColor: color(0.10, 0.08, 0.02), bold: true, fontSize: 9 },
            horizontalAlignment: "CENTER",
            verticalAlignment: "MIDDLE",
            wrapStrategy: "WRAP",
          },
          dataValidation: null,
        },
        fields: "userEnteredFormat.backgroundColor,userEnteredFormat.textFormat,userEnteredFormat.horizontalAlignment,userEnteredFormat.verticalAlignment,userEnteredFormat.wrapStrategy,dataValidation",
      },
    },
    {
      repeatCell: {
        range: gridRange(sheetId, 2, 3, 12, 14),
        cell: {
          userEnteredFormat: {
            backgroundColor: color(1.00, 0.86, 0.36),
            textFormat: { foregroundColor: color(0.10, 0.08, 0.02), bold: true, fontSize: 9 },
            horizontalAlignment: "CENTER",
            verticalAlignment: "MIDDLE",
            wrapStrategy: "WRAP",
          },
          dataValidation: null,
        },
        fields: "userEnteredFormat.backgroundColor,userEnteredFormat.textFormat,userEnteredFormat.horizontalAlignment,userEnteredFormat.verticalAlignment,userEnteredFormat.wrapStrategy,dataValidation",
      },
    },
    {
      repeatCell: {
        range: gridRange(sheetId, 2, 3, 14, visibleColumnCount),
        cell: {
          userEnteredFormat: {
            backgroundColor: color(0.11, 0.42, 0.48),
            textFormat: { foregroundColor: color(1, 1, 1), bold: true, fontSize: 9 },
            horizontalAlignment: "CENTER",
            verticalAlignment: "MIDDLE",
            wrapStrategy: "WRAP",
          },
          dataValidation: null,
        },
        fields: "userEnteredFormat.backgroundColor,userEnteredFormat.textFormat,userEnteredFormat.horizontalAlignment,userEnteredFormat.verticalAlignment,userEnteredFormat.wrapStrategy,dataValidation",
      },
    },
    {
      updateBorders: {
        range: gridRange(sheetId, 1, 3, 0, visibleColumnCount),
        top: { style: "SOLID_THICK", width: 2, color: color(1, 1, 1) },
        bottom: { style: "SOLID_THICK", width: 2, color: color(1, 1, 1) },
        left: { style: "SOLID_THICK", width: 2, color: color(1, 1, 1) },
        right: { style: "SOLID_THICK", width: 2, color: color(1, 1, 1) },
        innerVertical: { style: "SOLID_THICK", width: 2, color: color(1, 1, 1) },
        innerHorizontal: { style: "SOLID_THICK", width: 2, color: color(1, 1, 1) },
      },
    },
  );

  requests.push(
    {
      repeatCell: {
        range: gridRange(sheetId, 0, 3, 0, visibleColumnCount),
        cell: {
          note: "Use the slicers for Advertiser, Package ID, Initiative, Channel, Campaign, and Site to find the package row. Existing Package ID and Site values are locked; Package Friendly Name and metadata fields are editable. Planned values apply to the package. Delivered metric edits apply only to Delivery Override Start/End. Changed cells turn orange, live manual cells turn purple, and red rows must be fixed before load.",
        },
        fields: "note",
      },
    },
    {
      repeatCell: {
        range: gridRange(sheetId, 1, 2, 3, visibleColumnCount),
        cell: {
          note: "Check the box to send Gene a refresh request email. If a MANUAL_EDITOR_SLACK_WEBHOOK_URL script property exists, the same request also posts to Slack. The loader still runs from the scheduled/manual runner; this checkbox is a notification, not a direct warehouse write.",
        },
        fields: "note",
      },
    },
    {
      repeatCell: {
        range: gridRange(sheetId, headerRowIndex, headerRowIndex + 1, 0, 1),
        cell: {
          note: "Existing package: keep this ID. New package: enter a new unique package ID and complete the required metadata columns at the far right before the loader runs.",
        },
        fields: "note",
      },
    },
    {
      repeatCell: {
        range: gridRange(sheetId, headerRowIndex, headerRowIndex + 1, colIndex["Flight Start Date"], colIndex["Flight End Date"] + 1),
        cell: {
          note: "Package-level flight dates. Editing these overrides the package flight dates everywhere that Package ID appears.",
        },
        fields: "note",
      },
    },
    {
      repeatCell: {
        range: gridRange(sheetId, headerRowIndex, headerRowIndex + 1, colIndex["Delivery Override Start Date"], colIndex["Delivery Override End Date"] + 1),
        cell: {
          note: "Metric override window. Delivered metric edits affect only these dates. To correct one day or one week, add/duplicate a row for the same Package ID and set these dates to that smaller range.",
        },
        fields: "note",
      },
    },
    {
      repeatCell: {
        range: gridRange(sheetId, headerRowIndex, headerRowIndex + 1, colIndex["Spend"], colIndex["Impressions"] + 1),
        cell: {
          note: "Delivered actuals. Enter the replacement total for the Delivery Override Start/End range only. Leave unchanged metrics as-is.",
        },
        fields: "note",
      },
    },
    {
      repeatCell: {
        range: gridRange(sheetId, headerRowIndex, headerRowIndex + 1, colIndex["Planned Spend"], colIndex["Planned Impressions"] + 1),
        cell: {
          note: "Planned metrics are package-level flight totals. They must use the full delivery range, not a one-day or one-week exception row.",
        },
        fields: "note",
      },
    },
    {
      repeatCell: {
        range: gridRange(sheetId, headerRowIndex, headerRowIndex + 1, colIndex["Clicks"], colIndex["Video Completions"] + 1),
        cell: {
          note: "Delivered actuals. Enter the replacement total for the Delivery Override Start/End range only. Leave unchanged metrics as-is.",
        },
        fields: "note",
      },
    },
    {
      repeatCell: {
        range: gridRange(sheetId, headerRowIndex, headerRowIndex + 1, colIndex["Advertiser"], visibleColumnCount),
        cell: {
          note: "Metadata fields. Keep existing values unless you are adding a new package or correcting package metadata. New manual packages need Advertiser, Package Type, Channel, Campaign, Package Name, and GS Channel completed. Hidden columns to the right are internal comparison and manual-marker fields used only for formatting.",
        },
        fields: "note",
      },
    },
  );

  for (const index of [
    colIndex["Flight Start Date"],
    colIndex["Flight End Date"],
    colIndex["Delivery Override Start Date"],
    colIndex["Delivery Override End Date"],
    colIndex["Baseline Flight Start Date"],
    colIndex["Baseline Flight End Date"],
    colIndex["Baseline Delivery Start Date"],
    colIndex["Baseline Delivery End Date"],
  ]) {
    requests.push({
      repeatCell: {
        range: gridRange(sheetId, dataStartRowIndex, rowCount, index, index + 1),
        cell: { userEnteredFormat: { numberFormat: { type: "DATE", pattern: "yyyy-mm-dd" } } },
        fields: "userEnteredFormat.numberFormat",
      },
    });
  }

  for (const index of [colIndex["Spend"], colIndex["Planned Spend"], colIndex["Baseline Spend"], colIndex["Baseline Planned Spend"]]) {
    requests.push({
      repeatCell: {
        range: gridRange(sheetId, dataStartRowIndex, rowCount, index, index + 1),
        cell: { userEnteredFormat: { numberFormat: { type: "NUMBER", pattern: "$#,##0.00" } } },
        fields: "userEnteredFormat.numberFormat",
      },
    });
  }

  for (const index of [
    colIndex["Impressions"],
    colIndex["Planned Impressions"],
    colIndex["Clicks"],
    colIndex["Video Plays"],
    colIndex["Video Completions"],
    colIndex["Baseline Impressions"],
    colIndex["Baseline Planned Impressions"],
    colIndex["Baseline Clicks"],
    colIndex["Baseline Video Plays"],
    colIndex["Baseline Video Completions"],
  ]) {
    requests.push({
      repeatCell: {
        range: gridRange(sheetId, dataStartRowIndex, rowCount, index, index + 1),
        cell: { userEnteredFormat: { numberFormat: { type: "NUMBER", pattern: "#,##0" } } },
        fields: "userEnteredFormat.numberFormat",
      },
    });
  }

  if (firstBlankRow < rowCount) {
    requests.push(
      {
        repeatCell: {
          range: gridRange(sheetId, firstBlankRow, rowCount, 0, visibleColumnCount),
          cell: {
            note: "Add new manual-only rows in this blank area. Required fields: Package ID, Site, Package Friendly Name, Flight Start/End, Delivery Override Start/End, at least one metric, Advertiser, Package Type, Channel, Campaign, Package Name, and GS Channel. Package metadata changes apply to all dates for that Package ID; delivery metrics apply only to the delivery override dates.",
          },
          fields: "note",
        },
      },
      {
        updateBorders: {
          range: gridRange(sheetId, firstBlankRow, firstBlankRow + 1, 0, visibleColumnCount),
          top: { style: "SOLID_THICK", width: 3, color: color(0.43, 0.06, 0.04) },
          bottom: { style: "SOLID", width: 1, color: color(0.86, 0.90, 0.94) },
          left: { style: "SOLID", width: 1, color: color(0.86, 0.90, 0.94) },
          right: { style: "SOLID", width: 1, color: color(0.86, 0.90, 0.94) },
          innerVertical: { style: "SOLID", width: 1, color: color(0.86, 0.90, 0.94) },
        },
      },
    );
  }

  requests.push(
    {
      addProtectedRange: {
        protectedRange: {
          range: gridRange(sheetId, 0, 1, 0, visibleColumnCount),
          description: "Manual editor UX: lock slicer help row",
          warningOnly: false,
        },
      },
    },
    {
      addProtectedRange: {
        protectedRange: {
          range: gridRange(sheetId, 1, 2, 0, 3),
          description: "Manual editor UX: lock slicer instructions",
          warningOnly: false,
        },
      },
    },
    {
      addProtectedRange: {
        protectedRange: {
          range: gridRange(sheetId, 1, 2, 3, 4),
          description: "Manual editor UX: lock request label",
          warningOnly: false,
        },
      },
    },
    {
      addProtectedRange: {
        protectedRange: {
          range: gridRange(sheetId, 1, 2, 5, visibleColumnCount),
          description: "Manual editor UX: lock request status",
          warningOnly: false,
        },
      },
    },
    {
      addProtectedRange: {
        protectedRange: {
          range: gridRange(sheetId, 2, 3, 0, visibleColumnCount),
          description: "Manual editor UX: lock color legend row",
          warningOnly: false,
        },
      },
    },
    {
      addProtectedRange: {
        protectedRange: {
          range: gridRange(sheetId, headerRowIndex, headerRowIndex + 1, 0, colCount),
          description: "Manual editor UX: lock table headers",
          warningOnly: false,
        },
      },
    },
    {
      addProtectedRange: {
        protectedRange: {
          range: gridRange(sheetId, dataStartRowIndex, rowCount, visibleColumnCount, colCount),
          description: "Manual editor UX: lock hidden baseline columns",
          warningOnly: false,
        },
      },
    },
  );

  if (existingDataEndRowIndex > dataStartRowIndex) {
    requests.push(
      {
        addProtectedRange: {
          protectedRange: {
            range: gridRange(sheetId, dataStartRowIndex, existingDataEndRowIndex, 0, 2),
            description: "Manual editor UX: lock existing package identity columns",
            warningOnly: false,
          },
        },
      },
    );
  }

  return requests;
}

function buildInstructionsRequests(sheet) {
  const sheetId = sheet.properties.sheetId;
  const requests = [];
  const rowCount = Math.max(sheet.properties.gridProperties.rowCount || 80, 80);
  const colCount = Math.max(sheet.properties.gridProperties.columnCount || 8, 8);
  const instructionEndRowIndex = 47;
  const sectionRows = [2, 9, 19, 23, 29, 36, 43];
  const detailRows = [
    3, 4, 5, 6, 7,
    10, 11, 12, 13, 14, 15, 16, 17,
    20, 21,
    24, 25, 26, 27,
    30, 31, 32, 33, 34,
    37, 38, 39, 40, 41,
    44, 45, 46,
  ];
  const blankRows = new Set([1, 8, 18, 22, 28, 35, 42]);
  const sectionBands = [
    { start: 2, end: 8, header: 2, body: color(0.97, 0.99, 1.00), headerBg: color(0.90, 0.95, 1.00), headerFg: color(0.05, 0.13, 0.23) },
    { start: 9, end: 18, header: 9, body: color(0.97, 1.00, 0.96), headerBg: color(0.92, 0.98, 0.90), headerFg: color(0.07, 0.28, 0.12) },
    { start: 19, end: 22, header: 19, body: color(0.97, 1.00, 0.96), headerBg: color(0.92, 0.98, 0.90), headerFg: color(0.07, 0.28, 0.12) },
    { start: 23, end: 28, header: 23, body: color(0.93, 0.97, 1.00), headerBg: color(0.86, 0.93, 1.00), headerFg: color(0.05, 0.13, 0.23) },
    { start: 29, end: 35, header: 29, body: color(0.93, 0.97, 1.00), headerBg: color(0.86, 0.93, 1.00), headerFg: color(0.05, 0.13, 0.23) },
    { start: 36, end: 42, header: 36, body: color(1.00, 0.99, 0.94), headerBg: color(1.00, 0.96, 0.84), headerFg: color(0.38, 0.22, 0.00) },
    { start: 43, end: 47, header: 43, body: color(1.00, 0.99, 0.94), headerBg: color(1.00, 0.96, 0.84), headerFg: color(0.38, 0.22, 0.00) },
  ];

  for (const merge of sheet.merges || []) {
    requests.push({ unmergeCells: { range: merge } });
  }

  requests.push(
    {
      updateSheetProperties: {
        properties: {
          sheetId,
          gridProperties: {
            frozenRowCount: 1,
            columnCount: 8,
            rowCount,
            hideGridlines: true,
          },
        },
        fields: "gridProperties.frozenRowCount,gridProperties.columnCount,gridProperties.rowCount,gridProperties.hideGridlines",
      },
    },
    {
      repeatCell: {
        range: gridRange(sheetId, 0, rowCount, 0, colCount),
        cell: {
          userEnteredFormat: {
            backgroundColor: color(1, 1, 1),
            textFormat: { foregroundColor: color(0.12, 0.16, 0.22), fontSize: 10 },
            wrapStrategy: "WRAP",
            verticalAlignment: "TOP",
          },
        },
        fields: "userEnteredFormat.backgroundColor,userEnteredFormat.textFormat,userEnteredFormat.wrapStrategy,userEnteredFormat.verticalAlignment",
      },
    },
    {
      repeatCell: {
        range: gridRange(sheetId, 0, 1, 0, 8),
        cell: {
          userEnteredFormat: {
            backgroundColor: color(0.05, 0.13, 0.23),
            textFormat: { foregroundColor: color(1, 1, 1), bold: true, fontSize: 14 },
            horizontalAlignment: "LEFT",
            verticalAlignment: "MIDDLE",
          },
        },
        fields: "userEnteredFormat.backgroundColor,userEnteredFormat.textFormat,userEnteredFormat.horizontalAlignment,userEnteredFormat.verticalAlignment",
      },
    },
    {
      mergeCells: {
        range: gridRange(sheetId, 0, 1, 0, 8),
        mergeType: "MERGE_ALL",
      },
    },
    ...sectionRows.map((rowIndex) => ({
      mergeCells: {
        range: gridRange(sheetId, rowIndex, rowIndex + 1, 0, 8),
        mergeType: "MERGE_ALL",
      },
    })),
    ...detailRows.flatMap((rowIndex) => ([
      {
        mergeCells: {
          range: gridRange(sheetId, rowIndex, rowIndex + 1, 0, 2),
          mergeType: "MERGE_ALL",
        },
      },
      {
        mergeCells: {
          range: gridRange(sheetId, rowIndex, rowIndex + 1, 2, 8),
          mergeType: "MERGE_ALL",
        },
      },
    ])),
    {
      repeatCell: {
        range: gridRange(sheetId, 2, instructionEndRowIndex, 0, 8),
        cell: {
          userEnteredFormat: {
            backgroundColor: color(1, 1, 1),
            textFormat: { foregroundColor: color(0.12, 0.16, 0.22), fontSize: 10 },
            wrapStrategy: "WRAP",
            verticalAlignment: "MIDDLE",
          },
        },
        fields: "userEnteredFormat.backgroundColor,userEnteredFormat.textFormat,userEnteredFormat.wrapStrategy,userEnteredFormat.verticalAlignment",
      },
    },
    {
      repeatCell: {
        range: gridRange(sheetId, 2, 8, 0, 8),
        cell: {
          userEnteredFormat: {
            backgroundColor: color(0.90, 0.95, 1.00),
            textFormat: { foregroundColor: color(0.05, 0.13, 0.23), bold: true, fontSize: 12 },
            horizontalAlignment: "LEFT",
            verticalAlignment: "MIDDLE",
          },
        },
        fields: "userEnteredFormat.backgroundColor,userEnteredFormat.textFormat,userEnteredFormat.horizontalAlignment,userEnteredFormat.verticalAlignment",
      },
    },
    {
      repeatCell: {
        range: gridRange(sheetId, 3, 8, 0, 8),
        cell: {
          userEnteredFormat: {
            backgroundColor: color(0.97, 0.99, 1.00),
            wrapStrategy: "WRAP",
          },
        },
        fields: "userEnteredFormat.backgroundColor,userEnteredFormat.wrapStrategy",
      },
    },
    {
      repeatCell: {
        range: gridRange(sheetId, 9, 21, 0, 8),
        cell: {
          userEnteredFormat: {
            backgroundColor: color(0.97, 1.00, 0.96),
            wrapStrategy: "WRAP",
          },
        },
        fields: "userEnteredFormat.backgroundColor,userEnteredFormat.wrapStrategy",
      },
    },
    {
      repeatCell: {
        range: gridRange(sheetId, 9, 10, 0, 8),
        cell: {
          userEnteredFormat: {
            backgroundColor: color(0.92, 0.98, 0.90),
            textFormat: { foregroundColor: color(0.07, 0.28, 0.12), bold: true, fontSize: 12 },
            verticalAlignment: "MIDDLE",
          },
        },
        fields: "userEnteredFormat.backgroundColor,userEnteredFormat.textFormat,userEnteredFormat.verticalAlignment",
      },
    },
    {
      repeatCell: {
        range: gridRange(sheetId, 19, 20, 0, 8),
        cell: {
          userEnteredFormat: {
            backgroundColor: color(0.92, 0.98, 0.90),
            textFormat: { foregroundColor: color(0.07, 0.28, 0.12), bold: true, fontSize: 12 },
            verticalAlignment: "MIDDLE",
          },
        },
        fields: "userEnteredFormat.backgroundColor,userEnteredFormat.textFormat,userEnteredFormat.verticalAlignment",
      },
    },
    {
      repeatCell: {
        range: gridRange(sheetId, 23, 35, 0, 8),
        cell: {
          userEnteredFormat: {
            backgroundColor: color(0.93, 0.97, 1.00),
            wrapStrategy: "WRAP",
          },
        },
        fields: "userEnteredFormat.backgroundColor,userEnteredFormat.wrapStrategy",
      },
    },
    {
      repeatCell: {
        range: gridRange(sheetId, 23, 24, 0, 8),
        cell: {
          userEnteredFormat: {
            backgroundColor: color(0.86, 0.93, 1.00),
            textFormat: { foregroundColor: color(0.05, 0.13, 0.23), bold: true, fontSize: 12 },
            verticalAlignment: "MIDDLE",
          },
        },
        fields: "userEnteredFormat.backgroundColor,userEnteredFormat.textFormat,userEnteredFormat.verticalAlignment",
      },
    },
    {
      repeatCell: {
        range: gridRange(sheetId, 29, 30, 0, 8),
        cell: {
          userEnteredFormat: {
            backgroundColor: color(0.86, 0.93, 1.00),
            textFormat: { foregroundColor: color(0.05, 0.13, 0.23), bold: true, fontSize: 12 },
          },
        },
        fields: "userEnteredFormat.backgroundColor,userEnteredFormat.textFormat",
      },
    },
    {
      repeatCell: {
        range: gridRange(sheetId, 35, 41, 0, 8),
        cell: {
          userEnteredFormat: {
            backgroundColor: color(1.00, 0.99, 0.94),
            wrapStrategy: "WRAP",
          },
        },
        fields: "userEnteredFormat.backgroundColor,userEnteredFormat.wrapStrategy",
      },
    },
    {
      repeatCell: {
        range: gridRange(sheetId, 35, 36, 0, 8),
        cell: {
          userEnteredFormat: {
            backgroundColor: color(1.00, 0.96, 0.84),
            textFormat: { foregroundColor: color(0.38, 0.22, 0.00), bold: true, fontSize: 12 },
          },
        },
        fields: "userEnteredFormat.backgroundColor,userEnteredFormat.textFormat",
      },
    },
    {
      repeatCell: {
        range: gridRange(sheetId, 42, 46, 0, 8),
        cell: {
          userEnteredFormat: {
            backgroundColor: color(1.00, 0.99, 0.94),
            wrapStrategy: "WRAP",
          },
        },
        fields: "userEnteredFormat.backgroundColor,userEnteredFormat.wrapStrategy",
      },
    },
    {
      repeatCell: {
        range: gridRange(sheetId, 42, 43, 0, 8),
        cell: {
          userEnteredFormat: {
            backgroundColor: color(1.00, 0.96, 0.84),
            textFormat: { foregroundColor: color(0.38, 0.22, 0.00), bold: true, fontSize: 12 },
          },
        },
        fields: "userEnteredFormat.backgroundColor,userEnteredFormat.textFormat",
      },
    },
    {
      repeatCell: {
        range: gridRange(sheetId, 3, instructionEndRowIndex, 0, 2),
        cell: {
          userEnteredFormat: {
            textFormat: { bold: true },
          },
        },
        fields: "userEnteredFormat.textFormat.bold",
      },
    },
    {
      updateBorders: {
        range: gridRange(sheetId, 2, instructionEndRowIndex, 0, 8),
        top: { style: "SOLID", width: 1, color: color(0.82, 0.87, 0.93) },
        bottom: { style: "SOLID", width: 1, color: color(0.82, 0.87, 0.93) },
        left: { style: "SOLID", width: 1, color: color(0.82, 0.87, 0.93) },
        right: { style: "SOLID", width: 1, color: color(0.82, 0.87, 0.93) },
        innerHorizontal: { style: "SOLID", width: 1, color: color(0.90, 0.93, 0.96) },
        innerVertical: { style: "SOLID", width: 1, color: color(0.90, 0.93, 0.96) },
      },
    },
  );

  for (const band of sectionBands) {
    requests.push(
      {
        repeatCell: {
          range: gridRange(sheetId, band.start, band.end, 0, 8),
          cell: {
            userEnteredFormat: {
              backgroundColor: band.body,
              textFormat: { foregroundColor: color(0.12, 0.16, 0.22), fontSize: 10 },
              wrapStrategy: "WRAP",
              verticalAlignment: "MIDDLE",
            },
          },
          fields: "userEnteredFormat.backgroundColor,userEnteredFormat.textFormat,userEnteredFormat.wrapStrategy,userEnteredFormat.verticalAlignment",
        },
      },
      {
        repeatCell: {
          range: gridRange(sheetId, band.header, band.header + 1, 0, 8),
          cell: {
            userEnteredFormat: {
              backgroundColor: band.headerBg,
              textFormat: { foregroundColor: band.headerFg, bold: true, fontSize: 12 },
              horizontalAlignment: "LEFT",
              verticalAlignment: "MIDDLE",
            },
          },
          fields: "userEnteredFormat.backgroundColor,userEnteredFormat.textFormat,userEnteredFormat.horizontalAlignment,userEnteredFormat.verticalAlignment",
        },
      },
    );
  }

  requests.push({
    repeatCell: {
      range: gridRange(sheetId, 3, instructionEndRowIndex, 0, 2),
      cell: {
        userEnteredFormat: {
          textFormat: { bold: true },
        },
      },
      fields: "userEnteredFormat.textFormat.bold",
    },
  });

  [160, 160, 175, 175, 175, 175, 175, 175].forEach((width, index) => {
    requests.push({
      updateDimensionProperties: {
        range: { sheetId, dimension: "COLUMNS", startIndex: index, endIndex: index + 1 },
        properties: { pixelSize: width },
        fields: "pixelSize",
      },
    });
  });

  for (let index = 0; index < instructionEndRowIndex; index += 1) {
    let height = 52;
    if (index === 0) height = 38;
    if (blankRows.has(index)) height = 14;
    if (sectionRows.includes(index)) height = 32;
    if ([5, 10, 13, 16, 20, 24, 26, 30, 31, 34].includes(index)) height = 64;

    requests.push({
      updateDimensionProperties: {
        range: { sheetId, dimension: "ROWS", startIndex: index, endIndex: index + 1 },
        properties: { pixelSize: height },
        fields: "pixelSize",
      },
    });
  }

  return requests;
}

async function writeInstructionsTab() {
  const values = [
    ["Manual Package Editor - How to Use", "", "", "", "", "", "", ""],
    ["", "", "", "", "", "", "", ""],
    ["Quick workflow", "", "", "", "", "", "", ""],
    ["1. Open the editor", "", "Go to the Package Editor tab and use the slicers at the top to narrow by Advertiser, Package ID, Initiative, Channel, Campaign, and Site. You are filtering the real editable rows, not a copy.", "", "", "", "", ""],
    ["2. Find the package", "", "Use Package ID, Site, and Package Friendly Name first. Campaign and metadata fields are visible at the far right if you need more context. Package ID and hidden internal fields are locked so row identity and loader helpers do not get changed by accident.", "", "", "", "", ""],
    ["3. Edit the value", "", "Edit the visible field that needs to change. Flight Start/End and metadata corrections apply to the whole package. Delivered metric edits use Delivery Override Start/End. Planned Spend and Planned Impressions are full-flight only.", "", "", "", "", ""],
    ["4. Check markers", "", "Orange means the value is different from the current dashboard value. Purple means the value is already coming from a validated manual update. Red means the row needs fixing before it can load.", "", "", "", "", ""],
    ["5. Request refresh", "", "Check Request refresh on the Package Editor tab when edits are ready. This only notifies Gene to review or run the loader; it does not publish or write to the warehouse by itself.", "", "", "", "", ""],
    ["", "", "", "", "", "", "", ""],
    ["Adding a new package row", "", "", "", "", "", "", ""],
    ["When to add a row", "", "Add a row when the package does not already exist in the editor, or when you need a separate date range for delivered actuals, such as one specific week.", "", "", "", "", ""],
    ["1. Add or duplicate", "", "Insert a new row below the existing package list, or duplicate a similar fee/package row into the blank area below the current package list and replace the values. Do not type into hidden internal columns.", "", "", "", "", ""],
    ["2. Fill visible required fields", "", "Enter Package ID, Site, Package Friendly Name, Flight Start/End, Delivery Override Start/End, and at least one metric value. Flight dates describe the package; delivery override dates define the metric window.", "", "", "", "", ""],
    ["3. Fill metadata at the far right", "", "Complete Advertiser, Package Type, Channel, Campaign, Package Name, and GS Channel. These fields tell dashboards how to group and filter the new package.", "", "", "", "", ""],
    ["4. Respect planned-vs-delivered rules", "", "Planned Spend and Planned Impressions must use the full flight date range. Delivered actuals can use a partial date range.", "", "", "", "", ""],
    ["5. Request refresh", "", "Check Request refresh when the row is complete. If required fields are missing, the row turns red in the sheet and the loader blocks it instead of sending it into reporting.", "", "", "", "", ""],
    ["Common blockers", "", "Rows are blocked for missing Package ID, invalid dates, no changed metric/date value, duplicate active package/date/metric edits, or missing required metadata for a new package. Started new rows turn red when required fields are missing.", "", "", "", "", ""],
    ["Do not guess", "", "If you are not sure which metadata value belongs on a new package, pause and ask before requesting the update.", "", "", "", "", ""],
    ["", "", "", "", "", "", "", ""],
    ["Date range rules", "", "", "", "", "", "", ""],
    ["Delivered actuals", "", "Spend, Impressions, Clicks, Video Plays, and Video Completions can be edited for any date range. To correct one week only, add or duplicate a row, set Delivery Override Start/End to that week, and enter that week's replacement totals.", "", "", "", "", ""],
    ["Planned metrics", "", "Planned Spend and Planned Impressions are full-flight totals only. Do not use weekly or daily date ranges for planned values. Partial planned edits are blocked by the loader.", "", "", "", "", ""],
    ["", "", "", "", "", "", "", ""],
    ["Where the displayed data comes from", "", "", "", "", "", "", ""],
    ["Planned Values", "", "Planned spend, planned impressions, rates, and flight date information come from PRISMA.", "", "", "", "", ""],
    ["Delivered values", "", "Delivered spend, impressions, clicks, video plays, and video completions come from ad server, platform, or partner-specific First Party Data sheets.", "", "", "", "", ""],
    ["Package metadata", "", "Advertiser, campaign, channel, supplier, site, package name, initiative, and classification fields come from PRISMA.", "", "", "", "", ""],
    ["Current dashboard values", "", "The editor refreshes from the same combined reporting data used by the dashboards, so the visible values are the current reporting snapshot before manual edits.", "", "", "", "", ""],
    ["", "", "", "", "", "", "", ""],
    ["How the data model works", "", "", "", "", "", "", ""],
    ["One combined reporting layer", "", "The data model combines PRISMA planning and metadata with delivered reporting into one package/date reporting layer so dashboards can use consistent fields.", "", "", "", "", ""],
    ["Manual edits are compared", "", "When the loader runs, it compares the edited sheet values to the current dashboard snapshot and records only values that changed.", "", "", "", "", ""],
    ["Metadata vs metric edits", "", "Metadata edits apply to the whole package. Delivered metric edits become daily rows only across the selected Delivery Override Start/End dates, so other dates keep their normal delivery values.", "", "", "", "", ""],
    ["Manual values take priority", "", "Validated manual values are used first for the final reporting fields. Normal source data fills in wherever there is no manual edit.", "", "", "", "", ""],
    ["Where edits appear", "", "Approved edits flow into client dashboards and external partner reporting after the loader and downstream reporting refresh finish.", "", "", "", "", ""],
    ["", "", "", "", "", "", "", ""],
    ["What the colors mean", "", "", "", "", "", "", ""],
    ["White / blue cells", "", "Package identity and lookup context. Existing package IDs are locked; blank new rows below the current package list can be filled when adding a manual-only package.", "", "", "", "", ""],
    ["Yellow cells", "", "Actual delivery values and dates. These can be edited for the date range you are correcting.", "", "", "", "", ""],
    ["Green cells", "", "Planned flight totals. Edit only for full-flight planned corrections.", "", "", "", "", ""],
    ["Gray cells", "", "Package metadata. Edit these when a package-level metadata correction is needed, or fill them on new manual-only rows.", "", "", "", "", ""],
    ["Orange / purple / red cells", "", "Orange means an edited value differs from the current dashboard value. Purple means a value is already using a validated manual update. Red means a started new row is missing required fields, has invalid dates, or needs fixing before it can load.", "", "", "", "", ""],
    ["", "", "", "", "", "", "", ""],
    ["What not to edit", "", "", "", "", "", "", ""],
    ["Internal columns", "", "Baseline comparison columns are hidden because they are only used by the loader to detect changed cells.", "", "", "", "", ""],
    ["Existing metadata", "", "Metadata edits are allowed, but they apply to the full package. If you only mean to correct one week of delivery, change the delivery dates and metric cells instead.", "", "", "", "", ""],
    ["Filtered-out rows", "", "If a row disappears after using slicers, clear or adjust the slicer selections. The row has not been deleted.", "", "", "", "", ""],
  ];

  await sheetsFetch(`/values/${encodeURIComponent(`${INSTRUCTIONS_TAB_NAME}!A1:H80`)}:clear`, {
    method: "POST",
    body: JSON.stringify({}),
  });

  await sheetsFetch(`/values/${encodeURIComponent(`${INSTRUCTIONS_TAB_NAME}!A1:H${values.length}`)}?valueInputOption=USER_ENTERED`, {
    method: "PUT",
    body: JSON.stringify({ values }),
  });
}

const spreadsheet = await getSpreadsheet();
let sheet = spreadsheet.sheets.find((candidate) => candidate.properties.title === TAB_NAME);
if (!sheet) {
  throw new Error(`Could not find tab: ${TAB_NAME}`);
}
let instructionsSheet = await ensureInstructionsSheet(spreadsheet);
const existingDataEndRowIndex = await getExistingDataEndRowIndex();

await batchUpdate(buildRequests(sheet, existingDataEndRowIndex));
await writeInstructions();

const refreshedSpreadsheet = await getSpreadsheet();
instructionsSheet = refreshedSpreadsheet.sheets.find((candidate) => candidate.properties.title === INSTRUCTIONS_TAB_NAME);
await batchUpdate(buildInstructionsRequests(instructionsSheet));
await writeInstructionsTab();

console.log(`Configured ${TAB_NAME} UX and ${INSTRUCTIONS_TAB_NAME} instructions for ${SHEET_ID}.`);
