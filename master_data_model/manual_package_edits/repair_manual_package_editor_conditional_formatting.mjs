#!/usr/bin/env node

/*
 * Repairs only the conditional-formatting rules on the Manual Data Editor
 * Package Editor tab. This is intentionally narrower than
 * setup_manual_package_editor_sheet.mjs: it does not rebuild user formatting,
 * widths, slicers, protections, notes, or sheet values.
 */

import { execFileSync } from "node:child_process";

const SHEET_ID = process.env.MASTER_MANUAL_EDIT_SHEET_ID || "1p1aGAg8lMk7JvUKCJBKRj5rKQNNYL3iEKnl0kPHvZ7E";
const TAB_NAME = process.env.MASTER_MANUAL_EDIT_TAB || "Package Editor";
const AUTH_ACCOUNT = process.env.MASTER_MANUAL_EDIT_AUTH_EMAIL || "gene.tsenter@giantspoon.com";
const AUTH_CONFIG_BY_ACCOUNT = {
  "gene.tsenter@giantspoon.com": "/Users/eugenetsenter/.config/gcloud-giantspoon",
  "gene.tsenter@old.giantspoon.com": "/Users/eugenetsenter/.config/gcloud-old-giantspoon",
};
const AUTH_CONFIG = process.env.CLOUDSDK_CONFIG || process.env.MASTER_MANUAL_EDIT_GCLOUD_CONFIG || AUTH_CONFIG_BY_ACCOUNT[AUTH_ACCOUNT];

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
  "Benchmark KPI",
  "Benchmark Value",
  "Manually Edited?",
  "Manual Edit At",
  "Manual Edit By",
  "Manual Edit Published At",
  "Primary Row Data Source",
  "Validation Status",
  "Validation Reason",
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
  "Baseline Benchmark KPI",
  "Baseline Benchmark Value",
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
  "Manual Marker Benchmark KPI",
  "Manual Marker Benchmark Value",
  "Edited Row Filter",
];

const headerRowIndex = 3;
const dataStartRowIndex = 4;
const firstDataRow = dataStartRowIndex + 1;
const colIndex = Object.fromEntries(columns.map((name, index) => [name, index]));
const visibleColumnCount = colIndex["Baseline Flight Start Date"];
const markerNames = [
  "Flight Start Date", "Flight End Date", "Planned Spend", "Planned Impressions",
  "Spend", "Impressions", "Clicks", "Video Plays", "Video Completions", "Delivery Start Date", "Delivery End Date",
  "Advertiser", "Package Type", "Channel", "Campaign", "Initiative", "Supplier Code", "Supplier Name", "Package Name", "Package Friendly Name", "GS Channel",
  "Benchmark KPI", "Benchmark Value",
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
  { editedName: "Benchmark KPI", baselineName: "Baseline Benchmark KPI" },
  { editedName: "Benchmark Value", baselineName: "Baseline Benchmark Value" },
].map((pair) => ({
  editedIndex: colIndex[pair.editedName],
  baselineIndex: colIndex[pair.baselineName],
}));
const manualMarkerPairs = markerNames.map((name) => ({
  editedIndex: colIndex[name === "Delivery Start Date" ? "Delivery Override Start Date" : name === "Delivery End Date" ? "Delivery Override End Date" : name],
  markerIndex: colIndex[`Manual Marker ${name}`],
})).filter((pair) => pair.editedIndex !== undefined && pair.markerIndex !== undefined);

function color(red, green, blue) {
  return { red, green, blue };
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

function absoluteCell(headerName, sheetRowNumber) {
  const index = colIndex[headerName];
  if (index === undefined) {
    throw new Error(`Missing required header for formula: ${headerName}`);
  }
  return `$${columnLetter(index)}${sheetRowNumber}`;
}

function rowRange(startHeaderName, endHeaderName, sheetRowNumber) {
  return `${absoluteCell(startHeaderName, sheetRowNumber)}:${absoluteCell(endHeaderName, sheetRowNumber)}`;
}

function gridRange(sheetId, startRowIndex, endRowIndex, startColumnIndex, endColumnIndex) {
  return { sheetId, startRowIndex, endRowIndex, startColumnIndex, endColumnIndex };
}

function token() {
  if (!AUTH_CONFIG) {
    throw new Error(`No account-specific Google auth config is defined for ${AUTH_ACCOUNT}.`);
  }
  return execFileSync(
    "gcloud",
    ["auth", "application-default", "print-access-token"],
    {
      encoding: "utf8",
      env: {
        ...process.env,
        CLOUDSDK_CONFIG: AUTH_CONFIG,
      },
    },
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
  const spreadsheet = await sheetsFetch("?fields=sheets(properties(sheetId,title,gridProperties),conditionalFormats)");
  const sheet = spreadsheet.sheets.find((candidate) => candidate.properties.title === TAB_NAME);
  if (!sheet) {
    throw new Error(`Could not find tab: ${TAB_NAME}`);
  }
  return sheet;
}

async function getExistingDataEndRowIndex() {
  const response = await sheetsFetch(
    `/values/${encodeURIComponent(`${TAB_NAME}!A${dataStartRowIndex + 1}:Y`)}?valueRenderOption=UNFORMATTED_VALUE`,
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

function conditionalRule(sheetId, rowCount, startColumnIndex, endColumnIndex, formula, backgroundColor, textColor) {
  return {
    addConditionalFormatRule: {
      rule: {
        ranges: [gridRange(sheetId, dataStartRowIndex, rowCount, startColumnIndex, endColumnIndex)],
        booleanRule: {
          condition: {
            type: "CUSTOM_FORMULA",
            values: [{ userEnteredValue: formula }],
          },
          format: {
            backgroundColor,
            textFormat: {
              foregroundColor: textColor,
              bold: true,
            },
          },
        },
      },
      index: 0,
    },
  };
}

function buildConditionalFormatRequests(sheetId, rowCount, existingDataEndRowIndex) {
  const requests = [];

  for (const pair of manualMarkerPairs) {
    const editedColumn = columnLetter(pair.editedIndex);
    const markerColumn = columnLetter(pair.markerIndex);
    requests.push(conditionalRule(
      sheetId,
      rowCount,
      pair.editedIndex,
      pair.editedIndex + 1,
      `=AND($${markerColumn}${firstDataRow}=TRUE,NOT(ISBLANK(${editedColumn}${firstDataRow})))`,
      color(0.88, 0.83, 0.97),
      color(0.17, 0.10, 0.42),
    ));
  }

  for (const pair of editMarkerPairs) {
    const editedColumn = columnLetter(pair.editedIndex);
    const baselineColumn = columnLetter(pair.baselineIndex);
    const manualMarkerPair = manualMarkerPairs.find((markerPair) => markerPair.editedIndex === pair.editedIndex);
    const manualMarkerGuard = manualMarkerPair ? `$${columnLetter(manualMarkerPair.markerIndex)}${firstDataRow}<>TRUE,` : "";
    requests.push(conditionalRule(
      sheetId,
      rowCount,
      pair.editedIndex,
      pair.editedIndex + 1,
      `=AND(${manualMarkerGuard}NOT(ISBLANK(${editedColumn}${firstDataRow})),${editedColumn}${firstDataRow}<>$${baselineColumn}${firstDataRow})`,
      color(0.99, 0.84, 0.64),
      color(0.30, 0.16, 0.02),
    ));
  }

  requests.push(conditionalRule(
    sheetId,
    rowCount,
    0,
    visibleColumnCount,
    `=${absoluteCell("Validation Status", firstDataRow)}="blocked"`,
    color(1.00, 0.86, 0.84),
    color(0.43, 0.06, 0.04),
  ));

  const visibleRowRange = rowRange("Advertiser", "Validation Reason", firstDataRow);
  const metricRowRange = rowRange("Planned Spend", "Video Completions", firstDataRow);
  const newRowStartedFormula = `AND(${absoluteCell("Baseline Flight Start Date", firstDataRow)}="",COUNTA(${visibleRowRange})>0)`;
  const requiredHeaders = [
    "Advertiser",
    "Package ID",
    "Site",
    "Package Friendly Name",
    "Flight Start Date",
    "Flight End Date",
    "Delivery Override Start Date",
    "Delivery Override End Date",
    "Package Type",
    "Channel",
    "Campaign",
    "Package Name",
    "GS Channel",
  ];

  for (const headerName of requiredHeaders) {
    requests.push(conditionalRule(
      sheetId,
      rowCount,
      colIndex[headerName],
      colIndex[headerName] + 1,
      `=AND(${newRowStartedFormula},${absoluteCell(headerName, firstDataRow)}="")`,
      color(1.00, 0.74, 0.70),
      color(0.43, 0.06, 0.04),
    ));
  }

  requests.push(conditionalRule(
    sheetId,
    rowCount,
    colIndex["Planned Spend"],
    colIndex["Video Completions"] + 1,
    `=AND(${newRowStartedFormula},COUNTA(${metricRowRange})=0)`,
    color(1.00, 0.74, 0.70),
    color(0.43, 0.06, 0.04),
  ));

  requests.push(conditionalRule(
    sheetId,
    rowCount,
    colIndex["Flight Start Date"],
    colIndex["Flight End Date"] + 1,
    `=AND(${absoluteCell("Flight Start Date", firstDataRow)}<>"",${absoluteCell("Flight End Date", firstDataRow)}<>"",${absoluteCell("Flight Start Date", firstDataRow)}>${absoluteCell("Flight End Date", firstDataRow)})`,
    color(1.00, 0.74, 0.70),
    color(0.43, 0.06, 0.04),
  ));

  requests.push(conditionalRule(
    sheetId,
    rowCount,
    colIndex["Delivery Override Start Date"],
    colIndex["Delivery Override End Date"] + 1,
    `=AND(${absoluteCell("Delivery Override Start Date", firstDataRow)}<>"",${absoluteCell("Delivery Override End Date", firstDataRow)}<>"",${absoluteCell("Delivery Override Start Date", firstDataRow)}>${absoluteCell("Delivery Override End Date", firstDataRow)})`,
    color(1.00, 0.74, 0.70),
    color(0.43, 0.06, 0.04),
  ));

  if (existingDataEndRowIndex > dataStartRowIndex) {
    const plannedSpendMarkerColumn = columnLetter(colIndex["Manual Marker Planned Spend"]);
    const plannedImpressionsMarkerColumn = columnLetter(colIndex["Manual Marker Planned Impressions"]);
    requests.push({
      addConditionalFormatRule: {
        rule: {
          ranges: [gridRange(sheetId, dataStartRowIndex, existingDataEndRowIndex, colIndex["Planned Spend"], colIndex["Planned Impressions"] + 1)],
          booleanRule: {
            condition: {
              type: "CUSTOM_FORMULA",
              values: [{
                userEnteredValue: `=AND($${plannedSpendMarkerColumn}${firstDataRow}<>TRUE,$${plannedImpressionsMarkerColumn}${firstDataRow}<>TRUE,OR(${absoluteCell("Planned Spend", firstDataRow)}<>${absoluteCell("Baseline Planned Spend", firstDataRow)},${absoluteCell("Planned Impressions", firstDataRow)}<>${absoluteCell("Baseline Planned Impressions", firstDataRow)}),OR(${absoluteCell("Delivery Override Start Date", firstDataRow)}<>${absoluteCell("Flight Start Date", firstDataRow)},${absoluteCell("Delivery Override End Date", firstDataRow)}<>${absoluteCell("Flight End Date", firstDataRow)}))`,
              }],
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

  return requests;
}

async function main() {
  const sheet = await getSheet();
  const sheetId = sheet.properties.sheetId;
  const rowCount = sheet.properties.gridProperties.rowCount || 2366;
  const existingDataEndRowIndex = await getExistingDataEndRowIndex();
  const deleteRequests = (sheet.conditionalFormats || []).map(() => ({
    deleteConditionalFormatRule: { sheetId, index: 0 },
  }));
  const addRequests = buildConditionalFormatRequests(sheetId, rowCount, existingDataEndRowIndex);

  await sheetsFetch(":batchUpdate", {
    method: "POST",
    body: JSON.stringify({ requests: [...deleteRequests, ...addRequests] }),
  });

  console.log(JSON.stringify({
    sheet: TAB_NAME,
    deletedConditionalFormatRules: deleteRequests.length,
    addedConditionalFormatRules: addRequests.length,
    visibleColumnCount,
    firstBaselineColumn: columnLetter(visibleColumnCount),
    firstMarkerColumn: columnLetter(colIndex["Manual Marker Flight Start Date"]),
    existingDataEndRowIndex,
    rowCount,
  }, null, 2));
}

main().catch((error) => {
  console.error(error);
  process.exitCode = 1;
});
