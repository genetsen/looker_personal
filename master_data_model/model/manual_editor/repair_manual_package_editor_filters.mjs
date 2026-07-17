#!/usr/bin/env node

/*
 * Repairs the lightweight sheet geometry that can drift after the Manual Data
 * Editor loader writes a new header shape. It preserves existing slicers,
 * slicer positions, slicer column choices, current basic-filter criteria,
 * formatting, and widths; it only updates filter ranges, removes slicers, column
 * visibility, the protected ranges affected by visible/hidden column changes,
 * and the system-owned conditional-format rules that depend on column letters.
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
const IN_PROCESS_ACCESS_TOKEN = process.env.MASTER_MANUAL_EDIT_ACCESS_TOKEN;
const HEADER_ROW_INDEX = 3;
const DATA_START_ROW_INDEX = HEADER_ROW_INDEX + 1;
const EDITED_ROW_FILTER_HEADER = "Edited Row Filter";
const BASELINE_START_HEADER = "Baseline Flight Start Date";
const AUDIT_STATUS_START_HEADER = "Manually Edited?";
const EXISTING_PACKAGE_IDENTITY_PROTECTION_DESCRIPTION = "Manual editor UX: lock existing package identity columns";
const HIDDEN_BASELINE_PROTECTION_DESCRIPTION = "Manual editor UX: lock hidden baseline columns";
const AUDIT_STATUS_PROTECTION_DESCRIPTION = "Manual editor UX: lock audit and status columns";
const EDIT_MARKER_PAIRS = [
  ["Flight Start Date", "Baseline Flight Start Date"],
  ["Flight End Date", "Baseline Flight End Date"],
  ["Delivery Override Start Date", "Baseline Delivery Start Date"],
  ["Delivery Override End Date", "Baseline Delivery End Date"],
  ["Spend", "Baseline Spend"],
  ["Impressions", "Baseline Impressions"],
  ["Planned Spend", "Baseline Planned Spend"],
  ["Planned Impressions", "Baseline Planned Impressions"],
  ["Clicks", "Baseline Clicks"],
  ["Video Plays", "Baseline Video Plays"],
  ["Video Completions", "Baseline Video Completions"],
  ["Advertiser", "Baseline Advertiser"],
  ["Package Type", "Baseline Package Type"],
  ["Channel", "Baseline Channel"],
  ["Campaign", "Baseline Campaign"],
  ["Initiative", "Baseline Initiative"],
  ["Supplier Code", "Baseline Supplier Code"],
  ["Supplier Name", "Baseline Supplier Name"],
  ["Package Name", "Baseline Package Name"],
  ["Package Friendly Name", "Baseline Package Friendly Name"],
  ["GS Channel", "Baseline GS Channel"],
  ["Benchmark KPI", "Baseline Benchmark KPI"],
  ["Benchmark Value", "Baseline Benchmark Value"],
];
const MARKER_NAMES = [
  "Flight Start Date",
  "Flight End Date",
  "Planned Spend",
  "Planned Impressions",
  "Spend",
  "Impressions",
  "Clicks",
  "Video Plays",
  "Video Completions",
  "Delivery Start Date",
  "Delivery End Date",
  "Advertiser",
  "Package Type",
  "Channel",
  "Campaign",
  "Initiative",
  "Supplier Code",
  "Supplier Name",
  "Package Name",
  "Package Friendly Name",
  "GS Channel",
  "Benchmark KPI",
  "Benchmark Value",
];

function token() {
  if (IN_PROCESS_ACCESS_TOKEN) {
    return IN_PROCESS_ACCESS_TOKEN;
  }
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
  const spreadsheet = await sheetsFetch("?fields=sheets(properties(sheetId,title,gridProperties),basicFilter,slicers,protectedRanges(protectedRangeId,description,range),conditionalFormats)");
  const sheet = spreadsheet.sheets.find((candidate) => candidate.properties.title === TAB_NAME);
  if (!sheet) {
    throw new Error(`Could not find tab: ${TAB_NAME}`);
  }
  return sheet;
}

async function getHeaderMap() {
  const response = await sheetsFetch(
    `/values/${encodeURIComponent(`${TAB_NAME}!A${HEADER_ROW_INDEX + 1}:ZZ${HEADER_ROW_INDEX + 1}`)}?valueRenderOption=UNFORMATTED_VALUE`,
  );
  const headers = response.values?.[0] || [];
  return Object.fromEntries(headers.map((header, index) => [String(header).trim(), index]));
}

async function getExistingDataEndRowIndex() {
  const response = await sheetsFetch(
    `/values/${encodeURIComponent(`${TAB_NAME}!A${DATA_START_ROW_INDEX + 1}:Y`)}?valueRenderOption=UNFORMATTED_VALUE`,
  );
  const rows = response.values || [];
  let lastNonEmptyRowOffset = -1;

  rows.forEach((row, index) => {
    if (row.some((value) => value !== null && value !== undefined && String(value).trim() !== "")) {
      lastNonEmptyRowOffset = index;
    }
  });

  return lastNonEmptyRowOffset >= 0
    ? DATA_START_ROW_INDEX + lastNonEmptyRowOffset + 1
    : DATA_START_ROW_INDEX;
}

function rangeFor(sheetId, endRowIndex, endColumnIndex) {
  return {
    sheetId,
    startRowIndex: HEADER_ROW_INDEX,
    endRowIndex,
    startColumnIndex: 0,
    endColumnIndex,
  };
}

function dataColumnRange(sheetId, endRowIndex, startColumnIndex, endColumnIndex) {
  return {
    sheetId,
    startRowIndex: DATA_START_ROW_INDEX,
    endRowIndex,
    startColumnIndex,
    endColumnIndex,
  };
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

function absoluteCell(headerMap, headerName, sheetRowNumber) {
  const index = headerMap[headerName];
  if (index === undefined) {
    throw new Error(`Missing required header for formula: ${headerName}`);
  }
  return `$${columnLetter(index)}${sheetRowNumber}`;
}

function rowRange(headerMap, startHeaderName, endHeaderName, sheetRowNumber) {
  return `${absoluteCell(headerMap, startHeaderName, sheetRowNumber)}:${absoluteCell(headerMap, endHeaderName, sheetRowNumber)}`;
}

function color(red, green, blue) {
  return { red, green, blue };
}

function buildProtectedRangeRequests(sheet, description, desiredRange) {
  const matchingRanges = (sheet.protectedRanges || [])
    .filter((protectedRange) => protectedRange.description === description);

  if (matchingRanges.length === 0) {
    return [{
      addProtectedRange: {
        protectedRange: {
          range: desiredRange,
          description,
          warningOnly: false,
        },
      },
    }];
  }

  return matchingRanges.map((protectedRange) => ({
    updateProtectedRange: {
      protectedRange: {
        protectedRangeId: protectedRange.protectedRangeId,
        range: desiredRange,
      },
      fields: "range",
    },
  }));
}

function buildConditionalFormatRequests(sheet, sheetId, headerMap, rowCount, visibleColumnCount) {
  const requests = [];
  for (let index = (sheet.conditionalFormats || []).length - 1; index >= 0; index -= 1) {
    requests.push({
      deleteConditionalFormatRule: {
        sheetId,
        index,
      },
    });
  }

  const firstDataRow = DATA_START_ROW_INDEX + 1;
  const purpleFormat = {
    backgroundColor: color(0.88, 0.83, 0.97),
    textFormat: { foregroundColor: color(0.17, 0.10, 0.42), bold: true },
  };
  const orangeFormat = {
    backgroundColor: color(0.99, 0.84, 0.64),
    textFormat: { foregroundColor: color(0.30, 0.16, 0.02), bold: true },
  };
  const redFormat = {
    backgroundColor: color(1.00, 0.86, 0.84),
    textFormat: { foregroundColor: color(0.43, 0.06, 0.04), bold: true },
  };
  const strongRedFormat = {
    backgroundColor: color(1.00, 0.74, 0.70),
    textFormat: { foregroundColor: color(0.43, 0.06, 0.04), bold: true },
  };
  const addRule = (ruleRange, formula, format) => {
    requests.push({
      addConditionalFormatRule: {
        rule: {
          ranges: [ruleRange],
          booleanRule: {
            condition: {
              type: "CUSTOM_FORMULA",
              values: [{ userEnteredValue: formula }],
            },
            format,
          },
        },
        index: 0,
      },
    });
  };

  const manualMarkerPairs = MARKER_NAMES.map((name) => {
    const editedName = name === "Delivery Start Date"
      ? "Delivery Override Start Date"
      : name === "Delivery End Date"
        ? "Delivery Override End Date"
        : name;
    return {
      editedIndex: headerMap[editedName],
      markerIndex: headerMap[`Manual Marker ${name}`],
    };
  }).filter((pair) => pair.editedIndex !== undefined && pair.markerIndex !== undefined);

  for (const pair of manualMarkerPairs) {
    const editedColumn = columnLetter(pair.editedIndex);
    const markerColumn = columnLetter(pair.markerIndex);
    addRule(
      dataColumnRange(sheetId, rowCount, pair.editedIndex, pair.editedIndex + 1),
      `=AND($${markerColumn}${firstDataRow}=TRUE,NOT(ISBLANK(${editedColumn}${firstDataRow})))`,
      purpleFormat,
    );
  }

  for (const [editedName, baselineName] of EDIT_MARKER_PAIRS) {
    const editedIndex = headerMap[editedName];
    const baselineIndex = headerMap[baselineName];
    if (editedIndex === undefined || baselineIndex === undefined) {
      throw new Error(`Missing conditional-format header pair: ${editedName} / ${baselineName}`);
    }
    const markerPair = manualMarkerPairs.find((pair) => pair.editedIndex === editedIndex);
    const manualMarkerGuard = markerPair
      ? `$${columnLetter(markerPair.markerIndex)}${firstDataRow}<>TRUE,`
      : "";
    addRule(
      dataColumnRange(sheetId, rowCount, editedIndex, editedIndex + 1),
      `=AND(${manualMarkerGuard}NOT(ISBLANK(${columnLetter(editedIndex)}${firstDataRow})),${columnLetter(editedIndex)}${firstDataRow}<>$${columnLetter(baselineIndex)}${firstDataRow})`,
      orangeFormat,
    );
  }

  const validationStatusColumn = columnLetter(headerMap["Validation Status"]);
  const baselineFlightStartColumn = columnLetter(headerMap["Baseline Flight Start Date"]);
  const visibleRowRange = rowRange(headerMap, "Advertiser", "Validation Reason", firstDataRow);
  const metricRowRange = rowRange(headerMap, "Planned Spend", "Video Completions", firstDataRow);
  const newRowStartedFormula = `AND($${baselineFlightStartColumn}${firstDataRow}="",COUNTA(${visibleRowRange})>0)`;
  addRule(
    dataColumnRange(sheetId, rowCount, 0, visibleColumnCount),
    `=$${validationStatusColumn}${firstDataRow}="blocked"`,
    redFormat,
  );

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
    const index = headerMap[headerName];
    if (index === undefined) {
      throw new Error(`Missing required new-row header: ${headerName}`);
    }
    addRule(
      dataColumnRange(sheetId, rowCount, index, index + 1),
      `=AND(${newRowStartedFormula},${absoluteCell(headerMap, headerName, firstDataRow)}="")`,
      strongRedFormat,
    );
  }

  addRule(
    dataColumnRange(sheetId, rowCount, headerMap["Planned Spend"], headerMap["Video Completions"] + 1),
    `=AND(${newRowStartedFormula},COUNTA(${metricRowRange})=0)`,
    strongRedFormat,
  );

  addRule(
    dataColumnRange(sheetId, rowCount, headerMap["Flight Start Date"], headerMap["Flight End Date"] + 1),
    `=AND(${absoluteCell(headerMap, "Flight Start Date", firstDataRow)}<>"",${absoluteCell(headerMap, "Flight End Date", firstDataRow)}<>"",${absoluteCell(headerMap, "Flight Start Date", firstDataRow)}>${absoluteCell(headerMap, "Flight End Date", firstDataRow)})`,
    strongRedFormat,
  );

  addRule(
    dataColumnRange(sheetId, rowCount, headerMap["Delivery Override Start Date"], headerMap["Delivery Override End Date"] + 1),
    `=AND(${absoluteCell(headerMap, "Delivery Override Start Date", firstDataRow)}<>"",${absoluteCell(headerMap, "Delivery Override End Date", firstDataRow)}<>"",${absoluteCell(headerMap, "Delivery Override Start Date", firstDataRow)}>${absoluteCell(headerMap, "Delivery Override End Date", firstDataRow)})`,
    strongRedFormat,
  );

  addRule(
    dataColumnRange(sheetId, rowCount, headerMap["Planned Spend"], headerMap["Planned Impressions"] + 1),
    `=AND($${columnLetter(headerMap["Manual Marker Planned Spend"])}${firstDataRow}<>TRUE,$${columnLetter(headerMap["Manual Marker Planned Impressions"])}${firstDataRow}<>TRUE,OR(${absoluteCell(headerMap, "Planned Spend", firstDataRow)}<>${absoluteCell(headerMap, "Baseline Planned Spend", firstDataRow)},${absoluteCell(headerMap, "Planned Impressions", firstDataRow)}<>${absoluteCell(headerMap, "Baseline Planned Impressions", firstDataRow)}),OR(${absoluteCell(headerMap, "Delivery Override Start Date", firstDataRow)}<>${absoluteCell(headerMap, "Flight Start Date", firstDataRow)},${absoluteCell(headerMap, "Delivery Override End Date", firstDataRow)}<>${absoluteCell(headerMap, "Flight End Date", firstDataRow)}))`,
    strongRedFormat,
  );

  return requests;
}

async function main() {
  const sheet = await getSheet();
  const sheetId = sheet.properties.sheetId;
  const columnCount = sheet.properties.gridProperties.columnCount;
  const rowCount = sheet.properties.gridProperties.rowCount;
  const headerMap = await getHeaderMap();
  const editedRowFilterColumnIndex = headerMap[EDITED_ROW_FILTER_HEADER];
  if (editedRowFilterColumnIndex === undefined) {
    throw new Error(`Missing required header: ${EDITED_ROW_FILTER_HEADER}`);
  }
  const visibleColumnCount = headerMap[BASELINE_START_HEADER];
  if (visibleColumnCount === undefined) {
    throw new Error(`Missing required header: ${BASELINE_START_HEADER}`);
  }
  const auditStatusStartColumnIndex = headerMap[AUDIT_STATUS_START_HEADER];
  if (auditStatusStartColumnIndex === undefined) {
    throw new Error(`Missing required header: ${AUDIT_STATUS_START_HEADER}`);
  }
  const endRowIndex = rowCount;
  const existingDataEndRowIndex = await getExistingDataEndRowIndex();
  const tableRange = rangeFor(sheetId, endRowIndex, columnCount);
  const requests = [];

  requests.push(
    {
      updateDimensionProperties: {
        range: {
          sheetId,
          dimension: "COLUMNS",
          startIndex: 0,
          endIndex: visibleColumnCount,
        },
        properties: { hiddenByUser: false },
        fields: "hiddenByUser",
      },
    },
    {
      updateDimensionProperties: {
        range: {
          sheetId,
          dimension: "COLUMNS",
          startIndex: visibleColumnCount,
          endIndex: columnCount,
        },
        properties: { hiddenByUser: true },
        fields: "hiddenByUser",
      },
    },
  );

  requests.push(
    ...buildProtectedRangeRequests(
      sheet,
      EXISTING_PACKAGE_IDENTITY_PROTECTION_DESCRIPTION,
      dataColumnRange(sheetId, existingDataEndRowIndex, headerMap["Package ID"], headerMap["Site"] + 1),
    ),
    ...buildProtectedRangeRequests(
      sheet,
      HIDDEN_BASELINE_PROTECTION_DESCRIPTION,
      dataColumnRange(sheetId, endRowIndex, visibleColumnCount, columnCount),
    ),
    ...buildProtectedRangeRequests(
      sheet,
      AUDIT_STATUS_PROTECTION_DESCRIPTION,
      dataColumnRange(sheetId, endRowIndex, auditStatusStartColumnIndex, visibleColumnCount),
    ),
  );
  requests.push(...buildConditionalFormatRequests(sheet, sheetId, headerMap, endRowIndex, visibleColumnCount));

  requests.push({
    repeatCell: {
      range: {
        sheetId,
        startRowIndex: HEADER_ROW_INDEX,
        endRowIndex: HEADER_ROW_INDEX + 1,
        startColumnIndex: 0,
        endColumnIndex: 1,
      },
      cell: {
        userEnteredFormat: {
          backgroundColor: color(0, 0, 0),
          textFormat: { foregroundColor: color(1, 1, 1), bold: true },
        },
      },
      fields: "userEnteredFormat.backgroundColor,userEnteredFormat.textFormat.foregroundColor,userEnteredFormat.textFormat.bold",
    },
  });

  requests.push({
    setBasicFilter: {
      filter: {
        range: tableRange,
        criteria: {},
        sortSpecs: [],
      },
    },
  });

  for (const slicer of sheet.slicers || []) {
    requests.push({
      deleteEmbeddedObject: {
        objectId: slicer.slicerId,
      },
    });
  }

  await sheetsFetch(":batchUpdate", {
    method: "POST",
    body: JSON.stringify({ requests }),
  });

  console.log(JSON.stringify({
    sheet: TAB_NAME,
    updatedBasicFilter: true,
    removedSlicers: (sheet.slicers || []).map((slicer) => slicer.spec?.title || slicer.slicerId),
    editedRowFilterColumnIndex,
    visibleColumnCount,
    auditStatusStartColumnIndex,
    existingDataEndRowIndex,
    updatedColumnVisibility: true,
    updatedProtectedRanges: [
      HIDDEN_BASELINE_PROTECTION_DESCRIPTION,
      AUDIT_STATUS_PROTECTION_DESCRIPTION,
    ],
    rebuiltConditionalFormatRules: true,
    startRowIndex: tableRange.startRowIndex,
    endRowIndex: tableRange.endRowIndex,
    startColumnIndex: tableRange.startColumnIndex,
    endColumnIndex: tableRange.endColumnIndex,
  }, null, 2));
}

main().catch((error) => {
  console.error(error);
  process.exitCode = 1;
});
