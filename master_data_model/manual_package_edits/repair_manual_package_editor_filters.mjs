#!/usr/bin/env node

/*
 * Repairs only filter and slicer ranges on the Manual Data Editor Package
 * Editor tab. It preserves existing slicers, slicer positions, slicer column
 * choices, and current basic-filter criteria; it only expands their ranges to
 * the current sheet geometry after visible/hidden column changes.
 */

import { execFileSync } from "node:child_process";

const SHEET_ID = process.env.MASTER_MANUAL_EDIT_SHEET_ID || "1WerhrbBMggzCwIUCOsOCV33aHygV96jt1HgqiYcUHZo";
const TAB_NAME = process.env.MASTER_MANUAL_EDIT_TAB || "Package Editor";
const AUTH_ACCOUNT = process.env.MASTER_MANUAL_EDIT_AUTH_EMAIL || "gene.tsenter@giantspoon.com";
const HEADER_ROW_INDEX = 3;
const EDITED_ROW_FILTER_HEADER = "Edited Row Filter";

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
  const spreadsheet = await sheetsFetch("?fields=sheets(properties(sheetId,title,gridProperties),basicFilter,slicers)");
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

function rangeFor(sheetId, endRowIndex, endColumnIndex) {
  return {
    sheetId,
    startRowIndex: HEADER_ROW_INDEX,
    endRowIndex,
    startColumnIndex: 0,
    endColumnIndex,
  };
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
  const endRowIndex = rowCount;
  const tableRange = rangeFor(sheetId, endRowIndex, columnCount);
  const requests = [];

  requests.push({
    setBasicFilter: {
      filter: {
        range: tableRange,
        criteria: sheet.basicFilter?.criteria || {},
        sortSpecs: sheet.basicFilter?.sortSpecs || [],
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

  for (const slicer of sheet.slicers || []) {
    const columnIndex = slicer.spec?.title === "Edited Rows"
      ? editedRowFilterColumnIndex
      : slicer.spec?.columnIndex;
    requests.push({
      addSlicer: {
        slicer: {
          spec: {
            ...slicer.spec,
            columnIndex,
            dataRange: tableRange,
          },
          position: slicer.position,
        },
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
    updatedSlicers: (sheet.slicers || []).map((slicer) => ({
      title: slicer.spec?.title,
      columnIndex: slicer.spec?.title === "Edited Rows" ? editedRowFilterColumnIndex : slicer.spec?.columnIndex,
    })),
    editedRowFilterColumnIndex,
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
