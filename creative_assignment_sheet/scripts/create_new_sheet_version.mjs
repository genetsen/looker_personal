import fs from 'node:fs';
import path from 'node:path';
import { fileURLToPath } from 'node:url';
import {
  SOURCE_SPREADSHEET_ID,
  driveFetch,
  getAccessToken,
  sheetsFetch,
} from './sheets_api.mjs';

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const root = path.resolve(__dirname, '..');
const outputPath = path.join(root, 'data', 'new_google_sheet_version.json');
const token = getAccessToken();

const title = `Creative Assignment Sheet | Manual Canonical Prototype | Apollo | ${new Date()
  .toISOString()
  .slice(0, 10)}`;

const copy = await driveFetch(
  `/files/${SOURCE_SPREADSHEET_ID}/copy?fields=id,name,webViewLink`,
  {
    method: 'POST',
    token,
    body: JSON.stringify({ name: title }),
  },
);

async function addSystemMapTab(spreadsheetId) {
  const addResponse = await sheetsFetch(spreadsheetId, ':batchUpdate', {
    method: 'POST',
    token,
    body: JSON.stringify({
      requests: [
        {
          addSheet: {
            properties: {
              title: 'System Map - Build Notes',
              index: 2,
              gridProperties: {
                rowCount: 80,
                columnCount: 4,
                frozenRowCount: 2,
                hideGridlines: true,
              },
              tabColor: { red: 0.19, green: 0.36, blue: 0.42 },
            },
          },
        },
      ],
    }),
  });
  const sheetId = addResponse.replies[0].addSheet.properties.sheetId;
  const rows = [
    ['System Map - Build Notes', '', '', ''],
    [
      'This net-new document preserves the Apollo source workbook output while documenting the manual-only canonical direction for the next version.',
      '',
      '',
      '',
    ],
    ['Constraint', 'Decision', 'Why It Matters', 'Next Refactor Target'],
    [
      'Prisma import is immutable',
      'Keep Step 0 as raw manual paste input',
      'Avoids breaking the trafficker workflow or requiring Prisma export changes',
      'Add a normalized manual-only Prisma helper layer downstream',
    ],
    [
      'Auto import is deprecated',
      'Treat hidden auto/BigQuery tabs as legacy residue unless proven active',
      'Prevents old template logic from being mistaken for current truth',
      'Remove active formulas that depend on deprecated auto tabs',
    ],
    [
      'Creative assignment is user-facing',
      'Keep the matrix interaction but normalize assignments behind it',
      'Users like selecting creatives by placement; systems need one row per placement/creative',
      'Create CANONICAL | Creative Assignments',
    ],
    [
      'UTM rules are client-specific',
      'Move client branches out of formulas and into config tables',
      'Copied workbooks should not carry Mass/Apollo-specific logic invisibly',
      'Create CONFIG | Client and CONFIG | UTM Rules',
    ],
    [
      'Output parity is mandatory',
      'Validate Adswerve output against the Apollo source before changing logic',
      'Protects trafficking output from silent regressions',
      'Add QA | Readiness Checks and automated parity checks',
    ],
    ['', '', '', ''],
    ['Recommended Layer', 'Current Tab/Area', 'Target State', 'Notes'],
    [
      'Raw input',
      'STEP 0 | Prisma - Manual Entry',
      'raw_prisma_import',
      'Do not change raw imported columns',
    ],
    [
      'Normalized Prisma',
      'AUTO | Prisma | Combined',
      'CANONICAL | Prisma Manual Normalized',
      'Should read from Step 0 only in a manual-only version',
    ],
    [
      'Creative inventory',
      'STEP 1 | INPUT - Creative Details',
      'creative_assets',
      'Add validation for type, URL, duplicate names',
    ],
    [
      'Assignments',
      'STEP 2 | Creative Assignment Matrix v2',
      'creative_assignments',
      'Keep UI matrix, generate normalized rows',
    ],
    [
      'UTMs',
      'AUTO | UTM Builder | INTERNAL v2',
      'utm_output',
      'Replace hardcoded client branches with config-driven rules',
    ],
    [
      'Trafficking export',
      'OUTPUT | Adswerve Doc | v1',
      'adswerve_export',
      'Final reviewed export surface',
    ],
    [
      'QA',
      'Not formalized',
      'QA | Readiness Checks',
      'Flag missing inputs, formula errors, stale client residue, external imports',
    ],
  ];

  const values = rows.map((row) => ({
    values: row.map((value) => ({
      userEnteredValue: { stringValue: value },
      userEnteredFormat: {
        wrapStrategy: 'WRAP',
        verticalAlignment: 'MIDDLE',
        padding: { top: 7, right: 8, bottom: 7, left: 8 },
      },
    })),
  }));

  await sheetsFetch(spreadsheetId, ':batchUpdate', {
    method: 'POST',
    token,
    body: JSON.stringify({
      requests: [
        {
          updateCells: {
            range: {
              sheetId,
              startRowIndex: 0,
              endRowIndex: rows.length,
              startColumnIndex: 0,
              endColumnIndex: 4,
            },
            rows: values,
            fields:
              'userEnteredValue,userEnteredFormat(wrapStrategy,verticalAlignment,padding)',
          },
        },
        {
          mergeCells: {
            range: {
              sheetId,
              startRowIndex: 0,
              endRowIndex: 1,
              startColumnIndex: 0,
              endColumnIndex: 4,
            },
            mergeType: 'MERGE_ALL',
          },
        },
        {
          mergeCells: {
            range: {
              sheetId,
              startRowIndex: 1,
              endRowIndex: 2,
              startColumnIndex: 0,
              endColumnIndex: 4,
            },
            mergeType: 'MERGE_ALL',
          },
        },
        {
          repeatCell: {
            range: { sheetId, startRowIndex: 0, endRowIndex: 1 },
            cell: {
              userEnteredFormat: {
                backgroundColor: { red: 0.08, green: 0.29, blue: 0.34 },
                textFormat: {
                  bold: true,
                  fontSize: 16,
                  foregroundColor: { red: 1, green: 1, blue: 1 },
                },
              },
            },
            fields: 'userEnteredFormat(backgroundColor,textFormat)',
          },
        },
        {
          repeatCell: {
            range: { sheetId, startRowIndex: 1, endRowIndex: 2 },
            cell: {
              userEnteredFormat: {
                backgroundColor: { red: 0.08, green: 0.29, blue: 0.34 },
                textFormat: {
                  italic: true,
                  foregroundColor: { red: 1, green: 1, blue: 1 },
                },
              },
            },
            fields: 'userEnteredFormat(backgroundColor,textFormat)',
          },
        },
        {
          repeatCell: {
            range: {
              sheetId,
              startRowIndex: 2,
              endRowIndex: 3,
              startColumnIndex: 0,
              endColumnIndex: 4,
            },
            cell: {
              userEnteredFormat: {
                backgroundColor: { red: 0.19, green: 0.36, blue: 0.42 },
                textFormat: {
                  bold: true,
                  foregroundColor: { red: 1, green: 1, blue: 1 },
                },
              },
            },
            fields: 'userEnteredFormat(backgroundColor,textFormat)',
          },
        },
        {
          repeatCell: {
            range: {
              sheetId,
              startRowIndex: 9,
              endRowIndex: 10,
              startColumnIndex: 0,
              endColumnIndex: 4,
            },
            cell: {
              userEnteredFormat: {
                backgroundColor: { red: 0.19, green: 0.36, blue: 0.42 },
                textFormat: {
                  bold: true,
                  foregroundColor: { red: 1, green: 1, blue: 1 },
                },
              },
            },
            fields: 'userEnteredFormat(backgroundColor,textFormat)',
          },
        },
        {
          updateDimensionProperties: {
            range: { sheetId, dimension: 'COLUMNS', startIndex: 0, endIndex: 4 },
            properties: { pixelSize: 240 },
            fields: 'pixelSize',
          },
        },
        {
          updateDimensionProperties: {
            range: { sheetId, dimension: 'ROWS', startIndex: 0, endIndex: rows.length },
            properties: { pixelSize: 42 },
            fields: 'pixelSize',
          },
        },
      ],
    }),
  });

  return sheetId;
}

const systemMapSheetId = await addSystemMapTab(copy.id);

const result = {
  createdAt: new Date().toISOString(),
  sourceSpreadsheetId: SOURCE_SPREADSHEET_ID,
  newSpreadsheetId: copy.id,
  name: copy.name,
  url: copy.webViewLink ?? `https://docs.google.com/spreadsheets/d/${copy.id}/edit`,
  systemMapSheetId,
};

fs.writeFileSync(outputPath, JSON.stringify(result, null, 2));
console.log(JSON.stringify(result, null, 2));
