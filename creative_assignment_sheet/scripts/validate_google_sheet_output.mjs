import fs from 'node:fs';
import path from 'node:path';
import { fileURLToPath } from 'node:url';
import { OUTPUT_RANGE, encodeRange, getAccessToken, sheetsFetch } from './sheets_api.mjs';
import { diffRows, hashRows, normalizeRows } from './hash_rows.mjs';

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const root = path.resolve(__dirname, '..');
const fixturePath = path.join(root, 'data', 'apollo_adswerve_output.json');
const defaultSheetInfoPath = path.join(root, 'data', 'new_google_sheet_version.json');

const fixture = JSON.parse(fs.readFileSync(fixturePath, 'utf8'));
const defaultSheet = fs.existsSync(defaultSheetInfoPath)
  ? JSON.parse(fs.readFileSync(defaultSheetInfoPath, 'utf8')).newSpreadsheetId
  : null;

const spreadsheetId = process.argv[2] ?? defaultSheet;
if (!spreadsheetId) {
  throw new Error('Pass a spreadsheet id or create data/new_google_sheet_version.json first.');
}

const token = getAccessToken();
const payload = await sheetsFetch(
  spreadsheetId,
  `/values/${encodeRange(OUTPUT_RANGE)}?valueRenderOption=FORMATTED_VALUE&dateTimeRenderOption=FORMATTED_STRING&majorDimension=ROWS`,
  { token },
);

const actualRows = normalizeRows(payload.values ?? [], fixture.columnCount);
const actualHash = hashRows(actualRows);
const expectedRows = normalizeRows(fixture.rows, fixture.columnCount);
const expectedHash = fixture.sha256;
const diffs = diffRows(expectedRows, actualRows, 25);

const result = {
  checkedAt: new Date().toISOString(),
  spreadsheetId,
  outputRange: OUTPUT_RANGE,
  expected: {
    rows: expectedRows.length,
    columns: fixture.columnCount,
    sha256: expectedHash,
  },
  actual: {
    rows: actualRows.length,
    columns: fixture.columnCount,
    sha256: actualHash,
  },
  passed: actualHash === expectedHash && diffs.length === 0,
  diffs,
};

const outPath = path.join(root, 'data', `google_sheet_validation_${spreadsheetId}.json`);
fs.writeFileSync(outPath, JSON.stringify(result, null, 2));
console.log(JSON.stringify(result, null, 2));

if (!result.passed) process.exit(1);
