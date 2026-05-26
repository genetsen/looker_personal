import fs from 'node:fs';
import path from 'node:path';
import { fileURLToPath } from 'node:url';
import {
  SOURCE_SPREADSHEET_ID,
  OUTPUT_RANGE,
  encodeRange,
  getAccessToken,
  sheetsFetch,
} from './sheets_api.mjs';
import { hashRows, normalizeRows } from './hash_rows.mjs';

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const root = path.resolve(__dirname, '..');
const dataDir = path.join(root, 'data');
fs.mkdirSync(dataDir, { recursive: true });

const token = getAccessToken();

async function readValues(range, valueRenderOption = 'FORMATTED_VALUE') {
  const payload = await sheetsFetch(
    SOURCE_SPREADSHEET_ID,
    `/values/${encodeRange(range)}?valueRenderOption=${valueRenderOption}&dateTimeRenderOption=FORMATTED_STRING&majorDimension=ROWS`,
    { token },
  );
  return payload.values ?? [];
}

const metadata = await sheetsFetch(
  SOURCE_SPREADSHEET_ID,
  '?includeGridData=false&fields=properties(title,locale,timeZone,autoRecalc),sheets(properties(sheetId,title,index,sheetType,gridProperties(rowCount,columnCount),hidden)),namedRanges(name,range)',
  { token },
);

const outputRows = normalizeRows(await readValues(OUTPUT_RANGE), 23);
const outputHash = hashRows(outputRows);

const sourceInputs = {
  prismaPreview: await readValues("'STEP 0 | Prisma - Manual Entry'!A1:FC40"),
  creativeDetails: await readValues("'STEP 1 | INPUT - Creative Details'!A1:I120"),
  assignmentMatrixPreview: await readValues("'STEP 2 |  Creative Assignment Matrix v2'!A1:W150"),
  utmBuilderPreview: await readValues("'AUTO | UTM Builder | INTERNAL v2'!A1:AW150"),
};

const fixture = {
  exportedAt: new Date().toISOString(),
  sourceSpreadsheetId: SOURCE_SPREADSHEET_ID,
  sourceSpreadsheetUrl: `https://docs.google.com/spreadsheets/d/${SOURCE_SPREADSHEET_ID}/edit`,
  outputRange: OUTPUT_RANGE,
  adswerveOutput: {
    rowCount: outputRows.length,
    columnCount: 23,
    sha256: outputHash,
    rows: outputRows,
  },
  sourceInputs,
  workbookMetadata: metadata,
};

fs.writeFileSync(path.join(dataDir, 'apollo_fixture.json'), JSON.stringify(fixture, null, 2));
fs.writeFileSync(
  path.join(dataDir, 'apollo_adswerve_output.json'),
  JSON.stringify(fixture.adswerveOutput, null, 2),
);

console.log(
  JSON.stringify(
    {
      fixture: 'data/apollo_fixture.json',
      output: 'data/apollo_adswerve_output.json',
      rows: outputRows.length,
      columns: 23,
      sha256: outputHash,
    },
    null,
    2,
  ),
);
