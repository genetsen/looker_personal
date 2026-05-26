import fs from 'node:fs';
import path from 'node:path';
import { fileURLToPath } from 'node:url';
import { encodeRange, getAccessToken, sheetsFetch } from './sheets_api.mjs';

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const root = path.resolve(__dirname, '..');
const dataDir = path.join(root, 'data');
fs.mkdirSync(dataDir, { recursive: true });

const workbooks = [
  {
    key: 'apollo',
    label: 'Apollo',
    spreadsheetId: '1U46ZJ4U6XCLTXqtNXlun7uY0L1iyG88RZ6HMiOeACyU',
  },
  {
    key: 'variant_129tqk',
    label: 'Variant 1',
    spreadsheetId: '129tqkDAhmeUXQ0WVb4w0YHJ5DGIPMuL8fGRHOfH8mD8',
  },
  {
    key: 'variant_1wi9',
    label: 'Variant 2',
    spreadsheetId: '1WI9RlZ8qkgDs2TQDTLHkN55jWTZ3VOOba1fCTcFwZsQ',
  },
  {
    key: 'variant_1boq',
    label: 'Variant 3',
    spreadsheetId: '1boQDy5nSQDaheIBcXJZTuhbdNSBOVtVFb8xtIuAGyjA',
  },
];

const semanticRanges = [
  {
    key: 'directions',
    labels: ['Directions'],
    rangeA1: 'A1:K120',
  },
  {
    key: 'updatedDirections',
    labels: ['Directions - Updated'],
    rangeA1: 'A1:K120',
  },
  {
    key: 'prismaInput',
    labels: ['STEP 0 | Prisma - Manual Entry', '1 | INPUT - Prisma'],
    rangeA1: 'A1:FC120',
  },
  {
    key: 'creativeDetails',
    labels: ['STEP 1 | INPUT - Creative Details', '2 | INPUT - Creative Details'],
    rangeA1: 'A1:AZ200',
  },
  {
    key: 'assignmentMatrix',
    labels: ['STEP 2 |  Creative Assignment Matrix v2', '3 |  Creative Assignment Matrix v2'],
    rangeA1: 'A1:BC200',
  },
  {
    key: 'utmBuilder',
    labels: ['AUTO | UTM Builder | INTERNAL v2', ' AUTO | UTM Builder | INTERNAL | v2'],
    rangeA1: 'A1:AW200',
  },
  {
    key: 'adswerveProcessing',
    labels: ['AUTO | Adswerve doc - Processing | INTERNAL '],
    rangeA1: 'A1:Z200',
  },
  {
    key: 'adswerveOutput',
    labels: ['OUTPUT | Adswerve Doc | v1'],
    rangeA1: 'A1:W200',
  },
  {
    key: 'manualUtmBuilder',
    labels: ['MANUAL UTM BUILDER'],
    rangeA1: 'A1:W200',
  },
  {
    key: 'utmTaxonomy',
    labels: ['UTM_taxonomy', 'manual utm builder validations'],
    rangeA1: 'A1:AI200',
  },
  {
    key: 'urls',
    labels: ['URLs', 'All_UTMs', 'Unique values'],
    rangeA1: 'A1:AR200',
  },
];

const token = getAccessToken();

async function readMetadata(spreadsheetId) {
  return sheetsFetch(
    spreadsheetId,
    '?includeGridData=false&fields=properties(title,locale,timeZone,autoRecalc),sheets(properties(sheetId,title,index,sheetType,gridProperties(rowCount,columnCount,frozenRowCount,frozenColumnCount),hidden,tabColor)),namedRanges(name,range)',
    { token },
  );
}

async function readRange(spreadsheetId, range, valueRenderOption) {
  try {
    const payload = await sheetsFetch(
      spreadsheetId,
      `/values/${encodeRange(range)}?valueRenderOption=${valueRenderOption}&dateTimeRenderOption=FORMATTED_STRING&majorDimension=ROWS`,
      { token },
    );
    return payload.values ?? [];
  } catch (error) {
    return { error: error.message };
  }
}

function padRows(rows) {
  if (!Array.isArray(rows)) return rows;
  const width = Math.max(0, ...rows.map((row) => row.length));
  return rows.map((row) => [...row, ...Array(Math.max(0, width - row.length)).fill('')]);
}

function nonEmptyRows(rows) {
  if (!Array.isArray(rows)) return [];
  return padRows(rows).filter((row) => row.some((cell) => String(cell ?? '').trim() !== ''));
}

function rowSignature(row) {
  return row.map((cell) => String(cell ?? '').trim()).join('\u241f');
}

function extractFormulaInventory(formulasByRange) {
  const inventory = [];
  for (const [range, rows] of Object.entries(formulasByRange)) {
    if (!Array.isArray(rows)) continue;
    rows.forEach((row, rowIndex) => {
      row.forEach((cell, colIndex) => {
        if (typeof cell === 'string' && cell.startsWith('=')) {
          inventory.push({
            range,
            rowOffset: rowIndex + 1,
            colOffset: colIndex + 1,
            formula: cell,
          });
        }
      });
    });
  }
  return inventory;
}

function tabSummary(metadata) {
  return (metadata.sheets ?? []).map((sheet) => ({
    title: sheet.properties.title,
    sheetId: sheet.properties.sheetId,
    index: sheet.properties.index,
    hidden: Boolean(sheet.properties.hidden),
    rows: sheet.properties.gridProperties?.rowCount ?? null,
    columns: sheet.properties.gridProperties?.columnCount ?? null,
  }));
}

function quoteSheetName(title) {
  return `'${title.replaceAll("'", "''")}'`;
}

function resolveSemanticRange(tabs, semanticRange) {
  const titles = new Set(tabs.map((tab) => tab.title));
  const title = semanticRange.labels.find((label) => titles.has(label));
  if (!title) return null;
  return {
    key: semanticRange.key,
    title,
    range: `${quoteSheetName(title)}!${semanticRange.rangeA1}`,
  };
}

function compareToApollo(apollo, workbook) {
  const apolloTabs = new Map(apollo.tabs.map((tab) => [tab.title, tab]));
  const workbookTabs = new Map(workbook.tabs.map((tab) => [tab.title, tab]));

  const tabDiff = {
    missingVsApollo: [...apolloTabs.keys()].filter((title) => !workbookTabs.has(title)),
    addedVsApollo: [...workbookTabs.keys()].filter((title) => !apolloTabs.has(title)),
    dimensionChanges: [...workbookTabs.values()]
      .filter((tab) => apolloTabs.has(tab.title))
      .map((tab) => {
        const base = apolloTabs.get(tab.title);
        return {
          title: tab.title,
          apollo: { rows: base.rows, columns: base.columns, hidden: base.hidden },
          variant: { rows: tab.rows, columns: tab.columns, hidden: tab.hidden },
        };
      })
      .filter(
        (item) =>
          item.apollo.rows !== item.variant.rows ||
          item.apollo.columns !== item.variant.columns ||
          item.apollo.hidden !== item.variant.hidden,
      ),
  };

  const semanticDiff = {};
  for (const key of Object.keys(workbook.formattedValuesBySemanticKey)) {
    const baseRows = nonEmptyRows(apollo.formattedValuesBySemanticKey[key]?.values);
    const variantRows = nonEmptyRows(workbook.formattedValuesBySemanticKey[key]?.values);
    const baseSet = new Set(baseRows.map(rowSignature));
    const variantSet = new Set(variantRows.map(rowSignature));
    semanticDiff[key] = {
      apolloTab: apollo.formattedValuesBySemanticKey[key]?.title ?? null,
      variantTab: workbook.formattedValuesBySemanticKey[key]?.title ?? null,
      apolloNonEmptyRows: baseRows.length,
      variantNonEmptyRows: variantRows.length,
      rowsOnlyInVariantSample: variantRows
        .filter((row) => !baseSet.has(rowSignature(row)))
        .slice(0, 12),
      rowsOnlyInApolloSample: baseRows
        .filter((row) => !variantSet.has(rowSignature(row)))
        .slice(0, 12),
    };
  }

  const formulaDiff = {};
  for (const key of Object.keys(workbook.formulasBySemanticKey)) {
    const baseRows = nonEmptyRows(apollo.formulasBySemanticKey[key]?.values);
    const variantRows = nonEmptyRows(workbook.formulasBySemanticKey[key]?.values);
    const baseSet = new Set(baseRows.map(rowSignature));
    const variantSet = new Set(variantRows.map(rowSignature));
    formulaDiff[key] = {
      apolloTab: apollo.formulasBySemanticKey[key]?.title ?? null,
      variantTab: workbook.formulasBySemanticKey[key]?.title ?? null,
      apolloFormulaRows: baseRows.length,
      variantFormulaRows: variantRows.length,
      formulaRowsOnlyInVariantSample: variantRows
        .filter((row) => !baseSet.has(rowSignature(row)))
        .slice(0, 12),
      formulaRowsOnlyInApolloSample: baseRows
        .filter((row) => !variantSet.has(rowSignature(row)))
        .slice(0, 12),
    };
  }

  return { tabDiff, semanticDiff, formulaDiff };
}

const workbookSnapshots = {};

for (const workbook of workbooks) {
  const metadata = await readMetadata(workbook.spreadsheetId);
  const tabs = tabSummary(metadata);
  const formattedValuesBySemanticKey = {};
  const formulasBySemanticKey = {};

  for (const semanticRange of semanticRanges) {
    const resolved = resolveSemanticRange(tabs, semanticRange);
    if (!resolved) {
      formattedValuesBySemanticKey[semanticRange.key] = { title: null, range: null, values: [] };
      formulasBySemanticKey[semanticRange.key] = { title: null, range: null, values: [] };
      continue;
    }

    formattedValuesBySemanticKey[semanticRange.key] = {
      title: resolved.title,
      range: resolved.range,
      values: await readRange(
      workbook.spreadsheetId,
      resolved.range,
      'FORMATTED_VALUE',
      ),
    };
    formulasBySemanticKey[semanticRange.key] = {
      title: resolved.title,
      range: resolved.range,
      values: await readRange(workbook.spreadsheetId, resolved.range, 'FORMULA'),
    };
  }

  workbookSnapshots[workbook.key] = {
    ...workbook,
    url: `https://docs.google.com/spreadsheets/d/${workbook.spreadsheetId}/edit`,
    title: metadata.properties.title,
    locale: metadata.properties.locale,
    timeZone: metadata.properties.timeZone,
    tabs,
    namedRanges: metadata.namedRanges ?? [],
    formattedValuesBySemanticKey,
    formulasBySemanticKey,
    formulaInventorySample: extractFormulaInventory(
      Object.fromEntries(
        Object.entries(formulasBySemanticKey).map(([key, entry]) => [
          `${key}:${entry.range ?? 'missing'}`,
          entry.values,
        ]),
      ),
    ).slice(0, 200),
  };
}

const apollo = workbookSnapshots.apollo;
const comparisons = Object.fromEntries(
  Object.entries(workbookSnapshots)
    .filter(([key]) => key !== 'apollo')
    .map(([key, workbook]) => [key, compareToApollo(apollo, workbook)]),
);

const report = {
  comparedAt: new Date().toISOString(),
  workbooks: workbookSnapshots,
  comparisons,
};

const outPath = path.join(dataDir, 'sheet_variant_comparison.json');
fs.writeFileSync(outPath, JSON.stringify(report, null, 2));

console.log(
  JSON.stringify(
    {
      output: path.relative(root, outPath),
      workbooks: Object.fromEntries(
        Object.entries(workbookSnapshots).map(([key, workbook]) => [
          key,
          {
            title: workbook.title,
            tabs: workbook.tabs.length,
            namedRanges: workbook.namedRanges.length,
            url: workbook.url,
          },
        ]),
      ),
    },
    null,
    2,
  ),
);
