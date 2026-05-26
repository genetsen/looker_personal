import fs from 'node:fs';
import path from 'node:path';
import { fileURLToPath } from 'node:url';
import {
  SOURCE_SPREADSHEET_ID,
  driveFetch,
  encodeRange,
  getAccessToken,
  sheetsFetch,
} from './sheets_api.mjs';

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const root = path.resolve(__dirname, '..');
const outputPath = path.join(root, 'data', 'v2_google_sheet_version.json');
const token = getAccessToken();
const today = new Date().toISOString().slice(0, 10);

const title = `Creative Assignment Sheet | V2 Configurable Template | Apollo | ${today}`;

const COLORS = {
  navy: { red: 0.07, green: 0.23, blue: 0.27 },
  teal: { red: 0.12, green: 0.43, blue: 0.46 },
  blue: { red: 0.18, green: 0.36, blue: 0.64 },
  green: { red: 0.2, green: 0.52, blue: 0.35 },
  amber: { red: 0.82, green: 0.55, blue: 0.16 },
  gray: { red: 0.95, green: 0.96, blue: 0.96 },
  white: { red: 1, green: 1, blue: 1 },
};

function cell(value) {
  const userEnteredValue =
    typeof value === 'number'
      ? { numberValue: value }
      : typeof value === 'boolean'
        ? { boolValue: value }
        : typeof value === 'string' && value.startsWith('=')
          ? { formulaValue: value }
          : { stringValue: String(value ?? '') };

  return {
    userEnteredValue,
    userEnteredFormat: {
      wrapStrategy: 'WRAP',
      verticalAlignment: 'MIDDLE',
      padding: { top: 6, right: 8, bottom: 6, left: 8 },
    },
  };
}

function row(values) {
  return { values: values.map(cell) };
}

function quoteSheet(title) {
  return `'${title.replaceAll("'", "''")}'`;
}

function updateRowsRequest(sheetId, rows) {
  return {
    updateCells: {
      range: {
        sheetId,
        startRowIndex: 0,
        endRowIndex: rows.length,
        startColumnIndex: 0,
      },
      rows: rows.map(row),
      fields: 'userEnteredValue,userEnteredFormat(wrapStrategy,verticalAlignment,padding)',
    },
  };
}

function formatSheetRequests(sheetId, rowCount, columnCount, color = COLORS.teal) {
  return [
    {
      repeatCell: {
        range: { sheetId, startRowIndex: 0, endRowIndex: 1 },
        cell: {
          userEnteredFormat: {
            backgroundColor: color,
            textFormat: {
              bold: true,
              foregroundColor: COLORS.white,
            },
            horizontalAlignment: 'CENTER',
          },
        },
        fields: 'userEnteredFormat(backgroundColor,textFormat,horizontalAlignment)',
      },
    },
    {
      repeatCell: {
        range: { sheetId, startRowIndex: 1, endRowIndex: rowCount },
        cell: {
          userEnteredFormat: {
            backgroundColor: COLORS.white,
            textFormat: { foregroundColor: { red: 0.12, green: 0.12, blue: 0.12 } },
          },
        },
        fields: 'userEnteredFormat(backgroundColor,textFormat)',
      },
    },
    {
      updateDimensionProperties: {
        range: { sheetId, dimension: 'COLUMNS', startIndex: 0, endIndex: columnCount },
        properties: { pixelSize: 170 },
        fields: 'pixelSize',
      },
    },
    {
      updateDimensionProperties: {
        range: { sheetId, dimension: 'ROWS', startIndex: 0, endIndex: rowCount },
        properties: { pixelSize: 36 },
        fields: 'pixelSize',
      },
    },
    {
      setBasicFilter: {
        filter: {
          range: {
            sheetId,
            startRowIndex: 0,
            endRowIndex: rowCount,
            startColumnIndex: 0,
            endColumnIndex: columnCount,
          },
        },
      },
    },
  ];
}

function addWarningProtection(sheetId, title, startRowIndex = 0, endColumnIndex = null) {
  const range = { sheetId, startRowIndex };
  if (endColumnIndex !== null) range.endColumnIndex = endColumnIndex;
  return {
    addProtectedRange: {
      protectedRange: {
        range,
        description: title,
        warningOnly: true,
      },
    },
  };
}

async function batch(spreadsheetId, requests) {
  if (!requests.length) return {};
  return sheetsFetch(spreadsheetId, ':batchUpdate', {
    method: 'POST',
    token,
    body: JSON.stringify({ requests }),
  });
}

async function metadata(spreadsheetId) {
  return sheetsFetch(
    spreadsheetId,
    '?includeGridData=false&fields=properties(title),sheets(properties(sheetId,title,index,hidden,gridProperties(rowCount,columnCount)))',
    { token },
  );
}

function buildSheetDefinitions() {
  const step0 = quoteSheet('STEP 0 | Prisma - Manual Entry');
  const step1 = quoteSheet('STEP 1 | INPUT - Creative Details');
  const step2 = quoteSheet('STEP 2 |  Creative Assignment Matrix v2');
  const utm = quoteSheet('AUTO | UTM Builder | INTERNAL v2');
  const output = quoteSheet('OUTPUT | Adswerve Doc | v1');

  return [
    {
      title: 'V2 | Control Panel',
      color: COLORS.navy,
      rows: [
        ['V2 Configurable Creative Assignment Template', 'Status', 'Notes', 'Owner'],
        ['Raw Prisma import remains immutable', 'Implemented', 'Original Step 0 tab is preserved. Canonical tabs read downstream from it.', 'Workflow'],
        ['Client-specific behavior moved to config', 'Implemented', 'Use CONFIG tabs for client, UTM, media type, output, and template-version rules.', 'Config'],
        ['Canonical layers added', 'Implemented', 'Prisma, creative assets, assignments, and generated UTMs now have visible canonical tabs.', 'Data model'],
        ['QA layer added', 'Implemented', 'QA tab checks readiness, stale template residue, and output parity markers.', 'QA'],
        ['Original Adswerve output preserved', 'Implemented', 'Existing output formulas stay intact so parity can be verified before deeper formula replacement.', 'Trafficking'],
        ['Legacy tabs labelled', 'Implemented', 'Deprecated/archived tabs are renamed with LEGACY prefixes while preserving formulas.', 'Template hygiene'],
      ],
    },
    {
      title: 'CONFIG | Template Version',
      color: COLORS.blue,
      rows: [
        ['Field', 'Value', 'Purpose', 'Edit Guidance'],
        ['template_version', 'v2.0', 'Declares this workbook shape for future importers and web app migration.', 'Update only when structure changes.'],
        ['source_workbook', 'Creative Assignment Sheet | 2026 | Apollo', 'Original copied workbook.', 'Informational.'],
        ['created_date', today, 'Creation date for this V2 copy.', 'Informational.'],
        ['client', 'Apollo', 'Default client profile for this copy.', 'Change when cloning for another client.'],
        ['step0_tab_alias', 'STEP 0 | Prisma - Manual Entry', 'Raw Prisma paste area.', 'Do not change raw import shape.'],
        ['step1_tab_alias', 'STEP 1 | INPUT - Creative Details', 'Creative inventory input area.', 'Use canonical asset tab for validation.'],
        ['step2_tab_alias', 'STEP 2 |  Creative Assignment Matrix v2', 'User assignment matrix.', 'Matrix remains the user-facing UI.'],
        ['utm_builder_tab_alias', 'AUTO | UTM Builder | INTERNAL v2', 'Legacy formula-generated UTM layer.', 'Use canonical generated UTM tab for V2 review.'],
        ['output_tab_alias', 'OUTPUT | Adswerve Doc | v1', 'Adswerve trafficking export surface.', 'Do not change schema without output-rule update.'],
        ['urls_policy', 'local_only', 'Prevents silent cross-client IMPORTRANGE dependencies.', 'Use URLs V2 | Local for local outputs.'],
      ],
    },
    {
      title: 'CONFIG | Client',
      color: COLORS.blue,
      rows: [
        ['Field', 'Apollo Value', 'Meaning', 'Client Customization Notes'],
        ['client_name', 'Apollo', 'Human-readable client name.', 'Required.'],
        ['client_code', 'APO', 'Client code expected in Prisma placement names.', 'Change per client, such as OLI or RTL.'],
        ['default_country', 'USA', 'Default country token for UTM term when Prisma has geo:na.', 'Client-specific.'],
        ['default_audience', 'na', 'Default audience fallback.', 'Client-specific.'],
        ['landing_domain_allowlist', 'apollo.com', 'Allowed domain check for creative landing pages.', 'Use comma-separated domains.'],
        ['utm_campaign_prefix', '', 'Optional prefix such as USA_.', 'Apollo currently does not force USA_ for all rows.'],
        ['include_utm_source_platform', 'TRUE', 'Whether generated URLs include utm_source_platform.', 'Observed Apollo = TRUE; Olipop/ADIF/Ritual = FALSE.'],
        ['utm_content_pattern', 'source_medium_package-placement_creative', 'Pattern used for utm_content.', 'Future app should compile this from tokens.'],
        ['utm_term_pattern', 'audience_country', 'Pattern used for utm_term.', 'Future app should compile this from tokens.'],
        ['adswerve_output_version', 'v1', 'Output schema version.', 'Keep v1 unless Adswerve schema changes.'],
      ],
    },
    {
      title: 'CONFIG | UTM Rules',
      color: COLORS.blue,
      rows: [
        ['priority', 'client', 'field', 'source_field', 'normalization', 'default_value', 'include_when', 'notes'],
        [10, 'Apollo', 'utm_source', 'supplier_code', 'lowercase', '', 'always', 'Apollo currently uses supplier/source-like value.'],
        [20, 'Apollo', 'utm_medium', 'media_type_or_buy_type', 'lowercase', '', 'always', 'Current formulas infer medium from media/buy type.'],
        [30, 'Apollo', 'utm_campaign', 'campaign_name', 'mixed_existing', '', 'always', 'Preserve current output casing until approved.'],
        [40, 'Apollo', 'utm_source_platform', 'supplier_code', 'lowercase', '', 'include_utm_source_platform=TRUE', 'Apollo-specific field; not universal.'],
        [50, 'Apollo', 'utm_content', 'package_placement_creative', 'slug', '', 'always', 'Use package id, placement id, and creative name.'],
        [60, 'Apollo', 'utm_term', 'audience_geo', 'slug', 'na_usa', 'always', 'Default geo:na to USA.'],
        [70, 'Default Lean Client', 'utm_source_platform', '', 'omit', '', 'include_utm_source_platform=FALSE', 'Used by Olipop, ADIF, Ritual style templates.'],
      ],
    },
    {
      title: 'CONFIG | Media Type Rules',
      color: COLORS.blue,
      rows: [
        ['priority', 'match_field', 'match_pattern', 'utm_medium', 'active', 'notes'],
        [10, 'media_type', 'Audio|Podcast', 'audio', 'TRUE', 'Observed ADIF audio/podcast rows.'],
        [20, 'media_type', 'Video|OTT|CTV|Standard Video', 'video', 'TRUE', 'Observed Apollo, Olipop, Ritual video rows.'],
        [30, 'media_type', 'Display|Banner|Native', 'display', 'TRUE', 'Display fallback.'],
        [40, 'media_type', 'Social', 'social', 'TRUE', 'Social fallback.'],
        [50, 'buy_type', 'TV', 'tv', 'TRUE', 'Observed Ritual-style tv medium.'],
      ],
    },
    {
      title: 'CONFIG | Output Rules',
      color: COLORS.blue,
      rows: [
        ['Field', 'Value', 'Purpose', 'Notes'],
        ['adswerve_output_version', 'v1', 'The current Adswerve output schema.', 'Matches existing OUTPUT | Adswerve Doc | v1 columns.'],
        ['required_columns', 'Campaign, Site Name, Package Name, Size, Placement Name, Start, End, Est. Impressions, CPM, Media Cost, Ad Name, Creative Assignment, Creative Rotation, Landing Page Name, URL, Contact Info, Notes', 'Canonical export column list.', 'Do not reorder without versioning.'],
        ['creative_assignment_name_pattern', 'package-placement_size | creative', 'Human-friendly trafficking name.', 'Current formulas preserve this behavior.'],
        ['creative_rotation_default', '', 'Default value when no rotation is needed.', 'Blank in current output.'],
        ['output_blank_policy', 'preserve_existing', 'Keep blank contact/notes fields blank.', 'Avoid invented values.'],
      ],
    },
    {
      title: 'CANONICAL | Prisma Manual Normalized',
      color: COLORS.green,
      rows: [
        ['source_row', 'prisma_id', 'supplier_name', 'supplier_code', 'record_type', 'media_type', 'buy_type', 'raw_package_name', 'raw_placement_name', 'cost_method', 'unit_type', 'rate', 'units', 'media_cost', 'start_date', 'end_date', 'derived_package_id', 'derived_placement_id', 'parse_status', 'parse_warning'],
        [
          `=ARRAYFORMULA(IF(${step0}!A2:A="",,ROW(${step0}!A2:A)))`,
          `=ARRAYFORMULA(IF(${step0}!A2:A="",,${step0}!A2:A))`,
          `=ARRAYFORMULA(IF(${step0}!A2:A="",,${step0}!C2:C))`,
          `=ARRAYFORMULA(IF(${step0}!A2:A="",,${step0}!D2:D))`,
          `=ARRAYFORMULA(IF(${step0}!A2:A="",,${step0}!E2:E))`,
          `=ARRAYFORMULA(IF(${step0}!A2:A="",,${step0}!F2:F))`,
          `=ARRAYFORMULA(IF(${step0}!A2:A="",,${step0}!G2:G))`,
          `=ARRAYFORMULA(IF(${step0}!A2:A="",,${step0}!H2:H))`,
          `=ARRAYFORMULA(IF(${step0}!A2:A="",,${step0}!J2:J))`,
          `=ARRAYFORMULA(IF(${step0}!A2:A="",,${step0}!Q2:Q))`,
          `=ARRAYFORMULA(IF(${step0}!A2:A="",,${step0}!R2:R))`,
          `=ARRAYFORMULA(IF(${step0}!A2:A="",,${step0}!S2:S))`,
          `=ARRAYFORMULA(IF(${step0}!A2:A="",,${step0}!T2:T))`,
          `=ARRAYFORMULA(IF(${step0}!A2:A="",,${step0}!U2:U))`,
          `=ARRAYFORMULA(IF(${step0}!A2:A="",,${step0}!V2:V))`,
          `=ARRAYFORMULA(IF(${step0}!A2:A="",,${step0}!W2:W))`,
          `=ARRAYFORMULA(IF(${step0}!A2:A="",,IFERROR(REGEXEXTRACT(${step0}!J2:J,"[|,~](P......)[|,_]"),"")))`,
          `=ARRAYFORMULA(IF(${step0}!A2:A="",,IFERROR(REGEXEXTRACT(${step0}!A2:A,"P......"),${step0}!A2:A)))`,
          `=ARRAYFORMULA(IF(${step0}!A2:A="",,IF(${step0}!J2:J="","MISSING_PLACEMENT_NAME","OK")))`,
          `=ARRAYFORMULA(IF(${step0}!A2:A="",,IF(${step0}!J2:J="","Raw placement name is blank","")))`,
        ],
      ],
    },
    {
      title: 'CANONICAL | Creative Assets',
      color: COLORS.green,
      rows: [
        ['asset_row', 'creative_name', 'creative_type', 'landing_page_url', 'asset_key', 'asset_status', 'domain_check'],
        [
          `=ARRAYFORMULA(IF(${step1}!C3:C="",,ROW(${step1}!C3:C)))`,
          `=ARRAYFORMULA(IF(${step1}!C3:C="",,${step1}!C3:C))`,
          `=ARRAYFORMULA(IF(${step1}!C3:C="",,${step1}!D3:D))`,
          `=ARRAYFORMULA(IF(${step1}!C3:C="",,${step1}!E3:E))`,
          `=ARRAYFORMULA(IF(${step1}!C3:C="",,LOWER(${step1}!C3:C)))`,
          `=ARRAYFORMULA(IF(${step1}!C3:C="",,IF(${step1}!E3:E="","MISSING_URL","OK")))`,
          `=ARRAYFORMULA(IF(${step1}!C3:C="",,IF(REGEXMATCH(LOWER(${step1}!E3:E),"apollo\\.com|^$"),"OK","REVIEW_DOMAIN")))`,
        ],
      ],
    },
    {
      title: 'CANONICAL | Creative Assignments',
      color: COLORS.green,
      rows: [
        ['placement_id', 'placement_label', 'creative_name', 'assignment_key', 'assignment_status'],
        [
          `=ARRAYFORMULA(QUERY(SPLIT(FLATTEN(${step2}!F5:F&"♦"&${step2}!I5:I&"♦"&${step2}!J5:T),"♦"),"select Col1, Col2, Col3 where Col3 is not null and Col3 <> ''",0))`,
          '',
          '',
          '=ARRAYFORMULA(IF(A2:A="",,A2:A&" || "&C2:C))',
          '=ARRAYFORMULA(IF(A2:A="",,IF(C2:C="","MISSING_CREATIVE","OK")))',
        ],
      ],
    },
    {
      title: 'CANONICAL | Generated UTMs',
      color: COLORS.green,
      rows: [
        ['placement_or_assignment', 'package_id', 'prisma_id', 'creative_name', 'utm_source', 'utm_medium', 'utm_campaign', 'utm_source_platform', 'utm_content', 'utm_term', 'utm_code', 'landing_page', 'final_url_raw', 'final_url_clean', 'adswerve_lookup_key'],
        [
          `=ARRAYFORMULA(IF(${utm}!A3:A="",,${utm}!A3:A))`,
          `=ARRAYFORMULA(IF(${utm}!A3:A="",,${utm}!C3:C))`,
          `=ARRAYFORMULA(IF(${utm}!A3:A="",,${utm}!D3:D))`,
          `=ARRAYFORMULA(IF(${utm}!A3:A="",,${utm}!F3:F))`,
          `=ARRAYFORMULA(IF(${utm}!A3:A="",,${utm}!H3:H))`,
          `=ARRAYFORMULA(IF(${utm}!A3:A="",,${utm}!J3:J))`,
          `=ARRAYFORMULA(IF(${utm}!A3:A="",,${utm}!M3:M))`,
          `=ARRAYFORMULA(IF(${utm}!A3:A="",,${utm}!O3:O))`,
          `=ARRAYFORMULA(IF(${utm}!A3:A="",,${utm}!Q3:Q))`,
          `=ARRAYFORMULA(IF(${utm}!A3:A="",,${utm}!V3:V))`,
          `=ARRAYFORMULA(IF(${utm}!A3:A="",,${utm}!Z3:Z))`,
          `=ARRAYFORMULA(IF(${utm}!A3:A="",,${utm}!AA3:AA))`,
          `=ARRAYFORMULA(IF(${utm}!A3:A="",,${utm}!AB3:AB))`,
          `=ARRAYFORMULA(IF(${utm}!A3:A="",,${utm}!AC3:AC))`,
          `=ARRAYFORMULA(IF(${utm}!A3:A="",,${utm}!AU3:AU))`,
        ],
      ],
    },
    {
      title: 'URLs V2 | Local',
      color: COLORS.green,
      rows: [
        ['placement_or_assignment', 'final_url_clean', 'utm_source', 'utm_medium', 'utm_campaign', 'utm_content', 'utm_term'],
        [
          '=FILTER({\'CANONICAL | Generated UTMs\'!A2:A,\'CANONICAL | Generated UTMs\'!N2:N,\'CANONICAL | Generated UTMs\'!E2:E,\'CANONICAL | Generated UTMs\'!F2:F,\'CANONICAL | Generated UTMs\'!G2:G,\'CANONICAL | Generated UTMs\'!I2:I,\'CANONICAL | Generated UTMs\'!J2:J},\'CANONICAL | Generated UTMs\'!N2:N<>"")',
        ],
      ],
    },
    {
      title: 'QA | Readiness Checks',
      color: COLORS.amber,
      rows: [
        ['Check', 'Status', 'Observed Value', 'Severity', 'Recommended Action', 'Source'],
        ['Adswerve output row count', `=COUNTA(${output}!A2:A)`, '', 'Info', 'Compare to golden Apollo validation after build.', 'OUTPUT | Adswerve Doc | v1'],
        ['Creative assets missing URL', '=COUNTIF(\'CANONICAL | Creative Assets\'!F:F,"MISSING_URL")', '', 'High', 'Fill missing destination URLs before trafficking.', 'CANONICAL | Creative Assets'],
        ['Creative asset domain review', '=COUNTIF(\'CANONICAL | Creative Assets\'!G:G,"REVIEW_DOMAIN")', '', 'Medium', 'Confirm non-Apollo domains are intentional for client copies.', 'CANONICAL | Creative Assets'],
        ['Prisma parse warnings', '=COUNTIFS(\'CANONICAL | Prisma Manual Normalized\'!S:S,"<>OK",\'CANONICAL | Prisma Manual Normalized\'!S:S,"<>")', '', 'High', 'Review rows where raw placement names are missing or unparsable.', 'CANONICAL | Prisma Manual Normalized'],
        ['Normalized assignment rows', '=COUNTA(\'CANONICAL | Creative Assignments\'!A2:A)', '', 'Info', 'Should match generated UTM assignment volume after filtering blanks.', 'CANONICAL | Creative Assignments'],
        ['Generated final URLs', '=COUNTA(\'CANONICAL | Generated UTMs\'!N2:N)', '', 'Info', 'Review before using URLs V2 | Local externally.', 'CANONICAL | Generated UTMs'],
        ['External import policy', 'LOCAL_ONLY', '', 'High', 'Use URLs V2 | Local instead of copied IMPORTRANGE-based URL tabs.', 'CONFIG | Template Version'],
        ['Client-specific UTM source platform', '=VLOOKUP("include_utm_source_platform",\'CONFIG | Client\'!A:B,2,FALSE)', '', 'Info', 'Apollo keeps this TRUE; lean-client copies should set FALSE.', 'CONFIG | Client'],
        ['Legacy output parity', 'PENDING_SCRIPT_VALIDATION', '', 'High', 'Run automated validation against Apollo golden output after creation.', 'scripts/validate_google_sheet_output.mjs'],
      ],
    },
    {
      title: 'System Map - V2 Notes',
      color: COLORS.navy,
      rows: [
        ['V2 Recommendation', 'Implemented As', 'Why', 'Follow-up'],
        ['Immutable Prisma import', 'Original Step 0 preserved; canonical normalized tab reads downstream.', 'Avoids changing Prisma export shape.', 'Eventually replace helper regexes with named parser functions or app code.'],
        ['Config-driven client customization', 'CONFIG tabs for client, UTM, media type, output, and template version.', 'Client differences are visible instead of hidden in formulas.', 'Add dropdown validation for client config fields.'],
        ['Canonical assignment model', 'CANONICAL | Creative Assignments unpivots matrix slots into one row per assignment.', 'Downstream logic can stop parsing comma-separated strings.', 'Point UTM builder to canonical assignments in a later behavior-change release.'],
        ['Local URL output', 'URLs V2 | Local reads generated UTMs inside this workbook.', 'Avoids copied IMPORTRANGE links to another client workbook.', 'Archive old cross-workbook URL tabs after stakeholder signoff.'],
        ['QA layer', 'QA | Readiness Checks summarizes missing URLs, parse warnings, assignments, generated URLs, and parity status.', 'Makes readiness visible before trafficking.', 'Add formula-error scanners with Apps Script or app-side validation.'],
        ['Legacy hygiene', 'Deprecated tabs renamed with LEGACY prefix where safe.', 'Makes old logic harder to mistake for current workflow.', 'Remove legacy tabs only after full output parity and stakeholder signoff.'],
      ],
    },
  ];
}

function legacyRenamePlan(existingTitles) {
  const candidates = {
    'AUTO | PRISMA | Auto-Updates': 'LEGACY | AUTO PRISMA Auto-Updates',
    'AUTO | UTM Builder | INTERNAL v1 - archived': 'LEGACY | AUTO UTM Builder v1',
    'MANUAL UTM BUILDER v1 - archived': 'LEGACY | Manual UTM Builder v1',
    'Lookup talbe - archived': 'LEGACY | Lookup Table',
    data5: 'LEGACY | data5',
  };
  return Object.entries(candidates).filter(
    ([oldTitle, newTitle]) => existingTitles.has(oldTitle) && !existingTitles.has(newTitle),
  );
}

const copy = await driveFetch(
  `/files/${SOURCE_SPREADSHEET_ID}/copy?fields=id,name,webViewLink`,
  {
    method: 'POST',
    token,
    body: JSON.stringify({ name: title }),
  },
);

const sheetDefinitions = buildSheetDefinitions();

let meta = await metadata(copy.id);
const existingTitles = new Set(meta.sheets.map((sheet) => sheet.properties.title));
const initialRequests = [];

for (const [oldTitle, newTitle] of legacyRenamePlan(existingTitles)) {
  const sheet = meta.sheets.find((item) => item.properties.title === oldTitle);
  initialRequests.push({
    updateSheetProperties: {
      properties: {
        sheetId: sheet.properties.sheetId,
        title: newTitle,
      },
      fields: 'title',
    },
  });
}

for (const definition of sheetDefinitions) {
  initialRequests.push({
    addSheet: {
      properties: {
        title: definition.title,
        index: definition.title.startsWith('V2') ? 0 : undefined,
        gridProperties: {
          rowCount: Math.max(1000, definition.rows.length + 20),
          columnCount: Math.max(12, definition.rows[0].length),
          frozenRowCount: 1,
        },
        tabColor: definition.color,
        hidden: false,
      },
    },
  });
}

await batch(copy.id, initialRequests);

meta = await metadata(copy.id);
const sheetByTitle = new Map(meta.sheets.map((sheet) => [sheet.properties.title, sheet]));

const writeAndFormatRequests = [];
for (const definition of sheetDefinitions) {
  const sheetId = sheetByTitle.get(definition.title).properties.sheetId;
  writeAndFormatRequests.push(updateRowsRequest(sheetId, definition.rows));
  writeAndFormatRequests.push(
    ...formatSheetRequests(sheetId, Math.max(40, definition.rows.length + 10), Math.max(12, definition.rows[0].length), definition.color),
  );
  if (definition.title.startsWith('CANONICAL') || definition.title.startsWith('QA')) {
    writeAndFormatRequests.push(addWarningProtection(sheetId, `${definition.title} formulas are V2-managed`));
  }
}

await batch(copy.id, writeAndFormatRequests);

// The assignment unpivot formula in A2 spills into B:C. Clear the placeholder
// cells written by the rectangular setup pass so the spill range can expand.
await sheetsFetch(
  copy.id,
  `/values/${encodeRange("'CANONICAL | Creative Assignments'!B2:C2")}:clear`,
  {
    method: 'POST',
    token,
    body: JSON.stringify({}),
  },
);

const legacyHideRequests = [];
for (const titleToHide of [
  'LEGACY | AUTO PRISMA Auto-Updates',
  'LEGACY | AUTO UTM Builder v1',
  'LEGACY | Manual UTM Builder v1',
  'LEGACY | Lookup Table',
  'LEGACY | data5',
]) {
  const sheet = sheetByTitle.get(titleToHide);
  if (sheet) {
    legacyHideRequests.push({
      updateSheetProperties: {
        properties: { sheetId: sheet.properties.sheetId, hidden: true },
        fields: 'hidden',
      },
    });
  }
}

for (const formulaTab of [
  'STEP 0 | Prisma - Manual Entry',
  'STEP 1 | INPUT - Creative Details',
  'STEP 2 |  Creative Assignment Matrix v2',
  'AUTO | UTM Builder | INTERNAL v2',
  'OUTPUT | Adswerve Doc | v1',
]) {
  const sheet = sheetByTitle.get(formulaTab);
  if (sheet) {
    legacyHideRequests.push(addWarningProtection(sheet.properties.sheetId, `${formulaTab} is protected by V2 warning-only guardrails`));
  }
}

await batch(copy.id, legacyHideRequests);

const result = {
  createdAt: new Date().toISOString(),
  sourceSpreadsheetId: SOURCE_SPREADSHEET_ID,
  newSpreadsheetId: copy.id,
  name: copy.name,
  url: copy.webViewLink ?? `https://docs.google.com/spreadsheets/d/${copy.id}/edit`,
  v2Sheets: sheetDefinitions.map((sheet) => sheet.title),
  legacyRenames: legacyRenamePlan(existingTitles).map(([from, to]) => ({ from, to })),
};

fs.writeFileSync(outputPath, JSON.stringify(result, null, 2));
console.log(JSON.stringify(result, null, 2));
