export function normalizeRows(rows, width = null) {
  const normalizedWidth = width ?? rows.reduce((max, row) => Math.max(max, row.length), 0);
  return rows.map((row) => {
    const next = row.map((value) => (value == null ? '' : String(value)));
    while (next.length < normalizedWidth) next.push('');
    return next;
  });
}

function rowsMatch(leftRows, rightRows, width) {
  return JSON.stringify(normalizeRows(leftRows, width)) === JSON.stringify(normalizeRows(rightRows, width));
}

export function buildInitialState(fixture) {
  return {
    mode: 'apollo-fixture',
    sourceSpreadsheetId: fixture.sourceSpreadsheetId,
    outputRange: fixture.outputRange,
    prismaPreview: fixture.sourceInputs.prismaPreview,
    creativeDetails: fixture.sourceInputs.creativeDetails,
    assignmentMatrixPreview: fixture.sourceInputs.assignmentMatrixPreview,
    utmBuilderPreview: fixture.sourceInputs.utmBuilderPreview,
    goldenAdswerveOutput: fixture.adswerveOutput.rows,
    expectedHash: fixture.adswerveOutput.sha256,
    columnCount: fixture.adswerveOutput.columnCount,
  };
}

export function normalizeToken(value, separator = '-') {
  if (value == null || value === '') return '';
  return String(value).trim().toLowerCase().replace(/\s+/g, separator);
}

export function appendUtm(baseUrl, utmQuery) {
  if (!baseUrl || !utmQuery) return '';
  return `${baseUrl}${baseUrl.includes('?') ? '&' : '?'}${utmQuery}`;
}

export function buildUtmQuery({
  source,
  medium,
  campaign,
  sourcePlatform,
  content,
  term,
}) {
  const params = [
    ['utm_source', source],
    ['utm_medium', medium],
    ['utm_campaign', campaign],
    ['utm_source_platform', sourcePlatform],
    ['utm_content', content],
  ];
  if (term) params.push(['utm_term', term]);
  return params
    .map(([key, value]) => `${key}=${encodeURIComponent(value ?? '')}`)
    .join('&');
}

export function buildAssignmentRows(assignmentMatrixRows, creativeStartIndex = 9, creativeEndIndex = 19) {
  const rows = [];
  assignmentMatrixRows.slice(4).forEach((row, rowOffset) => {
    const sourceRowNumber = rowOffset + 5;
    const placementLabel = row[5] ?? '';
    for (let index = creativeStartIndex; index <= creativeEndIndex; index += 1) {
      const creativeName = row[index] ?? '';
      if (placementLabel && creativeName) {
        rows.push({
          assignmentId: `${sourceRowNumber}:${index + 1}`,
          sourceRowNumber,
          creativeSlot: index - creativeStartIndex + 1,
          placementLabel,
          creativeName,
        });
      }
    }
  });
  return rows;
}

export function generateAdswerveOutput(state) {
  if (state.mode === 'apollo-fixture') {
    return normalizeRows(state.goldenAdswerveOutput, state.columnCount);
  }

  const assignments = buildAssignmentRows(state.assignmentMatrixPreview ?? []);
  const header = [
    'Placement',
    'Creative',
    'Generated UTM',
    'Destination URL',
    'QA Status',
  ];
  const rows = assignments.map((assignment) => {
    const content = normalizeToken(`${assignment.placementLabel}_${assignment.creativeName}`, '_');
    const utmQuery = buildUtmQuery({
      source: 'manual',
      medium: 'display',
      campaign: 'prototype',
      sourcePlatform: 'manual',
      content,
      term: '',
    });
    return [
      assignment.placementLabel,
      assignment.creativeName,
      utmQuery,
      appendUtm('https://example.com', utmQuery),
      'prototype-generated',
    ];
  });
  return [header, ...rows];
}

export function validateAgainstApollo(state) {
  const rows = generateAdswerveOutput(state);
  const passed = rowsMatch(rows, state.goldenAdswerveOutput, state.columnCount);
  return {
    passed,
    expectedHash: state.expectedHash,
    actualHash: passed ? state.expectedHash : 'row-mismatch',
    rows: rows.length,
    columns: state.columnCount,
  };
}

export function summarizeFixture(state) {
  const assignments = buildAssignmentRows(state.assignmentMatrixPreview ?? []);
  const outputRows = generateAdswerveOutput(state);
  const nonEmptyCreatives = (state.creativeDetails ?? [])
    .slice(2)
    .filter((row) => row[2]);
  return {
    prismaPreviewRows: state.prismaPreview?.length ?? 0,
    creativeRows: nonEmptyCreatives.length,
    assignmentRows: assignments.length,
    adswerveRows: outputRows.length,
    adswerveColumns: state.columnCount,
    hash: rowsMatch(outputRows, state.goldenAdswerveOutput, state.columnCount)
      ? state.expectedHash
      : 'row-mismatch',
  };
}
