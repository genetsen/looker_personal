import crypto from 'node:crypto';

export function normalizeRows(rows, width = null) {
  const normalizedWidth = width ?? rows.reduce((max, row) => Math.max(max, row.length), 0);
  return rows.map((row) => {
    const next = row.map((value) => (value == null ? '' : String(value)));
    while (next.length < normalizedWidth) next.push('');
    return next;
  });
}

export function hashRows(rows) {
  const normalized = normalizeRows(rows);
  return crypto.createHash('sha256').update(JSON.stringify(normalized)).digest('hex');
}

export function diffRows(expected, actual, limit = 20) {
  const width = Math.max(
    expected.reduce((max, row) => Math.max(max, row.length), 0),
    actual.reduce((max, row) => Math.max(max, row.length), 0),
  );
  const left = normalizeRows(expected, width);
  const right = normalizeRows(actual, width);
  const maxRows = Math.max(left.length, right.length);
  const diffs = [];
  for (let rowIndex = 0; rowIndex < maxRows; rowIndex += 1) {
    const expectedRow = left[rowIndex] ?? Array(width).fill('');
    const actualRow = right[rowIndex] ?? Array(width).fill('');
    for (let columnIndex = 0; columnIndex < width; columnIndex += 1) {
      if (expectedRow[columnIndex] !== actualRow[columnIndex]) {
        diffs.push({
          row: rowIndex + 1,
          column: columnIndex + 1,
          expected: expectedRow[columnIndex],
          actual: actualRow[columnIndex],
        });
        if (diffs.length >= limit) return diffs;
      }
    }
  }
  return diffs;
}
