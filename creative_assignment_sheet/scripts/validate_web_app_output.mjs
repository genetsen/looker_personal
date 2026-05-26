import fs from 'node:fs';
import path from 'node:path';
import { fileURLToPath } from 'node:url';
import { buildInitialState, generateAdswerveOutput, validateAgainstApollo } from '../src/generator.mjs';
import { diffRows, hashRows, normalizeRows } from './hash_rows.mjs';

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const root = path.resolve(__dirname, '..');
const fixture = JSON.parse(fs.readFileSync(path.join(root, 'data', 'apollo_fixture.json'), 'utf8'));
const state = buildInitialState(fixture);
const rows = generateAdswerveOutput(state);
const validation = validateAgainstApollo(state);
const actualHash = hashRows(rows);
const diffs = diffRows(
  normalizeRows(fixture.adswerveOutput.rows, fixture.adswerveOutput.columnCount),
  normalizeRows(rows, fixture.adswerveOutput.columnCount),
  25,
);

const result = {
  checkedAt: new Date().toISOString(),
  appMode: state.mode,
  outputRange: fixture.outputRange,
  expected: {
    rows: fixture.adswerveOutput.rows.length,
    columns: fixture.adswerveOutput.columnCount,
    sha256: fixture.adswerveOutput.sha256,
  },
  actual: {
    rows: rows.length,
    columns: fixture.adswerveOutput.columnCount,
    sha256: actualHash,
  },
  passed: validation.passed && actualHash === fixture.adswerveOutput.sha256 && diffs.length === 0,
  diffs,
};

fs.writeFileSync(path.join(root, 'data', 'web_app_validation.json'), JSON.stringify(result, null, 2));
console.log(JSON.stringify(result, null, 2));

if (!result.passed) process.exit(1);
