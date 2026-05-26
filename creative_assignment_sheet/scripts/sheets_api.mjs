import { execFileSync } from 'node:child_process';

export const SOURCE_SPREADSHEET_ID = '1U46ZJ4U6XCLTXqtNXlun7uY0L1iyG88RZ6HMiOeACyU';
export const OUTPUT_TAB = 'OUTPUT | Adswerve Doc | v1';
export const OUTPUT_RANGE = `'${OUTPUT_TAB}'!A1:W1542`;

export function getAccessToken() {
  return execFileSync('gcloud', ['auth', 'print-access-token'], {
    encoding: 'utf8',
    stdio: ['ignore', 'pipe', 'pipe'],
  }).trim();
}

export async function sheetsFetch(spreadsheetId, path, options = {}) {
  const token = options.token ?? getAccessToken();
  const response = await fetch(
    `https://sheets.googleapis.com/v4/spreadsheets/${spreadsheetId}${path}`,
    {
      ...options,
      headers: {
        Authorization: `Bearer ${token}`,
        'Content-Type': 'application/json',
        ...(options.headers ?? {}),
      },
    },
  );
  const text = await response.text();
  const payload = JSON.parse(text);
  if (!response.ok || payload.error) {
    throw new Error(JSON.stringify(payload.error ?? payload, null, 2));
  }
  return payload;
}

export async function driveFetch(path, options = {}) {
  const token = options.token ?? getAccessToken();
  const response = await fetch(`https://www.googleapis.com/drive/v3${path}`, {
    ...options,
    headers: {
      Authorization: `Bearer ${token}`,
      'Content-Type': 'application/json',
      ...(options.headers ?? {}),
    },
  });
  const text = await response.text();
  const payload = text ? JSON.parse(text) : {};
  if (!response.ok || payload.error) {
    throw new Error(JSON.stringify(payload.error ?? payload, null, 2));
  }
  return payload;
}

export function encodeRange(range) {
  return encodeURIComponent(range);
}
