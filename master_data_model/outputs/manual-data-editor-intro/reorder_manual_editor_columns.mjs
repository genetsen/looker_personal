import { execFileSync } from "node:child_process";

const spreadsheetId = "1WerhrbBMggzCwIUCOsOCV33aHygV96jt1HgqiYcUHZo";
const sheetName = "Package Editor";
const range = `${sheetName}!A4:BM`;

const desiredColumns = [
  "Package ID",
  "Site",
  "Package Friendly Name",
  "Flight Start Date",
  "Flight End Date",
  "Planned Spend",
  "Planned Impressions",
  "Spend",
  "Impressions",
  "Clicks",
  "Video Plays",
  "Video Completions",
  "Delivery Override Start Date",
  "Delivery Override End Date",
  "Advertiser",
  "Package Type",
  "Channel",
  "Campaign",
  "Initiative",
  "Supplier Code",
  "Supplier Name",
  "Package Name",
  "GS Channel",
  "Baseline Flight Start Date",
  "Baseline Flight End Date",
  "Baseline Planned Spend",
  "Baseline Planned Impressions",
  "Baseline Spend",
  "Baseline Impressions",
  "Baseline Clicks",
  "Baseline Video Plays",
  "Baseline Video Completions",
  "Baseline Delivery Start Date",
  "Baseline Delivery End Date",
  "Baseline Advertiser",
  "Baseline Package Type",
  "Baseline Channel",
  "Baseline Campaign",
  "Baseline Initiative",
  "Baseline Supplier Code",
  "Baseline Supplier Name",
  "Baseline Package Name",
  "Baseline Package Friendly Name",
  "Baseline GS Channel",
  "Manual Marker Flight Start Date",
  "Manual Marker Flight End Date",
  "Manual Marker Planned Spend",
  "Manual Marker Planned Impressions",
  "Manual Marker Spend",
  "Manual Marker Impressions",
  "Manual Marker Clicks",
  "Manual Marker Video Plays",
  "Manual Marker Video Completions",
  "Manual Marker Delivery Start Date",
  "Manual Marker Delivery End Date",
  "Manual Marker Advertiser",
  "Manual Marker Package Type",
  "Manual Marker Channel",
  "Manual Marker Campaign",
  "Manual Marker Initiative",
  "Manual Marker Supplier Code",
  "Manual Marker Supplier Name",
  "Manual Marker Package Name",
  "Manual Marker Package Friendly Name",
  "Manual Marker GS Channel",
];

const token = execFileSync("gcloud", ["auth", "print-access-token", "--account", "gene.tsenter@giantspoon.com"], {
  encoding: "utf8",
}).trim();

async function googleJson(url, options = {}) {
  const response = await fetch(url, {
    ...options,
    headers: {
      Authorization: `Bearer ${token}`,
      "Content-Type": "application/json",
      "X-Goog-User-Project": "looker-studio-pro-452620",
      ...(options.headers ?? {}),
    },
  });
  const text = await response.text();
  if (!response.ok) throw new Error(`${response.status} ${response.statusText}\n${text}`);
  return text ? JSON.parse(text) : {};
}

const encodedRange = encodeURIComponent(range);
const read = await googleJson(
  `https://sheets.googleapis.com/v4/spreadsheets/${spreadsheetId}/values/${encodedRange}?valueRenderOption=FORMATTED_VALUE&dateTimeRenderOption=FORMATTED_STRING`,
);
const rows = read.values ?? [];
if (rows.length === 0) throw new Error("No rows found in Package Editor A4:BM.");

const currentHeaders = rows[0].map((value) => String(value ?? "").trim());
const currentIndex = new Map(currentHeaders.map((name, index) => [name, index]));
const missing = desiredColumns.filter((name) => !currentIndex.has(name));
if (missing.length > 0) throw new Error(`Missing expected columns: ${missing.join(", ")}`);

const reordered = rows.map((row, rowIndex) => {
  if (rowIndex === 0) return desiredColumns;
  return desiredColumns.map((name) => row[currentIndex.get(name)] ?? "");
});

await googleJson(
  `https://sheets.googleapis.com/v4/spreadsheets/${spreadsheetId}/values/${encodedRange}?valueInputOption=USER_ENTERED`,
  {
    method: "PUT",
    body: JSON.stringify({ range, majorDimension: "ROWS", values: reordered }),
  },
);

console.log(JSON.stringify({
  updatedRange: range,
  rowCount: reordered.length,
  columnCount: desiredColumns.length,
  headers: desiredColumns.slice(0, 23),
}, null, 2));
