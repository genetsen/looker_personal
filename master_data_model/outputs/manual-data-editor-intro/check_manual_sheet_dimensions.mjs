import { execFileSync } from "node:child_process";
import { writeFileSync, mkdirSync } from "node:fs";
import path from "node:path";

const spreadsheetId = "1WerhrbBMggzCwIUCOsOCV33aHygV96jt1HgqiYcUHZo";
const outDir = path.resolve("outputs/manual-data-editor-intro/sheet-formatting-proof");
mkdirSync(outDir, { recursive: true });

const token = execFileSync("gcloud", ["auth", "print-access-token"], { encoding: "utf8" }).trim();
const fields = "sheets(properties(title,sheetId),data(columnMetadata(pixelSize,hiddenByUser),rowMetadata(pixelSize,hiddenByUser)))";
const url = `https://sheets.googleapis.com/v4/spreadsheets/${spreadsheetId}?includeGridData=true&ranges=Package%20Editor!A1:BM20&ranges=Instructions!A1:H70&fields=${encodeURIComponent(fields)}`;
const res = await fetch(url, {
  headers: {
    Authorization: `Bearer ${token}`,
    "X-Goog-User-Project": "looker-studio-pro-452620",
  },
});
const text = await res.text();
if (!res.ok) throw new Error(text);
const data = JSON.parse(text);

function a1(colIndex) {
  let n = colIndex + 1;
  let s = "";
  while (n > 0) {
    const m = (n - 1) % 26;
    s = String.fromCharCode(65 + m) + s;
    n = Math.floor((n - 1) / 26);
  }
  return s;
}

const summary = data.sheets.map((sheet) => ({
  title: sheet.properties.title,
  columns: (sheet.data?.[0]?.columnMetadata ?? []).map((c, i) => ({
    col: a1(i),
    pixelSize: c.pixelSize ?? null,
    hiddenByUser: c.hiddenByUser ?? false,
  })),
  rows: (sheet.data?.[0]?.rowMetadata ?? []).map((r, i) => ({
    row: i + 1,
    pixelSize: r.pixelSize ?? null,
    hiddenByUser: r.hiddenByUser ?? false,
  })),
}));

writeFileSync(path.join(outDir, "live-sheet-dimensions.json"), `${JSON.stringify(summary, null, 2)}\n`);
console.log(JSON.stringify(summary, null, 2));
