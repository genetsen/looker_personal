import { execFileSync } from "node:child_process";
import { mkdirSync, writeFileSync } from "node:fs";
import path from "node:path";

const spreadsheetId = "1WerhrbBMggzCwIUCOsOCV33aHygV96jt1HgqiYcUHZo";
const outDir = path.resolve("outputs/manual-data-editor-intro/sheet-formatting-proof");
mkdirSync(outDir, { recursive: true });

const token = execFileSync("gcloud", ["auth", "print-access-token"], {
  encoding: "utf8",
}).trim();

async function googleJson(url) {
  const res = await fetch(url, {
    headers: {
      Authorization: `Bearer ${token}`,
      "X-Goog-User-Project": "looker-studio-pro-452620",
    },
  });
  const text = await res.text();
  if (!res.ok) throw new Error(`${res.status} ${res.statusText}\n${text}`);
  return JSON.parse(text);
}

const fields = [
  "properties(title)",
  "sheets(properties(sheetId,title,index,hidden,gridProperties(rowCount,columnCount,frozenRowCount,frozenColumnCount),tabColor)",
  "data(rowData(values(userEnteredValue,formattedValue,userEnteredFormat(backgroundColor,textFormat,horizontalAlignment,verticalAlignment,wrapStrategy,numberFormat,borders),note,dataValidation))),",
  "conditionalFormats",
  "protectedRanges",
  "basicFilter",
  "filterViews",
  "slicers",
  "bandedRanges)",
].join(",");

const meta = await googleJson(
  `https://sheets.googleapis.com/v4/spreadsheets/${spreadsheetId}?includeGridData=true&ranges=Package%20Editor!A1:BM20&ranges=Instructions!A1:H70&fields=${encodeURIComponent(fields)}`,
);

function color(c) {
  if (!c) return "";
  const r = Math.round((c.red ?? 0) * 255).toString(16).padStart(2, "0");
  const g = Math.round((c.green ?? 0) * 255).toString(16).padStart(2, "0");
  const b = Math.round((c.blue ?? 0) * 255).toString(16).padStart(2, "0");
  return `#${r}${g}${b}`;
}

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

const summary = {
  title: meta.properties?.title,
  sheets: meta.sheets.map((s) => ({
    title: s.properties.title,
    sheetId: s.properties.sheetId,
    hidden: s.properties.hidden ?? false,
    frozenRows: s.properties.gridProperties?.frozenRowCount ?? 0,
    frozenColumns: s.properties.gridProperties?.frozenColumnCount ?? 0,
    rowCount: s.properties.gridProperties?.rowCount,
    columnCount: s.properties.gridProperties?.columnCount,
    conditionalFormatCount: s.conditionalFormats?.length ?? 0,
    protectedRangeCount: s.protectedRanges?.length ?? 0,
    slicerCount: s.slicers?.length ?? 0,
    filterViewCount: s.filterViews?.length ?? 0,
    hasBasicFilter: Boolean(s.basicFilter),
    bandedRangeCount: s.bandedRanges?.length ?? 0,
  })),
};

const editor = meta.sheets.find((s) => s.properties.title === "Package Editor");
const instructions = meta.sheets.find((s) => s.properties.title === "Instructions");

function rowValues(sheet, rowNumber) {
  const values = sheet?.data?.flatMap((d) => d.rowData ?? [])?.[rowNumber - 1]?.values ?? [];
  return values.map((v, i) => ({
    col: a1(i),
    formattedValue: v.formattedValue ?? "",
    note: v.note ?? "",
    bg: color(v.userEnteredFormat?.backgroundColor),
    fg: color(v.userEnteredFormat?.textFormat?.foregroundColor),
    bold: v.userEnteredFormat?.textFormat?.bold ?? false,
    wrap: v.userEnteredFormat?.wrapStrategy ?? "",
    numberFormat: v.userEnteredFormat?.numberFormat?.type ?? "",
  }));
}

const proof = {
  checkedAt: new Date().toISOString(),
  summary,
  packageEditor: {
    slicers: (editor.slicers ?? []).map((s) => ({
      title: s.slicerSpec?.title,
      columnIndex: s.slicerSpec?.filterCriteria?.condition ? null : s.slicerSpec?.columnIndex,
      position: s.position,
    })),
    headerRow4: rowValues(editor, 4).filter((v) => v.formattedValue || v.note || v.bg),
    firstDataRow5: rowValues(editor, 5).filter((v) => v.formattedValue || v.note || v.bg).slice(0, 35),
    row1To3: [1, 2, 3].map((r) => rowValues(editor, r).filter((v) => v.formattedValue || v.note || v.bg)),
    conditionalFormats: (editor.conditionalFormats ?? []).map((cf, i) => ({ index: i, ranges: cf.ranges, rule: cf.booleanRule ?? cf.gradientRule ?? cf })),
    protectedRanges: (editor.protectedRanges ?? []).map((pr) => ({
      description: pr.description,
      range: pr.range,
      warningOnly: pr.warningOnly ?? false,
    })),
    bandedRanges: editor.bandedRanges ?? [],
  },
  instructions: {
    row1To60: Array.from({ length: 60 }, (_, idx) => rowValues(instructions, idx + 1).filter((v) => v.formattedValue || v.note || v.bg)),
    conditionalFormats: instructions.conditionalFormats ?? [],
    protectedRanges: instructions.protectedRanges ?? [],
  },
};

writeFileSync(path.join(outDir, "live-sheet-formatting-proof.json"), `${JSON.stringify(proof, null, 2)}\n`);
console.log(JSON.stringify(summary, null, 2));
