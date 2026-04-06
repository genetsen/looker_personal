#!/usr/bin/env bash
set -euo pipefail

PROJECT_ID="looker-studio-pro-452620"
DATASET_ID="landing"
TABLE_ID="prisma_supplier_logos"
FULL_TABLE_ID="${PROJECT_ID}:${DATASET_ID}.${TABLE_ID}"
SHEET_NAME="Logos"
DEFAULT_XLSX_PATH="/Users/eugenetsenter/Downloads/Supplier_logos.xlsx"

XLSX_PATH="${1:-$DEFAULT_XLSX_PATH}"
TMP_CSV="$(mktemp "${TMPDIR:-/tmp}/prisma_supplier_logos.XXXXXX.csv")"

cleanup() {
  rm -f "$TMP_CSV"
}

trap cleanup EXIT

if ! command -v bq >/dev/null 2>&1; then
  echo "Error: the 'bq' command is not available in this shell." >&2
  exit 1
fi

if ! command -v python3 >/dev/null 2>&1; then
  echo "Error: python3 is not available in this shell." >&2
  exit 1
fi

if [[ ! -f "$XLSX_PATH" ]]; then
  echo "Error: workbook not found at '$XLSX_PATH'." >&2
  echo "Usage: $0 [/full/path/to/Supplier_logos.xlsx]" >&2
  exit 1
fi

echo "Building a clean CSV from '$XLSX_PATH' (${SHEET_NAME}!A:C)..."
XLSX_PATH="$XLSX_PATH" TMP_CSV="$TMP_CSV" SHEET_NAME="$SHEET_NAME" python3 - <<'PY'
import csv
import os
import sys

try:
    from openpyxl import load_workbook
except ImportError:
    sys.stderr.write(
        "Error: openpyxl is not installed. Run: python3 -m pip install openpyxl\n"
    )
    sys.exit(1)

xlsx_path = os.environ["XLSX_PATH"]
tmp_csv = os.environ["TMP_CSV"]
sheet_name = os.environ["SHEET_NAME"]

workbook = load_workbook(xlsx_path, read_only=True, data_only=True)
if sheet_name not in workbook.sheetnames:
    sys.stderr.write(
        f"Error: sheet '{sheet_name}' was not found. "
        f"Available sheets: {', '.join(workbook.sheetnames)}\n"
    )
    sys.exit(1)

sheet = workbook[sheet_name]
rows_to_write = [["supplier_name", "count", "logo_url_final"]]

for row in sheet.iter_rows(min_row=2, max_row=sheet.max_row, min_col=1, max_col=3, values_only=True):
    supplier_name, count_value, logo_url_final = row

    if isinstance(supplier_name, str):
        supplier_name = supplier_name.strip()
    if isinstance(logo_url_final, str):
        logo_url_final = logo_url_final.strip()

    if not supplier_name:
        continue
    if supplier_name == "Grand Total":
        continue

    rows_to_write.append([supplier_name, count_value, logo_url_final])

with open(tmp_csv, "w", newline="", encoding="utf-8") as handle:
    writer = csv.writer(handle)
    writer.writerows(rows_to_write)

print(f"Prepared {len(rows_to_write) - 1} supplier rows for upload.")
PY

echo "Replacing '${FULL_TABLE_ID}' in BigQuery..."
bq load \
  --replace \
  --source_format=CSV \
  --skip_leading_rows=1 \
  "$FULL_TABLE_ID" \
  "$TMP_CSV" \
  supplier_name:STRING,count:INT64,logo_url_final:STRING

echo "Verification query results:"
bq query \
  --use_legacy_sql=false \
  --format=prettyjson \
  "SELECT
     COUNT(*) AS row_count,
     COUNTIF(supplier_name IS NULL OR TRIM(supplier_name) = '') AS blank_supplier_name_rows,
     COUNTIF(logo_url_final IS NULL OR TRIM(logo_url_final) = '') AS blank_logo_rows,
     COUNTIF(count IS NULL) AS null_count_rows
   FROM \`${PROJECT_ID}.${DATASET_ID}.${TABLE_ID}\`"
