# =============================================================================
# xlsx_reader.py -- Tolerant, dependency-free reader for .xlsx workbooks.
#
# Purpose:
#   Read network/MSA Excel workbooks without third-party libraries. openpyxl
#   raises TypeError on the ESPN MSA workbooks ("PrintPageSetup.__init__() got
#   an unexpected keyword argument 'collated'") because it strictly validates
#   OOXML attributes that newer Excel writers emit. This reader parses the zip
#   container and sheet XML directly, so unknown attributes are simply ignored.
#
# Inputs:
#   A path to an .xlsx file. Excel lock files ("~$name.xlsx") are not workbooks
#   and must be filtered out by the caller (see discover.py).
#
# Outputs:
#   Workbook -> list of Sheet objects. Each Sheet exposes a sparse cell grid
#   keyed by (column_number, row_number), 1-based, matching Excel's own
#   addressing so profile authors can write anchors as "A2" or "B14".
#
# Safe usage:
#   Read-only; never writes to the source file. Values come from cached results
#   (the <v> element), so formulas resolve to whatever Excel last computed. A
#   workbook saved by a tool that does not cache results will yield empty cells
#   rather than wrong ones.
# =============================================================================

import datetime
import re
import zipfile
from xml.etree import ElementTree as ET


# * SECTION [1]: OOXML CONSTANTS

  # Description: Namespaces and the number-format ids that mark a numeric cell
  # as a date. Excel stores dates as serial numbers, so the only way to tell
  # 45838 (a date) from 45838 (a quantity) is the cell's number format.

NS = {
    "m": "http://schemas.openxmlformats.org/spreadsheetml/2006/main",
    "r": "http://schemas.openxmlformats.org/officeDocument/2006/relationships",
    "pr": "http://schemas.openxmlformats.org/package/2006/relationships",
}

# Built-in numFmtIds reserved by the spec for date and time formats.
BUILTIN_DATE_FORMATS = set(range(14, 23)) | set(range(45, 48))

# Excel's day-zero. 1899-12-30 rather than 1899-12-31 absorbs the intentional
# 1900 leap-year bug, so serial 45838 resolves to 2025-06-30 as Excel displays it.
EXCEL_EPOCH = datetime.date(1899, 12, 30)

_CELL_REF = re.compile(r"([A-Z]+)(\d+)")


# * SECTION [2]: CELL ADDRESS HELPERS

  # Description: Convert between Excel's letter-based column addresses and the
  # integer indices used internally by the grid.

  # ? Split an "A1" style reference into (column_number, row_number)
def parse_ref(ref):
    match = _CELL_REF.match(ref)
    if not match:
        return None
    column = 0
    for char in match.group(1):
        column = column * 26 + (ord(char) - 64)
    return column, int(match.group(2))


  # ? Render a column number back to its Excel letters (1 -> "A", 27 -> "AA")
def column_letter(column):
    letters = ""
    while column > 0:
        column, remainder = divmod(column - 1, 26)
        letters = chr(65 + remainder) + letters
    return letters


# * SECTION [3]: SHEET

  # Description: A single worksheet as a sparse grid. Blank and whitespace-only
  # cells are omitted entirely, so `len(sheet.cells)` reflects real content and
  # membership tests double as "is this cell populated?".

class Sheet:
    def __init__(self, name, cells, merges):
        self.name = name
        self.cells = cells
        self.merges = merges

    @property
    def max_row(self):
        return max((row for _, row in self.cells), default=0)

    @property
    def max_column(self):
        return max((column for column, _ in self.cells), default=0)

      # ? Read one cell by Excel reference, returning None when empty
    def at(self, ref):
        key = parse_ref(ref)
        return self.cells.get(key) if key else None

      # ? Read one cell as stripped text, returning "" when empty
    def text(self, ref):
        value = self.at(ref)
        return "" if value is None else str(value).strip()

      # ? Yield populated rows as (row_number, {column_number: value})
    def rows(self):
        grouped = {}
        for (column, row), value in self.cells.items():
            grouped.setdefault(row, {})[column] = value
        for row in sorted(grouped):
            yield row, grouped[row]

      # ? Find the first cell whose text matches a predicate, scanning row-major
    def find(self, predicate, max_row=None):
        for row, values in self.rows():
            if max_row is not None and row > max_row:
                break
            for column in sorted(values):
                if predicate(str(values[column])):
                    return column, row
        return None


# * SECTION [4]: WORKBOOK

  # Description: Parse the zip container once and expose sheets in workbook
  # order. Sheet order matters because MSA group workbooks put a Table of
  # Contents first and then one sheet per proposal.

class Workbook:
    def __init__(self, path):
        self.path = str(path)
        self.sheets = []
        self._load()

    @property
    def sheet_names(self):
        return [sheet.name for sheet in self.sheets]

      # ? Fetch a sheet by exact name, or None
    def sheet(self, name):
        for candidate in self.sheets:
            if candidate.name == name:
                return candidate
        return None

    def _load(self):
        with zipfile.ZipFile(self.path) as archive:
            names = set(archive.namelist())
            shared = self._read_shared_strings(archive, names)
            date_styles = self._read_date_styles(archive, names)
            for name, target in self._read_sheet_index(archive):
                if target not in names:
                    continue
                cells, merges = self._read_sheet(
                    archive.read(target), shared, date_styles
                )
                self.sheets.append(Sheet(name, cells, merges))

      # ? Resolve sheet names to their XML parts via the workbook relationships
    def _read_sheet_index(self, archive):
        workbook = ET.fromstring(archive.read("xl/workbook.xml"))
        relationships = {}
        for relationship in ET.fromstring(archive.read("xl/_rels/workbook.xml.rels")):
            target = relationship.get("Target", "").lstrip("/")
            if not target.startswith("xl/"):
                target = "xl/" + target
            relationships[relationship.get("Id")] = target
        index = []
        container = workbook.find("m:sheets", NS)
        for sheet in container if container is not None else []:
            rid = sheet.get("{%s}id" % NS["r"])
            if rid in relationships:
                index.append((sheet.get("name", ""), relationships[rid]))
        return index

      # ? Flatten the shared string table, joining rich-text runs into one value
    def _read_shared_strings(self, archive, names):
        if "xl/sharedStrings.xml" not in names:
            return []
        root = ET.fromstring(archive.read("xl/sharedStrings.xml"))
        text_tag = "{%s}t" % NS["m"]
        return [
            "".join(node.text or "" for node in item.iter(text_tag)) for item in root
        ]

      # ? Determine which cell styles format their number as a date
    def _read_date_styles(self, archive, names):
        if "xl/styles.xml" not in names:
            return set()
        root = ET.fromstring(archive.read("xl/styles.xml"))
        # Custom formats are dates when the format code mentions a date part.
        # Guard against the "red for negatives" colour codes, which use [Red].
        date_format_ids = set(BUILTIN_DATE_FORMATS)
        formats = root.find("m:numFmts", NS)
        for entry in formats if formats is not None else []:
            code = (entry.get("formatCode") or "").lower()
            code = re.sub(r"\[[^\]]*\]", "", code)
            if any(part in code for part in ("yy", "dd", "mmm")) or re.search(
                r"\bd\b|\bm/|/d", code
            ):
                date_format_ids.add(int(entry.get("numFmtId")))
        date_styles = set()
        cell_formats = root.find("m:cellXfs", NS)
        for index, entry in enumerate(cell_formats if cell_formats is not None else []):
            if int(entry.get("numFmtId", 0)) in date_format_ids:
                date_styles.add(index)
        return date_styles

      # ? Parse one worksheet's cells and merged ranges
    def _read_sheet(self, payload, shared, date_styles):
        root = ET.fromstring(payload)
        cells = {}
        sheet_data = root.find("m:sheetData", NS)
        for row in sheet_data if sheet_data is not None else []:
            for cell in row:
                key = parse_ref(cell.get("r") or "")
                if key is None:
                    continue
                value = self._cell_value(cell, shared, date_styles)
                if value is None:
                    continue
                if isinstance(value, str) and not value.strip():
                    continue
                cells[key] = value
        merges = []
        merge_container = root.find("m:mergeCells", NS)
        for merge in merge_container if merge_container is not None else []:
            ref = merge.get("ref")
            if ref and ":" in ref:
                start, end = ref.split(":", 1)
                start_key, end_key = parse_ref(start), parse_ref(end)
                if start_key and end_key:
                    merges.append((start_key, end_key))
        return cells, merges

      # ? Convert one cell element to a Python str, float, or date
    def _cell_value(self, cell, shared, date_styles):
        cell_type = cell.get("t")
        if cell_type == "inlineStr":
            inline = cell.find("m:is", NS)
            if inline is None:
                return None
            text_tag = "{%s}t" % NS["m"]
            return "".join(node.text or "" for node in inline.iter(text_tag))
        raw = cell.find("m:v", NS)
        if raw is None or raw.text is None:
            return None
        text = raw.text
        if cell_type == "s":
            try:
                return shared[int(text)]
            except (ValueError, IndexError):
                return None
        if cell_type == "b":
            return "TRUE" if text == "1" else "FALSE"
        if cell_type in ("str", "e"):
            return text
        if cell_type == "d":
            return text
        # Numeric. A date style is the only signal that separates a serial date
        # from an ordinary number, so check the style before returning a float.
        try:
            number = float(text)
        except ValueError:
            return text
        style = int(cell.get("s", 0))
        if style in date_styles and 1 <= number <= 2958465:
            return EXCEL_EPOCH + datetime.timedelta(days=int(number))
        return number


# * SECTION [5]: CONVENIENCE ENTRY POINT

  # Description: Single call used by the extract stage and the tests.

  # ? Open a workbook, raising a clear error if the file is not a zip container
def load_workbook(path):
    try:
        return Workbook(path)
    except zipfile.BadZipFile as error:
        raise ValueError("not a valid .xlsx container: %s" % path) from error
