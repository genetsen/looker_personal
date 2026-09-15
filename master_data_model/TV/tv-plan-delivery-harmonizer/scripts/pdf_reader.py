# =============================================================================
# pdf_reader.py -- Layout-preserving text extraction for PDF postlogs.
#
# Purpose:
#   MSA detail postlogs (DTL_*.pdf, GRPDTL_*.pdf) are column-aligned reports.
#   Their spot rows are only parseable if the column spacing survives text
#   extraction, so this module shells out to `pdftotext -layout` rather than
#   reconstructing text from raw PDF operators.
#
# Inputs:
#   A path to a .pdf file. Requires the poppler `pdftotext` binary on PATH
#   (available at /opt/homebrew/bin/pdftotext on this machine).
#
# Outputs:
#   Document -> full text plus a per-page list, and small helpers for reading
#   the "Label: value" header block that precedes every MSA detail report.
#
# Safe usage:
#   Read-only and side-effect free; writes extracted text to stdout only.
#   Raises MissingDependency with an actionable message when pdftotext is
#   absent, so a run fails loudly instead of silently skipping PDF delivery.
# =============================================================================

import re
import shutil
import subprocess


# * SECTION [1]: DEPENDENCY GUARD

  # Description: PDF delivery data is not optional. If the extractor cannot read
  # postlog PDFs, delivered impressions would silently go missing, so the
  # absence of pdftotext is an error rather than a warning.

class MissingDependency(RuntimeError):
    pass


  # ? Locate the pdftotext binary, or explain how to install it
def pdftotext_path():
    found = shutil.which("pdftotext")
    if not found:
        raise MissingDependency(
            "pdftotext not found on PATH. It ships with poppler; "
            "install with `brew install poppler`."
        )
    return found


# * SECTION [2]: DOCUMENT

  # Description: Extracted text for one PDF, split into pages. MSA detail
  # reports repeat their header block on every page and continue spot tables
  # across page breaks, so both the whole-document text and the page list are
  # useful to profile authors.

class Document:
    def __init__(self, path, text):
        self.path = str(path)
        self.text = text
        # pdftotext separates pages with a form feed.
        self.pages = [page for page in text.split("\f")]

    @property
    def lines(self):
        return self.text.splitlines()

      # ? Read a "Label: value" header field, tolerating padded spacing
    def labeled(self, label, default=""):
        pattern = re.compile(
            r"^\s*%s\s*:\s*(.+?)\s*$" % re.escape(label), re.MULTILINE
        )
        match = pattern.search(self.text)
        return match.group(1).strip() if match else default

      # ? True when the document contains a marker string (used by fingerprints)
    def contains(self, marker):
        return marker.upper() in self.text.upper()


# * SECTION [3]: ENTRY POINT

  # Description: Single call used by the extract stage and the tests.

  # ? Extract layout-preserved text from a PDF
def load_pdf(path):
    binary = pdftotext_path()
    result = subprocess.run(
        [binary, "-layout", "-enc", "UTF-8", str(path), "-"],
        capture_output=True,
        check=False,
    )
    if result.returncode != 0:
        detail = result.stderr.decode("utf-8", "replace").strip()
        raise ValueError("pdftotext failed on %s: %s" % (path, detail))
    return Document(path, result.stdout.decode("utf-8", "replace"))
