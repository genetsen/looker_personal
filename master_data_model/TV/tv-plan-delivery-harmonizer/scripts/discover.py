# =============================================================================
# discover.py -- Stage 1. Find candidate files and fingerprint them to profiles.
#
# Purpose:
#   Decide WHICH parser handles each input file, using evidence inside the file
#   rather than its name alone. Filenames are the least reliable signal in this
#   domain: the same report arrives as "ESPNETS_3Q25_CS_..." from one sender and
#   "Copy of postlog (2).xlsx" from another. Anchor text inside the document is
#   stable, so fingerprints score both and require content evidence to match.
#
# Inputs:
#   One or more file or directory paths, plus the profile directory.
#
# Outputs:
#   A list of Discovery records: path, media, resolved profile (or None), the
#   score that resolved it, and the opened document handle for reuse by extract.
#
# Safe usage:
#   Read-only. Excel lock files ("~$*.xlsx") are skipped -- they are not
#   workbooks and opening them raises. Files that match no profile are returned
#   with profile=None so the caller can route them to the fallback path; they
#   are never silently dropped.
# =============================================================================

import json
import os
import re

import pdf_reader
import xlsx_reader


# * SECTION [1]: FILE SELECTION

  # Description: Only three media types are meaningful here. Anything else in a
  # dropped folder (images, .msg, .zip) is reported as skipped rather than
  # failing the run.

XLSX_EXTENSIONS = {".xlsx", ".xlsm"}
PDF_EXTENSIONS = {".pdf"}
CSV_EXTENSIONS = {".csv"}

# Excel writes a lock file alongside an open workbook. It shares the .xlsx
# extension but is not a zip container.
LOCK_PREFIX = "~$"


  # ? Classify a path into a media kind, or None when not of interest
def media_kind(path):
    name = os.path.basename(path)
    if name.startswith(LOCK_PREFIX) or name.startswith("."):
        return None
    extension = os.path.splitext(name)[1].lower()
    if extension in XLSX_EXTENSIONS:
        return "xlsx"
    if extension in PDF_EXTENSIONS:
        return "pdf"
    if extension in CSV_EXTENSIONS:
        return "csv"
    return None


  # ? Expand file and directory inputs into a sorted list of candidate files
def candidate_files(paths):
    found = []
    for path in paths:
        if os.path.isdir(path):
            for root, _dirs, names in os.walk(path):
                for name in sorted(names):
                    found.append(os.path.join(root, name))
        elif os.path.isfile(path):
            found.append(path)
    selected = []
    for path in sorted(set(found)):
        kind = media_kind(path)
        if kind:
            selected.append((path, kind))
    return selected


# * SECTION [2]: PROFILE LOADING

  # Description: Profiles are JSON rather than YAML so the skill needs no
  # third-party parser and behaves identically on every agent. Drafts written by
  # the fallback path live in formats/_drafts and are not loaded automatically;
  # they must be reviewed and promoted deliberately.

  # ? Load every profile in the registry, skipping the drafts directory
def load_profiles(formats_dir):
    profiles = []
    for name in sorted(os.listdir(formats_dir)):
        if not name.endswith(".json"):
            continue
        path = os.path.join(formats_dir, name)
        if not os.path.isfile(path):
            continue
        with open(path, "r", encoding="utf-8") as handle:
            profile = json.load(handle)
        profile["_path"] = path
        profiles.append(profile)
    return profiles


# * SECTION [3]: FINGERPRINT SCORING

  # Description: Each satisfied criterion adds weight; content evidence is worth
  # more than a filename. A profile only wins if it clears its own min_score, so
  # a coincidental filename match cannot select the wrong parser.

WEIGHT_FILENAME = 1
WEIGHT_SHEET = 1
WEIGHT_CELL_EQUALS = 2
WEIGHT_CELL_CONTAINS = 1
WEIGHT_TEXT = 1


  # ? True when any regex in a list matches the text
def _any_regex(patterns, text):
    return any(re.search(pattern, text, re.IGNORECASE) for pattern in patterns or [])


  # ? Score one profile against an opened workbook
def _score_xlsx(fingerprint, path, workbook):
    score = 0
    detail = []
    if _any_regex(fingerprint.get("filename_regex"), os.path.basename(path)):
        score += WEIGHT_FILENAME
        detail.append("filename")
    # A disqualifying sheet means this is a different report shape entirely.
    for pattern in fingerprint.get("sheet_regex_none") or []:
        if any(re.search(pattern, name, re.IGNORECASE) for name in workbook.sheet_names):
            return None, ["disqualified by sheet %s" % pattern]
    for pattern in fingerprint.get("sheet_regex_any") or []:
        if any(re.search(pattern, name, re.IGNORECASE) for name in workbook.sheet_names):
            score += WEIGHT_SHEET
            detail.append("sheet:%s" % pattern)
    for rule in fingerprint.get("cell_equals") or []:
        for sheet in _sheets_matching(workbook, rule.get("sheet_regex")):
            if sheet.text(rule["ref"]).upper() == str(rule["value"]).upper():
                score += WEIGHT_CELL_EQUALS
                detail.append("cell=%s" % rule["ref"])
                break
    for rule in fingerprint.get("cell_contains") or []:
        if _cell_contains(workbook, rule):
            score += WEIGHT_CELL_CONTAINS
            detail.append("contains:%s" % rule["value"])
    return score, detail


  # ? Sheets whose name matches a regex, or all sheets when no regex given
def _sheets_matching(workbook, pattern):
    if not pattern:
        return workbook.sheets
    return [
        sheet
        for sheet in workbook.sheets
        if re.search(pattern, sheet.name, re.IGNORECASE)
    ]


  # ? True when a marker appears at a reference, or anywhere in the first N rows
def _cell_contains(workbook, rule):
    marker = str(rule["value"]).upper()
    limit = rule.get("any_cell_in_rows")
    for sheet in _sheets_matching(workbook, rule.get("sheet_regex")):
        if limit:
            for row, values in sheet.rows():
                if row > int(limit):
                    break
                for value in values.values():
                    if marker in str(value).upper():
                        return True
        elif rule.get("ref") and marker in sheet.text(rule["ref"]).upper():
            return True
    return False


  # ? Score one profile against an extracted PDF document
def _score_pdf(fingerprint, path, document):
    score = 0
    detail = []
    if _any_regex(fingerprint.get("filename_regex"), os.path.basename(path)):
        score += WEIGHT_FILENAME
        detail.append("filename")
    for marker in fingerprint.get("text_contains") or []:
        if document.contains(marker):
            score += WEIGHT_TEXT
            detail.append("text:%s" % marker[:24])
    return score, detail


# * SECTION [4]: DISCOVERY RECORD

  # Description: Carries the opened document so extract does not re-read and
  # re-parse the same file, and so a failed open is reported once with context.

class Discovery:
    def __init__(self, path, media):
        self.path = path
        self.media = media
        self.document = None
        self.profile = None
        self.score = 0
        self.evidence = []
        self.error = None
        self.runners_up = []

    @property
    def format_id(self):
        return self.profile["format_id"] if self.profile else None

    @property
    def resolved(self):
        return self.profile is not None

    def describe(self):
        name = os.path.basename(self.path)
        if self.error:
            return "%s -> ERROR %s" % (name, self.error)
        if not self.resolved:
            return "%s -> no profile (fallback required)" % name
        return "%s -> %s (score %d: %s)" % (
            name,
            self.format_id,
            self.score,
            ", ".join(self.evidence),
        )


# * SECTION [5]: ENTRY POINT

  # Description: Open each candidate once, score every profile, and resolve the
  # single best match. Ambiguity is recorded rather than resolved arbitrarily.

  # ? Discover and fingerprint every candidate file under the given paths
def discover(paths, formats_dir):
    profiles = load_profiles(formats_dir)
    results = []
    for path, media in candidate_files(paths):
        if media == "csv":
            # CSV inputs are the Prisma base, supplied explicitly by the caller,
            # not fingerprinted as a delivery source.
            continue
        record = Discovery(path, media)
        try:
            if media == "xlsx":
                record.document = xlsx_reader.load_workbook(path)
            else:
                record.document = pdf_reader.load_pdf(path)
        except Exception as error:  # noqa: BLE001 - reported, never fatal
            record.error = "%s: %s" % (type(error).__name__, error)
            results.append(record)
            continue
        scored = []
        for profile in profiles:
            if profile.get("media") != media:
                continue
            fingerprint = profile.get("fingerprint") or {}
            if media == "xlsx":
                score, detail = _score_xlsx(fingerprint, path, record.document)
            else:
                score, detail = _score_pdf(fingerprint, path, record.document)
            if score is None:
                continue
            if score >= int(fingerprint.get("min_score", 1)):
                scored.append((score, profile, detail))
        scored.sort(key=lambda item: item[0], reverse=True)
        if scored:
            record.score, record.profile, record.evidence = scored[0]
            record.runners_up = [
                (item[1]["format_id"], item[0]) for item in scored[1:]
            ]
        results.append(record)
    return results
