# =============================================================================
# common.py -- Shared parsing, normalization, and matching primitives.
#
# Purpose:
#   Holds the small number of rules that every stage depends on and that must
#   behave identically everywhere: how a program name is normalized before
#   matching, how a spot date becomes a broadcast week, how source numbers are
#   read (including the NR sentinel), and how (000) values are scaled.
#
# Inputs / outputs:
#   Pure functions only. No file or network access, so every rule here is
#   directly unit-testable and produces identical results on any agent.
#
# Safe usage:
#   Changing anything in this module changes matching behaviour globally.
#   Each rule below is derived from verified evidence in the sample files;
#   see reference/matching_rules.md and reference/unit_rules.md.
# =============================================================================

import datetime
import re


# * SECTION [1]: NUMERIC PARSING

  # Description: MSA and network reports use sentinels and formatting that must
  # not be coerced to zero. "NR" means audience below the reportable minimum --
  # explicitly NOT zero, per the footnote printed on every MSA detail report.
  # Treating it as 0 would understate delivered impressions.

# Values that mean "no reportable figure", which must become None, never 0.
NULL_SENTINELS = {"", "-", "--", "N/A", "NA", "NR", "NULL", "NONE", "*"}

_NUMBER_CHARS = re.compile(r"[^0-9eE.+-]")


  # ? Parse a source cell into a float, returning None for blanks and sentinels
def to_number(value):
    if value is None:
        return None
    if isinstance(value, bool):
        return None
    if isinstance(value, (int, float)):
        return float(value)
    text = str(value).strip()
    if text.upper() in NULL_SENTINELS:
        return None
    negative = text.startswith("(") and text.endswith(")")
    cleaned = _NUMBER_CHARS.sub("", text.replace(",", ""))
    if cleaned in ("", "-", "+", ".", "-.", "+."):
        return None
    try:
        number = float(cleaned)
    except ValueError:
        return None
    return -number if negative else number


  # ? True when a token is an explicit "not reportable" marker rather than blank
def is_not_reportable(value):
    return value is not None and str(value).strip().upper() == "NR"


# * SECTION [2]: UNIT RULES

  # Description: Prisma carries raw impressions (1688000) while every network and
  # MSA figure is expressed in thousands (975). Mixing the two silently produces
  # a 1000x error, so scaling is a single named function used by every handler.

THOUSANDS = 1000.0


  # ? Convert an impressions value expressed in (000) to a raw impression count
def thousands_to_raw(value):
    number = to_number(value)
    return None if number is None else number * THOUSANDS


  # ? Convert a duration label (":030", ":15", "30") to seconds
def duration_seconds(value):
    if value is None:
        return None
    text = str(value).strip().lstrip(":")
    number = to_number(text)
    return None if number is None else int(round(number))


  # ? Convert a raw spot count to 30-second-equivalent units
def thirty_second_equivalent(units_raw, length_seconds):
    # MSA reports are "30 SEC EQUIVALENCED": a :15 counts as half a unit. When
    # the source prints its own equivalenced total, prefer that over this
    # derivation; this exists for sources that print only raw counts.
    if units_raw is None or not length_seconds:
        return None
    return units_raw * (float(length_seconds) / 30.0)


# * SECTION [3]: DATE RULES

  # Description: Prisma's `date` is predominantly a broadcast-week Monday anchor
  # (1194/1700 National and 571/767 Local rows fall on a Monday), while postlogs
  # carry actual air dates. Matching therefore needs the Monday that contains a
  # given air date. Verified against the ESPN Flow Chart, whose own
  # "Broadcast Week" column holds exactly these Mondays.

_DATE_PATTERNS = (
    "%Y-%m-%d",
    "%m/%d/%Y",
    "%m/%d/%y",
    "%m-%d-%Y",
    "%d-%b-%Y",
    "%b %d, %Y",
    "%B %d, %Y",
)


  # ? Coerce a cell value into a date, returning None when unparseable
def to_date(value):
    if value is None:
        return None
    if isinstance(value, datetime.datetime):
        return value.date()
    if isinstance(value, datetime.date):
        return value
    text = str(value).strip()
    if not text or text.upper() in NULL_SENTINELS:
        return None
    for pattern in _DATE_PATTERNS:
        try:
            return datetime.datetime.strptime(text, pattern).date()
        except ValueError:
            continue
    return None


  # ? The Monday of the broadcast week containing a date
def broadcast_week(value):
    date = to_date(value)
    if date is None:
        return None
    return date - datetime.timedelta(days=date.weekday())


  # ? Broadcast quarter label ("3Q2025") for a date
def quarter_label(value):
    date = to_date(value)
    if date is None:
        return None
    return "%dQ%d" % ((date.month - 1) // 3 + 1, date.year)


# * SECTION [4]: PROGRAM NAME NORMALIZATION

  # Description: Prisma truncates program names to about 25 characters and
  # replaces punctuation with spaces (518 sample rows are exactly 25 chars), so
  # "TENNIS: US OPEN - MENS SEMIFINALS" arrives as "TENNIS  US OPEN   MENS S".
  # Normalizing both sides the same way turns most of this into a prefix test.

_NON_ALNUM = re.compile(r"[^A-Z0-9]+")

# Minimum normalized prefix length accepted as a match. Short prefixes such as
# "TENNIS" would collide across unrelated programs.
MIN_PREFIX_LENGTH = 12


  # ? Normalize a program name for comparison: upper, punctuation to space
def normalize_program(value):
    if value is None:
        return ""
    return _NON_ALNUM.sub(" ", str(value).upper()).strip()


  # ? Collapse a normalized name further, removing spaces, for prefix testing
def prefix_key(value):
    return normalize_program(value).replace(" ", "")


  # ? True when the (shorter) Prisma name is a normalized prefix of the postlog
def is_prefix_match(prisma_program, postlog_program):
    short = prefix_key(prisma_program)
    long = prefix_key(postlog_program)
    if len(short) < MIN_PREFIX_LENGTH or not long:
        return False
    return long.startswith(short)


# * SECTION [5]: TEXT HELPERS

  # Description: MSA workbooks pad label cells ("PROP #:            876880") and
  # network names vary in punctuation ("ABC (ESPN TB)").

  # ? Strip a trailing label prefix and surrounding whitespace from a cell
def after_label(text, label):
    if text is None:
        return ""
    body = str(text)
    upper = body.upper()
    marker = label.upper()
    if marker in upper:
        body = body[upper.index(marker) + len(marker):]
    return body.lstrip(":").strip()


  # ? Normalize an outlet/network name for alias lookup
def normalize_outlet(value):
    return _NON_ALNUM.sub(" ", str(value or "").upper()).strip()


  # ? Collapse runs of whitespace to single spaces
def squeeze(value):
    return re.sub(r"\s+", " ", str(value or "")).strip()


# * SECTION [6]: DEMOGRAPHIC CODES

  # Description: The same demo is spelled three ways across these sources --
  # "P 25-54" on a flow chart cover page, "P25-54" in an MSA report, and
  # "P2554" in a unit-by-unit grid. They must collapse to one code or the demo
  # detail table would carry three rows for one demo.

  # ? Normalize a demo label to a comparable code ("P 25-54" -> "P2554")
def normalize_demo(value):
    if value is None:
        return None
    code = re.sub(r"[^A-Z0-9]", "", str(value).upper())
    if not code or code in ("NONE", "NA"):
        return None
    return code


  # ? Demo-like tokens found in a report header line, in order of appearance
DEMO_TOKEN = re.compile(r"\b(HH|[PAWMC]\d{1,2}\s?-\s?\d{1,2}\+?|[PAWM]\d{1,2}\+)\b")


  # ? Extract ordered, de-duplicated demo codes from a header line
def demos_in_text(text):
    found = []
    for match in DEMO_TOKEN.finditer(str(text or "").upper()):
        code = normalize_demo(match.group(1))
        if code and code not in found:
            found.append(code)
    return found
