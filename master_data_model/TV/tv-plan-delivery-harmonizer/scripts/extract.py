# =============================================================================
# extract.py -- Stage 2. Turn fingerprinted files into canonical staging rows.
#
# Purpose:
#   Each handler knows the SHAPE of one family of reports and emits the same
#   canonical records, so every stage after this one is source-agnostic. A
#   profile declares identity (fingerprint), which handler to use, and the rules
#   that vary between files of the same shape. It does not attempt a purely
#   declarative field map: these reports are not rectangular (blocks end at a
#   TOTALS row, column layouts differ between sheets of one workbook), and a
#   config language expressive enough to describe them would be harder to review
#   than the handler it replaced.
#
# Inputs:
#   Discovery records from discover.py, each carrying an opened document.
#
# Outputs:
#   ExtractResult with three streams:
#     events        -- one row per countable occurrence (spot / prop-quarter)
#     event_demos   -- one row per event per demo
#     control_totals-- authoritative source-stated totals used for validation
#
# Safe usage:
#   Handlers never invent values. Anything unreadable lowers extract_confidence
#   and is recorded in notes rather than defaulted, so a parsing failure is
#   visible in the reconciliation report instead of appearing as a zero.
# =============================================================================

import os
import re

import common
import schema


# * SECTION [1]: RESULT CONTAINER

  # Description: Accumulates all three streams plus human-readable notes. Notes
  # are surfaced in the reconciliation report, which is how a partial parse
  # becomes visible instead of silent.

class ExtractResult:
    def __init__(self):
        self.events = []
        self.event_demos = []
        self.control_totals = []
        self.notes = []

    def extend(self, other):
        self.events.extend(other.events)
        self.event_demos.extend(other.event_demos)
        self.control_totals.extend(other.control_totals)
        self.notes.extend(other.notes)

    def note(self, message):
        self.notes.append(message)


  # ? Build a deterministic event key so re-runs produce identical output
def event_key(format_id, path, scope, index):
    return "%s:%s:%s:%s" % (format_id, os.path.basename(path), scope, index)


  # ? Standard provenance block for one source file
def provenance(discovery, row_grain, method="profile", confidence=1.0):
    return {
        "source_file": os.path.basename(discovery.path),
        "source_format_id": discovery.format_id,
        "extract_method": method,
        "extract_confidence": confidence,
        "row_grain": row_grain,
    }


# * SECTION [2]: HANDLER -- ESPN FLOW CHART

  # Description: The flow chart is the network's own view of the deal: a Cover
  # Page of deal-level metadata and booked totals, then one "<network> Unit by
  # Unit" sheet per network carrying every ordered spot with its estimated
  # audience, an Actualized flag, and invoice identifiers.
  #
  # This is the only source that supplies billed evidence (invoice number and
  # date) at spot level, so it drives the act_billed_* family.

# Column A of Unit by Unit is a date-styled cell holding the broadcast-week
# Monday, which is the same anchor Prisma uses. Column B is the actual air date.
FLOWCHART_SHEET_SUFFIX = " Unit by Unit"

# Header labels, matched after whitespace squeezing because the source wraps
# them across lines ("Estimated\nImpressions(000)").
_FC_HEADERS = {
    "broadcast week": "broadcast_week",
    "date": "air_date",
    "program description": "program_raw",
    "brand": "brand",
    "campaign": "campaign_raw",
    "length": "length_seconds",
    "rate": "cost",
    "value": "value_type",
    "spot type": "spot_type",
    "feature type": "feature_type",
    "actualized": "actualized_flag",
    "hit time": "spot_time",
    "spot position": "spot_position",
    "isci": "isci",
    "invoice": "invoice_no",
    "invoice date": "invoice_date",
    "order id": "order_id",
    "unit id": "unit_id",
}

# Demo columns arrive as a demo label followed by its impressions column, and
# the impressions header repeats verbatim for each demo, so pairing has to be
# positional rather than by name.
_FC_DEMO_LABEL = re.compile(r"^(primary|secondary|additional)\s+demo", re.IGNORECASE)
_FC_DEMO_VALUE = re.compile(r"impressions", re.IGNORECASE)


  # ? Read the Cover Page "Label: value" pairs wherever they sit on the sheet
def _flowchart_cover(workbook):
    cover = workbook.sheet("Cover Page")
    fields = {}
    if cover is None:
        return fields
    for _row, values in cover.rows():
        for value in values.values():
            text = common.squeeze(value)
            if ":" not in text:
                continue
            label, _, body = text.partition(":")
            label = common.squeeze(label).lower()
            body = common.squeeze(body)
            if label and body:
                fields.setdefault(label, body)
    return fields


  # ? Map a Unit by Unit header row to field names and ordered demo slots
def _flowchart_header(sheet):
    mapping = {}
    demo_slots = []
    pending_demo = None
    header = {}
    for (column, row), value in sheet.cells.items():
        if row == 1:
            header[column] = value
    for column in sorted(header):
        label = common.squeeze(header[column]).lower()
        if _FC_DEMO_LABEL.match(label):
            role = label.split()[0]
            pending_demo = {"role": role, "label_column": column, "value_column": None}
            demo_slots.append(pending_demo)
            continue
        if _FC_DEMO_VALUE.search(label):
            # Belongs to the most recent demo label column.
            if pending_demo is not None and pending_demo["value_column"] is None:
                pending_demo["value_column"] = column
            continue
        if label in _FC_HEADERS:
            mapping[_FC_HEADERS[label]] = column
    return mapping, demo_slots


  # ? Extract spots and booked control totals from one flow chart workbook
def handle_espn_flowchart(discovery, profile):
    result = ExtractResult()
    workbook = discovery.document
    cover = _flowchart_cover(workbook)

    advertiser = cover.get("advertiser", "")
    agency = cover.get("agency", "")
    deal_title = cover.get("deal title", "")
    quarter = common.squeeze(cover.get("quarter/year", "")).replace("'", "")
    guaranteed = cover.get("guaranteed", "").strip().lower()
    gtd_basis = "GTD" if guaranteed.startswith("y") else "EST"
    cover_demo = common.normalize_demo(cover.get("primary demo"))
    rating_stream = cover.get("rating stream") or cover.get("rating type")
    # "Show Actualized Imps: N" means the workbook carries estimated audience
    # only, so any impressions read here are an estimate vintage, not final.
    shows_actualized = (cover.get("show actualized imps") or "N").strip().upper() == "Y"
    vintage = "final" if shows_actualized else "estimate"

    sheets = [
        sheet
        for sheet in workbook.sheets
        if sheet.name.endswith(FLOWCHART_SHEET_SUFFIX)
    ]
    if not sheets:
        result.note(
            "%s: no 'Unit by Unit' sheet found; no spots extracted"
            % os.path.basename(discovery.path)
        )
        return result

    for sheet in sheets:
        network = sheet.name[: -len(FLOWCHART_SHEET_SUFFIX)].strip()
        mapping, demo_slots = _flowchart_header(sheet)
        missing = [name for name in ("air_date", "program_raw") if name not in mapping]
        if missing:
            result.note(
                "%s / %s: header missing %s; sheet skipped"
                % (os.path.basename(discovery.path), sheet.name, ", ".join(missing))
            )
            continue
        for row, values in sheet.rows():
            if row == 1:
                continue
            air_date = common.to_date(values.get(mapping.get("air_date")))
            program = values.get(mapping.get("program_raw"))
            if air_date is None or not program:
                continue
            key = event_key(discovery.format_id, discovery.path, sheet.name, row)
            length = common.duration_seconds(values.get(mapping.get("length_seconds")))
            units_raw = 1.0
            week = common.to_date(values.get(mapping.get("broadcast_week")))
            record = schema.blank(
                schema.EVENT_FIELDS,
                **provenance(discovery, profile.get("row_grain", "spot"))
            )
            record.update(
                advertiser_raw=advertiser,
                agency=agency,
                deal_title=deal_title,
                quarter=quarter or common.quarter_label(air_date),
                network_raw=network,
                brand=common.squeeze(values.get(mapping.get("brand"))) or None,
                campaign_raw=common.squeeze(values.get(mapping.get("campaign_raw"))) or None,
                air_date=air_date,
                # Prefer the network's own week column; fall back to derivation.
                broadcast_week=week or common.broadcast_week(air_date),
                program_raw=common.squeeze(program),
                program_norm=common.normalize_program(program),
                spot_time=common.squeeze(values.get(mapping.get("spot_time"))) or None,
                spot_position=common.squeeze(values.get(mapping.get("spot_position"))) or None,
                length_seconds=length,
                spot_type=common.squeeze(values.get(mapping.get("spot_type"))) or None,
                value_type=common.squeeze(values.get(mapping.get("value_type"))) or None,
                feature_type=common.squeeze(values.get(mapping.get("feature_type"))) or None,
                unit_id=common.squeeze(values.get(mapping.get("unit_id"))) or None,
                isci=common.squeeze(values.get(mapping.get("isci"))) or None,
                invoice_no=common.squeeze(values.get(mapping.get("invoice_no"))) or None,
                invoice_date=common.to_date(values.get(mapping.get("invoice_date"))),
                cost=common.to_number(values.get(mapping.get("cost"))),
                units_raw=units_raw,
                units_30eq=common.thirty_second_equivalent(units_raw, length),
                actualized_flag=common.squeeze(values.get(mapping.get("actualized_flag"))) or None,
                gtd_basis=gtd_basis,
                rating_stream=common.squeeze(rating_stream) or None,
            )
            record["event_key"] = key
            result.events.append(record)

            for index, slot in enumerate(demo_slots):
                demo_code = common.normalize_demo(values.get(slot["label_column"]))
                if demo_code is None and index == 0:
                    demo_code = cover_demo
                impressions = common.thousands_to_raw(
                    values.get(slot["value_column"])
                ) if slot["value_column"] else None
                if demo_code is None or impressions is None:
                    continue
                demo_row = schema.blank(
                    schema.EVENT_DEMO_FIELDS,
                    **provenance(discovery, profile.get("row_grain", "spot"))
                )
                demo_row.update(
                    event_key=key,
                    demo_code=demo_code,
                    demo_role=slot["role"].lower(),
                    imps_est=impressions,
                    imps_vintage=vintage,
                )
                result.event_demos.append(demo_row)

    # Cover Page booked totals are the network's own control figure for the deal.
    booked = _flowchart_booked_totals(workbook)
    for entry in booked:
        entry.update(
            source_file=os.path.basename(discovery.path),
            source_format_id=discovery.format_id,
            scope="deal_booked",
            advertiser_raw=advertiser,
            deal_title=deal_title,
            quarter=quarter,
        )
        result.control_totals.append(entry)
    return result


  # ? Read the Cover Page "Quarterly Totals" block (Total Paid Media + per-network)
def _flowchart_booked_totals(workbook):
    cover = workbook.sheet("Cover Page")
    totals = []
    if cover is None:
        return totals
    # The block is laid out as label column C with Proposed/Booked/Pending in
    # D/E/F. Find the "Booked" header to locate the column rather than assuming.
    booked_column = None
    label_column = None
    for (column, row), value in cover.cells.items():
        if common.squeeze(value).lower() == "booked":
            booked_column = column
            header_row = row
            break
    else:
        return totals
    for (column, row), value in cover.cells.items():
        if row == header_row and common.squeeze(value).lower() == "quarterly totals":
            label_column = column
    if label_column is None:
        label_column = booked_column - 2
    for row, values in cover.rows():
        label = common.squeeze(values.get(label_column))
        if not label:
            continue
        amount = common.to_number(values.get(booked_column))
        if amount is None:
            continue
        lowered = label.lower()
        if "total paid media" in lowered:
            totals.append({"metric": "booked_cost", "network_raw": None, "value": amount})
        elif lowered.endswith(":") and "cpm" not in lowered:
            totals.append(
                {
                    "metric": "booked_cost",
                    "network_raw": label.rstrip(":").strip(),
                    "value": amount,
                }
            )
        elif "(000)" in lowered:
            totals.append(
                {
                    "metric": "booked_impressions",
                    "network_raw": None,
                    "value": amount * common.THOUSANDS,
                    "demo_code": common.normalize_demo(label.replace("(000)", "")),
                }
            )
    return totals


# * SECTION [3]: HANDLER -- MSA POST ANALYSIS (CS AND GS WORKBOOKS)

  # Description: Post analysis states guaranteed-or-estimated against delivered
  # audience at proposal/quarter grain. Column layout is NOT fixed: one sheet
  # prints CPM/(000) for both EST and DEL, another prints DEL only, and the
  # guarantee word itself changes between EST and GTD. Headers are therefore
  # read by combining the stacked header rows per column.
  #
  # Because these rows are quarter-level, they cannot populate a daily fact row
  # without inventing an allocation. They are emitted as control totals and used
  # by validate.py to check the spot-level sums, which is how a post-buy analyst
  # uses them.

# Rows that roll up other rows on the same sheet. Emitting them would double
# count, so only detail and CASH/ADU rows are kept.
_MSA_ROLLUP = re.compile(r"^(TOTALS?|GROUP TOTAL)$", re.IGNORECASE)
_MSA_NETWORK_ROLLUP = re.compile(r"^\S+\s+TOTAL$", re.IGNORECASE)
_MSA_CASH_ADU = re.compile(r"^(?P<network>.+?)\s+(?P<kind>CASH|ADU)\s+TOTAL$", re.IGNORECASE)
_MSA_ORDER_BRAND = re.compile(r"^(?P<order>\d+)\s*/\s*(?P<brand>.+)$")


  # ? Combine stacked header rows into one label per column
def _msa_header_map(sheet):
    # Locate the row that contains both COST and SPOTS; that is the last header
    # row of the block, with qualifiers ("EST", "DEL") on the same or prior row.
    anchor_row = None
    for row, values in sheet.rows():
        text = " ".join(str(value).upper() for value in values.values())
        if "COST" in text and "SPOTS" in text:
            anchor_row = row
            break
    if anchor_row is None:
        return None, None
    labels = {}
    for offset in (-2, -1, 0):
        for (column, row), value in sheet.cells.items():
            if row != anchor_row + offset:
                continue
            piece = common.squeeze(value).upper()
            if piece:
                labels[column] = (labels.get(column, "") + " " + piece).strip()
    mapping = {}
    for column, label in labels.items():
        if "COST" in label:
            mapping["cost"] = column
        elif "SPOTS" in label:
            mapping["units"] = column
        elif "CPM" in label and "DEL" in label:
            mapping["cpm_del"] = column
        elif "CPM" in label and ("EST" in label or "GTD" in label):
            mapping["cpm_gtd"] = column
            mapping["gtd_basis"] = "GTD" if "GTD" in label else "EST"
        elif "000" in label and "DEL" in label:
            mapping["imps_del"] = column
        elif "000" in label and ("EST" in label or "GTD" in label):
            mapping["imps_gtd"] = column
        elif "INDEX" in label:
            mapping["index_pct"] = column
    return anchor_row, mapping


  # ? Extract prop/quarter control totals from a CS or GS post analysis workbook
def handle_msa_post_analysis(discovery, profile):
    result = ExtractResult()
    workbook = discovery.document
    rules = profile.get("rules") or {}
    skip_patterns = rules.get("skip_sheets_regex") or []
    skip_blocks = [text.upper() for text in rules.get("skip_blocks") or []]

    for sheet in workbook.sheets:
        if any(re.search(pattern, sheet.name, re.IGNORECASE) for pattern in skip_patterns):
            continue
        scope = _msa_sheet_scope(sheet)
        anchor_row, mapping = _msa_header_map(sheet)
        if not mapping or "imps_del" not in mapping:
            continue
        demo = scope.get("demo")
        for row, values in sheet.rows():
            if row <= anchor_row:
                continue
            label = _msa_row_label(values, mapping)
            if not label:
                continue
            upper = label.upper()
            # The "PROP TO DATE" block below the main table repeats prior
            # quarters and would double count the current one.
            if any(marker in upper for marker in skip_blocks):
                break
            if _MSA_ROLLUP.match(upper):
                continue
            network = scope.get("network")
            value_type = "All"
            cash_adu = _MSA_CASH_ADU.match(label)
            order_brand = _MSA_ORDER_BRAND.match(label)
            if cash_adu:
                network = cash_adu.group("network").strip()
                value_type = "ADU" if cash_adu.group("kind").upper() == "ADU" else "Cash"
            elif order_brand:
                # A single order line covers cash and ADU combined at this grain.
                value_type = "All"
            elif _MSA_NETWORK_ROLLUP.match(upper):
                continue
            entry = {
                "source_file": os.path.basename(discovery.path),
                "source_format_id": discovery.format_id,
                "scope": "prop_quarter",
                "sheet": sheet.name,
                "row_label": label,
                "advertiser_raw": scope.get("advertiser"),
                "agency": scope.get("agency"),
                "prop_id": scope.get("prop_id"),
                "group_id": scope.get("group_id"),
                "quarter": scope.get("quarter"),
                "network_raw": network,
                "brand": order_brand.group("brand").strip() if order_brand else None,
                "order_id": order_brand.group("order") if order_brand else None,
                "value_type": value_type,
                "demo_code": demo,
                "gtd_basis": mapping.get("gtd_basis", "NONE"),
                "cost": common.to_number(values.get(mapping.get("cost"))),
                "units_30eq": common.to_number(values.get(mapping.get("units"))),
                "imps_gtd": common.thousands_to_raw(values.get(mapping.get("imps_gtd")))
                if "imps_gtd" in mapping
                else None,
                "imps_del": common.thousands_to_raw(values.get(mapping.get("imps_del"))),
                "cpm_gtd": common.to_number(values.get(mapping.get("cpm_gtd")))
                if "cpm_gtd" in mapping
                else None,
                "cpm_del": common.to_number(values.get(mapping.get("cpm_del")))
                if "cpm_del" in mapping
                else None,
                "index_pct": common.to_number(values.get(mapping.get("index_pct")))
                if "index_pct" in mapping
                else None,
                "metric": "post_analysis",
            }
            result.control_totals.append(entry)
    if not result.control_totals:
        result.note(
            "%s: post analysis header not recognised; no control totals extracted"
            % os.path.basename(discovery.path)
        )
    return result


  # ? Read the labelled scope block at the top of a post analysis sheet
def _msa_sheet_scope(sheet):
    scope = {}
    for row, values in sheet.rows():
        if row > 14:
            break
        for value in values.values():
            text = common.squeeze(value)
            upper = text.upper()
            if upper.startswith("ADVERTISER"):
                scope["advertiser"] = common.after_label(text, "ADVERTISER")
            elif upper.startswith("AGENCY"):
                scope["agency"] = common.after_label(text, "AGENCY")
            elif upper.startswith("PROP #"):
                scope["prop_id"] = common.after_label(text, "PROP #")
            elif upper.startswith("GROUP #"):
                scope["group_id"] = common.after_label(text, "GROUP #")
            elif upper.startswith("QUARTER"):
                scope["quarter"] = common.after_label(text, "QUARTER")
            elif upper.startswith("DEMO"):
                scope["demo"] = common.normalize_demo(common.after_label(text, "DEMO"))
    # Sheet names in CS workbooks carry the network ("ESPN_Linear").
    name = sheet.name.replace("_Linear", "").strip()
    if name.upper() not in ("TOTAL", "TABLE OF CONTENTS"):
        scope["network"] = name
    return scope


  # ? The leading text label of a data row, left of the first measure column
def _msa_row_label(values, mapping):
    measure_columns = [
        column for key, column in mapping.items() if isinstance(column, int)
    ]
    first_measure = min(measure_columns) if measure_columns else None
    parts = []
    for column in sorted(values):
        if first_measure is not None and column >= first_measure:
            break
        text = common.squeeze(values[column])
        if text:
            parts.append(text)
    return " ".join(parts).strip()


# * SECTION [4]: HANDLER -- MSA DEMOGRAPHIC DETAIL PDF

  # Description: The spot-level delivery source. Layout-preserved text is parsed
  # line by line with a state machine tracking the current page's network and the
  # current CASH/ADU section, because both change mid-document.
  #
  # The numeric tail of a spot line is self-validating: it must hold
  # 1 (spots) + 1 (HH rating) + 1 (HH audience) + 2 per reporting demo + 1 (cost)
  # tokens. A mismatch lowers confidence rather than mis-assigning columns.

# "  7/27/25 MLB REGULAR SEASON REPEAT   4:24 AM :015   1.0  NR  NR ... $0"
_PDF_SPOT = re.compile(
    r"^\s+(?P<date>\d{1,2}/\d{1,2}/\d{2})\s+"
    r"(?P<program>.+?)\s{2,}"
    r"(?P<time>\d{1,2}:\d{2}\s*[AP]M)\s+"
    r":(?P<duration>\d{2,3})\s+"
    r"(?P<tail>.+)$"
)
_PDF_SECTION = re.compile(r"^\s{2,6}(CASH|ADU|TRADE|BONUS)\s*$")
_PDF_TOTALS = re.compile(r"(UNEQUIVALENCED|EQUIVALENCED).*TOTAL", re.IGNORECASE)
_PDF_NETWORK = re.compile(r"^\s{20,}(?P<network>[A-Z0-9()\s\-\.]{2,40}?)\s{10,}Page:\s*\d+")
_PDF_PROP = re.compile(r"^\s*Prop #:\s*(?P<prop>\S+)")
_PDF_ORDER = re.compile(r"^\s*Order #:\s*(?P<order>\d+)")
_PDF_GROUP = re.compile(r"^\s*Group #:\s*(?P<group>\S+)")
_PDF_HEADER_DEMOS = re.compile(r"#\s*OF.*AUDIENCE", re.IGNORECASE)


  # ? Extract spot-level delivery from an MSA demographic detail report
def handle_msa_detail_pdf(discovery, profile):
    result = ExtractResult()
    document = discovery.document
    advertiser = document.labeled("Advertiser")
    agency = document.labeled("Agency")
    brand = document.labeled("Brand Name")
    guaranteed_demo = common.normalize_demo(document.labeled("Guaranteed Demo"))
    gtd_basis = "GTD" if guaranteed_demo else "NONE"
    quarter = _pdf_quarter(document.text)

    network = None
    prop_id = None
    order_id = None
    group_id = None
    section = "Cash"
    demos = []
    index = 0

    for line in document.lines:
        network_match = _PDF_NETWORK.match(line)
        if network_match:
            candidate = common.squeeze(network_match.group("network"))
            # The centered page header holds the network; report titles also sit
            # centered, so ignore known title text.
            if candidate.upper() not in ("DEMOGRAPHIC DETAIL REPORT",):
                network = candidate
            continue
        prop_match = _PDF_PROP.match(line)
        if prop_match:
            prop_id = prop_match.group("prop")
            continue
        order_match = _PDF_ORDER.match(line)
        if order_match:
            order_id = order_match.group("order")
            continue
        group_match = _PDF_GROUP.match(line)
        if group_match:
            group_id = group_match.group("group")
            continue
        if _PDF_HEADER_DEMOS.search(line):
            found = common.demos_in_text(line)
            # The first demo token in the header is HH; the rest are reporting
            # demos, each contributing a VPH and an AUDIENCE column.
            demos = [code for code in found if code != "HH"]
            continue
        section_match = _PDF_SECTION.match(line)
        if section_match:
            label = section_match.group(1).upper()
            section = {"CASH": "Cash", "ADU": "ADU", "TRADE": "Trade", "BONUS": "Bonus"}[label]
            continue
        if _PDF_TOTALS.search(line):
            # Section subtotals; excluded so spot delivery is not doubled.
            continue
        spot_match = _PDF_SPOT.match(line)
        if not spot_match:
            continue
        index += 1
        parsed = _pdf_spot_row(spot_match, demos)
        confidence = 1.0 if parsed["tokens_ok"] else 0.6
        if not parsed["tokens_ok"]:
            result.note(
                "%s: spot line had %d numeric tokens, expected %d (demos=%s); "
                "values recorded with reduced confidence: %r"
                % (
                    os.path.basename(discovery.path),
                    parsed["token_count"],
                    parsed["expected"],
                    ",".join(demos) or "unknown",
                    common.squeeze(line)[:90],
                )
            )
        key = event_key(discovery.format_id, discovery.path, "spot", index)
        record = schema.blank(
            schema.EVENT_FIELDS,
            **provenance(discovery, profile.get("row_grain", "spot"), confidence=confidence)
        )
        record.update(
            advertiser_raw=advertiser,
            agency=agency,
            prop_id=prop_id,
            order_id=order_id,
            group_id=group_id,
            brand=brand or None,
            quarter=quarter or common.quarter_label(parsed["air_date"]),
            network_raw=network,
            air_date=parsed["air_date"],
            broadcast_week=common.broadcast_week(parsed["air_date"]),
            program_raw=parsed["program"],
            program_norm=common.normalize_program(parsed["program"]),
            spot_time=parsed["spot_time"],
            length_seconds=parsed["length_seconds"],
            value_type=section,
            spot_type="Commercial",
            cost=parsed["cost"],
            units_raw=parsed["units_raw"],
            units_30eq=common.thirty_second_equivalent(
                parsed["units_raw"], parsed["length_seconds"]
            ),
            gtd_basis=gtd_basis,
        )
        record["event_key"] = key
        result.events.append(record)

        for demo_code, audience, vpvh in parsed["demo_values"]:
            demo_row = schema.blank(
                schema.EVENT_DEMO_FIELDS,
                **provenance(
                    discovery, profile.get("row_grain", "spot"), confidence=confidence
                )
            )
            demo_row.update(
                event_key=key,
                demo_code=demo_code,
                demo_role="hh" if demo_code == "HH" else "primary",
                imps_del=audience,
                rating=parsed["hh_rating"] if demo_code == "HH" else None,
                vpvh=vpvh,
                imps_vintage="final",
            )
            result.event_demos.append(demo_row)
    if not result.events:
        result.note(
            "%s: no spot lines matched the detail grammar"
            % os.path.basename(discovery.path)
        )
    return result


  # ? Split the numeric tail of a spot line into measures and per-demo values
def _pdf_spot_row(match, demos):
    tokens = match.group("tail").split()
    expected = 4 + 2 * len(demos)
    tokens_ok = bool(demos) and len(tokens) == expected
    # Structure: spots, HH rating, HH audience, (VPH, audience) per demo, cost.
    units_raw = common.to_number(tokens[0]) if tokens else None
    hh_rating = common.to_number(tokens[1]) if len(tokens) > 1 else None
    hh_audience = common.thousands_to_raw(tokens[2]) if len(tokens) > 2 else None
    cost = common.to_number(tokens[-1]) if tokens else None
    demo_values = []
    if hh_audience is not None or (len(tokens) > 2 and common.is_not_reportable(tokens[2])):
        demo_values.append(("HH", hh_audience, None))
    # Derive the demo count from the tokens when the header was unreadable, so a
    # missing header degrades to positional names rather than dropping delivery.
    pairs = (len(tokens) - 4) // 2 if len(tokens) >= 6 else 0
    codes = demos if demos else ["DEMO%d" % (position + 1) for position in range(pairs)]
    for position, code in enumerate(codes[:pairs]):
        vpvh_index = 3 + position * 2
        audience_index = vpvh_index + 1
        if audience_index >= len(tokens) - 1 + 1:
            break
        vpvh = common.to_number(tokens[vpvh_index])
        audience = common.thousands_to_raw(tokens[audience_index])
        demo_values.append((code, audience, vpvh))
    return {
        "air_date": common.to_date(match.group("date")),
        "program": common.squeeze(match.group("program")),
        "spot_time": common.squeeze(match.group("time")),
        "length_seconds": common.duration_seconds(match.group("duration")),
        "units_raw": units_raw,
        "hh_rating": hh_rating,
        "cost": cost,
        "demo_values": demo_values,
        "tokens_ok": tokens_ok,
        "token_count": len(tokens),
        "expected": expected,
    }


  # ? Read the broadcast quarter from the report banner ("BROADCAST 3RD QUARTER 2025")
def _pdf_quarter(text):
    match = re.search(
        r"BROADCAST\s+(\d)(?:ST|ND|RD|TH)\s+QUARTER\s+(\d{4})", text, re.IGNORECASE
    )
    return "%sQ%s" % (match.group(1), match.group(2)) if match else None


# * SECTION [5]: DISPATCH

  # Description: Profiles name their handler, so adding a report that shares an
  # existing shape needs only a new profile. A genuinely new shape needs a new
  # handler registered here.

HANDLERS = {
    "espn_flowchart": handle_espn_flowchart,
    "msa_post_analysis": handle_msa_post_analysis,
    "msa_detail_pdf": handle_msa_detail_pdf,
}


  # ? Run the appropriate handler for every resolved discovery
def extract(discoveries):
    combined = ExtractResult()
    for discovery in discoveries:
        if discovery.error:
            combined.note("%s: could not be opened -- %s" % (discovery.path, discovery.error))
            continue
        if not discovery.resolved:
            combined.note(
                "%s: no profile matched; requires fallback extraction "
                "(see reference/format_authoring.md)" % os.path.basename(discovery.path)
            )
            continue
        handler = HANDLERS.get(discovery.profile.get("handler"))
        if handler is None:
            combined.note(
                "%s: profile %s names unknown handler %r"
                % (
                    os.path.basename(discovery.path),
                    discovery.format_id,
                    discovery.profile.get("handler"),
                )
            )
            continue
        combined.extend(handler(discovery, discovery.profile))
    return combined
