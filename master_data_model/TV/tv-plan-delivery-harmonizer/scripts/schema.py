# =============================================================================
# schema.py -- The canonical staging and output contracts, in one place.
#
# Purpose:
#   Every stage agrees on field names here rather than in scattered literals.
#   The two staging streams are deliberately separate: demo-agnostic money and
#   units live on the event, and per-demo audience lives beside it. A single
#   long table keyed on demo would repeat cost on every demo row, so summing
#   spend across three demos would triple it. Splitting makes that impossible
#   rather than merely discouraged.
#
# Inputs / outputs:
#   Field-name constants and record factories. No I/O.
#
# Safe usage:
#   Adding a field is safe. Renaming one changes the published output contract
#   and must be mirrored in reference/output_contract.md and any BigQuery load.
# =============================================================================


# * SECTION [1]: PROVENANCE

  # Description: Carried on every staging and output row so a number can always
  # be traced to the file and method that produced it. Required by the
  # validation gate on provenance completeness.

PROVENANCE_FIELDS = [
    "source_file",
    "source_format_id",
    "extract_method",      # "profile" or "llm"
    "extract_confidence",  # 0.0 - 1.0
    "row_grain",           # spot | program_week | prop_quarter | network_quarter
]


# * SECTION [2]: STAGING STREAM 1 -- EVENTS

  # Description: One row per countable occurrence. `value_type` separates Cash
  # from ADU (makegood) because ADU carries real impressions at zero cost and
  # must never be folded into cash delivery.

EVENT_FIELDS = PROVENANCE_FIELDS + [
    "event_key",
    # Document scope
    "advertiser_raw",
    "agency",
    "prop_id",
    "order_id",
    "group_id",
    "deal_title",
    "brand",
    "campaign_raw",
    "quarter",
    "network_raw",
    # Dimensions
    "air_date",
    "broadcast_week",
    "program_raw",
    "program_norm",
    "market_raw",
    "spot_time",
    "spot_position",
    "length_seconds",
    "spot_type",     # Commercial, Promo
    "value_type",    # Cash, ADU, Trade, Bonus
    "feature_type",
    # Identifiers
    "unit_id",
    "isci",
    "invoice_no",
    "invoice_date",
    # Measures
    "cost",
    "units_raw",
    "units_30eq",
    # Flags
    "actualized_flag",
    "gtd_basis",       # GTD | EST | NONE
    "rating_stream",   # C3 | L3 | ...
]


# * SECTION [3]: STAGING STREAM 2 -- EVENT DEMOGRAPHICS

  # Description: One row per event per demo. Impressions are stored raw (already
  # scaled out of (000)) so no downstream consumer has to remember the unit.

EVENT_DEMO_FIELDS = PROVENANCE_FIELDS + [
    "event_key",
    "demo_code",
    "demo_role",       # primary | secondary | additional | hh
    "imps_gtd",
    "imps_est",
    "imps_del",
    "rating",
    "vpvh",
    "cpm",
    "imps_vintage",    # estimate | final
]


# * SECTION [4]: OUTPUT 1 -- HARMONIZED FACT

  # Description: One row per Prisma line per date. Prisma supplies planned; the
  # network guarantee, Nielsen delivery, and the three actualization views are
  # attached by matching. `primary_demo` names the demo the scalar measures
  # describe, so a reader never has to guess.

FACT_KEY_FIELDS = [
    "advertiser",
    "campaign_name",
    "media_outlet",
    "tv_type",
    "market",
    "program_name",
    "date",
]

FACT_FIELDS = (
    ["fact_key"]
    + FACT_KEY_FIELDS
    + [
        "broadcast_week",
        "quarter",
        "year",
        "prisma_line_count",
        # planned (Prisma, canonical)
        "plan_cost",
        "plan_impressions",
        "plan_units",
        # network guarantee / estimate
        "gtd_impressions",
        "gtd_cpm",
        "gtd_basis",
        # delivered (Nielsen, cash only)
        "del_impressions",
        "del_cpm",
        "del_rating",
        "del_units_raw",
        "del_units_30eq",
        # makegood
        "adu_impressions",
        "adu_units",
        # actualized -- billed
        "act_billed_cost",
        "act_billed_units",
        "invoice_no",
        "invoice_date",
        "actualized_flag",
        # actualized -- settled after makegoods
        "act_settled_impressions",
        # actualized -- Nielsen final
        "act_nielsen_impressions",
        "act_nielsen_vintage",
        "rating_stream",
        # demo context
        "primary_demo",
        # variance
        "var_impressions",
        "index_pct",
        # lineage
        "match_status",
        "match_tier",
        "match_confidence",
        "allocation_method",
        "extract_method",
        "extract_confidence",
        "source_files",
        "source_format_ids",
    ]
)


# * SECTION [5]: OUTPUT 2 -- SPOT DETAIL

  # Description: One row per airing, joined to the fact by `fact_key`. This is
  # the evidence used to defend a discrepancy with a network, so identifiers
  # (unit id, ISCI, invoice) are carried verbatim.

SPOT_DETAIL_FIELDS = PROVENANCE_FIELDS + [
    "fact_key",
    "spot_key",
    "air_date",
    "broadcast_week",
    "network_raw",
    "program_raw",
    "program_description",
    "spot_time",
    "spot_position",
    "length_seconds",
    "value_type",
    "spot_type",
    "feature_type",
    "unit_id",
    "isci",
    "order_id",
    "prop_id",
    "invoice_no",
    "invoice_date",
    "actualized_flag",
    "cost",
    "units_raw",
    "units_30eq",
    "primary_demo",
    "imps_del_primary",
    "imps_est_primary",
    "match_status",
    "match_tier",
]


# * SECTION [6]: OUTPUT 3 -- DEMO DETAIL

  # Description: One row per fact row per demo, holding everything the scalar
  # fact columns cannot: HH alongside the guarantee demo, plus secondary and
  # additional demos.

DEMO_DETAIL_FIELDS = PROVENANCE_FIELDS + [
    "fact_key",
    "demo_code",
    "demo_role",
    "imps_gtd",
    "imps_est",
    "imps_del",
    "rating",
    "vpvh",
    "cpm",
    "imps_vintage",
]


# * SECTION [7]: RECORD FACTORIES

  # Description: Build fully-populated dicts so every writer emits identical
  # column sets and CSV headers stay stable across runs.

  # ? Create an empty record with all fields present and None-valued
def blank(fields, **values):
    record = {field: None for field in fields}
    unknown = set(values) - set(record)
    if unknown:
        raise KeyError("unknown field(s) for this record: %s" % sorted(unknown))
    record.update(values)
    return record


  # ? Build the fact grain key as a single stable string
def make_fact_key(record):
    parts = []
    for field in FACT_KEY_FIELDS:
        value = record.get(field)
        parts.append("" if value is None else str(value).strip())
    return "|".join(parts)
