#!/usr/bin/env python3
"""
source_mapper.py

Epic 4: Guided Source Onboarding and Mapper

Provides:
  4.1 - Profile a new source (inspect schema, grain, metrics)
  4.2 - Propose source field and metric mappings into universal contract
  4.4 - Save versioned mapping configuration
  4.5 - Validate mapping before promotion

Usage:
  python3 source_mapper.py profile --source-table PROJECT.DATASET.TABLE
  python3 source_mapper.py propose --source-table PROJECT.DATASET.TABLE --output mdm_config.proposed_mapping
  python3 source_mapper.py validate --mapping-config PATH
"""

import argparse
import json
import subprocess
import sys
import os
from datetime import datetime
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
MAPPING_CONFIG_DIR = PROJECT_ROOT / "mapping_configs"
MAPPING_CONFIG_DIR.mkdir(parents=True, exist_ok=True)

def run_bq(sql: str) -> list:
    cmd = ["bq", "query", "--use_legacy_sql=false", "--format=json", sql]
    res = subprocess.run(cmd, capture_output=True, text=True, check=True)
    return json.loads(res.stdout.strip() or "[]")

# ============================================================
# 4.1: Profile A New Source
# ============================================================
def profile_source(source_table: str):
    parts = source_table.split(".")
    if len(parts) != 3:
        print("❌ Source table must be PROJECT.DATASET.TABLE format")
        sys.exit(1)
    project, dataset, table = parts

    print(f"🔍 Profiling source: {source_table}")

    # Schema
    schema_sql = f"""
    SELECT column_name, data_type, ordinal_position
    FROM `{project}.{dataset}.INFORMATION_SCHEMA.COLUMNS`
    WHERE table_name = '{table}'
    ORDER BY ordinal_position
    """
    cols = run_bq(schema_sql)

    # Row count + sample
    meta_sql = f"""
    SELECT COUNT(*) AS row_count
    FROM `{source_table}`
    """
    meta = run_bq(meta_sql)
    row_count = meta[0]["row_count"] if meta else 0

    # Profile: identify potential keys, dates, metrics
    string_cols = [c for c in cols if c["data_type"] == "STRING"]
    numeric_cols = [c for c in cols if c["data_type"] in ("FLOAT64", "INT64", "NUMERIC")]
    date_cols = [c for c in cols if c["data_type"] in ("DATE", "DATETIME", "TIMESTAMP")]

    # Candidate key fields
    candidate_keys = [c["column_name"] for c in string_cols
                     if any(k in c["column_name"].lower() for k in ("id", "key", "code", "name"))][:5]

    # Candidate metric fields
    candidate_metrics = [c["column_name"] for c in numeric_cols][:10]

    # Candidate date fields
    candidate_dates = [c["column_name"] for c in date_cols][:5]

    profile = {
        "source_table": source_table,
        "profiled_at": datetime.utcnow().isoformat(),
        "row_count": row_count,
        "column_count": len(cols),
        "candidate_grain": candidate_keys[:3],
        "candidate_dimensions": string_cols[:15],
        "candidate_metrics": candidate_metrics,
        "candidate_date_fields": candidate_dates,
        "all_columns": cols
    }

    output_path = MAPPING_CONFIG_DIR / f"profile_{dataset}_{table}.json"
    with open(output_path, "w") as f:
        json.dump(profile, f, indent=2, default=str)
    print(f"✅ Profile saved to {output_path}")
    return profile

# ============================================================
# 4.2: Propose Mappings
# ============================================================
def propose_mappings(source_table: str, output_table: str = None):
    profile = profile_source(source_table)

    source_cols = {c["column_name"]: c["data_type"] for c in profile["all_columns"]}

    # Smart mapping heuristics
    DIRECT_MAP = {
        "id": "univ_source_row_id",
        "date": "univ_date_placeholder",
        "campaign": "univ_campaign_placeholder",
        "creative": "univ_creative_placeholder",
        "placement": "univ_placement_placeholder",
        "supplier": "univ_supplier_placeholder",
        "spend": None,  # metric
        "impression": None,  # metric
        "click": None,  # metric
        "video": None,  # metric
    }

    mapped = []
    for col_name, data_type in source_cols.items():
        col_lower = col_name.lower()
        target = None
        for keyword, mapping in DIRECT_MAP.items():
            if keyword in col_lower:
                target = mapping
                break
        mapped.append({
            "source_column": col_name,
            "source_type": data_type,
            "proposed_target": target or f"univ_{col_name[:40]}",
            "mapping_confidence": "high" if target else "low"
        })

    proposal = {
        "source_table": source_table,
        "proposed_at": datetime.utcnow().isoformat(),
        "mappings": mapped,
        "candidate_grain": profile["candidate_grain"],
        "candidate_metrics": profile["candidate_metrics"],
        "placeholder_candidates": ["not_available_at_source", "unknown_from_source"],
        "metric_status_candidates": ["direct", "inferred", "allocated", "unavailable"],
        "summability_candidates": ["additive", "doNotSum", "blocked"]
    }

    output_path = MAPPING_CONFIG_DIR / f"mapping_proposal_{Path(source_table).name}.json"
    with open(output_path, "w") as f:
        json.dump(proposal, f, indent=2)

    print(f"✅ Mapping proposal saved to {output_path}")

    if output_table:
        # Save to BigQuery config table
        ddl = f"""
        CREATE OR REPLACE TABLE `{output_table}` (
          mapping_id STRING,
          source_table STRING,
          proposed_at TIMESTAMP,
          mapping_json JSON
        )
        OPTIONS(description="Source mapping proposal for source mapper. Story 4.2/4.4. Does not affect production.")
        """
        run_bq(ddl)
        safe_json = json.dumps(proposal).replace("'", "\\'")
        insert = f"""
        INSERT INTO `{output_table}`
        VALUES (
          GENERATE_UUID(),
          '{source_table}',
          CURRENT_TIMESTAMP(),
          PARSE_JSON('{safe_json}')
        )
        """
        run_bq(insert)
        print(f"✅ Mapping proposal also written to BigQuery: {output_table}")

# ============================================================
# 4.5: Validate Mapping
# ============================================================
def validate_mapping(mapping_path: str):
    print(f"🔍 Validating mapping config: {mapping_path}")

    with open(mapping_path) as f:
        config = json.load(f)

    errors = []
    warnings = []

    # Check 1: Source table is specified
    if not config.get("source_table"):
        errors.append("Missing source_table")

    # Check 2: Mappings exist
    mappings = config.get("mappings", [])
    if not mappings:
        errors.append("No mappings defined")
    else:
        # Check for unmapped columns
        unmapped = [m for m in mappings if m.get("proposed_target") is None]
        if unmapped:
            warnings.append(f"{len(unmapped)} columns have no proposed target")

        # Check grain fields
        if not config.get("candidate_grain"):
            errors.append("No candidate grain identified")

    # Check 3: Metric safety labels
    if not config.get("summability_candidates"):
        warnings.append("No summability labels defined")

    result = {
        "validated_at": datetime.utcnow().isoformat(),
        "config_path": mapping_path,
        "status": "FAIL" if errors else ("WARN" if warnings else "PASS"),
        "errors": errors,
        "warnings": warnings,
        "mapping_count": len(mappings)
    }

    print(f"📋 Validation result: {result['status']}")
    for e in errors:
        print(f"  ❌ ERROR: {e}")
    for w in warnings:
        print(f"  ⚠️  WARN: {w}")

    return result

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Source mapper for master data model redesign")
    sub = parser.add_subparsers(dest="command", required=True)

    profile_p = sub.add_parser("profile", help="4.1: Profile a new source")
    profile_p.add_argument("--source-table", required=True)

    propose_p = sub.add_parser("propose", help="4.2: Propose source mappings")
    propose_p.add_argument("--source-table", required=True)
    propose_p.add_argument("--output", default="looker-studio-pro-452620.mdm_config.mapping_proposals")

    validate_p = sub.add_parser("validate", help="4.5: Validate mapping configuration")
    validate_p.add_argument("--mapping-config", required=True)

    args = parser.parse_args()

    if args.command == "profile":
        profile_source(args.source_table)
    elif args.command == "propose":
        propose_mappings(args.source_table, args.output)
    elif args.command == "validate":
        validate_mapping(args.mapping_config)
