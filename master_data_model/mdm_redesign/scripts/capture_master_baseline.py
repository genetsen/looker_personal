#!/usr/bin/env python3
"""
capture_master_baseline.py

Captures the current live master table schema (columns and data types)
and key totals (row count, distinct packages, and sums of spend/impressions)
from `looker-studio-pro-452620.master_stg.data_model` and stores them
both locally as a JSON baseline and in BigQuery under the `mdm_qa` zone.

Inputs:
  - BigQuery live master table `looker-studio-pro-452620.master_stg.data_model`
Outputs:
  - Local JSON: `mdm_redesign/_bmad-output/implementation-artifacts/master_baseline_snapshot.json`
  - BigQuery Table: `looker-studio-pro-452620.mdm_qa.master_baseline_snapshot`
"""

import os
import json
import subprocess
import datetime
from pathlib import Path

# Target project/dataset/table
PROJECT_ID = "looker-studio-pro-452620"
SOURCE_TABLE = "master_stg.data_model"
TARGET_DATASET = "mdm_qa"
TARGET_TABLE = "master_baseline_snapshot"

def run_bq_query(query: str) -> list:
    cmd = ["bq", "query", "--use_legacy_sql=false", "--format=json", query]
    try:
        res = subprocess.run(cmd, capture_output=True, text=True, check=True)
        stdout = res.stdout.strip()
        if not stdout:
            return []
        try:
            return json.loads(stdout)
        except json.JSONDecodeError:
            # If it's not JSON (e.g. DDL/DML success message), return empty list or log it
            return []
    except subprocess.CalledProcessError as e:
        print(f"Error executing bq query: {e.stderr}")
        raise e

def main():
    print("🚀 Initiating Live Master Baseline Capture...")
    
    # 1. Fetch column metadata
    print("📋 Querying Column Metadata...")
    cols_query = f"""
    SELECT column_name, data_type 
    FROM `{PROJECT_ID}.master_stg.INFORMATION_SCHEMA.COLUMNS` 
    WHERE table_name = 'data_model' 
    ORDER BY ordinal_position
    """
    cols_res = run_bq_query(cols_query)
    schema_map = {row["column_name"]: row["data_type"] for row in cols_res}
    
    # 2. Query key totals & summaries
    print("📊 Querying Key Totals & Summaries...")
    totals_query = f"""
    SELECT 
      COUNT(*) as total_rows,
      COUNT(DISTINCT _package_id) as total_packages,
      SUM(_planned_spend) as total_planned_spend,
      SUM(_spend) as total_spend,
      SUM(_impressions) as total_impressions,
      SUM(_clicks) as total_clicks
    FROM `{PROJECT_ID}.{SOURCE_TABLE}`
    """
    totals_res = run_bq_query(totals_query)[0]
    
    # Identify doNotSum columns
    do_not_sum_cols = [col for col in schema_map.keys() if "doNotSum" in col]
    
    # 3. Assemble JSON snapshot
    timestamp = datetime.datetime.now(datetime.timezone.utc).isoformat()
    snapshot = {
        "timestamp": timestamp,
        "source_table": f"{PROJECT_ID}.{SOURCE_TABLE}",
        "schema": schema_map,
        "totals": {
            "total_rows": int(totals_res["total_rows"]),
            "total_packages": int(totals_res["total_packages"]),
            "total_planned_spend": float(totals_res["total_planned_spend"]) if totals_res["total_planned_spend"] is not None else 0.0,
            "total_spend": float(totals_res["total_spend"]) if totals_res["total_spend"] is not None else 0.0,
            "total_impressions": int(float(totals_res["total_impressions"])) if totals_res["total_impressions"] is not None else 0,
            "total_clicks": int(float(totals_res["total_clicks"])) if totals_res["total_clicks"] is not None else 0
        },
        "do_not_sum_fields": do_not_sum_cols
    }
    
    # 4. Save to local JSON artifact
    project_root = Path(__file__).resolve().parents[2]
    local_output_path = project_root / "mdm_redesign" / "_bmad-output" / "implementation-artifacts" / "master_baseline_snapshot.json"
    
    local_output_path.parent.mkdir(parents=True, exist_ok=True)
    with open(local_output_path, "w") as f:
        json.dump(snapshot, f, indent=2)
    print(f"💾 Saved local JSON artifact to: {local_output_path}")
    
    # 5. Save to BigQuery in mdm_qa zone
    print("🌐 Writing snapshot to BigQuery mdm_qa dataset...")
    # Escape JSON for query string
    json_str_escaped = json.dumps(snapshot).replace("\\", "\\\\").replace("'", "\\'")
    
    # Create mdm_qa.master_baseline_snapshot table if not exists or replace it
    # This aligns with Shared BigQuery QA Object Hygiene (visible metadata, owner, description)
    bq_table_ddl = f"""
    CREATE OR REPLACE TABLE `{PROJECT_ID}.{TARGET_DATASET}.{TARGET_TABLE}` (
      snapshot_timestamp TIMESTAMP,
      source_table STRING,
      snapshot_data JSON
    )
    OPTIONS(
      description="Pristine live baseline schema and totals for current production master table. Created by capture_master_baseline.py script. Safe to delete or overwrite on new runs."
    );
    """
    run_bq_query(bq_table_ddl)
    
    insert_dml = f"""
    INSERT INTO `{PROJECT_ID}.{TARGET_DATASET}.{TARGET_TABLE}` (snapshot_timestamp, source_table, snapshot_data)
    VALUES (
      TIMESTAMP('{timestamp}'),
      '{PROJECT_ID}.{SOURCE_TABLE}',
      PARSE_JSON('{json_str_escaped}')
    );
    """
    run_bq_query(insert_dml)
    print(f"✅ Baseline successfully written to BigQuery table: `{PROJECT_ID}.{TARGET_DATASET}.{TARGET_TABLE}`")
    print("🎉 Live Master Baseline Capture complete!")

if __name__ == "__main__":
    main()
