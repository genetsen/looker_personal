#!/usr/bin/env python3
"""
SQL Change Guard runner (BigQuery).

Behavior:
- Builds QA baseline/candidate tables.
- Compares baseline vs candidate for count/key/schema/metric/derived checks.
- Supports custom checks from a manifest without code edits.
- Prints pass/fail first, then optional details/comparisons.
"""

from __future__ import annotations

import argparse
import csv
import json
import os
import re
import subprocess
import sys
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, List, Optional, Sequence, Tuple


WINDOW_OPTIONS = (3, 6, 9, 12)
ACTIVE_QUERY_BACKEND = "mcp"
ACTIVE_MCP_SERVER = "bigquery"
ACTIVE_MCP_MAX_RETRIES = 2
ACTIVE_MCP_QUERY_TIMEOUT_SEC = 90


def _nvm_node_bin_candidates() -> List[Path]:
    root = Path.home() / ".nvm" / "versions" / "node"
    if not root.exists():
        return []

    def version_key(bin_path: Path) -> Tuple[int, ...]:
        version = bin_path.parent.name.lstrip("v")
        parts: List[int] = []
        for token in version.split("."):
            digits = "".join(ch for ch in token if ch.isdigit())
            parts.append(int(digits) if digits else 0)
        return tuple(parts)

    bins = [path for path in root.glob("v*/bin") if (path / "node").exists()]
    return sorted(bins, key=version_key, reverse=True)


@dataclass
class CheckResult:
    name: str
    passed: bool
    severity: str
    summary: str
    details: Dict[str, Any]


def run_cmd(
    cmd: Sequence[str],
    stdin_text: Optional[str] = None,
    timeout_sec: Optional[int] = None,
) -> str:
    env = os.environ.copy()
    if cmd and cmd[0] == "codex":
        for bin_path in _nvm_node_bin_candidates():
            prefix = str(bin_path)
            path_parts = [part for part in env.get("PATH", "").split(":") if part]
            path_parts = [part for part in path_parts if part != prefix]
            env["PATH"] = ":".join([prefix] + path_parts)
            break

    try:
        proc = subprocess.run(
            cmd,
            input=stdin_text,
            text=True,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            env=env,
            timeout=timeout_sec,
        )
    except subprocess.TimeoutExpired as exc:
        raise RuntimeError(
            f"Command timed out after {timeout_sec}s: {' '.join(cmd)}"
        ) from exc
    if proc.returncode != 0:
        raise RuntimeError(
            f"Command failed: {' '.join(cmd)}\nSTDERR:\n{proc.stderr}\nSTDOUT:\n{proc.stdout}"
        )
    return proc.stdout


def run_bq_query_json(project: str, query: str) -> List[Dict[str, Any]]:
    cmd = [
        "bq",
        "query",
        "--project_id",
        project,
        "--use_legacy_sql=false",
        "--format=json",
        query,
    ]
    output = run_cmd(cmd).strip()
    if not output:
        return []
    parsed = json.loads(output)
    if isinstance(parsed, list):
        return parsed
    return [parsed]


def strip_code_fence(text: str) -> str:
    cleaned = text.strip()
    if cleaned.startswith("```"):
        lines = cleaned.splitlines()
        if lines and lines[0].startswith("```"):
            lines = lines[1:]
        if lines and lines[-1].strip() == "```":
            lines = lines[:-1]
        cleaned = "\n".join(lines).strip()
    return cleaned


def extract_codex_agent_message(jsonl_text: str) -> str:
    messages: List[str] = []
    for line in jsonl_text.splitlines():
        line = line.strip()
        if not line or not line.startswith("{"):
            continue
        try:
            obj = json.loads(line)
        except Exception:
            continue
        if obj.get("type") != "item.completed":
            continue
        item = obj.get("item", {})
        if item.get("type") != "agent_message":
            continue
        msg = item.get("text", "")
        if msg:
            messages.append(msg)
    if not messages:
        raise RuntimeError("No agent_message found in codex exec output.")
    return messages[-1]


def run_mcp_query_json(project: str, query: str) -> List[Dict[str, Any]]:
    prompt = (
        f'Use the "{ACTIVE_MCP_SERVER}" MCP server only (no shell, no bq CLI). '
        f"Run this SQL in project {project} and return ONLY valid JSON in the exact format: "
        '{"rows":[{"col":"value"}]}. '
        'If there are zero rows, return {"rows":[]}. '
        "Do not include markdown fences or commentary.\n\n"
        f"SQL:\n{query}"
    )

    last_error: Optional[Exception] = None
    for attempt in range(ACTIVE_MCP_MAX_RETRIES + 1):
        try:
            output = run_cmd(
                ["codex", "exec", "--json", prompt],
                timeout_sec=ACTIVE_MCP_QUERY_TIMEOUT_SEC,
            )
            message = strip_code_fence(extract_codex_agent_message(output))
            payload = json.loads(message)
            if isinstance(payload, dict):
                rows = payload.get("rows")
                if isinstance(rows, list):
                    return rows
                if payload.get("success") is True and isinstance(payload.get("data"), list):
                    return payload["data"]
                return [payload]
            if isinstance(payload, list):
                return payload
            raise RuntimeError("MCP response was not a JSON object or array.")
        except Exception as exc:
            last_error = exc
            if attempt < ACTIVE_MCP_MAX_RETRIES:
                time.sleep(min(2 ** attempt, 4))
                continue
    raise RuntimeError(f"MCP query failed after retries: {last_error}")


def query_json(project: str, query: str) -> List[Dict[str, Any]]:
    if ACTIVE_QUERY_BACKEND == "mcp":
        return run_mcp_query_json(project, query)
    if ACTIVE_QUERY_BACKEND == "bq":
        return run_bq_query_json(project, query)
    raise ValueError(f"Unsupported query backend: {ACTIVE_QUERY_BACKEND}")


def load_source_sql(query_file: Optional[str], table_name: Optional[str]) -> str:
    if query_file:
        return Path(query_file).read_text()
    if table_name:
        return f"SELECT * FROM `{table_name}`"
    raise ValueError("Provide either query_file or table_name")


def load_manifest(path: str) -> Dict[str, Any]:
    return json.loads(Path(path).read_text())


def sql_literal(value: str) -> str:
    return "'" + value.replace("'", "\\'") + "'"


def normalize_table_fqn(table_name: str, default_project: str) -> str:
    raw = table_name.strip().strip("`")
    if ":" in raw:
        project_part, rest = raw.split(":", 1)
        raw = f"{project_part}.{rest}"
    parts = raw.split(".")
    if len(parts) == 3:
        return f"{parts[0]}.{parts[1]}.{parts[2]}"
    if len(parts) == 2:
        return f"{default_project}.{parts[0]}.{parts[1]}"
    raise ValueError(
        f"Table reference must be dataset.table or project.dataset.table: {table_name}"
    )


def table_exists(project: str, table_name: str) -> Tuple[bool, Optional[str], str]:
    normalized = normalize_table_fqn(table_name, project)
    table_project, dataset, table = normalized.split(".", 2)
    query = f"""
    SELECT table_type
    FROM `{table_project}.{dataset}.INFORMATION_SCHEMA.TABLES`
    WHERE table_name = {sql_literal(table)}
    LIMIT 1
    """
    rows = query_json(project, query)
    if not rows:
        return False, None, normalized
    return True, rows[0].get("table_type"), normalized


def infer_created_table_from_script(sql_text: str) -> Optional[str]:
    match = re.search(
        r"CREATE\s+(?:OR\s+REPLACE\s+)?TABLE\s+`([^`]+)`",
        sql_text,
        flags=re.IGNORECASE,
    )
    if not match:
        return None
    return match.group(1)


def looks_like_script(sql_text: str) -> bool:
    stripped = re.sub(r"--.*?$", "", sql_text, flags=re.MULTILINE)
    return bool(
        re.search(
            r"\b(CREATE|INSERT|UPDATE|DELETE|MERGE|DECLARE|BEGIN|DROP|TRUNCATE)\b",
            stripped,
            flags=re.IGNORECASE,
        )
    )


def to_float(value: Any) -> float:
    if value is None:
        return 0.0
    if isinstance(value, bool):
        return 1.0 if value else 0.0
    if isinstance(value, (int, float)):
        return float(value)
    text = str(value).strip()
    if not text:
        return 0.0
    return float(text)


def first_row_first_value(rows: List[Dict[str, Any]]) -> float:
    if not rows:
        return 0.0
    first_row = rows[0]
    if not first_row:
        return 0.0
    first_key = next(iter(first_row.keys()))
    return to_float(first_row[first_key])


def abs_pct_delta(baseline: float, candidate: float) -> float:
    delta_abs = abs(candidate - baseline)
    if baseline == 0:
        return 0.0 if candidate == 0 else 1.0
    return delta_abs / abs(baseline)


def evaluate_tolerance(
    abs_delta: float,
    abs_pct: float,
    tolerance: Optional[Dict[str, Any]],
) -> Tuple[bool, Optional[float], Optional[float]]:
    tolerance = tolerance or {}
    tol_abs = tolerance.get("max_abs_delta")
    tol_pct = tolerance.get("max_abs_pct")
    if tol_abs is None and tol_pct is None:
        tol_abs = 0.0

    pass_abs = True if tol_abs is None else abs_delta <= float(tol_abs)
    pass_pct = True if tol_pct is None else abs_pct <= float(tol_pct)
    return pass_abs and pass_pct, (None if tol_abs is None else float(tol_abs)), (
        None if tol_pct is None else float(tol_pct)
    )


def extract_table_refs(sql: str) -> List[str]:
    refs = set()
    for match in re.findall(r"`([^`]+)`", sql):
        if match.count(".") >= 2:
            refs.add(match)
    return sorted(refs)


def maybe_get_view_query(project: str, object_id: str) -> Optional[str]:
    parts = object_id.split(".")
    if len(parts) < 3:
        return None
    view_project, dataset, view_name = parts[0], parts[1], parts[2]
    if ACTIVE_QUERY_BACKEND == "mcp":
        view_query_sql = f"""
        SELECT view_definition
        FROM `{view_project}.{dataset}.INFORMATION_SCHEMA.VIEWS`
        WHERE table_name = {sql_literal(view_name)}
        LIMIT 1
        """
        rows = query_json(project, view_query_sql)
        if not rows:
            return None
        return rows[0].get("view_definition")

    ref = f"{view_project}:{dataset}.{view_name}"
    cmd = ["bq", "show", "--project_id", project, "--format=prettyjson", ref]
    try:
        output = run_cmd(cmd)
        data = json.loads(output)
        return data.get("view", {}).get("query")
    except Exception:
        return None


def choose_window_months(
    project: str,
    candidate_source_sql: str,
    date_column: Optional[str],
    min_rows: int,
    fixed_months: Optional[int],
    window_end_date: Optional[str],
) -> Optional[int]:
    if fixed_months is not None:
        return fixed_months
    if not date_column:
        return None

    for months in WINDOW_OPTIONS:
        if window_end_date:
            query = f"""
            WITH src AS (
              {candidate_source_sql}
            )
            SELECT COUNT(*) AS row_count
            FROM src
            WHERE DATE({date_column}) BETWEEN DATE_SUB(DATE {sql_literal(window_end_date)}, INTERVAL {months} MONTH)
              AND DATE {sql_literal(window_end_date)}
            """
        else:
            query = f"""
            WITH src AS (
              {candidate_source_sql}
            )
            SELECT COUNT(*) AS row_count
            FROM src
            WHERE DATE({date_column}) >= DATE_SUB((SELECT MAX(DATE({date_column})) FROM src), INTERVAL {months} MONTH)
            """
        rows = query_json(project, query)
        row_count = int(rows[0]["row_count"]) if rows else 0
        if row_count >= min_rows:
            return months
    return 12


def choose_window_end_date(
    project: str,
    date_column: Optional[str],
    candidate_source_sql: str,
    baseline_source_sql: Optional[str],
    exclude_recent_days: int,
) -> Tuple[Optional[str], Optional[str], Optional[str]]:
    if not date_column:
        return None, None, None
    safe_exclude_days = max(0, exclude_recent_days)
    if baseline_source_sql:
        query = f"""
        WITH candidate_src AS (
          {candidate_source_sql}
        ),
        baseline_src AS (
          {baseline_source_sql}
        )
        SELECT
          CAST((SELECT MAX(DATE({date_column})) FROM baseline_src) AS STRING) AS baseline_max_date,
          CAST((SELECT MAX(DATE({date_column})) FROM candidate_src) AS STRING) AS candidate_max_date,
          CAST(DATE_SUB(CURRENT_DATE(), INTERVAL {safe_exclude_days} DAY) AS STRING) AS window_end_date
        """
    else:
        query = f"""
        WITH candidate_src AS (
          {candidate_source_sql}
        )
        SELECT
          CAST(NULL AS STRING) AS baseline_max_date,
          CAST((SELECT MAX(DATE({date_column})) FROM candidate_src) AS STRING) AS candidate_max_date,
          CAST(DATE_SUB(CURRENT_DATE(), INTERVAL {safe_exclude_days} DAY) AS STRING) AS window_end_date
        """
    rows = query_json(project, query)
    if not rows:
        return None, None, None
    row = rows[0]
    return row.get("window_end_date"), row.get("baseline_max_date"), row.get("candidate_max_date")


def materialize_table(
    project: str,
    qa_dataset: str,
    table_name: str,
    source_sql: str,
    date_column: Optional[str],
    months: Optional[int],
    window_end_date: Optional[str],
) -> str:
    target = f"{project}.{qa_dataset}.{table_name}"
    if date_column and months and window_end_date:
        query = f"""
        CREATE OR REPLACE TABLE `{target}` AS
        WITH src AS (
          {source_sql}
        )
        SELECT *
        FROM src
        WHERE DATE({date_column}) BETWEEN DATE_SUB(DATE {sql_literal(window_end_date)}, INTERVAL {months} MONTH)
          AND DATE {sql_literal(window_end_date)}
        """
    elif date_column and months:
        query = f"""
        CREATE OR REPLACE TABLE `{target}` AS
        WITH src AS (
          {source_sql}
        )
        SELECT *
        FROM src
        WHERE DATE({date_column}) >= DATE_SUB((SELECT MAX(DATE({date_column})) FROM src), INTERVAL {months} MONTH)
        """
    elif date_column and window_end_date:
        query = f"""
        CREATE OR REPLACE TABLE `{target}` AS
        WITH src AS (
          {source_sql}
        )
        SELECT *
        FROM src
        WHERE DATE({date_column}) <= DATE {sql_literal(window_end_date)}
        """
    else:
        query = f"""
        CREATE OR REPLACE TABLE `{target}` AS
        {source_sql}
        """
    if ACTIVE_QUERY_BACKEND == "mcp":
        query_json(project, query)
    else:
        run_cmd(
            [
                "bq",
                "query",
                "--project_id",
                project,
                "--use_legacy_sql=false",
            ],
            stdin_text=query,
        )
    return target


def fetch_schema(project: str, table_fqn: str) -> List[Dict[str, Any]]:
    parts = table_fqn.split(".")
    dataset = parts[1]
    table = parts[2]
    query = f"""
    SELECT column_name, data_type, is_nullable
    FROM `{project}.{dataset}.INFORMATION_SCHEMA.COLUMNS`
    WHERE table_name = {sql_literal(table)}
    ORDER BY ordinal_position
    """
    return query_json(project, query)


def render_sql_template(
    sql_text: str,
    baseline_table: Optional[str],
    candidate_table: Optional[str],
    table: Optional[str],
) -> str:
    rendered = sql_text
    if baseline_table:
        rendered = rendered.replace("{baseline_table}", f"`{baseline_table}`")
    if candidate_table:
        rendered = rendered.replace("{candidate_table}", f"`{candidate_table}`")
    if table:
        rendered = rendered.replace("{table}", f"`{table}`")
    return rendered


def query_scalar(project: str, sql_text: str) -> float:
    rows = query_json(project, sql_text)
    return first_row_first_value(rows)


def run_row_count_check(
    project: str,
    base_table: str,
    cand_table: str,
    row_tolerance: Optional[Dict[str, Any]],
) -> CheckResult:
    query = f"""
    SELECT
      (SELECT COUNT(*) FROM `{base_table}`) AS baseline_count,
      (SELECT COUNT(*) FROM `{cand_table}`) AS candidate_count
    """
    row = query_json(project, query)[0]
    b = int(row["baseline_count"])
    c = int(row["candidate_count"])
    delta = c - b
    abs_delta = abs(delta)
    abs_pct = abs_pct_delta(float(b), float(c))
    passed, tol_abs, tol_pct = evaluate_tolerance(abs_delta, abs_pct, row_tolerance)
    return CheckResult(
        name="row_count",
        passed=passed,
        severity="critical",
        summary=(
            f"baseline={b}, candidate={c}, abs_delta={abs_delta}, "
            f"abs_pct={abs_pct:.6f}, tol_abs={tol_abs}, tol_pct={tol_pct}"
        ),
        details={
            "baseline_count": b,
            "candidate_count": c,
            "delta": delta,
            "abs_pct": abs_pct,
            "tolerance_abs_delta": tol_abs,
            "tolerance_abs_pct": tol_pct,
        },
    )


def run_duplicate_check(project: str, table_fqn: str, keys: List[str], label: str) -> CheckResult:
    if not keys:
        return CheckResult(
            name=f"duplicate_keys_{label}",
            passed=True,
            severity="warning",
            summary="skipped (no keys defined)",
            details={},
        )
    key_expr = ", ".join(keys)
    query = f"""
    SELECT COUNT(*) AS dup_groups
    FROM (
      SELECT {key_expr}, COUNT(*) AS c
      FROM `{table_fqn}`
      GROUP BY {key_expr}
      HAVING COUNT(*) > 1
    )
    """
    row = query_json(project, query)[0]
    dup_groups = int(row["dup_groups"])
    return CheckResult(
        name=f"duplicate_keys_{label}",
        passed=(dup_groups == 0),
        severity="critical",
        summary=f"dup_groups={dup_groups}",
        details={"dup_groups": dup_groups, "keys": keys},
    )


def run_key_overlap_check(
    project: str,
    base_table: str,
    cand_table: str,
    keys: List[str],
    tolerance: Optional[Dict[str, Any]],
) -> CheckResult:
    if not keys:
        return CheckResult(
            name="key_overlap",
            passed=True,
            severity="warning",
            summary="skipped (no keys defined)",
            details={},
        )

    first_key = keys[0]
    key_expr = ", ".join(keys)
    using_expr = ", ".join(keys)
    query = f"""
    WITH b AS (
      SELECT DISTINCT {key_expr}
      FROM `{base_table}`
    ),
    c AS (
      SELECT DISTINCT {key_expr}
      FROM `{cand_table}`
    )
    SELECT
      (SELECT COUNT(*) FROM b) AS baseline_key_count,
      (SELECT COUNT(*) FROM c) AS candidate_key_count,
      (SELECT COUNT(*) FROM b JOIN c USING ({using_expr})) AS overlap_key_count,
      (SELECT COUNT(*) FROM b LEFT JOIN c USING ({using_expr}) WHERE c.{first_key} IS NULL) AS baseline_only_key_count,
      (SELECT COUNT(*) FROM c LEFT JOIN b USING ({using_expr}) WHERE b.{first_key} IS NULL) AS candidate_only_key_count
    """
    row = query_json(project, query)[0]
    baseline_only = int(row["baseline_only_key_count"])
    candidate_only = int(row["candidate_only_key_count"])
    max_baseline_only = int((tolerance or {}).get("max_baseline_only", 0))
    max_candidate_only = int((tolerance or {}).get("max_candidate_only", 0))
    passed = baseline_only <= max_baseline_only and candidate_only <= max_candidate_only
    return CheckResult(
        name="key_overlap",
        passed=passed,
        severity="critical",
        summary=(
            f"baseline_only={baseline_only}, candidate_only={candidate_only}, "
            f"tol_baseline_only={max_baseline_only}, tol_candidate_only={max_candidate_only}"
        ),
        details={
            "keys": keys,
            "baseline_key_count": int(row["baseline_key_count"]),
            "candidate_key_count": int(row["candidate_key_count"]),
            "overlap_key_count": int(row["overlap_key_count"]),
            "baseline_only_key_count": baseline_only,
            "candidate_only_key_count": candidate_only,
            "tolerance_baseline_only": max_baseline_only,
            "tolerance_candidate_only": max_candidate_only,
        },
    )


def run_metric_sum_checks(
    project: str,
    base_table: str,
    cand_table: str,
    metrics: List[str],
    allowed_deltas: Dict[str, Any],
) -> List[CheckResult]:
    results: List[CheckResult] = []
    for metric in metrics:
        query = f"""
        SELECT
          (SELECT SUM(CAST({metric} AS FLOAT64)) FROM `{base_table}`) AS baseline_value,
          (SELECT SUM(CAST({metric} AS FLOAT64)) FROM `{cand_table}`) AS candidate_value
        """
        row = query_json(project, query)[0]
        b = to_float(row["baseline_value"])
        c = to_float(row["candidate_value"])
        delta = c - b
        abs_delta = abs(delta)
        pct = abs_pct_delta(b, c)
        passed, tol_abs, tol_pct = evaluate_tolerance(abs_delta, pct, allowed_deltas.get(metric, {}))
        results.append(
            CheckResult(
                name=f"metric_sum_{metric}",
                passed=passed,
                severity="critical" if metric in ("spend", "impressions") else "high",
                summary=(
                    f"baseline={b:.6f}, candidate={c:.6f}, abs_delta={abs_delta:.6f}, "
                    f"abs_pct={pct:.6f}, tol_abs={tol_abs}, tol_pct={tol_pct}"
                ),
                details={
                    "metric": metric,
                    "baseline_value": b,
                    "candidate_value": c,
                    "delta": delta,
                    "abs_pct": pct,
                    "tolerance_abs_delta": tol_abs,
                    "tolerance_abs_pct": tol_pct,
                },
            )
        )
    return results


def run_derived_metric_checks(
    project: str,
    base_table: str,
    cand_table: str,
    derived_metrics: List[Dict[str, Any]],
    allowed_deltas: Dict[str, Any],
) -> List[CheckResult]:
    results: List[CheckResult] = []
    for spec in derived_metrics:
        name = spec["name"]
        expr = spec["expr"]
        query = f"""
        SELECT
          (SELECT {expr} FROM `{base_table}`) AS baseline_value,
          (SELECT {expr} FROM `{cand_table}`) AS candidate_value
        """
        row = query_json(project, query)[0]
        b = to_float(row["baseline_value"])
        c = to_float(row["candidate_value"])
        delta = c - b
        abs_delta = abs(delta)
        pct = abs_pct_delta(b, c)
        passed, tol_abs, tol_pct = evaluate_tolerance(abs_delta, pct, allowed_deltas.get(name, {}))
        results.append(
            CheckResult(
                name=f"derived_metric_{name}",
                passed=passed,
                severity="high",
                summary=(
                    f"baseline={b:.6f}, candidate={c:.6f}, abs_delta={abs_delta:.6f}, "
                    f"abs_pct={pct:.6f}, tol_abs={tol_abs}, tol_pct={tol_pct}"
                ),
                details={
                    "metric": name,
                    "expr": expr,
                    "baseline_value": b,
                    "candidate_value": c,
                    "delta": delta,
                    "abs_pct": pct,
                    "tolerance_abs_delta": tol_abs,
                    "tolerance_abs_pct": tol_pct,
                },
            )
        )
    return results


def run_dimension_distinct_checks(
    project: str,
    base_table: str,
    cand_table: str,
    dimensions: List[str],
    allowed_deltas: Dict[str, Any],
) -> List[CheckResult]:
    results: List[CheckResult] = []
    for dim in dimensions:
        query = f"""
        SELECT
          (SELECT COUNT(DISTINCT CAST({dim} AS STRING)) FROM `{base_table}`) AS baseline_distinct,
          (SELECT COUNT(DISTINCT CAST({dim} AS STRING)) FROM `{cand_table}`) AS candidate_distinct
        """
        row = query_json(project, query)[0]
        b = int(to_float(row["baseline_distinct"]))
        c = int(to_float(row["candidate_distinct"]))
        delta = c - b
        abs_delta = abs(delta)
        pct = abs_pct_delta(float(b), float(c))
        delta_cfg = allowed_deltas.get(f"distinct_{dim}", {})
        passed, tol_abs, tol_pct = evaluate_tolerance(abs_delta, pct, delta_cfg)
        results.append(
            CheckResult(
                name=f"dimension_distinct_{dim}",
                passed=passed,
                severity="high",
                summary=(
                    f"baseline_distinct={b}, candidate_distinct={c}, abs_delta={abs_delta}, "
                    f"abs_pct={pct:.6f}, tol_abs={tol_abs}, tol_pct={tol_pct}"
                ),
                details={
                    "dimension": dim,
                    "baseline_distinct": b,
                    "candidate_distinct": c,
                    "delta": delta,
                    "abs_pct": pct,
                    "tolerance_abs_delta": tol_abs,
                    "tolerance_abs_pct": tol_pct,
                },
            )
        )
    return results


def parse_assertion(check: Dict[str, Any]) -> Tuple[Optional[str], Optional[float], float, Optional[str]]:
    if "expectation" in check:
        expectation = check["expectation"]
        op = str(expectation.get("op", "<=")).strip()
        if "value" not in expectation:
            return None, None, 0.0, "expectation.value is required"
        return op, to_float(expectation["value"]), float(check.get("equals_tolerance", 0.0)), None
    if "expected_max" in check:
        return "<=", to_float(check["expected_max"]), 0.0, None
    if "expected_min" in check:
        return ">=", to_float(check["expected_min"]), 0.0, None
    if "equals" in check:
        return "==", to_float(check["equals"]), float(check.get("equals_tolerance", 0.0)), None
    return None, None, 0.0, "No assertion found. Use expectation/expected_max/expected_min/equals."


def evaluate_assertion(value: float, op: str, target: float, equals_tolerance: float) -> bool:
    if op == "<=":
        return value <= target
    if op == "<":
        return value < target
    if op == ">=":
        return value >= target
    if op == ">":
        return value > target
    if op in ("==", "="):
        return abs(value - target) <= equals_tolerance
    raise ValueError(f"Unsupported assertion operator: {op}")


def run_custom_checks(
    project: str,
    base_table: str,
    cand_table: str,
    custom_checks: List[Dict[str, Any]],
) -> List[CheckResult]:
    results: List[CheckResult] = []

    for check in custom_checks:
        name = check.get("name", "unnamed_custom_check")
        severity = check.get("severity", "high")
        compare_to_baseline = bool(check.get("compare_to_baseline")) or ("baseline_sql" in check)
        try:
            if compare_to_baseline:
                sql_template = check.get("sql")
                baseline_sql_tmpl = check.get("baseline_sql", sql_template)
                candidate_sql_tmpl = check.get("candidate_sql", sql_template)
                if not baseline_sql_tmpl or not candidate_sql_tmpl:
                    raise ValueError(
                        "comparison custom check requires sql or both baseline_sql and candidate_sql"
                    )
                baseline_sql = render_sql_template(baseline_sql_tmpl, base_table, cand_table, base_table)
                candidate_sql = render_sql_template(candidate_sql_tmpl, base_table, cand_table, cand_table)
                b = query_scalar(project, baseline_sql)
                c = query_scalar(project, candidate_sql)
                delta = c - b
                abs_delta = abs(delta)
                abs_pct = abs_pct_delta(b, c)
                passed, tol_abs, tol_pct = evaluate_tolerance(
                    abs_delta,
                    abs_pct,
                    {
                        "max_abs_delta": check.get("max_abs_delta"),
                        "max_abs_pct": check.get("max_abs_pct"),
                    },
                )
                results.append(
                    CheckResult(
                        name=f"custom_{name}",
                        passed=passed,
                        severity=severity,
                        summary=(
                            f"baseline={b:.6f}, candidate={c:.6f}, abs_delta={abs_delta:.6f}, "
                            f"abs_pct={abs_pct:.6f}, tol_abs={tol_abs}, tol_pct={tol_pct}"
                        ),
                        details={
                            "baseline_sql": baseline_sql,
                            "candidate_sql": candidate_sql,
                            "baseline_value": b,
                            "candidate_value": c,
                            "delta": delta,
                            "abs_pct": abs_pct,
                            "tolerance_abs_delta": tol_abs,
                            "tolerance_abs_pct": tol_pct,
                        },
                    )
                )
                continue

            scope = check.get("scope", "candidate")
            table = base_table if scope == "baseline" else cand_table
            sql_template = check.get("sql", check.get("candidate_sql"))
            if not sql_template:
                raise ValueError("non-comparison custom check requires sql or candidate_sql")
            sql_text = render_sql_template(sql_template, base_table, cand_table, table)
            value = query_scalar(project, sql_text)
            op, target, eq_tol, err = parse_assertion(check)
            if err:
                raise ValueError(err)
            assert op is not None and target is not None
            passed = evaluate_assertion(value, op, target, eq_tol)
            results.append(
                CheckResult(
                    name=f"custom_{name}",
                    passed=passed,
                    severity=severity,
                    summary=(
                        f"value={value:.6f}, op={op}, target={target:.6f}, "
                        f"equals_tolerance={eq_tol:.6f}, scope={scope}"
                    ),
                    details={
                        "scope": scope,
                        "sql": sql_text,
                        "value": value,
                        "op": op,
                        "target": target,
                        "equals_tolerance": eq_tol,
                    },
                )
            )
        except Exception as exc:
            results.append(
                CheckResult(
                    name=f"custom_{name}",
                    passed=False,
                    severity="critical",
                    summary=f"error: {exc}",
                    details={"check": check},
                )
            )
    return results


def run_schema_check(base_schema: List[Dict[str, Any]], cand_schema: List[Dict[str, Any]]) -> CheckResult:
    def key(row: Dict[str, Any]) -> Tuple[str, str, str]:
        return (row["column_name"], row["data_type"], row["is_nullable"])

    bset = {key(row) for row in base_schema}
    cset = {key(row) for row in cand_schema}
    removed = sorted(list(bset - cset))
    added_or_changed = sorted(list(cset - bset))
    passed = len(removed) == 0
    return CheckResult(
        name="schema_compatibility",
        passed=passed,
        severity="critical",
        summary=f"removed={len(removed)}, added_or_changed={len(added_or_changed)}",
        details={"removed": removed, "added_or_changed": added_or_changed},
    )


def run_downstream_checks(
    project: str,
    downstream_checks: List[Dict[str, Any]],
    base_table: str,
    cand_table: str,
) -> List[CheckResult]:
    results: List[CheckResult] = []

    for check in downstream_checks:
        name = check.get("name", "unnamed_downstream_check")
        severity = check.get("severity", "critical")
        mode = check.get("mode", "rowset")
        try:
            if mode == "scalar":
                sql_template = check.get("sql")
                base_sql_tmpl = check.get("baseline_sql", sql_template)
                cand_sql_tmpl = check.get("candidate_sql", sql_template)
                if not base_sql_tmpl or not cand_sql_tmpl:
                    raise ValueError("downstream scalar check requires sql or baseline_sql+candidate_sql")
                base_sql = render_sql_template(base_sql_tmpl, base_table, cand_table, base_table)
                cand_sql = render_sql_template(cand_sql_tmpl, base_table, cand_table, cand_table)
                base_value = query_scalar(project, base_sql)
                cand_value = query_scalar(project, cand_sql)
                delta = cand_value - base_value
                abs_delta = abs(delta)
                abs_pct = abs_pct_delta(base_value, cand_value)
                passed, tol_abs, tol_pct = evaluate_tolerance(
                    abs_delta,
                    abs_pct,
                    {
                        "max_abs_delta": check.get("max_abs_delta"),
                        "max_abs_pct": check.get("max_abs_pct"),
                    },
                )
                results.append(
                    CheckResult(
                        name=f"downstream_{name}",
                        passed=passed,
                        severity=severity,
                        summary=(
                            f"baseline={base_value:.6f}, candidate={cand_value:.6f}, "
                            f"abs_delta={abs_delta:.6f}, abs_pct={abs_pct:.6f}, "
                            f"tol_abs={tol_abs}, tol_pct={tol_pct}"
                        ),
                        details={
                            "baseline_sql": base_sql,
                            "candidate_sql": cand_sql,
                            "baseline_value": base_value,
                            "candidate_value": cand_value,
                            "delta": delta,
                            "abs_pct": abs_pct,
                            "tolerance_abs_delta": tol_abs,
                            "tolerance_abs_pct": tol_pct,
                        },
                    )
                )
                continue

            sql_text = check.get("sql")
            sql_file = check.get("sql_file")
            if not sql_text and sql_file:
                sql_text = Path(sql_file).read_text()
            if not sql_text:
                raise ValueError("downstream rowset check requires sql or sql_file")

            base_sql = render_sql_template(sql_text, base_table, cand_table, base_table)
            cand_sql = render_sql_template(sql_text, base_table, cand_table, cand_table)
            base_rows = query_json(project, base_sql)
            cand_rows = query_json(project, cand_sql)
            base_count = len(base_rows)
            cand_count = len(cand_rows)
            max_row_count_delta = int(check.get("max_row_count_delta", 0))
            row_count_ok = abs(base_count - cand_count) <= max_row_count_delta

            same_schema = True
            if base_rows and cand_rows:
                same_schema = list(base_rows[0].keys()) == list(cand_rows[0].keys())
            elif base_rows or cand_rows:
                same_schema = False

            passed = row_count_ok and same_schema
            results.append(
                CheckResult(
                    name=f"downstream_{name}",
                    passed=passed,
                    severity=severity,
                    summary=(
                        f"base_rows={base_count}, cand_rows={cand_count}, "
                        f"max_row_count_delta={max_row_count_delta}, same_schema={same_schema}"
                    ),
                    details={
                        "base_rows_preview": base_rows[:5],
                        "cand_rows_preview": cand_rows[:5],
                        "base_row_count": base_count,
                        "cand_row_count": cand_count,
                        "max_row_count_delta": max_row_count_delta,
                        "same_schema": same_schema,
                    },
                )
            )
        except Exception as exc:
            results.append(
                CheckResult(
                    name=f"downstream_{name}",
                    passed=False,
                    severity="critical",
                    summary=f"error: {exc}",
                    details={"check": check},
                )
            )

    return results


def build_comparison_rows(checks: List[CheckResult]) -> List[Dict[str, Any]]:
    rows: List[Dict[str, Any]] = []
    for check in checks:
        details = check.details
        baseline_value = details.get("baseline_value")
        candidate_value = details.get("candidate_value")

        if baseline_value is None and "baseline_count" in details:
            baseline_value = details.get("baseline_count")
            candidate_value = details.get("candidate_count")
        if baseline_value is None and "baseline_distinct" in details:
            baseline_value = details.get("baseline_distinct")
            candidate_value = details.get("candidate_distinct")

        if baseline_value is None or candidate_value is None:
            continue

        delta = details.get("delta")
        if delta is None:
            delta = to_float(candidate_value) - to_float(baseline_value)

        rows.append(
            {
                "check_name": check.name,
                "severity": check.severity,
                "passed": check.passed,
                "baseline_value": baseline_value,
                "candidate_value": candidate_value,
                "delta": delta,
                "abs_pct": details.get("abs_pct"),
                "tolerance_abs_delta": details.get("tolerance_abs_delta"),
                "tolerance_abs_pct": details.get("tolerance_abs_pct"),
                "summary": check.summary,
            }
        )
    return rows


def ensure_dir(path: str) -> None:
    Path(path).mkdir(parents=True, exist_ok=True)


def write_json(path: str, data: Any) -> None:
    Path(path).write_text(json.dumps(data, indent=2, sort_keys=True))


def write_csv(path: str, rows: List[Dict[str, Any]]) -> None:
    fieldnames = [
        "check_name",
        "severity",
        "passed",
        "baseline_value",
        "candidate_value",
        "delta",
        "abs_pct",
        "tolerance_abs_delta",
        "tolerance_abs_pct",
        "summary",
    ]
    with open(path, "w", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        for row in rows:
            writer.writerow(row)


def print_comparison_summary(rows: List[Dict[str, Any]], limit: int) -> None:
    print("COMPARISON_SUMMARY:")
    if not rows:
        print("- none")
        return
    for row in rows[:limit]:
        print(
            "- "
            f"{row['check_name']}: baseline={row['baseline_value']}, "
            f"candidate={row['candidate_value']}, delta={row['delta']}, "
            f"abs_pct={row['abs_pct']}, passed={row['passed']}"
        )
    hidden = len(rows) - min(len(rows), limit)
    if hidden > 0:
        print(f"- ... {hidden} more rows available in comparisons.csv")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run SQL change guard checks.")
    parser.add_argument("--project", required=True)
    parser.add_argument("--qa-dataset", required=True)
    parser.add_argument("--manifest", required=True)
    parser.add_argument("--output-dir", required=True)
    parser.add_argument("--query-backend", choices=["mcp", "bq"], default="mcp")
    parser.add_argument("--mcp-server", default="bigquery")
    parser.add_argument("--mcp-max-retries", type=int, default=2)
    parser.add_argument("--mcp-query-timeout-sec", type=int, default=90)
    parser.add_argument("--mode", choices=["changed", "new"], default="changed")
    parser.add_argument("--baseline-query-file")
    parser.add_argument("--baseline-table")
    parser.add_argument("--candidate-query-file")
    parser.add_argument("--candidate-table")
    parser.add_argument("--date-column")
    parser.add_argument("--window-months", default="auto")
    parser.add_argument("--exclude-recent-days", type=int, default=5)
    parser.add_argument("--min-rows-for-window", type=int, default=5000)
    parser.add_argument("--skip-live-baseline-check", action="store_true")
    parser.add_argument("--show-details", action="store_true")
    parser.add_argument("--show-comparisons", action="store_true")
    parser.add_argument("--prompt-details", action="store_true")
    parser.add_argument("--detail-limit", type=int, default=15)
    parser.add_argument("--comparison-limit", type=int, default=20)
    return parser.parse_args()


def main() -> None:
    global ACTIVE_QUERY_BACKEND, ACTIVE_MCP_SERVER, ACTIVE_MCP_MAX_RETRIES, ACTIVE_MCP_QUERY_TIMEOUT_SEC
    args = parse_args()
    ensure_dir(args.output_dir)
    ACTIVE_QUERY_BACKEND = args.query_backend
    ACTIVE_MCP_SERVER = args.mcp_server
    ACTIVE_MCP_MAX_RETRIES = max(0, args.mcp_max_retries)
    ACTIVE_MCP_QUERY_TIMEOUT_SEC = max(1, args.mcp_query_timeout_sec)

    manifest = load_manifest(args.manifest)
    keys = manifest.get("keys", [])
    metrics = manifest.get("metrics", [])
    derived_metrics = manifest.get("derived_metrics", [])
    allowed_deltas = manifest.get("allowed_deltas", {})
    row_count_tolerance = allowed_deltas.get("row_count", {})
    key_overlap_tolerance = manifest.get("key_overlap_tolerance", {})
    custom_checks = manifest.get("custom_checks", [])
    downstream_checks = manifest.get("downstream_checks", [])
    downstream_objects = manifest.get("downstream_objects", [])
    high_risk_metrics = manifest.get("high_risk_metrics", [])
    high_risk_dimensions = manifest.get("high_risk_dimensions", [])
    comparison_dimensions = manifest.get("comparison_dimensions", [])
    if not comparison_dimensions:
        comparison_dimensions = high_risk_dimensions

    preflight: Dict[str, Any] = {
        "query_backend": ACTIVE_QUERY_BACKEND,
        "mcp_server": ACTIVE_MCP_SERVER if ACTIVE_QUERY_BACKEND == "mcp" else None,
        "baseline_source": None,
        "baseline_live_table": None,
        "baseline_live_table_type": None,
        "baseline_live_table_found": None,
        "candidate_source": None,
        "exclude_recent_days": max(0, args.exclude_recent_days),
        "baseline_max_date": None,
        "candidate_max_date": None,
        "window_end_date": None,
    }

    candidate_source_sql = load_source_sql(args.candidate_query_file, args.candidate_table)
    if args.candidate_table:
        _, _, normalized_candidate = table_exists(args.project, args.candidate_table)
        candidate_source_sql = f"SELECT * FROM `{normalized_candidate}`"
        preflight["candidate_source"] = "candidate_table"
    else:
        preflight["candidate_source"] = "candidate_query_file"
        if looks_like_script(candidate_source_sql):
            inferred = infer_created_table_from_script(candidate_source_sql)
            if inferred:
                raise ValueError(
                    "candidate-query-file appears to be a multi-statement script with CREATE TABLE. "
                    "Materialize candidate output first, then run with --candidate-table "
                    f"(inferred target: {normalize_table_fqn(inferred, args.project)})."
                )
            raise ValueError(
                "candidate-query-file appears to be a multi-statement script. "
                "Materialize candidate output first, then run with --candidate-table."
            )

    baseline_source_sql: Optional[str] = None
    if args.mode == "changed":
        if args.baseline_table:
            if args.skip_live_baseline_check:
                _, _, normalized_baseline = table_exists(args.project, args.baseline_table)
                baseline_source_sql = f"SELECT * FROM `{normalized_baseline}`"
                preflight["baseline_source"] = "baseline_table_no_check"
                preflight["baseline_live_table"] = normalized_baseline
            else:
                exists, table_type, normalized_baseline = table_exists(args.project, args.baseline_table)
                preflight["baseline_live_table"] = normalized_baseline
                preflight["baseline_live_table_type"] = table_type
                preflight["baseline_live_table_found"] = exists
                if exists:
                    baseline_source_sql = f"SELECT * FROM `{normalized_baseline}`"
                    preflight["baseline_source"] = "baseline_live_table"
                elif args.baseline_query_file:
                    baseline_source_sql = load_source_sql(args.baseline_query_file, None)
                    preflight["baseline_source"] = "baseline_query_file_fallback"
                else:
                    raise ValueError(
                        f"baseline table not found: {normalized_baseline}. "
                        "Provide --baseline-query-file or a valid --baseline-table."
                    )
        elif args.baseline_query_file:
            baseline_query_sql = load_source_sql(args.baseline_query_file, None)
            inferred_baseline = infer_created_table_from_script(baseline_query_sql)
            if not args.skip_live_baseline_check and inferred_baseline:
                exists, table_type, normalized_inferred = table_exists(args.project, inferred_baseline)
                preflight["baseline_live_table"] = normalized_inferred
                preflight["baseline_live_table_type"] = table_type
                preflight["baseline_live_table_found"] = exists
                if exists:
                    baseline_source_sql = f"SELECT * FROM `{normalized_inferred}`"
                    preflight["baseline_source"] = "inferred_live_table_from_baseline_script"
                elif looks_like_script(baseline_query_sql):
                    raise ValueError(
                        "baseline-query-file appears to be a multi-statement script and its inferred live "
                        f"table does not exist: {normalized_inferred}. Provide --baseline-table."
                    )
                else:
                    baseline_source_sql = baseline_query_sql
                    preflight["baseline_source"] = "baseline_query_file"
            else:
                if looks_like_script(baseline_query_sql):
                    raise ValueError(
                        "baseline-query-file appears to be a multi-statement script. "
                        "Provide --baseline-table so the validator can compare against the live baseline output."
                    )
                baseline_source_sql = baseline_query_sql
                preflight["baseline_source"] = "baseline_query_file"
        else:
            raise ValueError(
                "changed mode requires baseline input. Provide --baseline-table or --baseline-query-file."
            )

    up_l1 = extract_table_refs(candidate_source_sql)
    up_l2 = set()
    for ref in up_l1:
        view_sql = maybe_get_view_query(args.project, ref)
        if view_sql:
            for child_ref in extract_table_refs(view_sql):
                up_l2.add(child_ref)

    fixed_months = None if args.window_months == "auto" else int(args.window_months)
    window_end_date, baseline_max_date, candidate_max_date = choose_window_end_date(
        project=args.project,
        date_column=args.date_column,
        candidate_source_sql=candidate_source_sql,
        baseline_source_sql=baseline_source_sql if args.mode == "changed" else None,
        exclude_recent_days=args.exclude_recent_days,
    )
    preflight["baseline_max_date"] = baseline_max_date
    preflight["candidate_max_date"] = candidate_max_date
    preflight["window_end_date"] = window_end_date

    months = choose_window_months(
        project=args.project,
        candidate_source_sql=candidate_source_sql,
        date_column=args.date_column,
        min_rows=args.min_rows_for_window,
        fixed_months=fixed_months,
        window_end_date=window_end_date,
    )

    run_id = str(int(time.time()))
    cand_table_name = f"sqlchg_guard_{run_id}_candidate"
    base_table_name = f"sqlchg_guard_{run_id}_baseline"

    cand_table = materialize_table(
        project=args.project,
        qa_dataset=args.qa_dataset,
        table_name=cand_table_name,
        source_sql=candidate_source_sql,
        date_column=args.date_column,
        months=months,
        window_end_date=window_end_date,
    )

    checks: List[CheckResult] = []
    base_schema: List[Dict[str, Any]] = []
    cand_schema = fetch_schema(args.project, cand_table)
    base_table: Optional[str] = None

    if args.mode == "changed" and baseline_source_sql:
        base_table = materialize_table(
            project=args.project,
            qa_dataset=args.qa_dataset,
            table_name=base_table_name,
            source_sql=baseline_source_sql,
            date_column=args.date_column,
            months=months,
            window_end_date=window_end_date,
        )
        base_schema = fetch_schema(args.project, base_table)

        checks.append(run_row_count_check(args.project, base_table, cand_table, row_count_tolerance))
        checks.append(run_duplicate_check(args.project, base_table, keys, "baseline"))
        checks.append(run_duplicate_check(args.project, cand_table, keys, "candidate"))
        checks.append(run_key_overlap_check(args.project, base_table, cand_table, keys, key_overlap_tolerance))
        checks.extend(run_metric_sum_checks(args.project, base_table, cand_table, metrics, allowed_deltas))
        checks.extend(
            run_derived_metric_checks(args.project, base_table, cand_table, derived_metrics, allowed_deltas)
        )
        checks.extend(
            run_dimension_distinct_checks(
                args.project,
                base_table,
                cand_table,
                comparison_dimensions,
                allowed_deltas,
            )
        )
        checks.extend(run_custom_checks(args.project, base_table, cand_table, custom_checks))
        checks.extend(run_downstream_checks(args.project, downstream_checks, base_table, cand_table))
        checks.append(run_schema_check(base_schema, cand_schema))
    else:
        checks.append(run_duplicate_check(args.project, cand_table, keys, "candidate"))
        checks.extend(run_custom_checks(args.project, cand_table, cand_table, custom_checks))

    failed = [check for check in checks if not check.passed]
    comparison_rows = build_comparison_rows(checks)

    summary = {
        "result": "PASS" if not failed else "FAIL",
        "failed_checks": len(failed),
        "total_checks": len(checks),
        "comparison_rows": len(comparison_rows),
        "window_months": months,
        "preflight": preflight,
        "lineage": {
            "upstream_level_1": up_l1,
            "upstream_level_2": sorted(up_l2),
            "downstream_declared": downstream_objects,
        },
        "high_risk": {
            "metrics": high_risk_metrics,
            "dimensions": high_risk_dimensions,
        },
        "qa_tables": {
            "candidate": cand_table,
            "baseline": base_table,
        },
    }
    details = {
        "checks": [check.__dict__ for check in checks],
        "failed_checks": [check.__dict__ for check in failed],
        "schema_baseline": base_schema,
        "schema_candidate": cand_schema,
        "comparison_rows": comparison_rows,
    }

    summary_path = os.path.join(args.output_dir, "summary.json")
    details_path = os.path.join(args.output_dir, "details.json")
    comparisons_json_path = os.path.join(args.output_dir, "comparisons.json")
    comparisons_csv_path = os.path.join(args.output_dir, "comparisons.csv")

    write_json(summary_path, summary)
    write_json(details_path, details)
    write_json(comparisons_json_path, comparison_rows)
    write_csv(comparisons_csv_path, comparison_rows)

    print(f"RESULT: {summary['result']}")
    print(f"FAILED_CHECKS: {summary['failed_checks']}")
    print(f"COMPARISON_ROWS: {summary['comparison_rows']}")
    print(f"QUERY_BACKEND: {ACTIVE_QUERY_BACKEND}")
    print(f"BASELINE_SOURCE: {preflight.get('baseline_source')}")
    print(f"SUMMARY_AVAILABLE: {summary_path}")
    print(f"DETAILS_AVAILABLE: {details_path}")
    print(f"COMPARISONS_AVAILABLE: {comparisons_csv_path}")

    if args.prompt_details and not args.show_details:
        try:
            answer = input("Show failing check details now? [y/N/more]: ").strip().lower()
            if answer in ("y", "yes"):
                args.show_details = True
            if answer in ("m", "more", "full"):
                args.show_details = True
                args.show_comparisons = True
        except EOFError:
            pass

    if args.show_comparisons:
        print_comparison_summary(comparison_rows, args.comparison_limit)
    elif not args.show_details:
        print("Show comparisons? Re-run with --show-comparisons.")

    if args.show_details:
        print("FAILED_CHECK_SUMMARY:")
        if not failed:
            print("- none")
        for check in failed[: args.detail_limit]:
            print(f"- {check.name}: {check.summary}")
        hidden = len(failed) - min(len(failed), args.detail_limit)
        if hidden > 0:
            print(f"- ... {hidden} more failing checks in details.json")
    elif not args.show_comparisons:
        print("Show details? Re-run with --show-details.")

    if failed:
        sys.exit(2)


if __name__ == "__main__":
    main()
