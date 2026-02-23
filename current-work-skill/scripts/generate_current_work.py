#!/usr/bin/env python3
"""Generate resume-first current-work docs with headline -> detail structure."""

from __future__ import annotations

import argparse
import json
import os
import re
import shlex
import subprocess
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, Iterable, List, Sequence, Tuple

DEFAULT_WORKSPACE_ROOT = Path("/Users/eugenetsenter/Looker_clonedRepo/looker_personal")
DEFAULT_CODEX_HOME = Path("/Users/eugenetsenter/.codex")
DEFAULT_GLOBAL_OUT = Path("/Users/eugenetsenter/.codex/current-work.md")
DEFAULT_WORKSPACE_OUT = DEFAULT_WORKSPACE_ROOT / "current-work.md"
DEFAULT_PKM_INBOX = Path(
    "/Users/eugenetsenter/Library/Mobile Documents/"
    "iCloud~md~obsidian/Documents/2026-ob-vault/2026-ob-vault/00 Inbox"
)

SKIP_DIRS = {
    ".git",
    ".history",
    ".vscode",
    ".cursor",
    "node_modules",
    "renv",
    "venv",
    "__pycache__",
    ".pytest_cache",
    ".Rproj.user",
}

TABLE_TOKEN_RE = re.compile(r"`?([A-Za-z0-9_-]+\.[A-Za-z0-9_-]+\.[A-Za-z0-9_-]+)`?")
ABS_PATH_RE = re.compile(r"(/[^ \t\n\r'\"`<>()\[\],;]+)")
DEFAULT_LOG_WINDOW_HOURS = 24
MAX_SESSION_FILES = 40
MAX_COMMAND_SIGNALS = 120
MAX_MESSAGE_SIGNALS = 200


@dataclass
class RemoteSnapshot:
    name: str
    url: str
    default_branch: str
    remote_head: str
    compare_to_local: str


@dataclass
class RepoSnapshot:
    name: str
    path: Path
    is_git: bool
    git_root: str
    branch: str
    local_head: str
    staged: int
    modified: int
    untracked: int
    recent_files: List[Tuple[float, Path]]
    tables: List[str]
    remotes: List[RemoteSnapshot]


@dataclass
class CommandSignal:
    epoch: float
    timestamp: str
    cmd: str
    workdir: str


@dataclass
class MessageSignal:
    epoch: float
    timestamp: str
    role: str
    text: str


@dataclass
class CodexSignals:
    sessions_dir: Path
    shell_snapshots_dir: Path
    latest_session_files: List[Path]
    open_tabs: List[str]
    open_tab_sources: List[str]
    recent_commands: List[CommandSignal]
    recent_messages: List[MessageSignal]
    command_paths: List[str]
    message_paths: List[str]
    log_window_hours: int


@dataclass
class Workstream:
    title: str
    summary: str
    why_it_matters: str
    priority_reason: str
    next_step_label: str
    next_step_cmd: str
    eta_minutes: int
    confidence: str
    signal_notes: List[str]
    paths: List[str]
    tables: List[str]


@dataclass
class RunLookups:
    workspace_root: Path
    scanned_dirs: List[Path]
    codex_paths: List[Path]
    latest_session_files: List[Path]
    git_remote_checks: List[str]
    open_tab_sources: List[str]


@dataclass
class Provenance:
    skill_name: str
    skill_path: str
    automation_name: str
    automation_id: str


def run_cmd(args: Sequence[str], cwd: Path | None = None) -> Tuple[int, str, str]:
    proc = subprocess.run(
        list(args),
        cwd=str(cwd) if cwd else None,
        text=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        check=False,
    )
    return proc.returncode, proc.stdout.strip(), proc.stderr.strip()


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Generate resume-first current-work docs")
    parser.add_argument("--workspace-root", default=str(DEFAULT_WORKSPACE_ROOT))
    parser.add_argument("--scan-dirs", default="", help="Comma-separated directory names or absolute paths")
    parser.add_argument("--max-dirs", type=int, default=0)
    parser.add_argument("--codex-home", default=str(DEFAULT_CODEX_HOME))
    parser.add_argument("--workspace-out", default=str(DEFAULT_WORKSPACE_OUT))
    parser.add_argument("--global-out", default=str(DEFAULT_GLOBAL_OUT))
    parser.add_argument("--project-out", default="")
    parser.add_argument("--run-dir", default="")
    parser.add_argument("--pkm-inbox", default=str(DEFAULT_PKM_INBOX))
    parser.add_argument("--log-window-hours", type=int, default=DEFAULT_LOG_WINDOW_HOURS)
    parser.add_argument("--skill-name", default="current-work-skill")
    parser.add_argument("--skill-path", default="")
    parser.add_argument("--automation-name", default="")
    parser.add_argument("--automation-id", default="")
    return parser.parse_args()


def resolve_scan_dirs(workspace_root: Path, scan_dirs_arg: str, max_dirs: int) -> List[Path]:
    dirs: List[Path] = []

    if scan_dirs_arg.strip():
        tokens = [x.strip() for x in scan_dirs_arg.split(",") if x.strip()]
        for token in tokens:
            p = Path(token)
            if not p.is_absolute():
                p = workspace_root / token
            p = p.resolve()
            if p.exists() and p.is_dir():
                dirs.append(p)
    else:
        for child in sorted(workspace_root.iterdir()):
            if not child.is_dir() or child.name.startswith("."):
                continue
            if child.name in {"current-work-skill", "archive", "~"}:
                continue
            dirs.append(child.resolve())

    if max_dirs > 0:
        dirs = dirs[:max_dirs]

    return dirs


def is_git_repo(path: Path) -> bool:
    rc, out, _ = run_cmd(["git", "-C", str(path), "rev-parse", "--is-inside-work-tree"])
    return rc == 0 and out.strip() == "true"


def git_root(path: Path) -> str:
    rc, out, _ = run_cmd(["git", "-C", str(path), "rev-parse", "--show-toplevel"])
    if rc == 0 and out.strip():
        return out.strip()
    return ""


def parse_git_status(path: Path) -> Tuple[str, str, int, int, int]:
    rc, out, _ = run_cmd(["git", "-C", str(path), "status", "--short", "--branch"])
    if rc != 0:
        return "", "", 0, 0, 0

    lines = [line for line in out.splitlines() if line.strip()]
    branch = ""
    staged = 0
    modified = 0
    untracked = 0

    if lines and lines[0].startswith("##"):
        branch = lines[0].replace("##", "", 1).strip()

    for line in lines[1:]:
        if line.startswith("??"):
            untracked += 1
            continue
        if len(line) >= 2:
            x = line[0]
            y = line[1]
            if x not in (" ", "?"):
                staged += 1
            if y not in (" ", "?"):
                modified += 1

    rc_head, head, _ = run_cmd(["git", "-C", str(path), "rev-parse", "HEAD"])
    local_head = head if rc_head == 0 else ""

    return branch, local_head, staged, modified, untracked


def parse_remote_default_branch(text: str) -> str:
    for line in text.splitlines():
        if line.startswith("ref:") and line.endswith("\tHEAD"):
            ref = line.split()[1]
            return ref.replace("refs/heads/", "")
    return ""


def parse_remote_head(text: str) -> str:
    for line in text.splitlines():
        if line.endswith("\tHEAD"):
            return line.split("\t", 1)[0]
    return ""


def remote_activity(path: Path, local_head: str, lookup_log: List[str]) -> List[RemoteSnapshot]:
    rc, out, _ = run_cmd(["git", "-C", str(path), "remote"])
    if rc != 0:
        return []

    remotes = [line.strip() for line in out.splitlines() if line.strip()]
    snapshots: List[RemoteSnapshot] = []

    for remote in remotes[:3]:
        rc_url, url, _ = run_cmd(["git", "-C", str(path), "remote", "get-url", remote])
        if rc_url != 0:
            continue

        lookup_log.append(f"git -C {shlex.quote(str(path))} ls-remote --symref {remote} HEAD")

        rc_sym, sym_out, _ = run_cmd(["git", "-C", str(path), "ls-remote", "--symref", remote, "HEAD"])
        rc_head, head_out, _ = run_cmd(["git", "-C", str(path), "ls-remote", remote, "HEAD"])

        default_branch = parse_remote_default_branch(sym_out) if rc_sym == 0 else ""
        remote_head = parse_remote_head(head_out) if rc_head == 0 else ""

        if not local_head:
            compare = "local repository has no commits"
        elif not remote_head:
            compare = "remote HEAD not available"
        elif local_head == remote_head:
            compare = "local HEAD matches remote HEAD"
        else:
            compare = "local HEAD differs from remote HEAD"

        snapshots.append(
            RemoteSnapshot(
                name=remote,
                url=url,
                default_branch=default_branch or "unknown",
                remote_head=remote_head or "unknown",
                compare_to_local=compare,
            )
        )

    return snapshots


def collect_recent_files(path: Path, limit: int = 20) -> List[Tuple[float, Path]]:
    rows: List[Tuple[float, Path]] = []

    for root, dirs, files in os.walk(path):
        dirs[:] = [d for d in dirs if d not in SKIP_DIRS]
        root_path = Path(root)
        if "scrap" in root_path.parts and "basis_utms" not in root_path.parts:
            continue
        for name in files:
            if name in {"current-work.md", ".DS_Store"}:
                continue
            full = root_path / name
            try:
                mtime = full.stat().st_mtime
            except OSError:
                continue
            rows.append((mtime, full))

    rows.sort(key=lambda x: x[0], reverse=True)
    return rows[:limit]


def extract_tables(paths: Iterable[Path], limit: int = 20) -> List[str]:
    seen = set()
    out: List[str] = []

    for path in paths:
        if path.suffix.lower() != ".sql":
            continue
        try:
            text = path.read_text(encoding="utf-8", errors="ignore")
        except OSError:
            continue

        for match in TABLE_TOKEN_RE.finditer(text):
            token = match.group(1)
            if token not in seen:
                seen.add(token)
                out.append(token)
                if len(out) >= limit:
                    return out
    return out


def parse_open_tabs_from_user_text(text: str) -> List[str]:
    tabs: List[str] = []
    lines = text.splitlines()
    in_block = False

    for line in lines:
        raw = line.rstrip()
        if raw.strip().lower() == "## open tabs:":
            in_block = True
            continue
        if in_block:
            if not raw.strip().startswith("-"):
                if raw.strip() == "":
                    continue
                break
            item = raw.strip()[1:].strip()
            if ":" in item:
                candidate = item.split(":", 1)[1].strip()
            else:
                candidate = item
            if candidate.startswith("/"):
                tabs.append(candidate)

    return tabs


def parse_iso_epoch(ts_text: str) -> float:
    if not ts_text:
        return 0.0
    try:
        cleaned = ts_text.replace("Z", "+00:00")
        return datetime.fromisoformat(cleaned).timestamp()
    except ValueError:
        return 0.0


def clean_text(text: str, limit: int = 300) -> str:
    squashed = " ".join(text.split())
    if len(squashed) <= limit:
        return squashed
    return squashed[: limit - 3] + "..."


def extract_paths_from_text(text: str) -> List[str]:
    paths: List[str] = []
    for token in ABS_PATH_RE.findall(text):
        if token.startswith("//"):
            continue
        if not (token.startswith("/Users/") or token.startswith("/tmp/")):
            continue
        if token.endswith("...") or token.endswith("/..."):
            continue
        if ":" in token and token.startswith("/Users/"):
            base, maybe_line = token.rsplit(":", 1)
            if maybe_line.isdigit():
                token = base
        if len(token) < 3 or token == "/":
            continue
        paths.append(token)
    return paths


def extract_text_items(content: object) -> List[str]:
    out: List[str] = []
    if not isinstance(content, list):
        return out
    for item in content:
        if not isinstance(item, dict):
            continue
        txt = item.get("text") or item.get("input_text") or item.get("output_text")
        if isinstance(txt, str) and txt.strip():
            out.append(txt)
    return out


def load_codex_signals(codex_home: Path, window_hours: int) -> CodexSignals:
    sessions_dir = codex_home / "sessions"
    shell_dir = codex_home / "shell_snapshots"

    now_epoch = datetime.now(timezone.utc).timestamp()
    cutoff_epoch = now_epoch - max(window_hours, 1) * 3600
    all_jsonl = sorted(
        sessions_dir.rglob("*.jsonl"),
        key=lambda p: p.stat().st_mtime if p.exists() else 0,
        reverse=True,
    )

    latest_jsonl: List[Path] = []
    for path in all_jsonl:
        try:
            if path.stat().st_mtime >= cutoff_epoch:
                latest_jsonl.append(path)
        except OSError:
            continue
        if len(latest_jsonl) >= MAX_SESSION_FILES:
            break

    if not latest_jsonl:
        latest_jsonl = all_jsonl[:8]

    tabs: List[str] = []
    tab_sources: List[str] = []
    command_sources: List[str] = []
    message_sources: List[str] = []
    recent_commands: List[CommandSignal] = []
    recent_messages: List[MessageSignal] = []
    command_paths: List[str] = []
    message_paths: List[str] = []

    for jsonl in latest_jsonl:
        try:
            with jsonl.open("r", encoding="utf-8", errors="ignore") as handle:
                for raw in handle:
                    try:
                        obj = json.loads(raw)
                    except json.JSONDecodeError:
                        continue
                    if obj.get("type") != "response_item":
                        continue

                    line_ts = obj.get("timestamp", "")
                    line_epoch = parse_iso_epoch(line_ts)
                    if line_epoch and line_epoch < cutoff_epoch:
                        continue

                    payload = obj.get("payload", {})
                    if not isinstance(payload, dict):
                        continue
                    payload_type = payload.get("type")

                    if payload_type == "function_call" and payload.get("name") == "exec_command":
                        raw_args = payload.get("arguments", "")
                        cmd = ""
                        workdir = ""
                        if isinstance(raw_args, str) and raw_args.strip():
                            try:
                                parsed_args = json.loads(raw_args)
                            except json.JSONDecodeError:
                                parsed_args = {}
                            if isinstance(parsed_args, dict):
                                cmd = str(parsed_args.get("cmd", "")).strip()
                                workdir = str(parsed_args.get("workdir", "")).strip()
                        if cmd:
                            recent_commands.append(
                                CommandSignal(
                                    epoch=line_epoch or jsonl.stat().st_mtime,
                                    timestamp=line_ts,
                                    cmd=clean_text(cmd, 280),
                                    workdir=workdir,
                                )
                            )
                            command_sources.append(str(jsonl))
                            command_paths.extend(extract_paths_from_text(cmd))
                            if workdir.startswith("/"):
                                command_paths.append(workdir)

                    if payload_type == "message":
                        role = str(payload.get("role", "")).strip().lower()
                        if role not in {"user", "assistant"}:
                            continue
                        texts = extract_text_items(payload.get("content", []))
                        for txt in texts:
                            normalized = clean_text(txt, 320)
                            if not normalized:
                                continue
                            recent_messages.append(
                                MessageSignal(
                                    epoch=line_epoch or jsonl.stat().st_mtime,
                                    timestamp=line_ts,
                                    role=role,
                                    text=normalized,
                                )
                            )
                            message_sources.append(str(jsonl))
                            message_paths.extend(extract_paths_from_text(txt))
                            if "## Open tabs:" in txt:
                                found = parse_open_tabs_from_user_text(txt)
                                if found:
                                    tabs.extend(found)
                                    tab_sources.append(str(jsonl))
        except OSError:
            continue

    dedup_tabs: List[str] = []
    seen_tabs = set()
    for tab in tabs:
        if tab not in seen_tabs:
            dedup_tabs.append(tab)
            seen_tabs.add(tab)

    dedup_sources: List[str] = []
    seen_sources = set()
    for src in tab_sources:
        if src not in seen_sources:
            dedup_sources.append(src)
            seen_sources.add(src)

    dedup_command_paths = dedup_keep_order(command_paths)
    dedup_message_paths = dedup_keep_order(message_paths)

    recent_commands.sort(key=lambda row: row.epoch, reverse=True)
    recent_messages.sort(key=lambda row: row.epoch, reverse=True)

    # Keep bounded signal lists to avoid oversized metadata and docs.
    recent_commands = recent_commands[:MAX_COMMAND_SIGNALS]
    recent_messages = recent_messages[:MAX_MESSAGE_SIGNALS]

    return CodexSignals(
        sessions_dir=sessions_dir,
        shell_snapshots_dir=shell_dir,
        latest_session_files=latest_jsonl,
        open_tabs=dedup_tabs,
        open_tab_sources=dedup_sources,
        recent_commands=recent_commands,
        recent_messages=recent_messages,
        command_paths=dedup_command_paths,
        message_paths=dedup_message_paths,
        log_window_hours=window_hours,
    )


def ts_local(epoch_seconds: float) -> str:
    return datetime.fromtimestamp(epoch_seconds).astimezone().strftime("%Y-%m-%d %H:%M:%S %Z")


def dedup_keep_order(items: Iterable[str]) -> List[str]:
    out: List[str] = []
    seen = set()
    for item in items:
        if item and item not in seen:
            seen.add(item)
            out.append(item)
    return out


def path_matches(path: str, keywords: Sequence[str]) -> bool:
    low = path.lower()
    return any(k in low for k in keywords)


def table_matches(table: str, keywords: Sequence[str]) -> bool:
    low = table.lower()
    return any(k in low for k in keywords)


def build_workstreams(repo_snapshots: List[RepoSnapshot], codex_signals: CodexSignals) -> List[Workstream]:
    defs = [
        {
            "title": "Current-work skill accuracy tuning",
            "summary": "You were actively improving how the current-work skill detects true active work from logs and repo signals.",
            "why": "Better detection prevents false top-priority tasks and makes restart guidance trustworthy.",
            "priority_reason": "Recent terminal activity and messages are centered on current-work-skill scoring improvements.",
            "next_label": "Run a sample generation and verify the top task matches the last 24-hour activity",
            "next_cmd": (
                "python /Users/eugenetsenter/Looker_clonedRepo/looker_personal/current-work-skill/scripts/generate_current_work.py \\\n"
                "  --workspace-root /Users/eugenetsenter/Looker_clonedRepo/looker_personal \\\n"
                "  --scan-dirs current-work-skill,mft,util \\\n"
                "  --workspace-out /Users/eugenetsenter/Looker_clonedRepo/looker_personal/current-work.md \\\n"
                "  --global-out /Users/eugenetsenter/.codex/current-work.md \\\n"
                "  --project-out /Users/eugenetsenter/Looker_clonedRepo/looker_personal/current-work-skill/output/current-work-workspace.md \\\n"
                "  --log-window-hours 24"
            ),
            "eta_minutes": 8,
            "path_keywords": [
                "current-work-skill",
                "generate_current_work.py",
                "current-work.md",
                "changelog.md",
                "readme.md",
            ],
            "table_keywords": [],
            "prefer_dirs": ["current-work-skill"],
        },
        {
            "title": "ADIF notebook production and docs alignment",
            "summary": "You were updating notebook-first ADIF production flow and syncing local docs to the live notebook behavior.",
            "why": "This keeps production runbooks and dependency docs aligned to the real pipeline target table and steps.",
            "priority_reason": "Recent ADIF notebook, README, and AGENTS updates indicate active production documentation alignment.",
            "next_label": "Run notebook dependency and target-table sanity checks",
            "next_cmd": (
                "cd /Users/eugenetsenter/Looker_clonedRepo/looker_personal/adif\n"
                "jq -r '.cells[] | select(.cell_type==\"markdown\") | (.source // []) | join(\"\")' "
                "projects/social_layering/build__adif__prisma_expanded_plus_dcm_with_social_tbl.ipynb | head -n 60\n"
                "rg -n \"adif__mainDataTable_notebook|Section 1|Section 2\" "
                "README.md AGENTS.md projects/social_layering/README.md"
            ),
            "eta_minutes": 8,
            "path_keywords": [
                "/adif/",
                "build__adif__prisma_expanded_plus_dcm_with_social_tbl.ipynb",
                "adif__maindatatable_notebook",
                "social_layering/readme.md",
                "legacy_scheduled_sql",
            ],
            "table_keywords": [
                "repo_stg.adif__maindatatable_notebook",
                "repo_stg.adif__prisma_expanded_plus_dcm_updated_fpd_view",
                "repo_stg.stg__adif__social_crossplatform",
                "repo_int.crossplatform_pacing",
            ],
            "prefer_dirs": ["adif"],
        },
        {
            "title": "DCM + UTM enrichment hardening",
            "summary": "You were tightening how DCM rows get UTM fields with a constrained fallback for specific campaigns.",
            "why": "This prevents null UTM rows and keeps attribution reporting stable.",
            "priority_reason": "Recent SQL edits and key table hits point to active DCM-UTM alignment work.",
            "next_label": "Run a null-UTM health check for scoped campaigns",
            "next_cmd": (
                "cd /Users/eugenetsenter/Looker_clonedRepo/looker_personal/mft\n"
                "./scripts/bq-safe-query.sh --max-rows 25 --sql \"SELECT campaign, COUNT(*) AS rows, "
                "COUNTIF(utm_content IS NULL) AS null_utm_content FROM `looker-studio-pro-452620.repo_stg.dcm_plus_utms` "
                "WHERE campaign IN ('MassMutual20252026Media','MassMutualLVGP2025') AND date >= '2026-01-01' "
                "GROUP BY 1 ORDER BY rows DESC\""
            ),
            "eta_minutes": 8,
            "path_keywords": ["repo_stg__dcm_plus_utms.sql", "dcm_plus_utms", "massmutual", "utms_view"],
            "table_keywords": ["repo_stg.dcm_plus_utms", "final_views.dcm", "final_views.utms_view", "repo_mart.mft_view"],
            "prefer_dirs": ["mft"],
        },
        {
            "title": "Offline sheet daily sync pipeline",
            "summary": "You were maintaining the sheet-to-BigQuery sync used for `mft_offline` updates.",
            "why": "If this schedule breaks, offline reporting can drift from source-sheet truth.",
            "priority_reason": "Recent scheduler and sync SQL files are concentrated in this workflow.",
            "next_label": "Verify scheduled-query status and latest rows",
            "next_cmd": (
                "bq ls --transfer_config --transfer_location=US --project_id=looker-studio-pro-452620\n"
                "bq show --transfer_config --transfer_location=US "
                "projects/671028410185/locations/us/transferConfigs/699421ab-0000-2129-a27e-883d24f0f1b8\n"
                "bq head -n 5 looker-studio-pro-452620:mass_mutual_mft_ext.mft_offline"
            ),
            "eta_minutes": 7,
            "path_keywords": ["mft_offline", "setup-mft-offline", "stg__mm__mft_offline", "sheet_sync"],
            "table_keywords": ["mass_mutual_mft_ext.mft_offline", "repo_stg.stg__mm__mft_offline_connected_gsheet"],
            "prefer_dirs": ["mft"],
        },
        {
            "title": "Basis UTM merge and FY26 Q1 backfill",
            "summary": "You were working on Basis UTM union/backfill logic and a patched SQL flow in `/tmp`.",
            "why": "This controls whether Basis delivery rows join into UTM layers without gaps or duplicate logic.",
            "priority_reason": "Open tabs and recent util file edits both point to Basis UTM backfill tasks right now.",
            "next_label": "Validate patched SQL once, then decide whether to move it into repo",
            "next_cmd": "bq query --use_legacy_sql=false < /tmp/utm_updates_patched.sql",
            "eta_minutes": 10,
            "path_keywords": ["basis_utms", "utm_updates_patched", "fy26_q1", "unioned", "pivot_longer"],
            "table_keywords": ["landing.basis_utms", "utm_scrap", "data_model_2025.mm_utms", "repo_stg.basis_utms"],
            "prefer_dirs": ["util"],
        },
    ]

    repo_recent_paths: List[Tuple[float, str, str]] = []
    open_tab_paths: List[str] = dedup_keep_order(codex_signals.open_tabs)
    all_tables: List[str] = []
    now_epoch = datetime.now().timestamp()

    for repo in repo_snapshots:
        for ts, p in repo.recent_files:
            repo_recent_paths.append((ts, str(p), repo.name))
        all_tables.extend(repo.tables)

    def table_is_noise(token: str) -> bool:
        low = token.lower()
        if low.endswith(".del_key") or low.endswith(".key"):
            return True
        if low.count(".") != 2:
            return True
        return False

    all_tables = [t for t in dedup_keep_order(all_tables) if not table_is_noise(t)]

    scored: List[Tuple[int, Workstream]] = []
    for d in defs:
        matched_paths: List[str] = []
        matched_tables: List[str] = []
        signals: List[str] = []
        score = 0
        command_score = 0
        message_score = 0
        command_hits = 0
        message_hits = 0

        for ts, path, repo_name in repo_recent_paths:
            if path_matches(path, d["path_keywords"]):
                age_hours = max((now_epoch - ts) / 3600, 0)
                if age_hours <= 3:
                    bonus = 6
                elif age_hours <= 12:
                    bonus = 4
                elif age_hours <= 24:
                    bonus = 3
                else:
                    bonus = 1
                if repo_name in d["prefer_dirs"]:
                    bonus += 2
                score += bonus
                matched_paths.append(f"{ts_local(ts)} - {path}")

        for tab in open_tab_paths:
            if path_matches(tab, d["path_keywords"]):
                # Tabs remain useful, but no longer dominate ranking.
                score += 1
                matched_paths.append(tab)
                signals.append("open-tab hit")

        for path in codex_signals.command_paths:
            if path_matches(path, d["path_keywords"]):
                matched_paths.append(path)
                score += 3

        for path in codex_signals.message_paths:
            if path_matches(path, d["path_keywords"]):
                matched_paths.append(path)
                score += 3

        for table in all_tables:
            if table_matches(table, d["table_keywords"]):
                score += 2
                matched_tables.append(table)

        for command in codex_signals.recent_commands:
            hay = f"{command.workdir} {command.cmd}".lower()
            if path_matches(hay, d["path_keywords"]) or table_matches(hay, d["table_keywords"]):
                age_hours = max((now_epoch - command.epoch) / 3600, 0)
                if age_hours <= 1:
                    command_score += 8
                elif age_hours <= 6:
                    command_score += 6
                elif age_hours <= 24:
                    command_score += 4
                else:
                    command_score += 2
                command_hits += 1
                if command.workdir.startswith("/"):
                    matched_paths.append(f"{ts_local(command.epoch)} - {command.workdir}")

        for message in codex_signals.recent_messages:
            hay = message.text.lower()
            if path_matches(hay, d["path_keywords"]) or table_matches(hay, d["table_keywords"]):
                age_hours = max((now_epoch - message.epoch) / 3600, 0)
                if age_hours <= 1:
                    message_score += 6
                elif age_hours <= 6:
                    message_score += 5
                elif age_hours <= 24:
                    message_score += 4
                else:
                    message_score += 2
                message_hits += 1

        score += min(command_score, 36)
        score += min(message_score, 30)

        for repo in repo_snapshots:
            if repo.name not in d["prefer_dirs"]:
                continue
            if repo.staged + repo.modified + repo.untracked <= 0:
                continue
            git_score = min(12, repo.staged * 3 + repo.modified * 2 + min(repo.untracked, 4))
            score += git_score
            signals.append(
                f"git activity in {repo.name}: staged={repo.staged}, modified={repo.modified}, untracked={repo.untracked}"
            )

        matched_paths = dedup_keep_order(matched_paths)
        matched_tables = dedup_keep_order(matched_tables)

        ordered_tables: List[str] = []
        for keyword in d["table_keywords"]:
            for table in matched_tables:
                if keyword in table.lower() and table not in ordered_tables:
                    ordered_tables.append(table)
        for table in matched_tables:
            if table not in ordered_tables:
                ordered_tables.append(table)
        matched_tables = ordered_tables

        if score <= 0 and not matched_paths and not matched_tables:
            continue

        if score >= 16:
            confidence = "high"
        elif score >= 9:
            confidence = "medium"
        else:
            confidence = "low"

        if matched_paths:
            signals.append(f"path hits={len(matched_paths)}")
        if matched_tables:
            signals.append(f"table hits={len(matched_tables)}")
        if command_hits:
            signals.append(f"log command hits={command_hits} (last {codex_signals.log_window_hours}h)")
        if message_hits:
            signals.append(f"log message hits={message_hits} (last {codex_signals.log_window_hours}h)")

        scored.append(
            (
                score,
                Workstream(
                    title=d["title"],
                    summary=d["summary"],
                    why_it_matters=d["why"],
                    priority_reason=d["priority_reason"],
                    next_step_label=d["next_label"],
                    next_step_cmd=d["next_cmd"],
                    eta_minutes=d["eta_minutes"],
                    confidence=confidence,
                    signal_notes=dedup_keep_order(signals)[:4],
                    paths=matched_paths[:12],
                    tables=matched_tables[:6],
                ),
            )
        )

    scored.sort(key=lambda row: row[0], reverse=True)
    selected = [ws for _, ws in scored[:3]]

    if selected:
        return selected

    fallback_paths = dedup_keep_order(
        [p for _, p, _ in repo_recent_paths] + codex_signals.command_paths + codex_signals.message_paths
    )[:12]
    fallback_cmd = "git status --short"
    fallback_label = "Create checkpoint commit"
    if codex_signals.recent_commands:
        fallback_cmd = codex_signals.recent_commands[0].cmd
        fallback_label = "Re-run your most recent terminal command to resume context"
    fallback_summary = "Recent file, git, and agent-log signals were detected, but no named workstream scored strongly."
    return [
        Workstream(
            title="General workspace maintenance",
            summary=fallback_summary,
            why_it_matters="A quick checkpoint commit keeps your context safe while you reorient.",
            priority_reason=(
                f"No high-confidence themed cluster was detected across the last {codex_signals.log_window_hours} hours."
            ),
            next_step_label=fallback_label,
            next_step_cmd=fallback_cmd,
            eta_minutes=5,
            confidence="low",
            signal_notes=[f"fallback mode (last {codex_signals.log_window_hours}h logs included)"],
            paths=fallback_paths,
            tables=all_tables[:6],
        )
    ]


def render_list(items: List[str], empty_text: str) -> str:
    if not items:
        return f"- {empty_text}\n"
    return "".join(f"- `{item}`\n" for item in items)


def extract_abs_path(item: str) -> str:
    token = item.strip()
    if token.startswith("/"):
        return token
    if " - /" in token:
        _, suffix = token.split(" - /", 1)
        return "/" + suffix.strip()
    return ""


def format_clickable_path(item: str) -> str:
    token = item.strip()
    if token.startswith("/"):
        return f"[{token}]({token})"
    if " - /" in token:
        prefix, suffix = token.split(" - /", 1)
        abs_path = "/" + suffix.strip()
        return f"{prefix.strip()} - [{abs_path}]({abs_path})"
    return f"`{token}`"


def render_path_list(items: List[str], empty_text: str) -> str:
    if not items:
        return f"- {empty_text}\n"
    return "".join(f"- {format_clickable_path(item)}\n" for item in items)


def build_how_to_text(workstream: Workstream) -> str:
    label = workstream.next_step_label.lower()
    abs_paths = dedup_keep_order([extract_abs_path(p) for p in workstream.paths if extract_abs_path(p)])
    links = [f"[{Path(p).name}]({p})" for p in abs_paths[:3]]
    links_text = ", ".join(links)

    if "validate" in label and ("decide" in label or "move" in label):
        base = (
            "Run the command one time and confirm the output looks sane; "
            "then choose whether to keep temporary logic or copy it into the repo file."
        )
    elif "validate" in label:
        base = "Run the command one time and confirm the output looks sane before changing files."
    elif "verify" in label:
        base = "Run the check, confirm expected status and recent rows, then capture what you saw."
    elif "decide" in label or "move" in label:
        base = "Review the result first, then make the keep-vs-move decision and record it."
    elif "run" in label:
        base = "Run the command and confirm the key metric changed in the expected direction."
    else:
        base = "Run the command, confirm expected results, and note the outcome."

    if links_text:
        return f"{base} Start with {links_text}."
    return base


def render_provenance(provenance: Provenance) -> str:
    lines = ["## Snapshot Source\n\n"]
    lines.append(
        f"- Skill: `{provenance.skill_name or 'unknown'}` "
        f"({provenance.skill_path or 'unknown'})\n"
    )
    lines.append(
        f"- Automation: `{provenance.automation_name or 'unknown'}` "
        f"(`{provenance.automation_id or 'unknown'}`)\n\n"
    )
    return "".join(lines)


def render_entry_source_line(provenance: Provenance) -> str:
    return (
        "Source: "
        f"`{provenance.skill_name or 'unknown'}` skill via "
        f"`{provenance.automation_name or 'unknown'}` automation."
    )


def render_headlines(workstreams: List[Workstream]) -> str:
    lines = ["## What You Were Working On\n\n"]
    for i, ws in enumerate(workstreams, start=1):
        lines.append(
            f"{i}. **{ws.title}** - {ws.summary} "
            f"Confidence: {ws.confidence}. Next: {ws.next_step_label} (about {ws.eta_minutes} minutes).\n"
        )
    lines.append("\n")
    return "".join(lines)


def render_details(workstreams: List[Workstream], provenance: Provenance) -> str:
    lines = ["## What You Should Do Next (In Order)\n\n"]
    if workstreams:
        lines.append("### Do First (5 Minutes)\n")
        lines.append(f"{workstreams[0].next_step_label} (estimated {workstreams[0].eta_minutes} minutes)\n\n")
        lines.append("```bash\n")
        lines.append(workstreams[0].next_step_cmd + "\n")
        lines.append("```\n\n")

    for i, ws in enumerate(workstreams, start=1):
        lines.append(f"### {i}) {ws.title}\n")
        lines.append(f"What this means: {ws.summary}\n\n")
        lines.append(f"Entry source: {render_entry_source_line(provenance)}\n\n")
        lines.append(f"Confidence: {ws.confidence}\n\n")
        lines.append(f"Why this is prioritized now: {ws.priority_reason}\n\n")
        lines.append(f"Why it matters: {ws.why_it_matters}\n\n")
        lines.append(f"Next step now: {ws.next_step_label} (estimated {ws.eta_minutes} minutes)\n\n")
        lines.append(f"How to do this: {build_how_to_text(ws)}\n\n")
        lines.append("```bash\n")
        lines.append(ws.next_step_cmd + "\n")
        lines.append("```\n\n")

        lines.append(f"<details><summary>Signals - {ws.title}</summary>\n\n")
        lines.append(render_list(ws.signal_notes, "No signal notes"))
        lines.append("\n</details>\n\n")

        lines.append(f"<details><summary>Paths - {ws.title}</summary>\n\n")
        lines.append(render_path_list(ws.paths, "None detected"))
        lines.append("\n</details>\n\n")

        lines.append(f"<details><summary>Tables - {ws.title}</summary>\n\n")
        lines.append(render_list(ws.tables, "None detected"))
        lines.append("\n</details>\n\n")

    return "".join(lines)


def render_repo_status(repo_snapshots: List[RepoSnapshot]) -> str:
    lines = ["## Repo Status\n\n"]
    seen_roots = set()
    for repo in repo_snapshots:
        root_key = repo.git_root or str(repo.path)
        if root_key in seen_roots:
            continue
        seen_roots.add(root_key)
        lines.append(f"### `{repo.name}`\n")
        if not repo.is_git:
            lines.append("- Not a git repository.\n\n")
            continue
        lines.append(f"- Branch[^3]: `{repo.branch or 'unknown'}`\n")
        lines.append(f"- Working tree[^1]: staged={repo.staged}, modified={repo.modified}, untracked={repo.untracked}\n")
        lines.append("- Remote activity[^2]:\n")
        if repo.remotes:
            for rm in repo.remotes:
                lines.append(
                    f"  - `{rm.name}` ({rm.default_branch}) => {rm.compare_to_local}; url=`{rm.url}`\n"
                )
        else:
            lines.append("  - none detected\n")
        lines.append("\n")
    return "".join(lines)


def render_open_tabs(open_tabs: List[str]) -> str:
    lines = ["## Open Files Seen From VS Code Context[^4]\n\n"]
    if not open_tabs:
        lines.append("- No open-file list found in recent local session logs.\n\n")
        return "".join(lines)
    for tab in open_tabs[:25]:
        lines.append(f"- {format_clickable_path(tab)}\n")
    lines.append("\n")
    return "".join(lines)


def render_risks(repo_snapshots: List[RepoSnapshot], open_tabs: List[str]) -> str:
    risks: List[str] = []

    for repo in repo_snapshots:
        if repo.is_git and not repo.local_head:
            risks.append(f"`{repo.name}` has no local commit history.")
        if repo.untracked >= 10:
            risks.append(f"`{repo.name}` has a high untracked count ({repo.untracked}).")

    if any(tab.startswith("/tmp/") for tab in open_tabs):
        risks.append("At least one active file is under `/tmp`, which is temporary storage.")

    if not risks:
        risks.append("No high-risk signal found in this scan.")

    lines = ["## Current Risks\n\n"]
    for risk in dedup_keep_order(risks):
        lines.append(f"- {risk}\n")
    lines.append("\n")
    return "".join(lines)


def render_where_looked(lookups: RunLookups) -> str:
    lines = ["## Where I Looked This Run\n\n"]
    lines.append(f"- Workspace root: `{lookups.workspace_root}`\n")

    lines.append("- Scanned directories:\n")
    for path in lookups.scanned_dirs:
        lines.append(f"  - `{path}`\n")

    lines.append("- Codex data paths:\n")
    for path in lookups.codex_paths:
        lines.append(f"  - `{path}`\n")

    lines.append("- Latest session files inspected:\n")
    for p in lookups.latest_session_files[:8]:
        lines.append(f"  - `{p}`\n")

    lines.append("- Remote git checks executed:\n")
    if lookups.git_remote_checks:
        for cmd in lookups.git_remote_checks:
            lines.append(f"  - `{cmd}`\n")
    else:
        lines.append("  - `none`\n")

    lines.append("- Open-tab sources:\n")
    if lookups.open_tab_sources:
        for src in lookups.open_tab_sources:
            lines.append(f"  - `{src}`\n")
    else:
        lines.append("  - `No open-tab block found in inspected session files`\n")

    lines.append("\n")
    return "".join(lines)


def render_scope_note(omitted_dirs: List[Path]) -> str:
    if not omitted_dirs:
        return ""

    lines = ["## Sample Scope Note\n\n"]
    lines.append("- This run used a limited directory set, so some workspace areas were intentionally skipped.\n")
    lines.append("- Skipped top-level directories in this sample run:\n")
    for p in omitted_dirs[:20]:
        lines.append(f"  - `{p}`\n")
    lines.append("\n")
    return "".join(lines)


def render_footnotes() -> str:
    return (
        "## Footnotes\n\n"
        "[^1]: Working tree means your local file state before commit.\n"
        "[^2]: Remote means the server-side git copy (for example GitHub).\n"
        "[^3]: Branch means a named commit line in git.\n"
        "[^4]: VS Code context here comes from local session log messages that include open tabs.\n"
        "[^5]: PKM means personal knowledge management notes.\n"
    )


def build_workspace_doc(
    now_text: str,
    workspace_root: Path,
    scanned_dirs: List[Path],
    workstreams: List[Workstream],
    repo_snapshots: List[RepoSnapshot],
    codex_signals: CodexSignals,
    lookups: RunLookups,
    omitted_dirs: List[Path],
    provenance: Provenance,
) -> str:
    parts: List[str] = []
    parts.append("# Current Work - Workspace (Resume Fast)\n\n")
    parts.append(f"Updated: `{now_text}`\n")
    parts.append(f"Workspace root: `{workspace_root}`\n")
    parts.append(f"Directories scanned this run: `{len(scanned_dirs)}`\n\n")
    parts.append(render_provenance(provenance))

    parts.append(render_headlines(workstreams))
    parts.append(render_details(workstreams, provenance))
    parts.append(render_repo_status(repo_snapshots))
    parts.append(render_open_tabs(codex_signals.open_tabs))
    parts.append(render_risks(repo_snapshots, codex_signals.open_tabs))
    parts.append(render_scope_note(omitted_dirs))
    parts.append(render_footnotes())
    return "".join(parts)


def build_global_doc(
    now_text: str,
    workspace_root: Path,
    workstreams: List[Workstream],
    lookups: RunLookups,
    omitted_dirs: List[Path],
    provenance: Provenance,
) -> str:
    parts: List[str] = []
    parts.append("# Current Work - Global (Resume Fast)\n\n")
    parts.append(f"Updated: `{now_text}`\n")
    parts.append(f"Primary workspace: `{workspace_root}`\n\n")
    parts.append(render_provenance(provenance))

    parts.append(render_headlines(workstreams))
    parts.append(render_details(workstreams, provenance))
    parts.append(render_scope_note(omitted_dirs))
    parts.append(render_footnotes())
    return "".join(parts)


def build_pkm_note(
    now_text: str,
    workspace_root: Path,
    workstreams: List[Workstream],
    workspace_out: Path,
    global_out: Path,
    lookups: RunLookups,
    provenance: Provenance,
) -> str:
    date_str = datetime.now().astimezone().strftime("%Y-%m-%d")
    lines: List[str] = []
    lines.append("---\n")
    lines.append(f"title: Current Work Snapshot - {workspace_root.name}\n")
    lines.append(f"date: {date_str}\n")
    lines.append("tags: [codex, handoff, current-work, non-interactive]\n")
    lines.append("status: captured\n")
    lines.append("---\n\n")

    lines.append("# Numbered Summary\n\n")
    lines.append(f"1. Captured workspace snapshot at `{now_text}` in non-interactive mode.\n")
    lines.append(f"2. Workspace root: `{workspace_root}`.\n")
    for i, ws in enumerate(workstreams, start=3):
        lines.append(f"{i}. {ws.title}: {ws.summary} Next action is ~{ws.eta_minutes} minutes.\n")

    lines.append("\n# Captured Files\n\n")
    lines.append(f"- Workspace doc: `{workspace_out}`\n")
    lines.append(f"- Global doc: `{global_out}`\n")
    lines.append(f"- Skill: `{provenance.skill_name or 'unknown'}`\n")
    lines.append(f"- Skill path: `{provenance.skill_path or 'unknown'}`\n")
    lines.append(f"- Automation: `{provenance.automation_name or 'unknown'}` (`{provenance.automation_id or 'unknown'}`)\n")

    lines.append("\n# Where Looked\n\n")
    for p in lookups.scanned_dirs:
        lines.append(f"- `{p}`\n")
    for p in lookups.codex_paths:
        lines.append(f"- `{p}`\n")

    lines.append("\n# What To Do Next\n\n")
    for ws in workstreams:
        lines.append(f"- {ws.next_step_label}\n")

    return "".join(lines)


def write_text(path: Path, text: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(text, encoding="utf-8")


def main() -> int:
    args = parse_args()

    workspace_root = Path(args.workspace_root).expanduser().resolve()
    codex_home = Path(args.codex_home).expanduser().resolve()
    workspace_out = Path(args.workspace_out).expanduser().resolve()
    global_out = Path(args.global_out).expanduser().resolve()
    project_out = Path(args.project_out).expanduser().resolve() if args.project_out else None
    run_dir = Path(args.run_dir).expanduser().resolve() if args.run_dir else None
    pkm_inbox = Path(args.pkm_inbox).expanduser().resolve()
    provenance = Provenance(
        skill_name=args.skill_name.strip(),
        skill_path=args.skill_path.strip(),
        automation_name=args.automation_name.strip(),
        automation_id=args.automation_id.strip(),
    )
    if not provenance.skill_path:
        provenance.skill_path = str((Path(__file__).resolve().parent.parent / "SKILL.md"))
    if not provenance.automation_name:
        provenance.automation_name = "manual-run"
    if not provenance.automation_id:
        provenance.automation_id = "none"

    scan_dirs = resolve_scan_dirs(workspace_root, args.scan_dirs, args.max_dirs)
    scanned_set = {str(p) for p in scan_dirs}
    all_top_dirs = [
        p.resolve()
        for p in workspace_root.iterdir()
        if p.is_dir() and not p.name.startswith(".") and p.name not in {"current-work-skill", "archive", "~"}
    ]
    omitted_dirs = [p for p in all_top_dirs if str(p) not in scanned_set]

    remote_checks: List[str] = []
    repo_snapshots: List[RepoSnapshot] = []

    for directory in scan_dirs:
        git_repo = is_git_repo(directory)
        repo_root = git_root(directory) if git_repo else ""

        branch = ""
        local_head = ""
        staged = modified = untracked = 0
        remotes: List[RemoteSnapshot] = []

        if git_repo:
            branch, local_head, staged, modified, untracked = parse_git_status(directory)
            remotes = remote_activity(directory, local_head, remote_checks)

        recent_files = collect_recent_files(directory)
        tables = extract_tables([p for _, p in recent_files])

        repo_snapshots.append(
            RepoSnapshot(
                name=directory.name,
                path=directory,
                is_git=git_repo,
                git_root=repo_root,
                branch=branch,
                local_head=local_head,
                staged=staged,
                modified=modified,
                untracked=untracked,
                recent_files=recent_files,
                tables=tables,
                remotes=remotes,
            )
        )

    codex_signals = load_codex_signals(codex_home, args.log_window_hours)
    workstreams = build_workstreams(repo_snapshots, codex_signals)

    lookups = RunLookups(
        workspace_root=workspace_root,
        scanned_dirs=scan_dirs,
        codex_paths=[codex_signals.sessions_dir, codex_signals.shell_snapshots_dir],
        latest_session_files=codex_signals.latest_session_files,
        git_remote_checks=remote_checks,
        open_tab_sources=codex_signals.open_tab_sources,
    )

    now_text = datetime.now().astimezone().strftime("%Y-%m-%d %H:%M:%S %Z")

    workspace_doc = build_workspace_doc(
        now_text=now_text,
        workspace_root=workspace_root,
        scanned_dirs=scan_dirs,
        workstreams=workstreams,
        repo_snapshots=repo_snapshots,
        codex_signals=codex_signals,
        lookups=lookups,
        omitted_dirs=omitted_dirs,
        provenance=provenance,
    )

    global_doc = build_global_doc(
        now_text=now_text,
        workspace_root=workspace_root,
        workstreams=workstreams,
        lookups=lookups,
        omitted_dirs=omitted_dirs,
        provenance=provenance,
    )

    pkm_note_path = pkm_inbox / f"{datetime.now().astimezone().strftime('%Y-%m-%d')} Current Work Snapshot - {workspace_root.name}.md"
    pkm_doc = build_pkm_note(now_text, workspace_root, workstreams, workspace_out, global_out, lookups, provenance)

    write_text(workspace_out, workspace_doc)
    write_text(global_out, global_doc)
    write_text(pkm_note_path, pkm_doc)

    if project_out:
        write_text(project_out, workspace_doc)

    if run_dir:
        run_dir.mkdir(parents=True, exist_ok=True)
        write_text(run_dir / "workspace-current-work.md", workspace_doc)
        write_text(run_dir / "global-current-work.md", global_doc)
        write_text(run_dir / "pkm-note.md", pkm_doc)
        metadata = {
            "generated_at": now_text,
            "workspace_root": str(workspace_root),
            "scan_dirs": [str(p) for p in scan_dirs],
            "omitted_dirs": [str(p) for p in omitted_dirs],
            "workspace_out": str(workspace_out),
            "global_out": str(global_out),
            "project_out": str(project_out) if project_out else "",
            "pkm_note": str(pkm_note_path),
            "workstreams": [
                {
                    "title": ws.title,
                    "summary": ws.summary,
                    "priority_reason": ws.priority_reason,
                    "eta_minutes": ws.eta_minutes,
                    "confidence": ws.confidence,
                    "next_step_label": ws.next_step_label,
                    "signal_notes": ws.signal_notes,
                    "paths": ws.paths,
                    "tables": ws.tables,
                }
                for ws in workstreams
            ],
            "open_tabs_detected": codex_signals.open_tabs,
            "session_files_used": [str(p) for p in codex_signals.latest_session_files],
            "remote_checks": remote_checks,
            "open_tab_sources": codex_signals.open_tab_sources,
            "log_window_hours": codex_signals.log_window_hours,
            "recent_commands": [
                {
                    "timestamp": sig.timestamp,
                    "cmd": sig.cmd,
                    "workdir": sig.workdir,
                }
                for sig in codex_signals.recent_commands[:60]
            ],
            "recent_messages": [
                {
                    "timestamp": sig.timestamp,
                    "role": sig.role,
                    "text": sig.text,
                }
                for sig in codex_signals.recent_messages[:80]
            ],
            "command_paths": codex_signals.command_paths[:120],
            "message_paths": codex_signals.message_paths[:120],
            "provenance": {
                "skill_name": provenance.skill_name,
                "skill_path": provenance.skill_path,
                "automation_name": provenance.automation_name,
                "automation_id": provenance.automation_id,
            },
        }
        write_text(run_dir / "metadata.json", json.dumps(metadata, indent=2) + "\n")

    print(f"Wrote workspace doc: {workspace_out}")
    print(f"Wrote global doc: {global_out}")
    print(f"Wrote PKM note: {pkm_note_path}")
    if project_out:
        print(f"Wrote project copy: {project_out}")
    if run_dir:
        print(f"Saved run artifacts: {run_dir}")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
