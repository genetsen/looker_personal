---
name: project-status-intel
description: Build a practical status update for the current workspace by combining recent agent transcript activity, git status/history, GitHub PR and commit signals, local file edits, and recent BigQuery job logs. Use when the user asks what they are working on, what changed recently, what they did, and what they should do next.
---

# Project Status Intel Skill

## What this skill does

This skill creates one status report that answers:

1. What you are currently working on.
2. What you recently did.
3. What you should do next (in priority order).

It does this by reading and combining:

- Local agent transcript signals
- Local git signals (status, recent commits, changed files)
- GitHub signals via `gh` (when available)
- Recent local file modification signals
- BigQuery job logs (last N hours, when `bq` is available)

## Run command

```bash
python3 .cursor/skills/project-status-intel/scripts/generate_status_update.py \
  --workspace-root /Users/eugenetsenter/Looker_clonedRepo/looker_personal \
  --hours 48 \
  --max-transcripts 8 \
  --out /Users/eugenetsenter/Looker_clonedRepo/looker_personal/.cursor/skills/project-status-intel/output/status-update.md
```

## Notes

- The script is intentionally linear and heavily commented to make QA simple.
- If `gh` is not authenticated or not installed, GitHub sections fall back to local git only.
- If `bq` is not authenticated or not installed, BigQuery sections are marked as unavailable.
- This skill never changes production systems. It only reads signals and writes a local markdown report.
