# Project Status Intel V1 Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build one canonical, cross-agent personal skill that generates project-aware status updates with safety defaults, feedback loop, diff-from-last-report mode, and explicit signal-overload warnings.

**Architecture:** Keep one canonical Python script and config under a personal skill folder, then add tiny wrappers for Cursor/Codex/Claude so each agent calls the same command. Keep the script mostly linear with lightweight helpers for readability and QA.

**Tech Stack:** Python 3, git CLI, gh CLI (optional), bq CLI (optional), YAML config (PyYAML).

---

### Task 1: Create canonical personal skill scaffold

**Files:**
- Create: `/Users/eugenetsenter/.cursor/skills/project-status-intel/SKILL.md`
- Create: `/Users/eugenetsenter/.cursor/skills/project-status-intel/projects.yaml`
- Create: `/Users/eugenetsenter/.cursor/skills/project-status-intel/scripts/generate_status_update.py`
- Create: `/Users/eugenetsenter/.cursor/skills/project-status-intel/output/.gitkeep`
- Create: `/Users/eugenetsenter/.cursor/skills/project-status-intel/state/.gitkeep`

- [ ] **Step 1: Write canonical SKILL instructions**
- [ ] **Step 2: Add multi-project config template**
- [ ] **Step 3: Implement report generator script**
- [ ] **Step 4: Ensure script handles unavailable `gh`/`bq` gracefully**

### Task 2: Implement v1 safety defaults

**Files:**
- Modify: `/Users/eugenetsenter/.cursor/skills/project-status-intel/scripts/generate_status_update.py`

- [ ] **Step 1: Add source availability table output**
- [ ] **Step 2: Add blocker-first ranking logic**
- [ ] **Step 3: Add PII-lite redaction for report evidence snippets**
- [ ] **Step 4: Add project profile validation command mode**

### Task 3: Implement feedback loop and diff mode

**Files:**
- Modify: `/Users/eugenetsenter/.cursor/skills/project-status-intel/scripts/generate_status_update.py`
- Create: `/Users/eugenetsenter/.cursor/skills/project-status-intel/state/feedback.jsonl`

- [ ] **Step 1: Save report metadata with stable report_id**
- [ ] **Step 2: Add `--record-feedback` mode (`rating + free-text`)**
- [ ] **Step 3: Add diff-from-last-report section**
- [ ] **Step 4: Apply feedback hints to next-step ranking**

### Task 4: Implement explicit signal-overload detection

**Files:**
- Modify: `/Users/eugenetsenter/.cursor/skills/project-status-intel/scripts/generate_status_update.py`

- [ ] **Step 1: Compute per-source signal counts**
- [ ] **Step 2: Detect overload conditions with clear thresholds**
- [ ] **Step 3: Add explicit “Signal Overload” section**
- [ ] **Step 4: Down-weight noisy sources when overloaded**

### Task 5: Add cross-agent wrappers

**Files:**
- Create: `/Users/eugenetsenter/.codex/skills/project-status-intel/SKILL.md`
- Create: `/Users/eugenetsenter/.claude/skills/project-status-intel/SKILL.md`

- [ ] **Step 1: Add Codex wrapper that points to canonical script**
- [ ] **Step 2: Add Claude wrapper that points to canonical script**
- [ ] **Step 3: Keep wrappers minimal to avoid drift**

### Task 6: Validate with a real run

**Files:**
- Generate: `/Users/eugenetsenter/.cursor/skills/project-status-intel/output/status-update.md`
- Generate: `/Users/eugenetsenter/.cursor/skills/project-status-intel/output/status-update.metadata.json`

- [ ] **Step 1: Run generator for `looker_personal` profile**
- [ ] **Step 2: Confirm required report sections exist**
- [ ] **Step 3: Record one sample feedback entry**
- [ ] **Step 4: Re-run and verify feedback + diff are visible**
