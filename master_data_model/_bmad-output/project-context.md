---
project_name: 'master_data_model'
user_name: 'Gene'
date: '2026-06-15'
sections_completed: ['discovery_initialized', 'technology_stack', 'language_specific_rules', 'framework_specific_rules', 'testing_rules', 'code_quality_style_rules', 'development_workflow_rules', 'critical_dont_miss_rules']
existing_patterns_found: 6
status: 'complete'
rule_count: 38
optimized_for_llm: true
---

# Project Context for AI Agents

_This file contains critical rules and patterns that AI agents must follow when implementing code in this project. Focus on unobvious details that agents might otherwise miss._

---

## Technology Stack & Versions

- BigQuery Standard SQL is the primary modeling layer. SQL files create or replace warehouse views and tables, so agents must treat deploy SQL as live-system work, not local-only code.
- R is used for data loading and Google Sheet / BigQuery workflows, especially the Manual Package Editor loader. R package versions are not pinned in this repo; verify the active runtime before making package-specific assumptions.
- Node.js / JavaScript is used for Google Sheet UX setup, formatting repair, slicer/filter repair, and bound Apps Script notification behavior. No local `package.json` was found, so agents must not assume a Node dependency install step exists.
- Google Sheets is part of the production workflow surface, not just documentation. Sheet formatting, slicers, protected columns, hidden helper columns, and request controls can be user-facing behavior.
- BMad config is installed locally at version 6.8.0 and writes planning output under `_bmad-output`.
- Project docs and maps live in Markdown and HTML. Durable behavior changes should update the stable docs that explain model shape, source lineage, refresh dependencies, precedence rules, and modeling risks.

## Critical Implementation Rules

### Language-Specific Rules

- BigQuery SQL must preserve model grain intentionally. Do not add lower-grain fields such as creative, ad, line item, or placement detail to package/date views by silently aggregating with `STRING_AGG`, `ARRAY_AGG`, first-value selection, or similar shortcuts.
- BigQuery SQL aliases must avoid fragile names such as `rows`; use descriptive aliases like `row_count`, `record_count`, or `source_row_count`.
- For missing or blank fields, trace lineage through source table, CTEs, joins, aggregations, and final projection before answering. Schema checks and fill rates are supporting evidence only.
- R loader logic should keep data loading separate from Sheet design. `load_manual_package_edits.R` may refresh values and backend tables, but formatting rebuild behavior belongs to the JavaScript setup/repair scripts.
- R manual-edit comparisons must use source-derived baselines, not manual-affected final dashboard fields, so stale manual values do not re-mark themselves as active edits.
- JavaScript Sheet scripts are live UX tools. Formatting or setup scripts must preserve user-made Sheet formatting unless the user explicitly approves a full rebuild.
- Apps Script request controls are notification-only unless explicitly changed. Do not imply that checking `Request refresh` validates rows, runs the loader, or writes to BigQuery.

### Framework-Specific Rules

- BigQuery views are the production modeling surface. Inspect the live warehouse object before relying on local SQL when the task concerns current model behavior.
- `master_stg.data_model` is the evidence layer; `master_stg.data_model_mart` is the reporting-ready layer. Do not treat mart filters or rollups as proof of raw source behavior.
- The Manual Package Editor is a Sheet-backed workflow. Preserve the distinction between visible editable cells, hidden baseline columns, hidden manual-marker columns, raw manual rows, daily manual rows, model fields, and mart fields.
- Google Sheet slicers and filters are native user-facing controls. Do not replace them with Apps Script-driven behavior unless explicitly requested.
- The request-refresh control is a notification path only. Loader execution, validation, warehouse writes, and dashboard reflection are separate steps.

### Testing Rules

- Use the Manual Package Editor R tests for local regression coverage: `manual_package_edits/tests/test_loader_choice_logic.R` protects edit/baseline choice logic, and `manual_package_edits/tests/test_daily_total_proof.R` protects daily allocation totals.
- For R loader changes, a parse check or local test run is only local proof. Do not claim the live workflow works unless the relevant Sheet values, raw manual table, daily manual table, master model, and mart behavior are verified.
- For SQL model changes, use dry runs and read-only QA queries before deployment, then verify the live object after any approved deploy.
- For Google Sheet UX or formatting claims, use rendered visual evidence when the claim is about what the user sees, such as slicer order, marker colors, hidden columns, spacing, or filter layout.
- Keep fluctuating row counts, source mix counts, package counts, and media totals in run-specific handoffs rather than durable docs.

### Code Quality & Style Rules

- Follow the global file-header guidance in `/Users/eugenetsenter/.codex/CODE_STYLE.md`; new internal files should briefly explain purpose, inputs, outputs or side effects, and safe usage.
- Prefer descriptive names that reveal domain, grain, source, and purpose, such as `data_model_delivery_detail_v2`, `manual_package_edits`, `row_data_issue_category`, and `man_total_spend_doNotSum`.
- Documentation should use scan-friendly tables for contracts, field mappings, validation checks, workflow responsibilities, and implementation status.
- Documentation links should use human-readable labels with the full path or URL hidden behind the link unless the exact raw value is needed for a command or config.
- Avoid durable docs churn for live counts that naturally fluctuate. Keep changing row counts and source totals in run-specific proof notes instead.

### Development Workflow Rules

- Treat local SQL edits and warehouse deploys as separate steps. Editing a SQL file does not change BigQuery until the deploy command runs, and running deploy SQL can replace live objects.
- For BigQuery-backed model work, start from the live warehouse object when the question is about current behavior. Use local SQL after live inspection, when comparing drift, or when the user explicitly names the file.
- Keep shared warehouse datasets tidy. QA/test implementation artifacts need clear names, current BigQuery descriptions, and an obvious cleanup status or owner before handoff.
- For versioned work such as `v2`, create sibling versioned files and BigQuery objects unless the user explicitly asks to replace the unversioned production object.
- Manual Package Editor routine refreshes should use the loader path. Do not run formatting rebuild/setup scripts against the live Sheet unless the user explicitly asks for a full formatting rebuild.
- When deployed model behavior changes, update the stable repo docs that explain field semantics, source lineage, refresh dependencies, precedence rules, and modeling risks.

### Critical Don't-Miss Rules

- Do not silently drop, rename away, aggregate away, or hide upstream source columns in downstream models. If a source field cannot fit the current grain or schema, state the tradeoff and get explicit approval.
- Do not let unmatched DCM/FPD delivery rows populate final metrics unless explicitly approved. They may preserve raw evidence fields and issue labels, but should not inflate package actual rollups by default.
- Do not rebuild live Manual Package Editor formatting during routine loader, QA, or troubleshooting work. User-made live formatting is the source of truth unless a full rebuild is explicitly requested. When formatting edits are requested, first compare the formatting script's expected layout against the live Sheet; if drift exists, update the script so its pre-edit baseline matches the live Sheet, then add the requested formatting change as a clear delta.
- Do not describe request notifications, dry runs, parse checks, or local tests as end-to-end proof of live dashboard or warehouse behavior.

---

## Usage Guidelines

**For AI Agents:**

- Read this file before implementing code in this project.
- Use this as a lean project-context companion to global and project `AGENTS.md`, not as a replacement for those rules.
- When a rule here conflicts with a more specific live project instruction, stop and resolve the conflict before editing or deploying.
- Update this file only when a durable project pattern changes or a new easy-to-miss implementation rule emerges.

**For Humans:**

- Keep this file focused on agent-useful reminders, not a full runbook.
- Update it when the technology stack, model shape, live workflow, or repeated implementation risks change.
- Remove or compress rules that become obvious or are better owned by `AGENTS.md`, `CODE_STYLE.md`, `CHANGELOG_STYLE.md`, README files, or runbooks.

Last Updated: 2026-06-15
