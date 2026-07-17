# Formula-Guarded Manual Edits: Quick Dev Handoff

## Purpose and Current Status

This handoff is for the next developer or agent resuming the Formula-Guarded Manual Edits redesign. It preserves the approved user-facing design, explains why Quick Dev stopped, and gives the exact safe resume sequence.

> **Status:** Paused before BMAD planning on 2026-07-17. No Quick Dev specification or implementation was created. No live Google Sheet, BigQuery object, loader, model, permission, or scheduled workflow was changed.

## Intended User Outcome

Keep Google Sheets as the Manual Data Editor while removing hidden comparison columns, Sheet validation rules, retained-row authority, and refresh-driven overwrites.

Users edit calculated final values directly, add manual-only packages on a separate tab, and submit all pending work with one action. Every submitted cell becomes permanent history with its prior value, new value, submitting user, and timestamp. The latest active history event supplies the applicable `man_*` value.

## Approved Workbook Design

| Visible tab | Approved responsibility |
|---|---|
| **Manual Data Editor** | Correct existing evidence-backed packages by replacing calculated formulas |
| **Add Manual Packages** | Enter packages that do not exist in any source system |
| **Evidence Data** | Show the read-only evidence fields used by Sheet calculations |
| **Change History** | Show read-only permanent submission history and application status |

```text
Evidence Data ──────────────→ Manual Data Editor
                                      ↓
Add Manual Packages ────────→ Submit Everything
                                      ↓
                            Permanent Change History
                                      ↓
                              Refresh master model
                                      ↓
                           Safely reload all Sheet tabs
```

## Approved Behavior Decisions

| Decision | Approved behavior |
|---|---|
| Change detection | A refreshed editable cell contains an expected formula; typing, clearing, deleting, or altering that formula creates unsaved work |
| Batch scope | **Submit Everything** includes all pending corrections and new-package drafts |
| Confirmation | Submit immediately; do not show a preview or second confirmation |
| Validation | Validate without writing; one invalid item rejects the entire batch |
| Rejection message | Name the tab, Package Friendly Name, exact cell, entered value, plain-English reason, and required fix |
| Batch ownership | The signed-in person who clicks Submit owns the entire batch |
| Multiple edits | Multiple fields and package rows may be submitted together |
| Reset | Clearing a correction and submitting removes the active override while preserving history |
| Correction lifetime | Remains active until an explicit reset |
| Planned metrics | Apply across the full package flight |
| Delivered metrics | Apply only across the entered delivery correction dates |
| Metadata | Applies across every date for the package |
| Manual-only ID | System-generated stable internal ID; users do not invent a source-system ID |
| Manual-only removal | **Deactivate Manual Package** preserves history but removes the package from active results |
| Refresh | Block any refresh that could overwrite unsaved work |
| Refresh failure after save | Preserve saved events and show **Edits saved; refresh failed** |

## Row Ownership Contract

There is no separate exclusions list.

| Evidence | May create an active Editor row? |
|---|---|
| Current Evidence Data | Yes, as a normal package |
| Active audited manual-only creation event | Yes, as a manual-only package |
| Previous Sheet contents | No |
| Old correction for a package missing from current evidence | No |
| Change History by itself | No |
| Deactivated manual-only record | No |

The Sheet is never a row authority. Old corrections remain auditable without recreating missing normal packages.

## Six Required Regression Contracts

| Audited risk | Required redesign proof |
|---|---|
| False red incomplete-row styling | Blank flight dates never determine row origin or create warning styling |
| Raw trafficking strings used as friendly names | Missing friendly names display **Friendly name unavailable**; raw package names stay separately labeled |
| Planned metrics tied to delivery dates | Planned corrections use the full flight; delivery dates affect delivered metrics only |
| Removed packages return | Only current evidence or an active manual-only creation event may create a row |
| Refresh overwrites active work | Unsafe edits block refresh; history is saved before any post-submit refresh |
| Two competing row definitions | Evidence owns normal rows; audited creation events own manual-only rows; retained Sheet data owns nothing |

## Permanent History Contract

The proposed `manual_edits` history is append-only and records one event per changed cell. Required evidence includes:

- submission ID and event sequence;
- stable package key and Package Friendly Name;
- normal or manual-only row origin;
- field and action: replace, reset, create, or deactivate;
- previous and submitted values;
- effective dates when applicable;
- submitted user and timestamp;
- application status and status timestamp; and
- plain-English failure reason when applicable.

Current `man_*` state is derived from the newest ordered event for each package, field, and effective scope. A latest reset or deactivation event prevents an older value from remaining active. Earlier history is never overwritten or deleted to calculate current state.

## Authoritative Design Inputs

- [Approved redesign candidate](/Users/eugenetsenter/Looker_clonedRepo/looker_personal/master_data_model/manual_package_edits/redesign_candidates/formula_guarded_manual_edits/README.md)
- [Manual Editor Sheet risk investigation](/Users/eugenetsenter/Looker_clonedRepo/looker_personal/master_data_model/_bmad-output/implementation-artifacts/investigations/manual-editor-sheet-design-risks-investigation.md)
- Candidate documentation commit: `7b2ba07`

## Why Quick Dev Stopped

The required Git sanity check found overlapping uncommitted Manual Editor work on branch `dev`. Quick Dev stopped before creating a specification or reading these diffs deeply because their ownership and intended scope were not established.

| Worktree path at pause | State | Handoff treatment |
|---|---|---|
| `model/manual_editor/load_manual_package_edits.R` | Modified | Not inspected or changed by this Quick Dev run |
| `model/manual_editor/repair_manual_package_editor_conditional_formatting.mjs` | Modified | Not inspected or changed by this Quick Dev run |
| `model/manual_editor/manual_editor_contracts.R` | Untracked | Not inspected or changed by this Quick Dev run |
| `model/manual_editor/tests/test_manual_editor_contracts.R` | Untracked | Not inspected or changed by this Quick Dev run |
| Manual Editor risk investigation | Untracked | Read as design evidence; not changed by this Quick Dev run |
| MFT documentation files | Modified | Unrelated; exclude from Manual Editor commits |

## Safe Resume Sequence

1. Run `git status --short` and identify the owner and purpose of every overlapping Manual Editor file.
2. Review or commit that work as its own coherent change, or have its owner remove it safely. Do not discard unknown work.
3. Confirm the Manual Editor worktree is clean, or that remaining dirty files are unrelated and explicitly excluded.
4. Re-run `bmad-quick-dev` using the approved redesign candidate as the starting intent.
5. Create the development specification at the BMAD implementation-artifact location using the slug `formula-guarded-manual-edits` unless an active spec already exists.
6. Resolve every open builder decision during investigation; the final specification may not contain placeholders or TBDs.
7. Implement and verify only against isolated test assets until the specification's live-change checkpoint is explicitly approved.

## Open Builder Decisions

The next planning run must resolve these with repository and live-system evidence:

- exact editable field inventory and formula contract;
- generated manual-package ID format and collision protection;
- linking and retiring a manual-only package if it later appears in a source system;
- retry owner and user notification after a successful history write but failed model refresh; and
- exact production model version, dependent refreshes, and deployment sequence.

## Required Completion Proof

- Existing-package changes work for one cell, multiple fields, and multiple rows.
- New manual-only packages receive stable generated IDs.
- Reset and deactivation remove active effects without deleting history.
- One invalid item rejects a mixed batch with an exact user-facing reason.
- Unsafe refresh attempts change nothing and preserve all typed values.
- History-write failure leaves the Sheet unchanged and refresh blocked.
- Model-refresh failure preserves saved events for retry without duplication.
- Sheet calculations and model calculations match for each supported field and date scope.
- All six audited risks pass end to end.
- The rendered four-tab Google Sheet and error messages are visually verified before production use.
