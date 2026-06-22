# Changelog

## 2026-06-18

- **IMPROVED** - Expanded the Manual Data Editor product brief into slide-ready presentation notes and added the problems and bottlenecks the workflow resolves.
- **IMPROVED** - Replaced the duplicated Manual Data Editor workflow flowchart with a decision flow showing request boundaries, edit detection, validation, grain handling, and dashboard debugging.
- **ADDED** - Created a BMAD product brief for presenting the Manual Data Editor to a technical team, including an operator workflow diagram and main data model integration diagram.
- **ADDED** - Created a clickable Manual Data Editor workflow map showing the Sheet edit surface, request notification path, loader write path, manual evidence tables, model merge, reporting mart, and troubleshooting loop.
- **IMPROVED** - Added technical-audience notes to the Manual Data Editor workflow map covering write boundaries, comparison inputs, warehouse grain, model merge rules, validation proof, and common dashboard filter traps.
- **IMPROVED** - Simplified the Manual Data Editor workflow map into a presentation-ready five-step flow with technical details moved into expandable notes.
- **IMPROVED** - Added a workflow flowchart and main data model integration map to the Manual Data Editor workflow artifact.

### Next

- Publish the updated interactive maps to the shared location - PENDING

## 2026-06-17

- **FIXED** - Rebuilt the redesign's separate test version so it preserves every current master field and matches spend, impressions, and clicks both overall and by package.
- **IMPROVED** - Reduced the final test table from 258 columns to 217 by removing internal troubleshooting fields and keeping only 20 useful new fields.
- **FIXED** - Corrected project status so the verified test version is clearly separated from unfinished work to match source fields and safely replace the production model.

Related sessions:

- [Redesign test and proof][session-redesign-test-proof]

### Next

- Define who must sign off and what proof they need before replacing the production model - PENDING

### Pending Next Actions

- **Since Jun 17** - Define who must sign off and what proof they need before replacing the production model - BLOCKER
- **Since Jun 16** - Build the redesigned reporting tables described by the plan - RECOMMENDED
- **Since Jun 16** - Finish the interactive workflow for matching each source's fields to the master table
- **Since Jun 16** - Publish the updated interactive maps to the shared location

## 2026-06-16

- **PLANNED** - Turned the redesign idea into a build-ready plan for one flexible master table that supports package, creative, market, and other detail levels without hiding missing information.
- **ADDED** - Created a clickable DCM cost-model map and replaced confusing risk language with plain warnings and `join key` wording—the shared value used to connect matching records.
- **ADDED** - Created a four-page redesign report showing the proposed master table, how new data sources would be added, what still needs to be built, and how production could be replaced safely.

Related sessions:

- [Flexible master-table design][session-flexible-master-table]
- [DCM cost-model map][session-dcm-cost-model-map]
- [Redesign report][session-redesign-report]

### Next

- Complete a verified test version alongside production - DONE
- Reduce the final table to only useful new fields - DONE
- Finish the interactive workflow for matching each source's fields to the master table - PENDING
- Build the redesigned reporting tables described by the plan - PENDING
- Publish the updated interactive maps to the shared location - PENDING

## 2026-06-08

- **ADDED** - Amazon Ads is now included in master reporting, with Amazon's media cost counted as spend while sales remain revenue.

Related sessions:

- [Amazon Ads reporting][session-amazon-ads-reporting]

[session-redesign-test-proof]: /Users/eugenetsenter/.codex/sessions/2026/06/16/rollout-2026-06-16T13-22-25-019ed174-adb3-7001-93cb-3fb1add59980.jsonl
[session-flexible-master-table]: /Users/eugenetsenter/.codex/sessions/2026/06/16/rollout-2026-06-16T13-22-25-019ed174-adb3-7001-93cb-3fb1add59980.jsonl
[session-dcm-cost-model-map]: /Users/eugenetsenter/.codex/sessions/2026/06/16/rollout-2026-06-16T13-58-30-019ed195-b67a-7d42-8fdb-50738d95734f.jsonl
[session-redesign-report]: /Users/eugenetsenter/.codex/sessions/2026/06/16/rollout-2026-06-16T14-26-49-019ed1af-a24f-7992-8b7c-c4272fef4124.jsonl
[session-amazon-ads-reporting]: /Users/eugenetsenter/.codex/sessions/2026/06/08/rollout-2026-06-08T15-51-10-019ea8c9-fb8a-7a60-aa11-28c4eca53a6f.jsonl
