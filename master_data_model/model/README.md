# Master Data Model Workspace

This is the simplified working surface for the current master data model. It is organized by function and source branch, not by file type.

The legacy project root is still present for compatibility while this workspace becomes the clean entrypoint. Active automation paths are not changed yet.

## Start Here

| Area | Use it for |
|---|---|
| [final_model](./final_model/) | Current clustered, creative-capable final model. |
| [stable_base](./stable_base/) | Stable package/date base that the current final model reads from. |
| [manual_editor](./manual_editor/) | Manual Package Editor workflow: Sheet loader, table schema, QA, Apps Script, and repair tools. |
| [mappings](./mappings/) | Advertiser/client/source mapping logic. |
| [branches](./branches/) | Source-specific model branches such as DCM, FPD, social, Amazon, TV, and Prisma. |
| [rollups_and_fields](./rollups_and_fields/) | Canonical fields, rollups, final metrics, and do-not-sum context. |
| [reporting_outputs](./reporting_outputs/) | Reporting mart and dashboard-facing output logic. |
| [reference_maps](./reference_maps/) | HTML maps that are still useful and should stay updated. |
| [automation](./automation/) | Symlinks and notes for scheduler/runner connections. |
| [archive_candidates](./archive_candidates/) | Deprecated SQL and stale docs kept out of the active working path. |

## Current Truth

The current final model path is the clustered v3 table with creative/detail support. The supporting base model still matters because v3 reads from it, but day-to-day orientation should start in [final_model](./final_model/), not the old pile of root-level SQL files.

The HTML maps are keepers. Update [reference_maps](./reference_maps/) when model shape, source branches, runner paths, or manual editor behavior changes.
