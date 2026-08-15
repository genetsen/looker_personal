# FPD Publisher and Shortcut Current State

This is the shared handoff for Codex, Claude Code, and human maintainers working on the FPD publisher or shortcut-aware loader. Read it before changing the template, Apps Script library, shortcut destination, or loader folder configuration.

Last updated: August 14, 2026

## Current Source of Truth

| Surface | Current state | Evidence or entrypoint |
|---|---|---|
| Live publisher | Uses Apps Script library version 11 with Shared Drive shortcut support, unpublished-image removal, and a shortcut-folder action | [In-use template](https://docs.google.com/spreadsheets/d/1BYqrQrjL4_rf5-LKTlGkR94CkqSW6QOsYzAPLHxucfY/edit) |
| Workbook wrapper | Uses `FPDLib` version 11 and passes the active spreadsheet into the library | [Bound stub](apps_script/current_workbook_bound_stub/Stub.gs) and its manifest |
| Active library source | Repository copy corresponding to deployed version 11 | [Version 11 library](apps_script/publish_fpd_template_fpdLib__v11/Library.gs) |
| Shortcut destination | The Analytics `First_Party_Data` folder used for ingestion shortcuts | [Canonical shortcut folder](https://drive.google.com/drive/folders/1pqQVdROIhOkfuBLwexH00uW4eiqkb0GY) |
| Shortcut ingestion | The main R loader scans the same canonical shortcut folder | [Shortcut-aware loader](/Users/eugenetsenter/Looker_clonedRepo/looker_personal/FPD/FPD_loader/util_collect_fpd_shortcutsFolder.r) |

## Version Naming Rule

Use **version 11** when referring to the active publisher. Older `V4:` comments inside the library describe feature-generation history; they are not the current deployed Apps Script version. Historical v3 and old-account source folders remain reference-only.

When a later Apps Script version is published, update this file, the bound manifest, the active source directory/header, and the linked template guide together. Do not infer the live version from an old folder name or internal feature comment.

## Verification Boundary

- Live checked: version 11 was published and the bound project was pulled back with dependency version 11. The user verified the fresh Publish flow: unpublished warnings remain on the source, are absent from the published Sheet, and the shortcut action opens the canonical shortcut folder. Over-cell image labels are not exposed by the connection and therefore remain user-confirmed rather than independently read.
- Live checked earlier: the repaired Drive request created the missing `FTIME | AUG 26` shortcut in the canonical folder.
- Not rerun: the spreadsheet's full custom-menu `Publish` action, because browser control was explicitly excluded. The focused shortcut operation that previously failed is verified live.
- Not rerun in this handoff: the production FPD loader or its BigQuery upload.
