# Omni Changelog

This file records meaningful changes to Omni administration, access control,
semantic models, and user-facing Omni workflows.

## 2026-06-30

- **ADDED** - Created the Master Stg Data Model AI eval prompt set in Omni with 25 prompts covering standard performance questions, Ritual and Apollo topic routing, and QA checks for unmatched packages, missing delivery, pacing issues, source reconciliation, manual overrides, QTD reach, and v3 metric-grain behavior.

### Pending Next Actions

- **Since Jun 24** - Observe the current Apollo, Ritual, and Olipop dashboards during normal end-user use
- **Since Jun 24** - Repair the pre-existing Ritual dashboard filter and missing-field issues

## 2026-06-24

- **CHANGED** - Synchronized `client_access` for 26 active users from Ritual folder access, Apollo/Olipop/Ritual group membership, and confirmed organization-administrator status. Two users without a mapped access source intentionally remain unassigned.
- **CHANGED** - Granted the Ritual and Olipop groups the same `QUERY_TOPICS` model role already used by Apollo. Branch tests confirmed that representative Ritual and Olipop users now return only `RTL` and `OLI`, while an unassigned user remains blocked.
- **ADDED** - Published the validated access filter to the shared Master Stg Data Model topic. Production tests confirmed Ritual, Olipop, Apollo, both multi-client combinations, administrator bypass, and unassigned fail-closed behavior.
- **VERIFIED** - The current Ritual dashboard's saved tile ran as a Ritual user. Apollo and Olipop saved workbook impersonation is blocked by an Omni workbook-topic restriction, but their equivalent saved tile query paths succeeded through the shared production model for representative end users.
- **DOCUMENTED** - Added the operating map, verification evidence, role boundary, and safe publish sequence to the Omni workspace README. Earlier repository entries about Apollo and Ritual describe warehouse-model changes, not prior Omni access-control changes, so they were reviewed but not copied here.

### Pending Next Actions

- **Since Jun 24** - Observe the current Apollo, Ritual, and Olipop dashboards during normal end-user use
- **Since Jun 24** - Repair the pre-existing Ritual dashboard filter and missing-field issues

Related sessions:

- [Omni client access-filter setup](/Users/eugenetsenter/.codex/sessions/2026/06/24/rollout-2026-06-24T18-02-21-019efba7-d683-7290-b86d-c90b5d9dc625.jsonl)
