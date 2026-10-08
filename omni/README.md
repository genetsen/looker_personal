---
project: Omni Operations
source_type: live Omni models and dashboards
output: https://giantspoon.omniapp.co/dashboards/e8319cff
loader_script: none
verified: 2026-10-08
verified_against:
  - live Ritual model YAML
  - published Ritual dashboard and saved QA query
reviewers:
  - gene
---

# Omni Operations

## Table of Contents

- [Client Access Control](#client-access-control)
- [Current State](#current-state)
- [Verification Notes](#verification-notes)
- [Ritual QA Tile](#ritual-qa-tile)
- [Follow-Up Checks](#follow-up-checks)
- [AI Eval Prompt Sets](#ai-eval-prompt-sets)

This folder documents live Omni administration and semantic-model work for the
`giantspoon.omniapp.co` organization. The live Omni platform remains the source
of truth; this folder does not contain a local copy of the model.

## Client Access Control

The access-control design uses the multi-value `client_access` user attribute
to match users to the `Advertiser (Short)` field in the `Master Stg Data Model`
topic.

| Access source | `client_access` value | Meaning |
|---|---|---|
| Ritual folder owner or Ritual group | `RTL` | Ritual advertiser rows |
| Apollo group | `APO` | Apollo advertiser rows |
| Olipop group | `OLI` | Olipop advertiser rows |
| Confirmed organization administrator | `all_clients` | Unrestricted advertiser access |

Users can receive more than one value. For example, a user in Apollo and Ritual
receives both `APO` and `RTL`.

## Current State

| Surface | Status | Verification |
|---|---|---|
| `client_access` definition | Live | Confirmed as a multi-value string attribute |
| User assignments | Live | 26 assigned users matched the generated access map; 2 users intentionally remain empty |
| Group model roles | Live | Apollo, Olipop, and Ritual groups have `QUERY_TOPICS` on the shared model |
| Topic access filter | Live | Published to the shared Master Stg Data Model topic |
| Access-filter validation | Passed in the earlier access-control check | The October 8 full-model check reports existing field errors and stale references to deleted Guard test tables; it is not a clean model-validation result |
| Administrator bypass | Production-tested | A confirmed administrator returned all 12 advertiser short names |
| Master model AI eval prompt set | Live | Created in Omni with 25 prompts and read back by prompt-set ID; payload retained in [Master Stg Data Model eval prompt set](/Users/eugenetsenter/Looker_clonedRepo/looker_personal/omni/eval_prompt_sets/master-stg-data-model-core-qa-regression.json) |

The branch filter is:

```yaml
access_filters:
  - field: master_stg__data_model._advertiser_short_name
    user_attribute: client_access
    values_for_unfiltered:
      - all_clients
```

## Verification Notes

- Apollo-only access returned `APO`.
- Olipop-only access returned `OLI`.
- Ritual-only access returned `RTL`.
- Apollo plus Olipop access returned `APO` and `OLI`.
- Apollo plus Ritual access returned `APO` and `RTL`.
- The administrator bypass returned all 12 current advertiser short names.
- A user without `client_access` received `403 Permission denied`, confirming
  that production fails closed.
- A saved Ritual dashboard tile ran successfully as a Ritual end user.
- The saved Apollo and Olipop workbook models reject API impersonation because
  they contain workbook-level topic changes. Their equivalent saved tile
  fields, filters, sorts, and topics ran successfully through the shared
  production model as representative Apollo and Olipop users.
- The branch content validator found pre-existing Ritual dashboard issues: an
  invalid dashboard filter and a missing `d_video_comps_sum` field. The access
  filter did not introduce those issues.

## Ritual QA Tile

The [Ritual dashboard](https://giantspoon.omniapp.co/dashboards/e8319cff)
QA tile uses **Data Issue Category** (`row_data_issue_category`) to group
current data-quality issues. Its source is `qa_data_issues` in the
[live V3 model](https://console.cloud.google.com/bigquery?project=looker-studio-pro-452620&ws=!1m5!1m4!4m3!1slooker-studio-pro-452620!2smaster_stg!3sdata_model_v3),
renamed by the existing Ritual SQL projection. Values may contain several
issue categories joined by ` | `; `no_issues` means none of those checks flagged
the row. These are current issue categories, not a reconstruction of historical
`row_data_callouts` text.

| Surface | Responsibility | Canonical name |
|---|---|---|
| Warehouse V3 model | Produces the issue categories | `qa_data_issues` |
| Shared Ritual query view | Exposes the existing SQL alias as a dimension | `master_stg/ritual_data_model.query.view`, `row_data_issue_category` |
| Ritual workbook | Supplies its saved measures, filters and calculations | Live dashboard workbook model |
| QA tile | Groups issues with package delivery and planning measures | Saved tile key `20`, QA Data Callouts |

Use live Omni definitions for changes; this folder is documentation, not a
model deployment source. Keep warehouse source SQL and other dashboard tiles
outside a tile-only repair. Read model files using their complete stored paths,
then validate edits on a branch and test queries through the actual workbook.
A successful YAML write alone does not prove a field became available.

To verify the saved tile definition without opening a browser:

```sh
omni documents get-queries e8319cff
```

Expected: query key `20` uses `row_data_issue_category` in its fields and column
totals and no longer refers to `row_data_callouts`. Execute that returned query
through `omni query run` with its returned workbook model and existing filters;
expect a successful response with the Data Issue Category column. Row counts
change with the data and are not a fixed acceptance target.

The published query and saved dashboard configuration were checked through the
API. Browser rendering was not checked. The full shared-model validator still
reports unrelated field errors, a Ritual `amzn_total_cost` error, and stale
references to deleted Guard test tables. Those require separate investigation.
See the [October 8 verification record](/Users/eugenetsenter/Looker_clonedRepo/looker_personal/omni/verification/2026-10-08-ritual-qa-repair.md)
for the exact checks and their limits.

## Follow-Up Checks

1. Open the current Apollo, Ritual, and Olipop dashboards in the browser during
   normal user activity and watch for unexpected tile errors.
2. Repair the pre-existing Ritual dashboard filter and missing-field issues.

## AI Eval Prompt Sets

Omni AI eval prompt sets are live platform assets used to test whether Omni's
AI chooses the right topic, fields, filters, and aggregation behavior for common
business questions. The local files in [eval prompt sets](/Users/eugenetsenter/Looker_clonedRepo/looker_personal/omni/eval_prompt_sets/) are reviewable request payloads only; the live Omni platform is still the source of truth after creation.

| Prompt set | Purpose | Local payload |
|---|---|---|
| Master Stg Data Model - Core QA Regression | Live prompt set with 25 prompts. Tests the shared master model's standard and QA topics plus Ritual and Apollo topic routing for source reconciliation, unmatched packages, missing delivery, pacing issues, manual overrides, QTD reach, and v3 metric-grain behavior. | [Payload](/Users/eugenetsenter/Looker_clonedRepo/looker_personal/omni/eval_prompt_sets/master-stg-data-model-core-qa-regression.json) |
