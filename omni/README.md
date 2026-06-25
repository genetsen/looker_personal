# Omni Operations

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
| Production model | Validated | No blocking model errors after merge |
| Administrator bypass | Production-tested | A confirmed administrator returned all 12 advertiser short names |

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

## Follow-Up Checks

1. Open the current Apollo, Ritual, and Olipop dashboards in the browser during
   normal user activity and watch for unexpected tile errors.
2. Repair the pre-existing Ritual dashboard filter and missing-field issues.
