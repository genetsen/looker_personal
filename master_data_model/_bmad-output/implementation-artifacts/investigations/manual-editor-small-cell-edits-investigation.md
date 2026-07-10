# Investigation: Manual Editor Small-Cell Edit Reconstruction

## Hand-off Brief

1. **What happened.** User reports that human Google Sheets edits were overwritten by later refreshes; prior state diffs showed missing manual rows, but not the small edit events that created them.
2. **Where the case stands.** Active; confirmed that the archived editor workbook exposes hidden baseline/manual-marker columns, but the next question is whether Google revision/activity metadata can identify small cell-level user edits separately from full-sheet loader rewrites.
3. **What's needed next.** Query Drive revisions and Drive Activity for the current and archived workbooks, classify event size where possible, then join candidate small edit events back to package friendly names and changed fields.

## Case Info

| Field | Value |
| --- | --- |
| Ticket | N/A |
| Date opened | 2026-07-08 |
| Status | Active |
| System | macOS, Manual Data Editor Google Sheets workflow |
| Evidence sources | Archived Manual Data Editor workbook, current production workbook, Google Drive revision metadata, Google Drive Activity metadata, local CSV exports |

## Problem Statement

The user says overwritten manual edits should be found by looking for Google Sheets changes where only a few cells were edited at a time, rather than whole-sheet refreshes.

## Evidence Inventory

| Source | Status | Notes |
| --- | --- | --- |
| Archived editor workbook values | Available | Read-only export captured visible values, hidden baselines, and manual-marker columns. |
| Current production workbook values | Available | Read-only active-row export captured current state. |
| Google Drive revisions | In progress | Need to determine whether revision export or metadata is granular enough to distinguish small edits from bulk rewrites. |
| Google Drive Activity | In progress | Need to determine whether activity events provide actor/time and usable edit granularity. |

## Investigation Backlog

| # | Path to Explore | Priority | Status | Notes |
| - | --- | --- | --- | --- |
| 1 | Query Drive revision metadata for export links and revision timing | High | In Progress | Current and archived workbooks. |
| 2 | Query Drive Activity for edit events and actors | High | In Progress | Look for event clustering and possible small-edit indicators. |
| 3 | If revisions can be exported, diff adjacent revisions around small edit events | High | Open | This would directly identify edited cells. |
| 4 | Join recovered cell diffs to package friendly name and baseline/manual values | High | Open | Required output shape for user. |

## Timeline of Events

| Time | Event | Source | Confidence |
| --- | --- | --- | --- |
| 2026-07-08 | Archived workbook read-only exports showed hidden manual-marker and baseline columns available for row-state diffs. | Local generated exports in `manual_package_edits/ai_context_summaries/` | Confirmed |
| 2026-07-07T19:49:21.970Z | Archived workbook revision `2574 -> 2576` changed exactly 8 cells in one row for package friendly name `Columbus Circle DOOH`. | `manual_package_edits/ai_context_summaries/2026-07-08-small-cell-edit-events.csv` | Confirmed |

## Confirmed Findings

### Finding 1: Archived workbook contains the fields needed to identify changed cells by state

**Evidence:** `manual_package_edits/ai_context_summaries/2026-07-08-older-editor-visible-baseline-marker-api-raw.csv`

**Detail:** The workbook has paired visible values, hidden `Baseline ...` fields, and hidden `Manual Marker ...` booleans.

### Finding 2: The archived workbook revision history exposes one small-cell edit event

**Evidence:** `manual_package_edits/ai_context_summaries/2026-07-08-archived-all-valid-revision-cell-diff-summary.csv` and `manual_package_edits/ai_context_summaries/2026-07-08-small-cell-edit-events.csv`

**Detail:** All 23 archived workbook revisions exposed by Drive were downloaded and diffed. Only revision pair `2574 -> 2576` classified as a small-cell edit: 8 changed cells in one row for package friendly name `Columbus Circle DOOH`, package ID `ccdooh`.

### Finding 3: Drive Activity was not available through the current credential scope

**Evidence:** Drive Activity API returned `ACCESS_TOKEN_SCOPE_INSUFFICIENT` for both the current production workbook and the archived workbook.

**Detail:** This blocks actor/action activity-event metadata until the credential includes Drive Activity readonly scope. Drive revision metadata and exported workbook revisions were still usable.

## Hypothesized Paths

### Hypothesis 1: Small human edits can be isolated from loader refreshes through Google Drive activity/revision metadata

**Status:** Open

**Theory:** Human edits should appear as isolated edit events by a user, while loader refreshes should appear as broad rewrite events or clustered events from the automation identity.

**Supporting indicators:** The user reports manual edits were made a few cells at a time; loader code rewrites the full editor tab.

**Would confirm:** Drive Activity or revision export reveals event granularity, actor, and/or adjacent revision content that can be cell-diffed.

**Would refute:** Google only exposes coarse workbook-level edit metadata for these revisions, with no exportable prior state or cell/range detail.

**Resolution:** Open.

## Missing Evidence

| Gap | Impact | How to Obtain |
| --- | --- | --- |
| Drive Activity events | Would add a separate actor/action event log over the revision diffs | Refresh credentials with Drive Activity readonly scope |
| Revisions older than 2026-06-26 for the archived workbook and older than 2026-07-07 for the current workbook | Needed to prove few-cell edit events from months before the exposed Drive revision history | Google Workspace audit logs or another retained workbook/revision source outside the visible Drive revisions |

## Conclusion

**Confidence:** Medium

The available Drive revision exports support the user's method: small-cell edit events can be separated from bulk loader refreshes by adjacent revision cell counts. Within the complete exposed archived-workbook revision history, the only small-cell edit event found was `Columbus Circle DOOH` / `ccdooh`, where 8 visible cells were filled in. All other archived revision pairs were bulk/no-change events; Drive Activity remains blocked by OAuth scope, and older-than-visible revision history is still missing evidence.

## Follow-up: 2026-07-09

### New Evidence Inventory

| Source | Status | Notes |
| --- | --- | --- |
| Drive Activity through R/gargle | Blocked | `token_fetch()` returned `NULL`, so the initial R request reached Drive Activity without a bearer token and returned `CREDENTIALS_MISSING`. |
| Drive Activity through `gcloud-giantspoon` ADC | Blocked | The credential is `gene.tsenter@giantspoon.com` and has Drive/Sheets scopes, but not `drive.activity.readonly`; Drive Activity returned `ACCESS_TOKEN_SCOPE_INSUFFICIENT`. |
| Drive workbook search | Available | Found 45 spreadsheet candidates; the manual-editor-name candidates were the archived workbook, current production workbook, an Apollo UTM workbook, and a rollback copy named `Manual Data Editor rollback copy - before manual edit audit - 2026-06-23`. |
| Rollback copy export | Available | Sheets API content read returned 403 for the rollback copy, but Drive export returned a valid XLSX that was parsed locally. |
| Early archived revision diffs | Available | Downloaded and diffed archived revisions `2288`, `2293`, `2301`, `2303`, and `2310`, covering 2026-06-24 through the first 2026-06-26 revision. |

### Confirmed Findings

#### Finding 4: Drive Activity is still blocked by OAuth scope, not by workbook sharing

**Evidence:** `manual_package_edits/ai_context_summaries/2026-07-09-drive-activity-tokeninfo.json` and `manual_package_edits/ai_context_summaries/2026-07-09-current-production-drive-activity-page-01.json`.

**Detail:** The active Drive/Sheets credential for the separate `gcloud-giantspoon` ADC profile identifies as `gene.tsenter@giantspoon.com` and includes Drive and Sheets scopes. The Drive Activity API still returned `ACCESS_TOKEN_SCOPE_INSUFFICIENT`, so event metadata requires re-auth with `https://www.googleapis.com/auth/drive.activity.readonly`.

#### Finding 5: A rollback copy exists for the pre-audit state, but it does not contain the Purely Elizabeth QUAN line

**Evidence:** `manual_package_edits/ai_context_summaries/2026-07-09-drive-manual-editor-workbook-candidates.csv`, `manual_package_edits/ai_context_summaries/2026-07-09-rollback-copy-export.xlsx`, and `manual_package_edits/ai_context_summaries/2026-07-09-rollback-copy-xlsx-ccdooh-columbus-rows.csv`.

**Detail:** Drive metadata shows `Manual Data Editor rollback copy - before manual edit audit - 2026-06-23` owned by `gene.tsenter@giantspoon.com`. Drive export returned a valid XLSX. Local parsing found zero rows matching `ccdooh` or `Columbus Circle DOOH`, so that rollback copy predates the Purely Elizabeth QUAN edit or otherwise does not preserve it.

#### Finding 6: The rollback copy preserves older manual-marker edits for other 2026 rows

**Evidence:** `manual_package_edits/ai_context_summaries/2026-07-09-rollback-copy-manual-field-diffs.csv`.

**Detail:** After excluding social and Amazon Ads rows, the rollback copy contains 116 field-level manual-marker differences for 2026 rows. The CSV records Package Friendly Name first, then package ID, field, visible value, baseline value, and numeric delta where applicable.

#### Finding 7: Newly checked archived revisions before the prior June 26 cutoff were all bulk/refresh changes

**Evidence:** `manual_package_edits/ai_context_summaries/2026-07-09-archived-early-revision-cell-diff-summary.csv` and `manual_package_edits/ai_context_summaries/2026-07-09-archived-early-revision-cell-diff-details.csv`.

**Detail:** Adjacent archived revision pairs `2288 -> 2293`, `2293 -> 2301`, `2301 -> 2303`, and `2303 -> 2310` changed 1,395, 1,811, 2,058, and 2,580 cells respectively. All classify as bulk/refresh changes; none are small-cell edits. No diff detail rows matched `ccdooh`, `Columbus Circle DOOH`, or `PurelyElizabethProteinGranola2025`.

### Updated Missing Evidence

| Gap | Impact | How to Obtain |
| --- | --- | --- |
| Drive Activity event metadata | Would add actor/action event context over the revision diffs | Re-auth the working ADC profile with `drive.activity.readonly` scope, likely through `gcloud auth application-default login --scopes=...` and possibly an OAuth client ID file for non-GCP scopes. |
| Workspace audit logs beyond Drive revisions | Would be the only clear way to recover small human edits before the visible Drive revision/export window if no workbook copy preserves them | Google Workspace Admin audit export or another retained workbook copy/snapshot. |

### Follow-up Conclusion

**Confidence:** Medium-high for the newly checked evidence.

The evidence now says the `ccdooh` / Purely Elizabeth QUAN edit was not present in the June 23 rollback copy and was not created in the newly checked June 24-25 archived revision pairs; those early archived pairs were all bulk refreshes. The best preserved pre-June-26 evidence is the rollback copy's manual-marker state for other 2026 non-social/non-Amazon rows, while Drive Activity remains blocked until the credential has the Drive Activity readonly scope.

## Follow-up: 2026-07-09 #2

### New Evidence Inventory

| Source | Status | Notes |
| --- | --- | --- |
| Drive Activity OAuth scope | Available | Re-auth succeeded with the Giant Spoon installed OAuth client `346785476296-...apps.googleusercontent.com`; tokeninfo showed `drive.activity.readonly`, Drive, Sheets, and cloud-platform scopes for `gene.tsenter@giantspoon.com`. |
| Drive Activity quota-project behavior | Available | Forcing `X-Goog-User-Project: looker-studio-pro-452620` returned `SERVICE_DISABLED`; removing that forced header allowed Drive Activity to run successfully. |
| Drive Activity current workbook events | Available | Read-only query returned 95 activity rows for the current production workbook. |
| Drive Activity archived workbook events | Available | Read-only query returned 247 activity rows for the archived workbook, including events near the known small-cell edit. |

### Confirmed Findings

#### Finding 8: Drive Activity metadata is now available

**Evidence:** `manual_package_edits/ai_context_summaries/2026-07-09-drive-activity-tokeninfo.json`, `manual_package_edits/ai_context_summaries/2026-07-09-drive-activity-summary.csv`, `manual_package_edits/ai_context_summaries/2026-07-09-current-production-drive-activity-combined.json`, and `manual_package_edits/ai_context_summaries/2026-07-09-archived-2026-07-26-drive-activity-combined.json`.

**Detail:** After using the Giant Spoon OAuth client and removing the forced `looker-studio-pro-452620` user-project header, Drive Activity returned HTTP 200 for both workbooks. The summary file contains 342 rows total: 95 for `Manual Data Editor | GS Internal` and 247 for `ARCHIVED 07/26 | Manual Data Editor | GS Internal`.

#### Finding 9: Drive Activity corroborates activity around the `ccdooh` small-cell edit window, but does not provide cell-level detail

**Evidence:** `manual_package_edits/ai_context_summaries/2026-07-09-drive-activity-summary.csv` and `manual_package_edits/ai_context_summaries/2026-07-08-small-cell-edit-events.csv`.

**Detail:** The known cell-level diff was archived revision `2574 -> 2576` at `2026-07-07T19:49:21.970Z`. Drive Activity shows archived workbook edit events at `2026-07-07T19:48:51.859Z` and `2026-07-07T19:51:55.950Z` by `people/112399287567371269516`. The actor ID is consistent with the old Giant Spoon account from archived workbook ownership/revision metadata. Drive Activity still reports workbook-level edit actions, so the actual changed cells remain proven by adjacent revision XLSX diff, not by Drive Activity alone.

### Updated Missing Evidence

| Gap | Impact | How to Obtain |
| --- | --- | --- |
| Human-readable actor names for all Drive Activity `people/...` IDs | Would make the event table easier to read without relying on ID-to-email inference | Use additional directory/people lookup permission or map IDs to Drive revision metadata where timestamps overlap. |
| Cell/range-level edit details from Drive Activity | Drive Activity does not expose the individual cells edited in these events | Continue using adjacent revision exports for cell-level proof. |

### Follow-up Conclusion

**Confidence:** High that Drive Activity now works; medium-high for actor interpretation.

The OAuth blocker is resolved. Drive Activity now confirms workbook-level edit events around the recovered `Columbus Circle DOOH` / `ccdooh` small-cell edit window, and the event actor aligns with the old Giant Spoon account seen in archived revision metadata. The cell-level truth remains the revision diff, because Drive Activity gives timing/action/actor metadata rather than edited cell ranges.

## Follow-up: 2026-07-09 #3

### Correction To Investigation Focus

The `Columbus Circle DOOH` / `ccdooh` / QUAN row is useful proof that narrow human edits can be recovered from adjacent workbook revisions, but it is not the main lost-change population. The lost-change audit should focus on older manually marked rows and fields that disappeared from the current active manual state.

### New Evidence Inventory

| Source | Status | Notes |
| --- | --- | --- |
| Codex session logs | Available | Local session logs narrowed the useful branch to the July 8-9 Manual Data Editor incident evidence, especially the audit that captured missing active manual rows and field-level deltas. |
| Lost manual edit audit context | Available | `manual_package_edits/ai_context_summaries/2026-07-08-lost-manual-edits-audit-context.md` states the no-write boundary and separates rows missing from current manual state from rows whose human authorship cannot be proven because audit columns are blank. |
| Lost active manual row candidates | Available | `manual_package_edits/ai_context_summaries/2026-07-08-lost-manual-edit-candidates.csv` contains 60 rows present as active manual rows in historical raw snapshots but missing from the current active raw table. |
| Manual field deltas | Available | `manual_package_edits/ai_context_summaries/2026-07-08-manual-field-deltas-2026-non-social-non-amazon.csv` records the field-level manual value, baseline value, and delta for older editor rows whose package IDs were absent from current production active rows. |
| Rollback copy manual-marker diffs | Available | `manual_package_edits/ai_context_summaries/2026-07-09-rollback-copy-manual-field-diffs.csv` preserves 116 field-level manual-marker differences from the rollback copy for 2026 non-social/non-Amazon rows. |
| Current BigQuery presence recheck | Available | `manual_package_edits/ai_context_summaries/2026-07-09-field-delta-current-bq-presence.csv` compares the 49 filtered field-delta package IDs against current active raw and daily manual tables. |

### Confirmed Findings

#### Finding 10: The lost-change population is the older manual-marker/manual-row set, not the QUAN row

**Evidence:** `manual_package_edits/ai_context_summaries/2026-07-08-lost-manual-edits-audit-context.md`, `manual_package_edits/ai_context_summaries/2026-07-08-lost-manual-edit-candidates.csv`, and `manual_package_edits/ai_context_summaries/2026-07-08-manual-field-deltas-2026-non-social-non-amazon.csv`.

**Detail:** The July 8 audit context records that 60 active manual rows existed in historical raw snapshots but were missing from the current active raw table. The field-delta audit then expands the visible lost-change candidate set into 163 field records across 54 reviewed rows before the broader social-name exclusion. After excluding Amazon Ads and any social-like rows by package ID/name/site, the ranked candidate set contains 159 field changes across 49 packages.

#### Finding 11: The main affected columns are delivered metrics, plus smaller date/planned-metric edits

**Evidence:** `manual_package_edits/ai_context_summaries/2026-07-08-manual-field-deltas-2026-non-social-non-amazon.md`.

**Detail:** In the saved audit, changed fields were primarily `Impressions`, `Clicks`, `Spend`, `Video Plays`, and `Video Completions`, with smaller counts for `Delivery Override Start Date`, `Delivery Override End Date`, `Planned Spend`, `Planned Impressions`, `Flight Start Date`, and `Flight End Date`. When the broader social-like exclusion is applied, the counts are: `Impressions` 47, `Clicks` 34, `Spend` 33, `Video Plays` 12, `Video Completions` 12, delivery override dates 16 combined, planned metrics 3 combined, and flight dates 2 combined.

#### Finding 12: Largest non-Amazon, non-social-like lost field deltas start with Olipop, ADIF, and MassMutual packages

**Evidence:** `manual_package_edits/ai_context_summaries/2026-07-08-manual-field-deltas-2026-non-social-non-amazon.csv`.

**Detail:** The largest package-level candidates by absolute metric delta are:

| Package Friendly Name | Package ID | Largest Changed Field | Manual Value | Baseline Value | Delta |
| --- | --- | --- | --- | --- | --- |
| `MIQ_P3GT8NK_260601\|260831_OLIPOPLTOBrandPlatform2026_Spotify_CPM_pRate:19_p$:500000` | `P3GT8NK` | `Impressions` | `483,676` | `10,156,778` | `-9,673,102` |
| `MIQ_P3FFSQL_260401\|260630_ADIF2026_Q2PremiumCTV_CPM_pRate:34_p$:1080200` | `P3FFSQL` | `Impressions` | `25,559,237` | `33,854,913` | `-8,295,676` |
| `MIQ_P3GT8FP_260601\|260831_OLIPOPLTOBrandPlatform2026_PremiumCTVPubList_CPM_pRate:34_p$:1085000` | `P3GT8FP` | `Video Plays` | `5,028,237` | `12,784,626` | `-7,756,389` |
| `MIQ_P3FHBC6_260501\|260531_ADIF2026_Q2BridalDOOH_CPM_pRate:27_p$:0` | `P3FHBC6` | `Planned Impressions` | `3,703,704` | `0` | `3,703,704` |
| `MIQ_P3FFV6H_260401\|260630_ADIF2026_Q2HighImpactDisplay_CPM_pRate:17_p$:125000` | `P3FFV6H` | `Impressions` | `6,383,369` | `9,303,144` | `-2,919,775` |
| `PLAYFY_P3FFPRW_260330\|260930_20252026Media_VideoCommercialMLB_Free_pRate:0_p$:0` | `P3FFPRW` | `Impressions` | `15,581,482` | `18,243,509` | `-2,662,027` |

#### Finding 13: Current BigQuery still has no active raw or daily manual rows for the 49 field-delta packages

**Evidence:** `manual_package_edits/ai_context_summaries/2026-07-09-field-delta-current-bq-presence.csv`.

**Detail:** A read-only live BigQuery recheck against `landing.master_data_model_manual_package_edits_raw` and `landing.master_data_model_manual_package_daily` found 49 field-delta package IDs, 0 packages with active raw rows, 0 packages with active valid raw rows, 0 packages with active daily rows, and 0 packages with active valid daily rows. This makes the field-delta set the strongest current lost-change target: the older workbook preserved the manual values and baselines, but the current active manual backend tables do not carry active replacements for those packages.

### Updated Missing Evidence

| Gap | Impact | How to Obtain |
| --- | --- | --- |
| Human authorship and edit timestamp for many lost rows | Historical raw snapshots have blank `Manual Edit At` / `Manual Edit By`, so BigQuery alone cannot prove the person who edited each row | Use Drive Activity where possible, adjacent revision exports where available, and any older workbook snapshots that preserve cell-level edit history. |

### Follow-up Conclusion

**Confidence:** High that the QUAN row is not the main lost-change population; high that the July 8 field-delta file is the best current list of affected package/field candidates.

Codex session logs helped narrow the investigation back to the correct branch: the lost changes are the older manual rows and manual-marker field deltas, not the QUAN line. The strongest current field-level list is `2026-07-08-manual-field-deltas-2026-non-social-non-amazon.csv`, with `Package Friendly Name` as the primary label and baseline values preserved next to the manual values. A current read-only BigQuery recheck confirms that the 49 filtered field-delta package IDs still have no active raw or active daily manual rows.

## Follow-up: 2026-07-09 #4

### Loader Safety Fix

**Status:** Implemented locally; not verified end to end against the live Sheet because that would rewrite production-visible data.

The canonical loader now compares previously accepted active manual rows against the proposed refresh output before any BigQuery `WRITE_TRUNCATE` upload or visible Sheet rewrite. If a prior accepted manual edit would be missing, inactive, blocked, or missing an edited field without a newer user edit timestamp/editor stamp, the loader stops and reports the affected rows by `Package Friendly Name` first.

### Boundary

This fix prevents the next refresh from silently deleting accepted manual edits that are still present in the current manual raw table. It does not itself restore the already missing 49 field-delta packages. Those remain captured in `manual_package_edits/ai_context_summaries/2026-07-09-manual-edit-restore-list-2026-non-social-non-amazon.csv` and need an explicit restore step before running production.
