# First-Party Partner Data Collection Template

This document explains the configurable Google Sheets template used to request first-party delivery data from media partners. It is based on a live read of the workbook on July 8, 2026, plus formula and validation inspection through the Google Sheets API.

Source workbook: [2025 Template v2 Partner Data Collection](https://docs.google.com/spreadsheets/d/15zQ_IZx0kFpAffCFpp2d8ddDfjRf-kjnoSQXHxSu5eA/edit?gid=762675964#gid=762675964)

## What This Template Does

The workbook is a reusable request builder. Giant Spoon configures the request on `Config`, the workbook generates a partner-facing request list and data-entry table on `data`, and the partner selects the requested package/placement rows before filling in delivery metrics such as spend, impressions, clicks, sends, opens, views, or completions.

The template is intentionally named `Dupe before using`. The expected operating model is to duplicate the workbook, configure the copy for one client/partner/campaign/channel scope, send the copy to the partner, and later ingest the partner-reported first-party data into the FPD pipeline.

## Current Workbook Shape

| Tab | Visible? | Main audience | Purpose | Key ranges |
|---|---:|---|---|---|
| `Instructions for GS` | Yes | Giant Spoon setup owner | Internal setup checklist plus partner-facing data-entry rules. | `B2:D10`, `B19:C24` |
| `Config` | Yes | Giant Spoon setup owner | Selects scope, report settings, package inclusion, and package-name mappings. | `C4:C14`, `B18:I134`, `H5:AB10` |
| `data` | Yes | Partner | Final data-entry surface generated from `Config`; includes a requested-package list plus a dropdown-driven entry table. | `I55:K60`, `A61:AB500` |
| `simple_template` | Hidden | Maintainer | Static example table showing a simple partner data format. | `A1:L5` |
| `Validation` | Yes | Maintainer | Helper lists for dropdowns, filtered package lists, dates, tracking status, DCM impressions, and package IDs. | `A1:AF80` |
| `Log` | Yes | Maintainer | Historical record of generated partner copies and destination folders. | `A:H` |
| `ClientMapping` | Yes | Maintainer | Maps short client codes to folder names and year-folder defaults. | `A:D` |
| `Untracked_Media` | Yes | Maintainer | Lists partner/client combinations that need partner data sheets even when they are not tracked through standard package evidence. | `A:H` |
| `CustomFolderPrefs` | Yes | Maintainer | Stores custom Drive folder preferences by client code and partner. | `A:E` |

## Setup Flow

| Step | Owner | Where | What happens | Why it matters |
|---:|---|---|---|---|
| 1 | Giant Spoon | Google Drive | Duplicate the template before changing it. | Keeps the reusable source template clean. |
| 2 | Giant Spoon | `Config` connection status | Allow the `IMPORTRANGE` connection if the title/status area is red or disconnected. | The workbook pulls package metadata, tracking status, DCM impressions, and dropdown lists from an internal source workbook. |
| 3 | Giant Spoon | `Config!C4:C7` | Select client, partner, campaign, and channel. Campaign and channel can be left blank to keep the scope broader. | These controls filter the package list to the right request scope. |
| 4 | Giant Spoon or partner | `Config!C9:C13` | Confirm dates, date grain, extra dimensions, and extra metrics. | These settings create the column layout on `data`. |
| 5 | Giant Spoon | `Config!B19:B134` | Review or override the `First Party Data Needed` checkboxes. | Only checked packages flow into the partner request. |
| 6 | Giant Spoon | `Config!H:I` | Add package-name mappings when partner names do not match Giant Spoon names. | The `data` tab can translate partner-provided package labels back to the expected package names. |
| 7 | Giant Spoon | `data` | Review generated instructions, requested-package row count, package dropdown behavior, and columns before sending. | Prevents sending a partner a broken or incomplete request. |
| 8 | Giant Spoon | Workbook tabs | Hide internal/helper tabs before partner handoff. | Reduces partner confusion and lowers formula-break risk. |
| 9 | Partner | `data` | Enter delivery data in the generated table without adding, deleting, reordering, or renaming columns/tabs. | Preserves the loader-friendly structure. |

## Configuration Controls

| Control | Current live value | Validation source | Behavior |
|---|---|---|---|
| `Config!C4` Client | `APO` | `Validation!G2:G13` | Sets the client code and begins filtering the package list. |
| `Config!C5` Partner | `NYTIME` | Named range `L_partner` | Required. Many warning formulas remain active until this is populated. |
| `Config!C6` Campaign | Blank | Named range `l_campaign` | Optional narrowing control for campaign. |
| `Config!C7` Channel | Blank | `Validation!H2:H9` | Optional narrowing control for channel/media type. |
| `Config!C9` Start Date | `9/8/2025` | Valid date | Defaults to the minimum start date for the selected packages; can be overwritten. |
| `Config!C10` End Date | `6/29/2026` | Valid date | Defaults to the maximum end date for the selected packages; can be overwritten. |
| `Config!C11` Date Interval Type | `Date` | `Validation!Q2:Q5` | Setup-side date grain selector. A temp-copy test showed the active output also depends on the partner-facing date-grain cell on `data`. |
| `Config!C12` Additional Dimensions | `Placement` | `Validation!O2:O50` | Comma-split into the generated `data` table. |
| `Config!C13` Additional Metrics | `Clicks` | `Validation!P2:P14` | Comma-split into the generated `data` table after required spend/impressions columns. |
| `Config!C14` Date Interval Type (internal) | `Date` | Formula plus date-grain validation | Syncs with the partner-visible date grain on `data`; this is the value used by the generated header formula. |

## Package Selection Logic

The package list starts at `Config!B18`. It is driven by the `Validation` tab and then enriched with lookup formulas.

| Column | Meaning | Formula behavior |
|---|---|---|
| `First Party Data Needed` | Checkbox-like boolean that decides whether the package appears in the request. | Defaults to `TRUE` when tracking status is `Untagged - Needs FPD` or `Tagged but not tracking delivery`; defaults to `FALSE` for `Tracking Delivery` and `Pre-flight`. Giant Spoon can override it. |
| `Package` | Short display name for the package/placement. | Spills from `Validation!J2:J` after a partner is selected. |
| `Tracking Status` | Delivery-tracking classification. | Looks up the full package key in `Validations_table` and returns values like `Pre-flight`, `Tracking Delivery`, or `Untagged - Needs FPD`. |
| `DCM Impressions [to date]` | Current DCM impression evidence where available. | Looks up DCM impressions in `Validations_table`; blank is expected for untagged or unavailable rows. |
| `Full Package name` | Full package key used for matching and lineage. | Spills from `Validation!K2:K`. |
| `Package names from data sheet` | Partner-entered or generated package names not found in the configured package list. | Uses `UNIQUE(FILTER(..., ISNA(MATCH(...))))` to identify mapping gaps. |
| `GS package name` | Giant Spoon package name to map unmatched partner names back to. | Shows `mapping required` when an unmatched package name is detected. In the temp-copy prototype, this becomes a stable mapping target: partner label to `Campaign - Initiative - Package ID` plus a resolved full package key. |

The current live workbook generated a `data` requested-package row count of `9`, which matches nine checked packages in the selected APO/NYTIME scope.

## Generated Partner Data Tab

The `data` tab is the partner-facing output. It has three parts:

| Area | Range | Purpose |
|---|---|---|
| Connection/status area | Row 1 | Repeats the `IMPORTRANGE` connection status and partner-required warning formulas. |
| Partner notes | Around `I55:K59` | Lists the requested packages, date-format guidance, and what to do if the partner cannot use the requested date granularity. |
| Data-entry table | Starts at row 61 | Provides blank entry rows with a `Package / Placement` dropdown. Context columns populate after a package/placement is selected. |

The generated table starts with formula-owned context columns. These fields stay blank until the partner or setup owner selects a value in `Package / Placement`:

| Column | Field | Source behavior |
|---|---|---|
| `A` | Client | Extracted from the selected package key, falling back to `Config!C4`. |
| `B` | Channel | Looked up from the connected internal source workbook. |
| `C` | Site | Extracted from package naming, falling back to the selected partner. |
| `D` | Package | Resolved from the selected package/placement name and mapping table. |
| `E` | Campaign | Looked up from `Validation`. |
| `F:G` | Prisma Start/End Date | Looked up from `Validation`. |
| `H` | `package_id` | Looked up from `Validation`. |

The partner-fillable columns are dynamic:

| Setting | Current output | How it changes |
|---|---|---|
| Date interval | `Date` | Can become `Week (Sun-Sat)`, `Month`, or `Range`. `Range` creates separate reporting start/end date columns. |
| Additional dimensions | `Placement` | Split from comma-separated `Config!C12`. |
| Required metrics | `Spend`, `Impressions` | Always included. |
| Additional metrics | `Clicks` | Split from comma-separated `Config!C13`. |

## Temporary Copy Experiment Results

To avoid touching the source template, a temporary copy was created for experiments: [TEMP Codex FPD Template Experiment](https://docs.google.com/spreadsheets/d/1y5FoI-m_hijJnkVoy679ICIIopHY39NQg4OmuE703sQ).

| Experiment | Change made in temp copy | Observed result | What it means |
|---|---|---|---|
| Add columns | Set additional dimensions to `Placement,Creative` and additional metrics to `Clicks,Completed Views`. | The `data` header expanded to `Month`, `Package / Placement`, `Placement`, `Creative`, `Spend`, `Impressions`, `Clicks`, `Completed Views`. | Comma-separated dimensions and metrics do flow into the generated header. |
| Change date grain from setup side | Set `Config!C11` to `Month`. | The generated header stayed on `Date` until the partner-facing date-grain cell on `data` was also changed. | `Config!C11` alone is not enough; `Config!C14` follows the active value from the `data` tab. |
| Change date grain from partner-facing side | Set the date-grain selector on `data` to `Month`. | `Config!C14` changed to `Month`, and the generated header changed to `Month`. | The output header is driven by the synced internal date-grain value. |
| Select a package row | Entered `DealBook` in `data!J62`. | Context columns populated for client, channel, site, and full package key. | Partner rows are dropdown-driven; they are not fully prefilled until a package/placement is selected. |
| Force one extra package into the request | Set a previously `FALSE` package checkbox to `TRUE` on `Config`. | The right-side requested-package row count changed from `9` to `10`. | The checkbox column is the request-list control. |
| Prototype duplicate-name fix | Changed the temp copy's requested-package dropdown source to `Campaign - Initiative - Package ID`, backed by a hidden full-package-key helper column, then selected two `DealBook` rows. | The selected rows showed `Newsletters2026 - DealBook - P3FCQTS` and `Newsletters2026 - DealBook - P3CGHZG`, and resolved to distinct full package keys. | The simplest safe fix is to make the visible dropdown readable and unique while formulas resolve from a stable package key, not from the duplicated short label. |
| Prototype mapping-table fix | Changed the temp copy's mapping table to detect only partner-entered labels that are not already in the generated dropdown list, then map those labels to `Campaign - Initiative - Package ID` and a resolved full package key. | Test value `Partner DealBook Alias` mapped to `Newsletters2026 - DealBook - P3CGHZ4`, and the data row resolved to package ID `P3CGHZ4`. | Partner naming exceptions should map to a stable generated label/full key, not to a short package name or row position. |

## Implementation Note For Stable Package Mapping

Use this pattern when carrying the temp-copy fix back into the source template. The goal is to make the partner-facing selector readable, while keeping the actual lookup keyed by the full package key.

| Target | Formula shape | Purpose |
|---|---|---|
| `data!AB62` | Spill formula | Builds the visible dropdown label and hidden full package key from the same checked package rows. |
| `Config!H19` | Spill formula | Lists partner-entered labels that are not already valid generated dropdown labels. |
| `Config!J19:J` | Row formula | Resolves each mapped dropdown label to the hidden full package key. |
| `data!D62:D500` | Row formula | Resolves selected labels directly first, then falls back to the mapping table for partner aliases. |

Dropdown helper, entered once in `data!AB62`:

```gs
=FILTER({IFERROR(REGEXEXTRACT(Config!F19:F,"^Package_[^_]+_[^_]+_([^_]+)_"),Config!C19:C)&" - "&Config!C19:C&" - "&IFERROR(REGEXEXTRACT(Config!F19:F,"\|([^_]+)_"),Config!F19:F),Config!F19:F},Config!B19:B=TRUE)
```

Unmapped partner-label detector, entered once in `Config!H19`:

```gs
=UNIQUE(FILTER(data!J62:J500,data!J62:J500<>"",ISNA(MATCH(data!J62:J500,data!AB62:AB,0))))
```

Mapping resolver, filled down from `Config!J19`:

```gs
=IF(I19="","",XLOOKUP(I19,data!AB$62:AB,data!AC$62:AC,""))
```

Data-row package resolver, filled down from `data!D62`:

```gs
=IF($J62="", "", IFNA(XLOOKUP($J62,$AB$62:$AB,$AC$62:$AC), IFNA(XLOOKUP($J62,Config!$H$19:$H,Config!$J$19:$J), "")))
```

Important formula detail: leave the first `XLOOKUP` in `data!D62:D500` without a blank missing-value argument. If it uses `XLOOKUP(...,"")`, the direct lookup returns blank instead of erroring, so the mapping-table fallback never runs.

## Helper Tabs

| Helper tab | What it feeds | Maintenance note |
|---|---|---|
| `Validation` | Dropdown lists, date lists, filtered packages, tracking status, DCM impressions, package IDs, and internal source links. | Treat as formula-owned. Edits here can break several visible tabs at once. |
| `ClientMapping` | Client code to folder-name/year-folder defaults. | Add rows here when a client needs a better Drive folder destination. |
| `Untracked_Media` | Partner sheets needed for untracked/non-standard media. | Useful for FPD requests that are not naturally covered by package tracking. |
| `CustomFolderPrefs` | Client/partner-specific Drive folder overrides. | Used when the default folder mapping is not enough. |
| `Log` | Generated file and folder history. | Good for auditing whether a partner sheet was created and where it was stored. |
| `simple_template` | Static fallback/example table. | Hidden. Useful as a simple reference, not the active dynamic output. |

## Handoff Rules For Partners

Use the partner-facing instructions in `Instructions for GS` and `data` as the handoff contract:

| Rule | Why |
|---|---|
| Do not add, delete, or reorder columns. | The loader expects a stable table shape. |
| Do not rename tabs. | The loader and formulas refer to known tab names. |
| Use `MM/DD/YY` dates. | Keeps date parsing predictable. |
| Enter numbers without currency symbols or commas. | Keeps numeric fields parseable. |
| If package names do not match, use the mapping table instead of changing the generated package list. | Preserves the Giant Spoon package key while allowing partner naming differences. |

## Known Caveats Found During Review

| Caveat | Evidence | Recommended follow-up |
|---|---|---|
| Browser visual verification was blocked by Google sign-in in the in-app browser. | The in-app browser opened to `Google Sheets: Sign-in`; Sheets connector access worked. | Sign into the in-app browser if a rendered screenshot or visual order proof is required. |
| The visible instruction copy says to allow connection in `J1`, while active formulas and conditional formatting also reference `K1` and connection formulas appear in row 1. | Metadata shows conditional formatting rules tied to `$K$1 <> "connected"`, and `Config!K1` currently evaluates to `connected`. | Before training users, confirm the intended user-facing connection cell and update visible instructions if needed. |
| `data!L2` contains a formula that references `#REF!`. | Formula read showed `=IFNA(ARRAYFORMULA(FILTER(AB62:AB153,#REF!=true)))`. | Investigate whether this is legacy/dead helper logic or a broken output filter before changing the template. |
| The workbook exposes many helper tabs by default. | Metadata shows `Validation`, `Log`, `ClientMapping`, `Untracked_Media`, and `CustomFolderPrefs` are visible. | Hide helper tabs in each partner copy before sending unless the partner explicitly needs them. |
| Short package labels are not always unique. | In the temp copy, selecting `DealBook` populated the first matching full package key even though `DealBook` appears multiple times in the requested-package list. A later temp-copy prototype using `Campaign - Initiative - Package ID` let two `DealBook` selections resolve to different package IDs. | Make the partner-facing package selector unique with a readable label such as `Newsletters2026 - DealBook - P3FCQTS`, and keep a hidden helper column with the full package key. Avoid resolving rows from short package name alone. |
| Inserted or newly launched package rows can push manual ranges out of alignment. | The template has formula-owned package lists, user-editable checkbox selections, mapping columns, and helper ranges next to each other on `Config`. | Keep selectable/mapping logic keyed by stable package key. Prefer one spilled helper table that returns both the visible label and hidden full package key from the same checked rows, then use `XLOOKUP` from the selected label to the hidden key. This keeps new campaign rows aligned when the filtered package list changes. |
| Some visible copy has typos or dated wording. | Examples include `consistant`, `granularies`, and `pacakges`. | Clean copy after confirming the active workflow so wording changes do not mask functional changes. |

## Safe Maintenance Checklist

Before changing this template:

1. Work on a duplicate, not the source template.
2. Confirm the `IMPORTRANGE` connection status is `connected`.
3. Test one narrow scope with a known client and partner.
4. Confirm `Config` package checkboxes produce the expected `data` row count.
5. Confirm the generated `data` columns match the requested date interval, dimensions, and metrics.
6. Select at least one package in the `Package / Placement` dropdown and confirm the context columns populate correctly.
7. Check the mapping table if partner package names differ from Giant Spoon names.
8. Hide internal tabs before partner handoff.
9. After partner return, validate the sheet shape before loading it into the FPD pipeline.

## Glossary

| Term | Plain meaning |
|---|---|
| First-party data | Delivery data supplied directly by a partner or platform rather than by DCM. |
| DCM | Campaign Manager delivery evidence, used here as tracking/impression context. |
| `IMPORTRANGE` | Google Sheets formula that pulls cells from another spreadsheet after permission is granted. |
| Named range | A named reusable range such as `L_partner` or `Package_List`, used by formulas and dropdowns. |
| Date grain | The reporting time bucket: day, week, month, or custom range. |
