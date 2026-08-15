# FPD Partner-Template "Publish" Button — Understanding & Fix Design

Audience: whoever maintains the First-Party-Data (FPD) partner-sheet workflow.
Scope: explains what the in-sheet **Publish** button does, why it broke after
the Google account/domain migration, and the approved plan to restore it in a
way that matches the new per-client Shared Drive structure. Read the companion
[partner-data-collection-template.md](partner-data-collection-template.md) for
the workbook itself.

> **Correction (2026-08-12):** The in-use template is **`GS | Partner Data
> Collection | Template 2026 v3`** =
> `1BYqrQrjL4_rf5-LKTlGkR94CkqSW6QOsYzAPLHxucfY`. Earlier references below to
> `1pc9gXkMhWZ0dFNeagZWjUqsKUnWebIvB3xd5IGht4w4` ("*2025 Template* v3 … Dupe
> before using") point at an **older copy**; the canonical doc had mislabeled it
> as current. The bound stub goes on `1BYq`, and the library's
> `MASTER_SPREADSHEET_ID` (central Log / ClientMapping / CustomFolderPrefs) is
> now `1BYq`. Treat `1BYq` wherever `1pc9` appears below.

## 1. What the Publish button does

The button on the current template workbook
(`1pc9gXkMhWZ0dFNeagZWjUqsKUnWebIvB3xd5IGht4w4`) calls a standalone Apps Script
**duplicator library** via a tiny bound "stub":
`FPDLib.duplicateAndSetup(SpreadsheetApp.getActiveSpreadsheet())`.

`duplicateAndSetup(ss)` runs **as the operator who clicks it** and performs 11
steps (full recovered source: [apps_script/](apps_script/)):

1. Read + validate `Config`: `C4` client code, `C5` partner, `C6` campaign
   (optional), `C9` start date (→ year).
2. Find the client folder (legacy: subfolders under one My-Drive `CLIENTS` root).
3. Find/create the year folder.
4. Pick destination: a `First Party Data` subfolder, or a saved
   `CustomFolderPrefs` folder for that client+partner.
5. Generate filename `CLIENT | Partner Data Collection | PARTNER` (de-duplicated).
6. Confirmation dialog (proceed / change folder / cancel).
7. Duplicate the workbook into the destination (`makeCopy`).
8. **Hide every tab except `data`** (partner-visibility control).
9. **Transfer ownership to Gene** (`setOwner`) + grant `giantspoon.com` domain edit.
10. Create a Drive shortcut in a shortcuts folder.
11. Log the copy (timestamp, links, user, status) to the master workbook `Log` tab.

## 2. Why it broke

The account migration `gene.tsenter@old.giantspoon.com` → `@giantspoon.com`
severed the binding:

- The workbook button is still bound to the **old-account** library
  `publish_fpd_template` (`1sWAM…`), whose `MASTER_SPREADSHEET_ID` still points
  at the **retired v2** workbook (`15zQ…`).
- A corrected **v3 library** (`publish_fpd_template_fpdLib`, `139EY…`, owned by
  the new account) exists and its **version 3** ("fpdLib v3.1") targets the
  current workbook `1pc9…` and the migrated `CLIENTS` root — but nothing on the
  current workbook is wired to call it.
- Running `duplicateAndSetup` from the Apps Script editor fails in <1s because it
  needs the active spreadsheet **and** an interactive UI.

## 3. The bigger problem: Drive structure changed

Even repointing to v3 is **not enough**, because the whole Drive model changed:

- The v3 `CLIENTS` root (`1uwVd0H8…`) is a **My-Drive folder** of legacy
  per-client subfolders — the *old* layout.
- The current reality is **one Shared Drive per client** (`Apollo`, `Olipop`,
  `Purely Elizabeth` (+ `| External |` / `| Internal |`), `MassMutual`,
  `A Diamond is Forever`, `Ritual`, `NBC Universal`, `GE Aerospace`, …).
- In a Shared Drive, **`setOwner` is meaningless** — files are owned by the org,
  and control comes from Drive **membership** (Gene is Content Manager+, Manager
  on some). So step 9's ownership transfer both fails and is unnecessary there.

### Established destination convention (evidence from live drives)

Partner sheets already live in a **`First Party Data` folder inside the client's
main (non-External) Shared Drive**, named `CLIENT | Partner Data Collection |
PARTNER` (matches Apollo, Olipop, Purely Elizabeth today). "Internal" = the
client's main working drive where this folder already exists — **not** a
`| Internal |`-suffixed drive (Purely Elizabeth's real FPD folder is in the plain
`Purely Elizabeth` drive under `MEASUREMENT`).

| Client | Shared Drive | `First Party Data` folder ID |
|---|---|---|
| APO Apollo | `Apollo` | `1hojhJo2zzYKXvAINVf7X14AI3qWKoX_O` |
| OLI Olipop | `Olipop` | `1cMkgbplZ8sPIsDHluIuIEOmbam-BhZtY` |
| PURE Purely Elizabeth | `Purely Elizabeth` | `12JRxmVv6N1zsHbxgSl_BoilOWLFcsFob` |
| FMUS A Diamond is Forever | `A Diamond is Forever` | `1SU3Id8DdE2ApA3ofS-4OUp11HqPeOyWF` |
| MASS MassMutual | `MassMutual` | `1nABcyJ33sGm2EGHlrQhBKlZO5_8MKH4l` |

Clients without a folder yet (Ritual, NBC, GE) get one created on first use.

## 4. Approved decisions

- **Fix path:** restore the Apps Script (fastest; runs on Google, no Mac, instant,
  in-sheet). Source is now version-controlled to reduce future fragility.
- **Order:** fix routing **first**, then install the button (no interim misrouting).
- **Routing:** each client's sheet goes to its **main-drive `First Party Data`
  folder** (table above); create it if missing.
- **Ownership/control:** rely on **Shared Drive membership**, not `setOwner`
  (skip `setOwner` when the destination is a Shared Drive).
- **Partner access:** **optional** auto-share to a partner email if one is
  provided in `Config`, behind an explicit confirmation (external share); default
  stays manual.
- **Delivery:** ship the updated library + stub as **repo files the maintainer
  pastes + publishes in the editor** — no write Apps Script scope, no API
  mutation of the production library.

## 5. v4 library change (minimal, additive delta from v3)

1. **`CLIENT_FPD_FOLDERS` map** in `CONFIG` (client code → Shared Drive
   `First Party Data` folder ID), seeded from the table above.
2. **Destination resolver** used before the legacy My-Drive traversal:
   exact `CustomFolderPrefs` (client+partner) → **client-level** `CustomFolderPrefs`
   (blank partner) → `CLIENT_FPD_FOLDERS[client]` → else fall back to the existing
   My-Drive behavior/prompt. Mapped clients skip the old traversal entirely.
3. **Shared-Drive-aware `setPermissions_`:** detect the file's `driveId`; if in a
   Shared Drive, **skip `setOwner`** (log info, not a warning) and rely on
   membership; optionally `addEditor(partnerEmail)`.
4. **Optional auto-share:** read a partner-email `Config` cell
   (`CONFIG.CELLS.PARTNER_EMAIL`); if non-empty and confirmed, share the finished
   sheet to that address. Blank → manual (today's behavior).
5. Unchanged: hide-all-tabs-except-`data`, shortcut, master `Log`.

Publish this as library **version 4**; the stub binds to it.

## 6. Bound stub (done)

[apps_script/current_workbook_bound_stub/Stub.gs](apps_script/current_workbook_bound_stub/Stub.gs)
— `publishFpdTemplate()` passes the active spreadsheet to `FPDLib`; `onOpen()`
adds a `Publish New FPD Sheet ▸ Publish` menu (a menu survives migrations better
than a drawing button).

## 7. Install + test (after v4 is published)

1. Open `1pc9…` → Extensions → Apps Script.
2. Libraries **+** → Script ID `139EYkXK6w08HSN5wbWfdsoB3keQIEHSqFoEHqbckiYRWDIvvvIfz08BF`
   → **version 4** → identifier `FPDLib`. Remove any old-account library reference.
3. Paste `Stub.gs`. Save; reload workbook (menu appears).
4. Reassign the drawing button → `publishFpdTemplate`.
5. First click → authorize Drive as the new account.
6. **Test** on a throwaway scope (e.g. `PURE`/`MIQ`): confirm the copy lands in
   `Purely Elizabeth / MEASUREMENT / First Party Data`, only `data` is visible,
   Gene retains edit via membership, and the `Log` row is written.

## 8. Risks / open items

- `DriveApp.makeCopy` into a Shared Drive needs Content Manager (Gene has it).
- Non-uniform client structures: unmapped clients fall back to the legacy path or
  prompt — keep the `CLIENT_FPD_FOLDERS` map / `CustomFolderPrefs` current.
- External auto-share must stay confirmation-gated to avoid mis-sends.
- The library source lives only in the cloud + this repo copy; keep them in sync.
- **Central source workbook is in the OLD workspace.** The template's `IMPORTRANGE`
  source `1T4PCzCmkpB35MYaML1Z8ovitSgeE-2WCqFnNi8t_Qds` still lives in the old
  Google Workspace and must be migrated to the new one — every template and
  published sheet depends on it. (Track 2's BigQuery source ultimately replaces it.)

## 9. Additional improvements (folded into v4)

1. **Prefs & Log location — already correct.** `getClientMapping_`, `getYearMapping_`,
   `getCustomFolderPref_`/`saveCustomFolderPref_`, and `logAction_` all open
   `MASTER_SPREADSHEET_ID` (the base template `1pc9…`), never the active copy.
   Each dupe's own copies of those tabs are unused clutter — motivating #2.
2. **Mark the source dupe as outdated (v4 `archiveSourceDupe_`).** After a
   successful publish, the working copy Publish was launched from is renamed
   `ARCHIVE - OUTDATED - …` (never the master template) so the team uses the
   newly published sheet.
3. **Lock the partner scope (v4 `lockPublishedSheet_`).** Non-`data` tabs are
   protected to Gene-only so an editor cannot repoint the partner selection.
4. **Cross-partner confidentiality — see §10.** This is the important one and is
   *not* fully solved by Track 1.

## 10. Confidentiality finding & Track 2 (per-partner BigQuery source)

**Finding (evidence-based).** The template is riddled with live links to central
data: ~**1,202 `IMPORTRANGE` cells in `Validation`, 141 in `data`, 2 in `Config`**,
pulling from central sources `1T4PCz…` and `1qi2bDaV…`. The formulas pull **entire
central columns** and filter locally (e.g. `FILTER(IMPORTRANGE(central,"raw_data!o3:o"),
REGEXMATCH(…,Config!C4), REGEXMATCH(…,Config!C5))`). Because `IMPORTRANGE` renders
with the **owner's** access and authorization is **per-spreadsheet**, an editor
(the partner) can drop `=IMPORTRANGE(central,"raw_data!A:ZZ")` into any editable
entry cell and see **every** client/partner's data — no re-auth prompt. Hiding
tabs and protecting ranges do **not** stop this (protection blocks *writing* a
cell, not *reading* values via a formula). So **as-is, a partner is not prevented
from seeing other partners' data.** (Download/Make-a-copy are safer: a copy breaks
the central link, since the partner cannot re-authorize it.)

**Decision.** Keep the source **live and reconfigurable** but make it physically
**partner-scoped**, by moving the source from the central Sheet to a **BigQuery
view filtered in SQL** via **Connected Sheets**:

- One BQ view carries the partner filter in SQL
  (`WHERE advertiser_short_name = <client> AND supplier_code = <partner>`), so
  other partners' rows never enter the Sheet.
- An **external partner has no credentials in the BQ project**, so they cannot
  re-run the query, point at another table, or refresh to broaden it — the exact
  guarantee we need, including on download/duplicate.
- "Live" becomes **scheduled/on-demand refresh** run as Gene's BQ identity.

**Feasibility — confirmed.** `Prisma.prisma__stg__digital_plus_linear_view`
already has every field the template needs: `advertiser_short_name` (client),
`supplier_code`/`supplier_name` (partner), `campaign_name`, `channel`,
`package_id`, `package_name`, `p_package_friendly`, `start_date`/`end_date`,
`total_dcm_impressions`, `untracked_flag`.

**Sequencing (approved): Track 1 now, Track 2 next.** Track 1 (this v4) restores
creation + Shared-Drive routing + interim lockdown. **Because Track 1 leaves the
live central links in place, published sheets must NOT be shared to external
partners until Track 2 (per-partner BQ Connected-Sheets source) is in place.**
Track 2 design (BQ view definition, Connected-Sheets layout that preserves partner
entry columns, refresh mechanism) is the next work item.
