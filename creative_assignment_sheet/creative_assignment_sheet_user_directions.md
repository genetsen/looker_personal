# Creative Assignment Sheet User Directions

Last updated: May 13, 2026

## What This Sheet Does

This workbook helps a trafficker assign one or more creative assets to each Prisma placement, then turns those assignments into trafficking-ready output.

The current workflow is manual. The old automatic Prisma import flow is deprecated. Do not rely on overnight updates, hidden BigQuery-connected tabs, or archived auto-import tabs as the source of truth.

Use the workbook for four jobs:

1. Paste the latest Prisma export into the manual Prisma input tab.
2. Enter the creative assets and creative details.
3. Assign creatives to placements in the assignment matrix.
4. Review the generated UTMs and Adswerve output before sending.

## Before You Start

Make a fresh copy of the template for each client or campaign.

Before using a copied version, update or review:

- The workbook title.
- Any sample rows left from another client.
- Client-specific campaign names.
- Client-specific UTM rules.
- Client-specific validation lists.
- Any external spreadsheet links.
- Any hidden or archived tabs that came from the template.

If a tab is marked archived, legacy, deprecated, or internal, do not edit it unless you are maintaining the template logic.

## Step 0: Paste Prisma Data

Use tab: `STEP 0 | Prisma - Manual Entry`

1. Export the media plan from Prisma.
2. Open `STEP 0 | Prisma - Manual Entry`.
3. Paste the Prisma export into the designated paste area, starting where the sheet tells you to paste sample rows.
4. Do not insert or delete structural rows.
5. Do not rename or move the formula/helper columns at the far right of the tab.

This tab is the current source of truth for placement data.

Watch for:

- Missing placement IDs.
- Missing placement names.
- Changed Prisma column order.
- Formula errors such as `#REF!`, `#VALUE!`, or unusually wide date cells showing `#######`.

If the Prisma export shape changed, stop and ask for help before continuing.

## Step 1: Add Creative Details

Use tab: `STEP 1 | INPUT - Creative Details`

Enter one row per creative asset.

Required fields:

- `Asset Name`
- `Creative Type`
- `Destination URL`, if the creative needs a different landing page than the placement-level URL

Optional fields:

- `Rotation`
- `Campaign`
- `Supplier`
- `Package > Placement`

Creative name rules:

- Use the creative asset filename without the file extension.
- Do not include the ad size unless the ad size is truly part of the required filename.
- Keep names consistent, short, and human-readable.
- Avoid duplicate creative names.

The assignment matrix dropdowns pull creative names from this tab, so the `Asset Name` column must be clean.

## Step 2: Assign Creatives To Placements

Use tab: `STEP 2 |  Creative Assignment Matrix v2`

Each row represents a placement. Each creative assignment column lets you pick one creative from the dropdown list.

1. Filter the matrix to the campaign, supplier, package, or placement you are working on.
2. Review the placement details before assigning creatives.
3. Select the correct creative name from the dropdown.
4. Use one creative per assignment cell.
5. If a placement needs multiple creatives, fill the assignment cells from left to right.
6. Leave assignment cells blank when no creative should be assigned.

The creative dropdowns come from `STEP 1 | INPUT - Creative Details`.

Do not manually type a creative name unless you are intentionally adding a new value to Step 1 first.

## Step 3: Review Generated UTMs

Use tabs:

- `AUTO | UTM Builder | INTERNAL v2`
- `MANUAL UTM BUILDER`, if building UTMs outside the assignment matrix workflow
- `All_UTMs`, if a shared UTM list is needed

The internal UTM builder turns each placement and creative pairing into UTM fields.

Review these fields:

- `utm_source`
- `utm_medium`
- `utm_campaign`
- `utm_source_platform`
- `utm_content`
- `utm_term`
- Final destination URL

Check that:

- The UTM values match the client naming rules.
- The landing page URL works.
- The final URL uses `?` or `&` correctly.
- Blank assignment rows do not create fake output.
- Missing inputs are fixed before sending anything externally.

## Step 4: Review The Adswerve Output

Use tab: `OUTPUT | Adswerve Doc | v1`

This tab is intended to be the trafficking-ready output for Adswerve.

Before sending:

1. Filter out blank rows.
2. Check that every output row has a placement and creative.
3. Check that creative names match expected filenames.
4. Check that click-through URLs are populated where needed.
5. Check that final UTMs are present and readable.
6. Spot-check a few final URLs in the browser.

Do not send the output if there are unresolved formula errors, missing creative assignments, or stale client names.

## Manual UTM Builder

Use tab: `MANUAL UTM BUILDER`

This is a separate workflow for building UTMs manually.

Use it when:

- A placement does not fit the assignment matrix workflow.
- You need a one-off UTM.
- You are building a UTM before the full placement assignment flow is ready.

The manual builder has dropdowns and formulas for UTM values. Required inputs include the line item, landing page, supplier, campaign name, package or placement, creative, audience, and geography fields.

Review the final UTM and final destination URL before using the result.

## Deprecated Or Legacy Areas

This template was originally built for another client and has been duplicated/customized over time.

Treat these areas as legacy unless the template owner confirms otherwise:

- Auto Prisma import tabs.
- Hidden BigQuery-connected tabs.
- Archived UTM builder tabs.
- Old lookup-table tabs.
- External `IMPORTRANGE` formulas pointing to other spreadsheets.
- Sample rows from older clients.

Client-specific leftovers are a known template risk. If you see names from another client, do not assume they are active rules.

## Quality Checks Before Sending

Before sending the Adswerve output, check:

- No visible `#REF!`, `#VALUE!`, `#N/A`, or `missing input` errors.
- No blank creative names in assigned rows.
- No duplicate creative names unless intentional.
- No stale client names in visible output.
- No final URLs that fail to open.
- No UTMs with spaces or unexpected capitalization.
- No rows created from test/sample data.
- No output rows that came from deprecated auto-import logic by mistake.

## When To Stop And Ask For Help

Stop before sending if:

- The Prisma export columns changed.
- Dropdowns do not show the expected creative names.
- The assignment matrix is producing blank or duplicated rows.
- A formula was accidentally deleted.
- A hidden tab appears to be driving visible output.
- A copied workbook still contains another client's values in output fields.
- The generated Adswerve output does not match the expected row count.

## Suggested Template Improvements

For a more durable future version, move these pieces out of copied spreadsheet formulas and into a configurable app:

- Client configuration.
- Prisma export parsing.
- Creative asset management.
- Creative-to-placement assignment.
- UTM rule generation.
- QA checks.
- Adswerve export formatting.

The long-term goal should be a web app where each client has its own configuration, and the user follows the same clean workflow without needing to edit hidden formulas.
