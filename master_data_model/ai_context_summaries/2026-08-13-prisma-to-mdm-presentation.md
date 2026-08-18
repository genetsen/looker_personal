# Prisma-to-MDM presentation context — August 13, 2026

## Intended outcome

Create a short, plain-language presentation for the Wpromote team that starts with the Master Data Model, explains why Prisma is its plan-side backbone, follows the plan into Campaign Manager and partner-reported delivery, and compares Giant Spoon's Prisma workflow with Wpromote's taxonomy system.

## Current artifacts

- The current editable prototype contains nine slides and matches the visual style of `Master_Data_Model_Overview.pptx`.
- The working outline is outside the repository at `prisma-to-mdm-taxonomy-deck-outline.txt` in the task visualization folder.
- The outline is the current content source of truth; the PowerPoint has not yet been updated to reflect the new ten-slide narrative.

## Settled content decisions

- Use very clear, simple language.
- Prisma selections are stored as structured metadata; automatically generated package and placement names are a helpful operational aid, not the primary value of the selections.
- The Prisma Package ID continues into UTMs and downstream delivery data.
- The Creative Assignment Sheet turns the Prisma export into a trafficking document; the output is auto-populated, but emailing and trafficking remain manual.
- Polaris and Prisma describe many related dimensions at different workflow stages; the exact field-by-field map is still being defined.
- FPD-to-Prisma matching uses exact Package ID plus date, not fuzzy package-name matching.

## Latest user update

The outline has been rebuilt as twelve slides:

- Start with what the Master Data Model does.
- Establish Prisma as the model's backbone because it supplies plan, flight, cost, taxonomy and identifier context.
- Show functions enabled by Prisma, including categorization, pacing, cost normalization, plan-versus-actual reporting and QA.
- Explain the Prisma configuration and trafficking workflow.
- Follow Campaign Manager and FPD delivery back into the model.
- Close by comparing Giant Spoon's Prisma workflow with Wpromote's Polaris taxonomy workflow.
- State clearly that the two systems overlap in purpose but are not fully compatible or natively integrated yet.
- Separate the functions that are unique to Prisma from the functions that are unique to Polaris.
- End with a proposed Prisma-to-WP taxonomy connector that carries a shared field crosswalk and Prisma metadata, while preserving Package ID or an explicit partner crosswalk for delivery matching.
- Clarify that Giant Spoon's Master Data Model already performs many functions associated with Polaris, including multi-source ingestion, normalization, mapping, QA, corrections and cross-channel reporting, but those capabilities are distributed across several components and are not managed as centrally or robustly as Polaris.

## Open presentation decision

The revised outline removes the approximate 80% and 10% labels from visible slide copy. Those estimates can be restored later if the audience needs them and the user confirms what population the percentages describe. The proposed bridge remains intentionally high level: align fields, share structured Prisma metadata, reuse it in WP taxonomy mapping, and preserve Package ID. No technical connector architecture has been selected.
