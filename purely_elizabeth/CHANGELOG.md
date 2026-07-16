# Changelog

Notable Purely Elizabeth reporting changes are recorded here.

## 2026-07-16

- **CHANGED** - Converted the delivery-plus-sales weekly output from a view to a stored table and attached its refresh to the existing `master_raw_CopyToWest` scheduled query, which runs every two hours.

## 2026-07-15

- **ADDED** - Published the Purely Elizabeth campaign mapping table in the `PE` dataset with two approved Protein Granola campaigns and precedence rules that preserve explicit exclusions over the normalized-name fallback.
- **ADDED** - Published and verified the Sunday-ending delivery-plus-sales weekly view. It retains 11 sales-only weeks, 19 media-only weeks, and one overlapping week across 31 unique dates; sales and TDP reconcile exactly, and media measures reconcile within floating-point precision.
- **CHANGED** - Aligned the weekly view's shared media fields with the main data model and standardized its retail measures as `sales_dollars` and `sales_tdp`, avoiding product-specific prefixes already represented by `product_group`.
- **CHANGED** - Reduced the production weekly view to the confirmed metric-only contract: Sunday-ending date, year-free product group, eight media measures, and two sales measures. Campaign and other descriptive fields remain internal or test-only.
- **CHANGED** - Generalized product eligibility so every included mapping-table product group with media delivery can appear; the Protein Granola sales source remains correctly labeled and joins only to that product group.

## 2026-07-14

- **ADDED** - Published a weekly Protein Granola sales view that combines the approved MULO and Natural Expanded product set. Pre-deployment QA passed, and the live view returned 12 unique weeks with no missing dates while reconciling $992,033.87 in Dollar sales and 193.8 total TDP to the approved source calculation.

### Pending Next Actions

- **Since Jul 14** - Decide and implement the weekly SPINS source-file refresh workflow - RECOMMENDED
