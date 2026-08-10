# Changelog

## 2026-08-10

**Automated SPINS sales refresh source is now preserved on the canonical branch** ([Apps Script](/Users/eugenetsenter/Looker_clonedRepo/looker_personal/purely_elizabeth/download_purely_elizabeth_sales_attachment.gs)) — 🟢 **Verified and committed**<br>The complete [Purely Elizabeth sales workflow](/Users/eugenetsenter/Looker_clonedRepo/looker_personal/purely_elizabeth/README.md#scheduled-bigquery-refresh)—Gmail selection, dated Drive and Cloud Storage preservation, guarded BigQuery refresh, three sales views, and the master weekly table—is available on `dev`. Local syntax and file-integrity checks passed; the live 34,042-row July 12 snapshot has zero duplicate keys, matches its selected external file, and is reflected consistently in all downstream sales outputs after the August 10 scheduled rebuild. [More details](/Users/eugenetsenter/Looker_clonedRepo/looker_personal/purely_elizabeth/sales_data_current.qa.json)

- **⚠️ Unverified** - No newer sales email was ingested during this source-integration check; the selected SPINS sales snapshot remains July 12, 2026.

### Pending Next Actions

- **Since Jul 30** - Connect the production sales views to the sales dashboard

## 2026-08-03

**Weekly SPINS delivery now refreshes the complete BigQuery reporting path automatically** ([Workflow guide](/Users/eugenetsenter/Looker_clonedRepo/looker_personal/purely_elizabeth/PURELY_ELIZABETH_SALES_REFRESH_WORKFLOW.docx)) — 🟢 **Verified and committed**<br>The [weekly sales refresh workflow](/Users/eugenetsenter/Looker_clonedRepo/looker_personal/purely_elizabeth/README.md#scheduled-bigquery-refresh) now carries Tess Vargas's newest valid `Data thru M.D.YY` tab from Gmail through a create-only dated Cloud Storage snapshot into guarded six-hour BigQuery refreshes, loading every row from the latest file without imposing a fixed rolling-week count; the live source and master refreshes reconcile reporting to that selected file with no duplicate keys, and a linked plain-language guide explains the complete path. [More details](/Users/eugenetsenter/Looker_clonedRepo/looker_personal/purely_elizabeth/sales_data_current.qa.json)

### Pending Next Actions

- **Since Jul 30** - Connect the production sales views to the sales dashboard

## 2026-07-30

**Production sales views published** ([SQL](/Users/eugenetsenter/Looker_clonedRepo/looker_personal/purely_elizabeth/create_sales_by_category_subcategory_brand_weekly.sql)) — 🟢 **Verified**<br>The [production sales views](/Users/eugenetsenter/Looker_clonedRepo/looker_personal/purely_elizabeth/README.md#production-sales-comparison-views) use the original SPINS terminology, clearly separate individual-UPC comparisons from summarized category, subcategory, and Brand totals, and now include source-reconciled dollars, units, and TDP in the summarized view. [More details](/Users/eugenetsenter/Looker_clonedRepo/looker_personal/purely_elizabeth/create_sales_upc_comparison_weekly.sql)

### Pending Next Actions

- **Since Jul 14** - Decide and implement the weekly SPINS source-file refresh workflow - DONE
- **Since Jul 30** - Connect the production sales views to the sales dashboard

## 2026-07-20

- **CHANGED** - Restored `sales_dollars` and `sales_tdp` as temporary compatibility copies of the Protein Granola-only `product_group_sales_dollars` and `product_group_tdp` fields. Existing dashboards can continue working while they migrate; new dashboard work should use the replacement names.
- **CHANGED** - Expanded the weekly sales view from Protein Granola-only metrics to six explicit Total Brand, Total Brand Granola, and Protein Granola sales/TDP fields. Renamed the downstream dashboard fields from `sales_dollars` and `sales_tdp` to `product_group_sales_dollars` and `product_group_tdp`, added four Brand benchmark fields, updated the two-hour scheduled query, and documented the required Looker Studio field migration.

Notable Purely Elizabeth reporting changes are recorded here.

## 2026-07-17

- **FIXED** - Kept every Purely Elizabeth campaign's delivery in the weekly reporting table when its product is unknown. Unmapped delivery is labeled `UNMAPPED` with blank sales measures, while retail sales remain limited to eligible mapped products.

### Pending Next Actions

- **Since Jul 14** - Decide and implement the weekly SPINS source-file refresh workflow - DONE

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

- **Since Jul 14** - Decide and implement the weekly SPINS source-file refresh workflow - DONE
