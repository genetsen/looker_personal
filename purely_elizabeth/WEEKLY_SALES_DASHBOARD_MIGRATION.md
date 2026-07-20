# Purely Elizabeth Weekly Sales and Dashboard Migration

## What changed

The production weekly delivery-and-sales table now shows three retail comparisons beside weekly media delivery: Total Brand, Total Brand Granola, and Protein Granola. Sales are weekly comparison metrics; they are not attributed to a campaign.

## Dashboard field migration

The two previous fields were renamed because they contain the sales for the row's mapped product group:

| Previous field | Replacement field | Dashboard action |
|---|---|---|
| `sales_dollars` | `product_group_sales_dollars` | Replace the old field in charts, scorecards, filters, and calculated fields. |
| `sales_tdp` | `product_group_tdp` | Replace the old field in charts, scorecards, filters, and calculated fields. |

Four fields were added:

| New field | Plain-English meaning |
|---|---|
| `total_brand_sales_dollars` | All Purely Elizabeth Brand-level Dollar Sales in Shelf Stable Cold Cereals and Shelf Stable Hot Cereals. |
| `total_brand_tdp` | TDP for the same Total Brand definition. |
| `total_brand_granola_sales_dollars` | Purely Elizabeth Brand-level Dollar Sales in SS Granola & Muesli. |
| `total_brand_granola_tdp` | TDP for the same Total Brand Granola definition. |

### Update a Looker Studio data source

1. Open the dashboard's data source for the weekly delivery-and-sales table.
2. Select **Refresh fields** so Looker Studio detects the renamed and added fields.
3. Replace every use of `sales_dollars` with `product_group_sales_dollars`.
4. Replace every use of `sales_tdp` with `product_group_tdp`.
5. Add the four benchmark fields where the dashboard should compare product sales with Brand or Brand Granola.
6. Check the week ending June 14, 2026: Protein Granola should be $222,861.62 and 67.6 TDP; Total Brand should be $5,007,219.72 and 2,136.2 TDP; Total Brand Granola should be $4,274,655.28 and 1,334.4 TDP.

### Aggregation warning

Use `MAX` for Total Brand and Total Brand Granola when a chart combines more than one product group for the same week. Those benchmark values repeat on each mapped product-group row and must not be added together. Product-group sales may be summed only when the chart grain and filters prevent the same weekly product group from appearing more than once.

## Exact sales definitions

All definitions include `TOTAL US - MULO` plus `TOTAL US - NATURAL EXPANDED CHANNEL` and Brand `PURELY ELIZABETH`.

| Comparison | Product level | Additional filters |
|---|---|---|
| Total Brand | `BRAND` | Category is `SHELF STABLE COLD CEREALS` or `SHELF STABLE HOT CEREALS`. |
| Total Brand Granola | `BRAND` | Subcategory is `SS GRANOLA & MUESLI`. |
| Protein Granola | `UPC` | Subcategory is `SS GRANOLA & MUESLI`; UPC is `08-10589-03260`, `08-10589-03261`, or `08-10589-03259`. |

## Data lineage

1. The typed SPINS source supplies weekly geography, Brand, category, subcategory, UPC, Dollar Sales, and TDP.
2. The existing weekly sales view calculates all six sales fields at one row per week.
3. The campaign mapping assigns each delivery campaign to a product group or leaves it `UNMAPPED`.
4. The downstream table joins retail sales to weekly media by Sunday-ending date. Protein Granola populates the product-group fields only for `PROTEIN GRANOLA`; all sales fields remain blank for `UNMAPPED`.
5. The `master_raw_CopyToWest` scheduled query rebuilds the downstream table every two hours.

## Production objects and source files

- Weekly view: [BigQuery weekly sales view](https://console.cloud.google.com/bigquery?project=looker-studio-pro-452620&ws=!1m5!1m4!4m3!1slooker-studio-pro-452620!2sPE!3sprotein_granola_weekly_sales)
- Dashboard table: [BigQuery delivery-plus-sales table](https://console.cloud.google.com/bigquery?project=looker-studio-pro-452620&ws=!1m5!1m4!4m3!1slooker-studio-pro-452620!2smaster_ext_west!3smart_PE_delivery_plus_sales_weekly)
- Weekly SQL: [protein_granola_weekly_sales.sql](/Users/eugenetsenter/Looker_clonedRepo/looker_personal/purely_elizabeth/protein_granola_weekly_sales.sql)
- Downstream SQL: [create_master_ext_west_mart_PE_delivery_plus_sales_weekly.sql](/Users/eugenetsenter/Looker_clonedRepo/looker_personal/master_data_model/model/reporting_outputs/create_master_ext_west_mart_PE_delivery_plus_sales_weekly.sql)

## Validation completed July 20, 2026

- The live dashboard table has 176 rows and 176 unique week/product-group keys.
- No `UNMAPPED` row contains a sales or TDP value.
- Protein Granola values remained unchanged for the latest available sales week.
- The live schema contains all six requested sales/TDP fields.

