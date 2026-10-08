# October 8, 2026: Ritual QA repair verification

This dated record documents the live Omni repair and the checks that support it. It is evidence of saved configuration and query execution, not browser rendering or complete model health.

[Published Ritual dashboard](https://giantspoon.omniapp.co/dashboards/e8319cff)

## Change and preservation checks

- The shared file `master_stg/ritual_data_model.query.view` now explicitly exposes `row_data_issue_category` using the existing SQL column. Its SQL and all other field definitions match the earlier live file.
- Saved QA tile key `20` replaces the missing `row_data_callouts` field in the query, column totals and display-field list. Filters, measures, calculations, formatting and dashboard layout are preserved.
- The existing draft was compared with published content. The four differing query-model IDs had identical YAML definitions; two empty control lists were equivalent to their absent published values. The workbook definitions were identical.
- The other 25 draft tiles are unchanged. After publication, the dashboard configuration and workbook model exactly match the tested draft.
- A fresh execution of the published saved QA query returned 164 result rows, including subtotal/total rows. Its CSV is identical to the fresh tested draft result.
- Issue-category values observed include `no_issues`, `actual_imps_no_plan`, `missing_actuals`, `missing_prisma_daily`, `low_signal_dcm`, `conversion_only` and `spend_no_imps`, including compound categories. Historical callout-text equivalence is not established.

## Draft query execution

All saved draft queries ran with their original fields, filters, calculations and limits against the actual draft workbook model. API results were written locally as CSV; no browser was used.

| Saved tile | Query | Result | CSV result rows, including totals |
|---|---|---|---|
| 1 | Pie channel spend | Passed | 4 |
| 10 | Site table | Passed | 18 |
| 11 | Package Table | Passed | 84 |
| 12 | channel by week chart | Passed | 168 |
| 13 | channel table | Passed | 7 |
| 14 | data freshness | Passed | 1 |
| 15 | Site | Passed | 11 |
| 16 | Spend by Day | Passed | 132 |
| 17 | Spend by Day table | Passed | 132 |
| 18 | data freshness Copy | Passed | 1 |
| 2 | Ritual | Passed | 1 |
| 20 | QA / Data Callouts | Passed | 164 |
| 21 | 🐇 Query | Passed | 1000 |
| 22 | Coversions by Package | Passed | 1 |
| 23 | DCM Conversions | Passed | 17 |
| 24 | Coversions by Package Copy | Passed | 13 |
| 25 | RTL_Coversions by Package | Passed | 690 |
| 26 | 🍹 Query | Passed | 48 |
| 27 | Preview - RTL_Coversions by Package | Passed | 451 |
| 28 | Package Table Copy | Passed | 332 |
| 29 | 🍹 Query Copy | Passed | 10 |
| 3 | Planned v Delivered | Passed | 441 |
| 30 | 👚 Query | Passed | 14897 |
| 31 | Preview - RTL_Coversions by Package Copy | Passed | 1000 |
| 5 | Topline | Passed | 1 |
| 8 | Topline Copy | Passed | 1 |

## Remaining limits

- Browser rendering and interaction were not checked, as requested.
- Full shared-model validation is not clean. The prior mainline check returned 22 issues; the unchanged branch and repaired branch both returned the same 76 issues. The mainline check after merge also returned 76. Additional reported issues reference deleted `sqlchg_guard_` test tables; their underlying warehouse absence was not separately investigated in this repair.
- Existing field-validation issues, including Ritual `amzn_total_cost`, are outside this QA tile repair.
- This repair does not establish a successful full scheduled runner execution.
