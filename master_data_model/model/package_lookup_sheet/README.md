# Master Data Model Package Lookup Sheet

This folder maintains the bound Google Apps Script for the [Master Data Model Package Lookup Sheet](https://docs.google.com/spreadsheets/d/1Q_KK6WWqB4aUGNezFmWTMTKz43rQyARqXVGgNs5IT3c).

## What it does

The Sheet has one search box that checks Package ID, Package Name, Package Friendly Name, Supplier Code, and Supplier Name. It runs a read-only, parameterized query against the [complete stored master-model support table](https://console.cloud.google.com/bigquery?project=looker-studio-pro-452620&p=looker-studio-pro-452620&d=master_stg&t=data_model_clustered_by_advertiser_qa&page=table), then groups the matching daily rows into one result per package. This includes valid manual-only packages such as Columbus Circle DOOH.

| Search field | Fields checked | Match behavior |
|---|---|---|
| Any part of an ID, name, site, or supplier | Package ID, Package Name, Package Friendly Name, Supplier Code, and Supplier Name | Case-insensitive partial match; a match in any one field is returned |

For example, `QUAN`, `Columbus Circle DOOH`, and `ccdooh` all return the Columbus Circle DOOH package, regardless of capitalization.

The result includes the package identifiers plus the requested flight, planned delivery, delivered metrics, advertiser, package, supplier, campaign, initiative, channel, and GS Channel fields. Delivery Override Start Date and Delivery Override End Date are intentionally blank because they are user-entered Manual Data Editor windows rather than master-model source fields.

## Current verification boundary

The native Sheet, bound-script attachment, script syntax, manifest, visible tabs, input cells, result headers, formatting, and corrected complete-source query were verified on July 14, 2026. The final live menu proof must confirm that `QUAN`, `Columbus Circle DOOH`, and `ccdooh` return the same package before this change is called verified end to end.

## Safe maintenance

- Edit the Apps Script source in [`apps_script/Code.js`](/Users/eugenetsenter/Looker_clonedRepo/looker_personal/master_data_model/model/package_lookup_sheet/apps_script/Code.js).
- Keep the query read-only. Do not add BigQuery statements that create, replace, update, or delete warehouse objects.
- Keep the result order aligned with the 22 visible Sheet headers before pushing code.
- Push from the `apps_script/` folder with `clasp push` only after checking the bound spreadsheet ID in [`.clasp.json`](/Users/eugenetsenter/Looker_clonedRepo/looker_personal/master_data_model/model/package_lookup_sheet/apps_script/.clasp.json).
