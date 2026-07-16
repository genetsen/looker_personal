/**
 * Adds a package-search menu to the Master Data Model Package Lookup Sheet.
 *
 * The script reads one yellow search cell, checks the entered text against
 * Package ID, package names, and supplier/site fields, then runs one parameterized,
 * read-only BigQuery query, and replaces only the result rows in the bound
 * spreadsheet. It does not create, update, or delete BigQuery objects.
 */

const LOOKUP_PROJECT_ID = "looker-studio-pro-452620";
const LOOKUP_SHEET_NAME = "Package Lookup";
const SEARCH_CELL = "B4";
const STATUS_CELL = "B6";
const RESULT_HEADER_ROW = 9;
const RESULT_START_ROW = 10;
const RESULT_COLUMN_COUNT = 22;
const MAX_RESULT_COUNT = 200;

function onOpen() {
  SpreadsheetApp.getUi()
    .createMenu("Package Lookup")
    .addItem("Search packages", "searchPackages")
    .addItem("Clear search and results", "clearPackageSearch")
    .addToUi();
}

function searchPackages() {
  const sheet = getLookupSheet_();
  const searchText = normalizeSearchValue_(sheet.getRange(SEARCH_CELL).getDisplayValue());

  if (!searchText) {
    setStatus_(sheet, "Enter part of a Package ID or package name before running the lookup.", "#FCE4D6", "#843C0C");
    return;
  }

  setStatus_(sheet, "Searching the live package lookup…", "#D9EAF7", "#1F4E78");
  SpreadsheetApp.flush();

  try {
    clearResultRows_(sheet);
    const queryResults = runLookupQuery_(searchText);
    const outputRows = convertBigQueryRows_(queryResults);

    if (outputRows.length === 0) {
      setStatus_(sheet, "No matching packages were found. Try a shorter part of the ID or name.", "#FCE4D6", "#843C0C");
      return;
    }

    ensureResultCapacity_(sheet, outputRows.length);
    const outputRange = sheet.getRange(RESULT_START_ROW, 1, outputRows.length, RESULT_COLUMN_COUNT);
    outputRange.setValues(outputRows);
    formatResultRows_(sheet, outputRows.length);

    const limitNote = outputRows.length === MAX_RESULT_COUNT
      ? ` ${MAX_RESULT_COUNT} results are shown; use a more specific search to narrow the results.`
      : "";
    setStatus_(
      sheet,
      `Found ${outputRows.length} matching package${outputRows.length === 1 ? "" : "s"}.${limitNote}`,
      "#E2F0D9",
      "#385723",
    );
  } catch (error) {
    setStatus_(sheet, `Search failed: ${error.message}`, "#F4CCCC", "#9C0006");
    throw error;
  }
}

function clearPackageSearch() {
  const sheet = getLookupSheet_();
  sheet.getRange(SEARCH_CELL).clearContent();
  clearResultRows_(sheet);
  setStatus_(sheet, "Ready. Enter a search value and use the Package Lookup menu.", "#E2F0D9", "#385723");
}

function runLookupQuery_(searchText) {
  const query = `
    WITH matching_rows AS (
      SELECT *
      FROM \`looker-studio-pro-452620.master_stg.data_model_clustered_by_advertiser_qa\`
      WHERE CONTAINS_SUBSTR(LOWER(COALESCE(_package_id, '')), LOWER(@search_text))
        OR CONTAINS_SUBSTR(LOWER(COALESCE(_package_name, '')), LOWER(@search_text))
        OR CONTAINS_SUBSTR(LOWER(COALESCE(_package_name_friendly, '')), LOWER(@search_text))
        OR CONTAINS_SUBSTR(LOWER(COALESCE(_supplier_code, '')), LOWER(@search_text))
        OR CONTAINS_SUBSTR(LOWER(COALESCE(_supplier_name, '')), LOWER(@search_text))
    )
    SELECT
      _package_id AS package_id,
      ARRAY_AGG(_package_name_friendly IGNORE NULLS ORDER BY _date DESC LIMIT 1)[SAFE_OFFSET(0)] AS package_name_friendly,
      MIN(_start_date) AS current_flight_start_date,
      MAX(_end_date) AS current_flight_end_date,
      MAX(qa_pkg_est_spend_doNotSum) AS current_planned_spend,
      MAX(qa_pkg_est_impressions_doNotSum) AS current_planned_impressions,
      MAX(qa_pkg_act_spend_doNotSum) AS current_spend,
      MAX(qa_pkg_act_impressions_doNotSum) AS current_impressions,
      MAX(qa_pkg_act_clicks_doNotSum) AS current_clicks,
      SUM(_video_plays) AS current_video_plays,
      SUM(_video_comps) AS current_video_comps,
      CAST(NULL AS DATE) AS delivery_override_start_date,
      CAST(NULL AS DATE) AS delivery_override_end_date,
      ARRAY_AGG(_advertiser IGNORE NULLS ORDER BY _date DESC LIMIT 1)[SAFE_OFFSET(0)] AS advertiser_name,
      ARRAY_AGG(_package_type IGNORE NULLS ORDER BY _date DESC LIMIT 1)[SAFE_OFFSET(0)] AS package_type,
      ARRAY_AGG(_channel IGNORE NULLS ORDER BY _date DESC LIMIT 1)[SAFE_OFFSET(0)] AS channel,
      ARRAY_AGG(_campaign_name IGNORE NULLS ORDER BY _date DESC LIMIT 1)[SAFE_OFFSET(0)] AS campaign_name,
      ARRAY_AGG(initiative IGNORE NULLS ORDER BY _date DESC LIMIT 1)[SAFE_OFFSET(0)] AS initiative,
      ARRAY_AGG(_supplier_code IGNORE NULLS ORDER BY _date DESC LIMIT 1)[SAFE_OFFSET(0)] AS supplier_code,
      ARRAY_AGG(_supplier_name IGNORE NULLS ORDER BY _date DESC LIMIT 1)[SAFE_OFFSET(0)] AS supplier_name,
      ARRAY_AGG(_package_name IGNORE NULLS ORDER BY _date DESC LIMIT 1)[SAFE_OFFSET(0)] AS package_name,
      ARRAY_AGG(ADIF_channel IGNORE NULLS ORDER BY _date DESC LIMIT 1)[SAFE_OFFSET(0)] AS ADIF_channel
    FROM matching_rows
    GROUP BY package_id
    ORDER BY advertiser_name, package_name_friendly, package_id
    LIMIT ${MAX_RESULT_COUNT}
  `;

  const request = {
    query,
    useLegacySql: false,
    parameterMode: "NAMED",
    queryParameters: [
      stringQueryParameter_("search_text", searchText),
    ],
  };

  let queryResults = BigQuery.Jobs.query(request, LOOKUP_PROJECT_ID);
  const jobId = queryResults.jobReference.jobId;
  const location = queryResults.jobReference.location;

  while (!queryResults.jobComplete) {
    Utilities.sleep(300);
    queryResults = BigQuery.Jobs.getQueryResults(LOOKUP_PROJECT_ID, jobId, { location });
  }

  return queryResults;
}

function stringQueryParameter_(name, value) {
  return {
    name,
    parameterType: { type: "STRING" },
    parameterValue: { value },
  };
}

function convertBigQueryRows_(queryResults) {
  const fields = (queryResults.schema && queryResults.schema.fields) || [];
  const resultRows = queryResults.rows || [];

  return resultRows.map((resultRow) => resultRow.f.map((cell, columnIndex) => {
    const value = cell.v;
    if (value === null || value === undefined) return "";

    const fieldType = fields[columnIndex] ? fields[columnIndex].type : "STRING";
    if (fieldType === "DATE") return new Date(`${value}T00:00:00`);
    if (["INTEGER", "INT64", "FLOAT", "FLOAT64", "NUMERIC", "BIGNUMERIC"].includes(fieldType)) {
      return Number(value);
    }
    return value;
  }));
}

function formatResultRows_(sheet, resultRowCount) {
  const firstRow = RESULT_START_ROW;
  const lastRow = firstRow + resultRowCount - 1;

  sheet.getRange(`C${firstRow}:D${lastRow}`).setNumberFormat("yyyy-mm-dd");
  sheet.getRange(`L${firstRow}:M${lastRow}`).setNumberFormat("yyyy-mm-dd");
  sheet.getRange(`E${firstRow}:E${lastRow}`).setNumberFormat("$#,##0.00");
  sheet.getRange(`G${firstRow}:G${lastRow}`).setNumberFormat("$#,##0.00");
  sheet.getRange(`F${firstRow}:K${lastRow}`).setNumberFormat("#,##0");
  sheet.getRange(`A${firstRow}:V${lastRow}`).setVerticalAlignment("top");
  sheet.getRange(`B${firstRow}:B${lastRow}`).setWrap(true);
  sheet.getRange(`Q${firstRow}:V${lastRow}`).setWrap(true);
}

function ensureResultCapacity_(sheet, resultRowCount) {
  const requiredLastRow = RESULT_START_ROW + resultRowCount - 1;
  if (requiredLastRow > sheet.getMaxRows()) {
    sheet.insertRowsAfter(sheet.getMaxRows(), requiredLastRow - sheet.getMaxRows());
  }
}

function clearResultRows_(sheet) {
  const resultRowCount = Math.max(sheet.getMaxRows() - RESULT_HEADER_ROW, 1);
  sheet.getRange(RESULT_START_ROW, 1, resultRowCount, RESULT_COLUMN_COUNT).clearContent();
}

function getLookupSheet_() {
  const sheet = SpreadsheetApp.getActive().getSheetByName(LOOKUP_SHEET_NAME);
  if (!sheet) throw new Error(`Missing required tab: ${LOOKUP_SHEET_NAME}`);
  return sheet;
}

function normalizeSearchValue_(value) {
  return String(value || "").trim();
}

function setStatus_(sheet, message, background, textColor) {
  sheet.getRange(STATUS_CELL)
    .setValue(message)
    .setBackground(background)
    .setFontColor(textColor);
}
