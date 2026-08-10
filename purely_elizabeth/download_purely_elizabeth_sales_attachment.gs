/**
 * Downloads attachments from the newest Gmail message sent by Tess with the
 * exact subject "Purely Elizabeth Sales Data" into a dedicated Drive folder.
 * Spreadsheet attachments are also converted temporarily to Google Sheets so
 * the newest valid "Data thru M.D.YY" tab can be saved as a cleaned CSV in
 * Drive and uploaded without overwriting to both the archive root and a
 * schema-controlled Cloud Storage prefix read by a BigQuery scheduled query.
 *
 * Safe use:
 * 1. Copy this file into a Google Apps Script project at script.google.com.
 * 2. Copy the companion appsscript.json manifest into the same project.
 * 3. Link Apps Script to GCP project number 671028410185.
 * 4. Run downloadLatestPurelyElizabethSalesAttachment and approve access.
 *
 * The script does not delete, move, label, or mark email as read. It records each
 * saved attachment in Apps Script properties so reruns do not create duplicates.
 */

const PURELY_ELIZABETH_EMAIL_CONFIG = Object.freeze({
  sender: 'tess.vargas@giantspoon.com',
  subject: 'Purely Elizabeth Sales Data',
  driveFolderName: 'Purely Elizabeth Sales Data',
  maximumThreadsToInspect: 50,
  datedTabNamePrefix: 'Data thru',
  headerFirstCell: 'Geography',
  gcsBucketName: 'looker-studio-pro-452620-pe',
  gcsObjectPrefix: '',
  gcsBigQueryReadyPrefix: 'bigquery_ready',
});

/**
 * Source-faithful SPINS field contract. Only files matching this exact contract
 * are copied into the GCS prefix read by the scheduled BigQuery refresh.
 */
const PURELY_ELIZABETH_SOURCE_COLUMNS = Object.freeze([
  { sourceName: 'Geography', fieldName: 'geography', targetType: 'STRING', required: true },
  { sourceName: 'Time Period', fieldName: 'time_period', targetType: 'STRING' },
  {
    sourceName: 'Time Period End Date',
    fieldName: 'time_period_end_date',
    targetType: 'DATE',
    required: true,
  },
  { sourceName: 'Product Universe', fieldName: 'product_universe', targetType: 'STRING' },
  {
    sourceName: 'Product Level',
    fieldName: 'product_level',
    targetType: 'STRING',
    required: true,
  },
  { sourceName: 'Category', fieldName: 'category', targetType: 'STRING' },
  { sourceName: 'Subcategory', fieldName: 'subcategory', targetType: 'STRING' },
  { sourceName: 'Brand', fieldName: 'brand', targetType: 'STRING' },
  { sourceName: 'UPC', fieldName: 'upc', targetType: 'STRING' },
  { sourceName: 'Description', fieldName: 'description', targetType: 'STRING' },
  { sourceName: 'FLAVOR', fieldName: 'flavor', targetType: 'STRING' },
  { sourceName: 'NFP - PROTEIN', fieldName: 'nfp_protein', targetType: 'STRING' },
  { sourceName: 'PRODUCT TYPE', fieldName: 'product_type', targetType: 'STRING' },
  {
    sourceName: 'PACKAGING TYPE - PRIMARY',
    fieldName: 'packaging_type_primary',
    targetType: 'STRING',
  },
  { sourceName: 'SIZE', fieldName: 'size', targetType: 'STRING' },
  { sourceName: 'Dollars', fieldName: 'dollars', targetType: 'FLOAT64', required: true },
  { sourceName: 'Units', fieldName: 'units', targetType: 'FLOAT64', required: true },
  {
    sourceName: 'Avg % ACV',
    fieldName: 'avg_percent_acv',
    targetType: 'FLOAT64',
    required: true,
  },
  { sourceName: 'TDP', fieldName: 'tdp', targetType: 'FLOAT64', required: true },
  {
    sourceName: '# of Stores Selling',
    fieldName: 'number_of_stores_selling',
    targetType: 'INT64',
    required: true,
  },
  {
    sourceName: 'Average Weekly Dollars Per Store Selling Per Item',
    fieldName: 'average_weekly_dollars_per_store_selling_per_item',
    targetType: 'FLOAT64',
  },
  {
    sourceName: 'Average Weekly Units Per Store Selling Per Item',
    fieldName: 'average_weekly_units_per_store_selling_per_item',
    targetType: 'FLOAT64',
  },
]);

/**
 * Safely diagnoses the Apps Script identity, OAuth scope, and effective GCS
 * permissions without creating, changing, or deleting a Cloud Storage object.
 * Run this function first when a GCS upload returns HTTP 403.
 */
function diagnosePurelyElizabethGcsAuthorization() {
  const result = getGcsAuthorizationDiagnostic_(
    PURELY_ELIZABETH_EMAIL_CONFIG.gcsBucketName,
  );
  console.log(JSON.stringify(result, null, 2));
  return result;
}

/**
 * Finds the newest matching message and saves any attachments not saved before.
 * Run this function from the Google Apps Script editor.
 *
 * @return {{status: string, savedFiles: string[], skippedFiles: string[],
 *     createdCsvFiles: string[], skippedCsvFiles: string[], ignoredAttachments: string[],
 *     uploadedGcsObjects: string[], verifiedExistingGcsObjects: string[],
 *     uploadedBigQueryReadyObjects: string[], verifiedExistingBigQueryReadyObjects: string[],
 *     selectionNotes: string[], cleanupWarnings: string[], folderUrl: string}}
 *     A summary that is also written to the Apps Script execution log.
 */
function downloadLatestPurelyElizabethSalesAttachment() {
  const config = PURELY_ELIZABETH_EMAIL_CONFIG;
  const gmailQuery = [
    `from:${config.sender}`,
    `subject:"${config.subject}"`,
    'has:attachment',
  ].join(' ');

  const matchingMessages = findMatchingMessages_(gmailQuery, config);

  if (matchingMessages.length === 0) {
    throw new Error(
      `No email matched sender "${config.sender}" and exact subject "${config.subject}".`,
    );
  }

  matchingMessages.sort((left, right) => right.getDate() - left.getDate());
  const newestMessage = matchingMessages[0];
  const attachments = newestMessage.getAttachments({
    includeInlineImages: false,
    includeAttachments: true,
  });

  if (attachments.length === 0) {
    throw new Error('The newest matching email did not contain a downloadable attachment.');
  }

  const destinationFolder = getOrCreateDestinationFolder_(config.driveFolderName);
  const scriptProperties = PropertiesService.getScriptProperties();
  const savedFiles = [];
  const skippedFiles = [];
  const createdCsvFiles = [];
  const skippedCsvFiles = [];
  const ignoredAttachments = [];
  const uploadedGcsObjects = [];
  const verifiedExistingGcsObjects = [];
  const uploadedBigQueryReadyObjects = [];
  const verifiedExistingBigQueryReadyObjects = [];
  const selectionNotes = [];
  const cleanupWarnings = [];

  attachments.forEach((attachment, attachmentIndex) => {
    const originalName = attachment.getName() || `attachment-${attachmentIndex + 1}`;
    const processedKey = createProcessedKey_(newestMessage.getId(), attachmentIndex, originalName);

    if (scriptProperties.getProperty(processedKey)) {
      skippedFiles.push(originalName);
    } else {
      const savedFile = destinationFolder.createFile(attachment.copyBlob()).setName(originalName);
      scriptProperties.setProperty(processedKey, savedFile.getId());
      savedFiles.push(originalName);
    }

    if (!isSupportedWorkbook_(originalName)) {
      ignoredAttachments.push(originalName);
      return;
    }

    const csvResult = exportMatchingTabsToCsv_(
      attachment,
      originalName,
      newestMessage.getId(),
      attachmentIndex,
      destinationFolder,
      scriptProperties,
      config,
    );

    createdCsvFiles.push(...csvResult.createdCsvFiles);
    skippedCsvFiles.push(...csvResult.skippedCsvFiles);
    uploadedGcsObjects.push(...csvResult.uploadedGcsObjects);
    verifiedExistingGcsObjects.push(...csvResult.verifiedExistingGcsObjects);
    uploadedBigQueryReadyObjects.push(...csvResult.uploadedBigQueryReadyObjects);
    verifiedExistingBigQueryReadyObjects.push(
      ...csvResult.verifiedExistingBigQueryReadyObjects,
    );
    selectionNotes.push(...csvResult.selectionNotes);
    cleanupWarnings.push(...csvResult.cleanupWarnings);
  });

  const result = {
    status:
      savedFiles.length > 0 || createdCsvFiles.length > 0 || uploadedGcsObjects.length > 0
        ? 'saved'
        : 'already_saved',
    savedFiles,
    skippedFiles,
    createdCsvFiles,
    skippedCsvFiles,
    ignoredAttachments,
    uploadedGcsObjects,
    verifiedExistingGcsObjects,
    uploadedBigQueryReadyObjects,
    verifiedExistingBigQueryReadyObjects,
    selectionNotes,
    cleanupWarnings,
    folderUrl: destinationFolder.getUrl(),
  };

  console.log(JSON.stringify(result, null, 2));
  return result;
}

/**
 * Converts one workbook to a temporary Google Sheet and creates one cleaned CSV
 * from its newest dated data tab. The temporary conversion is moved to Trash.
 */
function exportMatchingTabsToCsv_(
  attachment,
  originalName,
  messageId,
  attachmentIndex,
  destinationFolder,
  scriptProperties,
  config,
) {
  let temporarySpreadsheetFile;
  const cleanupWarnings = [];
  const selectionNotes = [];
  let csvResult;

  try {
    temporarySpreadsheetFile = Drive.Files.create(
      {
        name: `Temporary conversion - ${originalName}`,
        mimeType: MimeType.GOOGLE_SHEETS,
      },
      attachment.copyBlob(),
      { fields: 'id,name' },
    );

    const spreadsheet = SpreadsheetApp.openById(temporarySpreadsheetFile.id);
    const allSheets = spreadsheet.getSheets();
    const rowsBySheetName = new Map();
    const getDisplayedRows = (sheet) => {
      if (!rowsBySheetName.has(sheet.getName())) {
        rowsBySheetName.set(sheet.getName(), sheet.getDataRange().getDisplayValues());
      }

      return rowsBySheetName.get(sheet.getName());
    };

    const datedSheets = allSheets
      .map((sheet) => ({
        sheet,
        dateInfo: parseDatedTabName_(sheet.getName(), config.datedTabNamePrefix),
      }))
      .filter((candidate) => candidate.dateInfo !== null)
      .sort((left, right) => right.dateInfo.timestamp - left.dateInfo.timestamp);

    if (datedSheets.length === 0) {
      const availableTabNames = allSheets.map((sheet) => `"${sheet.getName()}"`).join(', ');
      throw new Error(
        `Workbook "${originalName}" has no tab starting with "${config.datedTabNamePrefix}" ` +
          `followed by a valid date such as "${config.datedTabNamePrefix} 7.12.26". ` +
          `Available tabs: ${availableTabNames || '(none)'}.`,
      );
    }

    const latestTimestamp = datedSheets[0].dateInfo.timestamp;
    const latestCandidates = datedSheets.filter(
      (candidate) => candidate.dateInfo.timestamp === latestTimestamp,
    );

    if (latestCandidates.length > 1) {
      throw new Error(
        `Workbook "${originalName}" has multiple tabs for the latest date ` +
          `${latestCandidates[0].dateInfo.isoDate}: ` +
          latestCandidates.map((candidate) => `"${candidate.sheet.getName()}"`).join(', ') +
          '. Rename or remove the duplicate-date tab before rerunning.',
      );
    }

    const latestDatedSheet = latestCandidates[0];
    const matchingSheets = [latestDatedSheet.sheet];
    selectionNotes.push(
      `Selected latest dated tab "${latestDatedSheet.sheet.getName()}" ` +
        `(${latestDatedSheet.dateInfo.isoDate}) from "${originalName}".`,
    );

    const createdCsvFiles = [];
    const skippedCsvFiles = [];
    const uploadedGcsObjects = [];
    const verifiedExistingGcsObjects = [];
    const uploadedBigQueryReadyObjects = [];
    const verifiedExistingBigQueryReadyObjects = [];

    matchingSheets.forEach((sheet) => {
      const csvFileName = createCsvFileName_(originalName, sheet.getName());
      const csvProcessedKey = createProcessedKey_(
        messageId,
        attachmentIndex,
        `CSV_${sheet.getName()}`,
      );

      const displayedRows = getDisplayedRows(sheet);
      const headerRowIndex = findHeaderRowIndex_(displayedRows, config.headerFirstCell);

      if (headerRowIndex === -1) {
        throw new Error(
          `Tab "${sheet.getName()}" in "${originalName}" has no row where column A is ` +
            `"${config.headerFirstCell}".`,
        );
      }

      const cleanedRows = displayedRows.slice(headerRowIndex);
      cleanedRows[0][0] = config.headerFirstCell;
      removeEmptyTrailingRows_(cleanedRows);
      const sourceProfile = validateCleanedRowsForBigQuery_(
        cleanedRows,
        latestDatedSheet.dateInfo.isoDate,
        config,
      );

      const csvBlob = Utilities.newBlob(
        rowsToCsv_(cleanedRows),
        MimeType.CSV,
        csvFileName,
      );

      if (scriptProperties.getProperty(csvProcessedKey)) {
        skippedCsvFiles.push(csvFileName);
      } else {
        const csvFile = destinationFolder.createFile(csvBlob);
        scriptProperties.setProperty(csvProcessedKey, csvFile.getId());
        createdCsvFiles.push(csvFileName);
      }

      const gcsObjectName = createGcsObjectName_(config.gcsObjectPrefix, csvFileName);
      const gcsResult = uploadCsvBlobToGcs_(
        csvBlob,
        config.gcsBucketName,
        gcsObjectName,
      );

      if (gcsResult.status === 'uploaded') {
        uploadedGcsObjects.push(gcsResult.uri);
      } else {
        verifiedExistingGcsObjects.push(gcsResult.uri);
      }

      const bigQueryReadyObjectName = createGcsObjectName_(
        config.gcsBigQueryReadyPrefix,
        `${sourceProfile.latestIsoDate}.csv`,
      );
      const bigQueryReadyResult = uploadCsvBlobToGcs_(
        csvBlob,
        config.gcsBucketName,
        bigQueryReadyObjectName,
      );

      if (bigQueryReadyResult.status === 'uploaded') {
        uploadedBigQueryReadyObjects.push(bigQueryReadyResult.uri);
      } else {
        verifiedExistingBigQueryReadyObjects.push(bigQueryReadyResult.uri);
      }
    });

    csvResult = {
      createdCsvFiles,
      skippedCsvFiles,
      uploadedGcsObjects,
      verifiedExistingGcsObjects,
      uploadedBigQueryReadyObjects,
      verifiedExistingBigQueryReadyObjects,
    };
  } catch (error) {
    if (!temporarySpreadsheetFile) {
      throw new Error(
        `Could not convert "${originalName}". Enable the Drive API under Services in ` +
          `Google Apps Script, then run the function again. Google error: ${error.message}`,
      );
    }

    throw error;
  } finally {
    if (temporarySpreadsheetFile) {
      try {
        DriveApp.getFileById(temporarySpreadsheetFile.id).setTrashed(true);
      } catch (cleanupError) {
        const warning =
          `CSV creation finished, but the temporary conversion "${temporarySpreadsheetFile.name}" ` +
          `could not be moved to Trash: ${cleanupError.message}`;
        console.warn(warning);
        cleanupWarnings.push(warning);
      }
    }
  }

  return {
    ...csvResult,
    selectionNotes,
    cleanupWarnings,
  };
}

/**
 * Uploads a CSV to GCS only when the object name is unused. If an object already
 * exists, it is accepted only when its size and MD5 match the generated CSV.
 */
function uploadCsvBlobToGcs_(csvBlob, bucketName, objectName) {
  if (!bucketName) {
    throw new Error('The GCS bucket name is empty. Set gcsBucketName in the configuration.');
  }

  const uploadUrl =
    `https://storage.googleapis.com/upload/storage/v1/b/${encodeURIComponent(bucketName)}/o` +
    `?uploadType=media&name=${encodeURIComponent(objectName)}&ifGenerationMatch=0`;
  const response = UrlFetchApp.fetch(uploadUrl, {
    method: 'post',
    headers: {
      Authorization: `Bearer ${ScriptApp.getOAuthToken()}`,
    },
    contentType: MimeType.CSV,
    payload: csvBlob.getBytes(),
    muteHttpExceptions: true,
  });
  const responseCode = response.getResponseCode();
  const gcsUri = `gs://${bucketName}/${objectName}`;

  if (responseCode >= 200 && responseCode < 300) {
    const uploadedMetadata = parseGcsJsonResponse_(response, `upload ${gcsUri}`);
    verifyGcsObjectMatchesBlob_(uploadedMetadata, csvBlob, gcsUri);
    return createVerifiedGcsResult_('uploaded', gcsUri, uploadedMetadata);
  }

  if (responseCode === 412) {
    const existingMetadata = getGcsObjectMetadata_(bucketName, objectName);
    verifyGcsObjectMatchesBlob_(existingMetadata, csvBlob, gcsUri);
    return createVerifiedGcsResult_('verified_existing', gcsUri, existingMetadata);
  }

  if (responseCode === 403) {
    let diagnosticSummary;

    try {
      const diagnostic = getGcsAuthorizationDiagnostic_(bucketName);
      diagnosticSummary =
        `Apps Script user: ${diagnostic.effectiveUserEmail || '(unavailable)'}. ` +
        `Token email: ${diagnostic.tokenEmail || '(unavailable)'}. ` +
        `Cloud Storage write scope present: ${diagnostic.hasStorageWriteScope}. ` +
        `Granted bucket permissions: ${diagnostic.grantedBucketPermissions.join(', ') || '(none)'}. ` +
        `Diagnosis: ${diagnostic.diagnosis}`;
    } catch (diagnosticError) {
      diagnosticSummary = `Authorization diagnostic also failed: ${diagnosticError.message}`;
    }

    throw new Error(
      `Cloud Storage upload authorization failed for ${gcsUri}. ${diagnosticSummary}`,
    );
  }

  throw new Error(
    `Cloud Storage upload failed for ${gcsUri} with HTTP ${responseCode}: ` +
      response.getContentText().slice(0, 500),
  );
}

/**
 * Keeps only the verified GCS identity fields needed for BigQuery lineage.
 */
function createVerifiedGcsResult_(status, gcsUri, metadata) {
  return {
    status,
    uri: gcsUri,
    generation: String(metadata.generation || ''),
    md5Hash: String(metadata.md5Hash || ''),
    size: String(metadata.size || ''),
  };
}

/**
 * Enforces the vendor file contract before a CSV can reach BigQuery.
 */
function validateCleanedRowsForBigQuery_(rows, expectedLatestIsoDate, config) {
  if (rows.length < 2) {
    throw new Error('The selected tab contains a header but no data rows.');
  }

  const expectedHeaders = PURELY_ELIZABETH_SOURCE_COLUMNS.map(
    (column) => column.sourceName,
  );
  const actualHeaders = rows[0].map((value) => String(value).trim());

  if (
    actualHeaders.length !== expectedHeaders.length ||
    expectedHeaders.some((header, index) => actualHeaders[index] !== header)
  ) {
    throw new Error(
      'The selected tab does not match the expected 22-column SPINS header. ' +
        'Expected: ' + expectedHeaders.join(' | ') +
        '. Found: ' + actualHeaders.join(' | ') + '.',
    );
  }

  const dateIndex = getSourceColumnIndex_('time_period_end_date');
  const productLevelIndex = getSourceColumnIndex_('product_level');
  const requiredIndexes = PURELY_ELIZABETH_SOURCE_COLUMNS
    .map((column, index) => ({ column, index }))
    .filter(({ column }) => column.required);
  const numericIndexes = PURELY_ELIZABETH_SOURCE_COLUMNS
    .map((column, index) => ({ column, index }))
    .filter(({ column }) => ['FLOAT64', 'INT64'].includes(column.targetType));
  const distinctDates = new Set();
  const productLevels = new Set();
  const dimensionKeys = new Set();
  let latestIsoDate = '';

  rows.slice(1).forEach((row, rowOffset) => {
    const spreadsheetRowNumber = rowOffset + 2;

    if (row.length !== expectedHeaders.length) {
      throw new Error(
        'Row ' + spreadsheetRowNumber + ' has ' + row.length +
          ' columns; expected ' + expectedHeaders.length + '.',
      );
    }

    requiredIndexes.forEach(({ column, index }) => {
      if (String(row[index] == null ? '' : row[index]).trim() === '') {
        throw new Error(
          'Row ' + spreadsheetRowNumber + ' is missing required field "' +
            column.sourceName + '".',
        );
      }
    });

    const isoDate = parseSourceDateToIso_(row[dateIndex]);
    if (!isoDate) {
      throw new Error(
        'Row ' + spreadsheetRowNumber + ' has an invalid Time Period End Date: "' +
          row[dateIndex] + '". Expected M/D/YYYY.',
      );
    }

    distinctDates.add(isoDate);
    if (!latestIsoDate || isoDate > latestIsoDate) {
      latestIsoDate = isoDate;
    }

    const productLevel = String(row[productLevelIndex]).trim();
    if (!['BRAND', 'UPC'].includes(productLevel)) {
      throw new Error(
        'Row ' + spreadsheetRowNumber + ' has unexpected Product Level "' +
          productLevel + '".',
      );
    }
    productLevels.add(productLevel);

    numericIndexes.forEach(({ column, index }) => {
      const rawValue = String(row[index] == null ? '' : row[index]).trim();
      if (!rawValue && !column.required) {
        return;
      }

      const numericValue = parseSourceNumber_(rawValue);
      if (numericValue === null) {
        throw new Error(
          'Row ' + spreadsheetRowNumber + ' has a non-numeric value in "' +
            column.sourceName + '": "' + rawValue + '".',
        );
      }
      if (column.targetType === 'INT64' && !Number.isInteger(numericValue)) {
        throw new Error(
          'Row ' + spreadsheetRowNumber + ' has a non-integer value in "' +
            column.sourceName + '": "' + rawValue + '".',
        );
      }
    });

    const dimensionKey = JSON.stringify(
      row.slice(0, 15).map((value) => String(value == null ? '' : value).trim()),
    );
    if (dimensionKeys.has(dimensionKey)) {
      throw new Error(
        'Row ' + spreadsheetRowNumber +
          " duplicates an earlier row's 15-field dimension key.",
      );
    }
    dimensionKeys.add(dimensionKey);
  });

  if (latestIsoDate !== expectedLatestIsoDate) {
    throw new Error(
      'The selected tab is named for ' + expectedLatestIsoDate +
        ', but its latest data date is ' + latestIsoDate + '.',
    );
  }
  if (!productLevels.has('BRAND') || !productLevels.has('UPC')) {
    throw new Error('The selected tab must contain both BRAND and UPC rows.');
  }

  return {
    rowCount: rows.length - 1,
    distinctWeeks: distinctDates.size,
    latestIsoDate,
    columnCount: expectedHeaders.length,
  };
}

/**
 * Returns a configured source column's zero-based CSV position.
 */
function getSourceColumnIndex_(fieldName) {
  const index = PURELY_ELIZABETH_SOURCE_COLUMNS.findIndex(
    (column) => column.fieldName === fieldName,
  );

  if (index === -1) {
    throw new Error('Unknown configured source field "' + fieldName + '".');
  }
  return index;
}

/**
 * Parses the displayed SPINS date without locale-dependent JavaScript parsing.
 */
function parseSourceDateToIso_(value) {
  const match = String(value == null ? '' : value)
    .trim()
    .match(/^(\d{1,2})\/(\d{1,2})\/(\d{4})$/);

  if (!match) {
    return null;
  }

  const month = Number(match[1]);
  const day = Number(match[2]);
  const year = Number(match[3]);
  const parsedDate = new Date(Date.UTC(year, month - 1, day));

  if (
    parsedDate.getUTCFullYear() !== year ||
    parsedDate.getUTCMonth() !== month - 1 ||
    parsedDate.getUTCDate() !== day
  ) {
    return null;
  }

  return (
    String(year).padStart(4, '0') + '-' +
    String(month).padStart(2, '0') + '-' +
    String(day).padStart(2, '0')
  );
}

/**
 * Parses source metrics while allowing standard commas, currency, and percent signs.
 */
function parseSourceNumber_(value) {
  const normalized = String(value == null ? '' : value)
    .trim()
    .replace(/,/g, '')
    .replace(/\$/g, '')
    .replace(/%/g, '');

  if (!normalized) {
    return null;
  }

  const parsed = Number(normalized);
  return Number.isFinite(parsed) ? parsed : null;
}

/**
 * Checks the exact OAuth token used by Apps Script without exposing that token.
 */
function getGcsAuthorizationDiagnostic_(bucketName) {
  const accessToken = ScriptApp.getOAuthToken();
  const tokenInfoResponse = UrlFetchApp.fetch(
    `https://oauth2.googleapis.com/tokeninfo?access_token=${encodeURIComponent(accessToken)}`,
    { muteHttpExceptions: true },
  );

  if (tokenInfoResponse.getResponseCode() !== 200) {
    throw new Error(
      `Google token inspection returned HTTP ${tokenInfoResponse.getResponseCode()}.`,
    );
  }

  const tokenInfo = parseGcsJsonResponse_(tokenInfoResponse, 'inspect the Apps Script token');
  const grantedScopes = String(tokenInfo.scope || '')
    .split(/\s+/)
    .filter(Boolean);
  const acceptedStorageWriteScopes = [
    'https://www.googleapis.com/auth/devstorage.read_write',
    'https://www.googleapis.com/auth/devstorage.full_control',
    'https://www.googleapis.com/auth/cloud-platform',
  ];
  const hasStorageWriteScope = acceptedStorageWriteScopes.some((scope) =>
    grantedScopes.includes(scope),
  );
  const permissionUrl =
    `https://storage.googleapis.com/storage/v1/b/${encodeURIComponent(bucketName)}` +
    '/iam/testPermissions?permissions=storage.objects.create&permissions=storage.objects.get';
  const permissionResponse = UrlFetchApp.fetch(permissionUrl, {
    method: 'get',
    headers: {
      Authorization: `Bearer ${accessToken}`,
    },
    muteHttpExceptions: true,
  });
  let grantedBucketPermissions = [];

  if (permissionResponse.getResponseCode() === 200) {
    const permissionResult = parseGcsJsonResponse_(
      permissionResponse,
      `test permissions on gs://${bucketName}`,
    );
    grantedBucketPermissions = permissionResult.permissions || [];
  }

  const hasCreatePermission = grantedBucketPermissions.includes('storage.objects.create');
  let diagnosis;

  if (!hasStorageWriteScope) {
    diagnosis =
      'The live Apps Script token is missing a Cloud Storage write scope. Replace the live ' +
      'appsscript.json manifest, save it, and authorize the newly requested permissions.';
  } else if (!hasCreatePermission) {
    diagnosis =
      'The token has a Cloud Storage write scope, but its Google identity does not have ' +
      `storage.objects.create on gs://${bucketName}.`;
  } else {
    diagnosis =
      'The token has a Cloud Storage write scope and effective create permission. Retry the ' +
      'upload; if it still fails, preserve the complete new error for API-level diagnosis.';
  }

  return {
    effectiveUserEmail: Session.getEffectiveUser().getEmail(),
    tokenEmail: tokenInfo.email || '',
    hasStorageWriteScope,
    grantedStorageScopes: grantedScopes.filter((scope) =>
      acceptedStorageWriteScopes.includes(scope),
    ),
    bucketPermissionCheckHttpStatus: permissionResponse.getResponseCode(),
    grantedBucketPermissions,
    diagnosis,
  };
}

/**
 * Reads metadata for one existing GCS object using the Apps Script user's token.
 */
function getGcsObjectMetadata_(bucketName, objectName) {
  const metadataUrl =
    `https://storage.googleapis.com/storage/v1/b/${encodeURIComponent(bucketName)}/o/` +
    encodeURIComponent(objectName);
  const response = UrlFetchApp.fetch(metadataUrl, {
    method: 'get',
    headers: {
      Authorization: `Bearer ${ScriptApp.getOAuthToken()}`,
    },
    muteHttpExceptions: true,
  });

  if (response.getResponseCode() !== 200) {
    throw new Error(
      `Could not verify gs://${bucketName}/${objectName}; metadata returned HTTP ` +
        `${response.getResponseCode()}: ${response.getContentText().slice(0, 500)}`,
    );
  }

  return parseGcsJsonResponse_(response, `read metadata for gs://${bucketName}/${objectName}`);
}

/**
 * Confirms that GCS stored exactly the same bytes as the generated CSV.
 */
function verifyGcsObjectMatchesBlob_(metadata, blob, gcsUri) {
  const bytes = blob.getBytes();
  const expectedMd5 = Utilities.base64Encode(
    Utilities.computeDigest(Utilities.DigestAlgorithm.MD5, bytes),
  );
  const sizeMatches = String(metadata.size) === String(bytes.length);
  const md5Matches = metadata.md5Hash === expectedMd5;

  if (!sizeMatches || !md5Matches) {
    throw new Error(
      `A different object already exists at ${gcsUri}. The script did not overwrite it. ` +
        `Expected ${bytes.length} bytes with MD5 ${expectedMd5}; found ` +
        `${metadata.size || 'unknown'} bytes with MD5 ${metadata.md5Hash || 'unavailable'}.`,
    );
  }
}

/**
 * Parses a Cloud Storage JSON response with a reader-friendly error.
 */
function parseGcsJsonResponse_(response, actionDescription) {
  try {
    return JSON.parse(response.getContentText());
  } catch (error) {
    throw new Error(`Could not parse the Cloud Storage response for ${actionDescription}.`);
  }
}

/**
 * Joins an optional GCS prefix and filename without accidental leading slashes.
 */
function createGcsObjectName_(prefix, fileName) {
  const cleanedPrefix = String(prefix || '').replace(/^\/+|\/+$/g, '');
  return cleanedPrefix ? `${cleanedPrefix}/${fileName}` : fileName;
}

/**
 * Parses a leading "Data thru" date without relying on locale-specific Date parsing.
 */
function parseDatedTabName_(tabName, requiredPrefix) {
  const normalizedName = String(tabName || '')
    .normalize('NFKC')
    .replace(/\u00a0/g, ' ')
    .replace(/\s+/g, ' ')
    .trim();
  const normalizedPrefix = String(requiredPrefix || '').trim();

  if (!normalizedName.toLowerCase().startsWith(normalizedPrefix.toLowerCase())) {
    return null;
  }

  const dateText = normalizedName.slice(normalizedPrefix.length).trim();
  const dateMatch = dateText.match(/^(\d{1,2})[.\/-](\d{1,2})[.\/-](\d{2}|\d{4})(?:\b|$)/);

  if (!dateMatch) {
    return null;
  }

  const month = Number(dateMatch[1]);
  const day = Number(dateMatch[2]);
  const suppliedYear = Number(dateMatch[3]);
  const year = dateMatch[3].length === 2 ? 2000 + suppliedYear : suppliedYear;
  const timestamp = Date.UTC(year, month - 1, day);
  const parsedDate = new Date(timestamp);

  if (
    parsedDate.getUTCFullYear() !== year ||
    parsedDate.getUTCMonth() !== month - 1 ||
    parsedDate.getUTCDate() !== day
  ) {
    return null;
  }

  return {
    timestamp,
    isoDate: `${String(year).padStart(4, '0')}-${String(month).padStart(2, '0')}-${String(day).padStart(2, '0')}`,
  };
}

/**
 * Limits conversion to workbook formats supported by this workflow.
 */
function isSupportedWorkbook_(fileName) {
  return /\.(xlsx|xls|ods)$/i.test(fileName);
}

/**
 * Finds the first row whose first displayed cell is the required header.
 */
function findHeaderRowIndex_(rows, requiredHeader) {
  const normalizedHeader = requiredHeader.trim().toLowerCase();

  return rows.findIndex(
    (row) => String(row[0] || '').trim().toLowerCase() === normalizedHeader,
  );
}

/**
 * Removes only fully blank rows at the bottom; blank rows inside the data remain.
 */
function removeEmptyTrailingRows_(rows) {
  while (
    rows.length > 1 &&
    rows[rows.length - 1].every((cell) => String(cell).trim() === '')
  ) {
    rows.pop();
  }
}

/**
 * Creates standards-compliant CSV text, including commas, quotes, and line breaks.
 */
function rowsToCsv_(rows) {
  return rows
    .map((row) =>
      row
        .map((cell) => `"${String(cell == null ? '' : cell).replace(/"/g, '""')}"`)
        .join(','),
    )
    .join('\r\n');
}

/**
 * Preserves the workbook and tab names while removing filename-hostile characters.
 */
function createCsvFileName_(workbookName, tabName) {
  const workbookBaseName = workbookName.replace(/\.[^.]+$/, '');
  const safeTabName = tabName.replace(/[\\/:*?"<>|#%]/g, '-').trim();

  return `${workbookBaseName} - ${safeTabName}.csv`;
}

/**
 * Uses Gmail search for a shortlist, then enforces the exact subject in code.
 * Gmail can return every message in a matching thread, so each message is checked.
 */
function findMatchingMessages_(gmailQuery, config) {
  const senderText = config.sender.toLowerCase();
  const threads = GmailApp.search(gmailQuery, 0, config.maximumThreadsToInspect);

  return threads
    .flatMap((thread) => thread.getMessages())
    .filter((message) => message.getSubject() === config.subject)
    .filter((message) => message.getFrom().toLowerCase().includes(senderText));
}

/**
 * Reuses the same Drive folder on later runs, even if it is moved elsewhere.
 */
function getOrCreateDestinationFolder_(folderName) {
  const scriptProperties = PropertiesService.getScriptProperties();
  const savedFolderId = scriptProperties.getProperty('PURELY_ELIZABETH_DRIVE_FOLDER_ID');

  if (savedFolderId) {
    try {
      return DriveApp.getFolderById(savedFolderId);
    } catch (error) {
      console.warn('The saved Drive folder is unavailable; a new folder will be created.');
    }
  }

  const rootFolder = DriveApp.getRootFolder();
  const existingFolders = rootFolder.getFoldersByName(folderName);
  const destinationFolder = existingFolders.hasNext()
    ? existingFolders.next()
    : rootFolder.createFolder(folderName);

  scriptProperties.setProperty('PURELY_ELIZABETH_DRIVE_FOLDER_ID', destinationFolder.getId());
  return destinationFolder;
}

/**
 * Creates a compact, repeatable property key for one attachment on one message.
 */
function createProcessedKey_(messageId, attachmentIndex, attachmentName) {
  const nameDigest = Utilities.base64EncodeWebSafe(
    Utilities.computeDigest(Utilities.DigestAlgorithm.SHA_256, attachmentName),
  ).slice(0, 16);

  return `PE_ATTACHMENT_${messageId}_${attachmentIndex}_${nameDigest}`;
}
