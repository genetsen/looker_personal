/**
 * ============================================================================
 * FPD DUPLICATOR LIBRARY V4  (Track 1: Shared-Drive routing + interim lockdown)
 * ============================================================================
 *
 * NEW IN V4 (deltas from v3, all clearly marked with "V4:" comments):
 *   1. Shared-Drive routing: each client's sheet lands in that client's
 *      main-drive "First Party Data" folder (CLIENT_FPD_FOLDERS), resolved
 *      BEFORE the legacy My-Drive CLIENTS traversal. Falls back to the old
 *      behavior for unmapped clients.
 *   2. Ownership via membership: setPermissions_ skips setOwner when the copy
 *      lands in a Shared Drive (Shared-Drive files are org-owned; Gene's
 *      control comes from Drive membership, not ownership).
 *   3. Archive the source dupe: after a successful publish, the working copy it
 *      was launched from is marked outdated (never the master template).
 *   4. Interim lockdown: lock the partner selection + protect helper tabs so the
 *      partner cannot repoint the scope. NOTE: this is Track-1 interim only.
 *      True cross-partner confidentiality arrives in Track 2 (per-partner
 *      BigQuery Connected-Sheets source) - do NOT share published sheets to
 *      external partners until Track 2 is in place.
 *   5. Optional opt-in partner auto-share (CELLS.PARTNER_EMAIL), confirmation-
 *      gated; blank cell = manual sharing (today's behavior).
 *
 * NEW IN V2: User confirmation dialog before duplication with option to
 * change destination folder by pasting folder URL or ID.
 *
 * A standalone Google Apps Script library for duplicating FPD spreadsheets
 * with folder routing, permissions, and centralized logging.
 *
 * DEPLOYMENT INSTRUCTIONS:
 * 1. Create a new standalone Apps Script project at script.google.com
 * 2. Paste this entire script
 * 3. Save and name the project (e.g., "FPD Duplicator Library V2")
 * 4. Deploy > New deployment > Type: Library
 * 5. Copy the Script ID (from Project Settings)
 * 6. In your spreadsheets, add this library:
 *    - Extensions > Apps Script > Libraries > Add
 *    - Paste the Script ID
 *    - Select latest version
 *    - Set identifier as "FPDLib"
 * 7. Add the stub code (see Stub.gs) to each spreadsheet
 *
 * ============================================================================
 */

// ============================================================================
// CONFIGURATION - Update these values for your environment
// ============================================================================

const CONFIG = {
  // The ID of your CLIENTS folder (from the URL)
  // https://drive.google.com/drive/folders/1uwVd0H8QWn2LRGUPwj5Z9QXmtkWvxlcB
  CLIENTS_FOLDER_ID: '1uwVd0H8QWn2LRGUPwj5Z9QXmtkWvxlcB',

  // Email address to transfer ownership to
  NEW_OWNER_EMAIL: 'gene.tsenter@giantspoon.com',

  // Organization domain for "anyone in org can edit" permission
  ORG_DOMAIN: 'giantspoon.com',

  // Config sheet cell references
  CELLS: {
    CLIENT_CODE: 'C4',      // Client code (e.g., "APO")
    PARTNER_NAME: 'C5',     // Partner name (e.g., "NYTIMES")
    CAMPAIGN_NAME: 'C6',    // Campaign name (optional, for deduplication)
    START_DATE: 'C9',       // Start date (full date, year will be extracted)
    PARTNER_EMAIL: 'C8'     // V4: optional partner email for opt-in auto-share (blank = manual)
  },

  // Sheet names
  SHEETS: {
    CONFIG: 'Config',
    CLIENT_MAPPING: 'ClientMapping',
    CUSTOM_FOLDER_PREFS: 'CustomFolderPrefs',
    LOG: 'Log'
  },

  // Subfolder name to create/use within the year folder
  DATA_FOLDER_NAME: 'First Party Data',

  // Name of the only tab to keep visible in the duplicated sheet (case-insensitive)
  VISIBLE_TAB_NAME: 'data',

  // Folder ID for creating shortcuts to new files
  // https://drive.google.com/drive/folders/1d--Bc554eBaRCr8blt1LnUYiOMHQe7jF
  SHORTCUT_FOLDER_ID: '1pqQVdROIhOkfuBLwexH00uW4eiqkb0GY',

  // Master spreadsheet ID for centralized Log and ClientMapping
  // https://docs.google.com/spreadsheets/d/1pc9gXkMhWZ0dFNeagZWjUqsKUnWebIvB3xd5IGht4w4
  // All logging and client mappings are stored here, regardless of which sheet runs the script
  MASTER_SPREADSHEET_ID: '1pc9gXkMhWZ0dFNeagZWjUqsKUnWebIvB3xd5IGht4w4',

  // V4: Client code -> the "First Party Data" folder ID inside that client's
  // MAIN Shared Drive (verified live 2026-08). Publish routes here directly,
  // bypassing the legacy My-Drive CLIENTS traversal. Add clients as needed;
  // unmapped clients fall back to the legacy folder flow. A per-client override
  // can also be set in the CustomFolderPrefs tab (client row, blank partner).
  CLIENT_FPD_FOLDERS: {
    'apo':  '1hojhJo2zzYKXvAINVf7X14AI3qWKoX_O', // Apollo / 2026 Media / First Party Data
    'oli':  '1cMkgbplZ8sPIsDHluIuIEOmbam-BhZtY', // Olipop / Reporting / First Party Data
    'pure': '12JRxmVv6N1zsHbxgSl_BoilOWLFcsFob', // Purely Elizabeth / MEASUREMENT / First Party Data
    'fmus': '1SU3Id8DdE2ApA3ofS-4OUp11HqPeOyWF', // A Diamond is Forever / 2026 / First Party Data
    'mass': '1nABcyJ33sGm2EGHlrQhBKlZO5_8MKH4l'  // MassMutual / 2025 / First Party Data
  }
};

// ============================================================================
// MAIN FUNCTION - Called by the stub script in each spreadsheet
// ============================================================================

/**
 * Main function that orchestrates the entire duplication workflow.
 * Called from spreadsheets via: FPDLib.duplicateAndSetup(SpreadsheetApp.getActiveSpreadsheet())
 * @param {Spreadsheet} ss - The spreadsheet to duplicate
 */
function duplicateAndSetup(ss) {
  const ui = SpreadsheetApp.getUi();

  // Initialize progress tracker
  const progress = new ProgressTracker_(ss, [
    'Read configuration',
    'Find client folder',
    'Find year folder',
    'Set up data folder',
    'Generate filename',
    'Confirm destination',
    'Duplicate spreadsheet',
    'Configure tabs',
    'Set permissions',
    'Create shortcut',
    'Log action'
  ]);

  try {
    // Step 1: Read and validate config values
    progress.start(0);
    const configValues = getConfigValues_(ss);
    if (!configValues) return; // Validation failed, error already shown
    progress.complete(0);

    // V4 Steps 2-4: Resolve the destination folder.
    // Prefer the client's Shared-Drive "First Party Data" folder (via
    // resolveFpdDestination_: CustomFolderPrefs exact -> client-level ->
    // CLIENT_FPD_FOLDERS map). Fall back to the legacy My-Drive CLIENTS
    // traversal only for clients that are not mapped yet.
    let dataFolder;
    let usingCustomFolder = false;

    progress.start(1);
    const mappedFolder = resolveFpdDestination_(configValues.clientCode, configValues.partnerName);

    if (mappedFolder) {
      // Mapped Shared-Drive destination - skip the legacy client/year traversal.
      dataFolder = mappedFolder;
      usingCustomFolder = true;
      progress.complete(1, 'Shared Drive: ' + mappedFolder.getName());
      progress.start(2); progress.complete(2, 'n/a (mapped destination)');
      progress.start(3); progress.complete(3, 'First Party Data (mapped)');
    } else {
      // Legacy fallback: My-Drive CLIENTS root traversal.
      const clientFolder = getClientFolder_(configValues.clientCode, ui);
      if (!clientFolder) return; // User cancelled or error
      progress.complete(1, clientFolder.getName());

      progress.start(2);
      const yearFolder = getYearFolder_(clientFolder, configValues.year, configValues.clientCode, ui);
      if (!yearFolder) return; // User cancelled or error
      progress.complete(2, yearFolder.getName());

      progress.start(3);
      const savedCustomFolder = getCustomFolderPref_(configValues.clientCode, configValues.partnerName);
      if (savedCustomFolder) {
        dataFolder = savedCustomFolder;
        usingCustomFolder = true;
        progress.complete(3, 'Using saved preference');
      } else {
        dataFolder = getOrCreateDataFolder_(yearFolder);
        progress.complete(3);
      }
    }

    // Step 5: Generate filename and handle duplicates
    progress.start(4);
    const fileName = generateFileName_(configValues, dataFolder);
    progress.complete(4, fileName);

    // Step 6: ★ NEW IN V2 ★ Confirm destination with option to change
    progress.start(5);
    const confirmation = confirmDestination_(fileName, dataFolder, usingCustomFolder, ui);
    if (!confirmation) return; // User cancelled

    // If user changed the folder, update our dataFolder reference and save preference
    if (confirmation.changedFolder) {
      dataFolder = confirmation.folder;
      usingCustomFolder = true;
      // Save the custom folder preference for future runs
      saveCustomFolderPref_(configValues.clientCode, configValues.partnerName, dataFolder.getId());
    }
    progress.complete(5, confirmation.changedFolder ? 'Custom location (saved)' : (usingCustomFolder ? 'Saved preference' : 'Confirmed'));

    // Step 7: Duplicate the spreadsheet
    progress.start(6);
    const originalFile = DriveApp.getFileById(ss.getId());
    const newFile = originalFile.makeCopy(fileName, dataFolder);
    const newSpreadsheet = SpreadsheetApp.openById(newFile.getId());
    const newFileUrl = newFile.getUrl();
    progress.complete(6);

    // Step 8: Hide all tabs except 'data', then V4 interim lockdown
    progress.start(7);
    hideAllTabsExceptData_(newSpreadsheet);
    // V4: lock the partner selection + protect helper tabs so the scope cannot
    // be repointed. INTERIM ONLY - real cross-partner confidentiality is Track 2.
    lockPublishedSheet_(newSpreadsheet);
    progress.complete(7);

    // Step 9: Set permissions
    progress.start(8);
    const permissionResult = setPermissions_(newFile);
    progress.complete(8, permissionResult.success ? null : '⚠️ Manual action needed');

    // Step 10: Create shortcut in designated folder
    progress.start(9);
    const shortcutId = createShortcut_(newFile, fileName);
    progress.complete(9, shortcutId ? 'Created' : 'Skipped');

    // Step 11: Log the action (including custom folder indicator and shortcut link)
    progress.start(10);
    logAction_(ss, originalFile, newFile, dataFolder, permissionResult.success, usingCustomFolder, shortcutId);
    progress.complete(10);

    // V4: Optional opt-in partner auto-share (only if a partner email was
    // provided in Config and the operator confirms the external share).
    maybeSharePartner_(newFile, configValues, ui);

    // V4: Mark the working copy this was launched from as outdated so operators
    // stop editing it and use the newly published sheet. Never touches the
    // master template.
    archiveSourceDupe_(ss, fileName);

    // Step 12: Show success dialog with link
    showSuccessDialog_(newFileUrl, fileName, permissionResult, progress.getSummary());

  } catch (error) {
    showError_('An unexpected error occurred: ' + error.message);
    console.error(error);
  }
}

// ============================================================================
// CONFIG FUNCTIONS
// ============================================================================

/**
 * Reads and validates configuration values from the Config sheet.
 * @param {Spreadsheet} ss - The spreadsheet
 * @returns {Object|null} Configuration values or null if validation fails
 */
function getConfigValues_(ss) {
  const configSheet = ss.getSheetByName(CONFIG.SHEETS.CONFIG);

  if (!configSheet) {
    showError_('Config sheet not found. Please create a sheet named "' + CONFIG.SHEETS.CONFIG + '"');
    return null;
  }

  const clientCode = configSheet.getRange(CONFIG.CELLS.CLIENT_CODE).getValue();
  const partnerName = configSheet.getRange(CONFIG.CELLS.PARTNER_NAME).getValue();
  const campaignName = configSheet.getRange(CONFIG.CELLS.CAMPAIGN_NAME).getValue();
  const startDate = configSheet.getRange(CONFIG.CELLS.START_DATE).getValue();
  // V4: optional partner email for opt-in auto-share (blank = manual sharing)
  const partnerEmail = CONFIG.CELLS.PARTNER_EMAIL
    ? configSheet.getRange(CONFIG.CELLS.PARTNER_EMAIL).getValue()
    : '';

  // Validate required fields
  const missing = [];
  if (!clientCode) missing.push('Client Code (' + CONFIG.CELLS.CLIENT_CODE + ')');
  if (!partnerName) missing.push('Partner Name (' + CONFIG.CELLS.PARTNER_NAME + ')');
  if (!startDate) missing.push('Start Date (' + CONFIG.CELLS.START_DATE + ')');

  if (missing.length > 0) {
    showError_('Missing required config values:\n• ' + missing.join('\n• '));
    return null;
  }

  // Extract year from date
  const year = extractYear_(startDate);
  if (!year) {
    // Show what we actually received for debugging
    const receivedValue = String(startDate).substring(0, 50);
    const receivedType = typeof startDate;
    showError_('Invalid date format in ' + CONFIG.CELLS.START_DATE + '.\n\nReceived: "' + receivedValue + '" (type: ' + receivedType + ')\n\nPlease ensure the cell contains a valid date.');
    return null;
  }

  return {
    clientCode: String(clientCode).trim(),
    partnerName: String(partnerName).trim(),
    campaignName: campaignName ? String(campaignName).trim() : '',
    startDate: startDate,
    year: year,
    partnerEmail: partnerEmail ? String(partnerEmail).trim() : ''
  };
}

/**
 * Extracts the year from a date value.
 * Handles Date objects, strings, and Google Sheets serial date numbers.
 * @param {Date|string|number} dateValue - The date to extract year from
 * @returns {string|null} Four-digit year or null if invalid
 */
function extractYear_(dateValue) {
  try {
    let year;

    // Check if it's a Date-like object (has getFullYear method)
    if (dateValue && typeof dateValue.getFullYear === 'function') {
      year = dateValue.getFullYear();
    } else if (typeof dateValue === 'number') {
      // Google Sheets serial date: days since December 30, 1899
      const SHEETS_EPOCH = new Date(Date.UTC(1899, 11, 30));
      const msPerDay = 24 * 60 * 60 * 1000;
      const date = new Date(SHEETS_EPOCH.getTime() + (dateValue * msPerDay));
      year = date.getFullYear();
    } else if (typeof dateValue === 'string') {
      // Try parsing as string
      const date = new Date(dateValue);
      if (!isNaN(date.getTime())) {
        year = date.getFullYear();
      }
    }

    // Sanity check: year should be reasonable (1900-2100)
    if (year && year >= 1900 && year <= 2100) {
      return year.toString();
    }

    return null;
  } catch (e) {
    return null;
  }
}

// ============================================================================
// FOLDER RESOLUTION FUNCTIONS
// ============================================================================

/**
 * Resolves the client code to a folder in the CLIENTS directory.
 * Tries: 1) Direct match, 2) ClientMapping lookup, 3) User prompt
 * @param {string} clientCode - The client code from config
 * @param {Ui} ui - The UI object for dialogs
 * @returns {Folder|null} The client folder or null if cancelled
 */
function getClientFolder_(clientCode, ui) {
  const clientsFolder = DriveApp.getFolderById(CONFIG.CLIENTS_FOLDER_ID);
  const allClientFolders = getFolderList_(clientsFolder);

  // Try 1: Direct case-insensitive match
  const directMatch = findFolderCaseInsensitive_(allClientFolders, clientCode);
  if (directMatch) {
    return directMatch;
  }

  // Try 2: Check ClientMapping
  const mapping = getClientMapping_();
  const mappedName = mapping[clientCode.toLowerCase()];
  if (mappedName) {
    const mappedMatch = findFolderCaseInsensitive_(allClientFolders, mappedName);
    if (mappedMatch) {
      return mappedMatch;
    }
  }

  // Try 3: Prompt user with dropdown
  const selectedFolder = showFolderDropdown_(
    allClientFolders,
    'Client Folder Not Found',
    'Could not find a folder for client "' + clientCode + '".\n\nPlease select the correct client folder:',
    ui
  );

  // If user selected a folder, save the mapping for future use
  if (selectedFolder) {
    saveClientMapping_(clientCode, selectedFolder.getName());
  }

  return selectedFolder;
}

/**
 * Gets the year folder within a client folder.
 * If not found, prompts user to select from existing year folders.
 * @param {Folder} clientFolder - The client folder
 * @param {string} year - The year to find
 * @param {string} clientCode - The client code (for saving mapping)
 * @param {Ui} ui - The UI object for dialogs
 * @returns {Folder|null} The year folder or null if cancelled
 */
function getYearFolder_(clientFolder, year, clientCode, ui) {
  const yearFolders = getFolderList_(clientFolder);
  const clientFolderName = clientFolder.getName();

  // Try 1: Direct match for the year
  const yearFolder = findFolderCaseInsensitive_(yearFolders, year);
  if (yearFolder) {
    return yearFolder;
  }

  // Try 2: Check for saved year mapping in ClientMapping sheet
  const mappedYearFolder = getYearMapping_(clientCode, year);
  if (mappedYearFolder) {
    const mappedMatch = findFolderCaseInsensitive_(yearFolders, mappedYearFolder);
    if (mappedMatch) {
      return mappedMatch;
    }
  }

  // Year folder doesn't exist - show dropdown of existing years
  if (yearFolders.length === 0) {
    showError_('No year folders found in "' + clientFolderName + '". Please create a folder for year "' + year + '" first.');
    return null;
  }

  // Build informative message telling user what they're selecting
  const message = 'SELECTING YEAR FOLDER FOR:\n' +
    '━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n' +
    '  Client: ' + clientCode + '\n' +
    '  Folder: ' + clientFolderName + '\n' +
    '━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n\n' +
    'Year folder "' + year + '" was not found.\n' +
    'Please select an existing year folder:';

  const selectedFolder = showFolderDropdown_(
    yearFolders,
    'Select Year Folder',
    message,
    ui
  );

  // If user selected a folder, save the mapping for future use
  if (selectedFolder) {
    saveYearMapping_(clientCode, year, selectedFolder.getName());
  }

  return selectedFolder;
}

/**
 * Gets or creates the "First Party Data" folder within the year folder.
 * @param {Folder} yearFolder - The year folder
 * @returns {Folder} The data folder
 */
function getOrCreateDataFolder_(yearFolder) {
  const existingFolders = getFolderList_(yearFolder);
  const existing = findFolderCaseInsensitive_(existingFolders, CONFIG.DATA_FOLDER_NAME);

  if (existing) {
    return existing;
  }

  // Create the folder
  return yearFolder.createFolder(CONFIG.DATA_FOLDER_NAME);
}

/**
 * Gets all subfolders of a folder as an array of {name, folder} objects.
 * @param {Folder} parentFolder - The parent folder
 * @returns {Array} Array of {name: string, folder: Folder}
 */
function getFolderList_(parentFolder) {
  const folders = [];
  const iterator = parentFolder.getFolders();

  while (iterator.hasNext()) {
    const folder = iterator.next();
    folders.push({
      name: folder.getName(),
      folder: folder
    });
  }

  // Sort alphabetically
  folders.sort((a, b) => a.name.localeCompare(b.name));
  return folders;
}

/**
 * Finds a folder by name (case-insensitive).
 * @param {Array} folderList - Array of {name, folder} objects
 * @param {string} searchName - Name to search for
 * @returns {Folder|null} The matching folder or null
 */
function findFolderCaseInsensitive_(folderList, searchName) {
  const searchLower = searchName.toLowerCase();
  const match = folderList.find(f => f.name.toLowerCase() === searchLower);
  return match ? match.folder : null;
}

// ============================================================================
// CLIENT MAPPING FUNCTIONS
// ============================================================================

/**
 * Gets or creates the ClientMapping sheet in the MASTER spreadsheet.
 * Sheet structure:
 *   Column A: Client Code
 *   Column B: Client Folder Name
 *   Column C: Year
 *   Column D: Year Folder Name
 * @returns {Sheet} The ClientMapping sheet
 */
function getOrCreateMappingSheet_() {
  const masterSpreadsheet = SpreadsheetApp.openById(CONFIG.MASTER_SPREADSHEET_ID);
  let mappingSheet = masterSpreadsheet.getSheetByName(CONFIG.SHEETS.CLIENT_MAPPING);

  // Create ClientMapping sheet if it doesn't exist
  if (!mappingSheet) {
    mappingSheet = masterSpreadsheet.insertSheet(CONFIG.SHEETS.CLIENT_MAPPING);
    mappingSheet.getRange('A1:D1').setValues([['Client Code', 'Client Folder', 'Year', 'Year Folder']]);
    mappingSheet.getRange('A1:D1').setFontWeight('bold');
  }

  return mappingSheet;
}

/**
 * Reads the ClientMapping sheet from the MASTER spreadsheet.
 * Returns an object with client folder mappings.
 * @returns {Object} Mapping of lowercase client codes to folder names
 */
function getClientMapping_() {
  const masterSpreadsheet = SpreadsheetApp.openById(CONFIG.MASTER_SPREADSHEET_ID);
  const mappingSheet = masterSpreadsheet.getSheetByName(CONFIG.SHEETS.CLIENT_MAPPING);

  if (!mappingSheet) {
    return {};
  }

  const data = mappingSheet.getDataRange().getValues();
  const mapping = {};

  // Skip header row (start at index 1)
  // Column A = Client Code, Column B = Client Folder
  for (let i = 1; i < data.length; i++) {
    const code = data[i][0];
    const clientFolder = data[i][1];

    if (code && clientFolder) {
      mapping[String(code).toLowerCase().trim()] = String(clientFolder).trim();
    }
  }

  return mapping;
}

/**
 * Reads year folder mappings from the MASTER spreadsheet.
 * @param {string} clientCode - The client code to look up
 * @param {string} year - The year to look up
 * @returns {string|null} The mapped year folder name, or null if not found
 */
function getYearMapping_(clientCode, year) {
  const masterSpreadsheet = SpreadsheetApp.openById(CONFIG.MASTER_SPREADSHEET_ID);
  const mappingSheet = masterSpreadsheet.getSheetByName(CONFIG.SHEETS.CLIENT_MAPPING);

  if (!mappingSheet) {
    return null;
  }

  const data = mappingSheet.getDataRange().getValues();
  const codeLower = clientCode.toLowerCase();

  // Skip header row (start at index 1)
  // Column A = Client Code, Column C = Year, Column D = Year Folder
  for (let i = 1; i < data.length; i++) {
    const rowCode = String(data[i][0]).toLowerCase().trim();
    const rowYear = String(data[i][2]).trim();
    const yearFolder = data[i][3];

    if (rowCode === codeLower && rowYear === year && yearFolder) {
      return String(yearFolder).trim();
    }
  }

  return null;
}

/**
 * Saves a client code to folder name mapping to the ClientMapping sheet in MASTER spreadsheet.
 * Creates or updates the row for this client code.
 * @param {string} clientCode - The client code to map
 * @param {string} clientFolder - The client folder name
 */
function saveClientMapping_(clientCode, clientFolder) {
  const mappingSheet = getOrCreateMappingSheet_();
  const data = mappingSheet.getDataRange().getValues();
  const codeLower = clientCode.toLowerCase();

  // Check if this client code already exists
  for (let i = 1; i < data.length; i++) {
    if (String(data[i][0]).toLowerCase().trim() === codeLower) {
      // Update existing row - set Client Folder (column B)
      mappingSheet.getRange(i + 1, 2).setValue(clientFolder);
      return;
    }
  }

  // Append new row with client code and folder
  mappingSheet.appendRow([clientCode, clientFolder, '', '']);
}

/**
 * Saves a year folder mapping for a client to the ClientMapping sheet in MASTER spreadsheet.
 * @param {string} clientCode - The client code
 * @param {string} year - The year from the config (e.g., "2026")
 * @param {string} yearFolder - The actual year folder name (e.g., "2025-2026")
 */
function saveYearMapping_(clientCode, year, yearFolder) {
  // Only save if they're different
  if (year === yearFolder) {
    return;
  }

  const mappingSheet = getOrCreateMappingSheet_();
  const data = mappingSheet.getDataRange().getValues();
  const codeLower = clientCode.toLowerCase();

  // Check if this client code already exists
  for (let i = 1; i < data.length; i++) {
    if (String(data[i][0]).toLowerCase().trim() === codeLower) {
      // Update existing row - set Year (column C) and Year Folder (column D)
      mappingSheet.getRange(i + 1, 3, 1, 2).setValues([[year, yearFolder]]);
      return;
    }
  }

  // Append new row with client code and year mapping
  mappingSheet.appendRow([clientCode, '', year, yearFolder]);
}

// ============================================================================
// CUSTOM FOLDER PREFERENCE FUNCTIONS
// ============================================================================

/**
 * Gets or creates the CustomFolderPrefs sheet in the MASTER spreadsheet.
 * Sheet structure:
 *   Column A: Client Code
 *   Column B: Partner Name
 *   Column C: Folder ID
 *   Column D: Folder Path (for display)
 *   Column E: Last Used
 * @returns {Sheet} The CustomFolderPrefs sheet
 */
function getOrCreateCustomFolderPrefsSheet_() {
  const masterSpreadsheet = SpreadsheetApp.openById(CONFIG.MASTER_SPREADSHEET_ID);
  let prefsSheet = masterSpreadsheet.getSheetByName(CONFIG.SHEETS.CUSTOM_FOLDER_PREFS);

  if (!prefsSheet) {
    prefsSheet = masterSpreadsheet.insertSheet(CONFIG.SHEETS.CUSTOM_FOLDER_PREFS);
    prefsSheet.getRange('A1:E1').setValues([['Client Code', 'Partner Name', 'Folder ID', 'Folder Path', 'Last Used']]);
    prefsSheet.getRange('A1:E1').setFontWeight('bold');
    prefsSheet.setColumnWidth(3, 300); // Folder ID
    prefsSheet.setColumnWidth(4, 300); // Folder Path
  }

  return prefsSheet;
}

/**
 * Gets the saved custom folder preference for a client/partner combination.
 * @param {string} clientCode - The client code
 * @param {string} partnerName - The partner name
 * @returns {Folder|null} The saved folder or null if none saved
 */
function getCustomFolderPref_(clientCode, partnerName) {
  const masterSpreadsheet = SpreadsheetApp.openById(CONFIG.MASTER_SPREADSHEET_ID);
  const prefsSheet = masterSpreadsheet.getSheetByName(CONFIG.SHEETS.CUSTOM_FOLDER_PREFS);

  if (!prefsSheet) {
    return null;
  }

  const data = prefsSheet.getDataRange().getValues();
  const clientLower = clientCode.toLowerCase();
  const partnerLower = partnerName.toLowerCase();

  // Skip header row (start at index 1)
  for (let i = 1; i < data.length; i++) {
    const rowClient = String(data[i][0]).toLowerCase().trim();
    const rowPartner = String(data[i][1]).toLowerCase().trim();
    const folderId = data[i][2];

    if (rowClient === clientLower && rowPartner === partnerLower && folderId) {
      try {
        // Verify folder still exists and is accessible
        const folder = DriveApp.getFolderById(folderId);
        folder.getName(); // This will throw if no access
        return folder;
      } catch (e) {
        // Folder no longer accessible, return null
        console.warn('Saved custom folder no longer accessible: ' + folderId);
        return null;
      }
    }
  }

  return null;
}

/**
 * Saves a custom folder preference for a client/partner combination.
 * @param {string} clientCode - The client code
 * @param {string} partnerName - The partner name
 * @param {string} folderId - The folder ID to save
 */
function saveCustomFolderPref_(clientCode, partnerName, folderId) {
  const prefsSheet = getOrCreateCustomFolderPrefsSheet_();
  const data = prefsSheet.getDataRange().getValues();
  const clientLower = clientCode.toLowerCase();
  const partnerLower = partnerName.toLowerCase();

  // Get folder path for display
  let folderPath = '';
  try {
    const folder = DriveApp.getFolderById(folderId);
    folderPath = getFolderPath_(folder);
  } catch (e) {
    folderPath = '(unknown)';
  }

  // Check if this client/partner already exists
  for (let i = 1; i < data.length; i++) {
    const rowClient = String(data[i][0]).toLowerCase().trim();
    const rowPartner = String(data[i][1]).toLowerCase().trim();

    if (rowClient === clientLower && rowPartner === partnerLower) {
      // Update existing row
      prefsSheet.getRange(i + 1, 3, 1, 3).setValues([[folderId, folderPath, new Date()]]);
      return;
    }
  }

  // Append new row
  prefsSheet.appendRow([clientCode, partnerName, folderId, folderPath, new Date()]);
}

// ============================================================================
// FILENAME FUNCTIONS
// ============================================================================

/**
 * Generates the filename for the duplicated sheet.
 * Format: {CLIENT} | Partner Data Collection | {PARTNER}
 * If duplicate exists, appends campaign name or month/year.
 * @param {Object} configValues - The config values
 * @param {Folder} destinationFolder - The destination folder
 * @returns {string} The generated filename
 */
function generateFileName_(configValues, destinationFolder) {
  // Normalize client code to uppercase
  const clientUpper = configValues.clientCode.toUpperCase();

  // Base filename
  let fileName = clientUpper + ' | Partner Data Collection | ' + configValues.partnerName;

  // Check for duplicates
  if (fileExistsInFolder_(destinationFolder, fileName)) {
    if (configValues.campaignName) {
      // Append campaign name
      fileName += ' | ' + configValues.campaignName;
    } else {
      // Append month/year
      fileName += ' | ' + formatMonthYear_(new Date());
    }

    // If still a duplicate, add a number
    let counter = 2;
    let uniqueName = fileName;
    while (fileExistsInFolder_(destinationFolder, uniqueName)) {
      uniqueName = fileName + ' (' + counter + ')';
      counter++;
    }
    fileName = uniqueName;
  }

  return fileName;
}

/**
 * Checks if a file with the given name exists in the folder.
 * @param {Folder} folder - The folder to check
 * @param {string} fileName - The filename to look for
 * @returns {boolean} True if file exists
 */
function fileExistsInFolder_(folder, fileName) {
  const files = folder.getFilesByName(fileName);
  return files.hasNext();
}

/**
 * Formats the current date as "MON YY" (e.g., "JAN 26").
 * @param {Date} date - The date to format
 * @returns {string} Formatted month/year
 */
function formatMonthYear_(date) {
  const months = ['JAN', 'FEB', 'MAR', 'APR', 'MAY', 'JUN',
                  'JUL', 'AUG', 'SEP', 'OCT', 'NOV', 'DEC'];
  const month = months[date.getMonth()];
  const year = String(date.getFullYear()).slice(-2);
  return month + ' ' + year;
}

// ============================================================================
// ★ NEW IN V2: DESTINATION CONFIRMATION
// ============================================================================

/**
 * Shows a confirmation dialog with the filename and destination.
 * Allows user to confirm, change folder, or cancel.
 * @param {string} fileName - The generated filename
 * @param {Folder} folder - The destination folder
 * @param {boolean} usingCustomFolder - Whether using a saved custom folder preference
 * @param {Ui} ui - The UI object
 * @returns {Object|null} {folder: Folder, changedFolder: boolean} or null if cancelled
 */
function confirmDestination_(fileName, folder, usingCustomFolder, ui) {
  const folderPath = getFolderPath_(folder);

  // Build confirmation message
  const savedIndicator = usingCustomFolder ? '\n  ★ Using saved preference\n' : '';
  const message = 'READY TO DUPLICATE\n' +
    '━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n\n' +
    'File name:\n' +
    '  ' + fileName + '\n\n' +
    'Destination:\n' +
    '  📁 ' + folderPath + savedIndicator + '\n' +
    '━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n\n' +
    'Enter:\n' +
    '  1 - Proceed with duplication\n' +
    '  2 - Change destination folder' + (usingCustomFolder ? ' (clears saved preference)' : '') + '\n' +
    '  cancel - Cancel operation';

  const response = ui.prompt('Confirm Destination', message, ui.ButtonSet.OK_CANCEL);

  // Check if user cancelled
  if (response.getSelectedButton() !== ui.Button.OK) {
    return null;
  }

  const input = response.getResponseText().trim().toLowerCase();

  if (input === '1') {
    // Proceed with current folder
    return { folder: folder, changedFolder: false };
  } else if (input === '2') {
    // Change folder
    const newFolder = showChangeFolderDialog_(ui);
    if (!newFolder) {
      return null; // User cancelled folder change
    }
    return { folder: newFolder, changedFolder: true };
  } else if (input === 'cancel') {
    return null;
  } else {
    showError_('Invalid selection: "' + input + '". Please enter 1, 2, or cancel.');
    return null;
  }
}

/**
 * Shows a dialog allowing the user to input a custom folder URL or ID.
 * @param {Ui} ui - The UI object
 * @returns {Folder|null} The selected folder or null if cancelled
 */
function showChangeFolderDialog_(ui) {
  const message = 'CUSTOM DESTINATION FOLDER\n' +
    '━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n\n' +
    'Paste the Google Drive folder URL or ID:\n\n' +
    'Examples:\n' +
    '  • Full URL:\n' +
    '    https://drive.google.com/drive/folders/1ABC...xyz\n\n' +
    '  • Just the ID:\n' +
    '    1ABC...xyz\n\n' +
    'Enter the folder URL or ID:';

  const response = ui.prompt('Change Destination Folder', message, ui.ButtonSet.OK_CANCEL);

  // Check if user cancelled
  if (response.getSelectedButton() !== ui.Button.OK) {
    return null;
  }

  const input = response.getResponseText().trim();

  if (!input) {
    showError_('Please enter a folder URL or ID.');
    return null;
  }

  // Extract folder ID from URL or use as-is if it's an ID
  const folderId = extractFolderIdFromInput_(input);

  if (!folderId) {
    showError_('Invalid folder URL or ID. Please check and try again.');
    return null;
  }

  // Try to access the folder to validate it
  try {
    const folder = DriveApp.getFolderById(folderId);
    folder.getName(); // This will throw if no access

    // Show success message with folder name
    const folderPath = getFolderPath_(folder);
    ui.alert('Folder Selected', '✓ Folder found:\n\n' + folderPath, ui.ButtonSet.OK);

    return folder;

  } catch (e) {
    showError_('Cannot access this folder. Please check the ID and your permissions.\n\nError: ' + e.message);
    return null;
  }
}

/**
 * Extracts a folder ID from various input formats.
 * Handles:
 * - Direct folder ID
 * - Full Google Drive URL
 * - Shortened Drive URL
 * @param {string} input - The user's input
 * @returns {string|null} The folder ID or null if invalid
 */
function extractFolderIdFromInput_(input) {
  const trimmed = input.trim();

  // Pattern 1: Full URL - https://drive.google.com/drive/folders/FOLDER_ID
  const fullUrlMatch = trimmed.match(/\/folders\/([a-zA-Z0-9_-]+)/);
  if (fullUrlMatch) {
    return fullUrlMatch[1];
  }

  // Pattern 2: URL with parameters - https://drive.google.com/drive/u/0/folders/FOLDER_ID
  const urlWithParamsMatch = trimmed.match(/\/folders\/([a-zA-Z0-9_-]+)/);
  if (urlWithParamsMatch) {
    return urlWithParamsMatch[1];
  }

  // Pattern 3: Just the ID (alphanumeric, hyphens, underscores, typically 25-50 chars)
  if (/^[a-zA-Z0-9_-]{15,100}$/.test(trimmed)) {
    return trimmed;
  }

  return null;
}

// ============================================================================
// TAB VISIBILITY FUNCTIONS
// ============================================================================

/**
 * Hides all sheets/tabs in the spreadsheet except the 'data' tab.
 * The 'data' tab is matched case-insensitively.
 * @param {Spreadsheet} spreadsheet - The spreadsheet to modify
 */
function hideAllTabsExceptData_(spreadsheet) {
  const sheets = spreadsheet.getSheets();
  const visibleTabName = CONFIG.VISIBLE_TAB_NAME.toLowerCase();

  // First, find the 'data' tab and make sure it's visible
  let dataSheet = null;
  for (const sheet of sheets) {
    if (sheet.getName().toLowerCase() === visibleTabName) {
      dataSheet = sheet;
      break;
    }
  }

  // If 'data' tab doesn't exist, log warning and skip hiding
  if (!dataSheet) {
    console.warn('No "' + CONFIG.VISIBLE_TAB_NAME + '" tab found. Skipping tab hiding.');
    return;
  }

  // Make sure the data sheet is visible and active
  dataSheet.showSheet();
  spreadsheet.setActiveSheet(dataSheet);

  // Hide all other sheets
  for (const sheet of sheets) {
    if (sheet.getName().toLowerCase() !== visibleTabName) {
      sheet.hideSheet();
    }
  }
}

// ============================================================================
// PERMISSION FUNCTIONS
// ============================================================================

/**
 * Sets permissions on the new file:
 * 1. Transfers ownership to configured email
 * 2. Makes original creator an editor (automatic with ownership transfer)
 * 3. Allows anyone in organization to edit
 * @param {File} file - The file to set permissions on
 * @returns {Object} Result object with success status and messages
 */
function setPermissions_(file) {
  const result = {
    success: true,
    ownershipTransferred: false,
    orgAccessSet: false,
    sharedDrive: false,
    errors: []
  };

  // V4: Shared-Drive files are owned by the org, not a person - setOwner is
  // impossible AND unnecessary there (control comes from Drive membership).
  const inSharedDrive = isInSharedDrive_(file.getId());
  result.sharedDrive = inSharedDrive;

  if (inSharedDrive) {
    // Ensure Gene has edit rights via membership (harmless if already a member).
    try {
      file.addEditor(CONFIG.NEW_OWNER_EMAIL);
    } catch (e) {
      // Already has access through the Shared Drive - fine.
    }
    result.ownershipTransferred = true; // satisfied via membership
  } else {
    // Legacy My-Drive destination: transfer ownership as before.
    try {
      file.setOwner(CONFIG.NEW_OWNER_EMAIL);
      result.ownershipTransferred = true;
    } catch (e) {
      result.success = false;
      result.errors.push('Could not transfer ownership to ' + CONFIG.NEW_OWNER_EMAIL + ': ' + e.message);
      try {
        file.addEditor(CONFIG.NEW_OWNER_EMAIL);
        result.errors.push('Added ' + CONFIG.NEW_OWNER_EMAIL + ' as editor instead.');
      } catch (e2) {
        result.errors.push('Could not add as editor either: ' + e2.message);
      }
    }
  }

  // Organization-wide editor access. For Shared-Drive files this is governed by
  // the drive's membership, so a failure here is expected and non-fatal.
  try {
    file.setSharing(DriveApp.Access.DOMAIN_WITH_LINK, DriveApp.Permission.EDIT);
    result.orgAccessSet = true;
  } catch (e) {
    if (!inSharedDrive) {
      result.errors.push('Could not set organization-wide access: ' + e.message);
    }
  }

  return result;
}

/**
 * V4: Returns true if the file lives in a Shared Drive (Team Drive).
 * Uses the Drive advanced service; defensively returns false if undeterminable.
 * @param {string} fileId - The file ID to check
 * @returns {boolean}
 */
function isInSharedDrive_(fileId) {
  try {
    const meta = Drive.Files.get(fileId, { supportsAllDrives: true, fields: 'id,driveId,teamDriveId' });
    return !!(meta && (meta.driveId || meta.teamDriveId));
  } catch (e) {
    console.warn('isInSharedDrive_ check failed for ' + fileId + ': ' + e.message);
    return false;
  }
}

// ============================================================================
// V4 FUNCTIONS - Shared-Drive routing, interim lockdown, source archiving
// ============================================================================

/**
 * V4: Resolve the destination "First Party Data" folder for a client/partner.
 * Precedence: (1) CustomFolderPrefs exact client+partner row, (2) client-level
 * CustomFolderPrefs row (partner blank), (3) CLIENT_FPD_FOLDERS map.
 * Returns a Folder, or null to fall back to the legacy My-Drive traversal.
 */
function resolveFpdDestination_(clientCode, partnerName) {
  // 1. exact client+partner override (existing mechanism)
  const exact = getCustomFolderPref_(clientCode, partnerName);
  if (exact) return exact;

  // 2. client-level override (partner column blank)
  const clientLevel = getClientLevelFolderPref_(clientCode);
  if (clientLevel) return clientLevel;

  // 3. hard-coded Shared-Drive map
  const mapped = CONFIG.CLIENT_FPD_FOLDERS[String(clientCode).toLowerCase().trim()];
  if (mapped) {
    try {
      const folder = DriveApp.getFolderById(mapped);
      folder.getName(); // throws if inaccessible
      return folder;
    } catch (e) {
      console.warn('CLIENT_FPD_FOLDERS entry for ' + clientCode + ' not accessible: ' + mapped);
    }
  }
  return null;
}

/**
 * V4: CustomFolderPrefs lookup where the partner column is blank - a client-level
 * default that applies to every partner for that client.
 */
function getClientLevelFolderPref_(clientCode) {
  const masterSpreadsheet = SpreadsheetApp.openById(CONFIG.MASTER_SPREADSHEET_ID);
  const prefsSheet = masterSpreadsheet.getSheetByName(CONFIG.SHEETS.CUSTOM_FOLDER_PREFS);
  if (!prefsSheet) return null;

  const data = prefsSheet.getDataRange().getValues();
  const clientLower = String(clientCode).toLowerCase().trim();
  for (let i = 1; i < data.length; i++) {
    const rowClient = String(data[i][0]).toLowerCase().trim();
    const rowPartner = String(data[i][1]).trim();
    const folderId = data[i][2];
    if (rowClient === clientLower && rowPartner === '' && folderId) {
      try {
        const folder = DriveApp.getFolderById(folderId);
        folder.getName();
        return folder;
      } catch (e) {
        return null;
      }
    }
  }
  return null;
}

/**
 * V4 INTERIM lockdown of a freshly published copy: lock the partner selection
 * and protect every non-`data` tab to Gene-only, so an editor cannot repoint the
 * scope to another partner. The `data` tab keeps the template's own protections
 * (context columns locked, metric-entry columns editable).
 * NOTE: this is Track-1 interim only. Live central links remain until Track 2,
 * so do NOT share published sheets externally until Track 2 is in place.
 */
function lockPublishedSheet_(spreadsheet) {
  const me = CONFIG.NEW_OWNER_EMAIL;
  const dataName = CONFIG.VISIBLE_TAB_NAME.toLowerCase();

  spreadsheet.getSheets().forEach(function (sheet) {
    if (sheet.getName().toLowerCase() === dataName) return; // leave data tab as-is
    try {
      const protection = sheet.protect().setDescription('FPD locked: scope frozen (V4)');
      protection.removeEditors(protection.getEditors());
      if (protection.canDomainEdit()) protection.setDomainEdit(false);
      protection.addEditor(me);
    } catch (e) {
      console.warn('Could not protect tab ' + sheet.getName() + ': ' + e.message);
    }
  });
}

/**
 * V4: Opt-in partner auto-share. If a partner email was provided in Config, ask
 * the operator to confirm the EXTERNAL share, then add that address as editor.
 * Blank -> no-op (operator shares manually, today's behavior).
 */
function maybeSharePartner_(file, configValues, ui) {
  const email = configValues.partnerEmail;
  if (!email) return;

  const response = ui.alert(
    'Share externally?',
    'Add ' + email + ' as an editor of the published sheet?\n\n' +
    'This shares FPD data OUTSIDE the agency. Only proceed if the address is correct.',
    ui.ButtonSet.YES_NO
  );
  if (response !== ui.Button.YES) return;

  try {
    file.addEditor(email);
  } catch (e) {
    ui.alert('Could not share to ' + email + ': ' + e.message);
  }
}

/**
 * V4: Mark the working copy the operator launched Publish from as outdated, so
 * the team stops editing it and uses the newly published sheet instead. Per the
 * global never-delete rule this only renames; it must NEVER touch the master
 * template.
 * @param {Spreadsheet} ss - the source spreadsheet Publish was run from
 * @param {string} publishedName - the name of the newly published sheet
 */
function archiveSourceDupe_(ss, publishedName) {
  try {
    const sourceId = ss.getId();
    const sourceFile = DriveApp.getFileById(sourceId);
    const current = sourceFile.getName();

    // Safety guard - only archive a genuine throwaway working copy:
    // 1) Never rename the master template; everyone dupes from it.
    if (sourceId === CONFIG.MASTER_SPREADSHEET_ID) return;
    // 2) Never re-archive a file already marked archived (prevents stacked
    //    prefixes and re-archiving on a repeat publish from the same copy).
    if (/^ARCHIVE\b/i.test(current.trim())) return;

    sourceFile.setName('ARCHIVE - OUTDATED - ' + current);
  } catch (e) {
    console.warn('archiveSourceDupe_ skipped: ' + e.message);
  }
}

// ============================================================================
// SHORTCUT FUNCTIONS
// ============================================================================

/**
 * Creates a shortcut to the new file in the designated shortcuts folder.
 * @param {File} file - The file to create a shortcut to
 * @param {string} fileName - The name for the shortcut
 * @returns {string|null} The shortcut file ID, or null if creation failed
 */
function createShortcut_(file, fileName) {
  try {
    const shortcutFolder = DriveApp.getFolderById(CONFIG.SHORTCUT_FOLDER_ID);

    // Use Drive API to create shortcut (DriveApp doesn't have native shortcut support)
    const shortcut = Drive.Files.create({
      name: fileName,
      mimeType: 'application/vnd.google-apps.shortcut',
      shortcutDetails: {
        targetId: file.getId()
      },
      parents: [CONFIG.SHORTCUT_FOLDER_ID]
    });

    console.log('Created shortcut: ' + shortcut.id);
    return shortcut.id;
  } catch (e) {
    // Log error but don't fail the whole process
    console.error('Could not create shortcut: ' + e.message);
    return null;
  }
}

// ============================================================================
// LOGGING FUNCTIONS
// ============================================================================

/**
 * Logs the duplication action to the Log sheet in the MASTER spreadsheet.
 * Uses HYPERLINK formulas for clickable links.
 * @param {Spreadsheet} ss - The source spreadsheet (for reference only)
 * @param {File} originalFile - The original file
 * @param {File} newFile - The new file
 * @param {Folder} destinationFolder - The destination folder
 * @param {boolean} success - Whether the operation was fully successful
 * @param {boolean} customFolder - Whether a custom folder was used
 * @param {string} shortcutId - Optional shortcut file ID
 */
function logAction_(ss, originalFile, newFile, destinationFolder, success, customFolder, shortcutId) {
  // Always log to the master spreadsheet
  const masterSpreadsheet = SpreadsheetApp.openById(CONFIG.MASTER_SPREADSHEET_ID);
  let logSheet = masterSpreadsheet.getSheetByName(CONFIG.SHEETS.LOG);

  // Define headers
  const headers = [
    'Timestamp', 'Original File', 'New File', 'Destination Folder',
    'Shortcut', 'User Email', 'Status', 'Custom Folder'
  ];

  // Create Log sheet if it doesn't exist
  if (!logSheet) {
    logSheet = masterSpreadsheet.insertSheet(CONFIG.SHEETS.LOG);
    logSheet.getRange('A1:H1').setValues([headers]);
    logSheet.getRange('A1:H1').setFontWeight('bold');
    logSheet.setColumnWidth(2, 250); // Original File
    logSheet.setColumnWidth(3, 250); // New File
    logSheet.setColumnWidth(4, 200); // Destination Folder
    logSheet.setColumnWidth(5, 100); // Shortcut
  }

  // Build hyperlink formulas
  const originalFileLink = '=HYPERLINK("' + originalFile.getUrl() + '","' + escapeForFormula_(originalFile.getName()) + '")';
  const newFileLink = '=HYPERLINK("' + newFile.getUrl() + '","' + escapeForFormula_(newFile.getName()) + '")';
  const folderLink = '=HYPERLINK("https://drive.google.com/drive/folders/' + destinationFolder.getId() + '","' + escapeForFormula_(getFolderPath_(destinationFolder)) + '")';

  // Shortcut link (if available)
  let shortcutLink = '';
  if (shortcutId) {
    shortcutLink = '=HYPERLINK("https://drive.google.com/file/d/' + shortcutId + '/view","Open Shortcut")';
  }

  // Append log entry with formulas
  const newRow = logSheet.getLastRow() + 1;
  logSheet.getRange(newRow, 1, 1, 8).setValues([[
    new Date(),
    originalFileLink,
    newFileLink,
    folderLink,
    shortcutLink,
    Session.getActiveUser().getEmail(),
    success ? 'Success' : 'Completed with warnings',
    customFolder ? 'Yes' : 'No'
  ]]);
}

/**
 * Escapes special characters for use in HYPERLINK formula text.
 * @param {string} text - The text to escape
 * @returns {string} Escaped text safe for formula
 */
function escapeForFormula_(text) {
  return String(text).replace(/"/g, '""');
}

/**
 * Gets the full path of a folder.
 * @param {Folder} folder - The folder
 * @returns {string} The folder path
 */
function getFolderPath_(folder) {
  const pathParts = [];
  let current = folder;

  // Walk up the folder tree (limit to 10 levels to prevent infinite loops)
  for (let i = 0; i < 10; i++) {
    pathParts.unshift(current.getName());
    const parents = current.getParents();
    if (!parents.hasNext()) break;
    current = parents.next();
  }

  return pathParts.join(' / ');
}

/**
 * Gets the path of a file based on its parent folder.
 * @param {File} file - The file
 * @returns {string} The file path
 */
function getFilePath_(file) {
  const parents = file.getParents();
  if (parents.hasNext()) {
    return getFolderPath_(parents.next()) + ' / ' + file.getName();
  }
  return file.getName();
}

// ============================================================================
// PROGRESS TRACKER
// ============================================================================

/**
 * A class to track and display progress through a multi-step workflow.
 * Shows a toast notification for each step and builds a summary.
 */
class ProgressTracker_ {
  /**
   * @param {Spreadsheet} ss - The spreadsheet to show toasts on
   * @param {string[]} steps - Array of step names
   */
  constructor(ss, steps) {
    this.ss = ss;
    this.steps = steps.map((name, index) => ({
      name: name,
      status: 'pending', // pending, in_progress, complete
      detail: null,
      index: index
    }));
    this.startTime = new Date();

    // Show initial progress
    this.showCurrentProgress_();
  }

  /**
   * Mark a step as started (in progress).
   * @param {number} stepIndex - The step index
   */
  start(stepIndex) {
    this.steps[stepIndex].status = 'in_progress';
    this.showCurrentProgress_();
  }

  /**
   * Mark a step as complete.
   * @param {number} stepIndex - The step index
   * @param {string} [detail] - Optional detail to show (e.g., folder name)
   */
  complete(stepIndex, detail) {
    this.steps[stepIndex].status = 'complete';
    if (detail) {
      this.steps[stepIndex].detail = detail;
    }
    this.showCurrentProgress_();
  }

  /**
   * Show current progress as a toast notification.
   */
  showCurrentProgress_() {
    const completed = this.steps.filter(s => s.status === 'complete').length;
    const current = this.steps.find(s => s.status === 'in_progress');
    const total = this.steps.length;

    let message = `Step ${completed + (current ? 1 : 0)} of ${total}`;
    if (current) {
      message += `: ${current.name}...`;
    }

    // Build checklist preview
    const checklistPreview = this.steps.slice(0, Math.min(completed + 2, total)).map(s => {
      if (s.status === 'complete') return `✓ ${s.name}`;
      if (s.status === 'in_progress') return `► ${s.name}`;
      return `○ ${s.name}`;
    }).join('\n');

    this.ss.toast(
      checklistPreview,
      `Progress: ${completed}/${total} complete`,
      30
    );
  }

  /**
   * Get a summary of all steps for the final dialog.
   * @returns {string} HTML-formatted summary
   */
  getSummary() {
    const elapsed = Math.round((new Date() - this.startTime) / 1000);

    let html = '<div style="text-align: left; font-size: 13px; margin: 15px 0;">';
    html += '<div style="font-weight: bold; margin-bottom: 8px;">Completed Steps:</div>';

    for (const step of this.steps) {
      const icon = step.status === 'complete' ? '✓' : '○';
      const color = step.status === 'complete' ? '#28a745' : '#666';
      const detail = step.detail ? ` → ${step.detail}` : '';
      html += `<div style="color: ${color}; margin: 4px 0;">${icon} ${step.name}${detail}</div>`;
    }

    html += `<div style="margin-top: 10px; color: #666; font-size: 12px;">Completed in ${elapsed} seconds</div>`;
    html += '</div>';

    return html;
  }
}

// ============================================================================
// UI FUNCTIONS
// ============================================================================

/**
 * Shows a numbered list prompt for folder selection.
 * Uses native Google Sheets UI prompt for reliability.
 * @param {Array} folderList - Array of {name, folder} objects
 * @param {string} title - Dialog title
 * @param {string} message - Dialog message
 * @param {Ui} ui - The UI object
 * @returns {Folder|null} Selected folder or null if cancelled
 */
function showFolderDropdown_(folderList, title, message, ui) {
  // Build numbered list of folders
  const folderListText = folderList.map((f, i) =>
    (i + 1) + '. ' + f.name
  ).join('\n');

  const promptMessage = message + '\n\n' + folderListText + '\n\nEnter the number of your choice:';

  const response = ui.prompt(title, promptMessage, ui.ButtonSet.OK_CANCEL);

  // Check if user cancelled
  if (response.getSelectedButton() !== ui.Button.OK) {
    return null;
  }

  // Parse the selection
  const input = response.getResponseText().trim();
  const selectedIndex = parseInt(input) - 1; // Convert to 0-based index

  // Validate selection
  if (isNaN(selectedIndex) || selectedIndex < 0 || selectedIndex >= folderList.length) {
    showError_('Invalid selection: "' + input + '". Please enter a number between 1 and ' + folderList.length + '.');
    return null;
  }

  return folderList[selectedIndex].folder;
}

/**
 * Shows the success dialog with a link to the new file.
 * @param {string} fileUrl - URL of the new file
 * @param {string} fileName - Name of the new file
 * @param {Object} permissionResult - Result from permission setting
 * @param {string} progressSummary - HTML summary of completed steps
 */
function showSuccessDialog_(fileUrl, fileName, permissionResult, progressSummary) {
  let warningHtml = '';
  if (!permissionResult.success) {
    warningHtml = `
      <div style="background-color: #fff3cd; border: 1px solid #ffc107; padding: 10px; margin-bottom: 15px; border-radius: 4px;">
        <strong>⚠️ Manual action required:</strong><br>
        ${permissionResult.errors.map(e => '• ' + escapeHtml_(e)).join('<br>')}
        <br><br>
        Please manually transfer ownership to ${CONFIG.NEW_OWNER_EMAIL}
      </div>
    `;
  }

  const html = HtmlService.createHtmlOutput(`
    <style>
      body { font-family: Arial, sans-serif; padding: 15px; text-align: center; }
      .success { color: #28a745; font-size: 48px; }
      h2 { margin: 10px 0; font-size: 18px; }
      .filename { background-color: #f1f1f1; padding: 10px; border-radius: 4px; margin: 10px 0; word-break: break-all; font-size: 13px; }
      .open-btn {
        display: inline-block;
        background-color: #4285f4;
        color: white;
        padding: 12px 24px;
        text-decoration: none;
        border-radius: 4px;
        font-size: 16px;
        margin-top: 10px;
      }
      .open-btn:hover { background-color: #3367d6; }
      .close-btn { margin-top: 15px; color: #666; cursor: pointer; font-size: 13px; }
      .summary {
        background-color: #f8f9fa;
        border: 1px solid #e9ecef;
        border-radius: 4px;
        padding: 10px;
        margin: 10px 0;
        max-height: 200px;
        overflow-y: auto;
      }
      .toggle-summary {
        color: #4285f4;
        cursor: pointer;
        font-size: 13px;
        margin: 5px 0;
      }
    </style>
    <div class="success">✓</div>
    <h2>Sheet Duplicated Successfully!</h2>
    ${warningHtml}
    <div class="filename">${escapeHtml_(fileName)}</div>

    <div class="toggle-summary" onclick="toggleSummary()">▶ Show details</div>
    <div id="summarySection" class="summary" style="display: none;">
      ${progressSummary || ''}
    </div>

    <a class="open-btn" href="${fileUrl}" target="_blank" onclick="google.script.host.close()">
      Open New Sheet
    </a>
    <p class="close-btn" onclick="google.script.host.close()">Close</p>

    <script>
      function toggleSummary() {
        const section = document.getElementById('summarySection');
        const toggle = document.querySelector('.toggle-summary');
        if (section.style.display === 'none') {
          section.style.display = 'block';
          toggle.textContent = '▼ Hide details';
        } else {
          section.style.display = 'none';
          toggle.textContent = '▶ Show details';
        }
      }
    </script>
  `)
  .setWidth(450)
  .setHeight(permissionResult.success ? 380 : 480);

  SpreadsheetApp.getUi().showModalDialog(html, 'Success');
}

/**
 * Shows an error dialog.
 * @param {string} message - The error message
 */
function showError_(message) {
  SpreadsheetApp.getUi().alert('Error', message, SpreadsheetApp.getUi().ButtonSet.OK);
}

/**
 * Escapes HTML special characters.
 * @param {string} text - Text to escape
 * @returns {string} Escaped text
 */
function escapeHtml_(text) {
  return String(text)
    .replace(/&/g, '&amp;')
    .replace(/</g, '&lt;')
    .replace(/>/g, '&gt;')
    .replace(/"/g, '&quot;')
    .replace(/'/g, '&#039;');
}
