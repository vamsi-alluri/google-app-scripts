/**
 * Google Docs Tab to Drive PDF Exporter & Markdown Syncer
 * * FEATURES:
 * 1. SYNC: Mirrors Doc Tabs -> Drive Folders/PDFs.
 * 2. MARKDOWN: Generates Markdown from Tabs -> Google Sheet.
 * 3. CLEANUP: Handles deletions (Trashes PDFs, Archives Sheet Rows).
 * 4. STATE: Tracks structure to prevent duplicate work.
 */

// ================= CONFIGURATION =================
const DOCUMENT_ID = ''; 
const ROOT_FOLDER_ID = '';         
const LOG_SHEET_ID = ''; 
// Sheet for Markdown content
const MARKDOWN_SHEET_ID = ''; 

const FILENAME_PREFIX = ''; 
const FILENAME_SUFFIX = '';                    

const DELAY_BETWEEN_EXPORTS = 2500; 
const MAX_RETRIES = 3; 
const INITIAL_BACKOFF = 1500; 

// ================= INTERNAL CONSTANTS =================
const SCRIPT_PROPERTIES = PropertiesService.getScriptProperties();
const STATE_KEY = 'doc_structure_state'; 
const LOCK_KEY = 'script_mutex_lock';
const LOCK_TIMEOUT_MS = 9 * 60 * 1000; 

// ================= LOCK MANAGEMENT =================
let CURRENT_LOCK_TIMESTAMP = null;

function lockCheck() {
  const lockValue = SCRIPT_PROPERTIES.getProperty(LOCK_KEY);
  
  if (!lockValue) {
    return true; // No lock exists, safe to proceed
  }
  
  const lockTime = parseInt(lockValue, 10);
  
  // If this process owns the lock, allow it
  if (CURRENT_LOCK_TIMESTAMP !== null && lockTime === CURRENT_LOCK_TIMESTAMP) {
    Logger.log('✓ Lock owned by this process.');
    return true;
  }
  
  // Check if lock is stale
  const now = Date.now();
  if (now - lockTime >= LOCK_TIMEOUT_MS) {
    logToSheet('System', 'Stale lock detected.', 'Warning');
    return true; // Stale lock, can take over
  }
  
  // Active lock from another process
  Logger.log('⚠️ Script is already running (Locked by another process). Exiting.');
  return false;
}

function lockSet() {
  const now = Date.now();
  SCRIPT_PROPERTIES.setProperty(LOCK_KEY, now.toString());
  CURRENT_LOCK_TIMESTAMP = now;
  Logger.log('🔒 Lock set.');
}

function lockRelease() {
  SCRIPT_PROPERTIES.deleteProperty(LOCK_KEY);
  CURRENT_LOCK_TIMESTAMP = null;
  Logger.log('🔓 Lock released.');
}

// ================= EXPORT LOGIC =================
function exportUpdatedTabsToPDF() {
  // 1 Lock Check:
  if (!lockCheck()){
    return;
  }
  // 2 Set the lock.
  if (CURRENT_LOCK_TIMESTAMP === null){
    lockSet();
  }

  try {
    Logger.log('🔒 Lock acquired. Starting export...');
    
    const doc = DocumentApp.openById(DOCUMENT_ID);
    const rootFolder = DriveApp.getFolderById(ROOT_FOLDER_ID);
    
    let state = getStoredState();
    const activeTabIds = new Set(); 
    
    // Pre-load Sheet Map to prevent duplicates
    const markdownSheetMap = getMarkdownSheetRowMap();

    const topLevelTabs = doc.getTabs();
    let exportCount = 0;
    
    // Process hierarchy
    topLevelTabs.forEach(tab => {
      if (tab.getType() === DocumentApp.TabType.DOCUMENT_TAB) {
        exportCount += processTabHierarchy(tab, rootFolder, [], state, activeTabIds, markdownSheetMap);
      }
    });

    cleanupOrphans(state, activeTabIds, markdownSheetMap);

    SCRIPT_PROPERTIES.setProperty(STATE_KEY, JSON.stringify(state));
    SCRIPT_PROPERTIES.setProperty('lastDocumentCheck', Date.now().toString());
    
    Logger.log(`Run completed. Processed: ${exportCount}.`);
    if (exportCount > 0) logToSheet('Info', `Run completed. ${exportCount} items processed.`, 'Success');
    
  } catch (e) {
    Logger.log(`❌ Error: ${e.message}`);
    logToSheet('Error', e.message, 'Failed');
  } finally {
    lockRelease();
  }
}

function processTabHierarchy(tab, parentFolder, pathArray, state, activeTabIds, markdownSheetMap) {
  const tabId = tab.getId();
  const tabTitle = tab.getTitle();
  let exportCount = 0;
  
  activeTabIds.add(tabId);

  // Init State
  if (!state[tabId]) state[tabId] = { fileId: null, folderId: null, title: tabTitle, parentName: null };
  let stateDirty = false;

  // 1. Structure Check
  const currentParentName = pathArray.length > 0 ? pathArray[pathArray.length - 1] : 'ROOT';
  const savedParentName = state[tabId].parentName;
  const structureChanged = (state[tabId].fileId && currentParentName !== savedParentName);

  // 2. Rename Check
  const storedTitle = state[tabId].title;
  if (storedTitle && storedTitle !== tabTitle) {
    Logger.log(`📝 Rename: "${storedTitle}" -> "${tabTitle}"`);
    const newFileName = `${FILENAME_PREFIX}${tabTitle}${FILENAME_SUFFIX}.pdf`;
    
    if (state[tabId].fileId) safeRenameFile(state[tabId].fileId, newFileName);
    if (state[tabId].folderId) safeRenameFolder(state[tabId].folderId, tabTitle);
    
    // Update Sheet Name
    updateMarkdownSheetName(markdownSheetMap, storedTitle, tabTitle);

    state[tabId].title = tabTitle; 
    stateDirty = true; 
  }

  // 3. Export & Sync Check
  const currentHash = getTabContentHash(tab);
  const storedHashKey = `hash_${tabId}`;
  const storedHash = SCRIPT_PROPERTIES.getProperty(storedHashKey);
  const contentChanged = (currentHash !== storedHash);
  
  // Verify File Exists
  let fileExists = false;
  if (state[tabId].fileId) {
    try { DriveApp.getFileById(state[tabId].fileId); fileExists = true; } 
    catch (e) { state[tabId].fileId = null; }
  }

  // Check if missing from Sheet (using trimmed name check)
  const missingFromSheet = !markdownSheetMap.has(tabTitle.trim());

  if (contentChanged || !fileExists || missingFromSheet) {
    // >> ACTION: Export PDF + Generate Markdown
    const fullPath = pathArray.concat(tabTitle).join(' > ');
    const exportedFile = exportTabToPDF(DOCUMENT_ID, tabId, tabTitle, parentFolder);
    
    if (exportedFile) {
      SCRIPT_PROPERTIES.setProperty(storedHashKey, currentHash);
      state[tabId].fileId = exportedFile.getId();
      state[tabId].title = tabTitle;
      state[tabId].parentName = currentParentName;
      
      // Generate Markdown
      const markdown = convertTabToMarkdown(tab);
      const filePath = getDrivePath(exportedFile);
      
      // Update Sheet
      syncToMarkdownSheet(markdownSheetMap, tabTitle, markdown, 'Active', filePath);

      exportCount++;
      stateDirty = true;
      Logger.log(`✓ Synced: ${fullPath}`);
      Utilities.sleep(DELAY_BETWEEN_EXPORTS);
    }
  } else if (structureChanged) {
    // >> ACTION: Move only
    if (state[tabId].fileId) {
      try {
        const file = DriveApp.getFileById(state[tabId].fileId);
        moveItemToFolder(file, parentFolder);
        
        // Update Path in Sheet
        const filePath = getDrivePath(file);
        syncToMarkdownSheet(markdownSheetMap, tabTitle, null, 'Active', filePath); 
        
        state[tabId].parentName = currentParentName;
        stateDirty = true;
      } catch (e) {}
    }
  }

  if (stateDirty) SCRIPT_PROPERTIES.setProperty(STATE_KEY, JSON.stringify(state));

  // 4. Recursion
  const childTabs = tab.getChildTabs();
  if (childTabs.length > 0) {
    let subFolder;
    let folderDirty = false;
    const folderNeedsMove = (state[tabId].folderId && currentParentName !== savedParentName);

    if (state[tabId].folderId) {
      try {
        subFolder = DriveApp.getFolderById(state[tabId].folderId);
        if (folderNeedsMove) moveItemToFolder(subFolder, parentFolder);
      } catch (e) {
        subFolder = getOrCreateFolder(parentFolder, tabTitle);
        state[tabId].folderId = subFolder.getId();
        folderDirty = true;
      }
    } else {
      subFolder = getOrCreateFolder(parentFolder, tabTitle);
      state[tabId].folderId = subFolder.getId();
      folderDirty = true;
    }
    
    if (folderDirty) SCRIPT_PROPERTIES.setProperty(STATE_KEY, JSON.stringify(state));

    childTabs.forEach(childTab => {
      if (childTab.getType() === DocumentApp.TabType.DOCUMENT_TAB) {
        exportCount += processTabHierarchy(childTab, subFolder, pathArray.concat(tabTitle), state, activeTabIds, markdownSheetMap);
      }
    });
  }
  
  return exportCount;
}

// ================= SHEET HELPERS (FIXED DUPLICATES) =================

function getMarkdownSheetRowMap() {
  const map = new Map();
  if (!MARKDOWN_SHEET_ID) return map;
  
  try {
    const sheet = SpreadsheetApp.openById(MARKDOWN_SHEET_ID).getSheets()[0];
    const lastRow = sheet.getLastRow();
    
    // If only header exists or empty
    if (lastRow < 2) return map; 
    
    // Get all names in Col A (DisplayValues ensures strings)
    const data = sheet.getRange(2, 1, lastRow, 1).getDisplayValues(); 
    
    data.forEach((row, index) => {
      // TRIM whitespace to ensure accurate matching
      const name = row[0].trim();
      if (name) {
        map.set(name, index + 2); // Store 1-based Row Index
      }
    });
  } catch(e) {
    Logger.log("Error reading sheet map: " + e.message);
  }
  return map;
}

function syncToMarkdownSheet(map, name, content, status, path) {
  if (!MARKDOWN_SHEET_ID) return;
  
  // Normalize name
  const cleanName = name.trim();
  
  try {
    const sheet = SpreadsheetApp.openById(MARKDOWN_SHEET_ID).getSheets()[0];
    
    if (map.has(cleanName)) {
      // >> UPDATE EXISTING ROW
      const rowIndex = map.get(cleanName);
      if (content !== null) sheet.getRange(rowIndex, 2).setValue(content);
      if (status !== null) sheet.getRange(rowIndex, 3).setValue(status);
      if (path !== null) sheet.getRange(rowIndex, 4).setValue(path);
      Logger.log(`[Sheet] Updated row ${rowIndex} for "${cleanName}"`);
    } else {
      // >> APPEND NEW ROW
      sheet.appendRow([cleanName, content || '', status, path || '']);
      
      // Update Map Immediately
      const newRowIndex = sheet.getLastRow();
      map.set(cleanName, newRowIndex);
      Logger.log(`[Sheet] Appended row ${newRowIndex} for "${cleanName}"`);
    }
    // Flush to ensure data persistence
    SpreadsheetApp.flush();
  } catch(e) { Logger.log("Error syncing to sheet: " + e.message); }
}

function updateMarkdownSheetName(map, oldName, newName) {
  if (!MARKDOWN_SHEET_ID) return;
  const cleanOld = oldName.trim();
  const cleanNew = newName.trim();

  if (!map.has(cleanOld)) return;
  
  try {
    const sheet = SpreadsheetApp.openById(MARKDOWN_SHEET_ID).getSheets()[0];
    const rowIndex = map.get(cleanOld);
    
    sheet.getRange(rowIndex, 1).setValue(cleanNew);
    
    // Update Map
    map.delete(cleanOld);
    map.set(cleanNew, rowIndex);
  } catch(e) {}
}

// ================= MARKDOWN GENERATOR (FIXED RUNTIME) =================
function convertTabToMarkdown(tab) {
  try {
    const documentTab = tab.asDocumentTab();
    if (!documentTab) return "";
    const body = documentTab.getBody();
    if (!body) return "";

    let md = "";
    // FIX: Using getNumChildren + getChild loop instead of getChildren()
    const numChildren = body.getNumChildren();
    
    for (let i = 0; i < numChildren; i++) {
      const child = body.getChild(i);
      const type = child.getType();
      
      if (type === DocumentApp.ElementType.PARAGRAPH) {
        const p = child.asParagraph();
        const text = p.getText();
        if (!text.trim()) { md += "\n"; continue; } 
        
        const heading = p.getHeading();
        if (heading === DocumentApp.ParagraphHeading.NORMAL) {
          md += text + "\n\n";
        } else {
          let prefix = "";
          if (heading === DocumentApp.ParagraphHeading.HEADING1) prefix = "# ";
          else if (heading === DocumentApp.ParagraphHeading.HEADING2) prefix = "## ";
          else if (heading === DocumentApp.ParagraphHeading.HEADING3) prefix = "### ";
          else if (heading === DocumentApp.ParagraphHeading.HEADING4) prefix = "#### ";
          else if (heading === DocumentApp.ParagraphHeading.HEADING5) prefix = "##### ";
          else if (heading === DocumentApp.ParagraphHeading.HEADING6) prefix = "###### ";
          else if (heading === DocumentApp.ParagraphHeading.TITLE) prefix = "# ";
          else if (heading === DocumentApp.ParagraphHeading.SUBTITLE) prefix = "## ";
          md += prefix + text + "\n\n";
        }
      } 
      else if (type === DocumentApp.ElementType.LIST_ITEM) {
        const item = child.asListItem();
        const text = item.getText();
        const nesting = item.getNestingLevel();
        const indent = "  ".repeat(nesting); 
        md += `${indent}* ${text}\n`;
      }
      else if (type === DocumentApp.ElementType.TABLE) {
         md += "[Table content skipped]\n\n";
      }
    }
    return md;
  } catch(e) {
    Logger.log("Markdown generation error: " + e.message);
    return "Error: " + e.message;
  }
}

// ================= UTILITIES (Standard) =================
function cleanupOrphans(state, activeTabIds, markdownSheetMap) {
  const allKnownTabIds = Object.keys(state);
  allKnownTabIds.forEach(tabId => {
    if (!activeTabIds.has(tabId)) {
      const orphanData = state[tabId];
      Logger.log(`🗑️ Cleanup: ${tabId}`);

      if (orphanData.fileId) {
        try { DriveApp.getFileById(orphanData.fileId).setTrashed(true); } catch (e) {}
      }
      if (orphanData.folderId) {
        try {
          const folder = DriveApp.getFolderById(orphanData.folderId);
          if (!folder.getFiles().hasNext() && !folder.getFolders().hasNext()) folder.setTrashed(true);
        } catch (e) {}
      }
      if (orphanData.title) {
        syncToMarkdownSheet(markdownSheetMap, orphanData.title, null, 'Archived', null);
      }
      delete state[tabId];
      SCRIPT_PROPERTIES.deleteProperty(`hash_${tabId}`);
    }
  });
}

function moveItemToFolder(item, targetFolder) {
  const parents = item.getParents();
  let isAlreadyHere = false;
  while (parents.hasNext()) {
    const parent = parents.next();
    if (parent.getId() === targetFolder.getId()) isAlreadyHere = true;
    else {
      try { parent.removeFile(item); } catch (e) { 
        try { parent.removeFolder(item); } catch(e2) {}
      }
    }
  }
  if (!isAlreadyHere) {
    try { targetFolder.addFile(item); } catch (e) {
       try { targetFolder.addFolder(item); } catch(e2) {}
    }
  }
}

function safeRenameFile(id, name) { try { DriveApp.getFileById(id).setName(name); } catch(e) {} }
function safeRenameFolder(id, name) { try { DriveApp.getFolderById(id).setName(name); } catch(e) {} }

function getStoredState() {
  const json = SCRIPT_PROPERTIES.getProperty(STATE_KEY);
  if (!json) return {};
  try { return JSON.parse(json); } catch (e) { return {}; }
}

function getTabContentHash(tab) {
  const documentTab = tab.asDocumentTab();
  const body = documentTab.getBody();
  const content = body.getText();
  const hash = Utilities.computeDigest(Utilities.DigestAlgorithm.MD5, content, Utilities.Charset.UTF_8);
  return hash.map(b => ('0' + ((b < 0) ? 256 + b : b).toString(16)).slice(-2)).join('');
}

function getOrCreateFolder(parentFolder, folderName) {
  const existing = parentFolder.getFoldersByName(folderName);
  if (existing.hasNext()) return existing.next();
  return parentFolder.createFolder(folderName);
}

function getDrivePath(file) {
  try {
    let path = [file.getName()];
    let parent = file.getParents().hasNext() ? file.getParents().next() : null;
    while (parent) {
      path.unshift(parent.getName());
      parent = parent.getParents().hasNext() ? parent.getParents().next() : null;
    }
    return path.join(' / ');
  } catch (e) { return "Unknown Path"; }
}

function exportTabToPDF(documentId, tabId, tabTitle, folder) {
  const exportUrl = `https://docs.google.com/document/d/${documentId}/export?format=pdf&tab=${tabId}`;
  const pdfFileName = `${FILENAME_PREFIX}${tabTitle}${FILENAME_SUFFIX}.pdf`;

  for (let i = 0; i <= MAX_RETRIES; i++) {
    try {
      const response = UrlFetchApp.fetch(exportUrl, {
        headers: { 'Authorization': `Bearer ${ScriptApp.getOAuthToken()}` },
        muteHttpExceptions: true
      });
      if (response.getResponseCode() === 200) {
        const blob = response.getBlob().setName(pdfFileName);
        const existing = folder.getFilesByName(pdfFileName);
        while (existing.hasNext()) existing.next().setTrashed(true);
        return folder.createFile(blob);
      }
      if (response.getResponseCode() === 429) {
        Utilities.sleep(INITIAL_BACKOFF * Math.pow(2, i));
        continue;
      }
    } catch (e) {
      if (i < MAX_RETRIES) Utilities.sleep(INITIAL_BACKOFF * Math.pow(2, i));
    }
  }
  return null;
}

function logToSheet(type, msg, status) {
  if (!LOG_SHEET_ID) return;
  try {
    const sheet = SpreadsheetApp.openById(LOG_SHEET_ID).getSheets()[0];
    sheet.appendRow([new Date().toLocaleString(), type, msg, status]);
  } catch(e) {}
}

function setupTimeDrivenTrigger() {
  const triggers = ScriptApp.getProjectTriggers();
  triggers.forEach(trigger => {
    if (trigger.getHandlerFunction() === 'exportUpdatedTabsToPDF') ScriptApp.deleteTrigger(trigger);
  });
  ScriptApp.newTrigger('exportUpdatedTabsToPDF').timeBased().everyMinutes(1).create();
  Logger.log('Trigger set up.');
}

function forceExportAllTabs() {
  if (!lockCheck()){
    return;
  }
  lockSet();
  try {
    Logger.log('Starting force export...');
    clearMarkdownSheet();
    cleanDriveUsingTrackedState(getStoredState());
    SCRIPT_PROPERTIES.deleteAllProperties(); 
    
    // Re-set the lock after deleteAllProperties wiped it
    lockSet();
    
    exportUpdatedTabsToPDF();
    
  } catch (e) {
    Logger.log(`❌ Force export error: ${e.message}`);
    logToSheet('Error', e.message, 'Failed');
  } finally {
    lockRelease();
  }
}

/**
 * Completely wipes the Markdown sheet.  
 */
function clearMarkdownSheet() {
  if (!MARKDOWN_SHEET_ID) return;
  try {
    const sheet = SpreadsheetApp.openById(MARKDOWN_SHEET_ID).getSheets()[0];
    sheet.clear(); 
  } catch (e) {
    Logger.log("Error clearing sheet: " + e.message);
  }
}

/**
 * Iterates through the provided state object and trashes 
 * the associated files and folders in Google Drive.
 * * @param {Object} state - The JSON object retrieved from SCRIPT_PROPERTIES
 */
function cleanDriveUsingTrackedState(state) {
  if (!state || Object.keys(state).length === 0) return;

  Logger.log(`🗑️ Cleaning ${Object.keys(state).length} tracked items from Drive...`);
  
  Object.values(state).forEach(entry => {
    if (entry.fileId) {
      try { DriveApp.getFileById(entry.fileId).setTrashed(true); } catch (e) {}
    }
    if (entry.folderId) {
      try { DriveApp.getFolderById(entry.folderId).setTrashed(true); } catch (e) {}
    }
  });
}

function forceResetState(){
  if (!lockCheck()){
    return;
  }
  lockSet();
  try{
    // 1. Mark Markdown sheet's all rows as Archived
    clearMarkdownSheet();
    // 2. Physical Wipe: Delete files/folders tracked in current state
    // (Idempotent: Safe to re-run if it times out)
    cleanDriveUsingTrackedState(getStoredState());
    // 3. Logical Wipe: Clear state.
    SCRIPT_PROPERTIES.deleteAllProperties();
  }
  catch (e) {
    Logger.log("Exception occurred resetting state or archiving the sheet.");
    logToSheet('Error', e.message, 'Failed');
  }
  finally{
    lockRelease();
  }
}

// Obsolete: replaced by clearMarkdownSheet.
function archiveAllSheetRows() {
  if (!MARKDOWN_SHEET_ID) return;
  try {
    const sheet = SpreadsheetApp.openById(MARKDOWN_SHEET_ID).getSheets()[0];
    const lastRow = sheet.getLastRow();
    if (lastRow < 2) return;
    
    // Set all status cells (column 3) to "Archived"
    const statusRange = sheet.getRange(1, 3, lastRow - 1, 1);
    const values = statusRange.getValues().map(() => ['Archived']);
    statusRange.setValues(values);
    
    Logger.log(`Pre-archived ${lastRow - 1} rows before force export`);
  } catch(e) {
    Logger.log("Error pre-archiving: " + e.message);
  }
}
