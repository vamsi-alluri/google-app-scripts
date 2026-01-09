// Configuration - Replace these with your values
const DOCUMENT_ID = 'configs'; // Your Google Doc ID
const ROOT_FOLDER_ID = 'configs';         // Your Drive folder ID 
const LOG_SHEET_ID = 'configs'; // <--- UPDATED WITH YOUR ID

// Rate limiting configuration
const DELAY_BETWEEN_EXPORTS = 2000; 
const MAX_RETRIES = 5; 
const INITIAL_BACKOFF = 1000; 

const SCRIPT_PROPERTIES = PropertiesService.getScriptProperties();
const STATE_KEY = 'doc_structure_state'; // Key to store the JSON map of TabID -> DriveID

// --- MUTEX LOCK CONFIGURATION ---
const LOCK_KEY = 'script_mutex_lock';
// Safety timeout: If the lock is older than this (10 mins), assume previous run crashed and take over.
// Google Scripts have a max runtime of 6 mins, so 10 mins is a safe buffer.
const LOCK_TIMEOUT_MS = 8 * 60 * 1000; 

function exportUpdatedTabsToPDF() {
  const now = Date.now();
  const lockValue = SCRIPT_PROPERTIES.getProperty(LOCK_KEY);

  // 1. Check if locked
  if (lockValue) {
    const lockTime = parseInt(lockValue, 10);
    if (now - lockTime < LOCK_TIMEOUT_MS) {
      Logger.log('⚠️ Script is already running (Locked). Exiting.');
      return;
    } else {
      Logger.log('⚠️ Found stale lock. Taking over.');
      logToSheet('System', 'Stale lock detected. Taking over execution.', 'Warning');
    }
  }

  // 2. Set Lock
  SCRIPT_PROPERTIES.setProperty(LOCK_KEY, now.toString());

  try {
    // --- START MAIN LOGIC ---
    Logger.log('🔒 Lock acquired. Starting export process...');
    
    const doc = DocumentApp.openById(DOCUMENT_ID);
    const rootFolder = DriveApp.getFolderById(ROOT_FOLDER_ID);
    
    // Load state
    let state = getStoredState();
    const activeTabIds = new Set(); 

    const topLevelTabs = doc.getTabs();
    let exportCount = 0;
    
    // Process hierarchy
    topLevelTabs.forEach(tab => {
      if (tab.getType() === DocumentApp.TabType.DOCUMENT_TAB) {
        exportCount += processTabHierarchy(tab, rootFolder, [], state, activeTabIds);
      }
    });

    // Cleanup Phase
    cleanupOrphans(state, activeTabIds);

    // Save state
    SCRIPT_PROPERTIES.setProperty(STATE_KEY, JSON.stringify(state));
    
    // Update last check time
    SCRIPT_PROPERTIES.setProperty('lastDocumentCheck', Date.now().toString());
    
    Logger.log(`Run completed. Exported: ${exportCount}.`);
    logToSheet('Info', `Run completed. ${exportCount} files exported.`, 'Success');
  } catch (e) {
    Logger.log(`❌ Error: ${e.message}`);
    logToSheet('Error', e.message, 'Failed');
  } finally {
    // 3. Release Lock
    SCRIPT_PROPERTIES.deleteProperty(LOCK_KEY);
    Logger.log('🔓 Lock released.');
  }
}

function processTabHierarchy(tab, parentFolder, pathArray, state, activeTabIds) {
  const tabId = tab.getId();
  const tabTitle = tab.getTitle();
  let exportCount = 0;
  
  activeTabIds.add(tabId);

  // Initialize state if missing
  if (!state[tabId]) state[tabId] = { fileId: null, folderId: null, title: tabTitle };
  
  let stateDirty = false; // Track if we need to save changes

  // 1. SYNC TITLES (Handle Renaming)
  const storedTitle = state[tabId].title;
  if (storedTitle && storedTitle !== tabTitle) {
    Logger.log(`📝 Rename detected: "${storedTitle}" -> "${tabTitle}"`);
    
    if (state[tabId].fileId) {
      try { DriveApp.getFileById(state[tabId].fileId).setName(`${tabTitle}.pdf`); } 
      catch (e) { console.warn('Could not rename file', e); }
    }
    if (state[tabId].folderId) {
      try { DriveApp.getFolderById(state[tabId].folderId).setName(tabTitle); } 
      catch (e) { console.warn('Could not rename folder', e); }
    }
    state[tabId].title = tabTitle; 
    stateDirty = true; // Mark for save
  }

  // 2. CHECK CONTENT
  const currentHash = getTabContentHash(tab);
  const storedHashKey = `hash_${tabId}`;
  const storedHash = SCRIPT_PROPERTIES.getProperty(storedHashKey);
  const contentChanged = (currentHash !== storedHash);
  
  if (contentChanged || !state[tabId].fileId) {
    const fullPath = pathArray.concat(tabTitle).join(' > ');
    const exportedFile = exportTabToPDF(DOCUMENT_ID, tabId, tabTitle, parentFolder);
    
    if (exportedFile) {
      // Update Hash immediately
      SCRIPT_PROPERTIES.setProperty(storedHashKey, currentHash);
      
      // Update State
      state[tabId].fileId = exportedFile.getId();
      state[tabId].title = tabTitle;
      
      exportCount++;
      stateDirty = true; // Mark for save
      
      Logger.log(`✓ Exported: ${fullPath}`);
      Utilities.sleep(DELAY_BETWEEN_EXPORTS);
    }
  } else {
    // 3. SYNC LOCATION (Move Logic)
    if (state[tabId].fileId) {
      try {
        const file = DriveApp.getFileById(state[tabId].fileId);
        // Only move if not in correct folder
        const parents = file.getParents();
        let isCorrect = false;
        while (parents.hasNext()) { if (parents.next().getId() === parentFolder.getId()) isCorrect = true; }
        
        if (!isCorrect) {
          moveItemToFolder(file, parentFolder);
          // No need to update 'state' here as IDs didn't change, just location
        }
      } catch (e) {
        Logger.log(`Warning: Could not check/move file ${state[tabId].fileId}`);
      }
    }
  }

  // --- CRITICAL FIX: INCREMENTAL SAVE ---
  // If we changed the state (Renamed or Exported), save it NOW.
  if (stateDirty) {
    SCRIPT_PROPERTIES.setProperty(STATE_KEY, JSON.stringify(state));
  }
  // --------------------------------------

  // 4. PROCESS CHILDREN
  const childTabs = tab.getChildTabs();
  if (childTabs.length > 0) {
    let subFolder;
    let folderIdChanged = false;

    if (state[tabId].folderId) {
      try {
        subFolder = DriveApp.getFolderById(state[tabId].folderId);
        moveItemToFolder(subFolder, parentFolder); 
      } catch (e) {
        subFolder = getOrCreateFolder(parentFolder, tabTitle);
        state[tabId].folderId = subFolder.getId();
        folderIdChanged = true;
      }
    } else {
      subFolder = getOrCreateFolder(parentFolder, tabTitle);
      state[tabId].folderId = subFolder.getId();
      folderIdChanged = true;
    }
    
    // If we just created a new folder ID, save state immediately
    if (folderIdChanged) {
      SCRIPT_PROPERTIES.setProperty(STATE_KEY, JSON.stringify(state));
    }

    childTabs.forEach(childTab => {
      if (childTab.getType() === DocumentApp.TabType.DOCUMENT_TAB) {
        exportCount += processTabHierarchy(childTab, subFolder, pathArray.concat(tabTitle), state, activeTabIds);
      }
    });
  }
  
  return exportCount;
}

/**
 * Moves a File or Folder to the target folder if it isn't already there.
 * This fixes "Duplicates" by removing it from the old location.
 */
function moveItemToFolder(item, targetFolder) {
  const parents = item.getParents();
  let isAlreadyHere = false;
  
  // Check current parents
  while (parents.hasNext()) {
    const parent = parents.next();
    if (parent.getId() === targetFolder.getId()) {
      isAlreadyHere = true;
    } else {
      // Remove from old parent (This fixes the duplication/ghosting)
      try { parent.removeFile(item); } catch (e) { 
        // Try removing as folder if file fails (API quirk)
        try { parent.removeFolder(item); } catch(e2) {}
      }
    }
  }
  
  // Add to new parent if needed
  if (!isAlreadyHere) {
    try { targetFolder.addFile(item); } catch (e) {
       try { targetFolder.addFolder(item); } catch(e2) {}
    }
    Logger.log(`Moved item "${item.getName()}" to folder "${targetFolder.getName()}"`);
  }
}

/**
 * Compares known state against currently active tabs.
 * Deletes files/folders for tabs that no longer exist.
 */
function cleanupOrphans(state, activeTabIds) {
  const allKnownTabIds = Object.keys(state);
  
  allKnownTabIds.forEach(tabId => {
    // If the tab ID from history is NOT in the current document structure
    if (!activeTabIds.has(tabId)) {
      const orphanData = state[tabId];
      Logger.log(`Detected removed tab (ID: ${tabId}). Cleaning up...`);

      // 1. Delete PDF
      if (orphanData.fileId) {
        try {
          const file = DriveApp.getFileById(orphanData.fileId);
          const fileName = file.getName();
          file.setTrashed(true);
          Logger.log(` - Deleted PDF: ${fileName}`);
          logToSheet('Cleanup', `Deleted PDF: ${fileName}`, 'Success');
        } catch (e) {
            logToSheet('Cleanup', `PDF missing/inaccessible (ID: ${orphanData.fileId})`, 'Warning');
        }
      }

      // 2. Delete Subfolder
      if (orphanData.folderId) {
        try {
          const folder = DriveApp.getFolderById(orphanData.folderId);
          // Only delete if empty to be safe
          if (!folder.getFiles().hasNext() && !folder.getFolders().hasNext()) {
            const folderName = folder.getName();
            folder.setTrashed(true);
            Logger.log(` - Deleted empty folder: ${folderName}`);
            logToSheet('Cleanup', `Deleted empty folder: ${folderName}`, 'Success'); 
          }
        } catch (e) {
           // Folder likely already gone
        }
      }

      // 3. Remove from state object
      delete state[tabId];
      
      // Also clean up the hash property to keep Property store clean
      SCRIPT_PROPERTIES.deleteProperty(`hash_${tabId}`);
    }
  });
}

/**
 * Helper function to append logs to the Google Sheet
 */
function logToSheet(actionType, message, status) {
  try {
    if (!LOG_SHEET_ID) return;
    
    const sheet = SpreadsheetApp.openById(LOG_SHEET_ID).getSheets()[0];
    const timestamp = new Date().toLocaleString();
    
    // Check if headers exist, if not create them (Optional, good for first run)
    if (sheet.getLastRow() === 0) {
        sheet.appendRow(['Timestamp', 'Action Type', 'Message', 'Status']);
    }

    sheet.appendRow([timestamp, actionType, message, status]);
  } catch (e) {
    Logger.log(`Failed to write to log sheet: ${e.message}`);
  }
}

// ... (Standard Helpers below this line) ...

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
  return hash.map(byte => {
    const v = (byte < 0) ? 256 + byte : byte;
    return ('0' + v.toString(16)).slice(-2);
  }).join('');
}

function getOrCreateFolder(parentFolder, folderName) {
  const existingFolders = parentFolder.getFoldersByName(folderName);
  if (existingFolders.hasNext()) return existingFolders.next();
  return parentFolder.createFolder(folderName);
}

function exportTabToPDF(documentId, tabId, tabTitle, folder) {
  const exportUrl = `https://docs.google.com/document/d/${documentId}/export?format=pdf&tab=${tabId}`;
  
  for (let attempt = 0; attempt <= MAX_RETRIES; attempt++) {
    try {
      const response = UrlFetchApp.fetch(exportUrl, {
        headers: { 'Authorization': `Bearer ${ScriptApp.getOAuthToken()}` },
        muteHttpExceptions: true
      });
      
      if (response.getResponseCode() === 200) {
        const blob = response.getBlob();
        const pdfFileName = `${tabTitle}.pdf`;
        blob.setName(pdfFileName);
        
        // Check if file already exists and replace it
        const existingFiles = folder.getFilesByName(pdfFileName);
        if (existingFiles.hasNext()) existingFiles.next().setTrashed(true);
        return folder.createFile(blob); 
      }
      if (response.getResponseCode() === 429 && attempt < MAX_RETRIES) {
        Utilities.sleep(INITIAL_BACKOFF * Math.pow(2, attempt));
        continue;
      }
      return null;
    } catch (e) {
      if (attempt < MAX_RETRIES) {
        Utilities.sleep(INITIAL_BACKOFF * Math.pow(2, attempt));
        continue;
      }
      return null;
    }
  }
  return null;
}

// Setup function - run this once
function setupTimeDrivenTrigger() {
  const triggers = ScriptApp.getProjectTriggers();
  triggers.forEach(trigger => {
    if (trigger.getHandlerFunction() === 'exportUpdatedTabsToPDF') {
      ScriptApp.deleteTrigger(trigger);
    }
  });
  
  ScriptApp.newTrigger('exportUpdatedTabsToPDF')
    .timeBased()
    .everyMinutes(1)
    .create();
  Logger.log('Trigger set up.');
}

function forceExportAllTabs() {
  // Clear both the state/hashes AND the lock
  SCRIPT_PROPERTIES.deleteAllProperties(); 
  exportUpdatedTabsToPDF();
}
