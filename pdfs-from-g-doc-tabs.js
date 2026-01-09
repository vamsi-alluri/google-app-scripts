/**
 * Google Docs Tab to Drive PDF Exporter
 * * 1. SYNC: Creates and stores document tabs from a Google Doc into Google Drive.
 * - Mirrors the hierarchical structure of the Doc (Child Tabs -> Subfolders).
 * - Updates PDFs if the tab content changes (MD5 Hash check).
 * - Renames PDFs and Folders if the Tab is renamed.
 * - Moves PDFs and Folders if the Tab is moved to a different parent in the Doc.
 * * 2. CLEANUP: Handles deletions automatically.
 * - If a tab is deleted from the Doc, the corresponding PDF is trashed.
 * - If a folder becomes empty after deletion, the folder is trashed.
 * * 3. STATE: Keeps track of files using a JSON object in Script Properties.
 * - This prevents duplicate exports and allows the script to "resume" if it crashes.
 * - State Example:
 * {
 * "t.qa3t443mbhc": {
 * "fileId": "1mRa53nJL6uaNd000L36ULOVAYcUauW00",
 * "folderId": "1DjLsB81LqGu000TL_QyPnKc8K0pim-00",
 * "title": "My Tab Name",
 * "parentName": "Parent Section Name"
 * },
 * "t.fczrk1tvmpu2": {
 * "fileId": "1GXLmPuiQFTi9ZcCM0005dpbuBQTBnO00",
 * "folderId": null,
 * "title": "Orphan Tab",
 * "parentName": "ROOT"
 * }
 * }
 * * 4. PERMISSIONS: Ensure this script has Edit access to the target Drive Folder.
 * Note: Other files in the target directory are ignored/unaffected.
 */

// Configuration - Replace these with your values
const DOCUMENT_ID = '1sb3UZBaaaBYN_XiI0f6L7eMZF_8sKFIkaaaaaGCWMs'; 
const ROOT_FOLDER_ID = '11Kjaaaqp-OksLxj_PSI0qd15aaaaa4pX';         
const LOG_SHEET_ID = '1jkqONaaaXNLJTYEkMOEu9W4b4pU79fd4HNsaaaabFA8'; 

// Rate limiting configuration
const DELAY_BETWEEN_EXPORTS = 2500; // 2.5 seconds to be safe
const MAX_RETRIES = 3; 
const INITIAL_BACKOFF = 1500; 

const SCRIPT_PROPERTIES = PropertiesService.getScriptProperties();
const STATE_KEY = 'doc_structure_state'; 
const LOCK_KEY = 'script_mutex_lock';
const LOCK_TIMEOUT_MS = 9 * 60 * 1000; // 9 mins (Safe buffer for 6 min runtime)

function exportUpdatedTabsToPDF() {
  const now = Date.now();
  const lockValue = SCRIPT_PROPERTIES.getProperty(LOCK_KEY);

  // 1. Mutex Lock Check
  if (lockValue) {
    const lockTime = parseInt(lockValue, 10);
    // If lock is older than timeout, assume crash and take over
    if (now - lockTime < LOCK_TIMEOUT_MS) {
      Logger.log('⚠️ Script is already running (Locked). Exiting.');
      return;
    } else {
      logToSheet('System', 'Stale lock detected. Taking over.', 'Warning');
    }
  }

  // 2. Set Lock
  SCRIPT_PROPERTIES.setProperty(LOCK_KEY, now.toString());

  try {
    Logger.log('🔒 Lock acquired. Starting export...');
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
        // Path starts empty for top level
        exportCount += processTabHierarchy(tab, rootFolder, [], state, activeTabIds);
      }
    });

    // Cleanup Phase: Delete files for tabs that no longer exist
    cleanupOrphans(state, activeTabIds);

    // Save final state (Cleaned up version)
    SCRIPT_PROPERTIES.setProperty(STATE_KEY, JSON.stringify(state));
    SCRIPT_PROPERTIES.setProperty('lastDocumentCheck', Date.now().toString());
    
    Logger.log(`Run completed. Exported: ${exportCount}.`);
    if (exportCount > 0) logToSheet('Info', `Run completed. ${exportCount} files exported.`, 'Success');
    
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
  if (!state[tabId]) state[tabId] = { fileId: null, folderId: null, title: tabTitle, parentName: null };
  
  let stateDirty = false;

  // --- 1. PARENT/STRUCTURE CHECK (The Optimization) ---
  // pathArray contains parent titles. The last one is the direct parent.
  const currentParentName = pathArray.length > 0 ? pathArray[pathArray.length - 1] : 'ROOT';
  const savedParentName = state[tabId].parentName;
  
  // Flag: check drive ONLY if the parent name changed in the Doc structure
  const structureChanged = (state[tabId].fileId && currentParentName !== savedParentName);

  // --- 2. RENAME CHECK ---
  const storedTitle = state[tabId].title;
  if (storedTitle && storedTitle !== tabTitle) {
    Logger.log(`📝 Rename detected: "${storedTitle}" -> "${tabTitle}"`);
    // Attempt rename in Drive
    if (state[tabId].fileId) safeRenameFile(state[tabId].fileId, `${tabTitle}.pdf`);
    if (state[tabId].folderId) safeRenameFolder(state[tabId].folderId, tabTitle);
    
    state[tabId].title = tabTitle; 
    stateDirty = true; 
  }

  // --- 3. EXPORT CHECK ---
  const currentHash = getTabContentHash(tab);
  const storedHashKey = `hash_${tabId}`;
  const storedHash = SCRIPT_PROPERTIES.getProperty(storedHashKey);
  const contentChanged = (currentHash !== storedHash);
  
  // Verify file actually exists (Self-Healing)
  let fileExists = false;
  if (state[tabId].fileId) {
    try {
      DriveApp.getFileById(state[tabId].fileId);
      fileExists = true;
    } catch (e) {
      Logger.log(`⚠️ File missing for "${tabTitle}". Will re-export.`);
      state[tabId].fileId = null; // Reset state so we export again
    }
  }

  if (contentChanged || !fileExists) {
    // >> EXPORT ACTION
    const fullPath = pathArray.concat(tabTitle).join(' > ');
    const exportedFile = exportTabToPDF(DOCUMENT_ID, tabId, tabTitle, parentFolder);
    
    if (exportedFile) {
      SCRIPT_PROPERTIES.setProperty(storedHashKey, currentHash);
      
      state[tabId].fileId = exportedFile.getId();
      state[tabId].title = tabTitle;
      state[tabId].parentName = currentParentName; // Sync parent
      
      exportCount++;
      stateDirty = true;
      Logger.log(`✓ Exported: ${fullPath}`);
      Utilities.sleep(DELAY_BETWEEN_EXPORTS);
    }
  } else {
    // >> NO EXPORT, BUT CHECK STRUCTURE
    // Only verify location if structureChanged is TRUE. (Saves time!)
    if (structureChanged) {
      Logger.log(`↻ Structure change for "${tabTitle}". Verifying location...`);
      if (state[tabId].fileId) {
        try {
          const file = DriveApp.getFileById(state[tabId].fileId);
          moveItemToFolder(file, parentFolder);
          state[tabId].parentName = currentParentName; // Update state
          stateDirty = true;
        } catch (e) {
           // File likely gone, next run will catch it
        }
      }
    }
  }

  // --- INCREMENTAL SAVE (Prevents data loss on timeout) ---
  if (stateDirty) {
    SCRIPT_PROPERTIES.setProperty(STATE_KEY, JSON.stringify(state));
  }

  // --- 4. PROCESS CHILDREN ---
  const childTabs = tab.getChildTabs();
  
  if (childTabs.length > 0) {
    let subFolder;
    let folderDirty = false;
    
    // Check if folder needs moving (Structure Check)
    const folderNeedsMove = (state[tabId].folderId && currentParentName !== savedParentName);

    if (state[tabId].folderId) {
      try {
        subFolder = DriveApp.getFolderById(state[tabId].folderId);
        if (folderNeedsMove) {
           moveItemToFolder(subFolder, parentFolder);
        }
      } catch (e) {
        // Folder missing/deleted manually
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
        // Pass current title as parent for the next level
        exportCount += processTabHierarchy(childTab, subFolder, pathArray.concat(tabTitle), state, activeTabIds);
      }
    });
  }
  
  return exportCount;
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
      Logger.log(`🗑️ Tab removed (ID: ${tabId}). Cleaning up Drive...`);

      // 1. Delete PDF
      if (orphanData.fileId) {
        try {
          DriveApp.getFileById(orphanData.fileId).setTrashed(true);
          logToSheet('Cleanup', `Deleted PDF for removed tab`, 'Success');
        } catch (e) {}
      }

      // 2. Delete Folder
      if (orphanData.folderId) {
        try {
          const folder = DriveApp.getFolderById(orphanData.folderId);
          // 3. Only delete if empty
          if (!folder.getFiles().hasNext() && !folder.getFolders().hasNext()) {
             folder.setTrashed(true);
             logToSheet('Cleanup', `Deleted empty folder for removed tab`, 'Success');
          }
        } catch (e) {}
      }

      // 4. Remove from state object
      delete state[tabId];
      SCRIPT_PROPERTIES.deleteProperty(`hash_${tabId}`);
    }
  });
}

// --- HELPER FUNCTIONS ---

function moveItemToFolder(item, targetFolder) {
  const parents = item.getParents();
  let isAlreadyHere = false;
  // Check if it's already in the target folder
  while (parents.hasNext()) {
    const parent = parents.next();
    if (parent.getId() === targetFolder.getId()) {
      isAlreadyHere = true;
    } else {
      // Remove from old parent (Fixes duplication/ghosting)
      try { parent.removeFile(item); } catch (e) { 
        try { parent.removeFolder(item); } catch(e2) {}
      }
    }
  }
  // Add to new parent if needed
  if (!isAlreadyHere) {
    try { targetFolder.addFile(item); } catch (e) {
       try { targetFolder.addFolder(item); } catch(e2) {}
    }
  }
}

function safeRenameFile(id, name) {
  try { DriveApp.getFileById(id).setName(name); } catch(e) {}
}
function safeRenameFolder(id, name) {
  try { DriveApp.getFolderById(id).setName(name); } catch(e) {}
}

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

function exportTabToPDF(documentId, tabId, tabTitle, folder) {
  const exportUrl = `https://docs.google.com/document/d/${documentId}/export?format=pdf&tab=${tabId}`;
  
  for (let i = 0; i <= MAX_RETRIES; i++) {
    try {
      const response = UrlFetchApp.fetch(exportUrl, {
        headers: { 'Authorization': `Bearer ${ScriptApp.getOAuthToken()}` },
        muteHttpExceptions: true
      });
      
      if (response.getResponseCode() === 200) {
        const blob = response.getBlob().setName(`${tabTitle}.pdf`);
        
        // Remove existing file with same name to prevent duplicates
        const existing = folder.getFilesByName(`${tabTitle}.pdf`);
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
