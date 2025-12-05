// Create and store the document tabs in google docs into the same directory structure as in docs.
// This script creates, updates and deletes the created files. So make sure to provide it with appropriate perms.
// It keeps track of the files it had created with a json file similar to the below example. 
// Other files and folders in the same directory shouldn't be affected, I didn't test this though.
// My doc_structure_state:
// {
//     "t.qa3t443mbhc": {
//         "fileId": "1mRa53nJL6uaNd000L36ULOVAYcUauW00",
//         "folderId": "1DjLsB81LqGu000TL_QyPnKc8K0pim-00"
//     },
//     "t.fczrk1tvmpu2": {
//         "fileId": "1GXLmPuiQFTi9ZcCM0005dpbuBQTBnO00",
//         "folderId": null
//     },
//     "t.j8l6vh31v3ij": {
//         "fileId": "1eEtDJ17nq1aNG000HbQ30S8P9llV8L00",
//         "folderId": "1kX5VHY3KJ2B000t2ek8e__5g8BgowR00"
//     },
//   ..... so on
// }
// Deletes the file if there is a tabId missing in the document and,
// checks if there is a folder with the same name, and empty - deletes it as well.


// Configuration - Replace these with your values
const DOCUMENT_ID = '1sb3UZBOm000_XiI0f6L7eMZF_8sKFIkWCHm0000000'; // Your Google Doc ID
const ROOT_FOLDER_ID = '11Kjl9000-OksLxj_PSI0qd15a0000000';     // Your Drive folder ID

// Rate limiting configuration
const DELAY_BETWEEN_EXPORTS = 2000; // 2 seconds between each export
const MAX_RETRIES = 5; // Maximum retry attempts for 429 errors
const INITIAL_BACKOFF = 1000; // Initial backoff delay (1 second)

const SCRIPT_PROPERTIES = PropertiesService.getScriptProperties();
const STATE_KEY = 'doc_structure_state'; // Key to store the JSON map of TabID -> DriveID

// --- MUTEX LOCK CONFIGURATION ---
const LOCK_KEY = 'script_mutex_lock';
// Safety timeout: If the lock is older than this (10 mins), assume previous run crashed and take over.
// Google Scripts have a max runtime of 6 mins, so 10 mins is a safe buffer.
const LOCK_TIMEOUT_MS = 10 * 60 * 1000; 

function exportUpdatedTabsToPDF() {
  const now = Date.now();
  const lockValue = SCRIPT_PROPERTIES.getProperty(LOCK_KEY);

  // 1. Check if locked
  if (lockValue) {
    const lockTime = parseInt(lockValue, 10);
    // Check if the lock is "fresh"
    if (now - lockTime < LOCK_TIMEOUT_MS) {
      Logger.log('⚠️ Script is already running (Locked). Exiting to prevent overlap.');
      return;
    } else {
      Logger.log('⚠️ Found stale lock from previous crash. Taking over lock.');
    }
  }

  // 2. Set Lock (Mutex)
  // We store the current timestamp so future runs can detect stale locks
  SCRIPT_PROPERTIES.setProperty(LOCK_KEY, now.toString());

  try {
    // --- START MAIN LOGIC ---
    Logger.log('🔒 Lock acquired. Starting export process...');
    
    const doc = DocumentApp.openById(DOCUMENT_ID);
    const rootFolder = DriveApp.getFolderById(ROOT_FOLDER_ID);
    
    // Load the previous state
    let state = getStoredState();
    const activeTabIds = new Set(); // Track IDs seen in this run

    const topLevelTabs = doc.getTabs();
    let exportCount = 0;
    
    // Process hierarchy and update 'state' with current File IDs
    topLevelTabs.forEach(tab => {
      if (tab.getType() === DocumentApp.TabType.DOCUMENT_TAB) {
        exportCount += processTabHierarchy(tab, rootFolder, [], state, activeTabIds);
      }
    });

    // Cleanup Phase: Detect removed tabs and delete their files
    cleanupOrphans(state, activeTabIds);

    // Save the updated state
    SCRIPT_PROPERTIES.setProperty(STATE_KEY, JSON.stringify(state));
    
    // Update last check time
    SCRIPT_PROPERTIES.setProperty('lastDocumentCheck', Date.now().toString());
    
    Logger.log(`Run completed. Exported: ${exportCount}. Updated state saved.`);
    // --- END MAIN LOGIC ---

  } catch (e) {
    Logger.log(`❌ Error during execution: ${e.message}`);
    // Optional: Re-throw if you want it to appear as Failed in the Apps Script dashboard
    // throw e; 
  } finally {
    // 3. Release Lock
    // This block executes whether the script succeeds or fails
    SCRIPT_PROPERTIES.deleteProperty(LOCK_KEY);
    Logger.log('🔓 Lock released.');
  }
}

function processTabHierarchy(tab, parentFolder, pathArray, state, activeTabIds) {
  const tabId = tab.getId();
  const tabTitle = tab.getTitle();
  let exportCount = 0;
  
  // Mark this tab ID as "seen" (Active)
  activeTabIds.add(tabId);

  // Initialize state entry if missing
  if (!state[tabId]) state[tabId] = { fileId: null, folderId: null };
  
  // Get content hash
  const currentHash = getTabContentHash(tab);
  const storedHashKey = `hash_${tabId}`;
  const storedHash = SCRIPT_PROPERTIES.getProperty(storedHashKey);
  const contentChanged = (currentHash !== storedHash);
  
  let currentFileId = state[tabId].fileId;

  if (contentChanged || !currentFileId) {
    // Export needed
    const exportedFile = exportTabToPDF(DOCUMENT_ID, tabId, tabTitle, parentFolder);
    
    if (exportedFile) {
      // Store the new hash
      SCRIPT_PROPERTIES.setProperty(storedHashKey, currentHash);
      
      // Update state with new File ID
      state[tabId].fileId = exportedFile.getId();
      
      const fullPath = pathArray.concat(tabTitle).join(' > ');
      Logger.log(`✓ Exported (changed/new): ${fullPath}`);
      exportCount++;
      Utilities.sleep(DELAY_BETWEEN_EXPORTS);
    } else {
      const fullPath = pathArray.concat(tabTitle).join(' > ');
      Logger.log(`✗ Failed to export: ${fullPath}`);
    }
  } else {
    const fullPath = pathArray.concat(tabTitle).join(' > ');
    Logger.log(`○ Skipped (unchanged): ${fullPath}`);
  }
  
  // Process Children
  const childTabs = tab.getChildTabs();
  
  if (childTabs.length > 0) {
    // Create/get subfolder for this tab's children
    const subFolder = getOrCreateFolder(parentFolder, tabTitle);
    
    // Track the folder ID in the state so we can clean it up later if empty
    state[tabId].folderId = subFolder.getId();
    
    childTabs.forEach(childTab => {
      if (childTab.getType() === DocumentApp.TabType.DOCUMENT_TAB) {
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
      Logger.log(`Detected removed tab (ID: ${tabId}). Cleaning up...`);

      // 1. Delete the PDF File
      if (orphanData.fileId) {
        try {
          const file = DriveApp.getFileById(orphanData.fileId);
          file.setTrashed(true);
          Logger.log(`  - Deleted PDF: ${file.getName()}`);
        } catch (e) {
          Logger.log(`  - PDF already gone or inaccessible (ID: ${orphanData.fileId})`);
        }
      }

      // 2. Delete the Subfolder (if it existed and is now empty)
      if (orphanData.folderId) {
        try {
          const folder = DriveApp.getFolderById(orphanData.folderId);
          // Only delete if empty to be safe
          if (!folder.getFiles().hasNext() && !folder.getFolders().hasNext()) {
            folder.setTrashed(true);
            Logger.log(`  - Deleted empty folder: ${folder.getName()}`);
          } else {
            Logger.log(`  - Folder not empty, skipped deletion: ${folder.getName()}`);
          }
        } catch (e) {
          Logger.log(`  - Folder already gone (ID: ${orphanData.folderId})`);
        }
      }

      // 3. Remove from state object
      delete state[tabId];
      
      // Also clean up the hash property to keep Property store clean
      SCRIPT_PROPERTIES.deleteProperty(`hash_${tabId}`);
    }
  });
}

function getStoredState() {
  const json = SCRIPT_PROPERTIES.getProperty(STATE_KEY);
  if (!json) return {};
  try {
    return JSON.parse(json);
  } catch (e) {
    Logger.log('Error parsing state, resetting.');
    return {};
  }
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
  if (existingFolders.hasNext()) {
    return existingFolders.next();
  } else {
    return parentFolder.createFolder(folderName);
  }
}

// Modified to return the File object on success, null on failure
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
        if (existingFiles.hasNext()) {
          const existingFile = existingFiles.next();
          existingFile.setTrashed(true);
        }
        
        // Create new file and return it
        return folder.createFile(blob); 
      }
      
      if (response.getResponseCode() === 429) {
        if (attempt < MAX_RETRIES) {
          const backoffDelay = INITIAL_BACKOFF * Math.pow(2, attempt);
          Utilities.sleep(backoffDelay);
          continue;
        }
      }
      return null;
    } catch (e) {
      if (attempt < MAX_RETRIES) {
        const backoffDelay = INITIAL_BACKOFF * Math.pow(2, attempt);
        Utilities.sleep(backoffDelay);
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
