import {
  getPendingSyncQueue,
  markSyncQueueAsSynced,
  clearSyncQueue,
  getLastSyncTime,
  setLastSyncTime,
  getItems,
  savePlaylists,
  saveFolders,
  saveTheme,
} from "./db.js";

let isSyncing = false;
let syncCallbacks = [];

export function onSyncStatusChange(callback) {
  syncCallbacks.push(callback);
  return () => {
    syncCallbacks = syncCallbacks.filter(cb => cb !== callback);
  };
}

function notifySyncStatus(status) {
  syncCallbacks.forEach(cb => cb(status));
}

export async function syncWithServer() {
  if (isSyncing) return;
  isSyncing = true;
  notifySyncStatus("syncing");

  try {
    const pendingOps = await getPendingSyncQueue();

    if (pendingOps.length === 0) {
      notifySyncStatus("idle");
      isSyncing = false;
      return;
    }

    // Send pending operations to server
    const response = await fetch("/api/sync", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({
        operations: pendingOps,
        lastSync: await getLastSyncTime()
      })
    });

    if (!response.ok) {
      notifySyncStatus("error");
      isSyncing = false;
      return;
    }

    const result = await response.json();

    // Mark synced operations
    if (result.syncedIds && result.syncedIds.length > 0) {
      await markSyncQueueAsSynced(result.syncedIds);
    }

    // Handle any server-side updates (conflicts resolved by server)
    if (result.items) {
      await savePlaylists(result.items);
    }
    if (result.playlists) {
      await savePlaylists(result.playlists);
    }
    if (result.folders) {
      await saveFolders(result.folders);
    }
    if (result.theme) {
      await saveTheme(result.theme);
    }

    // Update last sync time
    await setLastSyncTime(Date.now());

    notifySyncStatus("synced");
  } catch (error) {
    console.error("Sync error:", error);
    notifySyncStatus("error");
  } finally {
    isSyncing = false;
  }
}

export function setupOfflineSync() {
  // Detect online/offline
  const handleOnline = async () => {
    console.log("Back online, syncing...");
    await syncWithServer();
  };

  const handleOffline = () => {
    console.log("Gone offline, queuing operations");
  };

  window.addEventListener("online", handleOnline);
  window.addEventListener("offline", handleOffline);

  // Sync periodically when online
  if (navigator.onLine) {
    const syncInterval = setInterval(async () => {
      if (navigator.onLine) {
        await syncWithServer();
      }
    }, 30000); // Sync every 30 seconds when online

    return () => {
      clearInterval(syncInterval);
      window.removeEventListener("online", handleOnline);
      window.removeEventListener("offline", handleOffline);
    };
  }

  return () => {
    window.removeEventListener("online", handleOnline);
    window.removeEventListener("offline", handleOffline);
  };
}

export function isOnline() {
  return navigator.onLine;
}

export async function getSyncStatus() {
  const pendingOps = await getPendingSyncQueue();
  return {
    isOnline: navigator.onLine,
    hasPending: pendingOps.length > 0,
    pendingCount: pendingOps.length,
    isSyncing
  };
}
