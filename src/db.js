// IndexedDB wrapper for offline-first sync
const DB_NAME = "SubconsciousDB";
const DB_VERSION = 1;

let dbInstance = null;

function openDB() {
  return new Promise((resolve, reject) => {
    if (dbInstance) {
      resolve(dbInstance);
      return;
    }

    const request = indexedDB.open(DB_NAME, DB_VERSION);

    request.onerror = () => reject(request.error);
    request.onsuccess = () => {
      dbInstance = request.result;
      resolve(dbInstance);
    };

    request.onupgradeneeded = (event) => {
      const db = event.target.result;

      // Items store
      if (!db.objectStoreNames.contains("items")) {
        db.createObjectStore("items", { keyPath: "id" });
      }

      // Playlists store
      if (!db.objectStoreNames.contains("playlists")) {
        db.createObjectStore("playlists", { keyPath: "id" });
      }

      // Folders store
      if (!db.objectStoreNames.contains("folders")) {
        db.createObjectStore("folders", { keyPath: "id" });
      }

      // Theme store
      if (!db.objectStoreNames.contains("theme")) {
        db.createObjectStore("theme", { keyPath: "key" });
      }

      // Sync queue: tracks pending operations
      if (!db.objectStoreNames.contains("syncQueue")) {
        const syncStore = db.createObjectStore("syncQueue", { keyPath: "id", autoIncrement: true });
        syncStore.createIndex("status", "status", { unique: false });
        syncStore.createIndex("timestamp", "timestamp", { unique: false });
      }

      // Sync metadata: tracks last sync time and conflicts
      if (!db.objectStoreNames.contains("syncMetadata")) {
        db.createObjectStore("syncMetadata", { keyPath: "key" });
      }
    };
  });
}

// CRUD operations for items
export async function getItems() {
  const db = await openDB();
  return new Promise((resolve, reject) => {
    const tx = db.transaction("items", "readonly");
    const store = tx.objectStore("items");
    const request = store.getAll();
    request.onsuccess = () => resolve(request.result);
    request.onerror = () => reject(request.error);
  });
}

export async function saveItems(items) {
  const db = await openDB();
  return new Promise((resolve, reject) => {
    const tx = db.transaction("items", "readwrite");
    const store = tx.objectStore("items");

    // Clear and repopulate
    store.clear();
    items.forEach(item => store.add(item));

    tx.oncomplete = () => resolve();
    tx.onerror = () => reject(tx.error);
  });
}

export async function addItem(item) {
  const db = await openDB();
  return new Promise((resolve, reject) => {
    const tx = db.transaction(["items", "syncQueue"], "readwrite");
    const itemStore = tx.objectStore("items");
    const queueStore = tx.objectStore("syncQueue");

    itemStore.put(item);
    queueStore.add({
      type: "CREATE",
      entityType: "item",
      entityId: item.id,
      data: item,
      timestamp: Date.now(),
      status: "pending",
      synced: false
    });

    tx.oncomplete = () => resolve();
    tx.onerror = () => reject(tx.error);
  });
}

export async function updateItem(item) {
  const db = await openDB();
  return new Promise((resolve, reject) => {
    const tx = db.transaction(["items", "syncQueue"], "readwrite");
    const itemStore = tx.objectStore("items");
    const queueStore = tx.objectStore("syncQueue");

    itemStore.put(item);
    queueStore.add({
      type: "UPDATE",
      entityType: "item",
      entityId: item.id,
      data: item,
      timestamp: Date.now(),
      status: "pending",
      synced: false
    });

    tx.oncomplete = () => resolve();
    tx.onerror = () => reject(tx.error);
  });
}

export async function deleteItem(itemId) {
  const db = await openDB();
  return new Promise((resolve, reject) => {
    const tx = db.transaction(["items", "syncQueue"], "readwrite");
    const itemStore = tx.objectStore("items");
    const queueStore = tx.objectStore("syncQueue");

    itemStore.delete(itemId);
    queueStore.add({
      type: "DELETE",
      entityType: "item",
      entityId: itemId,
      timestamp: Date.now(),
      status: "pending",
      synced: false
    });

    tx.oncomplete = () => resolve();
    tx.onerror = () => reject(tx.error);
  });
}

// Playlists
export async function getPlaylists() {
  const db = await openDB();
  return new Promise((resolve, reject) => {
    const tx = db.transaction("playlists", "readonly");
    const store = tx.objectStore("playlists");
    const request = store.getAll();
    request.onsuccess = () => resolve(request.result);
    request.onerror = () => reject(request.error);
  });
}

export async function savePlaylists(playlists) {
  const db = await openDB();
  return new Promise((resolve, reject) => {
    const tx = db.transaction(["playlists", "syncQueue"], "readwrite");
    const store = tx.objectStore("playlists");
    const queueStore = tx.objectStore("syncQueue");

    store.clear();
    playlists.forEach(p => store.add(p));

    // Queue the update
    queueStore.add({
      type: "UPDATE",
      entityType: "playlists",
      entityId: "playlists",
      data: playlists,
      timestamp: Date.now(),
      status: "pending",
      synced: false
    });

    tx.oncomplete = () => resolve();
    tx.onerror = () => reject(tx.error);
  });
}

// Folders
export async function getFolders() {
  const db = await openDB();
  return new Promise((resolve, reject) => {
    const tx = db.transaction("folders", "readonly");
    const store = tx.objectStore("folders");
    const request = store.getAll();
    request.onsuccess = () => resolve(request.result);
    request.onerror = () => reject(request.error);
  });
}

export async function saveFolders(folders) {
  const db = await openDB();
  return new Promise((resolve, reject) => {
    const tx = db.transaction(["folders", "syncQueue"], "readwrite");
    const store = tx.objectStore("folders");
    const queueStore = tx.objectStore("syncQueue");

    store.clear();
    folders.forEach((f, idx) => store.add({ id: idx, name: f }));

    queueStore.add({
      type: "UPDATE",
      entityType: "folders",
      entityId: "folders",
      data: folders,
      timestamp: Date.now(),
      status: "pending",
      synced: false
    });

    tx.oncomplete = () => resolve();
    tx.onerror = () => reject(tx.error);
  });
}

// Theme
export async function getTheme() {
  const db = await openDB();
  return new Promise((resolve, reject) => {
    const tx = db.transaction("theme", "readonly");
    const store = tx.objectStore("theme");
    const request = store.get("current");
    request.onsuccess = () => resolve(request.result?.value);
    request.onerror = () => reject(request.error);
  });
}

export async function saveTheme(theme) {
  const db = await openDB();
  return new Promise((resolve, reject) => {
    const tx = db.transaction(["theme", "syncQueue"], "readwrite");
    const store = tx.objectStore("theme");
    const queueStore = tx.objectStore("syncQueue");

    store.put({ key: "current", value: theme });

    queueStore.add({
      type: "UPDATE",
      entityType: "theme",
      entityId: "current",
      data: theme,
      timestamp: Date.now(),
      status: "pending",
      synced: false
    });

    tx.oncomplete = () => resolve();
    tx.onerror = () => reject(tx.error);
  });
}

// Sync Queue operations
export async function getPendingSyncQueue() {
  const db = await openDB();
  return new Promise((resolve, reject) => {
    const tx = db.transaction("syncQueue", "readonly");
    const store = tx.objectStore("syncQueue");
    const index = store.index("status");
    const request = index.getAll("pending");
    request.onsuccess = () => resolve(request.result);
    request.onerror = () => reject(request.error);
  });
}

export async function markSyncQueueAsSynced(queueIds) {
  const db = await openDB();
  return new Promise((resolve, reject) => {
    const tx = db.transaction("syncQueue", "readwrite");
    const store = tx.objectStore("syncQueue");

    queueIds.forEach(id => {
      const request = store.get(id);
      request.onsuccess = () => {
        const item = request.result;
        if (item) {
          item.status = "synced";
          item.synced = true;
          store.put(item);
        }
      };
    });

    tx.oncomplete = () => resolve();
    tx.onerror = () => reject(tx.error);
  });
}

export async function clearSyncQueue() {
  const db = await openDB();
  return new Promise((resolve, reject) => {
    const tx = db.transaction("syncQueue", "readwrite");
    const store = tx.objectStore("syncQueue");
    store.clear();

    tx.oncomplete = () => resolve();
    tx.onerror = () => reject(tx.error);
  });
}

// Sync metadata
export async function getLastSyncTime() {
  const db = await openDB();
  return new Promise((resolve, reject) => {
    const tx = db.transaction("syncMetadata", "readonly");
    const store = tx.objectStore("syncMetadata");
    const request = store.get("lastSync");
    request.onsuccess = () => resolve(request.result?.value || 0);
    request.onerror = () => reject(request.error);
  });
}

export async function setLastSyncTime(timestamp) {
  const db = await openDB();
  return new Promise((resolve, reject) => {
    const tx = db.transaction("syncMetadata", "readwrite");
    const store = tx.objectStore("syncMetadata");
    store.put({ key: "lastSync", value: timestamp });

    tx.oncomplete = () => resolve();
    tx.onerror = () => reject(tx.error);
  });
}
