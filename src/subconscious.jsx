import React, { useEffect, useMemo, useRef, useState } from 'react';
import { createRoot } from 'react-dom/client';
import './index.css';

let dbInstance = null;

function openDB() {
  return new Promise((resolve, reject) => {
    if (dbInstance) {
      resolve(dbInstance);
      return;
    }
    const request = indexedDB.open("SubconsciousDB", 2);
    request.onerror = () => reject(request.error);
    request.onsuccess = () => {
      dbInstance = request.result;
      resolve(dbInstance);
    };
    request.onupgradeneeded = (event) => {
      const db = event.target.result;
      if (!db.objectStoreNames.contains("items")) db.createObjectStore("items", { keyPath: "id" });
      if (!db.objectStoreNames.contains("playlists")) db.createObjectStore("playlists", { keyPath: "id" });
      if (!db.objectStoreNames.contains("folders")) db.createObjectStore("folders", { keyPath: "id" });
      if (!db.objectStoreNames.contains("theme")) db.createObjectStore("theme", { keyPath: "key" });
      if (!db.objectStoreNames.contains("syncQueue")) {
        const sq = db.createObjectStore("syncQueue", { keyPath: "id", autoIncrement: true });
        sq.createIndex("status", "status", { unique: false });
      }
      // v2: dedicated store for audio binary data — keeps items store lean
      if (!db.objectStoreNames.contains("audioBlobs")) {
        db.createObjectStore("audioBlobs", { keyPath: "id" });
      }
    };
  });
}

async function dbGet(storeName, key) {
  const db = await openDB();
  return new Promise((resolve, reject) => {
    const tx = db.transaction(storeName, "readonly");
    const store = tx.objectStore(storeName);
    const request = store.get(key);
    request.onsuccess = () => resolve(request.result);
    request.onerror = () => reject(request.error);
  });
}

async function dbGetAll(storeName) {
  const db = await openDB();
  return new Promise((resolve, reject) => {
    const tx = db.transaction(storeName, "readonly");
    const store = tx.objectStore(storeName);
    const request = store.getAll();
    request.onsuccess = () => resolve(request.result);
    request.onerror = () => reject(request.error);
  });
}

async function dbPut(storeName, data) {
  const db = await openDB();
  return new Promise((resolve, reject) => {
    const tx = db.transaction(storeName, "readwrite");
    const store = tx.objectStore(storeName);
    store.put(data);
    tx.oncomplete = () => resolve();
    tx.onerror = () => reject(tx.error);
  });
}

async function dbClear(storeName) {
  const db = await openDB();
  return new Promise((resolve, reject) => {
    const tx = db.transaction(storeName, "readwrite");
    const store = tx.objectStore(storeName);
    store.clear();
    tx.oncomplete = () => resolve();
    tx.onerror = () => reject(tx.error);
  });
}

// Batch-write all items in a single readwrite transaction (fast, no sequential awaits)
async function dbPutAllItems(items) {
  const db = await openDB();
  return new Promise((resolve, reject) => {
    const tx = db.transaction("items", "readwrite");
    const store = tx.objectStore("items");
    store.clear();
    items.forEach(item => store.put(item));
    tx.oncomplete = () => resolve();
    tx.onerror = () => reject(tx.error);
  });
}

// Audio binary store — ArrayBuffer in, ArrayBuffer out
async function saveAudioBlob(id, buffer, mime) {
  const db = await openDB();
  return new Promise((resolve, reject) => {
    const tx = db.transaction("audioBlobs", "readwrite");
    tx.objectStore("audioBlobs").put({ id, buffer, mime: mime || "audio/mpeg" });
    tx.oncomplete = () => resolve();
    tx.onerror = () => reject(tx.error);
  });
}
async function loadAudioBlob(id) {
  const db = await openDB();
  return new Promise((resolve, reject) => {
    const tx = db.transaction("audioBlobs", "readonly");
    const req = tx.objectStore("audioBlobs").get(id);
    req.onsuccess = () => resolve(req.result || null);
    req.onerror = () => reject(req.error);
  });
}
async function dbDeleteAudioBlob(id) {
  const db = await openDB();
  return new Promise((resolve, reject) => {
    const tx = db.transaction("audioBlobs", "readwrite");
    tx.objectStore("audioBlobs").delete(id);
    tx.oncomplete = () => resolve();
    tx.onerror = () => reject(tx.error);
  });
}

async function queueOp(type, entityType, entityId, data = null) {
  const db = await openDB();
  return new Promise((resolve, reject) => {
    const tx = db.transaction("syncQueue", "readwrite");
    const store = tx.objectStore("syncQueue");
    store.add({ type, entityType, entityId, data, timestamp: Date.now(), status: "pending" });
    tx.oncomplete = () => resolve();
    tx.onerror = () => reject(tx.error);
  });
}

async function getPendingOps() {
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

async function markOpsSynced(opIds) {
  const db = await openDB();
  return new Promise((resolve, reject) => {
    const tx = db.transaction("syncQueue", "readwrite");
    const store = tx.objectStore("syncQueue");
    opIds.forEach(id => {
      const getReq = store.get(id);
      getReq.onsuccess = () => {
        const op = getReq.result;
        if (op) {
          op.status = "synced";
          store.put(op);
        }
      };
    });
    tx.oncomplete = () => resolve();
    tx.onerror = () => reject(tx.error);
  });
}

let syncCallbacks = [];
function onSyncChange(cb) {
  syncCallbacks.push(cb);
  return () => { syncCallbacks = syncCallbacks.filter(c => c !== cb); };
}

async function syncWithServer() {
  try {
    const ops = await getPendingOps();
    if (ops.length === 0) {
      syncCallbacks.forEach(cb => cb({ syncing: false, pending: 0 }));
      return;
    }
    syncCallbacks.forEach(cb => cb({ syncing: true, pending: ops.length }));

    const response = await fetch("/api/sync", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ operations: ops })
    });

    if (response.ok) {
      const result = await response.json();
      if (result.syncedIds) {
        await markOpsSynced(result.syncedIds);
      }
      syncCallbacks.forEach(cb => cb({ syncing: false, pending: 0, synced: true }));
    } else {
      syncCallbacks.forEach(cb => cb({ syncing: false, pending: ops.length, error: true }));
    }
  } catch (err) {
    console.error("Sync error:", err);
    const ops = await getPendingOps();
    syncCallbacks.forEach(cb => cb({ syncing: false, pending: ops.length, error: true }));
  }
}

function setupOfflineSync() {
  // Static hosting (GitHub Pages) — no server to sync with, skip interval
  return () => {
  };
}

// Voice recording utility
async function requestMicrophoneAccess() {
  try {
    const stream = await navigator.mediaDevices.getUserMedia({ audio: true });
    return stream;
  } catch (err) {
    console.error("Microphone access denied:", err);
    throw new Error("Microphone access required to record voice");
  }
}

function createAudioRecorder(stream) {
  const audioContext = new (window.AudioContext || window.webkitAudioContext)();
  const analyser = audioContext.createAnalyser();
  const microphone = audioContext.createMediaStreamSource(stream);
  // Pick the best supported MIME type (iOS needs mp4, others prefer webm)
  const mimeTypes = ['audio/webm;codecs=opus','audio/webm','audio/mp4','audio/ogg;codecs=opus','audio/ogg'];
  const mimeType = mimeTypes.find(m => { try { return MediaRecorder.isTypeSupported(m); } catch { return false; } }) || '';
  const recorder = new MediaRecorder(stream, mimeType ? { mimeType } : {});

  microphone.connect(analyser);

  const chunks = [];
  recorder.ondataavailable = (e) => { if (e.data.size > 0) chunks.push(e.data); };

  return {
    start() {
      chunks.length = 0;
      recorder.start(100); // 100ms timeslices for reliable capture
    },
    stop() {
      return new Promise(resolve => {
        recorder.onstop = () => {
          const blob = new Blob(chunks, { type: recorder.mimeType || 'audio/webm' });
          resolve(blob);
        };
        recorder.stop();
      });
    },
    getFrequencies() {
      const data = new Uint8Array(analyser.frequencyBinCount);
      analyser.getByteFrequencyData(data);
      return Array.from(data.slice(0, 16));
    },
    cleanup() {
      stream.getTracks().forEach(track => track.stop());
      audioContext.close();
    }
  };
}

// Store audio/voice as data URL — no server required
async function blobToDataUrl(blob) {
  return new Promise((resolve, reject) => {
    const reader = new FileReader();
    reader.onload = () => resolve(String(reader.result || ""));
    reader.onerror = () => reject(reader.error);
    reader.readAsDataURL(blob);
  });
}

// Compress an image File to a small square data URL for use as cover art
async function compressCoverArt(file) {
  return new Promise((resolve) => {
    const img = new window.Image();
    const src = URL.createObjectURL(file);
    img.onload = () => {
      const size = 240;
      const canvas = document.createElement("canvas");
      canvas.width = size; canvas.height = size;
      const ctx = canvas.getContext("2d");
      // Centre-crop to square
      const s = Math.min(img.width, img.height);
      const ox = (img.width - s) / 2, oy = (img.height - s) / 2;
      ctx.drawImage(img, ox, oy, s, s, 0, 0, size, size);
      URL.revokeObjectURL(src);
      resolve(canvas.toDataURL("image/jpeg", 0.75));
    };
    img.onerror = () => { URL.revokeObjectURL(src); resolve(null); };
    img.src = src;
  });
}

function createIcon(paths) {
  return function Icon({ className = "", style, width, height }) {
    const sized = width || height || (style && (style.width || style.height));
    return (
      <svg className={className} style={style}
        width={width || (sized ? undefined : 18)} height={height || (sized ? undefined : 18)}
        viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round" aria-hidden="true">
        {paths.map((path, index) => path.type === "circle"
          ? <circle key={index} cx={path.cx} cy={path.cy} r={path.r} />
          : path.type === "rect"
            ? <rect key={index} x={path.x} y={path.y} width={path.width} height={path.height} rx={path.rx} />
            : <path key={index} d={path.d} fill={path.fill || "none"} />
        )}
      </svg>
    );
  };
}

const Copy = createIcon([{ type: "rect", x: 9, y: 9, width: 13, height: 13, rx: 2 }, { d: "M5 15H4a2 2 0 0 1-2-2V4a2 2 0 0 1 2-2h9a2 2 0 0 1 2 2v1" }]);
const ExternalLink = createIcon([{ d: "M15 3h6v6" }, { d: "M10 14 21 3" }, { d: "M18 13v6a2 2 0 0 1-2 2H5a2 2 0 0 1-2-2V8a2 2 0 0 1 2-2h6" }]);
const FileText = createIcon([{ d: "M14 2H6a2 2 0 0 0-2 2v16a2 2 0 0 0 2 2h12a2 2 0 0 0 2-2V8z" }, { d: "M14 2v6h6" }, { d: "M16 13H8" }, { d: "M16 17H8" }, { d: "M10 9H8" }]);
const Folder = createIcon([{ d: "M3 7a2 2 0 0 1 2-2h5l2 2h7a2 2 0 0 1 2 2v8a2 2 0 0 1-2 2H5a2 2 0 0 1-2-2Z" }]);
const Image = createIcon([{ type: "rect", x: 3, y: 3, width: 18, height: 18, rx: 2 }, { type: "circle", cx: 8.5, cy: 8.5, r: 1.5 }, { d: "m21 15-5-5L5 21" }]);
const Link2 = createIcon([{ d: "M10 13a5 5 0 0 0 7.54.54l3-3a5 5 0 0 0-7.07-7.07l-1.72 1.71" }, { d: "M14 11a5 5 0 0 0-7.54-.54l-3 3a5 5 0 0 0 7.07 7.07l1.71-1.71" }]);
const MessageCircle = createIcon([{ d: "M21 11.5a8.38 8.38 0 0 1-.9 3.8 8.5 8.5 0 0 1-7.6 4.7 8.38 8.38 0 0 1-3.8-.9L3 21l1.9-5.7a8.38 8.38 0 0 1-.9-3.8 8.5 8.5 0 0 1 4.7-7.6 8.38 8.38 0 0 1 3.8-.9h.5a8.48 8.48 0 0 1 8 8v.5z" }]);
const MoreHorizontal = createIcon([{ type: "circle", cx: 5, cy: 12, r: 1 }, { type: "circle", cx: 12, cy: 12, r: 1 }, { type: "circle", cx: 19, cy: 12, r: 1 }]);
const Pause = createIcon([{ d: "M10 4H6v16h4V4z", fill: "currentColor" }, { d: "M18 4h-4v16h4V4z", fill: "currentColor" }]);
const Play = createIcon([{ d: "M5 3 19 12 5 21V3z", fill: "currentColor" }]);
const Plus = createIcon([{ d: "M12 5v14" }, { d: "M5 12h14" }]);
const Search = createIcon([{ type: "circle", cx: 11, cy: 11, r: 8 }, { d: "m21 21-4.35-4.35" }]);
const Send = createIcon([{ d: "m22 2-7 20-4-9-9-4Z" }, { d: "M22 2 11 13" }]);
const Sparkles = createIcon([{ d: "m12 3 1.7 5.3L19 10l-5.3 1.7L12 17l-1.7-5.3L5 10l5.3-1.7Z" }, { d: "M5 3v4" }, { d: "M3 5h4" }, { d: "M19 17v4" }, { d: "M17 19h4" }]);
const Trash2 = createIcon([{ d: "M3 6h18" }, { d: "M8 6V4h8v2" }, { d: "M19 6l-1 14H6L5 6" }, { d: "M10 11v6" }, { d: "M14 11v6" }]);
const Upload = createIcon([{ d: "M21 15v4a2 2 0 0 1-2 2H5a2 2 0 0 1-2-2v-4" }, { d: "M17 8 12 3 7 8" }, { d: "M12 3v12" }]);
const X = createIcon([{ d: "M18 6 6 18" }, { d: "m6 6 12 12" }]);
const ChevronLeft = createIcon([{ d: "M15 18l-6-6 6-6" }]);
const ChevronDown = createIcon([{ d: "M6 9l6 6 6-6" }]);
const Pencil = createIcon([{ d: "M17 3a2.828 2.828 0 1 1 4 4L7.5 20.5 2 22l1.5-5.5L17 3z" }]);
const Mic = createIcon([{ d: "M12 1a3 3 0 0 0-3 3v8a3 3 0 0 0 6 0V4a3 3 0 0 0-3-3z" }, { d: "M19 10v2a7 7 0 0 1-14 0v-2" }, { d: "M12 19v3" }, { d: "M8 22h8" }]);
const Music = createIcon([{ d: "M9 18V5l12-2v13" }, { type: "circle", cx: 6, cy: 18, r: 3 }, { type: "circle", cx: 18, cy: 16, r: 3 }]);
const Vault = createIcon([{ type: "rect", x: 2, y: 3, width: 20, height: 18, rx: 2 }, { type: "circle", cx: 12, cy: 12, r: 4 }, { d: "M12 8v1.5M12 14.5V16M8 12h1.5M14.5 12H16" }, { d: "M18 3v18" }]);
const Feed = createIcon([{ type: "rect", x: 3, y: 3, width: 18, height: 5, rx: 1 }, { type: "rect", x: 3, y: 11, width: 18, height: 5, rx: 1 }, { d: "M3 19h12" }]);
const FilePdf = createIcon([{ d: "M14 2H6a2 2 0 0 0-2 2v16a2 2 0 0 0 2 2h12a2 2 0 0 0 2-2V8z" }, { d: "M14 2v6h6" }, { d: "M9 13h1.5M9 17h6M11.5 13c.83 0 1.5.67 1.5 1.5S12.33 16 11.5 16H9v-3h2.5z" }]);
const Camera = createIcon([{ d: "M23 19a2 2 0 0 1-2 2H3a2 2 0 0 1-2-2V8a2 2 0 0 1 2-2h4l2-3h6l2 3h4a2 2 0 0 1 2 2z" }, { type: "circle", cx: 12, cy: 13, r: 4 }]);
const UserCircle = createIcon([{ type: "circle", cx: 12, cy: 12, r: 10 }, { type: "circle", cx: 12, cy: 10, r: 3 }, { d: "M7 20.662V19a2 2 0 0 1 2-2h6a2 2 0 0 1 2 2v1.662" }]);
const LogOut = createIcon([{ d: "M9 21H5a2 2 0 0 1-2-2V5a2 2 0 0 1 2-2h4" }, { d: "M16 17l5-5-5-5" }, { d: "M21 12H9" }]);
const Lock = createIcon([{ type: "rect", x: 3, y: 11, width: 18, height: 11, rx: 2 }, { d: "M7 11V7a5 5 0 0 1 10 0v4" }]);
const Mail = createIcon([{ type: "rect", x: 2, y: 4, width: 20, height: 16, rx: 2 }, { d: "m22 7-8.97 5.7a1.94 1.94 0 0 1-2.06 0L2 7" }]);
const Eye = createIcon([{ d: "M1 12s4-8 11-8 11 8 11 8-4 8-11 8-11-8-11-8z" }, { type: "circle", cx: 12, cy: 12, r: 3 }]);
const EyeOff = createIcon([{ d: "M17.94 17.94A10.07 10.07 0 0 1 12 20c-7 0-11-8-11-8a18.45 18.45 0 0 1 5.06-5.94M9.9 4.24A9.12 9.12 0 0 1 12 4c7 0 11 8 11 8a18.5 18.5 0 0 1-2.16 3.19m-6.72-1.07a3 3 0 1 1-4.24-4.24" }, { d: "M1 1l22 22" }]);
const SettingsIcon = createIcon([{ type: "circle", cx: 12, cy: 12, r: 3 }, { d: "M19.4 15a1.65 1.65 0 0 0 .33 1.82l.06.06a2 2 0 0 1-2.83 2.83l-.06-.06a1.65 1.65 0 0 0-1.82-.33 1.65 1.65 0 0 0-1 1.51V21a2 2 0 0 1-4 0v-.09A1.65 1.65 0 0 0 9 19.4a1.65 1.65 0 0 0-1.82.33l-.06.06a2 2 0 0 1-2.83-2.83l.06-.06A1.65 1.65 0 0 0 4.68 15a1.65 1.65 0 0 0-1.51-1H3a2 2 0 0 1 0-4h.09A1.65 1.65 0 0 0 4.6 9a1.65 1.65 0 0 0-.33-1.82l-.06-.06a2 2 0 0 1 2.83-2.83l.06.06A1.65 1.65 0 0 0 9 4.68a1.65 1.65 0 0 0 1-1.51V3a2 2 0 0 1 4 0v.09a1.65 1.65 0 0 0 1 1.51 1.65 1.65 0 0 0 1.82-.33l.06-.06a2 2 0 0 1 2.83 2.83l-.06.06A1.65 1.65 0 0 0 19.4 9a1.65 1.65 0 0 0 1.51 1H21a2 2 0 0 1 0 4h-.09a1.65 1.65 0 0 0-1.51 1z" }]);
const Bell = createIcon([{ d: "M18 8A6 6 0 0 0 6 8c0 7-3 9-3 9h18s-3-2-3-9" }, { d: "M13.73 21a2 2 0 0 1-3.46 0" }]);
const BellOff = createIcon([{ d: "M13.73 21a2 2 0 0 1-3.46 0" }, { d: "M18.63 13A17.89 17.89 0 0 1 18 8" }, { d: "M6.26 6.26A5.86 5.86 0 0 0 6 8c0 7-3 9-3 9h14" }, { d: "M18 8a6 6 0 0 0-9.33-5" }, { d: "M1 1l22 22" }]);
const InfoIcon = createIcon([{ type: "circle", cx: 12, cy: 12, r: 10 }, { d: "M12 16v-4" }, { d: "M12 8h.01" }]);
const MessageSquare = createIcon([{ d: "M21 15a2 2 0 0 1-2 2H7l-4 4V5a2 2 0 0 1 2-2h14a2 2 0 0 1 2 2z" }]);
const Star = createIcon([{ d: "M12 2l3.09 6.26L22 9.27l-5 4.87 1.18 6.88L12 17.77l-6.18 3.25L7 14.14 2 9.27l6.91-1.01L12 2z" }]);

function hexToRgb(hex) {
  const c = hex.replace("#","");
  return [parseInt(c.slice(0,2),16), parseInt(c.slice(2,4),16), parseInt(c.slice(4,6),16)];
}
function rgbToHex(r,g,b) {
  return "#" + [r,g,b].map(v => Math.round(Math.min(255,Math.max(0,v))).toString(16).padStart(2,"0")).join("");
}
function hexLuminance(hex) {
  return hexToRgb(hex).reduce((s,v,i) => {
    v /= 255; v = v<=0.03928 ? v/12.92 : Math.pow((v+0.055)/1.055,2.4);
    return s + v*[0.2126,0.7152,0.0722][i];
  }, 0);
}
function buildCustomTheme(page, accent, text) {
  const [pr,pg,pb] = hexToRgb(page);
  const [ar,ag,ab] = hexToRgb(accent);
  const [tr,tg,tb] = hexToRgb(text);
  const dark = hexLuminance(page) < 0.25;
  const glowB = rgbToHex(ar-20, ag+10, ab+30);
  const glowC = rgbToHex(ar+30, ag-10, ab-20);
  return {
    page, text,
    muted: `rgba(${tr},${tg},${tb},.5)`,
    soft:  `rgba(${tr},${tg},${tb},.28)`,
    panel:  `rgba(${pr},${pg},${pb},.85)`,
    panel2: `rgba(${pr},${pg},${pb},.93)`,
    input:  dark ? "rgba(255,255,255,.12)" : "rgba(0,0,0,.08)",
    active: `rgba(${ar},${ag},${ab},.25)`,
    border: `rgba(${ar},${ag},${ab},.22)`,
    shadow: `0 0 0 1px rgba(${ar},${ag},${ab},.14), 0 24px 64px rgba(0,0,0,.5)`,
    glowA: accent, glowB, glowC, accent,
  };
}

const themes = {
  dark:   { page: "#07080F", text: "#EDEFFF", muted: "#7B82A8", soft: "#4A5070", panel: "rgba(13,15,30,.82)",   panel2: "rgba(7,8,18,.92)",    input: "rgba(255,255,255,.05)", active: "rgba(255,255,255,.11)", border: "rgba(255,255,255,.08)", shadow: "0 0 0 1px rgba(255,255,255,.07), 0 24px 64px rgba(0,0,0,.55)", glowA: "#1768FF", glowB: "#4A2CFF", glowC: "#8B1CFF", accent: "#7DBDFF" },
  light:  { page: "#EEF2EC", text: "#151515",  muted: "#5E665E", soft: "#8A928A", panel: "rgba(255,255,255,.72)", panel2: "rgba(245,250,244,.88)", input: "rgba(255,255,255,.78)", active: "rgba(255,255,255,.92)", border: "rgba(0,0,0,.07)",       shadow: "0 0 0 1px rgba(0,0,0,.06), 0 20px 56px rgba(50,70,55,.14)",   glowA: "#A6FF1F", glowB: "#2FE883", glowC: "#C7FF45", accent: "#58C820" },
  franki: { page: "#0D0305", text: "#F5C4AC",  muted: "#9A5040", soft: "#5A2515", panel: "rgba(42,10,14,.85)",  panel2: "rgba(20,5,8,.92)",    input: "rgba(255,255,255,.12)", active: "rgba(232,114,58,.22)", border: "rgba(232,114,58,.18)", shadow: "0 0 0 1px rgba(232,114,58,.12), 0 24px 64px rgba(0,0,0,.7)",  glowA: "#E8723A", glowB: "#C43A1A", glowC: "#F5A070", accent: "#E8723A" },
  ice:    { page: "#C8DCE8", text: "#1A2A3A",  muted: "#6A7A9A", soft: "#9AAABB", panel: "rgba(255,255,255,.55)", panel2: "rgba(255,255,255,.75)", input: "rgba(255,255,255,.6)", active: "rgba(255,255,255,.85)", border: "rgba(91,170,196,.25)",  shadow: "0 0 0 1px rgba(142,155,196,.2), 0 20px 56px rgba(91,170,196,.22)", glowA: "#5BAAC4", glowB: "#8E9BC4", glowC: "#C4A4C0", accent: "#5BAAC4" },
  grape:  { page: "#2F0147", text: "#E2C2C6",  muted: "#9C528B", soft: "#610F7F", panel: "rgba(97,15,127,.4)", panel2: "rgba(47,1,71,.88)",   input: "rgba(255,255,255,.12)", active: "rgba(156,82,139,.3)",  border: "rgba(156,82,139,.22)", shadow: "0 0 0 1px rgba(156,82,139,.18), 0 24px 64px rgba(0,0,0,.65)", glowA: "#9C528B", glowB: "#610F7F", glowC: "#E2C2C6", accent: "#C480B0" },
};

const FOLDERS = ["Ideas", "Music", "Visuals", "Content"];

const starterItems = [
  { id: 1, type: "song", title: "Fluid Player Demo", url: "https://www.soundhelix.com/examples/mp3/SoundHelix-Song-1.mp3", note: "Sample audio with waveform visualization.", folder: "Music", createdAt: Date.now() - 10800000 },
  { id: 2, type: "image", title: "Hoodie Mockup Inspo", url: "https://images.unsplash.com/photo-1556821840-3a63f95609a7?auto=format&fit=crop&w=900&q=80", note: "Visual inspiration for the vault.", folder: "Visuals", createdAt: Date.now() - 43200000 },
  { id: 3, type: "link", title: "Studio Clip Idea", url: "https://youtube.com/shorts/example", note: "Fast pacing for Reels", folder: "Content", createdAt: Date.now() - 86400000 },
  { id: 4, type: "note", title: "Song Idea", url: "", note: "Make a hook with a darker second half.", folder: "Ideas", createdAt: Date.now() - 129600000 }
];

function normalizeUrl(value) {
  const trimmed = String(value || "").trim();
  if (!trimmed) return "";
  if (/^https?:\/\//i.test(trimmed) || /^data:/i.test(trimmed) || /^blob:/i.test(trimmed)) return trimmed;
  return `https://${trimmed}`;
}
function detectType(value) {
  const lower = String(value || "").toLowerCase();
  if (/\.(mp3|wav|m4a|aac|flac|ogg|opus|webm)(\?.*)?$/i.test(lower)) return "song";
  if (/\.(jpg|jpeg|png|gif|webp|avif)(\?.*)?$/i.test(lower) || lower.includes("unsplash") || lower.includes("image")) return "image";
  if (/\.pdf(\?.*)?$/i.test(lower)) return "pdf";
  return "link";
}
function iconFor(type) {
  if (type === "image") return Image;
  if (type === "note") return FileText;
  if (type === "pdf") return FilePdf;
  return Link2;
}
function titleFromUrl(value, type) {
  try {
    const parsed = new URL(value);
    const site = parsed.hostname.replace("www.", "");
    const prefix = type === "song" ? "Song" : type === "image" ? "Image" : "Link";
    return `${prefix} - ${site}`;
  } catch {
    return value || "Untitled";
  }
}
function isPlayableAudioUrl(value, hasAudio) {
  if (hasAudio) return true; // stored locally in audioBlobs IDB
  const lower = String(value || "").toLowerCase();
  return /^blob:/i.test(lower) || /^data:audio\//i.test(lower) || /\.(mp3|wav|m4a|aac|flac|ogg)(\?.*)?$/i.test(lower);
}
function dateLabel(timestamp) {
  return new Date(timestamp || Date.now()).toLocaleDateString(undefined, { month: "short", day: "numeric" });
}
function newId() {
  return Date.now() + Math.floor(Math.random() * 10000);
}

// Session-only in-memory cache: itemId → blob URL (revoked on item delete)
// Never stored to IDB — blob URLs are session-only by definition.
const audioBlobUrlCache = new Map();

// Migration promises: tracks background data: URL → audioBlobs migrations
// so resolveAudioUrl() can await them if play is tapped during migration.
const migrationPromises = new Map();

async function loadItemsFromDB(fallback) {
  try {
    const items = await dbGetAll("items");
    if (!items.length) return fallback;
    return items.map(item => {
      if (item.url?.startsWith("data:audio")) {
        // Old item has binary data embedded — migrate it to audioBlobs in the background.
        // Use fetch(dataUrl) which is async and doesn't block the main thread.
        const dataUrl = item.url;
        const migratedItem = { ...item, url: null, hasAudio: true };
        const p = fetch(dataUrl)
          .then(r => r.arrayBuffer())
          .then(async buf => {
            const mime = dataUrl.split(";")[0].split(":")[1] || "audio/mpeg";
            await saveAudioBlob(item.id, buf, mime);
            await dbPut("items", { ...migratedItem });
          })
          .catch(e => console.warn("Audio migration failed for", item.id, e))
          .finally(() => migrationPromises.delete(item.id));
        migrationPromises.set(item.id, p);
        return migratedItem;
      }
      // Strip any stray blob: URLs that can't survive reload
      if (item.url?.startsWith("blob:")) {
        return { ...item, url: null };
      }
      return item;
    });
  } catch (e) {
    console.error("Failed to load items:", e);
    return fallback;
  }
}

async function loadPlaylistsFromDB(fallback) {
  try {
    const playlists = await dbGetAll("playlists");
    return playlists.length > 0 ? playlists : fallback;
  } catch (e) {
    console.error("Failed to load playlists:", e);
    return fallback;
  }
}

async function loadFoldersFromDB(fallback) {
  try {
    const folders = await dbGetAll("folders");
    return folders.map(f => f.name).filter(n => n) || fallback;
  } catch (e) {
    console.error("Failed to load folders:", e);
    return fallback;
  }
}

async function loadThemeFromDB(fallback) {
  try {
    const theme = await dbGet("theme", "current");
    return theme?.value || fallback;
  } catch (e) {
    console.error("Failed to load theme:", e);
    return fallback;
  }
}

function saveToStorage(key, data) {
  try {
    localStorage.setItem(key, JSON.stringify(data));
  } catch (e) {
    console.warn("Failed to save to localStorage:", e);
  }
}

function loadFromStorage(key, fallback) {
  try {
    const data = localStorage.getItem(key);
    return data ? JSON.parse(data) : fallback;
  } catch (e) {
    console.warn("Failed to load from localStorage:", e);
    return fallback;
  }
}

function buildFluidPath(values, width, height) {
  if (!values.length) return "";
  if (values.length === 1) return `M 0 ${height} L 0 ${height - values[0] * height} L ${width} ${height} Z`;
  const step = width / (values.length - 1);
  const points = values.map((value, index) => ({ x: index * step, y: height - value * height }));
  let d = `M 0 ${height} L ${points[0].x} ${points[0].y}`;
  for (let i = 0; i < points.length - 1; i += 1) {
    const current = points[i];
    const next = points[i + 1];
    const cx = (current.x + next.x) / 2;
    const cy = (current.y + next.y) / 2;
    d += ` Q ${current.x} ${current.y} ${cx} ${cy}`;
  }
  const last = points[points.length - 1];
  return `${d} T ${last.x} ${last.y} L ${width} ${height} Z`;
}

function useAudioPlayback(item, activeAudioId, setActiveAudioId) {
  const [isPlaying, setIsPlaying] = useState(false);
  const [progress, setProgress] = useState(0);
  const [duration, setDuration] = useState(0);
  const [loadingAudio, setLoadingAudio] = useState(false);
  // Imperative Audio — created only when first played, zero memory until then.
  const audioRef = useRef(null);
  const playable = isPlayableAudioUrl(item.url, item.hasAudio);
  const expanded = activeAudioId === item.id;

  // Resolve the playable URL: direct URL → immediate; hasAudio → IDB lookup.
  const resolveUrl = async () => {
    if (item.url && !item.url.startsWith("blob:null")) return item.url;
    if (!item.hasAudio) return null;
    // Check session cache first (blob URL from this session)
    if (audioBlobUrlCache.has(item.id)) return audioBlobUrlCache.get(item.id);
    // If a background migration is in flight for this item, wait for it
    if (migrationPromises.has(item.id)) {
      await migrationPromises.get(item.id);
    }
    // Load binary from audioBlobs IDB → create an in-memory blob URL
    const data = await loadAudioBlob(item.id);
    if (!data) return null;
    const blob = new Blob([data.buffer], { type: data.mime || "audio/mpeg" });
    const url = URL.createObjectURL(blob);
    audioBlobUrlCache.set(item.id, url);
    return url;
  };

  // Wire up a new Audio element to the given URL
  const buildAudio = (url) => {
    const a = new Audio();
    a.preload = "none";
    a.src = url;
    a.addEventListener("timeupdate", () => {
      if (a.duration) setProgress((a.currentTime / a.duration) * 100);
    });
    a.addEventListener("loadedmetadata", () => setDuration(a.duration));
    a.addEventListener("ended", () => { setIsPlaying(false); setProgress(0); });
    audioRef.current = a;
    return a;
  };

  // Release memory when card unmounts
  useEffect(() => {
    return () => {
      if (audioRef.current) {
        audioRef.current.pause();
        audioRef.current.src = "";
        audioRef.current = null;
      }
    };
  }, []);

  const stopPlayback = () => {
    audioRef.current?.pause();
    setIsPlaying(false);
  };

  const toggle = () => {
    if (!expanded) setActiveAudioId(item.id);
    if (isPlaying) { stopPlayback(); setActiveAudioId(null); return; }
    if (!playable) { setActiveAudioId(item.id); return; }

    // Fast path: URL already available in state (blob: from current session or https:)
    if (item.url && !item.url.startsWith("blob:null")) {
      if (!audioRef.current) buildAudio(item.url);
      audioRef.current.play().then(() => setIsPlaying(true)).catch(() => {});
      return;
    }

    // Check session cache synchronously before going async
    if (audioBlobUrlCache.has(item.id)) {
      const url = audioBlobUrlCache.get(item.id);
      if (!audioRef.current) buildAudio(url);
      audioRef.current.play().then(() => setIsPlaying(true)).catch(() => {});
      return;
    }

    // Slow path: load from IDB. Show loading state then auto-play.
    setLoadingAudio(true);
    resolveUrl().then(url => {
      setLoadingAudio(false);
      if (!url) return;
      if (!audioRef.current) buildAudio(url);
      else if (!audioRef.current.src || audioRef.current.src === "about:blank") {
        audioRef.current.src = url;
      }
      audioRef.current.play().then(() => setIsPlaying(true)).catch(() => {});
    }).catch(() => setLoadingAudio(false));
  };

  const scrub = (pct) => {
    setProgress(pct);
    const a = audioRef.current;
    if (a && a.duration) a.currentTime = (pct / 100) * a.duration;
  };

  useEffect(() => {
    if (activeAudioId !== item.id && isPlaying) stopPlayback();
    if (activeAudioId !== item.id) setProgress(0);
  }, [activeAudioId]);

  return { isPlaying, expanded, progress, duration, playable, loadingAudio, toggle, scrub };
}

function Styles() {
  return <style>{`
    /* ── Reset + Base ── */
    *{-webkit-font-smoothing:antialiased;-moz-osx-font-smoothing:grayscale;box-sizing:border-box}
    body{
      font-family:-apple-system,'SF Pro Display','SF Pro Text',BlinkMacSystemFont,'Segoe UI',system-ui,sans-serif;
      -webkit-text-size-adjust:100%;
    }

    /* ── Spring physics variables ── */
    :root{
      --spring-snappy:cubic-bezier(0.34,1.56,0.64,1);
      --spring-smooth:cubic-bezier(0.25,0.46,0.45,0.94);
      --spring-slow:cubic-bezier(0.16,1,0.3,1);
    }

    /* ── Type scale — enforced globally ── */
    .type-title{font-size:34px;font-weight:700;letter-spacing:-0.5px;line-height:1.05}
    .type-title-sm{font-size:17px;font-weight:600;letter-spacing:-0.3px;line-height:1.2}
    .type-section{font-size:20px;font-weight:600;letter-spacing:-0.3px;line-height:1.2}
    .type-primary{font-size:15px;font-weight:590;letter-spacing:-0.1px;line-height:1.4}
    .type-secondary{font-size:13px;font-weight:400;letter-spacing:0;line-height:1.4}
    .type-label{font-size:11px;font-weight:600;letter-spacing:0.5px;text-transform:uppercase}

    /* ── Keyframes ── */
    @keyframes spin{to{transform:rotate(360deg)}}
    @keyframes reveal{from{opacity:0;transform:translateY(6px) scale(.98)}to{opacity:1;transform:translateY(0) scale(1)}}
    @keyframes slideUp{from{opacity:0;transform:translateY(40px)}to{opacity:1;transform:translateY(0)}}
    @keyframes glowPulse{0%,100%{opacity:0}40%{opacity:1}}
    @keyframes fadeIn{from{opacity:0}to{opacity:1}}
    @keyframes pageFade{from{opacity:0;transform:translateY(10px)}to{opacity:1;transform:translateY(0)}}
    @keyframes playRing{0%,100%{box-shadow:0 0 0 0 var(--ring),0 0 12px var(--ring)}60%{box-shadow:0 0 0 6px transparent,0 0 28px var(--ring)}}
    @keyframes waveBar{0%{transform:scaleY(0.12)}100%{transform:scaleY(1)}}
    @keyframes boatBob{0%,100%{transform:translateY(0) rotate(-2deg)}50%{transform:translateY(-2.5px) rotate(2deg)}}
    @keyframes borderSpin{to{--angle:360deg}}
    /* Card stagger-in on scroll into view */
    @keyframes cardIn{from{opacity:0;transform:translateY(12px)}to{opacity:1;transform:translateY(0)}}
    /* New item dropped in from above */
    @keyframes newItemIn{from{opacity:0;transform:translateY(-24px) scale(0.97)}to{opacity:1;transform:translateY(0) scale(1)}}
    /* Play button tap pulse — fires once per tap */
    @keyframes tapPulse{0%{box-shadow:0 0 0 0 var(--glow,rgba(100,200,255,.6)),0 0 8px var(--glow,rgba(100,200,255,.4))}55%{box-shadow:0 0 0 8px transparent,0 0 28px var(--glow,rgba(100,200,255,.5))}100%{box-shadow:0 0 0 0 transparent,0 0 8px var(--glow,rgba(100,200,255,.2))}}
    /* FAB bounce on release */
    @keyframes fabRelease{0%{transform:scale(0.93)}55%{transform:scale(1.05)}100%{transform:scale(1)}}
    /* Vibe dot breathing */
    @keyframes dotBreathe{0%,100%{opacity:.65;transform:scale(1)}50%{opacity:1;transform:scale(1.18)}}
    /* Context/dropdown spring entry */
    @keyframes contextIn{from{opacity:0;transform:scale(0.92) translateY(-4px)}to{opacity:1;transform:scale(1) translateY(0)}}
    /* Item remove */
    @keyframes itemOut{0%{opacity:1;transform:translateX(0);max-height:220px;margin-bottom:20px}100%{opacity:0;transform:translateX(-10px);max-height:0;margin-bottom:0;padding:0}}

    /* ── Scrollbar ── */
    .no-scrollbar{scrollbar-width:none;-webkit-overflow-scrolling:touch}
    .no-scrollbar::-webkit-scrollbar{display:none}

    /* ── Text clamp ── */
    .line-clamp-2{display:-webkit-box;-webkit-line-clamp:2;-webkit-box-orient:vertical;overflow:hidden}

    /* ── Inputs ── */
    .tray-input::placeholder{color:rgba(128,128,128,.55)}

    /* ── Theme fade ── */
    .theme-bg{transition:background 500ms ease,color 300ms ease}

    /* ── Cards — springy press, hover lift ── */
    .card-press{
      transition:
        transform 220ms var(--spring-snappy),
        box-shadow 200ms ease;
      will-change:transform;
    }
    .card-press:active{transform:scale(0.974)!important}
    @media(hover:hover){.card-press:hover{transform:translateY(-2px)}}

    /* ── Scroll-reveal for feed items ── */
    .card-reveal{opacity:0;transform:translateY(10px)}
    .card-reveal.is-visible{
      animation:cardIn 0.38s var(--spring-slow) both;
    }
    .card-reveal.is-new{
      animation:newItemIn 0.3s var(--spring-smooth) both;
    }

    /* ── Nav button ── */
    .nav-btn{
      display:flex;align-items:center;justify-content:center;
      border-radius:18px;padding:10px 8px;
      border:none;background:transparent;cursor:pointer;
      transition:background 180ms ease,color 180ms ease,transform 220ms var(--spring-snappy);
      min-height:48px;min-width:48px;-webkit-tap-highlight-color:transparent;
    }
    .nav-btn:active{transform:scale(0.85)}
    .nav-icon{transition:transform 240ms var(--spring-snappy)}
    .nav-btn.is-active .nav-icon{transform:scale(1.22)}

    /* ── FAB ── */
    .fab{transition:transform 200ms var(--spring-snappy),box-shadow 200ms ease}
    .fab:active{transform:scale(0.91)!important}
    .fab.fab-open{animation:fabRelease 0.28s var(--spring-snappy) both}

    /* ── Play button — tap pulse glow ── */
    .play-btn{transition:transform 200ms var(--spring-snappy),box-shadow 200ms ease}
    .play-btn:active{transform:scale(0.88)!important}
    .play-btn.tapped{animation:tapPulse 0.5s ease forwards}

    /* ── Scrubber thumb scale on grab ── */
    .scrub-thumb{transition:transform 120ms var(--spring-snappy),left 80ms linear}
    .scrub-thumb.dragging{transform:translate(-50%,-50%) scale(1.35)!important}

    /* ── Vibe dot breathe ── */
    .dot-breathe{animation:dotBreathe 2.5s ease-in-out infinite}

    /* ── Flare title gradient shimmer ── */
    @keyframes flareTitleGrad{
      0%  { background-position: 0%   50% }
      50% { background-position: 100% 50% }
      100%{ background-position: 0%   50% }
    }
    /* Static props live in the class so React never touches background during animation.
       Only --ft-grad (the CSS variable) is swapped on theme change — no clip glitch. */
    .flare-title{
      background: var(--ft-grad);
      background-size: 300% 300%;
      -webkit-background-clip: text;
      background-clip: text;
      -webkit-text-fill-color: transparent;
      animation: flareTitleGrad 5s ease-in-out infinite;
    }

    /* ── Dropdown / context spring ── */
    .dropdown-spring{animation:contextIn 200ms var(--spring-snappy) both}

    /* ── Note editor ── */
    .note-body{font-size:15px;line-height:1.8;outline:none}
    .note-body:empty::before{content:attr(data-placeholder);opacity:0.35;pointer-events:none}
    .note-body p{margin:0 0 .5em}
    .note-body h1{font-size:1.9rem;font-weight:800;line-height:1.2;margin:.8em 0 .2em}
    .note-body h2{font-size:1.4rem;font-weight:700;line-height:1.25;margin:.7em 0 .2em}
    .note-body h3{font-size:1.1rem;font-weight:700;margin:.6em 0 .15em}
    .note-body blockquote{border-left:3px solid currentColor;padding-left:1em;opacity:.65;margin:.5em 0;font-style:italic}
    .note-body pre,.note-body code{font-family:ui-monospace,monospace;font-size:.85em;background:rgba(128,128,128,.13);padding:2px 6px;border-radius:5px}
    .note-body pre{display:block;padding:.75em 1em;border-radius:10px;overflow-x:auto;white-space:pre}
    .note-body ul{list-style:disc;padding-left:1.5em;margin:.3em 0}
    .note-body ol{list-style:decimal;padding-left:1.5em;margin:.3em 0}
    .note-body li{margin:.2em 0}
    .note-body a{text-decoration:underline;color:inherit;opacity:.8}
    .note-body hr{border:none;border-top:1px solid rgba(128,128,128,.25);margin:1.2em 0}
    .note-body mark{border-radius:3px;padding:0 2px}
    .note-toolbar-btn{
      display:flex;align-items:center;justify-content:center;border-radius:10px;
      height:38px;min-width:38px;padding:0 8px;font-size:14px;font-weight:700;
      cursor:pointer;border:none;background:transparent;flex-shrink:0;
      transition:background 120ms,color 120ms,transform 120ms var(--spring-snappy);
    }
    .note-toolbar-btn:active{transform:scale(0.84)}
    .safe-top{padding-top:env(safe-area-inset-top)}
    .safe-bottom{padding-bottom:env(safe-area-inset-bottom)}
    .note-body img{max-width:100%;border-radius:12px;margin:8px 0;display:block}
    .note-body audio{width:100%;margin:8px 0;display:block}
  `}</style>;
}

function FlareTitle({ t }) {
  return (
    <span style={{
      fontFamily: "'Nunito', -apple-system, system-ui, sans-serif",
      fontSize: 42,
      fontWeight: 900,
      letterSpacing: -1.5,
      lineHeight: 1,
      color: t.text,
      userSelect: "none",
      display: "block",
    }}>Flare</span>
  );
}

// ── Local auth helpers ──────────────────────────────────────────────────────
function localAuth_getUsers() {
  try { return JSON.parse(localStorage.getItem("flare_users") || "[]"); } catch { return []; }
}
function localAuth_saveUsers(users) {
  localStorage.setItem("flare_users", JSON.stringify(users));
}
function localAuth_getSession() {
  try { return JSON.parse(localStorage.getItem("flare_session")); } catch { return null; }
}
function localAuth_saveSession(user) {
  localStorage.setItem("flare_session", JSON.stringify(user));
}
function localAuth_clearSession() {
  localStorage.removeItem("flare_session");
}

// ── ProfilePage ─────────────────────────────────────────────────────────────
function ProfilePage({ t, currentUser, setCurrentUser }) {
  const [view, setView] = useState(currentUser ? "profile" : "login");
  const [name, setName] = useState("");
  const [email, setEmail] = useState("");
  const [password, setPassword] = useState("");
  const [showPw, setShowPw] = useState(false);
  const [error, setError] = useState("");
  const [editName, setEditName] = useState(currentUser?.name || "");
  const [editBio, setEditBio] = useState(currentUser?.bio || "");
  const [editMode, setEditMode] = useState(false);
  const avatarRef = useRef(null);

  const primaryBg = `linear-gradient(135deg, ${t.glowA} 0%, ${t.glowB} 100%)`;

  const field = (val, set, placeholder, type = "text", rightEl = null) => (
    <div style={{ position: "relative" }}>
      <input
        value={val} onChange={e => set(e.target.value)}
        placeholder={placeholder} type={showPw && type === "password" ? "text" : type}
        style={{
          width: "100%", boxSizing: "border-box",
          background: t.input, color: t.text, border: `1px solid ${t.border}`,
          borderRadius: 14, padding: "13px 16px", fontSize: 15, outline: "none",
          paddingRight: rightEl ? 44 : 16,
        }}
      />
      {rightEl && (
        <button onClick={rightEl.action} style={{ position: "absolute", right: 12, top: "50%", transform: "translateY(-50%)", background: "none", border: "none", cursor: "pointer", color: t.muted, display: "flex" }}>
          {rightEl.icon}
        </button>
      )}
    </div>
  );

  const handleRegister = () => {
    setError("");
    if (!name.trim() || !email.trim() || !password.trim()) return setError("All fields required.");
    const users = localAuth_getUsers();
    if (users.find(u => u.email.toLowerCase() === email.toLowerCase())) return setError("Email already registered.");
    const user = { id: Date.now().toString(), name: name.trim(), email: email.trim(), password, bio: "", avatar: null, createdAt: Date.now() };
    localAuth_saveUsers([...users, user]);
    localAuth_saveSession(user);
    setCurrentUser(user);
    setEditName(user.name); setEditBio(user.bio);
  };

  const handleLogin = () => {
    setError("");
    if (!email.trim() || !password.trim()) return setError("Enter email and password.");
    const users = localAuth_getUsers();
    const user = users.find(u => u.email.toLowerCase() === email.toLowerCase() && u.password === password);
    if (!user) return setError("Incorrect email or password.");
    localAuth_saveSession(user);
    setCurrentUser(user);
    setEditName(user.name); setEditBio(user.bio);
  };

  const handleLogout = () => {
    localAuth_clearSession();
    setCurrentUser(null);
    setView("login");
    setEmail(""); setPassword(""); setName(""); setError("");
  };

  const handleSaveProfile = () => {
    const updated = { ...currentUser, name: editName.trim() || currentUser.name, bio: editBio };
    const users = localAuth_getUsers().map(u => u.id === updated.id ? updated : u);
    localAuth_saveUsers(users);
    localAuth_saveSession(updated);
    setCurrentUser(updated);
    setEditMode(false);
  };

  const handleAvatar = async (file) => {
    if (!file) return;
    const compressed = await compressCoverArt(file);
    if (!compressed) return;
    const updated = { ...currentUser, avatar: compressed };
    const users = localAuth_getUsers().map(u => u.id === updated.id ? updated : u);
    localAuth_saveUsers(users);
    localAuth_saveSession(updated);
    setCurrentUser(updated);
  };

  // ── Auth screen ──
  if (!currentUser) return (
    <div style={{ padding: "8px 0 80px" }}>
      <div style={{ background: t.panel, borderRadius: 24, padding: 24, border: `0.5px solid ${t.border}` }}>
        {/* Toggle */}
        <div style={{ display: "flex", background: t.input, borderRadius: 12, padding: 4, marginBottom: 24 }}>
          {["login", "register"].map(v => (
            <button key={v} onClick={() => { setView(v); setError(""); }}
              style={{ flex: 1, padding: "8px 0", borderRadius: 9, border: "none", cursor: "pointer", fontSize: 14, fontWeight: 700,
                background: view === v ? t.panel : "transparent",
                color: view === v ? t.text : t.muted,
                boxShadow: view === v ? "0 1px 4px rgba(0,0,0,.12)" : "none",
                transition: "all 180ms ease" }}>
              {v === "login" ? "Sign In" : "Create Account"}
            </button>
          ))}
        </div>

        <div style={{ display: "flex", flexDirection: "column", gap: 12 }}>
          {view === "register" && field(name, setName, "Display name")}
          <div style={{ display: "flex", alignItems: "center", gap: 10, background: t.input, borderRadius: 14, padding: "13px 16px", border: `1px solid ${t.border}` }}>
            <Mail style={{ width: 18, height: 18, color: t.muted, flexShrink: 0 }} />
            <input value={email} onChange={e => setEmail(e.target.value)} placeholder="Email" type="email"
              style={{ flex: 1, background: "none", border: "none", outline: "none", color: t.text, fontSize: 15 }} />
          </div>
          <div style={{ display: "flex", alignItems: "center", gap: 10, background: t.input, borderRadius: 14, padding: "13px 16px", border: `1px solid ${t.border}` }}>
            <Lock style={{ width: 18, height: 18, color: t.muted, flexShrink: 0 }} />
            <input value={password} onChange={e => setPassword(e.target.value)} placeholder="Password" type={showPw ? "text" : "password"}
              style={{ flex: 1, background: "none", border: "none", outline: "none", color: t.text, fontSize: 15 }} />
            <button onClick={() => setShowPw(p => !p)} style={{ background: "none", border: "none", cursor: "pointer", color: t.muted, display: "flex" }}>
              {showPw ? <EyeOff style={{ width: 17, height: 17 }} /> : <Eye style={{ width: 17, height: 17 }} />}
            </button>
          </div>

          {error && <p style={{ color: "#FF6B6B", fontSize: 13, margin: 0 }}>{error}</p>}

          <button onClick={view === "login" ? handleLogin : handleRegister}
            style={{ marginTop: 4, padding: "14px 0", borderRadius: 14, border: "none", cursor: "pointer",
              background: primaryBg, color: "#fff", fontWeight: 700, fontSize: 15 }}>
            {view === "login" ? "Sign In" : "Create Account"}
          </button>
        </div>
      </div>
    </div>
  );

  // ── Profile screen ──
  return (
    <div style={{ padding: "8px 0 80px", display: "flex", flexDirection: "column", gap: 16 }}>
      {/* Avatar + name */}
      <div style={{ background: t.panel, borderRadius: 24, padding: 24, border: `0.5px solid ${t.border}`, display: "flex", flexDirection: "column", alignItems: "center", gap: 12 }}>
        <button onClick={() => avatarRef.current?.click()} style={{ position: "relative", background: "none", border: "none", cursor: "pointer", padding: 0 }}>
          {currentUser.avatar
            ? <img src={currentUser.avatar} style={{ width: 80, height: 80, borderRadius: "50%", objectFit: "cover", border: `2px solid ${t.border}` }} />
            : <div style={{ width: 80, height: 80, borderRadius: "50%", background: primaryBg, display: "flex", alignItems: "center", justifyContent: "center" }}>
                <UserCircle style={{ width: 44, height: 44, color: "#fff" }} />
              </div>
          }
          <div style={{ position: "absolute", bottom: 0, right: 0, width: 26, height: 26, borderRadius: "50%", background: t.panel, border: `1.5px solid ${t.border}`, display: "flex", alignItems: "center", justifyContent: "center" }}>
            <Camera style={{ width: 13, height: 13, color: t.muted }} />
          </div>
        </button>
        <input ref={avatarRef} type="file" accept="image/*" style={{ display: "none" }} onChange={e => { handleAvatar(e.target.files?.[0]); e.target.value = ""; }} />

        {editMode ? (
          <div style={{ width: "100%", display: "flex", flexDirection: "column", gap: 10 }}>
            <input value={editName} onChange={e => setEditName(e.target.value)} placeholder="Display name"
              style={{ background: t.input, color: t.text, border: `1px solid ${t.border}`, borderRadius: 12, padding: "10px 14px", fontSize: 15, outline: "none", textAlign: "center", fontWeight: 700, width: "100%", boxSizing: "border-box" }} />
            <textarea value={editBio} onChange={e => setEditBio(e.target.value)} placeholder="Short bio…" rows={2}
              style={{ background: t.input, color: t.text, border: `1px solid ${t.border}`, borderRadius: 12, padding: "10px 14px", fontSize: 14, outline: "none", resize: "none", width: "100%", boxSizing: "border-box" }} />
            <div style={{ display: "flex", gap: 8 }}>
              <button onClick={() => setEditMode(false)} style={{ flex: 1, padding: "10px 0", borderRadius: 12, border: `1px solid ${t.border}`, background: "transparent", color: t.muted, fontWeight: 600, cursor: "pointer", fontSize: 14 }}>Cancel</button>
              <button onClick={handleSaveProfile} style={{ flex: 1, padding: "10px 0", borderRadius: 12, border: "none", background: primaryBg, color: "#fff", fontWeight: 700, cursor: "pointer", fontSize: 14 }}>Save</button>
            </div>
          </div>
        ) : (
          <>
            <div style={{ textAlign: "center" }}>
              <p style={{ margin: 0, fontWeight: 800, fontSize: 20, color: t.text }}>{currentUser.name}</p>
              <p style={{ margin: "2px 0 0", fontSize: 13, color: t.muted }}>{currentUser.email}</p>
              {currentUser.bio && <p style={{ margin: "6px 0 0", fontSize: 14, color: t.soft, lineHeight: 1.4 }}>{currentUser.bio}</p>}
            </div>
            <button onClick={() => { setEditMode(true); setEditName(currentUser.name); setEditBio(currentUser.bio || ""); }}
              style={{ padding: "8px 20px", borderRadius: 20, border: `1px solid ${t.border}`, background: t.active, color: t.text, fontWeight: 600, fontSize: 13, cursor: "pointer" }}>
              Edit Profile
            </button>
          </>
        )}
      </div>

      {/* Account info */}
      <div style={{ background: t.panel, borderRadius: 24, border: `0.5px solid ${t.border}`, overflow: "hidden" }}>
        <div style={{ padding: "14px 20px", borderBottom: `0.5px solid ${t.border}`, display: "flex", alignItems: "center", gap: 12 }}>
          <Mail style={{ width: 18, height: 18, color: t.muted, flexShrink: 0 }} />
          <div>
            <p style={{ margin: 0, fontSize: 12, color: t.muted, fontWeight: 600, textTransform: "uppercase", letterSpacing: "0.08em" }}>Email</p>
            <p style={{ margin: 0, fontSize: 15, color: t.text }}>{currentUser.email}</p>
          </div>
        </div>
        <div style={{ padding: "14px 20px", display: "flex", alignItems: "center", gap: 12 }}>
          <UserCircle style={{ width: 18, height: 18, color: t.muted, flexShrink: 0 }} />
          <div>
            <p style={{ margin: 0, fontSize: 12, color: t.muted, fontWeight: 600, textTransform: "uppercase", letterSpacing: "0.08em" }}>Member since</p>
            <p style={{ margin: 0, fontSize: 15, color: t.text }}>{new Date(currentUser.createdAt).toLocaleDateString("en-US", { month: "long", year: "numeric" })}</p>
          </div>
        </div>
      </div>

      {/* Sign out */}
      <button onClick={handleLogout}
        style={{ display: "flex", alignItems: "center", justifyContent: "center", gap: 8, padding: "14px 0", borderRadius: 20, border: `1px solid rgba(255,80,80,.3)`, background: "rgba(255,80,80,.08)", color: "#FF5050", fontWeight: 700, fontSize: 15, cursor: "pointer" }}>
        <LogOut style={{ width: 18, height: 18 }} /> Sign Out
      </button>
    </div>
  );
}

function NavButton({ active, icon: Icon, label, onClick, t }) {
  return (
    <button
      onClick={onClick}
      aria-label={label}
      className={`nav-btn${active ? " is-active" : ""}`}
      style={{ color: active ? t.text : t.muted, background: active ? t.active : "transparent" }}
    >
      <span className="nav-icon" style={{ display: "flex", alignItems: "center", justifyContent: "center" }}>
        <Icon style={{ width: "1.45rem", height: "1.45rem" }} />
      </span>
    </button>
  );
}
function MediaButton({ active, onClick, icon: Icon, label, t }) {
  return (
    <button
      onClick={onClick}
      style={{
        display: "flex", flexDirection: "column", alignItems: "center", gap: 8,
        color: active ? t.text : t.muted, background: "none", border: "none",
        cursor: "pointer", padding: 0,
        transition: "transform 200ms var(--spring-snappy), color 180ms ease",
      }}
      onMouseDown={e => e.currentTarget.style.transform = "scale(0.91)"}
      onMouseUp={e => e.currentTarget.style.transform = ""}
      onMouseLeave={e => e.currentTarget.style.transform = ""}
      onTouchStart={e => e.currentTarget.style.transform = "scale(0.91)"}
      onTouchEnd={e => e.currentTarget.style.transform = ""}
    >
      <span style={{
        display: "flex", width: 56, height: 56, alignItems: "center", justifyContent: "center",
        borderRadius: 18, background: active ? t.active : t.input, color: active ? t.text : t.muted,
        border: active ? `1.5px solid ${t.border}` : "1.5px solid transparent",
        transition: "background 180ms ease, border-color 180ms ease",
        boxShadow: active ? `0 4px 16px rgba(0,0,0,.12)` : "none",
      }}>
        <Icon style={{ width: 20, height: 20 }} />
      </span>
      {label && <span style={{ fontSize: 10, fontWeight: 600, letterSpacing: "0.02em" }}>{label}</span>}
    </button>
  );
}

// Scroll-reveal wrapper — fades + lifts card in when it enters the viewport
function CardReveal({ children, isNew }) {
  const ref = useRef(null);
  const [vis, setVis] = useState(false);
  useEffect(() => {
    const el = ref.current;
    if (!el) return;
    if (isNew) { setVis(true); return; }
    const obs = new IntersectionObserver(
      ([entry]) => { if (entry.isIntersecting) { setVis(true); obs.disconnect(); } },
      { threshold: 0.04 }
    );
    obs.observe(el);
    return () => obs.disconnect();
  }, []);
  return (
    <div
      ref={ref}
      className={`card-reveal${vis ? (isNew ? " is-new" : " is-visible") : ""}`}
    >
      {children}
    </div>
  );
}

// Consistent empty state — icon, label, optional CTA
function EmptyState({ icon: Icon, label, cta, onCta, t }) {
  return (
    <div style={{ display: "flex", flexDirection: "column", alignItems: "center", justifyContent: "center", padding: "52px 24px 40px", gap: 14 }}>
      {Icon && (
        <div style={{ opacity: 0.18, transform: "scale(1.5)", marginBottom: 8 }}>
          <Icon style={{ width: 52, height: 52, color: t.muted }} />
        </div>
      )}
      <p className="type-primary" style={{ color: t.muted, textAlign: "center", margin: 0 }}>{label}</p>
      {cta && (
        <button onClick={onCta} style={{
          marginTop: 2, fontSize: 13, fontWeight: 600, color: t.glowA,
          background: `${t.glowA}18`, border: "none", cursor: "pointer",
          padding: "8px 18px", borderRadius: 12,
          transition: "background 150ms ease, transform 150ms var(--spring-snappy)",
        }}
          onMouseDown={e => e.currentTarget.style.transform = "scale(0.95)"}
          onMouseUp={e => e.currentTarget.style.transform = ""}
        >
          {cta}
        </button>
      )}
    </div>
  );
}

// CSS-only animated bars — zero JS per frame, no AudioContext, no white noise
const BAR_CONFIGS = [0.18,0.55,0.82,0.45,0.95,0.30,0.70,0.60,0.88,0.25,0.75,0.50,0.92,0.38,0.65,0.20,0.80,0.48,0.72,0.35];
function AnimatedBars({ isPlaying, glowA, glowB }) {
  return (
    <div style={{ display:"flex", alignItems:"flex-end", gap:3, height:32, padding:"0 2px" }}>
      {BAR_CONFIGS.map((seed, i) => {
        const dur = 0.55 + seed * 0.5;
        const delay = (i * 41) % 380;
        const minH = 4 + seed * 6;
        return (
          <div key={i} style={{
            flex: 1,
            minHeight: minH,
            borderRadius: 3,
            background: `linear-gradient(to top, ${glowA}, ${glowB})`,
            transformOrigin: "bottom center",
            animation: isPlaying ? `waveBar ${dur.toFixed(2)}s ease-in-out ${delay}ms infinite alternate` : "none",
            transform: isPlaying ? undefined : `scaleY(${(minH / 32).toFixed(2)})`,
            opacity: isPlaying ? 0.85 : 0.3,
            transition: "opacity 200ms, transform 200ms",
          }} />
        );
      })}
    </div>
  );
}

// Touch-friendly scrubber — pointer capture, thumb scales on grab
function AudioScrubber({ progress, onScrub, glowA, glowB }) {
  const trackRef = useRef(null);
  const draggingRef = useRef(false);
  const [dragging, setDragging] = useState(false);
  const getPct = (e) => {
    const rect = trackRef.current?.getBoundingClientRect();
    if (!rect || !rect.width) return 0;
    return Math.max(0, Math.min(100, ((e.clientX - rect.left) / rect.width) * 100));
  };
  return (
    <div
      ref={trackRef}
      onPointerDown={(e) => {
        e.stopPropagation();
        draggingRef.current = true;
        setDragging(true);
        e.currentTarget.setPointerCapture(e.pointerId);
        onScrub(getPct(e));
      }}
      onPointerMove={(e) => { if (draggingRef.current) onScrub(getPct(e)); }}
      onPointerUp={(e) => { draggingRef.current = false; setDragging(false); onScrub(getPct(e)); }}
      onPointerCancel={() => { draggingRef.current = false; setDragging(false); }}
      style={{ padding: "12px 0", cursor: "pointer", touchAction: "none", userSelect: "none" }}
    >
      <div style={{ position: "relative", height: dragging ? 5 : 4, borderRadius: 3, background: "rgba(255,255,255,.12)", transition: "height 120ms var(--spring-snappy)" }}>
        <div style={{
          position: "absolute", inset: 0, right: `${100 - progress}%`, borderRadius: 3,
          background: `linear-gradient(90deg,${glowA},${glowB})`,
          transition: "right 80ms linear",
        }} />
        <div
          className={`scrub-thumb${dragging ? " dragging" : ""}`}
          style={{
            position: "absolute", top: "50%", left: `${progress}%`,
            transform: dragging ? "translate(-50%,-50%) scale(1.35)" : "translate(-50%,-50%) scale(1)",
            width: 16, height: 16, borderRadius: "50%",
            background: glowA, border: "2px solid rgba(255,255,255,.92)",
            boxShadow: dragging ? `0 0 14px ${glowA}99, 0 2px 8px rgba(0,0,0,.3)` : `0 0 8px ${glowA}66`,
            transition: "left 80ms linear, transform 130ms var(--spring-snappy), box-shadow 130ms ease",
          }}
        />
      </div>
    </div>
  );
}

function AudioPlayer({ item, t, isPlaying, expanded, progress, duration, playable, loadingAudio, scrub }) {
  if (!expanded) return null;
  const fmt = (s) => isFinite(s) && s > 0 ? `${Math.floor(s/60)}:${String(Math.floor(s%60)).padStart(2,"0")}` : "0:00";
  const currentSec = (progress / 100) * (duration || 0);
  return (
    <div style={{ marginTop:12, borderRadius:18, overflow:"hidden", padding:1, background:`linear-gradient(135deg,${t.glowA},${t.glowB},${t.glowC})`, boxShadow: isPlaying ? `0 0 20px ${t.glowA}44` : "none", animation:"reveal 250ms ease both" }}>
      <div style={{ borderRadius:17, padding:"12px 14px 10px", background:t.panel2 }}>
        {loadingAudio
          ? <div style={{ display:"flex", alignItems:"center", justifyContent:"center", height:32, gap:8 }}>
              <div style={{ width:16, height:16, borderRadius:"50%", border:`2px solid ${t.glowA}`, borderTopColor:"transparent", animation:"spin 600ms linear infinite" }} />
              <span style={{ fontSize:11, color:t.muted }}>Loading…</span>
            </div>
          : <AnimatedBars isPlaying={isPlaying} glowA={t.glowA} glowB={t.glowB} />
        }
        {playable && (
          <>
            <AudioScrubber progress={progress} onScrub={scrub} glowA={t.glowA} glowB={t.glowB} />
            <div style={{ display:"flex", justifyContent:"space-between", fontSize:10, color:t.muted, marginTop:2 }}>
              <span>{fmt(currentSec)}</span><span>{fmt(duration)}</span>
            </div>
          </>
        )}
        {!playable && item.url && (
          <a href={normalizeUrl(item.url)} target="_blank" rel="noopener noreferrer" style={{ marginTop:8, display:"flex", alignItems:"center", gap:6, fontSize:11, color:t.muted, textDecoration:"none" }}>
            <ExternalLink style={{ width:12, height:12, flexShrink:0 }} />Open in {item.url.includes("spotify") ? "Spotify" : "browser"}
          </a>
        )}
      </div>
    </div>
  );
}

function SongCard({ item, t, theme, activeAudioId, setActiveAudioId, editorProps, folders, openMenuId, setOpenMenuId, patchItem, removeItem, onOpen }) {
  const { isPlaying, expanded, progress, duration, playable, loadingAudio, toggle, scrub } = useAudioPlayback(item, activeAudioId, setActiveAudioId);
  const [tapped, setTapped] = useState(false);
  const onPlayTap = (e) => {
    e.stopPropagation();
    setTapped(true);
    setTimeout(() => setTapped(false), 520);
    toggle();
  };
  return (
    <article
      onClick={onOpen}
      className="card-press relative w-full min-w-0 cursor-pointer rounded-3xl"
      style={{
        background: t.panel,
        boxShadow: "0 2px 16px rgba(0,0,0,.1), 0 1px 4px rgba(0,0,0,.06)",
        border: `0.5px solid ${t.border}`,
      }}
    >
      <div className="p-5">
        <div className="flex items-center gap-3">
          <button
            onClick={onPlayTap}
            className={`play-btn flex h-11 w-11 shrink-0 items-center justify-center rounded-xl overflow-hidden${tapped ? " tapped" : ""}`}
            style={{
              background: item.cover
                ? `url(${item.cover}) center/cover no-repeat`
                : expanded ? `conic-gradient(from 120deg, ${t.glowA}, ${t.glowB}, ${t.glowC}, ${t.glowA})` : t.panel2,
              "--ring": `${t.glowA}99`,
              "--glow": `${t.glowA}88`,
              animation: isPlaying && !tapped ? "playRing 2s ease infinite" : undefined,
              boxShadow: isPlaying ? `0 0 20px ${t.glowA}66` : "none",
            }}
            aria-label={isPlaying ? "Pause" : "Play"}
          >
            <span className="flex h-7 w-7 items-center justify-center rounded-full"
              style={{ background: item.cover ? "rgba(0,0,0,.45)" : t.text, color: item.cover ? "#fff" : t.page, backdropFilter: item.cover ? "blur(1px)" : "none" }}>
              {isPlaying ? <Pause className="h-3 w-3 fill-current" /> : <Play className="ml-0.5 h-3 w-3 fill-current" />}
            </span>
          </button>
          <div className="min-w-0 flex-1">
            <div className="flex items-baseline gap-2">
              <TextEditor item={item} {...editorProps} />
              <span className="shrink-0 text-[10px] font-medium tabular-nums" style={{ color: t.soft }}>{dateLabel(item.createdAt)}</span>
            </div>
            {item.note && <p className="mt-1 line-clamp-2 text-xs leading-relaxed" style={{ color: t.muted }}>{item.note}</p>}
          </div>
          <PostMenu item={item} folders={folders} t={t} openMenuId={openMenuId} setOpenMenuId={setOpenMenuId} patchItem={patchItem} removeItem={removeItem} />
        </div>
        <AudioPlayer item={item} t={t} isPlaying={isPlaying} expanded={expanded} progress={progress} duration={duration} playable={playable} loadingAudio={loadingAudio} scrub={scrub} />
      </div>
    </article>
  );
}

function AudioCard({ item, t, theme, activeAudioId, setActiveAudioId }) {
  const { isPlaying, expanded, progress, duration, playable, loadingAudio, toggle, scrub } = useAudioPlayback(item, activeAudioId, setActiveAudioId);
  return (
    <>
      <button onClick={toggle} style={{ display:"flex", alignItems:"center", gap:10, width:"100%", background:"none", border:"none", padding:"6px 0", cursor:"pointer", color:"inherit" }}>
        <span style={{ width:36, height:36, borderRadius:"50%", background: isPlaying ? `linear-gradient(135deg,${t.glowA},${t.glowB})` : t.input, display:"flex", alignItems:"center", justifyContent:"center", flexShrink:0, boxShadow: isPlaying ? `0 2px 12px ${t.glowA}55` : "none" }}>
          {isPlaying ? <Pause className="h-3.5 w-3.5 fill-current" style={{color:"#fff"}} /> : <Play className="h-3.5 w-3.5 fill-current" style={{color:t.muted}} />}
        </span>
        <span style={{ fontSize:13, fontWeight:600, color:t.text, overflow:"hidden", textOverflow:"ellipsis", whiteSpace:"nowrap", flex:1, textAlign:"left" }}>{item.title}</span>
      </button>
      <AudioPlayer item={item} t={t} isPlaying={isPlaying} expanded={expanded} progress={progress} duration={duration} playable={playable} loadingAudio={loadingAudio} scrub={scrub} />
    </>
  );
}

function PdfCard({ item, t, editorProps, folders, openMenuId, setOpenMenuId, patchItem, removeItem, onOpen }) {
  return (
    <article
      onClick={onOpen}
      className="card-press relative w-full min-w-0 cursor-pointer rounded-3xl"
      style={{ background: t.panel, boxShadow: "0 2px 16px rgba(0,0,0,.1), 0 1px 4px rgba(0,0,0,.06)", border: `0.5px solid ${t.border}` }}
    >
      <div className="p-5">
        <div className="flex items-center gap-3">
          {/* Thumbnail or default icon */}
          {item.cover
            ? <img src={item.cover} style={{ width: 44, height: 44, borderRadius: 12, objectFit: "cover", flexShrink: 0 }} />
            : <div style={{ width: 44, height: 44, borderRadius: 12, background: "linear-gradient(135deg,#FF6B6B22,#FF8E5322)", display: "flex", alignItems: "center", justifyContent: "center", flexShrink: 0, border: "0.5px solid #FF6B6B33" }}>
                <FilePdf style={{ width: 22, height: 22, color: "#FF6B6B" }} />
              </div>
          }
          <div className="min-w-0 flex-1">
            <div className="flex items-baseline gap-2">
              <TextEditor item={item} {...editorProps} />
              <span className="shrink-0 text-[10px] font-medium tabular-nums" style={{ color: t.soft }}>{dateLabel(item.createdAt)}</span>
            </div>
            <p className="mt-0.5 text-[10px] font-semibold uppercase tracking-wide" style={{ color: "#FF6B6B", opacity: 0.8 }}>PDF</p>
            {item.note && <p className="mt-1 line-clamp-2 text-xs leading-relaxed" style={{ color: t.muted }}>{item.note}</p>}
          </div>
          <PostMenu item={item} folders={folders} t={t} openMenuId={openMenuId} setOpenMenuId={setOpenMenuId} patchItem={patchItem} removeItem={removeItem} />
        </div>
      </div>
    </article>
  );
}

function TextEditor({ item, t, inputStyle, editingTitleId, editingTitle, setEditingTitle, saveTitle, startTitle }) {
  return (
    <div className="min-w-0 flex-1">
      {editingTitleId === item.id ? (
        <input
          value={editingTitle}
          onChange={(event) => setEditingTitle(event.target.value)}
          onBlur={() => saveTitle(item.id)}
          onKeyDown={(event) => { if (event.key === "Enter") saveTitle(item.id); }}
          autoFocus
          className="w-full rounded-xl px-3 py-2 text-sm font-semibold outline-none"
          style={inputStyle}
        />
      ) : (
        <button onClick={(event) => startTitle(item, event)} className="block w-full text-left">
          <p className="truncate type-primary" style={{ color: t.text }}>{item.title}</p>
        </button>
      )}
    </div>
  );
}

function SearchTray({ open, t, search, setSearch, close }) {
  if (!open) return null;
  return (
    <div className="fixed inset-x-4 bottom-28 z-30 mx-auto max-w-md overflow-hidden rounded-[2rem] p-4 backdrop-blur-2xl" style={{ background: t.panel2, boxShadow: t.shadow, border: `1px solid ${t.border}` }}>
      <div className="relative">
        <Search className="absolute left-4 top-1/2 h-4 w-4 -translate-y-1/2" style={{ color: t.muted }} />
        <input value={search} onChange={(event) => setSearch(event.target.value)} placeholder="Search vault..." autoFocus className="tray-input w-full rounded-2xl px-4 py-3.5 pl-11 pr-11 text-sm outline-none" style={{ background: t.input, color: t.text }} />
        <button onClick={close} className="absolute right-2 top-1/2 flex h-8 w-8 -translate-y-1/2 items-center justify-center rounded-full" style={{ background: t.input, color: t.muted }}><X className="h-4 w-4" /></button>
      </div>
    </div>
  );
}

function ChatTray({ open, t, messages, input, setInput, ask, close }) {
  if (!open) return null;
  return (
    <div className="fixed inset-x-4 bottom-28 z-30 mx-auto max-w-md overflow-hidden rounded-[2rem] p-4 backdrop-blur-2xl" style={{ background: t.panel2, boxShadow: t.shadow, border: `1px solid ${t.border}` }}>
      <div className="mb-3 flex items-center justify-between gap-3">
        <div className="flex items-center gap-2">
          <div className="flex h-9 w-9 items-center justify-center rounded-full" style={{ background: `linear-gradient(135deg,${t.glowA},${t.glowB})`, color: t.text }}><Sparkles className="h-4 w-4" /></div>
          <p className="text-sm font-semibold" style={{ color: t.text }}>Ask Subconscious</p>
        </div>
        <button onClick={close} className="flex h-8 w-8 items-center justify-center rounded-full" style={{ background: t.input, color: t.muted }}><X className="h-4 w-4" /></button>
      </div>
      <div className="max-h-72 space-y-2 overflow-y-auto pr-1 no-scrollbar">
        {messages.map((message) => (
          <div key={message.id} className={message.role === "user" ? "flex justify-end" : "flex justify-start"}>
            <div className="max-w-[82%] rounded-2xl px-3 py-2 text-xs leading-relaxed" style={{ background: message.role === "user" ? `linear-gradient(135deg,${t.glowA}55,${t.glowB}55)` : t.input, color: t.text }}>{message.text}</div>
          </div>
        ))}
      </div>
      <div className="mt-3 flex gap-2">
        <input value={input} onChange={(event) => setInput(event.target.value)} onKeyDown={(event) => { if (event.key === "Enter") ask(); }} placeholder="Ask about your vault..." className="tray-input min-w-0 flex-1 rounded-2xl px-4 py-2.5 text-sm outline-none" style={{ background: t.input, color: t.text }} />
        <button onClick={ask} className="flex h-10 w-10 items-center justify-center rounded-2xl" style={{ background: `linear-gradient(135deg,${t.glowA},${t.glowB})`, color: t.text }}><Send className="h-4 w-4" /></button>
      </div>
    </div>
  );
}

function VoiceRecorder({ open, t, onRecord, close }) {
  const [recording, setRecording] = useState(false);
  const [duration, setDuration] = useState(0);
  const [spectrum, setSpectrum] = useState(Array(16).fill(0));
  const [permissionError, setPermissionError] = useState(null);
  const recorderRef = useRef(null);
  const streamRef = useRef(null);
  const timerRef = useRef(null);

  const startRecording = async () => {
    try {
      setPermissionError(null);
      const stream = await requestMicrophoneAccess();
      streamRef.current = stream;
      recorderRef.current = createAudioRecorder(stream);
      recorderRef.current.start();
      setRecording(true);
      setDuration(0);

      timerRef.current = setInterval(() => {
        setDuration(d => d + 1);
        setSpectrum(recorderRef.current?.getFrequencies?.() || Array(16).fill(0));
      }, 100);
    } catch (err) {
      setPermissionError(err.message);
    }
  };

  const stopRecording = async () => {
    if (!recorderRef.current) return;

    clearInterval(timerRef.current);
    const blob = await recorderRef.current.stop();
    recorderRef.current.cleanup();
    recorderRef.current = null;
    streamRef.current = null;
    setRecording(false);

    // Use blob URL in state (no base64 in heap); cache data URL async for IndexedDB
    try {
      const blobUrl = URL.createObjectURL(blob);
      onRecord({
        type: "song",
        title: `Voice Memo - ${new Date().toLocaleTimeString()}`,
        url: blobUrl,
        blob,   // passed so caller can cache data URL for persistence
        note: "",
        duration
      });
      close();
    } catch (err) {
      console.error("Recording save failed:", err);
      setPermissionError("Failed to save recording. Please try again.");
    }
  };

  if (!open) return null;

  const formatTime = (secs) => {
    const mins = Math.floor(secs / 10 / 6);
    const s = Math.floor((secs / 10) % 60);
    return `${mins}:${s.toString().padStart(2, '0')}`;
  };

  return (
    <div className="fixed inset-0 z-40 flex items-center justify-center">
      <button aria-label="Close" onClick={close} className="absolute inset-0 backdrop-blur-md" style={{ background: "rgba(0,0,0,.4)" }} />
      <div className="relative mx-4 w-full max-w-md rounded-[2rem] p-6 backdrop-blur-2xl" style={{ background: t.panel, boxShadow: t.shadow, border: `1px solid ${t.border}` }}>
        <button onClick={close} className="absolute right-4 top-4 p-2" style={{ color: t.muted }}><X className="h-5 w-5" /></button>

        <h2 className="mb-6 text-lg font-bold" style={{ color: t.text }}>Record Voice Memo</h2>

        {permissionError && (
          <div className="mb-4 rounded-lg p-3" style={{ background: "rgba(255,107,107,.1)", color: "#ff6b6b" }}>
            {permissionError}
          </div>
        )}

        {!recording ? (
          <div className="flex flex-col items-center gap-4">
            <button
              onClick={startRecording}
              className="h-24 w-24 rounded-full flex items-center justify-center transition active:scale-95"
              style={{ background: `linear-gradient(135deg, ${t.glowA} 0%, ${t.glowB} 100%)`, color: "#fff" }}
            >
              <Mic className="h-10 w-10" />
            </button>
            <p style={{ color: t.muted }}>Tap to start recording</p>
          </div>
        ) : (
          <div className="flex flex-col items-center gap-6">
            <div className="text-3xl font-bold" style={{ color: t.glowA }}>{formatTime(duration)}</div>

            <div className="flex h-16 items-end justify-center gap-1">
              {spectrum.map((val, i) => (
                <div
                  key={i}
                  className="w-2 rounded-sm transition-all"
                  style={{
                    height: `${(val / 255) * 100}%`,
                    background: t.glowA,
                    opacity: 0.7
                  }}
                />
              ))}
            </div>

            <button
              onClick={stopRecording}
              className="h-20 w-20 rounded-full flex items-center justify-center transition active:scale-95"
              style={{ background: "#FF6B6B", color: "#fff" }}
            >
              <div className="h-8 w-8 rounded-sm" style={{ background: "#fff" }} />
            </button>
            <p style={{ color: t.muted }}>Tap to stop</p>
          </div>
        )}
      </div>
    </div>
  );
}

function AddTrayPanel({ open, t, theme, inputStyle, mediaMode, setMediaMode, url, note, folder, setUrl, setNote, setFolder, addItem, addImageFile, addAudioFile, addPdfFile, onVoiceRecord, close }) {
  if (!open) return null;
  const showTextFields = mediaMode === "link" || mediaMode === "note";
  const primaryBg = `linear-gradient(135deg, ${t.glowA} 0%, ${t.glowB} 100%)`;
  const primaryText = "#fff";
  return (
    <div className="fixed inset-0 z-30">
      <button aria-label="Close add panel" onClick={close} className="absolute inset-0 w-full backdrop-blur-md" style={{ background: "rgba(0,0,0,.22)" }} />
      <div className="absolute inset-x-4 bottom-28 mx-auto max-w-md rounded-[2rem] p-5 backdrop-blur-2xl" style={{ background: t.panel2, boxShadow: t.shadow, border: `1px solid ${t.border}` }}>
        <button onClick={close} className="absolute right-3 top-3 flex h-8 w-8 items-center justify-center rounded-full" style={{ background: t.input, color: t.muted }}><X className="h-4 w-4" /></button>
        <div className="pb-2 pr-10">
          <p className="text-sm font-semibold" style={{ color: t.text }}>Capture something</p>
        </div>
        <div className="grid grid-cols-4 gap-2 pt-3">
          <MediaButton active={mediaMode === "link"} onClick={() => setMediaMode("link")} icon={Link2} label="Link" t={t} />
          <MediaButton active={mediaMode === "note"} onClick={() => setMediaMode("note")} icon={FileText} label="Note" t={t} />
          <button onClick={onVoiceRecord} className="flex cursor-pointer flex-col items-center gap-2 transition active:scale-95" style={{ color: t.muted }}>
            <span className="flex h-14 w-14 items-center justify-center rounded-2xl" style={{ background: t.input, color: t.muted }}><Mic className="h-5 w-5" /></span>
            <span style={{ fontSize: 10, fontWeight: 600 }}>Voice</span>
          </button>
          <label className="flex cursor-pointer flex-col items-center gap-2 transition active:scale-95" style={{ color: t.muted }}>
            <span className="flex h-14 w-14 items-center justify-center rounded-2xl" style={{ background: t.input, color: t.muted }}><Image className="h-5 w-5" /></span>
            <span style={{ fontSize: 10, fontWeight: 600 }}>Media</span>
            <input type="file" accept="image/*,.mp3,.wav,.m4a,.aac,.ogg,.flac,.opus,.webm,.mp4,.pdf" onChange={(event) => { const f = event.target.files?.[0]; if (f) { if (f.type.startsWith("image/")) addImageFile(f); else if (f.type === "application/pdf" || f.name.endsWith(".pdf")) addPdfFile(f); else addAudioFile(f); } event.target.value = ""; }} className="hidden" />
          </label>
        </div>
        {showTextFields && (
          <div className="mt-6 space-y-3">
            {mediaMode === "link" && <input value={url} onChange={(event) => setUrl(event.target.value)} placeholder="Paste link..." className="tray-input w-full rounded-2xl px-5 py-4 text-sm outline-none" style={{ background: t.input, color: t.text }} />}
            <textarea value={note} onChange={(event) => setNote(event.target.value)} placeholder={mediaMode === "link" ? "Add a note..." : "Write note..."} className="tray-input min-h-20 w-full resize-none rounded-2xl px-5 py-4 text-sm outline-none" style={{ background: t.input, color: t.text }} />
            <button onClick={addItem} className="flex min-h-[3.25rem] w-full items-center justify-center gap-2 rounded-2xl px-5 py-4 text-sm font-semibold" style={{ background: primaryBg, color: primaryText }}><Plus className="h-4 w-4" />Save to vault</button>
          </div>
        )}
      </div>
    </div>
  );
}

function LinkPreviewCard({ url, t, className = "" }) {
  let domain = "", faviconUrl = "";
  try {
    const parsed = new URL(url);
    domain = parsed.hostname.replace(/^www\./, "");
    faviconUrl = `https://www.google.com/s2/favicons?domain=${parsed.hostname}&sz=64`;
  } catch {}
  return (
    <a href={url} target="_blank" rel="noopener noreferrer" onClick={e => e.stopPropagation()}
      className={`flex items-center gap-3 rounded-2xl px-3 py-2.5 ${className}`}
      style={{ background: t.input, border: `1px solid ${t.border}`, textDecoration: "none" }}>
      {faviconUrl && (
        <img src={faviconUrl} alt="" style={{ width: 28, height: 28, borderRadius: 8, flexShrink: 0, objectFit: "cover" }}
          onError={e => e.currentTarget.style.display = "none"} />
      )}
      <div style={{ minWidth: 0, flex: 1 }}>
        <p style={{ fontSize: 12, fontWeight: 700, color: t.text, margin: 0, overflow: "hidden", textOverflow: "ellipsis", whiteSpace: "nowrap" }}>{domain}</p>
        <p style={{ fontSize: 10, color: t.muted, margin: 0, overflow: "hidden", textOverflow: "ellipsis", whiteSpace: "nowrap" }}>{url.replace(/^https?:\/\//, "").slice(0, 60)}</p>
      </div>
      <ExternalLink className="h-3.5 w-3.5 shrink-0" style={{ color: t.muted }} />
    </a>
  );
}

function stripHtml(html) {
  if (!html) return '';
  if (!html.startsWith('<')) return html;
  return html
    .replace(/<br\s*\/?>/gi, ' ')
    .replace(/<\/?(p|div|h[1-6]|li|blockquote|pre)[^>]*>/gi, ' ')
    .replace(/<[^>]+>/g, '')
    .replace(/&nbsp;/g, ' ').replace(/&lt;/g, '<').replace(/&gt;/g, '>').replace(/&amp;/g, '&')
    .replace(/\s+/g, ' ').trim();
}

function RichNoteEditor({ value, onChange, t, onAttachImage, onAttachAudio }) {
  const editorRef = useRef(null);
  const [fmt, setFmt] = useState({ bold:false, italic:false, underline:false, strike:false });
  const [block, setBlock] = useState('p');
  const [showAttach, setShowAttach] = useState(false);

  useEffect(() => {
    if (!editorRef.current) return;
    const html = value && value.startsWith('<') ? value : value ? `<p>${value.replace(/\n/g,'</p><p>')}</p>` : '';
    editorRef.current.innerHTML = html;
  }, []);

  const poll = () => {
    try {
      setFmt({
        bold: document.queryCommandState('bold'),
        italic: document.queryCommandState('italic'),
        underline: document.queryCommandState('underline'),
        strike: document.queryCommandState('strikeThrough'),
      });
      let node = window.getSelection()?.getRangeAt(0)?.startContainer;
      while (node && node !== editorRef.current) {
        const tag = node.nodeName?.toLowerCase();
        if (['h1','pre'].includes(tag)) { setBlock(tag); return; }
        node = node.parentNode;
      }
      setBlock('p');
    } catch {}
  };

  const exec = (cmd, val = null) => {
    editorRef.current?.focus();
    document.execCommand(cmd, false, val);
    onChange(editorRef.current?.innerHTML || '');
    poll();
  };

  const fmtBlock = (tag) => { exec('formatBlock', tag); setBlock(tag); };

  const onKeyDown = (e) => {
    const mod = e.metaKey || e.ctrlKey;
    if (mod && e.key==='b') { e.preventDefault(); exec('bold'); }
    if (mod && e.key==='i') { e.preventDefault(); exec('italic'); }
    if (mod && e.key==='u') { e.preventDefault(); exec('underline'); }
    if (mod && e.key==='z') { e.preventDefault(); exec('undo'); }
    if (mod && e.key==='y') { e.preventDefault(); exec('redo'); }
  };

  const Btn = ({ cmd, val=null, label, active }) => (
    <button className="note-toolbar-btn"
      style={{ background: active ? t.active : 'transparent', color: active ? t.text : t.muted }}
      onMouseDown={e => { e.preventDefault(); exec(cmd, val); }}>
      {label}
    </button>
  );

  const BlkBtn = ({ tag, label }) => (
    <button className="note-toolbar-btn"
      style={{ background: block===tag ? t.active : 'transparent', color: block===tag ? t.text : t.muted }}
      onMouseDown={e => { e.preventDefault(); fmtBlock(tag); }}>
      {label}
    </button>
  );

  const sep = <div style={{ width:1, height:20, background:t.border, margin:'0 2px', flexShrink:0 }} />;

  return (
    <div style={{ display:'flex', flexDirection:'column', flex:1, minHeight:0 }}>
      {/* Single compact toolbar */}
      <div style={{ display:'flex', alignItems:'center', gap:2, padding:'4px 0', borderBottom:`1px solid ${t.border}`, overflowX:'auto', flexShrink:0 }} className="no-scrollbar">
        <Btn cmd="bold" label={<b>B</b>} active={fmt.bold} />
        <Btn cmd="italic" label={<i style={{fontFamily:'Georgia,serif'}}>I</i>} active={fmt.italic} />
        <Btn cmd="underline" label={<span style={{textDecoration:'underline'}}>U</span>} active={fmt.underline} />
        <Btn cmd="strikeThrough" label={<span style={{textDecoration:'line-through'}}>S</span>} active={fmt.strike} />
        {sep}
        <BlkBtn tag="p" label="Text" />
        <BlkBtn tag="h1" label="H1" />
        <BlkBtn tag="pre" label="Code" />
        {sep}
        <Btn cmd="insertUnorderedList" label="• List" />
        <Btn cmd="insertOrderedList" label="1. List" />
        {sep}
        <Btn cmd="undo" label="↩" />
        <Btn cmd="redo" label="↪" />
        {sep}
        {/* + Attach media */}
        <div style={{ position:'relative', flexShrink:0 }}>
          <button className="note-toolbar-btn" style={{ color: t.muted }}
            onMouseDown={e => { e.preventDefault(); setShowAttach(a => !a); }}>
            <Plus className="h-4 w-4" />
          </button>
          {showAttach && (
            <div onMouseDown={e=>e.stopPropagation()} style={{ position:'absolute', bottom:'calc(100% + 6px)', right:0, background:t.panel2, border:`1px solid ${t.border}`, borderRadius:14, padding:6, zIndex:200, boxShadow:t.shadow, minWidth:130 }}>
              <button className="flex w-full items-center gap-2 rounded-xl px-3 py-2 text-left text-xs font-semibold"
                style={{ color:t.text }} onMouseDown={e=>{ e.preventDefault(); setShowAttach(false); onAttachImage?.(); }}>
                📷 Image
              </button>
              <button className="flex w-full items-center gap-2 rounded-xl px-3 py-2 text-left text-xs font-semibold"
                style={{ color:t.text }} onMouseDown={e=>{ e.preventDefault(); setShowAttach(false); onAttachAudio?.(); }}>
                🎵 Audio
              </button>
              <button className="flex w-full items-center gap-2 rounded-xl px-3 py-2 text-left text-xs font-semibold"
                style={{ color:t.text }} onMouseDown={e=>{ e.preventDefault(); setShowAttach(false); const url=prompt('Link URL:'); if(url) exec('createLink', url); }}>
                🔗 Link
              </button>
            </div>
          )}
        </div>
      </div>

      {/* Editable area */}
      <div
        ref={editorRef}
        contentEditable
        suppressContentEditableWarning
        data-placeholder="Start writing..."
        className="note-body no-scrollbar"
        style={{ flex:1, overflowY:'auto', paddingTop:16, paddingBottom:32, color:t.text }}
        onInput={() => onChange(editorRef.current?.innerHTML || '')}
        onKeyDown={onKeyDown}
        onKeyUp={poll}
        onMouseUp={poll}
        onSelect={poll}
      />
    </div>
  );
}

function ExpandedPost({ item, t, theme, activeAudioId, setActiveAudioId, patchItem, close }) {
  const imgInputRef = useRef(null);
  const audInputRef = useRef(null);
  const coverInputRef = useRef(null);
  const [pdfUrl, setPdfUrl] = React.useState(null);

  // Resolve blob URL for PDF items
  React.useEffect(() => {
    if (item?.type !== "pdf") return;
    if (item.url && !item.url.startsWith("blob:null")) { setPdfUrl(item.url); return; }
    if (audioBlobUrlCache.has(item.id)) { setPdfUrl(audioBlobUrlCache.get(item.id)); return; }
    loadAudioBlob(item.id).then(data => {
      if (!data) return;
      const blob = new Blob([data.buffer], { type: data.mime || "application/pdf" });
      const url = URL.createObjectURL(blob);
      audioBlobUrlCache.set(item.id, url);
      setPdfUrl(url);
    });
  }, [item?.id, item?.type]);

  if (!item) return null;

  const changeCover = async (file) => {
    if (!file) return;
    const compressed = await compressCoverArt(file);
    if (compressed) patchItem(item.id, { cover: compressed });
  };

  const insertAttachment = (file, kind) => {
    if (!file) return;
    const reader = new FileReader();
    reader.onload = () => {
      const src = String(reader.result || "");
      const html = kind === "image"
        ? `<img src="${src}" style="max-width:100%;border-radius:12px;margin:8px 0;display:block" />`
        : `<audio controls src="${src}" style="width:100%;margin:8px 0;display:block"></audio>`;
      patchItem(item.id, { note: (item.note || "") + html });
    };
    reader.readAsDataURL(file);
  };

  return (
    <div className="fixed inset-0 z-40 flex flex-col" style={{ background: t.page, color: t.text, animation: "slideUp 280ms cubic-bezier(0.32,0.72,0,1) both" }}>
      {/* Safe-area top spacer */}
      <div style={{ height: "env(safe-area-inset-top)", background: t.page, flexShrink: 0 }} />

      {/* Header */}
      <div className="flex shrink-0 items-center gap-3 px-4 py-3" style={{ borderBottom: `1px solid ${t.border}` }}>
        <button onClick={close} className="flex h-10 w-10 items-center justify-center rounded-full transition active:scale-95" style={{ background: t.input, color: t.muted }}>
          <ChevronLeft className="h-4 w-4" />
        </button>
        <span className="flex-1 text-center text-[11px] font-semibold uppercase tracking-[0.18em]" style={{ color: t.muted }}>{item.folder || "Ideas"}</span>
        <div style={{ width: 40 }} />
      </div>

      {/* Title + date */}
      <div className="shrink-0 px-5 pt-4 pb-2">
        <input
          value={item.title}
          onChange={e => patchItem(item.id, { title: e.target.value })}
          placeholder="Title"
          className="w-full bg-transparent text-[1.4rem] font-bold tracking-tight outline-none leading-snug"
          style={{ color: t.text }}
        />
        <p className="mt-1 text-[11px]" style={{ color: t.soft }}>{dateLabel(item.createdAt)}</p>
      </div>

      <div className="mx-5 h-px shrink-0" style={{ background: t.border }} />

      {/* Body — fills remaining space */}
      <div className="flex min-h-0 flex-1 flex-col px-5 overflow-hidden">
        {item.type === "pdf" ? (
          <div className="flex flex-1 flex-col overflow-hidden pt-4 pb-4 gap-3">
            {pdfUrl
              ? <iframe src={pdfUrl} style={{ flex: 1, border: "none", borderRadius: 14, background: "#fff", minHeight: 0 }} title={item.title} />
              : <div style={{ flex: 1, display: "flex", alignItems: "center", justifyContent: "center", color: t.muted, fontSize: 14 }}>Loading PDF…</div>
            }
          </div>
        ) : item.type === "note" ? (
          <RichNoteEditor
            value={item.note || ""}
            onChange={val => patchItem(item.id, { note: val })}
            t={t}
            onAttachImage={() => imgInputRef.current?.click()}
            onAttachAudio={() => audInputRef.current?.click()}
          />
        ) : (
          <div className="flex-1 overflow-y-auto pb-8 no-scrollbar pt-4">
            {/* Cover art strip for songs */}
            {item.type === "song" && (
              <div className="mb-4 flex items-center gap-3">
                <div style={{ position: "relative", width: 64, height: 64, borderRadius: 14, overflow: "hidden", flexShrink: 0, background: item.cover ? "transparent" : `linear-gradient(135deg,${t.glowA}22,${t.glowB}22)`, border: `0.5px solid ${t.border}` }}>
                  {item.cover && <img src={item.cover} style={{ width: "100%", height: "100%", objectFit: "cover", display: "block" }} />}
                  <button onClick={() => coverInputRef.current?.click()} style={{ position: "absolute", inset: 0, display: "flex", alignItems: "center", justifyContent: "center", background: item.cover ? "rgba(0,0,0,.35)" : "transparent", border: "none", cursor: "pointer", color: item.cover ? "#fff" : t.muted }}>
                    <Camera style={{ width: 20, height: 20 }} />
                  </button>
                </div>
                <p className="text-xs" style={{ color: t.muted }}>Tap to {item.cover ? "change" : "add"} cover art</p>
              </div>
            )}
            <textarea
              value={item.note || ""}
              onChange={e => patchItem(item.id, { note: e.target.value })}
              placeholder="Add a note..."
              className="min-h-[6rem] w-full resize-none bg-transparent text-[15px] leading-[1.75] outline-none"
              style={{ color: t.text }}
            />
            {item.type === "song" && (
              <div className="mt-4">
                <AudioCard item={item} t={t} theme={theme} activeAudioId={activeAudioId} setActiveAudioId={setActiveAudioId} />
              </div>
            )}
            {item.type === "image" && item.url && (
              <div className="mt-4 overflow-hidden rounded-2xl">
                <img src={item.url} alt={item.title} className="w-full h-auto" style={{ display: "block" }} />
              </div>
            )}
            {item.type === "link" && item.url && (
              <LinkPreviewCard url={normalizeUrl(item.url)} t={t} className="mt-4" />
            )}
          </div>
        )}
      </div>

      {/* Hidden file inputs */}
      <input ref={imgInputRef} type="file" accept="image/*" style={{ display: "none" }}
        onChange={e => { insertAttachment(e.target.files?.[0], "image"); e.target.value = ""; }} />
      <input ref={audInputRef} type="file" accept="audio/*" style={{ display: "none" }}
        onChange={e => { insertAttachment(e.target.files?.[0], "audio"); e.target.value = ""; }} />
      <input ref={coverInputRef} type="file" accept="image/*" style={{ display: "none" }}
        onChange={e => { changeCover(e.target.files?.[0]); e.target.value = ""; }} />

      {/* Safe-area bottom spacer */}
      <div style={{ height: "env(safe-area-inset-bottom)", background: t.page, flexShrink: 0 }} />
    </div>
  );
}

function PostMenu({ item, folders, t, openMenuId, setOpenMenuId, patchItem, removeItem }) {
  const open = openMenuId === item.id;
  const [folderOpen, setFolderOpen] = useState(false);
  const coverInputRef = useRef(null);
  const setCover = async (file) => {
    if (!file) return;
    const compressed = await compressCoverArt(file);
    if (compressed) patchItem(item.id, { cover: compressed });
    setOpenMenuId(null);
  };
  const copyItem = () => { if (item.url) navigator.clipboard?.writeText(item.url); setOpenMenuId(null); };
  const shareItem = async () => {
    const shareUrl = item.url ? normalizeUrl(item.url) : "";
    try {
      if (navigator.share && shareUrl) await navigator.share({ title: item.title, text: item.note || item.title, url: shareUrl });
      else if (shareUrl) await navigator.clipboard?.writeText(shareUrl);
    } catch {}
    setOpenMenuId(null);
  };
  const assignFolder = (name) => { patchItem(item.id, { folder: name }); setFolderOpen(false); setOpenMenuId(null); };
  useEffect(() => { if (!open) setFolderOpen(false); }, [open]);
  return (
    <div className="relative shrink-0">
      {open && (
        <div className="absolute bottom-12 right-0 z-20 w-52 rounded-2xl p-2 backdrop-blur-2xl dropdown-spring" style={{ background: t.panel, boxShadow: t.shadow, border: `1px solid ${t.border}` }}>
          <button onClick={copyItem} disabled={!item.url} className="flex w-full items-center gap-3 rounded-xl px-3 py-3 text-left text-sm font-semibold disabled:opacity-35 active:opacity-70" style={{ color: t.text }}><Copy className="h-4 w-4 shrink-0" />Copy link</button>
          <button onClick={shareItem} disabled={!item.url} className="flex w-full items-center gap-3 rounded-xl px-3 py-3 text-left text-sm font-semibold disabled:opacity-35 active:opacity-70" style={{ color: t.text }}><ExternalLink className="h-4 w-4 shrink-0" />Share</button>
          <button onClick={() => setFolderOpen((c) => !c)} className="flex w-full items-center gap-3 rounded-xl px-3 py-3 text-left text-sm font-semibold active:opacity-70" style={{ color: t.text }}><Folder className="h-4 w-4 shrink-0" />Move to folder</button>
          {folderOpen && (
            <div className="mb-1 ml-4 rounded-xl p-1" style={{ background: t.input }}>
              <button onClick={() => assignFolder(null)} className="block w-full rounded-lg px-3 py-2.5 text-left text-xs font-semibold active:opacity-70" style={{ color: !item.folder ? t.accent : t.muted }}>— No folder</button>
              {folders.map((name) => (
                <button key={name} onClick={() => assignFolder(name)} className="block w-full rounded-lg px-3 py-2.5 text-left text-xs font-semibold active:opacity-70" style={{ color: item.folder === name ? t.accent : t.muted }}>{name}</button>
              ))}
            </div>
          )}
          <button onClick={() => coverInputRef.current?.click()} className="flex w-full items-center gap-3 rounded-xl px-3 py-3 text-left text-sm font-semibold active:opacity-70" style={{ color: t.text }}><Camera className="h-4 w-4 shrink-0" />Set Cover</button>
          {item.cover && <button onClick={() => { patchItem(item.id, { cover: null }); setOpenMenuId(null); }} className="flex w-full items-center gap-3 rounded-xl px-3 py-3 text-left text-sm font-semibold active:opacity-70" style={{ color: t.muted }}><X className="h-4 w-4 shrink-0" />Remove Cover</button>}
          <button onClick={() => removeItem(item.id)} className="flex w-full items-center gap-3 rounded-xl px-3 py-3 text-left text-sm font-semibold active:opacity-70" style={{ color: "#ff6b6b" }}><Trash2 className="h-4 w-4 shrink-0" />Delete</button>
        </div>
      )}
      <button onClick={() => setOpenMenuId(open ? null : item.id)} className="flex h-10 w-10 items-center justify-center rounded-full transition active:scale-95" style={{ background: t.input, color: t.muted }} aria-label="Post actions">
        <MoreHorizontal className="h-4 w-4" />
      </button>
      <input ref={coverInputRef} type="file" accept="image/*" style={{ display: "none" }}
        onChange={e => { setCover(e.target.files?.[0]); e.target.value = ""; }} />
    </div>
  );
}

const VIBES = [
  { id: "dark",   label: "Dark",   swatches: ["#07080F", "#1D2040", "#7DBDFF"] },
  { id: "light",  label: "Light",  swatches: ["#EEF2EC", "#8A928A", "#58C820"] },
  { id: "franki", label: "Franki", swatches: ["#0D0305", "#7A3020", "#E8723A"] },
  { id: "ice",    label: "Ice",    swatches: ["#5BAAC4", "#A899C4", "#F0C8DC"] },
  { id: "grape",  label: "Grape",  swatches: ["#2F0147", "#610F7F", "#9C528B"] },
];

function SyncStatus({ status, t }) {
  if (!status) return null;
  let dotColor = "#888";
  let label = "";

  if (status.syncing) {
    dotColor = "#FFA500";
    label = "Syncing...";
  } else if (!navigator.onLine && status.pending > 0) {
    dotColor = "#FF6B6B";
    label = `${status.pending} pending`;
  } else if (status.synced) {
    dotColor = "#4ECB71";
    label = "Synced";
  } else if (status.pending > 0) {
    dotColor = "#FFD700";
    label = `${status.pending} pending`;
  }

  return (
    <div className="flex items-center gap-2 text-xs" style={{ color: dotColor }}>
      <div className="w-2 h-2 rounded-full" style={{ backgroundColor: dotColor }} />
      {label}
    </div>
  );
}

function CustomThemeEditor({ initial, t, onSave, onDelete, onClose }) {
  const [name, setName] = useState(initial?.label || "My Theme");
  const [page, setPage] = useState(initial?.page || "#0D0D1A");
  const [accent, setAccent] = useState(initial?.accent || "#7B61FF");
  const [text, setText] = useState(initial?.text || "#E8E8FF");
  const preview = buildCustomTheme(page, accent, text);

  const ColorRow = ({ label, value, onChange }) => (
    <div className="flex items-center justify-between py-2">
      <span className="text-sm font-medium" style={{ color: t.text }}>{label}</span>
      <label className="relative flex cursor-pointer items-center gap-3">
        <span className="text-xs font-mono" style={{ color: t.muted }}>{value.toUpperCase()}</span>
        <div className="h-9 w-9 rounded-full" style={{ background: value, boxShadow: `0 0 0 2px ${t.border}, 0 0 0 4px ${value}44` }} />
        <input type="color" value={value} onChange={e => onChange(e.target.value)} className="absolute inset-0 h-full w-full cursor-pointer opacity-0" />
      </label>
    </div>
  );

  return (
    <div className="fixed inset-0 z-[60] flex items-end justify-center">
      <button onClick={onClose} className="absolute inset-0 backdrop-blur-md" style={{ background: "rgba(0,0,0,.45)" }} />
      <div className="relative w-full max-w-md rounded-t-[2rem] p-6 pb-10" style={{ background: t.panel2, border: `1px solid ${t.border}`, boxShadow: t.shadow, animation: "slideUp 320ms cubic-bezier(0.32,0.72,0,1) both" }}>
        <div className="mb-5 flex justify-center"><div className="h-1 w-10 rounded-full" style={{ background: t.border }} /></div>
        <h2 className="mb-5 text-lg font-bold" style={{ color: t.text }}>Custom Theme</h2>
        <input value={name} onChange={e => setName(e.target.value)} placeholder="Theme name" className="mb-5 w-full rounded-2xl px-4 py-3 text-sm font-medium outline-none" style={{ background: t.input, color: t.text }} />

        <div className="mb-5 divide-y" style={{ borderColor: t.border }}>
          <ColorRow label="Background" value={page} onChange={setPage} />
          <ColorRow label="Accent" value={accent} onChange={setAccent} />
          <ColorRow label="Text" value={text} onChange={setText} />
        </div>

        {/* Live preview */}
        <div className="mb-6 rounded-2xl p-4" style={{ background: preview.panel, border: `1px solid ${preview.border}` }}>
          <div className="flex items-center gap-3">
            <div className="h-8 w-8 rounded-full" style={{ background: `linear-gradient(135deg,${preview.glowA},${preview.glowB})` }} />
            <div className="flex-1">
              <p className="text-sm font-semibold" style={{ color: preview.text }}>{name || "My Theme"}</p>
              <p className="text-xs" style={{ color: preview.muted }}>Preview</p>
            </div>
            <div className="h-6 w-6 rounded-full" style={{ background: preview.accent }} />
          </div>
        </div>

        <div className="flex gap-3">
          {initial && (
            <button onClick={onDelete} className="flex h-12 w-12 shrink-0 items-center justify-center rounded-2xl" style={{ background: "rgba(255,60,60,.12)", color: "#ff6b6b" }}>
              <Trash2 className="h-4 w-4" />
            </button>
          )}
          <button onClick={onClose} className="h-12 flex-1 rounded-2xl text-sm font-semibold" style={{ background: t.input, color: t.text }}>Cancel</button>
          <button onClick={() => onSave({ label: name.trim() || "Custom", page, accent, text })} className="h-12 flex-1 rounded-2xl text-sm font-semibold" style={{ background: `linear-gradient(135deg,${t.glowA},${t.glowB})`, color: "#fff" }}>Save</button>
        </div>
      </div>
    </div>
  );
}

function VibeDropdown({ theme, setTheme, t, customThemes, setCustomThemes }) {
  const [open, setOpen] = useState(false);
  const [editingTheme, setEditingTheme] = useState(null); // null | "new" | {id,label,page,accent,text}
  const ref = useRef(null);
  const current = VIBES.find(v => v.id === theme) || customThemes.find(v => v.id === theme) || VIBES[0];
  const swatches = current.swatches || [current.page, current.accent, current.text];

  useEffect(() => {
    if (!open) return;
    const handler = (e) => { if (ref.current && !ref.current.contains(e.target)) setOpen(false); };
    document.addEventListener("mousedown", handler);
    return () => document.removeEventListener("mousedown", handler);
  }, [open]);

  const saveCustomTheme = (data) => {
    if (editingTheme === "new") {
      const newTheme = { ...data, id: `custom_${Date.now()}` };
      setCustomThemes(prev => [...prev, newTheme]);
      setTheme(newTheme.id);
    } else {
      setCustomThemes(prev => prev.map(ct => ct.id === editingTheme.id ? { ...editingTheme, ...data } : ct));
    }
    setEditingTheme(null);
    setOpen(false);
  };

  const deleteCustomTheme = () => {
    setCustomThemes(prev => prev.filter(ct => ct.id !== editingTheme.id));
    if (theme === editingTheme.id) setTheme("dark");
    setEditingTheme(null);
  };

  return (
    <div ref={ref} className="relative">
      <button
        onClick={() => setOpen(o => !o)}
        className="flex items-center gap-2 rounded-full px-4 py-2.5 text-xs font-semibold uppercase tracking-[0.08em] backdrop-blur-xl active:scale-95 transition"
        style={{ background: t.panel, border: `1px solid ${t.border}`, color: t.text, boxShadow: `${t.shadow}, 0 0 16px ${t.glowA}30` }}
      >
        <div className="flex gap-1">
          {swatches.map((c, i) => <div key={i} className="h-3 w-3 rounded-full" style={{ background: c, boxShadow: `0 0 0 1px rgba(255,255,255,.15)` }} />)}
        </div>
        Vibe
        <ChevronDown className="h-3 w-3" style={{ color: t.muted, transform: open ? "rotate(180deg)" : "none", transition: "transform 200ms" }} />
      </button>

      {open && (
        <div className="absolute right-0 top-full mt-2 z-50 w-48 overflow-hidden rounded-2xl p-1 dropdown-spring" style={{ background: t.panel2, border: `1px solid ${t.border}`, boxShadow: t.shadow }}>
          {VIBES.map(({ id, label, swatches }) => (
            <button key={id} onClick={() => { setTheme(id); setOpen(false); }}
              className="flex w-full items-center gap-2.5 rounded-xl px-3 py-2.5 text-left text-[12px] font-semibold"
              style={{
                background: theme === id ? t.active : "transparent", color: t.text,
                transition: "background 150ms ease",
              }}>
              <div className="flex gap-1">
                {swatches.map((c, i) => (
                  <div key={i} className={`h-3.5 w-3.5 rounded-full${theme === id && i === 0 ? " dot-breathe" : ""}`}
                    style={{ background: c, boxShadow: `0 0 0 1px rgba(255,255,255,.1)` }} />
                ))}
              </div>
              {label}
              {theme === id && <span className="ml-auto text-[10px]" style={{ color: t.accent }}>✓</span>}
            </button>
          ))}

          {customThemes.length > 0 && (
            <>
              <div className="my-1 mx-2 h-px" style={{ background: t.border }} />
              {customThemes.map(ct => (
                <div key={ct.id} className="flex items-center gap-1 rounded-xl px-1 py-0.5" style={{ background: theme === ct.id ? t.active : "transparent" }}>
                  <button onClick={() => { setTheme(ct.id); setOpen(false); }}
                    className="flex flex-1 items-center gap-2 px-2 py-2 text-left text-[12px] font-semibold"
                    style={{ color: t.text }}>
                    <div className="flex gap-1">
                      {[ct.page, ct.accent, ct.text].map((c, i) => <div key={i} className="h-3.5 w-3.5 rounded-full" style={{ background: c, boxShadow: `0 0 0 1px rgba(255,255,255,.1)` }} />)}
                    </div>
                    <span className="truncate">{ct.label}</span>
                    {theme === ct.id && <span className="ml-auto shrink-0 text-[10px]" style={{ color: t.accent }}>✓</span>}
                  </button>
                  <button onClick={() => setEditingTheme(ct)} className="flex h-7 w-7 shrink-0 items-center justify-center rounded-lg" style={{ color: t.muted }}>
                    <Pencil className="h-3 w-3" />
                  </button>
                </div>
              ))}
            </>
          )}

          <div className="my-1 mx-2 h-px" style={{ background: t.border }} />
          <button onClick={() => setEditingTheme("new")}
            className="flex w-full items-center gap-2 rounded-xl px-3 py-2.5 text-[12px] font-semibold"
            style={{ color: t.glowA }}>
            <Plus className="h-3.5 w-3.5" /> New Theme
          </button>
        </div>
      )}

      {editingTheme !== null && (
        <CustomThemeEditor
          initial={editingTheme === "new" ? null : editingTheme}
          t={t}
          onSave={saveCustomTheme}
          onDelete={deleteCustomTheme}
          onClose={() => setEditingTheme(null)}
        />
      )}
    </div>
  );
}

// ── SettingsDrawer ───────────────────────────────────────────────────────────
function SettingsDrawer({ open, onClose, t, theme, setTheme, currentUser, setCurrentUser, onGoProfile }) {
  const [notifPermission, setNotifPermission] = useState(() => {
    if (typeof Notification === "undefined") return "unsupported";
    return Notification.permission;
  });
  const primaryBg = `linear-gradient(135deg, ${t.glowA} 0%, ${t.glowB} 100%)`;
  const requestNotifications = async () => {
    if (typeof Notification === "undefined") return;
    if (notifPermission === "granted") {
      // Can't revoke via JS — guide user to browser settings
      setNotifPermission("prompt-revoke");
      setTimeout(() => setNotifPermission("granted"), 3000);
      return;
    }
    const result = await Notification.requestPermission();
    setNotifPermission(result);
    if (result === "granted") {
      new Notification("Flare", { body: "Notifications are on 🔔", icon: "/favicon.ico" });
    }
  };
  return (
    <>
      {/* Backdrop */}
      <div
        onClick={onClose}
        style={{
          position: "fixed", inset: 0, zIndex: 40,
          background: "rgba(0,0,0,.45)",
          backdropFilter: "blur(4px)",
          WebkitBackdropFilter: "blur(4px)",
          opacity: open ? 1 : 0,
          pointerEvents: open ? "auto" : "none",
          transition: "opacity 280ms ease",
        }}
      />

      {/* Drawer */}
      <div style={{
        position: "fixed", top: 0, right: 0, bottom: 0, zIndex: 50,
        width: "min(85vw, 340px)",
        background: t.panel2,
        borderLeft: `0.5px solid ${t.border}`,
        backdropFilter: "blur(32px) saturate(160%)",
        WebkitBackdropFilter: "blur(32px) saturate(160%)",
        boxShadow: "-8px 0 40px rgba(0,0,0,.35)",
        transform: open ? "translateX(0)" : "translateX(100%)",
        transition: "transform 300ms cubic-bezier(.32,.72,0,1)",
        display: "flex", flexDirection: "column",
        paddingTop: "max(24px, env(safe-area-inset-top))",
        paddingBottom: "max(24px, env(safe-area-inset-bottom))",
        overflowY: "auto",
      }}>
        {/* Header row */}
        <div style={{ display: "flex", alignItems: "center", justifyContent: "space-between", padding: "0 20px 20px" }}>
          <p style={{ margin: 0, fontWeight: 800, fontSize: 18, color: t.text }}>Settings</p>
          <button onClick={onClose} style={{ background: t.active, border: "none", borderRadius: "50%", width: 32, height: 32, display: "flex", alignItems: "center", justifyContent: "center", cursor: "pointer", color: t.muted }}>
            <X style={{ width: 16, height: 16 }} />
          </button>
        </div>

        <div style={{ flex: 1, display: "flex", flexDirection: "column", gap: 8, padding: "0 14px" }}>
          {/* ── Profile card ── */}
          <div style={{ background: t.panel, borderRadius: 20, padding: 16, border: `0.5px solid ${t.border}` }}>
            <p style={{ margin: "0 0 12px", fontSize: 11, fontWeight: 700, textTransform: "uppercase", letterSpacing: "0.1em", color: t.muted }}>Account</p>
            {currentUser ? (
              <button onClick={() => { onGoProfile(); onClose(); }}
                style={{ display: "flex", alignItems: "center", gap: 12, width: "100%", background: "none", border: "none", cursor: "pointer", padding: 0, textAlign: "left" }}>
                {currentUser.avatar
                  ? <img src={currentUser.avatar} style={{ width: 44, height: 44, borderRadius: "50%", objectFit: "cover", flexShrink: 0 }} />
                  : <div style={{ width: 44, height: 44, borderRadius: "50%", background: primaryBg, display: "flex", alignItems: "center", justifyContent: "center", flexShrink: 0 }}>
                      <UserCircle style={{ width: 24, height: 24, color: "#fff" }} />
                    </div>
                }
                <div style={{ minWidth: 0 }}>
                  <p style={{ margin: 0, fontWeight: 700, fontSize: 15, color: t.text, overflow: "hidden", textOverflow: "ellipsis", whiteSpace: "nowrap" }}>{currentUser.name}</p>
                  <p style={{ margin: 0, fontSize: 12, color: t.muted, overflow: "hidden", textOverflow: "ellipsis", whiteSpace: "nowrap" }}>{currentUser.email}</p>
                </div>
                <ChevronLeft style={{ width: 16, height: 16, color: t.muted, transform: "rotate(180deg)", flexShrink: 0, marginLeft: "auto" }} />
              </button>
            ) : (
              <button onClick={() => { onGoProfile(); onClose(); }}
                style={{ display: "flex", alignItems: "center", gap: 12, width: "100%", background: t.input, border: `1px solid ${t.border}`, borderRadius: 14, padding: "12px 14px", cursor: "pointer" }}>
                <UserCircle style={{ width: 22, height: 22, color: t.muted }} />
                <div style={{ textAlign: "left" }}>
                  <p style={{ margin: 0, fontWeight: 700, fontSize: 14, color: t.text }}>Sign in</p>
                  <p style={{ margin: 0, fontSize: 12, color: t.muted }}>Sync your vault across devices</p>
                </div>
                <ChevronLeft style={{ width: 16, height: 16, color: t.muted, transform: "rotate(180deg)", flexShrink: 0, marginLeft: "auto" }} />
              </button>
            )}
          </div>

          {/* ── Appearance / Vibe ── */}
          <div style={{ background: t.panel, borderRadius: 20, padding: 16, border: `0.5px solid ${t.border}` }}>
            <p style={{ margin: "0 0 12px", fontSize: 11, fontWeight: 700, textTransform: "uppercase", letterSpacing: "0.1em", color: t.muted }}>Appearance</p>
            <div style={{ display: "flex", flexDirection: "column", gap: 2 }}>
              {VIBES.map(({ id, label, swatches }) => (
                <button key={id} onClick={() => setTheme(id)}
                  style={{ display: "flex", alignItems: "center", gap: 10, padding: "10px 12px", borderRadius: 12, border: "none", cursor: "pointer",
                    background: theme === id ? t.active : "transparent", color: t.text, textAlign: "left", transition: "background 150ms" }}>
                  <div style={{ display: "flex", gap: 4 }}>
                    {swatches.map((c, i) => <div key={i} style={{ width: 14, height: 14, borderRadius: "50%", background: c, boxShadow: "0 0 0 1px rgba(255,255,255,.1)" }} />)}
                  </div>
                  <span style={{ fontWeight: 600, fontSize: 14 }}>{label}</span>
                  {theme === id && <span style={{ marginLeft: "auto", fontSize: 12, color: t.accent }}>✓</span>}
                </button>
              ))}

            </div>
          </div>
          {/* ── Notifications ── */}
          <div style={{ background: t.panel, borderRadius: 20, padding: 16, border: `0.5px solid ${t.border}` }}>
            <p style={{ margin: "0 0 12px", fontSize: 11, fontWeight: 700, textTransform: "uppercase", letterSpacing: "0.1em", color: t.muted }}>Notifications</p>
            <div style={{ display: "flex", alignItems: "center", justifyContent: "space-between", gap: 12 }}>
              <div style={{ display: "flex", alignItems: "center", gap: 10 }}>
                {notifPermission === "granted"
                  ? <Bell style={{ width: 18, height: 18, color: t.glowA }} />
                  : <BellOff style={{ width: 18, height: 18, color: t.muted }} />}
                <div>
                  <p style={{ margin: 0, fontWeight: 600, fontSize: 14, color: t.text }}>Push notifications</p>
                  <p style={{ margin: 0, fontSize: 12, color: t.muted, marginTop: 1 }}>
                    {notifPermission === "granted" ? "Enabled" :
                     notifPermission === "denied" ? "Blocked in browser" :
                     notifPermission === "unsupported" ? "Not supported" :
                     notifPermission === "prompt-revoke" ? "Turn off in browser settings" :
                     "Get reminders & updates"}
                  </p>
                </div>
              </div>
              {/* Toggle */}
              {notifPermission !== "unsupported" && notifPermission !== "denied" && (
                <button
                  onClick={requestNotifications}
                  style={{
                    flexShrink: 0, width: 44, height: 26, borderRadius: 13, border: "none", cursor: "pointer",
                    background: notifPermission === "granted" ? t.glowA : t.input,
                    position: "relative", transition: "background 220ms ease",
                  }}
                >
                  <div style={{
                    position: "absolute", top: 3, left: notifPermission === "granted" ? 21 : 3,
                    width: 20, height: 20, borderRadius: "50%", background: "#fff",
                    boxShadow: "0 1px 4px rgba(0,0,0,.3)",
                    transition: "left 220ms cubic-bezier(.32,.72,0,1)",
                  }} />
                </button>
              )}
              {notifPermission === "denied" && (
                <span style={{ fontSize: 11, color: "#FF6B6B", fontWeight: 600 }}>Blocked</span>
              )}
            </div>
          </div>

          {/* ── About ── */}
          <div style={{ background: t.panel, borderRadius: 20, overflow: "hidden", border: `0.5px solid ${t.border}` }}>
            <p style={{ margin: 0, padding: "14px 16px 10px", fontSize: 11, fontWeight: 700, textTransform: "uppercase", letterSpacing: "0.1em", color: t.muted }}>About</p>

            {[
              { icon: <Star style={{ width: 17, height: 17 }} />, label: "Rate Flare", sub: "Enjoying the app? Leave a review", action: () => {} },
              { icon: <MessageSquare style={{ width: 17, height: 17 }} />, label: "Send Feedback", sub: "Report a bug or share an idea", action: () => window.open("mailto:feedback@flare.app?subject=Flare Feedback") },
              { icon: <InfoIcon style={{ width: 17, height: 17 }} />, label: "Privacy Policy", sub: null, action: () => {} },
            ].map(({ icon, label, sub, action }, i, arr) => (
              <button key={label} onClick={action}
                style={{
                  display: "flex", alignItems: "center", gap: 12, width: "100%",
                  padding: "12px 16px", border: "none", background: "transparent",
                  borderBottom: i < arr.length - 1 ? `0.5px solid ${t.border}` : "none",
                  cursor: "pointer", textAlign: "left",
                }}>
                <span style={{ color: t.muted, display: "flex", flexShrink: 0 }}>{icon}</span>
                <div style={{ flex: 1, minWidth: 0 }}>
                  <p style={{ margin: 0, fontWeight: 600, fontSize: 14, color: t.text }}>{label}</p>
                  {sub && <p style={{ margin: 0, fontSize: 12, color: t.muted, marginTop: 1 }}>{sub}</p>}
                </div>
                <ChevronLeft style={{ width: 15, height: 15, color: t.muted, transform: "rotate(180deg)", flexShrink: 0 }} />
              </button>
            ))}

            <div style={{ padding: "12px 16px", borderTop: `0.5px solid ${t.border}`, display: "flex", justifyContent: "space-between", alignItems: "center" }}>
              <p style={{ margin: 0, fontSize: 12, color: t.muted }}>Flare by Loveem</p>
              <span style={{ fontSize: 11, fontWeight: 700, color: t.muted, background: t.active, padding: "3px 8px", borderRadius: 6 }}>v1.0.0</span>
            </div>
          </div>

        </div>
      </div>

    </>
  );
}

function SlidingTabBar({ tabs, active, onChange, t, className = "", textSize = "text-[13px]", py = "py-2.5", px = "", equalWidth = true }) {
  const containerRef = React.useRef(null);
  const [pill, setPill] = React.useState({ left: 0, width: 0, ready: false });

  React.useEffect(() => {
    const container = containerRef.current;
    if (!container) return;
    const measure = () => {
      const btn = container.querySelector("[data-active='true']");
      if (!btn) return;
      const cr = container.getBoundingClientRect();
      const br = btn.getBoundingClientRect();
      if (br.width === 0) return;
      setPill({ left: br.left - cr.left, width: br.width, ready: true });
    };
    measure();
    const ro = new ResizeObserver(measure);
    ro.observe(container);
    return () => ro.disconnect();
  }, [active]);

  return (
    <div ref={containerRef} className={`relative flex ${className}`}>
      <div
        className="absolute inset-y-0 rounded-full"
        style={{
          left: pill.left,
          width: pill.width,
          background: t.active,
          boxShadow: `0 0 14px ${t.glowA}28`,
          transition: pill.ready ? "left 300ms var(--spring-smooth), width 300ms var(--spring-smooth)" : "none",
        }}
      />
      {tabs.map(([id, label]) => (
        <button
          key={id}
          data-active={active === id}
          onClick={() => onChange(id)}
          className={`relative z-10 ${equalWidth ? "flex-1" : ""} ${py} ${px} ${textSize} font-semibold tracking-tight transition-colors duration-200`}
          style={{ color: active === id ? t.text : t.muted, background: "transparent" }}
        >
          {label}
        </button>
      ))}
    </div>
  );
}

function ColorPicker({ color, onChange, strokeWidth, onWidthChange, onClose, boardDark }) {
  const presets = ["#1768FF","#FF3B30","#34C759","#FF9500","#AF52DE","#FF2D55","#FFCC00","#5AC8FA","#000000","#ffffff"];
  const bg = boardDark ? "rgba(18,20,38,.97)" : "rgba(250,250,248,.97)";
  const border = boardDark ? "rgba(255,255,255,.1)" : "rgba(0,0,0,.1)";
  const muted = boardDark ? "rgba(255,255,255,.35)" : "rgba(0,0,0,.35)";
  const active = boardDark ? "rgba(255,255,255,.15)" : "rgba(0,0,0,.12)";
  return (
    <div onPointerDown={e=>e.stopPropagation()} style={{position:"fixed",bottom:"calc(88px + env(safe-area-inset-bottom))",left:"50%",transform:"translateX(-50%)",background:bg,borderRadius:20,padding:16,zIndex:62,boxShadow:"0 8px 40px rgba(0,0,0,.45)",border:`1px solid ${border}`,minWidth:230}}>
      <div style={{display:"flex",justifyContent:"space-between",alignItems:"center",marginBottom:12}}>
        <span style={{fontSize:10,fontWeight:700,letterSpacing:"0.12em",textTransform:"uppercase",color:muted}}>Color & Stroke</span>
        <button onClick={onClose} style={{fontSize:14,border:"none",background:"transparent",cursor:"pointer",color:muted,lineHeight:1,padding:4}}>✕</button>
      </div>
      <div style={{display:"flex",flexWrap:"wrap",gap:7,marginBottom:12,justifyContent:"center"}}>
        {presets.map(c=>(
          <button key={c} onClick={()=>onChange(c)}
            style={{width:30,height:30,borderRadius:"50%",background:c,border:color===c?`2.5px solid ${boardDark?"#fff":"#333"}`:"1.5px solid rgba(128,128,128,.25)",cursor:"pointer",flexShrink:0,boxShadow:c==="#ffffff"?"inset 0 0 0 1px rgba(0,0,0,.15)":"none"}} />
        ))}
      </div>
      <div style={{display:"flex",alignItems:"center",gap:8,marginBottom:14}}>
        <input type="color" value={color} onChange={e=>onChange(e.target.value)}
          style={{width:48,height:34,border:`1px solid ${border}`,borderRadius:8,cursor:"pointer",background:"transparent",padding:2,flexShrink:0}} />
        <span style={{fontSize:11,fontFamily:"monospace",color:muted,flex:1}}>{color.toUpperCase()}</span>
      </div>
      <div style={{borderTop:`1px solid ${border}`,paddingTop:12}}>
        <span style={{fontSize:10,fontWeight:700,letterSpacing:"0.1em",textTransform:"uppercase",color:muted}}>Stroke width</span>
        <div style={{display:"flex",gap:6,marginTop:8,alignItems:"center"}}>
          {[1,3,6,10].map(w=>(
            <button key={w} onClick={()=>onWidthChange(w)}
              style={{flex:1,height:38,display:"flex",alignItems:"center",justifyContent:"center",background:strokeWidth===w?active:"transparent",borderRadius:10,border:`1px solid ${strokeWidth===w?border:"transparent"}`,cursor:"pointer"}}>
              <div style={{width:"60%",height:Math.min(w,6),borderRadius:3,background:strokeWidth===w?color:muted}}/>
            </button>
          ))}
        </div>
      </div>
    </div>
  );
}

function CanvasNode({ node, selected, t, theme, boardDark, tool, onSelect, onDragStart, onUpdate, onResize, scale, onDelete }) {
  const noteColors = ["#FFF176","#A8D8EA","#FFDAC1","#B5EAD7","#C7CEEA","#FFB7B2"];
  const isSelect = tool === "select";
  const resizeRef = useRef(null);

  // Bottom-right resize handle (width-based; height stays auto for media/text)
  const resizeHandle = selected ? (
    <div
      onPointerDown={(e)=>{ e.stopPropagation(); try{e.currentTarget.setPointerCapture(e.pointerId);}catch{} resizeRef.current={x:e.clientX,w:node.w||220}; }}
      onPointerMove={(e)=>{ const R=resizeRef.current; if(!R) return; e.stopPropagation(); onResize&&onResize(Math.max(90, R.w+(e.clientX-R.x)/(scale||1))); }}
      onPointerUp={(e)=>{ e.stopPropagation(); resizeRef.current=null; }}
      onPointerCancel={(e)=>{ e.stopPropagation(); resizeRef.current=null; }}
      style={{position:"absolute",bottom:-13,right:-13,width:26,height:26,borderRadius:"50%",background:"#1768FF",border:"2.5px solid #fff",boxShadow:"0 2px 6px rgba(23,104,255,.5)",cursor:"nwse-resize",display:"flex",alignItems:"center",justifyContent:"center",touchAction:"none",zIndex:25}}>
      <svg width="11" height="11" viewBox="0 0 24 24" fill="none" stroke="#fff" strokeWidth="3" strokeLinecap="round"><path d="M9 21H3v-6M21 3l-7.5 7.5M3 21l7.5-7.5M15 3h6v6"/></svg>
    </div>
  ) : null;

  // Freeform-style: drag from anywhere on the node (not just a handle strip)
  const onDown = (e) => {
    if (!isSelect) return;
    const tag = e.target.tagName;
    // Let buttons, inputs and textareas handle their own events
    if (tag === "BUTTON" || tag === "INPUT" || tag === "A") return;
    onDragStart(e);
  };
  const onClick = () => { if (isSelect) onSelect(); };

  // Freeform-style blue selection handles: border + 8 corner/edge dots
  const selHandles = selected ? (
    <div style={{position:"absolute",inset:-5,borderRadius:20,border:"2px solid #1768FF",pointerEvents:"none",zIndex:15}}>
      {[
        {top:-5,left:-5},{top:-5,left:"calc(50% - 5px)"},{top:-5,right:-5},
        {top:"calc(50% - 5px)",right:-5},
        {bottom:-5,right:-5},{bottom:-5,left:"calc(50% - 5px)"},{bottom:-5,left:-5},
        {top:"calc(50% - 5px)",left:-5},
      ].map((pos,i)=>(
        <div key={i} style={{position:"absolute",...pos,width:10,height:10,borderRadius:"50%",background:"#1768FF",border:"2.5px solid #fff",boxShadow:"0 1px 5px rgba(23,104,255,.45)"}}/>
      ))}
    </div>
  ) : null;

  const baseStyle = {
    position:"absolute", left:node.x, top:node.y,
    borderRadius:16, cursor:isSelect?"grab":"default", userSelect:"none",
    boxShadow: selected ? "0 8px 36px rgba(0,0,0,.18)" : "0 4px 20px rgba(0,0,0,.18)",
    transition:"box-shadow .15s ease",
    willChange:"transform",
  };

  if (node.type === "text") {
    const [editing, setEditing] = useState(node.text === "");
    useEffect(() => { if (!selected) setEditing(false); }, [selected]);
    const textareaRef = useRef(null);
    useEffect(() => { if (node.text === "") setTimeout(() => textareaRef.current?.focus(), 50); }, []);
    const fontSizes = [12,16,20,28,36,48];
    const fmBg = "rgba(250,250,248,.97)";
    const fmBorder = "rgba(0,0,0,.1)";
    const fmActive = "rgba(0,0,0,.13)";
    const fmText = "#111";
    const fmMuted = "rgba(0,0,0,.35)";
    const enterEdit = (e) => { e.stopPropagation(); setEditing(true); setTimeout(() => textareaRef.current?.focus(), 30); };
    return (
      <div data-node-id={node.id} style={{position:"absolute",left:node.x,top:node.y,cursor:isSelect?"grab":"text"}}
        onPointerDown={(e)=>{
          if (!isSelect) return;
          if (editing && e.target.tagName === "TEXTAREA") return;
          if (e.target.tagName === "BUTTON" || e.target.tagName === "INPUT") return;
          onDragStart(e);
        }}
        onClick={onClick}>
        {selHandles}
        {resizeHandle}
        {selected && (
          <div onPointerDown={e=>e.stopPropagation()} style={{position:"absolute",top:-52,left:0,display:"flex",gap:3,alignItems:"center",background:fmBg,borderRadius:12,padding:"5px 8px",boxShadow:"0 4px 20px rgba(0,0,0,.22)",border:`1px solid ${fmBorder}`,whiteSpace:"nowrap",zIndex:30,overflowX:"auto",maxWidth:"90vw"}}>
            <button onClick={()=>onUpdate({bold:!node.bold})} style={{fontWeight:"bold",fontSize:13,width:28,height:28,border:"none",borderRadius:7,cursor:"pointer",background:node.bold?fmActive:"transparent",color:node.bold?fmText:fmMuted,flexShrink:0}}>B</button>
            <button onClick={()=>onUpdate({italic:!node.italic})} style={{fontStyle:"italic",fontSize:13,width:28,height:28,border:"none",borderRadius:7,cursor:"pointer",background:node.italic?fmActive:"transparent",color:node.italic?fmText:fmMuted,flexShrink:0}}>I</button>
            <div style={{width:1,height:16,background:fmBorder,margin:"0 2px",flexShrink:0}}/>
            {fontSizes.map(s=>(
              <button key={s} onClick={()=>onUpdate({fontSize:s})} style={{fontSize:9,width:26,height:24,border:"none",borderRadius:6,cursor:"pointer",fontWeight:700,background:node.fontSize===s?fmActive:"transparent",color:node.fontSize===s?fmText:fmMuted,flexShrink:0}}>
                {s<=12?"Xs":s<=16?"S":s<=20?"M":s<=28?"L":s<=36?"XL":"2X"}
              </button>
            ))}
            <div style={{width:1,height:16,background:fmBorder,margin:"0 2px",flexShrink:0}}/>
            {["#111111","#ffffff","#1768FF","#FF3B30","#34C759","#FF9500","#FFCC00","#AF52DE","#FF2D55"].map(c=>(
              <button key={c} onClick={()=>onUpdate({color:c})}
                style={{width:18,height:18,borderRadius:"50%",background:c,border:(node.color||"#111111")===c?`2.5px solid #1768FF`:"1.5px solid rgba(128,128,128,.3)",cursor:"pointer",flexShrink:0,boxShadow:c==="#ffffff"?"inset 0 0 0 1px rgba(0,0,0,.2)":"none"}} />
            ))}
            <div style={{width:1,height:16,background:fmBorder,margin:"0 2px",flexShrink:0}}/>
            <button onPointerDown={e=>e.stopPropagation()} onClick={enterEdit}
              style={{fontSize:10,height:24,padding:"0 10px",border:`1px solid ${fmBorder}`,borderRadius:7,cursor:"pointer",fontWeight:700,background:editing?fmActive:"transparent",color:editing?"#1768FF":fmMuted,flexShrink:0,whiteSpace:"nowrap"}}>
              {editing ? "✓ Done" : "Edit"}
            </button>
          </div>
        )}
        {/* Tap-to-select / drag overlay when not editing */}
        {!editing && (
          <div
            onPointerDown={e=>{ e.stopPropagation(); if(isSelect) onDragStart(e); }}
            onDoubleClick={enterEdit}
            style={{position:"absolute",inset:0,cursor:isSelect?"grab":"text",zIndex:5,borderRadius:4,touchAction:"none"}} />
        )}
        <textarea
          ref={textareaRef}
          value={node.text}
          onChange={e=>onUpdate({text:e.target.value})}
          onPointerDown={e=>{ if(!editing) e.stopPropagation(); }}
          readOnly={!editing}
          placeholder="Type here…"
          onInput={e=>{e.target.style.height="auto";e.target.style.height=e.target.scrollHeight+"px";}}
          style={{display:"block",background:"transparent",border:"none",outline:"none",resize:"none",fontFamily:"inherit",
            padding:"2px 4px",margin:0,lineHeight:1.45,
            cursor: editing ? "text" : "default",
            width:node.w||200, minWidth:80, minHeight:30,
            fontSize:node.fontSize||16,
            color:node.color||"#111111",
            fontWeight:node.bold?"bold":"normal",
            fontStyle:node.italic?"italic":"normal",
            pointerEvents: editing ? "auto" : "none",
          }}
        />
      </div>
    );
  }

  if (node.type === "note") return (
    <div data-node-id={node.id} style={{...baseStyle, width:node.w||200, minHeight:130, background:node.color||"#FFF176", display:"flex", flexDirection:"column"}}
      onPointerDown={onDown} onClick={onClick}>
      {selHandles}
      {resizeHandle}
      <textarea value={node.text} onChange={e=>onUpdate({text:e.target.value})}
        onPointerDown={e=>e.stopPropagation()} onClick={e=>{e.stopPropagation();onSelect();}}
        placeholder="Type something…"
        style={{flex:1,background:"transparent",border:"none",outline:"none",padding:"14px 16px 10px",fontSize:13,lineHeight:1.65,color:"#333",resize:"none",fontFamily:"inherit",minHeight:100,borderRadius:16}} />
      {selected && <div style={{padding:"4px 12px 10px",display:"flex",gap:6}}>
        {noteColors.map(c=><button key={c} onPointerDown={e=>e.stopPropagation()} onClick={()=>onUpdate({color:c})}
          style={{width:20,height:20,borderRadius:"50%",background:c,border:node.color===c?"2.5px solid #1768FF":"1.5px solid rgba(0,0,0,.18)",cursor:"pointer"}} />)}
      </div>}
    </div>
  );

  if (node.type === "vault") { const {item} = node;
    const isSong = item.type === "song" && item.url;
    const [audioPlaying, setAudioPlaying] = useState(false);
    const boardAudioRef = useRef(null);
    const toggleAudio = (e) => {
      e.stopPropagation();
      const audio = boardAudioRef.current;
      if (!audio) return;
      if (audioPlaying) { audio.pause(); setAudioPlaying(false); }
      else { audio.play().then(() => setAudioPlaying(true)).catch(() => {}); }
    };
    return (
    <div data-node-id={node.id} style={{...baseStyle, width:node.w||240, background:"#fff", overflow:"hidden"}}
      onPointerDown={onDown} onClick={onClick}>
      {selHandles}
      {resizeHandle}
      {isSong && <audio ref={boardAudioRef} src={item.url} onEnded={()=>setAudioPlaying(false)} style={{display:"none"}} />}
      {item.type==="image" && item.url && <img src={item.url} alt={item.title} style={{width:"100%",height:"auto",maxHeight:520,objectFit:"cover",display:"block",pointerEvents:"none"}} />}
      {item.type==="video" && item.url && <video src={item.url} controls playsInline onPointerDown={e=>e.stopPropagation()} style={{width:"100%",height:"auto",maxHeight:520,display:"block",background:"#000"}} />}
      <div style={{padding:"10px 12px 12px"}}>
        <div style={{display:"flex",alignItems:"center",gap:10}}>
          {isSong && (
            <button onPointerDown={e=>e.stopPropagation()} onClick={toggleAudio}
              style={{width:34,height:34,borderRadius:"50%",background:"#1768FF",border:"none",cursor:"pointer",display:"flex",alignItems:"center",justifyContent:"center",flexShrink:0,boxShadow:"0 2px 10px rgba(23,104,255,.45)"}}>
              {audioPlaying
                ? <span style={{width:10,height:10,display:"flex",gap:2.5}}><span style={{width:3,height:10,background:"#fff",borderRadius:1}}/><span style={{width:3,height:10,background:"#fff",borderRadius:1}}/></span>
                : <span style={{width:0,height:0,borderTop:"6px solid transparent",borderBottom:"6px solid transparent",borderLeft:"10px solid #fff",marginLeft:2}}/>}
            </button>
          )}
          <div style={{minWidth:0,flex:1}}>
            <p style={{fontSize:12,fontWeight:700,color:"#111",margin:0,overflow:"hidden",textOverflow:"ellipsis",whiteSpace:"nowrap"}}>{item.title}</p>
            {item.note && <p style={{fontSize:10,color:"#666",margin:"3px 0 0",overflow:"hidden",textOverflow:"ellipsis",whiteSpace:"nowrap"}}>{item.note.slice(0,60)}</p>}
          </div>
        </div>
      </div>
    </div>
  );}
  return null;
}

// Selection + move/resize overlay for vector nodes (drawings & lines)
function VecOverlay({ node, scale, bbox, onTranslate, onScale, onDelete }) {
  const bb = bbox(node);
  const pad = 14; // grab padding so thin lines are easy to hit
  const x = bb.x - pad, y = bb.y - pad, w = bb.w + pad * 2, h = bb.h + pad * 2;
  const lastRef = useRef(null);
  const moveDown = (e) => {
    e.stopPropagation();
    try { e.currentTarget.setPointerCapture(e.pointerId); } catch {}
    lastRef.current = { x: e.clientX, y: e.clientY, mode: "move" };
  };
  const sizeDown = (e) => {
    e.stopPropagation();
    try { e.currentTarget.setPointerCapture(e.pointerId); } catch {}
    lastRef.current = { x: e.clientX, y: e.clientY, mode: "scale" };
  };
  const onMove = (e) => {
    const L = lastRef.current; if (!L) return;
    e.stopPropagation();
    const dx = (e.clientX - L.x) / scale, dy = (e.clientY - L.y) / scale;
    if (L.mode === "move") onTranslate(node.id, dx, dy);
    else {
      const diag = Math.hypot(bb.w, bb.h) || 1;
      const f = Math.max(0.2, 1 + (dx + dy) / diag);
      onScale(node.id, bb.x, bb.y, f);
    }
    lastRef.current = { ...L, x: e.clientX, y: e.clientY };
  };
  const onUp = (e) => { e.stopPropagation(); lastRef.current = null; };
  return (
    <div style={{position:"absolute",left:x,top:y,width:w,height:h,zIndex:14,touchAction:"none",cursor:"move"}}
      onPointerDown={moveDown} onPointerMove={onMove} onPointerUp={onUp} onPointerCancel={onUp}>
      <div style={{position:"absolute",inset:0,border:"2px dashed #1768FF",borderRadius:6,background:"rgba(23,104,255,.05)"}}/>
      {[{top:-6,left:-6},{top:-6,right:-6},{bottom:-6,left:-6}].map((p,i)=>(
        <div key={i} style={{position:"absolute",...p,width:11,height:11,borderRadius:"50%",background:"#1768FF",border:"2.5px solid #fff",boxShadow:"0 1px 5px rgba(23,104,255,.45)"}}/>
      ))}
      {/* bottom-right = resize handle */}
      <div onPointerDown={sizeDown} onPointerMove={onMove} onPointerUp={onUp} onPointerCancel={onUp}
        style={{position:"absolute",bottom:-13,right:-13,width:26,height:26,borderRadius:"50%",background:"#1768FF",border:"2.5px solid #fff",boxShadow:"0 2px 6px rgba(23,104,255,.5)",cursor:"nwse-resize",display:"flex",alignItems:"center",justifyContent:"center",touchAction:"none"}}>
        <svg width="11" height="11" viewBox="0 0 24 24" fill="none" stroke="#fff" strokeWidth="3" strokeLinecap="round"><path d="M9 21H3v-6M21 3l-7.5 7.5M3 21l7.5-7.5M15 3h6v6"/></svg>
      </div>
      <button onPointerDown={e=>e.stopPropagation()} onClick={(e)=>{e.stopPropagation();onDelete(node.id);}}
        style={{position:"absolute",top:-15,right:-15,width:26,height:26,background:"#FF3B30",color:"#fff",border:"2.5px solid #fff",borderRadius:"50%",cursor:"pointer",fontSize:15,display:"flex",alignItems:"center",justifyContent:"center",boxShadow:"0 2px 8px rgba(0,0,0,.3)"}}>×</button>
    </div>
  );
}

const BOARD_ICONS = ["✦","🎬","🎵","🍳","🎨","📷","✈️","💡","📚","🏋️","🌿","🎮","💻","🛋️","👗","🎤"];

function CreativeSessions({ t, theme, sessions, onOpen, onCreate, onDelete, onRename, onSetCover }) {
  const isDark = ["dark","franki","grape"].includes(theme);
  const [renamingId, setRenamingId] = useState(null);
  const [renameVal, setRenameVal] = useState("");
  const [iconPickerId, setIconPickerId] = useState(null);

  const startRename = (s, e) => { e.stopPropagation(); setRenamingId(s.id); setRenameVal(s.name); };
  const commitRename = (id) => { if (renameVal.trim()) onRename(id, renameVal.trim()); setRenamingId(null); };
  const pickCoverPhoto = async (id, file) => {
    if (!file) return;
    const url = await blobToDataUrl(file);
    onSetCover(id, { cover: url, icon: null });
    setIconPickerId(null);
  };

  return (
    <div style={{paddingTop:4}}>
      <div style={{display:"grid",gridTemplateColumns:"1fr 1fr",gap:12}}>
        {sessions.map(s => (
          <div key={s.id}>
            <div onClick={()=>{ if(renamingId!==s.id) onOpen(s); }}
              style={{borderRadius:20,overflow:"hidden",background:t.panel,border:`1px solid ${t.border}`,
                boxShadow:t.shadow,cursor:"pointer",display:"flex",flexDirection:"column",aspectRatio:"1"}}>
              {/* Cover */}
              <div style={{flex:1,position:"relative",background:s.cover?"transparent":isDark?"rgba(255,255,255,.04)":"rgba(0,0,0,.04)",
                display:"flex",alignItems:"center",justifyContent:"center",overflow:"hidden"}}>
                {s.cover
                  ? <img src={s.cover} alt="" style={{position:"absolute",inset:0,width:"100%",height:"100%",objectFit:"cover"}}/>
                  : <span style={{fontSize:32,pointerEvents:"none"}}>{s.icon||"✦"}</span>}
                {/* Action buttons */}
                <div style={{position:"absolute",top:6,right:6,display:"flex",gap:4}} onClick={e=>e.stopPropagation()}>
                  <button onClick={e=>startRename(s,e)}
                    style={{width:26,height:26,borderRadius:8,border:"none",cursor:"pointer",
                      background:"rgba(0,0,0,.45)",backdropFilter:"blur(8px)",WebkitBackdropFilter:"blur(8px)",
                      display:"flex",alignItems:"center",justifyContent:"center",color:"#fff"}}>
                    <Pencil style={{width:12,height:12}}/>
                  </button>
                  <button onClick={e=>{e.stopPropagation();onDelete(s.id);}}
                    style={{width:26,height:26,borderRadius:8,border:"none",cursor:"pointer",
                      background:"rgba(0,0,0,.45)",backdropFilter:"blur(8px)",WebkitBackdropFilter:"blur(8px)",
                      display:"flex",alignItems:"center",justifyContent:"center",color:"#FF6B6B"}}>
                    <X style={{width:12,height:12}}/>
                  </button>
                </div>
                {/* Cover/icon picker trigger */}
                <button onClick={e=>{e.stopPropagation();setIconPickerId(iconPickerId===s.id?null:s.id);}}
                  style={{position:"absolute",bottom:6,left:6,width:26,height:26,borderRadius:8,border:"none",cursor:"pointer",
                    background:"rgba(0,0,0,.45)",backdropFilter:"blur(8px)",WebkitBackdropFilter:"blur(8px)",
                    display:"flex",alignItems:"center",justifyContent:"center",color:"#fff"}}>
                  <Image style={{width:12,height:12}}/>
                </button>
              </div>
              {/* Info */}
              <div style={{padding:"8px 10px 10px",background:t.panel}}>
                {renamingId===s.id ? (
                  <input value={renameVal} onChange={e=>setRenameVal(e.target.value)}
                    onBlur={()=>commitRename(s.id)}
                    onKeyDown={e=>{if(e.key==="Enter")commitRename(s.id);if(e.key==="Escape")setRenamingId(null);}}
                    autoFocus onClick={e=>e.stopPropagation()}
                    style={{width:"100%",background:"transparent",border:"none",outline:`1px solid ${t.border}`,
                      borderRadius:6,fontSize:12,fontWeight:700,color:t.text,padding:"1px 4px"}}/>
                ) : (
                  <p style={{fontSize:12,fontWeight:700,color:t.text,margin:"0 0 1px",overflow:"hidden",textOverflow:"ellipsis",whiteSpace:"nowrap"}}>{s.name}</p>
                )}
                <p style={{fontSize:10,color:t.muted,margin:0}}>{s.nodes.length} item{s.nodes.length===1?"":"s"}</p>
              </div>
            </div>
            {/* Icon/cover picker */}
            {iconPickerId===s.id && (
              <div style={{marginTop:6,background:t.panel,border:`1px solid ${t.border}`,borderRadius:14,padding:12,boxShadow:t.shadow}}>
                <div style={{display:"grid",gridTemplateColumns:"repeat(8,1fr)",gap:6,marginBottom:10}}>
                  {BOARD_ICONS.map(ic=>(
                    <button key={ic} onClick={()=>{onSetCover(s.id,{icon:ic,cover:null});setIconPickerId(null);}}
                      style={{aspectRatio:"1",borderRadius:9,border:`1px solid ${(s.icon===ic&&!s.cover)?t.accent:t.border}`,
                        background:(s.icon===ic&&!s.cover)?t.input:"transparent",cursor:"pointer",fontSize:18,
                        display:"flex",alignItems:"center",justifyContent:"center",padding:0}}>{ic}</button>
                  ))}
                </div>
                <label style={{display:"flex",alignItems:"center",justifyContent:"center",gap:7,width:"100%",padding:"9px 0",
                  borderRadius:10,border:`1.5px dashed ${t.border}`,cursor:"pointer",fontSize:12,fontWeight:600,color:t.muted}}>
                  <Image style={{width:15,height:15}}/> Upload cover photo
                  <input type="file" accept="image/*" className="hidden"
                    onChange={e=>{const f=e.target.files?.[0];if(f)pickCoverPhoto(s.id,f);e.target.value="";}}/>
                </label>
              </div>
            )}
          </div>
        ))}
        {/* New board button */}
        <button onClick={onCreate}
          style={{borderRadius:20,border:`1.5px dashed ${t.border}`,background:"transparent",
            aspectRatio:"1",display:"flex",flexDirection:"column",alignItems:"center",justifyContent:"center",
            gap:10,cursor:"pointer"}}>
          <div style={{width:40,height:40,borderRadius:13,background:t.input,display:"flex",alignItems:"center",justifyContent:"center"}}>
            <Plus style={{width:20,height:20,color:t.muted}}/>
          </div>
          <p style={{fontSize:12,fontWeight:600,color:t.muted,margin:0}}>New Board</p>
        </button>
      </div>
    </div>
  );
}

function BoardView({ session, t, theme, vaultItems, onSave, onClose }) {
  const [nodes, setNodes] = useState(session.nodes || []);
  const [offset, setOffset] = useState({x:0,y:0});
  const [scale, setScale] = useState(1);
  const [selectedId, setSelectedId] = useState(null);
  const [dragging, setDragging] = useState(null);
  const [panning, setPanning] = useState(null);
  const [showVaultPicker, setShowVaultPicker] = useState(false);
  const [tool, setTool] = useState("select");
  const [drawColor, setDrawColor] = useState("#1768FF");
  const [drawWidth, setDrawWidth] = useState(3);
  const [bgMode, setBgMode] = useState(() => loadFromStorage("board_bg_" + session.id, "light"));
  const boardDark = bgMode === "dark";
  useEffect(() => {
    saveToStorage("board_bg_" + session.id, bgMode);
    // Flip text node colors when background switches dark ↔ light
    setNodes(ns => ns.map(nd => {
      if (nd.type !== "text") return nd;
      const dark = bgMode === "dark";
      const wasDefault = nd.color === (dark ? "#111111" : "#ffffff");
      if (wasDefault) return { ...nd, color: dark ? "#ffffff" : "#111111" };
      return nd;
    }));
  }, [bgMode]);
  const [showColorPicker, setShowColorPicker] = useState(false);
  const [currentDraw, setCurrentDraw] = useState(null);
  const [lineStart, setLineStart] = useState(null);
  const [linePreview, setLinePreview] = useState(null);
  const [penType, setPenType] = useState("pen"); // "pen" | "marker" | "pencil" | "line"
  const containerRef = useRef(null);
  // Per-pointer tracking for proper pinch-to-zoom
  const pointersRef = useRef(new Map()); // pointerId → {x,y}
  const lastPinchRef = useRef(null);     // {dist, cx, cy} of the previous pinch frame
  const erasingRef = useRef(false);
  // Mutable copies kept in sync so pointer handlers never read stale closure values
  const panningRef = useRef(null);
  const draggingRef = useRef(null);
  const dragPositionRef = useRef(null); // live position during drag — avoids setNodes every frame
  const lineStartRef = useRef(null);
  const toolRef = useRef("select");
  const drawColorRef = useRef("#1768FF");
  const drawWidthRef = useRef(3);
  const penTypeRef = useRef("pen");

  useEffect(() => { panningRef.current = panning; }, [panning]);
  useEffect(() => { draggingRef.current = dragging; }, [dragging]);
  useEffect(() => { lineStartRef.current = lineStart; }, [lineStart]);
  useEffect(() => { toolRef.current = tool; }, [tool]);
  useEffect(() => { drawColorRef.current = drawColor; }, [drawColor]);
  useEffect(() => { drawWidthRef.current = drawWidth; }, [drawWidth]);
  useEffect(() => { penTypeRef.current = penType; }, [penType]);
  // Debounce saves — don't write to storage on every pointer-move frame
  const saveTimerRef = useRef(null);
  useEffect(() => {
    if (saveTimerRef.current) clearTimeout(saveTimerRef.current);
    saveTimerRef.current = setTimeout(() => onSave(nodes), 600);
    return () => clearTimeout(saveTimerRef.current);
  }, [nodes]);

  // ── Undo / redo history ───────────────────────────────────────────────
  const historyRef = useRef([session.nodes || []]);
  const histIdxRef = useRef(0);
  const skipHistRef = useRef(false);
  const histTimerRef = useRef(null);
  const [histVer, setHistVer] = useState(0);
  useEffect(() => {
    if (skipHistRef.current) { skipHistRef.current = false; return; }
    clearTimeout(histTimerRef.current);
    histTimerRef.current = setTimeout(() => {
      const h = historyRef.current.slice(0, histIdxRef.current + 1);
      h.push(nodes);
      while (h.length > 60) h.shift();
      historyRef.current = h;
      histIdxRef.current = h.length - 1;
      setHistVer(v => v + 1);
    }, 350);
    return () => clearTimeout(histTimerRef.current);
  }, [nodes]);
  const undo = () => {
    if (histIdxRef.current <= 0) return;
    histIdxRef.current--; skipHistRef.current = true;
    setNodes(historyRef.current[histIdxRef.current]); setSelectedId(null); setHistVer(v => v + 1);
  };
  const redo = () => {
    if (histIdxRef.current >= historyRef.current.length - 1) return;
    histIdxRef.current++; skipHistRef.current = true;
    setNodes(historyRef.current[histIdxRef.current]); setSelectedId(null); setHistVer(v => v + 1);
  };
  const canUndo = histIdxRef.current > 0;
  const canRedo = histIdxRef.current < historyRef.current.length - 1;

  // ── Recenter / fit view ───────────────────────────────────────────────
  const recenter = () => {
    const r = containerRef.current?.getBoundingClientRect() || { width: 375, height: 700 };
    if (nodes.length === 0) { setOffset({ x: 0, y: 0 }); setScale(1); return; }
    let minX = Infinity, minY = Infinity, maxX = -Infinity, maxY = -Infinity;
    nodes.forEach(n => {
      if (n.type === "drawing") n.points.forEach(p => { minX = Math.min(minX, p.x); minY = Math.min(minY, p.y); maxX = Math.max(maxX, p.x); maxY = Math.max(maxY, p.y); });
      else if (n.type === "line") [[n.x1, n.y1], [n.x2, n.y2]].forEach(([x, y]) => { minX = Math.min(minX, x); minY = Math.min(minY, y); maxX = Math.max(maxX, x); maxY = Math.max(maxY, y); });
      else { const w = n.w || 220, h = n.h || 160; minX = Math.min(minX, n.x); minY = Math.min(minY, n.y); maxX = Math.max(maxX, n.x + w); maxY = Math.max(maxY, n.y + h); }
    });
    const bw = maxX - minX || 1, bh = maxY - minY || 1, pad = 70;
    const s = Math.min(1.6, Math.max(0.2, Math.min((r.width - pad * 2) / bw, (r.height - pad * 2) / bh)));
    setScale(s);
    setOffset({ x: r.width / 2 - (minX + bw / 2) * s, y: r.height / 2 - (minY + bh / 2) * s });
  };

  const toCanvas = (sx, sy) => {
    const r = containerRef.current?.getBoundingClientRect() || {left:0,top:0};
    return { x:(sx-r.left-offset.x)/scale, y:(sy-r.top-offset.y)/scale };
  };
  const center = () => {
    const r = containerRef.current?.getBoundingClientRect()||{width:375,height:700};
    return {x:(r.width/2-offset.x)/scale, y:(r.height/2-offset.y)/scale};
  };
  const getPinchInfo = () => {
    const pts = [...pointersRef.current.values()];
    if (pts.length < 2) return null;
    const [a,b] = pts;
    return {dist:Math.hypot(b.x-a.x,b.y-a.y), cx:(a.x+b.x)/2, cy:(a.y+b.y)/2};
  };

  const addNote = () => { const c=center(); setNodes(n=>[...n,{id:Date.now(),type:"note",x:c.x-100,y:c.y-65,w:200,text:"",color:"#FFF176"}]); setShowVaultPicker(false); setTool("select"); toolRef.current="select"; };
  const addText = (x,y) => setNodes(n=>[...n,{id:Date.now(),type:"text",x,y,text:"",fontSize:16,color:boardDark?"#ffffff":"#111111",bold:false,italic:false,w:200}]);
  const addTextCenter = () => {
    const c = center();
    const id = Date.now();
    setNodes(n=>[...n,{id,type:"text",x:c.x-100,y:c.y-20,text:"",fontSize:20,color:boardDark?"#ffffff":"#111111",bold:false,italic:false,w:200}]);
    setSelectedId(id);
    setTool("select"); toolRef.current="select";
    setShowVaultPicker(false); setShowColorPicker(false);
  };
  const addVault = (item) => { const c=center(); setNodes(n=>[...n,{id:Date.now(),type:"vault",x:c.x-120,y:c.y-70,w:240,item}]); setShowVaultPicker(false); setTool("select"); toolRef.current="select"; };
  const addMediaFile = async (file) => {
    if (!file) return;
    try {
      const url = await blobToDataUrl(file);
      const kind = file.type.startsWith("video") ? "video" : "image";
      addVault({ id: Date.now(), type: kind, url, title: file.name || kind });
    } catch {}
  };
  const deleteSelected = () => { if(!selectedId) return; setNodes(n=>n.filter(nd=>nd.id!==selectedId)); setSelectedId(null); };

  // ── Vector (drawing/line) selection + transform ───────────────────────
  const vecBBox = (nd) => {
    if (nd.type === "drawing") {
      let a = Infinity, b = Infinity, c = -Infinity, d = -Infinity;
      nd.points.forEach(p => { a = Math.min(a, p.x); b = Math.min(b, p.y); c = Math.max(c, p.x); d = Math.max(d, p.y); });
      return { x: a, y: b, w: c - a, h: d - b };
    }
    return { x: Math.min(nd.x1, nd.x2), y: Math.min(nd.y1, nd.y2), w: Math.abs(nd.x2 - nd.x1), h: Math.abs(nd.y2 - nd.y1) };
  };
  const hitVector = (x, y) => {
    const r = 12 / scale;
    for (let i = nodes.length - 1; i >= 0; i--) {
      const nd = nodes[i];
      if (nd.type === "drawing") {
        if (nd.points.some(p => Math.hypot(p.x - x, p.y - y) < r)) return nd.id;
      } else if (nd.type === "line") {
        const dx = nd.x2 - nd.x1, dy = nd.y2 - nd.y1, len2 = dx * dx + dy * dy;
        const tt = len2 === 0 ? 0 : Math.max(0, Math.min(1, ((x - nd.x1) * dx + (y - nd.y1) * dy) / len2));
        if (Math.hypot(nd.x1 + tt * dx - x, nd.y1 + tt * dy - y) < r) return nd.id;
      }
    }
    return null;
  };
  const translateVec = (id, dx, dy) => setNodes(ns => ns.map(nd => {
    if (nd.id !== id) return nd;
    if (nd.type === "drawing") return { ...nd, points: nd.points.map(p => ({ x: p.x + dx, y: p.y + dy })) };
    if (nd.type === "line") return { ...nd, x1: nd.x1 + dx, y1: nd.y1 + dy, x2: nd.x2 + dx, y2: nd.y2 + dy };
    return nd;
  }));
  const scaleVec = (id, ox, oy, f) => setNodes(ns => ns.map(nd => {
    if (nd.id !== id) return nd;
    if (nd.type === "drawing") return { ...nd, points: nd.points.map(p => ({ x: ox + (p.x - ox) * f, y: oy + (p.y - oy) * f })) };
    if (nd.type === "line") return { ...nd, x1: ox + (nd.x1 - ox) * f, y1: oy + (nd.y1 - oy) * f, x2: ox + (nd.x2 - ox) * f, y2: oy + (nd.y2 - oy) * f };
    return nd;
  }));

  const eraseAt = (x,y) => {
    const r = 20/scale;
    setNodes(ns=>ns.filter(nd=>{
      if(nd.type==="drawing") return !nd.points.some(p=>Math.hypot(p.x-x,p.y-y)<r);
      if(nd.type==="line"){
        const dx=nd.x2-nd.x1, dy=nd.y2-nd.y1, len2=dx*dx+dy*dy;
        if(len2===0) return Math.hypot(nd.x1-x,nd.y1-y)>=r;
        const tt=Math.max(0,Math.min(1,((x-nd.x1)*dx+(y-nd.y1)*dy)/len2));
        return Math.hypot(nd.x1+tt*dx-x,nd.y1+tt*dy-y)>=r;
      }
      return true;
    }));
  };

  // Single unified pointer-down: tracks all fingers, handles tools on background tap
  const onPtrDown = (e) => {
    // Reject pointer-type mixing: prevents phantom mouse events during touch sessions
    if (pointersRef.current.size > 0) {
      const existingType = pointersRef.current.values().next().value?.type;
      if (existingType && existingType !== e.pointerType) return;
    }
    try { e.currentTarget.setPointerCapture(e.pointerId); } catch {}
    pointersRef.current.set(e.pointerId, {x:e.clientX, y:e.clientY, type:e.pointerType});

    if (pointersRef.current.size >= 2) {
      // Second finger down → cancel single-finger actions and start pinch
      setPanning(null); panningRef.current = null;
      setDragging(null); draggingRef.current = null;
      setCurrentDraw(null);
      setLineStart(null); lineStartRef.current = null;
      setLinePreview(null);
      erasingRef.current = false;
      lastPinchRef.current = getPinchInfo();
      return;
    }

    // Single finger — only trigger tool actions when tapping the background itself
    if (e.target !== e.currentTarget) return;

    setShowColorPicker(false);
    const {x,y} = toCanvas(e.clientX, e.clientY);
    const t_ = toolRef.current;
    if (t_==="select") {
      setShowVaultPicker(false);
      const hit = hitVector(x, y);
      if (hit) { setSelectedId(hit); return; }
      setSelectedId(null);
      const p = {sx:e.clientX,sy:e.clientY,ox:offset.x,oy:offset.y};
      setPanning(p); panningRef.current = p;
    } else if (t_==="text") {
      addText(x,y); setTool("select"); toolRef.current="select";
    } else if (t_==="draw") {
      const pt = penTypeRef.current;
      if (pt === "line") {
        const ls = {x,y};
        setLineStart(ls); lineStartRef.current = ls; setLinePreview({x,y});
      } else {
        setCurrentDraw({id:Date.now(),type:"drawing",points:[{x,y}],color:drawColorRef.current,width:drawWidthRef.current,penType:penTypeRef.current});
      }
    } else if (t_==="erase") {
      erasingRef.current = true; eraseAt(x,y);
    }
  };

  const onPtrMove = (e) => {
    if (!pointersRef.current.has(e.pointerId)) return;
    pointersRef.current.set(e.pointerId, {x:e.clientX, y:e.clientY, type:e.pointerType});

    if (pointersRef.current.size >= 2) {
      // Pinch-to-zoom centered at pinch midpoint
      const curr = getPinchInfo();
      if (!curr) return;
      if (lastPinchRef.current) {
        const ratio = curr.dist / lastPinchRef.current.dist;
        const r = containerRef.current?.getBoundingClientRect() || {left:0,top:0};
        const mx = curr.cx - r.left, my = curr.cy - r.top;
        setScale(s => {
          const ns = Math.max(0.15, Math.min(5, s * ratio));
          setOffset(o => ({x: mx-(mx-o.x)*ns/s, y: my-(my-o.y)*ns/s}));
          return ns;
        });
      }
      lastPinchRef.current = curr;
      return;
    }

    // Single finger
    lastPinchRef.current = null;
    const cx = e.clientX, cy = e.clientY;
    const {x,y} = toCanvas(cx, cy);
    const pan = panningRef.current;
    const drag = draggingRef.current;
    if (pan) setOffset({x:pan.ox+cx-pan.sx, y:pan.oy+cy-pan.sy});
    if (drag) {
      // Move node via direct DOM mutation — avoids React re-rendering all nodes every frame
      const newX = drag.nx + (cx - drag.mx) / scale;
      const newY = drag.ny + (cy - drag.my) / scale;
      dragPositionRef.current = {id: drag.id, x: newX, y: newY};
      const el = document.querySelector(`[data-node-id="${drag.id}"]`);
      if (el) { el.style.left = newX + "px"; el.style.top = newY + "px"; }
    }
    if (toolRef.current==="draw") {
      if (penTypeRef.current === "line") {
        if (lineStartRef.current) setLinePreview({x,y});
      } else {
        setCurrentDraw(d=>d?{...d,points:[...d.points,{x,y}]}:d);
      }
    }
    if (toolRef.current==="erase"&&erasingRef.current) eraseAt(x,y);
  };

  const onPtrUp = (e) => {
    if (!pointersRef.current.has(e.pointerId)) return;
    pointersRef.current.delete(e.pointerId);
    if (pointersRef.current.size < 2) lastPinchRef.current = null;
    if (pointersRef.current.size > 0) return; // still fingers on screen

    // All pointers lifted — finalise any in-progress action
    setCurrentDraw(d => { if(d && d.points.length>1) setNodes(n=>[...n,d]); return null; });
    if (lineStartRef.current && linePreview) {
      const ls = lineStartRef.current;
      setNodes(n=>[...n,{id:Date.now(),type:"line",x1:ls.x,y1:ls.y,x2:linePreview.x,y2:linePreview.y,color:drawColorRef.current,width:drawWidthRef.current,penType:penTypeRef.current}]);
    }
    setLineStart(null); lineStartRef.current=null; setLinePreview(null);
    erasingRef.current=false;
    setPanning(null); panningRef.current=null;
    // Commit drag position to state now that the drag is done
    if (dragPositionRef.current) {
      const {id, x, y} = dragPositionRef.current;
      setNodes(n => n.map(nd => nd.id === id ? {...nd, x, y} : nd));
      dragPositionRef.current = null;
    }
    setDragging(null); draggingRef.current=null;
  };

  // Mouse wheel zoom — centered at cursor position
  useEffect(()=>{
    const el=containerRef.current; if(!el) return;
    const fn=(e)=>{
      e.preventDefault();
      const r = el.getBoundingClientRect();
      const mx = e.clientX - r.left, my = e.clientY - r.top;
      setScale(s=>{
        const ns=Math.max(0.15,Math.min(5,s*(e.deltaY<0?1.12:0.89)));
        setOffset(o=>({x:mx-(mx-o.x)*ns/s, y:my-(my-o.y)*ns/s}));
        return ns;
      });
    };
    el.addEventListener("wheel",fn,{passive:false});
    return ()=>el.removeEventListener("wheel",fn);
  },[]);

  const bg = boardDark ? "#0d0f1e" : "#ffffff";
  const dot = boardDark?"rgba(255,255,255,.06)":"rgba(0,0,0,.07)";
  const sp = 24*scale;
  const tbBg = boardDark?"rgba(7,8,18,.96)":"rgba(248,248,246,.96)";
  const tbBorder = boardDark?"rgba(255,255,255,.09)":"rgba(0,0,0,.1)";
  const tbText = boardDark?"#fff":"#111";
  const tbMuted = boardDark?"rgba(255,255,255,.38)":"rgba(0,0,0,.38)";
  const tbGroupBg = boardDark?"rgba(255,255,255,.07)":"rgba(0,0,0,.06)";
  const tbActive = boardDark?"rgba(255,255,255,.2)":"rgba(0,0,0,.14)";
  const drawNodes = nodes.filter(n=>n.type==="drawing"||n.type==="line");
  const htmlNodes = nodes.filter(n=>n.type!=="drawing"&&n.type!=="line");
  const cursor = tool==="draw"?"crosshair":tool==="erase"?"cell":tool==="text"?"text":panning?"grabbing":"default";

  return (
    <>
      <div ref={containerRef}
        style={{position:"fixed",inset:0,zIndex:40,background:bg,touchAction:"none",cursor,overflow:"hidden"}}
        onPointerDown={onPtrDown} onPointerMove={onPtrMove} onPointerUp={onPtrUp} onPointerCancel={onPtrUp}>

        <svg style={{position:"absolute",inset:0,pointerEvents:"none",width:"100%",height:"100%"}}>
          <defs>
            <pattern id="bdots" x={(offset.x%sp+sp)%sp} y={(offset.y%sp+sp)%sp} width={sp} height={sp} patternUnits="userSpaceOnUse">
              <circle cx={sp/2} cy={sp/2} r={Math.max(0.8,scale)} fill={dot}/>
            </pattern>
          </defs>
          <rect width="100%" height="100%" fill="url(#bdots)"/>
        </svg>

        <div style={{position:"absolute",top:"max(16px, calc(env(safe-area-inset-top) + 8px))",left:"50%",transform:"translateX(-50%)",fontSize:11,fontWeight:700,color:boardDark?"rgba(255,255,255,.35)":"rgba(0,0,0,.32)",letterSpacing:"0.12em",textTransform:"uppercase",pointerEvents:"none",whiteSpace:"nowrap"}}>{session.name}</div>
        {nodes.length === 0 && (
          <div style={{position:"absolute",top:"50%",left:"50%",transform:"translate(-50%,-50%)",textAlign:"center",pointerEvents:"none",userSelect:"none"}}>
            <div style={{fontSize:36,opacity:boardDark?.2:.12,marginBottom:12,color:boardDark?"#fff":"#000"}}>✦</div>
            <p style={{fontSize:13,color:boardDark?"rgba(255,255,255,.3)":"rgba(0,0,0,.22)",fontWeight:600,margin:"0 0 4px"}}>Empty canvas</p>
            <p style={{fontSize:11,color:boardDark?"rgba(255,255,255,.18)":"rgba(0,0,0,.15)",margin:0}}>Use the toolbar below to add notes, text, or drawings</p>
          </div>
        )}

        <div style={{position:"absolute",inset:0,overflow:"visible",pointerEvents:"none"}}>
          <div style={{position:"absolute",left:0,top:0,transform:`translate(${offset.x}px,${offset.y}px) scale(${scale})`,transformOrigin:"0 0",pointerEvents:"none",willChange:"transform"}}>
            <svg style={{position:"absolute",left:0,top:0,overflow:"visible",width:1,height:1,pointerEvents:"none"}}>
              {drawNodes.map(nd=>{
                const pt = nd.penType || "pen";
                const isMarker = pt === "marker";
                const isPencil = pt === "pencil";
                const opacity = isMarker ? 0.58 : isPencil ? 0.72 : 1;
                const sw = isMarker ? nd.width * 2.2 : isPencil ? nd.width * 0.75 : nd.width;
                const cap = isMarker ? "square" : "round";
                const dash = isPencil ? "1 0.4" : "none";
                return nd.type==="drawing"
                  ? <polyline key={nd.id} points={nd.points.map(p=>`${p.x},${p.y}`).join(" ")} stroke={nd.color} strokeWidth={sw} fill="none" strokeLinecap={cap} strokeLinejoin={isMarker?"miter":"round"} opacity={opacity} strokeDasharray={dash}/>
                  : <line key={nd.id} x1={nd.x1} y1={nd.y1} x2={nd.x2} y2={nd.y2} stroke={nd.color} strokeWidth={sw} strokeLinecap={cap} opacity={opacity}/>;
              })}
              {currentDraw&&(()=>{
                const pt = currentDraw.penType || "pen";
                const isMarker = pt === "marker";
                const isPencil = pt === "pencil";
                const opacity = isMarker ? 0.58 : isPencil ? 0.72 : 1;
                const sw = isMarker ? currentDraw.width * 2.2 : isPencil ? currentDraw.width * 0.75 : currentDraw.width;
                const cap = isMarker ? "square" : "round";
                const dash = isPencil ? "1 0.4" : "none";
                return <polyline points={currentDraw.points.map(p=>`${p.x},${p.y}`).join(" ")} stroke={currentDraw.color} strokeWidth={sw} fill="none" strokeLinecap={cap} strokeLinejoin={isMarker?"miter":"round"} opacity={opacity} strokeDasharray={dash}/>;
              })()}
              {lineStart&&linePreview&&<line x1={lineStart.x} y1={lineStart.y} x2={linePreview.x} y2={linePreview.y} stroke={drawColor} strokeWidth={drawWidth} strokeLinecap="round" strokeDasharray="6,4" opacity={0.7}/>}
            </svg>
            <div style={{pointerEvents:"all"}}>
              {htmlNodes.map(node=>(
                <CanvasNode key={node.id} node={node} selected={selectedId===node.id} t={t} theme={theme} boardDark={boardDark} tool={tool}
                  onSelect={()=>{ if(tool==="select") setSelectedId(node.id); }}
                  onDragStart={(e)=>{
                    if(tool!=="select") return;
                    setSelectedId(node.id);
                    const drag={id:node.id,mx:e.clientX,my:e.clientY,nx:node.x,ny:node.y};
                    setDragging(drag); draggingRef.current=drag;
                  }}
                  onUpdate={(p)=>setNodes(n=>n.map(nd=>nd.id===node.id?{...nd,...p}:nd))}
                  onResize={(w,h)=>setNodes(n=>n.map(nd=>nd.id===node.id?{...nd,w,...(h?{h}:{})}:nd))}
                  scale={scale}
                  onDelete={()=>{setNodes(n=>n.filter(nd=>nd.id!==node.id));setSelectedId(null);}} />
              ))}
              {(()=>{
                const v = nodes.find(n=>n.id===selectedId && (n.type==="drawing"||n.type==="line"));
                return v ? <VecOverlay node={v} scale={scale} bbox={vecBBox}
                  onTranslate={translateVec} onScale={scaleVec}
                  onDelete={(id)=>{setNodes(n=>n.filter(nd=>nd.id!==id));setSelectedId(null);}} /> : null;
              })()}
            </div>
          </div>
        </div>

        {showVaultPicker&&(
          <div style={{position:"absolute",bottom:"calc(88px + env(safe-area-inset-bottom))",left:"50%",transform:"translateX(-50%)",width:300,maxHeight:260,overflowY:"auto",background:t.panel,boxShadow:t.shadow,border:`1px solid ${t.border}`,borderRadius:20,padding:10,zIndex:60}}
            onPointerDown={e=>e.stopPropagation()}>
            <p style={{fontSize:10,fontWeight:700,letterSpacing:"0.15em",textTransform:"uppercase",color:t.muted,padding:"4px 8px 8px"}}>From vault</p>
            {vaultItems.length===0&&<p style={{fontSize:12,color:t.muted,textAlign:"center",padding:"12px 0"}}>No vault items yet</p>}
            {vaultItems.slice(0,16).map(item=>(
              <button key={item.id} onClick={()=>addVault(item)}
                style={{display:"flex",alignItems:"center",gap:10,width:"100%",padding:"8px 10px",background:"transparent",border:"none",borderRadius:12,cursor:"pointer",textAlign:"left",color:t.text}}>
                <div style={{width:26,height:26,background:t.input,borderRadius:8,display:"flex",alignItems:"center",justifyContent:"center",flexShrink:0}}>
                  {React.createElement(iconFor(item.type),{className:"h-3 w-3",style:{color:t.muted}})}
                </div>
                <span style={{fontSize:12,fontWeight:500,flex:1,overflow:"hidden",textOverflow:"ellipsis",whiteSpace:"nowrap"}}>{item.title}</span>
              </button>
            ))}
          </div>
        )}
        {showColorPicker&&<ColorPicker color={drawColor} onChange={setDrawColor} strokeWidth={drawWidth} onWidthChange={setDrawWidth} onClose={()=>setShowColorPicker(false)} boardDark={boardDark}/>}
      </div>

      {/* Freeform-style single-row contextual toolbar */}
      <div style={{position:"fixed",bottom:0,left:0,right:0,zIndex:50,background:tbBg,backdropFilter:"blur(28px)",WebkitBackdropFilter:"blur(28px)",borderTop:`1px solid ${tbBorder}`,paddingBottom:"env(safe-area-inset-bottom)"}}
        onPointerDown={e=>e.stopPropagation()}>

        {/* Utility strip: undo / redo · recenter · background */}
        <div style={{display:"flex",alignItems:"center",justifyContent:"space-between",padding:"7px 16px 0",gap:8}}>
          <div style={{display:"flex",gap:6}}>
            <button onClick={undo} disabled={!canUndo}
              style={{width:34,height:30,background:tbGroupBg,border:"none",borderRadius:9,cursor:canUndo?"pointer":"default",display:"flex",alignItems:"center",justifyContent:"center",color:canUndo?tbText:tbMuted,opacity:canUndo?1:0.4}}>
              <svg viewBox="0 0 24 24" width="16" height="16" fill="none" stroke="currentColor" strokeWidth="2.2" strokeLinecap="round" strokeLinejoin="round"><path d="M3 7v6h6"/><path d="M21 17a9 9 0 0 0-9-9 9 9 0 0 0-6 2.3L3 13"/></svg>
            </button>
            <button onClick={redo} disabled={!canRedo}
              style={{width:34,height:30,background:tbGroupBg,border:"none",borderRadius:9,cursor:canRedo?"pointer":"default",display:"flex",alignItems:"center",justifyContent:"center",color:canRedo?tbText:tbMuted,opacity:canRedo?1:0.4}}>
              <svg viewBox="0 0 24 24" width="16" height="16" fill="none" stroke="currentColor" strokeWidth="2.2" strokeLinecap="round" strokeLinejoin="round"><path d="M21 7v6h-6"/><path d="M3 17a9 9 0 0 1 9-9 9 9 0 0 1 6 2.3L21 13"/></svg>
            </button>
          </div>
          <div style={{display:"flex",gap:6}}>
            <button onClick={recenter} title="Recenter"
              style={{width:34,height:30,background:tbGroupBg,border:"none",borderRadius:9,cursor:"pointer",display:"flex",alignItems:"center",justifyContent:"center",color:tbText}}>
              <svg viewBox="0 0 24 24" width="16" height="16" fill="none" stroke="currentColor" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round"><circle cx="12" cy="12" r="3"/><path d="M12 2v3M12 19v3M2 12h3M19 12h3"/></svg>
            </button>
            <button onClick={()=>setBgMode(m=>m==="dark"?"light":"dark")} title="Background"
              style={{width:34,height:30,background:tbGroupBg,border:"none",borderRadius:9,cursor:"pointer",display:"flex",alignItems:"center",justifyContent:"center",color:tbText}}>
              {boardDark
                ? <svg viewBox="0 0 24 24" width="16" height="16" fill="none" stroke="currentColor" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round"><circle cx="12" cy="12" r="4.5"/><path d="M12 2v2M12 20v2M4.9 4.9l1.4 1.4M17.7 17.7l1.4 1.4M2 12h2M20 12h2M4.9 19.1l1.4-1.4M17.7 6.3l1.4-1.4"/></svg>
                : <svg viewBox="0 0 24 24" width="16" height="16" fill="none" stroke="currentColor" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round"><path d="M21 12.8A9 9 0 1 1 11.2 3a7 7 0 0 0 9.8 9.8z"/></svg>}
            </button>
          </div>
        </div>

        <div style={{display:"flex",alignItems:"center",padding:"6px 14px 12px",gap:10,minHeight:56}}>

          {/* Left: Back button */}
          <button onClick={onClose}
            style={{width:40,height:40,background:tbGroupBg,border:"none",borderRadius:13,cursor:"pointer",display:"flex",alignItems:"center",justifyContent:"center",flexShrink:0,color:tbText}}>
            <ChevronLeft style={{width:20,height:20}}/>
          </button>

          {/* Center: context-sensitive */}
          <div style={{flex:1,minWidth:0,display:"flex",alignItems:"center",justifyContent:(tool==="draw"||tool==="erase")?"flex-start":"center",gap:6,overflowX:"auto",WebkitOverflowScrolling:"touch",scrollbarWidth:"none"}}>
            {(tool === "draw" || tool === "erase") ? (
              // ── Draw mode toolbar ──────────────────────────────────────────────
              <>
                {/* Pen type pills */}
                <div style={{display:"flex",background:tbGroupBg,borderRadius:13,padding:3,gap:1}}>
                  {[
                    ["pen", <svg viewBox="0 0 24 24" width="18" height="18" fill="none" stroke="currentColor" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round"><path d="M17 3a2.85 2.85 0 0 1 4 4L7.5 20.5 2 22l1.5-5.5Z"/></svg>],
                    ["marker", <svg viewBox="0 0 24 24" width="18" height="18" fill="none" stroke="currentColor" strokeWidth="2.5" strokeLinecap="round"><path d="M9 11 4.5 15.5a2 2 0 0 0 3 3L12 14"/><path d="m14.5 3 6.5 6.5-10 10-7-7Z"/></svg>],
                    ["pencil", <svg viewBox="0 0 24 24" width="18" height="18" fill="none" stroke="currentColor" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round"><path d="m15 5 4 4"/><path d="M13 7 8.7 2.7a2.72 2.72 0 0 0-3.86 0L2.7 4.86A2.72 2.72 0 0 0 2.7 8.72L7 13"/><path d="M8 6l2 2"/><path d="m2 22 5.5-1.5L21 7a2.12 2.12 0 0 0-3-3L4.5 17.5Z"/></svg>],
                    ["line", <svg viewBox="0 0 24 24" width="18" height="18" fill="none" stroke="currentColor" strokeWidth="2" strokeLinecap="round"><line x1="5" y1="19" x2="19" y2="5"/><circle cx="19" cy="5" r="2.5"/><circle cx="5" cy="19" r="2.5"/></svg>],
                  ].map(([type, icon]) => (
                    <button key={type} onClick={()=>{setPenType(type);penTypeRef.current=type;if(tool==="erase"){setTool("draw");toolRef.current="draw";}}}
                      style={{width:38,height:34,background:penType===type&&tool!=="erase"?tbActive:"transparent",borderRadius:10,border:"none",cursor:"pointer",display:"flex",alignItems:"center",justifyContent:"center",color:penType===type&&tool!=="erase"?tbText:tbMuted,transition:"background .12s,color .12s"}}>
                      {icon}
                    </button>
                  ))}
                </div>
                {/* Stroke widths */}
                <div style={{display:"flex",background:tbGroupBg,borderRadius:13,padding:3,gap:1}}>
                  {[2,5,9].map(w=>(
                    <button key={w} onClick={()=>{setDrawWidth(w);drawWidthRef.current=w;}}
                      style={{width:38,height:34,background:drawWidth===w?tbActive:"transparent",borderRadius:10,border:"none",cursor:"pointer",display:"flex",alignItems:"center",justifyContent:"center",transition:"background .12s"}}>
                      <div style={{width:22,height:Math.max(1.5,Math.min(w*.6,7)),borderRadius:99,background:drawWidth===w?drawColor:tbMuted,transition:"background .12s"}}/>
                    </button>
                  ))}
                </div>
                {/* Color dot */}
                <button onClick={()=>{setShowColorPicker(v=>!v);}}
                  style={{width:40,height:40,background:showColorPicker?tbActive:tbGroupBg,borderRadius:13,border:"none",cursor:"pointer",display:"flex",alignItems:"center",justifyContent:"center",transition:"background .12s"}}>
                  <div style={{width:24,height:24,borderRadius:"50%",background:drawColor,border:"2.5px solid rgba(0,0,0,.15)",boxShadow:"0 1px 4px rgba(0,0,0,.2)"}}/>
                </button>
                {/* Erase toggle */}
                <button onClick={()=>{const next=tool==="erase"?"draw":"erase";setTool(next);toolRef.current=next;}}
                  style={{width:40,height:40,background:tool==="erase"?"rgba(255,59,48,.12)":tbGroupBg,borderRadius:13,border:"none",cursor:"pointer",display:"flex",alignItems:"center",justifyContent:"center",color:tool==="erase"?"#FF3B30":tbMuted,transition:"background .12s,color .12s"}}>
                  <svg viewBox="0 0 24 24" width="19" height="19" fill="none" stroke="currentColor" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round"><path d="m7 21-4.3-4.3c-1-1-1-2.5 0-3.4l9.6-9.6c1-1 2.5-1 3.4 0l5.6 5.6c1 1 1 2.5 0 3.4L13 21"/><path d="M22 21H7"/></svg>
                </button>
              </>
            ) : selectedId ? (
              // ── Node selected toolbar ──────────────────────────────────────────
              <>
                <button onPointerDown={e=>e.stopPropagation()} onClick={()=>{
                  const nd = nodes.find(n=>n.id===selectedId);
                  if(!nd) return;
                  let clone;
                  if(nd.type==="drawing") clone={...nd,id:Date.now(),points:nd.points.map(p=>({x:p.x+24,y:p.y+24}))};
                  else if(nd.type==="line") clone={...nd,id:Date.now(),x1:nd.x1+24,y1:nd.y1+24,x2:nd.x2+24,y2:nd.y2+24};
                  else clone={...nd,id:Date.now(),x:nd.x+24,y:nd.y+24};
                  setNodes(n=>[...n,clone]); setSelectedId(clone.id);
                }}
                  style={{height:40,padding:"0 16px",background:tbGroupBg,border:"none",borderRadius:13,cursor:"pointer",display:"flex",alignItems:"center",gap:7,color:tbText,fontWeight:600,fontSize:13}}>
                  <svg viewBox="0 0 24 24" width="17" height="17" fill="none" stroke="currentColor" strokeWidth="2" strokeLinecap="round"><rect x="9" y="9" width="13" height="13" rx="2"/><path d="M5 15H4a2 2 0 0 1-2-2V4a2 2 0 0 1 2-2h9a2 2 0 0 1 2 2v1"/></svg>
                  Duplicate
                </button>
                <button onPointerDown={e=>e.stopPropagation()} onClick={()=>{setNodes(n=>n.filter(nd=>nd.id!==selectedId));setSelectedId(null);}}
                  style={{height:40,padding:"0 16px",background:"rgba(255,59,48,.1)",border:"none",borderRadius:13,cursor:"pointer",display:"flex",alignItems:"center",gap:7,color:"#FF3B30",fontWeight:600,fontSize:13}}>
                  <Trash2 style={{width:17,height:17}}/> Delete
                </button>
              </>
            ) : (
              // ── Default toolbar: add tools ─────────────────────────────────────
              <>
                {/* Sticky note */}
                <button onClick={addNote}
                  style={{width:48,height:48,background:tbGroupBg,border:"none",borderRadius:15,cursor:"pointer",display:"flex",alignItems:"center",justifyContent:"center",color:tbText}}>
                  <svg viewBox="0 0 24 24" width="22" height="22" fill="none" stroke="currentColor" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round"><path d="M14.5 2H6a2 2 0 0 0-2 2v16a2 2 0 0 0 2 2h12a2 2 0 0 0 2-2V7.5L14.5 2z"/><polyline points="14 2 14 8 20 8"/></svg>
                </button>
                {/* Pen/Draw */}
                <button onClick={()=>{setTool("draw");toolRef.current="draw";setShowColorPicker(false);setShowVaultPicker(false);}}
                  style={{width:48,height:48,background:tbGroupBg,border:"none",borderRadius:15,cursor:"pointer",display:"flex",alignItems:"center",justifyContent:"center",color:tbText}}>
                  <svg viewBox="0 0 24 24" width="22" height="22" fill="none" stroke="currentColor" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round"><path d="M17 3a2.85 2.85 0 0 1 4 4L7.5 20.5 2 22l1.5-5.5Z"/><path d="m15 5 4 4"/></svg>
                </button>
                {/* Text */}
                <button onClick={addTextCenter}
                  style={{width:48,height:48,background:tbGroupBg,border:"none",borderRadius:15,cursor:"pointer",display:"flex",alignItems:"center",justifyContent:"center",color:tbText,fontSize:18,fontWeight:800,fontFamily:"Georgia,serif",letterSpacing:"-0.02em",lineHeight:1}}>
                  Aa
                </button>
                {/* Instant photo / video upload */}
                <label style={{width:48,height:48,background:tbGroupBg,borderRadius:15,cursor:"pointer",display:"flex",alignItems:"center",justifyContent:"center",color:tbText}}>
                  <svg viewBox="0 0 24 24" width="22" height="22" fill="none" stroke="currentColor" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round"><rect x="3" y="3" width="18" height="18" rx="2"/><circle cx="8.5" cy="8.5" r="1.6"/><path d="m21 15-5-5L5 21"/></svg>
                  <input type="file" accept="image/*,video/*" className="hidden"
                    onChange={e=>{const f=e.target.files?.[0];if(f)addMediaFile(f);e.target.value="";}} />
                </label>
                {/* Vault attachment */}
                <button onClick={()=>{setShowVaultPicker(v=>!v);setShowColorPicker(false);}}
                  style={{width:48,height:48,background:showVaultPicker?tbActive:tbGroupBg,border:"none",borderRadius:15,cursor:"pointer",display:"flex",alignItems:"center",justifyContent:"center",color:showVaultPicker?"#1768FF":tbText,transition:"background .12s,color .12s"}}>
                  <svg viewBox="0 0 24 24" width="22" height="22" fill="none" stroke="currentColor" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round"><path d="m21.44 11.05-9.19 9.19a6 6 0 0 1-8.49-8.49l8.57-8.57A4 4 0 1 1 18 8.84l-8.59 8.57a2 2 0 0 1-2.83-2.83l8.49-8.48"/></svg>
                </button>
              </>
            )}
          </div>

          {/* Right: Done (draw mode) or zoom% */}
          {(tool === "draw" || tool === "erase") ? (
            <button onClick={()=>{setTool("select");toolRef.current="select";setShowColorPicker(false);}}
              style={{height:40,padding:"0 16px",background:"#1768FF",border:"none",borderRadius:13,cursor:"pointer",color:"#fff",fontSize:13,fontWeight:700,flexShrink:0,boxShadow:"0 2px 10px rgba(23,104,255,.4)"}}>
              Done
            </button>
          ) : (
            <button onClick={()=>{setOffset({x:0,y:0});setScale(1);}}
              style={{height:40,padding:"0 11px",background:tbGroupBg,border:"none",borderRadius:13,cursor:"pointer",color:tbMuted,fontSize:11,fontWeight:700,flexShrink:0,minWidth:48}}>
              {Math.round(scale*100)}%
            </button>
          )}
        </div>
      </div>
    </>
  );
}

function QuickMediaVault() {
  const [theme, setTheme] = useState(loadFromStorage("subconscious_theme", "dark"));
  const t = themes[theme] || themes.dark;
  const [items, setItems] = useState(loadFromStorage("subconscious_items", starterItems));
  const [folderNames, setFolderNames] = useState(loadFromStorage("subconscious_folders", FOLDERS));
  const [url, setUrl] = useState("");
  const [note, setNote] = useState("");
  const [folder, setFolder] = useState("Ideas");
  const [newFolderName, setNewFolderName] = useState("");
  const [search, setSearch] = useState("");
  const [activeFilter, setActiveFilter] = useState("all");
  const [selectedFolder, setSelectedFolder] = useState("Ideas");
  const [editingTitleId, setEditingTitleId] = useState(null);
  const [editingTitle, setEditingTitle] = useState("");
  const [page, setPage] = useState("vault");
  const [currentUser, setCurrentUser] = useState(() => localAuth_getSession());
  const [settingsOpen, setSettingsOpen] = useState(false);
  const [addOpen, setAddOpen] = useState(false);
  const [voiceRecorderOpen, setVoiceRecorderOpen] = useState(false);
  const [mediaMode, setMediaMode] = useState("link");
  const [ideaStackIds, setIdeaStackIds] = useState([]);
  const [pulledItem, setPulledItem] = useState(null);
  const [pullGlow, setPullGlow] = useState(false);
  const [stackOutput, setStackOutput] = useState("");
  const [expandedItemId, setExpandedItemId] = useState(null);
  const [activeAudioId, setActiveAudioId] = useState(null);
  const [openMenuId, setOpenMenuId] = useState(null);
  const [playlists, setPlaylists] = useState(loadFromStorage("subconscious_playlists", [{ id: 1, name: "Favorites", songIds: [] }, { id: 2, name: "Study", songIds: [] }]));
  const [newPlaylistName, setNewPlaylistName] = useState("");
  const [newItemIds, setNewItemIds] = useState(new Set());
  const [playingPlaylistId, setPlayingPlaylistId] = useState(null);
  const [playlistQueueIndex, setPlaylistQueueIndex] = useState(0);
  const [expandedPlaylistId, setExpandedPlaylistId] = useState(null);
  const [playlistCurrentTime, setPlaylistCurrentTime] = useState(0);
  const [playlistDuration, setPlaylistDuration] = useState(0);
  const [syncStatus, setSyncStatus] = useState({ syncing: false, pending: 0 });
  const [sessions, setSessions] = useState(loadFromStorage("subconscious_sessions", []));
  const [renamingFolder, setRenamingFolder] = useState(null);
  const [renameFolderVal, setRenameFolderVal] = useState("");
  const [boardSession, setBoardSession] = useState(null);
  const playlistAudioRef = useRef(null);
  const playlistLoadingRef = useRef(false); // true while switching tracks — blocks onPause from clearing state
  const saveTimerRef = useRef(null);         // debounce handle for items → IDB writes
  const audioCtxRef = useRef(null);
  const analyserRef = useRef(null);
  const bassRafRef = useRef(null);
  const [bassLevel, setBassLevel] = useState(0);


  // Orientation lock: portrait everywhere except inside a board
  useEffect(() => {
    const so = screen.orientation || screen.msOrientation || screen.mozOrientation;
    if (!so || typeof so.lock !== "function") return;
    if (boardSession) {
      // Inside a board — unlock so the user can rotate freely
      try { so.unlock(); } catch (_) {}
    } else {
      // Everywhere else — lock to portrait
      so.lock("portrait").catch(() => {});
    }
  }, [boardSession]);

  // Update document title when page changes
  useEffect(() => {
    const titles = { vault: "Flare", creative: "Sandbox", folders: "Vault", playlists: "Playlist", profile: "Profile" };
    document.title = boardSession ? boardSession.name : (titles[page] || "Flare");
  }, [page, boardSession]);

  // Load data from IndexedDB on mount
  useEffect(() => {
    (async () => {
      try {
        const [loadedTheme, loadedItems, loadedPlaylists, loadedFolders] = await Promise.all([
          loadThemeFromDB("dark"),
          loadItemsFromDB(starterItems),
          loadPlaylistsFromDB([{ id: 1, name: "Favorites", songIds: [] }, { id: 2, name: "Study", songIds: [] }]),
          loadFoldersFromDB(FOLDERS)
        ]);
        setTheme(loadedTheme);
        setItems(loadedItems);
        setPlaylists(loadedPlaylists);
        setFolderNames(loadedFolders);
      } catch (e) {
        console.error("Error loading data:", e);
      }
    })();
  }, []);

  // Set up offline sync on mount
  useEffect(() => {
    const unsubscribe = onSyncChange(setSyncStatus);
    const cleanup = setupOfflineSync();
    return () => {
      unsubscribe();
      cleanup && cleanup();
    };
  }, []);

  // Visualizer is now CSS-only (AnimatedBars) — no RAF/AudioContext loop needed

  // Debounced items save: 300ms after the last change, batch-write all items in a single
  // IDB transaction. Audio items are saved with url:null (binary lives in audioBlobs store).
  useEffect(() => {
    if (saveTimerRef.current) clearTimeout(saveTimerRef.current);
    saveTimerRef.current = setTimeout(async () => {
      try {
        // Strip session blob URLs — they won't survive a reload anyway.
        // hasAudio items have their binary in the audioBlobs store.
        const itemsToSave = items.map(item =>
          (item.hasAudio || item.hasPdf || item.url?.startsWith("blob:"))
            ? { ...item, url: null }
            : item
        );
        await dbPutAllItems(itemsToSave);
      } catch (e) {
        console.error("Failed to save items:", e);
      }
    }, 300);
    return () => clearTimeout(saveTimerRef.current);
  }, [items]);

  useEffect(() => {
    (async () => {
      try {
        await dbClear("playlists");
        for (const p of playlists) {
          await dbPut("playlists", p);
        }
        await queueOp("UPDATE", "playlists", "playlists", playlists);
      } catch (e) {
        console.error("Failed to save playlists:", e);
      }
    })();
  }, [playlists]);

  useEffect(() => {
    (async () => {
      try {
        await dbClear("folders");
        folderNames.forEach((f, idx) => dbPut("folders", { id: idx, name: f }));
        await queueOp("UPDATE", "folders", "folders", folderNames);
      } catch (e) {
        console.error("Failed to save folders:", e);
      }
    })();
  }, [folderNames]);

  useEffect(() => {
    (async () => {
      try {
        await dbPut("theme", { key: "current", value: theme });
        await queueOp("UPDATE", "theme", "current", theme);
      } catch (e) {
        console.error("Failed to save theme:", e);
      }
    })();
  }, [theme]);

  // Auto-save to localStorage — strip binary data so we stay under the ~5MB quota
  useEffect(() => {
    const compact = items.map(item =>
      (item.hasAudio || item.hasPdf || item.url?.startsWith("blob:") || (item.url?.startsWith("data:") && item.url.length > 150000))
        ? { ...item, url: null }
        : item
    );
    saveToStorage("subconscious_items", compact);
  }, [items]);

  useEffect(() => {
    saveToStorage("subconscious_playlists", playlists);
  }, [playlists]);

  useEffect(() => {
    saveToStorage("subconscious_folders", folderNames);
  }, [folderNames]);

  useEffect(() => {
    saveToStorage("subconscious_theme", theme);
  }, [theme]);


  useEffect(() => {
    saveToStorage("subconscious_sessions", sessions);
  }, [sessions]);

  const inputStyle = { background: t.input, color: t.text };
  const primaryBg = `linear-gradient(135deg, ${t.glowA} 0%, ${t.glowB} 100%)`;
  const primaryText = "#fff";
  const filtersList = [["all", "All"], ["images", "Images"], ["links", "Links"], ["notes", "Notes"], ["audio", "Audio"], ["pdfs", "PDFs"]];
  const folders = useMemo(() => [...new Set([...folderNames, ...items.map((item) => item.folder || "Ideas")])], [folderNames, items]);
  const ideaStackItems = items.filter((item) => ideaStackIds.includes(item.id));
  const filteredItems = useMemo(() => {
    const q = search.toLowerCase().trim();
    return items.filter((item) => {
      const matches = activeFilter === "all" || (activeFilter === "images" && item.type === "image") || (activeFilter === "links" && item.type === "link") || (activeFilter === "audio" && item.type === "song") || (activeFilter === "notes" && item.type === "note") || (activeFilter === "pdfs" && item.type === "pdf");
      const haystack = [item.title, item.url, item.note, item.fileName, item.type].join(" ").toLowerCase();
      return matches && (!q || haystack.includes(q));
    }).sort((a, b) => (b.createdAt || 0) - (a.createdAt || 0));
  }, [items, search, activeFilter]);
  const folderItems = useMemo(() => items.filter((item) => (item.folder || "Ideas") === selectedFolder).sort((a, b) => (b.createdAt || 0) - (a.createdAt || 0)), [items, selectedFolder]);
  const patchItem = (id, patch) => setItems((current) => current.map((item) => item.id === id ? { ...item, ...patch } : item));
  const removeItem = (id) => {
    // Clean up audio memory when deleting a locally-stored audio item
    const target = items.find(i => i.id === id);
    if (target?.hasAudio) {
      const cached = audioBlobUrlCache.get(id);
      if (cached) { URL.revokeObjectURL(cached); audioBlobUrlCache.delete(id); }
      dbDeleteAudioBlob(id).catch(() => {});
    }
    setItems((current) => current.filter((item) => item.id !== id));
    setIdeaStackIds((current) => current.filter((itemId) => itemId !== id));
  };
  const saveTitle = (id) => { patchItem(id, { title: editingTitle.trim() || "Untitled" }); setEditingTitleId(null); setEditingTitle(""); };
  const startTitle = (item, event) => { event?.stopPropagation(); setEditingTitleId(item.id); setEditingTitle(item.title || "Untitled"); };
  const resetCapture = () => { setUrl(""); setNote(""); setFolder("Ideas"); setAddOpen(false); };
  const markNew = (id) => {
    setNewItemIds(prev => { const n = new Set(prev); n.add(id); return n; });
    setTimeout(() => setNewItemIds(prev => { const n = new Set(prev); n.delete(id); return n; }), 1200);
  };
  const addItem = () => {
    if (!url.trim() && !note.trim()) return;
    const id = newId();
    if (!url.trim()) {
      setItems((current) => [{ id, type: "note", title: "Note", url: "", note: note.trim(), folder, createdAt: Date.now() }, ...current]);
      markNew(id); resetCapture(); return;
    }
    const safeUrl = normalizeUrl(url);
    const type = detectType(safeUrl);
    setItems((current) => [{ id, type, title: titleFromUrl(safeUrl, type), url: safeUrl, note: note.trim(), folder, createdAt: Date.now() }, ...current]);
    markNew(id); resetCapture();
  };
  const addImageFile = (file) => {
    if (!file) return;
    const id = newId();
    const reader = new FileReader();
    reader.onload = () => {
      setItems((current) => [{ id, type: "image", title: file.name.replace(/\.[^/.]+$/, ""), url: String(reader.result || ""), note: note.trim(), folder, createdAt: Date.now() }, ...current]);
      setActiveFilter("images"); markNew(id); resetCapture();
    };
    reader.readAsDataURL(file);
  };
  const addAudioFile = (file) => {
    if (!file) return;
    const id = newId();
    const blobUrl = URL.createObjectURL(file);
    audioBlobUrlCache.set(id, blobUrl);
    setItems(current => [{
      id, type: "song",
      title: file.name.replace(/\.[^/.]+$/, ""),
      url: blobUrl,
      hasAudio: true,
      fileName: file.name,
      note: note.trim(), folder, createdAt: Date.now()
    }, ...current]);
    setActiveFilter("audio"); markNew(id);
    resetCapture();
    file.arrayBuffer()
      .then(buf => saveAudioBlob(id, buf, file.type || "audio/mpeg"))
      .catch(e => console.error("Failed to save audio blob:", e));
  };
  const addPdfFile = (file) => {
    if (!file) return;
    const id = newId();
    const blobUrl = URL.createObjectURL(file);
    audioBlobUrlCache.set(id, blobUrl);
    setItems(current => [{
      id, type: "pdf",
      title: file.name.replace(/\.[^/.]+$/, ""),
      url: blobUrl,
      hasPdf: true,
      fileName: file.name,
      note: note.trim(), folder, createdAt: Date.now()
    }, ...current]);
    setActiveFilter("pdfs"); markNew(id);
    resetCapture();
    file.arrayBuffer()
      .then(buf => saveAudioBlob(id, buf, file.type || "application/pdf"))
      .catch(e => console.error("Failed to save PDF blob:", e));
  };
  const createFolder = () => {
    const next = newFolderName.trim();
    if (!next) return;
    setFolderNames((current) => current.includes(next) ? current : [...current, next]);
    setSelectedFolder(next);
    setFolder(next);
    setNewFolderName("");
  };
  const renameFolder = (oldName, newName) => {
    const trimmed = newName.trim();
    if (!trimmed || trimmed === oldName) return;
    setFolderNames(prev => prev.map(f => f === oldName ? trimmed : f));
    setItems(prev => prev.map(item => (item.folder || "Ideas") === oldName ? { ...item, folder: trimmed } : item));
    if (selectedFolder === oldName) setSelectedFolder(trimmed);
    setRenamingFolder(null);
  };
  const pullFromSubconscious = () => {
    if (!items.length) return;
    setPullGlow(true);
    window.setTimeout(() => setPullGlow(false), 1000);
    setPulledItem(items[Math.floor(Math.random() * items.length)]);
  };
  const buildIdeaStack = () => {
    if (!ideaStackItems.length) return;
    const notes = ideaStackItems.map((item) => item.note || item.title).filter(Boolean);
    const types = [...new Set(ideaStackItems.map((item) => item.type))];
    const outputType = types.includes("song") ? "song/content concept" : types.includes("image") ? "visual/content concept" : "creative concept";
    setStackOutput(`CONCEPT\nMake a ${outputType} from: ${notes.join(" + ")}.\n\nDIRECTION\nUse the first selected save as the main hook. Use the second save as the visual world. Use the rest as texture or caption ideas.\n\nNEXT STEP\nTurn this into one post, one beat idea, one cover art direction, or one campaign angle.`);
  };
  const openPost = (item, event) => {
    if (event.target.closest("button,a,input,textarea,label,select")) return;
    setExpandedItemId(item.id);
  };
  const expandedItem = items.find((item) => item.id === expandedItemId) || null;
  const editorProps = { t, inputStyle, editingTitleId, editingTitle, setEditingTitle, saveTitle, startTitle };

  const renderCard = (item) => {
    if (item.type === "song") {
      return (
        <SongCard
          key={item.id}
          item={item}
          t={t}
          theme={theme}
          activeAudioId={activeAudioId}
          setActiveAudioId={setActiveAudioId}
          editorProps={editorProps}
          folders={folders}
          openMenuId={openMenuId}
          setOpenMenuId={setOpenMenuId}
          patchItem={patchItem}
          removeItem={removeItem}
          onOpen={(event) => openPost(item, event)}
        />
      );
    }
    if (item.type === "pdf") {
      return (
        <PdfCard
          key={item.id}
          item={item}
          t={t}
          editorProps={editorProps}
          folders={folders}
          openMenuId={openMenuId}
          setOpenMenuId={setOpenMenuId}
          patchItem={patchItem}
          removeItem={removeItem}
          onOpen={(event) => openPost(item, event)}
        />
      );
    }
    const Icon = iconFor(item.type);
    return (
      <article
        key={item.id}
        onClick={(event) => openPost(item, event)}
        className="card-press relative w-full min-w-0 cursor-pointer rounded-3xl"
        style={{
          background: t.panel,
          boxShadow: "0 2px 16px rgba(0,0,0,.1), 0 1px 4px rgba(0,0,0,.06)",
          border: `0.5px solid ${t.border}`,
        }}
      >
        {item.type === "image" && item.url && (
          <div style={{ padding: "1.5px", borderRadius: "1.5rem 1.5rem 0 0", background: `linear-gradient(160deg, ${t.glowA}60, ${t.glowB}30, transparent)` }}>
            <div style={{ position: "relative", overflow: "hidden", borderRadius: "calc(1.5rem - 1.5px) calc(1.5rem - 1.5px) 0 0" }}>
              <img src={item.url} alt={item.title} style={{ width: "100%", height: "auto", display: "block" }} />
              <div style={{ position: "absolute", inset: 0, background: "linear-gradient(to bottom, transparent 55%, rgba(0,0,0,.45))" }} />
            </div>
          </div>
        )}
        <div className="p-5">
          <div className="flex items-start gap-3">
            <div style={{
              display: "flex", width: 40, height: 40, flexShrink: 0,
              alignItems: "center", justifyContent: "center", borderRadius: 14,
              background: `linear-gradient(135deg, ${t.glowA}20, ${t.glowB}20)`,
              color: t.accent,
              border: `0.5px solid ${t.glowA}22`,
            }}>
              <Icon style={{ width: "1.05rem", height: "1.05rem" }} />
            </div>
            <div className="min-w-0 flex-1">
              <div className="flex items-baseline gap-2">
                <TextEditor item={item} {...editorProps} />
                <span className="type-secondary shrink-0 tabular-nums" style={{ color: t.soft, fontSize: 10 }}>{dateLabel(item.createdAt)}</span>
              </div>
              {item.note && <p className="mt-1.5 line-clamp-2 type-secondary leading-relaxed" style={{ color: t.muted }}>{stripHtml(item.note)}</p>}
            </div>
            <PostMenu item={item} folders={folders} t={t} openMenuId={openMenuId} setOpenMenuId={setOpenMenuId} patchItem={patchItem} removeItem={removeItem} />
          </div>
          {item.type === "link" && item.url && (
            <LinkPreviewCard url={normalizeUrl(item.url)} t={t} className="mt-3" />
          )}
          {item.type === "song" && item.url && !item.url.startsWith("data:") && !item.url.startsWith("blob:") && !item.hasAudio && (
            <a href={normalizeUrl(item.url)} target="_blank" rel="noopener noreferrer" onClick={e => e.stopPropagation()}
              className="mt-3 flex items-center gap-2 rounded-2xl px-3.5 py-2.5"
              style={{ background: t.input, color: t.soft, border: `0.5px solid ${t.border}`, textDecoration: "none", fontSize: 11, fontWeight: 500 }}>
              <ExternalLink style={{ width: 12, height: 12, flexShrink: 0 }} />
              <span style={{ overflow: "hidden", textOverflow: "ellipsis", whiteSpace: "nowrap" }}>{item.url.replace(/^https?:\/\//, "")}</span>
            </a>
          )}
        </div>
      </article>
    );
  };

  const filters = (
    <div style={{
      borderRadius: 22, padding: 6,
      background: t.panel,
      boxShadow: "0 2px 12px rgba(0,0,0,.1), 0 1px 4px rgba(0,0,0,.06)",
      border: `0.5px solid ${t.border}`,
    }}>
      <SlidingTabBar tabs={filtersList} active={activeFilter} onChange={setActiveFilter} t={t} textSize="text-xs" py="py-2.5" />
    </div>
  );

  const vaultPage = (
    <>
      <div>{filters}</div>
      <main className="grid min-w-0 gap-4 pt-4">
        {filteredItems.map((item) => (
          <CardReveal key={item.id} isNew={newItemIds.has(item.id)}>
            {renderCard(item)}
          </CardReveal>
        ))}
      </main>
      {filteredItems.length === 0 && (
        <EmptyState
          icon={activeFilter === "audio" ? Music : activeFilter === "images" ? Image : activeFilter === "notes" ? FileText : Vault}
          label={search ? `No results for "${search}"` : "Nothing here yet"}
          cta={search ? null : "Add something"}
          onCta={() => setAddOpen(true)}
          t={t}
        />
      )}
    </>
  );

  const foldersPage = (
    <div className="space-y-4">
      <section className="rounded-[2rem] p-4 backdrop-blur-xl" style={{ background: t.panel, boxShadow: t.shadow, border: `1px solid ${t.border}` }}>
        <div className="flex gap-2">
          <input value={newFolderName} onChange={(event) => setNewFolderName(event.target.value)} onKeyDown={(event) => { if (event.key === "Enter") createFolder(); }} placeholder="New folder..." className="min-w-0 flex-1 rounded-2xl px-4 py-3 text-sm outline-none" style={inputStyle} />
          <button onClick={createFolder} className="flex h-12 w-12 items-center justify-center rounded-2xl" style={{ background: primaryBg, color: primaryText }}><Plus className="h-4 w-4" /></button>
        </div>
      </section>
      <section className="rounded-[2rem] p-3 backdrop-blur-xl" style={{ background: t.panel, boxShadow: t.shadow, border: `1px solid ${t.border}` }}>
        <div className="grid grid-cols-2 gap-2">
          {folders.map((name) => {
            const count = items.filter((item) => (item.folder || "Ideas") === name).length;
            const isRenaming = renamingFolder === name;
            return (
              <div key={name} className="relative">
                <button onClick={() => setSelectedFolder(name)} className="flex w-full items-center gap-3 rounded-2xl p-3 text-left" style={{ background: selectedFolder === name ? t.active : t.input, color: t.text }}>
                  <Folder className="h-4 w-4 shrink-0" />
                  <span className="min-w-0 flex-1 pr-5">
                    {isRenaming ? (
                      <input
                        value={renameFolderVal}
                        onChange={e => setRenameFolderVal(e.target.value)}
                        onBlur={() => renameFolder(name, renameFolderVal)}
                        onKeyDown={e => { if(e.key==="Enter") renameFolder(name, renameFolderVal); if(e.key==="Escape") setRenamingFolder(null); }}
                        autoFocus
                        onClick={e => e.stopPropagation()}
                        className="block w-full bg-transparent outline-none text-xs font-semibold"
                        style={{ color: t.text }}
                      />
                    ) : (
                      <span className="block truncate text-xs font-semibold">{name}</span>
                    )}
                    <span className="block text-[10px]" style={{ color: t.muted }}>{count} saved</span>
                  </span>
                </button>
                <button onClick={e => { e.stopPropagation(); setRenamingFolder(name); setRenameFolderVal(name); }}
                  className="absolute right-2 top-1/2 -translate-y-1/2 flex h-6 w-6 items-center justify-center rounded-full"
                  style={{ background: "rgba(128,128,128,.15)", color: t.muted }}>
                  <Pencil style={{ width: 10, height: 10 }} />
                </button>
              </div>
            );
          })}
        </div>
      </section>
      <main className="grid min-w-0 gap-4">
        {folderItems.map((item) => (
          <CardReveal key={item.id} isNew={newItemIds.has(item.id)}>
            {renderCard(item)}
          </CardReveal>
        ))}
      </main>
      {folderItems.length === 0 && (
        <EmptyState
          icon={Folder}
          label={`Nothing in ${selectedFolder} yet`}
          cta="Add something"
          onCta={() => setAddOpen(true)}
          t={t}
        />
      )}
    </div>
  );

  const createSession = () => {
    const name = `Board ${sessions.length + 1}`;
    const s = { id: Date.now(), name, createdAt: Date.now(), nodes: [] };
    setSessions(prev => [...prev, s]);
    setBoardSession(s);
  };
  const renameSession = (id, newName) => {
    setSessions(prev => prev.map(s => s.id === id ? { ...s, name: newName } : s));
    if (boardSession?.id === id) setBoardSession(prev => prev ? { ...prev, name: newName } : prev);
  };
  const creativePage = (
    <CreativeSessions t={t} theme={theme} sessions={sessions}
      onOpen={(s) => setBoardSession(s)}
      onCreate={createSession}
      onDelete={(id) => setSessions(prev => prev.filter(s => s.id !== id))}
      onRename={renameSession}
      onSetCover={(id, patch) => setSessions(prev => prev.map(s => s.id === id ? { ...s, ...patch } : s))} />
  );

  const songs = items.filter((item) => item.type === "song");
  const createPlaylist = () => { if (!newPlaylistName.trim()) return; setPlaylists([...playlists, { id: Date.now(), name: newPlaylistName, songIds: [] }]); setNewPlaylistName(""); };
  const deletePlaylist = (id) => setPlaylists(playlists.filter(p => p.id !== id));
  const addSongToPlaylist = (playlistId, songId) => setPlaylists(playlists.map(p => p.id === playlistId ? { ...p, songIds: p.songIds.includes(songId) ? p.songIds.filter(id => id !== songId) : [...p.songIds, songId] } : p));

  // Playlist: play a song — plain HTML5, no Web Audio, no glitches
  const playPlaylistSong = async (pId, idx) => {
    const playlist = playlists.find(p => p.id === pId);
    if (!playlist) return;
    const pSongs = items.filter(s => playlist.songIds.includes(s.id));
    const song = pSongs[idx];
    if (!song || !playlistAudioRef.current) return;

    // Resolve the URL: direct URL or lazy-load from audioBlobs IDB
    let url = song.url;
    if ((!url || url.startsWith("blob:null")) && song.hasAudio) {
      if (audioBlobUrlCache.has(song.id)) {
        url = audioBlobUrlCache.get(song.id);
      } else {
        if (migrationPromises.has(song.id)) await migrationPromises.get(song.id);
        const data = await loadAudioBlob(song.id);
        if (data) {
          const blob = new Blob([data.buffer], { type: data.mime || "audio/mpeg" });
          url = URL.createObjectURL(blob);
          audioBlobUrlCache.set(song.id, url);
        }
      }
    }
    if (!url) return;

    // Guard so onPause doesn't reset state while we're switching tracks
    playlistLoadingRef.current = true;
    playlistAudioRef.current.pause();
    playlistAudioRef.current.src = url;
    playlistAudioRef.current.load();
    playlistAudioRef.current.play().catch(() => {});
    // Web Audio bass detection
    try {
      if (!audioCtxRef.current) {
        const ctx = new (window.AudioContext || window.webkitAudioContext)();
        const analyser = ctx.createAnalyser();
        analyser.fftSize = 256;
        analyser.smoothingTimeConstant = 0.82;
        const source = ctx.createMediaElementSource(playlistAudioRef.current);
        source.connect(analyser);
        analyser.connect(ctx.destination);
        audioCtxRef.current = ctx;
        analyserRef.current = analyser;
      }
      if (audioCtxRef.current.state === 'suspended') audioCtxRef.current.resume();
      cancelAnimationFrame(bassRafRef.current);
      const freq = new Uint8Array(analyserRef.current.frequencyBinCount);
      const tick = () => {
        analyserRef.current.getByteFrequencyData(freq);
        const bass = freq.slice(1, 9).reduce((a, b) => a + b, 0) / (8 * 255);
        setBassLevel(bass);
        bassRafRef.current = requestAnimationFrame(tick);
      };
      bassRafRef.current = requestAnimationFrame(tick);
    } catch(_) {}
    // Media Session API
    try {
      const msSong = items.filter(s => playlists.find(p=>p.id===pId)?.songIds.includes(s.id))[idx];
      if (navigator.mediaSession && msSong) {
        navigator.mediaSession.metadata = new MediaMetadata({ title: msSong.title, artist: 'Flare' });
        navigator.mediaSession.setActionHandler('pause', () => stopPlaylist());
        navigator.mediaSession.setActionHandler('play', () => playlistAudioRef.current?.play());
        navigator.mediaSession.setActionHandler('previoustrack', () => { if(idx > 0) playPlaylistSong(pId, idx-1); });
        navigator.mediaSession.setActionHandler('nexttrack', () => {
          const pl = playlists.find(p=>p.id===pId);
          const songs = items.filter(s=>pl?.songIds.includes(s.id));
          if(idx < songs.length-1) playPlaylistSong(pId, idx+1);
        });
      }
    } catch(_) {}
    playlistLoadingRef.current = false;
    setPlayingPlaylistId(pId);
    setPlaylistQueueIndex(idx);
    setExpandedPlaylistId(pId);
  };
  const stopPlaylist = () => {
    playlistAudioRef.current?.pause();
    setPlayingPlaylistId(null);
    cancelAnimationFrame(bassRafRef.current);
    setBassLevel(0);
  };
  const pct = playlistDuration > 0 ? (playlistCurrentTime / playlistDuration) * 100 : 0;
  const fmt = s => `${Math.floor(s/60)}:${String(Math.floor(s%60)).padStart(2,"0")}`;

  const playlistPage = (
    <div style={{display:"flex",flexDirection:"column",gap:14}}>

      {/* ── NOW PLAYING CARD ── */}
      {playingPlaylistId && (() => {
        const pl = playlists.find(p=>p.id===playingPlaylistId);
        const pSongs = pl ? items.filter(s=>pl.songIds.includes(s.id)) : [];
        const song = pSongs[playlistQueueIndex];
        if (!song) return null;
        const isFirst = playlistQueueIndex === 0;
        const isLast = playlistQueueIndex >= pSongs.length - 1;
        const scrubPlaylist = (pct) => {
          if (playlistAudioRef.current && playlistDuration) {
            playlistAudioRef.current.currentTime = (pct / 100) * playlistDuration;
            setPlaylistCurrentTime(playlistAudioRef.current.currentTime);
          }
        };
        return (
          <div style={{borderRadius:24,overflow:"hidden",background:`linear-gradient(145deg,${t.glowA}18,${t.glowB}12,${t.panel})`,border:`1px solid ${t.glowA}44`,boxShadow:t.shadow,padding:"18px 16px 16px"}}>
            {/* Song info */}
            <div style={{marginBottom:12}}>
              <p style={{fontSize:10,fontWeight:700,letterSpacing:"0.14em",textTransform:"uppercase",color:t.muted,margin:"0 0 4px"}}>{pl.name}</p>
              <p style={{fontSize:17,fontWeight:800,color:t.text,margin:"0 0 2px",lineHeight:1.2,overflow:"hidden",textOverflow:"ellipsis",whiteSpace:"nowrap"}}>{song.title}</p>
              {song.note && <p style={{fontSize:12,color:t.muted,margin:0,overflow:"hidden",textOverflow:"ellipsis",whiteSpace:"nowrap"}}>{song.note}</p>}
            </div>

            {/* Animated bars */}
            <AnimatedBars isPlaying={!!playingPlaylistId} glowA={t.glowA} glowB={t.glowB} />

            {/* Scrubber */}
            <AudioScrubber progress={pct} onScrub={scrubPlaylist} glowA={t.glowA} glowB={t.glowB} />
            <div style={{display:"flex",justifyContent:"space-between",fontSize:10,color:t.muted,marginTop:2,marginBottom:14}}>
              <span>{fmt(playlistCurrentTime)}</span><span>{fmt(playlistDuration||0)}</span>
            </div>

            {/* Controls */}
            <div style={{display:"flex",alignItems:"center",justifyContent:"center",gap:12}}>
              <button onClick={()=>{ if(!isFirst) playPlaylistSong(playingPlaylistId,playlistQueueIndex-1); }}
                style={{width:40,height:40,borderRadius:"50%",border:"none",cursor:"pointer",background:t.input,color:t.text,fontSize:18,display:"flex",alignItems:"center",justifyContent:"center",opacity:isFirst?0.25:1}}>⏮</button>
              <button onClick={()=>{ if(playlistAudioRef.current) playlistAudioRef.current.currentTime=Math.max(0,playlistCurrentTime-10); }}
                style={{width:38,height:38,borderRadius:"50%",border:"none",cursor:"pointer",background:t.input,color:t.text,fontSize:11,fontWeight:800,display:"flex",alignItems:"center",justifyContent:"center"}}>−10</button>
              <button onClick={stopPlaylist}
                style={{width:56,height:56,borderRadius:"50%",border:"none",cursor:"pointer",background:`linear-gradient(135deg,${t.glowA},${t.glowB})`,color:"#fff",display:"flex",alignItems:"center",justifyContent:"center",boxShadow:`0 4px 22px ${t.glowA}55`,flexShrink:0}}>
                <Pause className="h-5 w-5 fill-current"/>
              </button>
              <button onClick={()=>{ if(playlistAudioRef.current) playlistAudioRef.current.currentTime=Math.min(playlistDuration,playlistCurrentTime+10); }}
                style={{width:38,height:38,borderRadius:"50%",border:"none",cursor:"pointer",background:t.input,color:t.text,fontSize:11,fontWeight:800,display:"flex",alignItems:"center",justifyContent:"center"}}>+10</button>
              <button onClick={()=>{ if(!isLast) playPlaylistSong(playingPlaylistId,playlistQueueIndex+1); }}
                style={{width:40,height:40,borderRadius:"50%",border:"none",cursor:"pointer",background:t.input,color:t.text,fontSize:18,display:"flex",alignItems:"center",justifyContent:"center",opacity:isLast?0.25:1}}>⏭</button>
            </div>
          </div>
        );
      })()}

      {/* ── PLAYLISTS GRID ── */}
      <div style={{display:"grid",gridTemplateColumns:"1fr 1fr",gap:12}}>
        {playlists.map(playlist => {
          const pSongs = items.filter(s => playlist.songIds.includes(s.id));
          const isActive = playingPlaylistId === playlist.id;
          const isExpanded = expandedPlaylistId === playlist.id;
          return (
            <div key={playlist.id} onClick={()=>setExpandedPlaylistId(isExpanded?null:playlist.id)}
              style={{borderRadius:20,overflow:"hidden",background:t.panel,
                border:`1.5px solid ${isActive?t.glowA:isExpanded?t.accent+"66":t.border}`,
                boxShadow:isActive?`0 0 24px ${t.glowA}44`:t.shadow,cursor:"pointer",
                display:"flex",flexDirection:"column",
                transition:"border-color .2s,box-shadow .2s"}}>
              {/* Cover */}
              <div style={{position:"relative",aspectRatio:"1",background:isActive?`linear-gradient(135deg,${t.glowA},${t.glowB})`:t.input,display:"flex",alignItems:"center",justifyContent:"center",overflow:"hidden"}}>
                {playlist.cover && <img src={playlist.cover} style={{position:"absolute",inset:0,width:"100%",height:"100%",objectFit:"cover"}}/>}
                {!playlist.cover && <Music style={{width:28,height:28,color:isActive?"rgba(255,255,255,.6)":t.muted}}/>}
                {/* Play */}
                <button onClick={e=>{e.stopPropagation();isActive?stopPlaylist():playPlaylistSong(playlist.id,0);}}
                  style={{position:"absolute",bottom:8,right:8,width:34,height:34,borderRadius:"50%",border:"none",cursor:"pointer",
                    background:playlist.cover?"rgba(0,0,0,.55)":isActive?"rgba(255,255,255,.22)":"rgba(0,0,0,.22)",
                    color:"#fff",display:"flex",alignItems:"center",justifyContent:"center",
                    backdropFilter:"blur(8px)",WebkitBackdropFilter:"blur(8px)"}}>
                  {isActive?<Pause className="h-3.5 w-3.5 fill-current"/>:<Play className="h-3.5 w-3.5 fill-current ml-0.5"/>}
                </button>
                {/* Delete */}
                <button onClick={e=>{e.stopPropagation();deletePlaylist(playlist.id);}}
                  style={{position:"absolute",top:6,right:6,width:26,height:26,borderRadius:"50%",border:"none",cursor:"pointer",
                    background:"rgba(0,0,0,.45)",color:"#fff",display:"flex",alignItems:"center",justifyContent:"center",
                    backdropFilter:"blur(8px)",WebkitBackdropFilter:"blur(8px)"}}>
                  <X style={{width:12,height:12}}/>
                </button>
              </div>
              {/* Info */}
              <div style={{padding:"9px 11px 10px"}}>
                <p style={{fontSize:13,fontWeight:700,color:isActive?t.glowA:t.text,margin:"0 0 2px",overflow:"hidden",textOverflow:"ellipsis",whiteSpace:"nowrap"}}>{playlist.name}</p>
                <p style={{fontSize:10,color:t.muted,margin:0}}>{pSongs.length} track{pSongs.length!==1?"s":""}</p>
              </div>
            </div>
          );
        })}
      </div>

      {/* Expanded playlist detail */}
      {expandedPlaylistId && (() => {
        const playlist = playlists.find(p=>p.id===expandedPlaylistId);
        if(!playlist) return null;
        const pSongs = items.filter(s=>playlist.songIds.includes(s.id));
        const isActive = playingPlaylistId === playlist.id;
        return (
          <div style={{borderRadius:20,background:t.panel,border:`1px solid ${t.border}`,padding:"14px",animation:"contextIn 220ms var(--spring-snappy) both"}}>
            <p style={{fontSize:11,fontWeight:700,letterSpacing:"0.1em",textTransform:"uppercase",color:t.muted,margin:"0 0 10px"}}>{playlist.name}</p>
            {/* Cover art picker */}
            <div style={{display:"flex",alignItems:"center",gap:10,marginBottom:12}}>
              <label style={{position:"relative",width:44,height:44,borderRadius:12,overflow:"hidden",flexShrink:0,cursor:"pointer",background:playlist.cover?"transparent":`${t.glowA}18`,border:`1px dashed ${t.border}`}}>
                {playlist.cover && <img src={playlist.cover} style={{position:"absolute",inset:0,width:"100%",height:"100%",objectFit:"cover"}}/>}
                <div style={{position:"absolute",inset:0,display:"flex",alignItems:"center",justifyContent:"center",background:playlist.cover?"rgba(0,0,0,.35)":"transparent"}}>
                  <Camera style={{width:16,height:16,color:playlist.cover?"#fff":t.muted}}/>
                </div>
                <input type="file" accept="image/*" style={{display:"none"}} onChange={async e=>{
                  const f=e.target.files?.[0]; if(!f)return;
                  const c=await compressCoverArt(f);
                  if(c) setPlaylists(prev=>prev.map(p=>p.id===playlist.id?{...p,cover:c}:p));
                  e.target.value="";
                }}/>
              </label>
              <div>
                <p style={{fontSize:12,fontWeight:600,color:t.text,margin:"0 0 2px"}}>{playlist.cover?"Change cover":"Add cover"}</p>
                {playlist.cover && <button onClick={()=>setPlaylists(prev=>prev.map(p=>p.id===playlist.id?{...p,cover:null}:p))} style={{fontSize:11,color:t.muted,background:"none",border:"none",cursor:"pointer",padding:0}}>Remove</button>}
              </div>
            </div>
            {/* Tracks */}
            {pSongs.length===0 && <p style={{color:t.muted,textAlign:"center",padding:"10px 0",fontSize:12}}>No tracks yet — add some below</p>}
            {pSongs.map((song,idx)=>(
              <div key={song.id} onClick={()=>playPlaylistSong(playlist.id,idx)}
                style={{display:"flex",alignItems:"center",gap:10,padding:"8px 10px",borderRadius:12,cursor:"pointer",
                  background:isActive&&playlistQueueIndex===idx?`${t.glowA}18`:"transparent",marginBottom:2}}>
                <div style={{width:30,height:30,borderRadius:9,flexShrink:0,display:"flex",alignItems:"center",justifyContent:"center",
                  background:isActive&&playlistQueueIndex===idx?`linear-gradient(135deg,${t.glowA},${t.glowB})`:t.input,
                  color:isActive&&playlistQueueIndex===idx?"#fff":t.muted}}>
                  {isActive&&playlistQueueIndex===idx?<Pause className="h-3 w-3 fill-current"/>:<span style={{fontSize:10,fontWeight:700}}>{idx+1}</span>}
                </div>
                <p style={{flex:1,fontSize:13,fontWeight:600,color:isActive&&playlistQueueIndex===idx?t.glowA:t.text,margin:0,overflow:"hidden",textOverflow:"ellipsis",whiteSpace:"nowrap"}}>{song.title}</p>
                <button onClick={e=>{e.stopPropagation();addSongToPlaylist(playlist.id,song.id);}}
                  style={{width:24,height:24,borderRadius:"50%",background:"transparent",border:"none",cursor:"pointer",color:t.muted,display:"flex",alignItems:"center",justifyContent:"center"}}>
                  <X style={{width:14,height:14}}/>
                </button>
              </div>
            ))}
            {songs.some(s=>!playlist.songIds.includes(s.id)) && (
              <>
                <p style={{fontSize:10,fontWeight:700,letterSpacing:"0.1em",textTransform:"uppercase",color:t.muted,margin:"12px 0 6px 4px"}}>Add tracks</p>
                {songs.filter(s=>!playlist.songIds.includes(s.id)).map(song=>(
                  <button key={song.id} onClick={()=>addSongToPlaylist(playlist.id,song.id)}
                    style={{width:"100%",display:"flex",alignItems:"center",gap:8,padding:"7px 10px",background:"transparent",border:`1px dashed ${t.border}`,borderRadius:10,cursor:"pointer",marginBottom:4,textAlign:"left"}}>
                    <Plus style={{width:14,height:14,color:t.muted,flexShrink:0}}/>
                    <span style={{fontSize:12,fontWeight:500,color:t.muted,overflow:"hidden",textOverflow:"ellipsis",whiteSpace:"nowrap"}}>{song.title}</span>
                  </button>
                ))}
              </>
            )}
          </div>
        );
      })()}

      {/* ── CREATE PLAYLIST ── */}
      <div style={{display:"flex",gap:8,padding:"4px 0"}}>
        <input value={newPlaylistName} onChange={e=>setNewPlaylistName(e.target.value)}
          onKeyDown={e=>{if(e.key==="Enter")createPlaylist();}}
          placeholder="New playlist name…"
          style={{flex:1,background:t.input,border:`1px solid ${t.border}`,borderRadius:14,padding:"12px 16px",fontSize:14,color:t.text,outline:"none"}}/>
        <button onClick={createPlaylist}
          style={{width:48,height:48,borderRadius:14,border:"none",cursor:"pointer",background:primaryBg,color:"#fff",display:"flex",alignItems:"center",justifyContent:"center",flexShrink:0}}>
          <Plus className="h-5 w-5"/>
        </button>
      </div>

      {playlists.length===0 && (
        <EmptyState
          icon={Music}
          label="No playlists yet"
          cta={null}
          t={t}
        />
      )}
    </div>
  );

  const pageTitle = page === "creative" ? "Sandbox" : page === "playlists" ? "Playlist" : page === "folders" ? "Vault" : "Flare";
  const profilePage = <ProfilePage t={t} currentUser={currentUser} setCurrentUser={setCurrentUser} />;
  const currentPage = page === "vault" ? vaultPage : page === "creative" ? creativePage : page === "folders" ? foldersPage : page === "profile" ? profilePage : playlistPage;

  return (
    <div className="theme-bg min-h-screen px-4" style={{ background: t.page, color: t.text, paddingTop: 0, paddingBottom: "calc(7rem + env(safe-area-inset-bottom))" }}>
      <Styles />
      <audio ref={playlistAudioRef} style={{display:"none"}}
        onPause={()=>{ if(playingPlaylistId && !playlistLoadingRef.current) setPlayingPlaylistId(null); }}
        onTimeUpdate={()=>{ if(playlistAudioRef.current) setPlaylistCurrentTime(playlistAudioRef.current.currentTime); }}
        onLoadedMetadata={()=>{ if(playlistAudioRef.current) setPlaylistDuration(playlistAudioRef.current.duration); }}
        onEnded={()=>{
          const playlist = playlists.find(p=>p.id===playingPlaylistId);
          const pSongs = playlist ? items.filter(s=>playlist.songIds.includes(s.id)) : [];
          if(playlistQueueIndex < pSongs.length-1) playPlaylistSong(playingPlaylistId, playlistQueueIndex+1);
          else setPlayingPlaylistId(null);
        }}
      />
      {/* Fixed background — prevents overscroll flash */}
      <div style={{ position: "fixed", inset: 0, background: t.page, zIndex: -1 }} />
      {/* Ambient glow orbs */}
      <div className="pointer-events-none fixed inset-0 overflow-hidden">
        <div className="absolute -left-32 -top-16 h-96 w-96 rounded-full blur-3xl" style={{ background: t.glowA, opacity: theme === "dark" ? 0.15 : 0.22 }} />
        <div className="absolute -right-32 top-48 h-80 w-80 rounded-full blur-3xl" style={{ background: t.glowC, opacity: theme === "dark" ? 0.13 : 0.18 }} />
        <div className="absolute left-1/2 top-[60%] h-64 w-64 -translate-x-1/2 rounded-full blur-3xl" style={{ background: t.glowB, opacity: theme === "dark" ? 0.1 : 0.12 }} />
      </div>
      {/* Bass-reactive glow — pulses with music */}
      {playingPlaylistId && (
        <div style={{
          position:"fixed", inset:0, zIndex:9, pointerEvents:"none",
          mixBlendMode: "screen",
          background:`radial-gradient(ellipse 100% 65% at 50% 105%, ${t.glowA}${Math.round((bassLevel*0.75+0.08)*99).toString(16).padStart(2,"0")}, ${t.glowB}${Math.round((bassLevel*0.5+0.04)*99).toString(16).padStart(2,"0")} 45%, transparent 72%)`,
        }}/>
      )}

      {/* ── Header ── */}
      <div style={{ paddingTop: "max(0px, env(safe-area-inset-top))" }}>
        <div className="mx-auto max-w-md">
          <header style={{ display: "flex", alignItems: "flex-start", justifyContent: "space-between", gap: 12, padding: "28px 0 20px" }}>
            <div style={{ minWidth: 0 }}>
              <FlareTitle t={t} />
              <p className="type-label" style={{ color: t.muted, marginTop: 6, letterSpacing: "0.18em" }}>
                {page === "vault"    ? "By Loveem, For You."
                : page === "creative" ? "Sandbox"
                : page === "folders"  ? "Vault"
                : page === "playlists"? "Playlist"
                : page === "profile"  ? "Profile"
                : "By Loveem, For You."}
              </p>
            </div>
            <button
              onClick={() => setSettingsOpen(true)}
              aria-label="Settings"
              style={{
                flexShrink: 0, display: "flex", alignItems: "center", justifyContent: "center",
                width: 40, height: 40, borderRadius: "50%", border: `1px solid ${t.border}`,
                background: t.panel, color: t.muted, cursor: "pointer",
                boxShadow: `0 0 16px ${t.glowA}22`,
                backdropFilter: "blur(12px)", WebkitBackdropFilter: "blur(12px)",
              }}
            >
              <SettingsIcon style={{ width: 18, height: 18 }} />
            </button>
          </header>
        </div>
      </div>

      {/* ── Page content ── */}
      <div className="relative mx-auto max-w-md" style={{ paddingTop: 8 }}>
        <div key={page} style={{ animation: "pageFade 320ms var(--spring-smooth) both" }}>
          {currentPage}
        </div>
      </div>

      {boardSession && <BoardView session={boardSession} t={t} theme={theme} vaultItems={items}
        onSave={(nodes) => setSessions(prev => prev.map(s => s.id === boardSession.id ? {...s, nodes} : s))}
        onClose={() => setBoardSession(null)} />}
      <AddTrayPanel open={addOpen} t={t} theme={theme} inputStyle={inputStyle} mediaMode={mediaMode} setMediaMode={setMediaMode} url={url} note={note} folder={folder} setUrl={setUrl} setNote={setNote} setFolder={setFolder} addItem={addItem} addImageFile={addImageFile} addAudioFile={addAudioFile} addPdfFile={addPdfFile} onVoiceRecord={() => setVoiceRecorderOpen(true)} close={() => setAddOpen(false)} />
      <VoiceRecorder open={voiceRecorderOpen} t={t} onRecord={(recording) => {
        const id = newId();
        if (recording.url) audioBlobUrlCache.set(id, recording.url);
        setItems(current => [{
          id, type: "song",
          title: recording.title,
          url: recording.url,
          hasAudio: true,
          note: recording.note || "",
          folder, createdAt: Date.now()
        }, ...current]);
        markNew(id);
        setActiveFilter("audio");
        setVoiceRecorderOpen(false);
        if (recording.blob) {
          recording.blob.arrayBuffer()
            .then(buf => saveAudioBlob(id, buf, recording.blob.type || "audio/webm"))
            .catch(e => console.error("Failed to save voice memo:", e));
        }
      }} close={() => setVoiceRecorderOpen(false)} />
      <ExpandedPost item={expandedItem} t={t} theme={theme} activeAudioId={activeAudioId} setActiveAudioId={setActiveAudioId} patchItem={patchItem} close={() => setExpandedItemId(null)} />

      <SettingsDrawer
        open={settingsOpen}
        onClose={() => setSettingsOpen(false)}
        t={t}
        theme={theme} setTheme={setTheme}
        currentUser={currentUser} setCurrentUser={setCurrentUser}
        onGoProfile={() => setPage("profile")}
      />

      {!boardSession && (
        <nav
          style={{
            position: "fixed", bottom: "max(12px, env(safe-area-inset-bottom))",
            left: "50%", transform: "translateX(-50%)",
            width: "calc(100% - 32px)", maxWidth: 384,
            display: "grid", gridTemplateColumns: "repeat(5, 1fr)",
            alignItems: "center", gap: 4,
            borderRadius: 26, padding: 6,
            background: t.panel,
            backdropFilter: "blur(28px) saturate(160%)",
            WebkitBackdropFilter: "blur(28px) saturate(160%)",
            boxShadow: `${t.shadow}, 0 0 40px ${t.glowA}22`,
            border: `0.5px solid ${t.border}`,
            zIndex: 20,
          }}
        >
          <NavButton active={page === "vault"} icon={Feed} label="Feed" onClick={() => { setAddOpen(false); setPage("vault"); window.scrollTo({top:0,behavior:"smooth"}); }} t={t} />
          <NavButton active={page === "creative"} icon={Sparkles} label="Sandbox" onClick={() => { setAddOpen(false); setPage("creative"); }} t={t} />
          {/* FAB — centre button */}
          <button
            onClick={() => setAddOpen(o => !o)}
            aria-label="Add"
            className="fab"
            style={{
              margin: "0 auto",
              display: "flex", width: 48, height: 48,
              alignItems: "center", justifyContent: "center",
              borderRadius: "50%", border: "none", cursor: "pointer",
              background: primaryBg, color: "#fff",
              boxShadow: addOpen
                ? `0 0 0 4px ${t.glowA}44, 0 0 28px ${t.glowA}77`
                : "0 6px 20px rgba(0,0,0,.28)",
            }}
          >
            <Plus style={{
              width: 22, height: 22,
              transform: addOpen ? "rotate(45deg)" : "rotate(0deg)",
              transition: "transform 260ms var(--spring-snappy)",
            }} />
          </button>
          <NavButton active={page === "folders"} icon={Vault} label="Vault" onClick={() => { setAddOpen(false); setPage("folders"); }} t={t} />
          <NavButton active={page === "playlists"} icon={Music} label="Playlist" onClick={() => { setAddOpen(false); setPage("playlists"); }} t={t} />
        </nav>
      )}
    </div>
  );
}

const rootEl = document.getElementById("root");
if (rootEl) createRoot(rootEl).render(<QuickMediaVault />);
