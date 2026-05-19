import { createServer } from "node:http";
import { readFile, writeFile, mkdir } from "node:fs/promises";
import { extname, join, normalize } from "node:path";
import { fileURLToPath } from "node:url";

const root = fileURLToPath(new URL(".", import.meta.url));
const uploadsDir = join(root, "uploads");
const port = Number(process.env.PORT || 4173);
const types = {
  ".html": "text/html; charset=utf-8",
  ".jsx": "text/babel; charset=utf-8",
  ".js": "text/javascript; charset=utf-8",
  ".css": "text/css; charset=utf-8",
  ".mp3": "audio/mpeg",
  ".wav": "audio/wav",
  ".m4a": "audio/mp4",
  ".ogg": "audio/ogg",
};

await mkdir(uploadsDir, { recursive: true });

// In-memory storage for synced data (replace with real DB in production)
let serverState = {
  items: [],
  playlists: [],
  folders: [],
  theme: "dark"
};

createServer(async (req, res) => {
  try {
    // Handle sync endpoint
    if (req.method === "POST" && req.url === "/api/sync") {
      let body = "";
      req.on("data", chunk => body += chunk);
      req.on("end", async () => {
        try {
          const syncData = JSON.parse(body);
          const syncedIds = [];

          // Process operations
          for (const op of syncData.operations || []) {
            if (op.type === "CREATE" || op.type === "UPDATE") {
              if (op.entityType === "item") {
                const idx = serverState.items.findIndex(i => i.id === op.entityId);
                if (idx >= 0) {
                  serverState.items[idx] = op.data;
                } else {
                  serverState.items.push(op.data);
                }
              } else if (op.entityType === "playlists") {
                serverState.playlists = op.data;
              } else if (op.entityType === "folders") {
                serverState.folders = op.data;
              } else if (op.entityType === "theme") {
                serverState.theme = op.data;
              }
            } else if (op.type === "DELETE") {
              if (op.entityType === "item") {
                serverState.items = serverState.items.filter(i => i.id !== op.entityId);
              }
            }
            syncedIds.push(op.id);
          }

          res.writeHead(200, { "Content-Type": "application/json" });
          res.end(JSON.stringify({
            success: true,
            syncedIds,
            items: serverState.items,
            playlists: serverState.playlists,
            folders: serverState.folders,
            theme: serverState.theme
          }));
        } catch (err) {
          console.error("Sync error:", err);
          res.writeHead(500);
          res.end("Sync failed");
        }
      });
      return;
    }

    // Handle file uploads
    if (req.method === "POST" && req.url === "/upload") {
      const chunks = [];
      let boundary = null;

      req.on("data", chunk => chunks.push(chunk));
      req.on("end", async () => {
        try {
          const buffer = Buffer.concat(chunks);
          const contentType = req.headers["content-type"];

          // Extract boundary from content-type header
          const boundaryMatch = contentType?.match(/boundary=([^\r\n]+)/);
          if (!boundaryMatch) {
            res.writeHead(400);
            res.end("No boundary found");
            return;
          }

          boundary = boundaryMatch[1];
          const boundaryBuffer = Buffer.from(`--${boundary}`);

          // Find the file content between boundaries
          const startIdx = buffer.indexOf(boundaryBuffer) + boundaryBuffer.length;
          const endIdx = buffer.indexOf(boundaryBuffer, startIdx);

          // Find the actual file data (after headers)
          const headerEndIdx = buffer.indexOf("\r\n\r\n", startIdx);
          const fileData = buffer.slice(headerEndIdx + 4, endIdx - 2);

          const fileName = `audio-${Date.now()}.mp3`;
          const filePath = join(uploadsDir, fileName);
          await writeFile(filePath, fileData);

          res.writeHead(200, { "Content-Type": "application/json" });
          res.end(JSON.stringify({ url: `/uploads/${fileName}` }));
        } catch (err) {
          console.error("Upload error:", err);
          res.writeHead(500);
          res.end("Upload failed");
        }
      });
      return;
    }

    const url = new URL(req.url || "/", `http://localhost:${port}`);
    const requested = url.pathname === "/" ? "/index.html" : url.pathname;
    const filePath = normalize(join(root, requested));

    if (!filePath.startsWith(normalize(root))) {
      res.writeHead(403);
      res.end("Forbidden");
      return;
    }

    const body = await readFile(filePath);
    res.writeHead(200, {
      "Content-Type": types[extname(filePath)] || "application/octet-stream",
      "Cache-Control": "no-store",
    });
    res.end(body);
  } catch {
    res.writeHead(404);
    res.end("Not found");
  }
}).listen(port, () => {
  console.log(`Subconscious running at http://localhost:${port}`);
});
