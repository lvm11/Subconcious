# Subconscious Prototype

This workspace contains a repaired local copy of the Canvas export.

## Run

Use the bundled Node runtime:

```powershell
& "C:\Users\juuj1\.cache\codex-runtimes\codex-primary-runtime\dependencies\node\bin\node.exe" server.mjs
```

Then open:

```text
http://localhost:4173
```

The current prototype is intentionally dependency-light and loads React, ReactDOM, Lucide, Tailwind, and Babel from CDNs in `index.html` / `src/subconscious.jsx`.
