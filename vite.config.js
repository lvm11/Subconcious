import { defineConfig } from 'vite'
import react from '@vitejs/plugin-react'

export default defineConfig({
  plugins: [
    react(), // handles JSX transform + fast refresh in dev
  ],
  server: {
    port: 5173,
    host: true,  // expose on local network (0.0.0.0)
    open: true, // auto-open browser on npm run dev
  },
  build: {
    outDir: 'dist',
    // Increase the chunk-size warning threshold — single-file app is expected to be large
    chunkSizeWarningLimit: 1000,
  },
})
