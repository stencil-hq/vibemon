import react from "@vitejs/plugin-react";
import { defineConfig } from "vite";

// The driver crate embeds these exact file names (driver/src/dashboard.rs):
// index.html, assets/app.js, assets/app.css, and public/ copied verbatim.
// `bun run dev` serves the app on :5173 and proxies everything outside
// /dashboard/ (E2B API, admin API, sandbox ingress) to `just serve` on :3000.
export default defineConfig({
  base: "/dashboard/",
  plugins: [react()],
  build: {
    outDir: "../driver/web",
    emptyOutDir: true,
    assetsInlineLimit: 0,
    // One bundle on purpose (fixed embedded names); xterm + React dominate it.
    chunkSizeWarningLimit: 900,
    rolldownOptions: {
      output: {
        entryFileNames: "assets/app.js",
        chunkFileNames: "assets/app-[name].js",
        assetFileNames: "assets/app[extname]",
      },
    },
  },
  server: {
    port: 5173,
    proxy: {
      "^/(?!dashboard/|@|src/|node_modules/).*": { target: "http://127.0.0.1:3000" },
    },
  },
});
