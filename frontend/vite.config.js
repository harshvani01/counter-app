import { defineConfig } from "vite";
import react from "@vitejs/plugin-react";

// During local `npm run dev`, proxy /api calls to the backend so the frontend
// can use the same relative paths it uses in production behind Traefik.
export default defineConfig({
  plugins: [react()],
  server: {
    // Bind to all interfaces so the dev server is reachable from outside the VM,
    // not just localhost.
    host: true,
    port: 5173,
    // Vite blocks requests whose Host header it doesn't recognise. Allow our
    // public domain so http://tpproject.duckdns.org:5173 works.
    allowedHosts: ["tpproject.duckdns.org"],
    proxy: {
      // The browser calls /api/... on the dev server; Vite forwards it to the
      // backend running locally on the VM. This proxy runs server-side, so the
      // backend only needs to listen on localhost.
      "/api": {
        target: "http://localhost:8000",
        changeOrigin: true,
      },
    },
  },
});
