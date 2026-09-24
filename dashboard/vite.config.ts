import { defineConfig } from "vite";
import react from "@vitejs/plugin-react";

// `npm run dev` proxies the API to a controller on :8000; `npm run build` writes dist/,
// which the controller serves at / (see BURST_DASHBOARD_DIR).
const api = "http://127.0.0.1:8000";

export default defineConfig({
  plugins: [react()],
  server: {
    proxy: Object.fromEntries(
      ["/jobs", "/stats", "/workers", "/backends", "/policy", "/healthz"].map((p) => [p, api]),
    ),
  },
});
