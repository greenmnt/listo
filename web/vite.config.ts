import { defineConfig } from "vite";
import react from "@vitejs/plugin-react";

export default defineConfig({
  plugins: [react()],
  server: {
    port: 5173,
    // The Rust API listens on :8080. Connect-Web will hit this directly via
    // CORS (configured server-side), so no proxy needed for the gRPC-Web
    // calls — but keep this here in case we want to flip to a same-origin
    // setup later.
    proxy: {
      "/api": {
        target: "http://localhost:8080",
        changeOrigin: true,
      },
    },
  },
});
