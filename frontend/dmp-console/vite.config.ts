import { fileURLToPath, URL } from 'node:url'
import { defineConfig } from 'vite'
import react from '@vitejs/plugin-react'

export default defineConfig({
  plugins: [react()],
  resolve: {
    // Must mirror the `paths` entry in tsconfig.json. TypeScript and the bundler resolve
    // independently, so an alias in one and not the other typechecks cleanly and fails at build.
    alias: {
      '@': fileURLToPath(new URL('./src', import.meta.url)),
    },
  },
  server: {
    port: 5173,
    // The console talks to the API on the same origin, so nothing in the app needs to know a
    // base URL. In development that origin is Vite; in Docker it is nginx. Same code, no
    // environment-specific configuration to get wrong.
    proxy: {
      '/api': {
        target: process.env.DMP_API_URL ?? 'http://localhost:8080',
        changeOrigin: true,
      },
      '/actuator': {
        target: process.env.DMP_API_URL ?? 'http://localhost:8080',
        changeOrigin: true,
      },
    },
  },
  build: {
    outDir: 'dist',
    sourcemap: true,
    rollupOptions: {
      output: {
        // Split the heavy, rarely-changing libraries out of the app bundle so a code change
        // does not invalidate the cache for React Flow and Monaco, which together dwarf it.
        manualChunks: {
          react: ['react', 'react-dom', 'react-router-dom'],
          mui: ['@mui/material', '@mui/icons-material', '@mui/x-data-grid'],
          flow: ['@xyflow/react'],
          editor: ['@monaco-editor/react'],
        },
      },
    },
  },
})
