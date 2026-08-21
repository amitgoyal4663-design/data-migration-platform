# =============================================================================
# Console image. Build context is the repository root.
#
#   docker build -f deploy/docker/console.Dockerfile -t dmp-console .
#
# nginx serves the built assets and proxies /api to the backend, so the console
# and the API share an origin. That means no CORS configuration and no build-time
# API URL — the same image works in every environment.
# =============================================================================

# ----------------------------------------------------------------- build stage
FROM node:22-alpine AS build

WORKDIR /build

# Manifest first so the dependency layer survives source edits.
COPY frontend/dmp-console/package.json frontend/dmp-console/package-lock.json* ./
RUN npm install --no-audit --no-fund

COPY frontend/dmp-console/ ./
RUN npm run build

# --------------------------------------------------------------- runtime stage
FROM nginx:1.27-alpine AS runtime

COPY --from=build /build/dist /usr/share/nginx/html
COPY deploy/docker/nginx.conf /etc/nginx/conf.d/default.conf

EXPOSE 80

HEALTHCHECK --interval=15s --timeout=3s --retries=3 \
    CMD wget -qO- http://localhost/ >/dev/null || exit 1

CMD ["nginx", "-g", "daemon off;"]
