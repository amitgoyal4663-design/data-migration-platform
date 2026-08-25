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

# 127.0.0.1, not localhost. wget resolves localhost to ::1 first and nginx listens on IPv4 only,
# so the check failed against a server that was serving perfectly — the container reported
# unhealthy for its whole life while every request to it succeeded. That matters more than it
# sounds: the quickstart tells people to judge readiness by `make ps`, so the first thing somebody
# setting this up saw was a permanently broken console that was not broken.
HEALTHCHECK --interval=15s --timeout=3s --retries=3 \
    CMD wget -qO- http://127.0.0.1/ >/dev/null || exit 1

CMD ["nginx", "-g", "daemon off;"]
