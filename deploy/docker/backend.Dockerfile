# =============================================================================
# Backend image. Build context is the repository root.
#
#   docker build -f deploy/docker/backend.Dockerfile -t dmp-app .
#
# One image serves both roles; the profile picks which (ADR-0004).
# =============================================================================

# ----------------------------------------------------------------- build stage
FROM maven:3.9-eclipse-temurin-21 AS build

WORKDIR /build

# POMs first, so the dependency layer is cached and only re-resolves when a POM
# actually changes. Without this every source edit re-downloads the world.
COPY backend/pom.xml                                  ./pom.xml
COPY backend/dmp-bom/pom.xml                          ./dmp-bom/pom.xml
COPY backend/dmp-common/pom.xml                       ./dmp-common/pom.xml
COPY backend/dmp-domain/pom.xml                       ./dmp-domain/pom.xml
COPY backend/dmp-application/pom.xml                  ./dmp-application/pom.xml
COPY backend/dmp-persistence-postgres/pom.xml         ./dmp-persistence-postgres/pom.xml
COPY backend/dmp-persistence-mongo/pom.xml            ./dmp-persistence-mongo/pom.xml
COPY backend/dmp-connector-api/pom.xml                ./dmp-connector-api/pom.xml
COPY backend/dmp-connector-runtime/pom.xml            ./dmp-connector-runtime/pom.xml
COPY backend/connectors/dmp-connector-jdbc/pom.xml    ./connectors/dmp-connector-jdbc/pom.xml
COPY backend/dmp-engine/pom.xml                       ./dmp-engine/pom.xml
COPY backend/apps/dmp-app/pom.xml                     ./apps/dmp-app/pom.xml

RUN mvn -B -q dependency:go-offline -DskipTests || true

COPY backend/ ./

# Tests are not run here. They need Docker for Testcontainers, which is not available
# inside a build container, and CI is where they belong anyway.
RUN mvn -B -q clean package -DskipTests

# --------------------------------------------------------------- runtime stage
FROM eclipse-temurin:21-jre-alpine AS runtime

# Never root. A connector executes user-supplied configuration against arbitrary
# systems; the blast radius of a mistake should not include the container.
RUN addgroup -S dmp && adduser -S dmp -G dmp

WORKDIR /app

COPY --from=build /build/apps/dmp-app/target/dmp-app-*.jar ./dmp-app.jar

# Where third-party connector jars are mounted. Each subdirectory is loaded by its
# own child-first classloader, so their dependencies cannot collide (ADR-0006).
RUN mkdir -p /app/plugins && chown -R dmp:dmp /app

USER dmp

EXPOSE 8080

ENV JAVA_TOOL_OPTIONS="\
    -XX:MaxRAMPercentage=75 \
    -XX:+UseG1GC \
    -XX:+ExitOnOutOfMemoryError \
    -Djava.security.egd=file:/dev/./urandom"

# MaxRAMPercentage rather than a fixed -Xmx: the JVM then respects whatever the
# container is given, so raising the memory limit actually raises the heap.
# ExitOnOutOfMemoryError is deliberate — a worker that has exhausted its heap should
# die and let its chunks be reclaimed by lease expiry, not limp along half-working.

HEALTHCHECK --interval=15s --timeout=5s --start-period=60s --retries=5 \
    CMD wget -qO- http://localhost:8080/actuator/health/readiness | grep -q UP || exit 1

ENTRYPOINT ["java", "-jar", "/app/dmp-app.jar"]
