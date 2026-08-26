# =============================================================================
# One entry point for the whole monorepo.
# =============================================================================

# Compose ships two ways: as a `docker compose` subcommand (v2, current) and as a
# standalone `docker-compose` binary (v1, still what a lot of installs have). Asking
# Docker for the subcommand it does not have produces `unknown shorthand flag: 'f'`,
# because the -f is then read as a flag to `docker` itself -- an error that names
# nothing a person could act on. So the choice is made here, once.
DOCKER_COMPOSE := $(shell docker compose version >/dev/null 2>&1 && echo "docker compose" \
                     || (command -v docker-compose >/dev/null 2>&1 && echo "docker-compose"))

COMPOSE  := $(DOCKER_COMPOSE) -f deploy/compose/docker-compose.yml
MVN      := cd backend && ./mvnw
CONSOLE  := cd frontend/dmp-console && npm

# Checked before any target that needs it, so a missing Compose says so in one line
# rather than failing somewhere inside a recipe.
.PHONY: require-compose
require-compose:
	@test -n "$(DOCKER_COMPOSE)" || { \
	  echo ""; \
	  echo "  Docker Compose was not found."; \
	  echo ""; \
	  echo "  Neither 'docker compose' nor 'docker-compose' runs on this machine."; \
	  echo "  Install Docker Desktop, which includes it:"; \
	  echo "      https://www.docker.com/products/docker-desktop/"; \
	  echo ""; \
	  echo "  If Docker Desktop is installed, it is probably not started yet."; \
	  echo ""; \
	  exit 1; }

.DEFAULT_GOAL := help
.PHONY: help require-compose up stack update seed down reset logs ps build test verify run events console clean

help: ## Show available targets
	@grep -hE '^[a-zA-Z_-]+:.*?## ' $(MAKEFILE_LIST) \
		| awk 'BEGIN {FS = ":.*?## "}; {printf "  \033[36m%-10s\033[0m %s\n", $$1, $$2}'

# --------------------------------------------------------------- running things

up: require-compose ## Start infrastructure only (run the app yourself from an IDE)
	$(COMPOSE) up -d postgres mongo kafka redis
	@echo "Waiting for the MongoDB replica set to elect a primary..."
	@$(COMPOSE) up mongo-init
	@echo ""
	@echo "  Postgres  localhost:5432   dmp / dmp"
	@echo "  MongoDB   localhost:27018  replica set rs0"
	@echo "  Kafka     localhost:9092"
	@echo "  Redis     localhost:6379"

stack: require-compose ## Start EVERYTHING, including the app and the console. Only needs Docker.
	# mongo-init before the rest: the replica set has to elect a primary before anything
	# connects to it, and a service that starts first simply fails and restarts in a loop.
	$(COMPOSE) up -d mongo
	@$(COMPOSE) up mongo-init
	$(COMPOSE) --profile full up -d --build
	# Waits for the API to be healthy, then adds the connections and pipelines a new
	# machine has none of. Idempotent, so a second `make stack` changes nothing.
	@$(COMPOSE) --profile full up seed
	@echo ""
	@echo "  Console        http://localhost:3000"
	@echo "  API docs       http://localhost:8080/swagger-ui.html"
	@echo "  Health         http://localhost:8080/actuator/health"
	@echo "  Search/logs    http://localhost:5601"
	@echo "  Mock warehouse http://localhost:8099"
	@echo ""
	@echo "  The API waits for Postgres, Mongo, OpenSearch and the Kafka topics."
	@echo "  'make ps' shows dmp-app as healthy when it is genuinely ready."

update: ## Get the latest code and restart on it
	# The whole update in one target, because the alternative is a four-command recipe
	# somebody half-remembers. `git pull` alone changes nothing that is running: the
	# containers keep serving the code they were built from until they are rebuilt.
	git pull --ff-only
	$(MAKE) stack

seed: require-compose ## Add the sample connections and pipelines (safe to repeat)
	@$(COMPOSE) --profile full up seed

down: require-compose ## Stop everything, keeping data
	$(COMPOSE) --profile full down

reset: require-compose ## Stop everything and DELETE all local data
	$(COMPOSE) --profile full down -v

logs: require-compose ## Tail logs from every service
	$(COMPOSE) --profile full logs -f

ps: require-compose ## Show container status
	$(COMPOSE) --profile full ps

# -------------------------------------------------------------------- building

build: ## Compile backend and console without running tests
	$(MVN) -q clean install -DskipTests
	$(CONSOLE) install --silent && $(CONSOLE) run build

test: ## Unit tests only, no containers needed
	$(MVN) test

verify: ## Unit + integration tests (needs Docker for Testcontainers)
	$(MVN) verify

run: ## Run the backend locally with both roles active, against the compose stack
	# Not $(MVN): it expands to "cd backend && ./mvnw", so a variable prefix would bind
	# to the cd and never reach Maven. The compose stack publishes MongoDB on 27018 to
	# avoid colliding with a local one, and directConnection because the replica set
	# advertises its in-network name, which the host cannot resolve.
	cd backend && \
	DMP_MONGO_URI='mongodb://localhost:27018/dmp?directConnection=true' \
	DMP_EVENTS_KAFKA_ENABLED=true \
	DMP_KAFKA_BOOTSTRAP=localhost:9092 \
	./mvnw -pl apps/dmp-app spring-boot:run -Dspring-boot.run.profiles=all

events: require-compose ## Tail run events from Kafka (Ctrl-C to stop)
	docker exec -it dmp-kafka /opt/kafka/bin/kafka-console-consumer.sh \
		--bootstrap-server localhost:9092 \
		--topic dmp.run.events.v1 --from-beginning --property print.key=true

console: ## Run the console dev server against a local backend
	$(CONSOLE) run dev

clean: ## Remove build output
	$(MVN) -q clean
	rm -rf frontend/dmp-console/dist
