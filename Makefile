# =============================================================================
# One entry point for the whole monorepo.
# =============================================================================

COMPOSE  := docker compose -f deploy/compose/docker-compose.yml
MVN      := cd backend && ./mvnw
CONSOLE  := cd frontend/dmp-console && npm

.DEFAULT_GOAL := help
.PHONY: help up stack down reset logs ps build test verify run events console clean

help: ## Show available targets
	@grep -hE '^[a-zA-Z_-]+:.*?## ' $(MAKEFILE_LIST) \
		| awk 'BEGIN {FS = ":.*?## "}; {printf "  \033[36m%-10s\033[0m %s\n", $$1, $$2}'

# --------------------------------------------------------------- running things

up: ## Start infrastructure only (run the app yourself from an IDE)
	$(COMPOSE) up -d postgres mongo kafka redis
	@echo "Waiting for the MongoDB replica set to elect a primary..."
	@$(COMPOSE) up mongo-init
	@echo ""
	@echo "  Postgres  localhost:5432   dmp / dmp"
	@echo "  MongoDB   localhost:27017  replica set rs0"
	@echo "  Kafka     localhost:9092"
	@echo "  Redis     localhost:6379"

stack: ## Start EVERYTHING, including the app and the console
	$(COMPOSE) --profile full up -d --build
	@$(COMPOSE) up mongo-init
	@echo ""
	@echo "  Console   http://localhost:3000"
	@echo "  API docs  http://localhost:8080/swagger-ui.html"
	@echo "  Health    http://localhost:8080/actuator/health"

down: ## Stop everything, keeping data
	$(COMPOSE) --profile full down

reset: ## Stop everything and DELETE all local data
	$(COMPOSE) --profile full down -v

logs: ## Tail logs from every service
	$(COMPOSE) --profile full logs -f

ps: ## Show container status
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

events: ## Tail run events from Kafka (Ctrl-C to stop)
	docker exec -it dmp-kafka /opt/kafka/bin/kafka-console-consumer.sh \
		--bootstrap-server localhost:9092 \
		--topic dmp.run.events.v1 --from-beginning --property print.key=true

console: ## Run the console dev server against a local backend
	$(CONSOLE) run dev

clean: ## Remove build output
	$(MVN) -q clean
	rm -rf frontend/dmp-console/dist
