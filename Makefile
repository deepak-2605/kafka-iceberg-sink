# Use docker from PATH, else Docker Desktop's bundled CLI (its /usr/local/bin
# symlinks are not always installed).
DOCKER  ?= $(shell command -v docker 2>/dev/null || echo /Applications/Docker.app/Contents/Resources/bin/docker)
COMPOSE := $(DOCKER) compose -f deploy/compose/docker-compose.yml
RECORDS ?= 1000000
TRUTH   ?= .run/ground-truth.json
TABLE   ?= sink.events

.PHONY: up down down-clean ps logs build test check-rest loadgen verify reset-truth

## Stack -------------------------------------------------------------------

up:            ## start Kafka, S3 (SeaweedFS) and the REST catalog; create topic and bucket
	$(COMPOSE) up -d --wait kafka s3 catalog
	$(COMPOSE) up kafka-init
	@echo "kafka localhost:9092 · s3 localhost:8333 · catalog localhost:8181"

down:          ## stop the stack, keep data
	$(COMPOSE) down

down-clean:    ## stop the stack and delete all data
	$(COMPOSE) down -v

ps:
	$(COMPOSE) ps -a

logs:
	$(COMPOSE) logs -f --tail=100

## Build and test ----------------------------------------------------------

build:
	go build -o bin/ ./cmd/...

test:          ## unit tests and week-1 iceberg-go checks on the SQLite catalog (no Docker)
	go test ./... -count=1

check-rest:    ## week-1 iceberg-go checks against the REST catalog in the stack
	KIS_REST=1 go test ./internal/committer -count=1 -v -run TestCheck

## Harness -----------------------------------------------------------------

loadgen: build ## produce RECORDS records, continuing from TRUTH
	@mkdir -p .run
	./bin/loadgen -records $(RECORDS) -truth $(TRUTH)

verify: build  ## check TABLE against TRUTH through DuckDB; exits 1 on any duplicate or loss
	./bin/verify -truth $(TRUTH) -table $(TABLE)

reset-truth:   ## forget the ground truth (do this together with down-clean)
	rm -f $(TRUTH)
