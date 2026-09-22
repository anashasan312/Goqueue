# GoQueue — developer entry points.
#
# Every command a contributor needs lives here, so nobody has to reconstruct a
# twelve-flag `go test` invocation from memory or from CI logs.

SHELL        := /bin/bash
BINARY       := goqueue
BUILD_DIR    := ./bin
CMD_DIR      := ./cmd
CONFIG_PATH  ?= ./config/config.local.yaml
COMPOSE      := docker compose -f deployment/docker-compose.yml

# Version metadata is stamped into the binary at link time, so a running
# process can be traced back to the exact commit that produced it.
VERSION      ?= $(shell git describe --tags --always --dirty 2>/dev/null || echo dev)
COMMIT       ?= $(shell git rev-parse --short HEAD 2>/dev/null || echo unknown)
BUILD_TIME   := $(shell date -u +%Y-%m-%dT%H:%M:%SZ)
LDFLAGS      := -s -w -X main.version=$(VERSION) -X main.commit=$(COMMIT) -X main.buildTime=$(BUILD_TIME)

.DEFAULT_GOAL := help

# ---------------------------------------------------------------------------
# Build
# ---------------------------------------------------------------------------

.PHONY: deps
deps: ## Download dependencies and generate go.sum (run this first after cloning)
	@echo "==> resolving dependencies"
	@go mod tidy
	@go mod download
	@echo "==> go.sum is ready"

.PHONY: build
build: ## Build the binary into ./bin
	@echo "==> building $(BINARY) $(VERSION)"
	@mkdir -p $(BUILD_DIR)
	@go build -trimpath -ldflags "$(LDFLAGS)" -o $(BUILD_DIR)/$(BINARY) $(CMD_DIR)
	@echo "==> $(BUILD_DIR)/$(BINARY)"

.PHONY: run
run: ## Run the service against the local config
	@GOQUEUE_CONFIG_PATH=$(CONFIG_PATH) go run $(CMD_DIR)

.PHONY: clean
clean: ## Remove build artefacts and coverage output
	@rm -rf $(BUILD_DIR) coverage.out coverage.html
	@echo "==> cleaned"

# ---------------------------------------------------------------------------
# Code generation
# ---------------------------------------------------------------------------

.PHONY: wire
wire: ## Regenerate the dependency injection container
	@echo "==> running wire"
	@go run github.com/google/wire/cmd/wire ./pkg/di
	@echo "==> pkg/di/wire_gen.go regenerated"

# ---------------------------------------------------------------------------
# Quality
# ---------------------------------------------------------------------------

.PHONY: fmt
fmt: ## Format all Go source
	@gofmt -s -w .
	@echo "==> formatted"

.PHONY: vet
vet: ## Run go vet
	@go vet ./...

.PHONY: lint
lint: ## Run golangci-lint (install: make tools)
	@golangci-lint run ./...

.PHONY: tidy
tidy: ## Tidy and verify the module graph
	@go mod tidy
	@go mod verify

# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------

.PHONY: test
test: ## Run unit tests (no external dependencies required)
	@go test -race -count=1 ./...

.PHONY: test-integration
test-integration: ## Run integration tests against a live Redis (set REDIS_ADDR to override)
	@echo "==> integration tests require Redis at $${REDIS_ADDR:-127.0.0.1:6379}"
	@GOQUEUE_TEST_REDIS=1 go test -race -count=1 -tags=integration ./test/...

.PHONY: cover
cover: ## Run tests with coverage and open the HTML report
	@go test -race -count=1 -coverprofile=coverage.out -covermode=atomic ./...
	@go tool cover -func=coverage.out | tail -1
	@go tool cover -html=coverage.out -o coverage.html
	@echo "==> coverage.html"

.PHONY: validate
validate: fmt tidy wire vet test ## Full pre-push check
	@echo "==> validate passed"

# ---------------------------------------------------------------------------
# Local stack
# ---------------------------------------------------------------------------

.PHONY: env-up
env-up: ## Start Redis only, for running the service from source
	@$(COMPOSE) up -d redis
	@echo "==> redis is up on 127.0.0.1:6379"

.PHONY: env-down
env-down: ## Stop Redis
	@$(COMPOSE) stop redis

.PHONY: stack-up
stack-up: ## Start the full stack: service, Redis, Prometheus, Grafana
	@$(COMPOSE) up -d --build
	@echo ""
	@echo "  dashboard   http://localhost:9090/dashboard"
	@echo "  api         http://localhost:8080/api/v1"
	@echo "  metrics     http://localhost:9090/metrics"
	@echo "  prometheus  http://localhost:9091"
	@echo "  grafana     http://localhost:3000  (admin/admin)"
	@echo ""

.PHONY: stack-down
stack-down: ## Stop the full stack
	@$(COMPOSE) down

.PHONY: stack-clean
stack-clean: ## Stop the full stack and delete its volumes
	@$(COMPOSE) down -v

.PHONY: logs
logs: ## Tail the service logs
	@$(COMPOSE) logs -f goqueue

# ---------------------------------------------------------------------------
# Demo
# ---------------------------------------------------------------------------

.PHONY: seed
seed: ## Enqueue a spread of demo jobs against a running service
	@./scripts/seed.sh

.PHONY: load
load: ## Generate continuous load against a running service
	@./scripts/load.sh

# ---------------------------------------------------------------------------
# Tools
# ---------------------------------------------------------------------------

.PHONY: tools
tools: ## Install the development tools
	@go install github.com/google/wire/cmd/wire@latest
	@go install github.com/golangci/golangci-lint/cmd/golangci-lint@latest

.PHONY: help
help: ## Show this help
	@grep -hE '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) \
		| awk 'BEGIN {FS = ":.*?## "}; {printf "  \033[36m%-20s\033[0m %s\n", $$1, $$2}'
