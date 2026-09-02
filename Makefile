.DEFAULT_GOAL := help

BIN     := bin/mediaservice
PKG     := ./...
MAIN    := ./cmd/mediaservice

GOLANGCI_LINT_VERSION := v2.1.6
GOLANGCI_LINT         := $(shell go env GOPATH)/bin/golangci-lint

.PHONY: help
help: ## Показать доступные команды
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) \
		| awk 'BEGIN {FS = ":.*?## "}; {printf "  \033[36m%-12s\033[0m %s\n", $$1, $$2}'

.PHONY: build
build: ## Собрать бинарь в bin/
	go build -o $(BIN) $(MAIN)

.PHONY: lint
lint: ## Прогнать линтер и go vet
	go vet $(PKG)
	@$(GOLANGCI_LINT) --version 2>/dev/null | grep -q "$(GOLANGCI_LINT_VERSION)" || \
		go install github.com/golangci/golangci-lint/v2/cmd/golangci-lint@$(GOLANGCI_LINT_VERSION)
	$(GOLANGCI_LINT) run

.PHONY: test
test: ## Прогнать тесты с race-детектором
	go test -race -count=1 $(PKG)

.PHONY: test-cover
test-cover: ## Тесты с отчётом о покрытии
	go test -race -count=1 -coverprofile=coverage.out $(PKG)
	go tool cover -func=coverage.out | tail -1

.PHONY: run
run: ## Запустить сервис локально
	go run $(MAIN)

.PHONY: tidy
tidy: ## Привести go.mod в порядок
	go mod tidy

.PHONY: proto
proto: ## Сгенерировать Go stubs из proto (идемпотентно)
	@which buf > /dev/null || (echo "ERROR: buf не установлен. См. README.md#proto-toolchain" && exit 1)
	@which protoc-gen-go > /dev/null || (echo "ERROR: protoc-gen-go не установлен. Запусти: go install google.golang.org/protobuf/cmd/protoc-gen-go@latest" && exit 1)
	@which protoc-gen-go-grpc > /dev/null || (echo "ERROR: protoc-gen-go-grpc не установлен. Запусти: go install google.golang.org/grpc/cmd/protoc-gen-go-grpc@latest" && exit 1)
	rm -f proto/media/v1/*.pb.go proto/media/v1/*_grpc.pb.go
	buf generate

.PHONY: proto-lint
proto-lint: ## Проверить proto на lint и breaking changes
	@which buf > /dev/null || (echo "ERROR: buf не установлен." && exit 1)
	buf dep update
	buf lint
	@BASE=$$(git merge-base HEAD main 2>/dev/null || echo main); \
	WT=$$(mktemp -d); \
	git worktree add -q $$WT $$BASE; \
	if [ ! -f $$WT/buf.lock ] && [ -f buf.lock ]; then cp buf.lock $$WT/buf.lock; fi; \
	(cd $$WT && buf dep update); \
	buf breaking --against $$WT; \
	git worktree remove -f $$WT

.PHONY: up
up: ## Поднять инфраструктуру и сервис
	docker compose up -d --build

.PHONY: down
down: ## Остановить всё
	docker compose down

.PHONY: logs
logs: ## Логи сервиса
	docker compose logs -f mediaservice

.PHONY: clean
clean: ## Убрать артефакты сборки
	rm -rf bin coverage.out
