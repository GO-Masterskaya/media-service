.DEFAULT_GOAL := help

BIN     := bin/mediaservice
PKG     := ./...
MAIN    := ./cmd/mediaservice

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
	golangci-lint run

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
proto: ## Сгенерировать код из proto
	protoc --go_out=. --go_opt=paths=source_relative \
		--go-grpc_out=. --go-grpc_opt=paths=source_relative \
		proto/media/v1/*.proto

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
