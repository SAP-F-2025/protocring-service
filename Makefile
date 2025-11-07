.PHONY: help run build test clean docker-up docker-down fmt lint

help: ## Display this help screen
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | awk 'BEGIN {FS = ":.*?## "}; {printf "\033[36m%-20s\033[0m %s\n", $$1, $$2}'

run: ## Run the application
	go run cmd/server/main.go

build: ## Build the application
	go build -o bin/server cmd/server/main.go

test: ## Run tests
	go test -v -race -coverprofile=coverage.out ./...

coverage: test ## Run tests and show coverage
	go tool cover -html=coverage.out

clean: ## Clean build files
	rm -rf bin/
	rm -f coverage.out

docker-up: ## Start docker services
	docker compose up -d

docker-down: ## Stop docker services
	docker compose down

docker-logs: ## Show docker logs
	docker compose logs -f

fmt: ## Format code
	go fmt ./...

lint: ## Run linter
	golangci-lint run

deps: ## Download dependencies
	go mod download
	go mod tidy

migrate-install: ## Install golang-migrate CLI
	go install -tags 'postgres' github.com/golang-migrate/migrate/v4/cmd/migrate@latest

migrate-create: ## Create a new migration file (usage: make migrate-create name=create_users_table)
	migrate create -ext sql -dir migrations -seq $(name)

migrate-up: ## Run all database migrations up
	migrate -path migrations -database "postgresql://postgres:postgres@localhost:5432/protocring?sslmode=disable" up

migrate-down: ## Run all database migrations down
	migrate -path migrations -database "postgresql://postgres:postgres@localhost:5432/protocring?sslmode=disable" down

migrate-force: ## Force set migration version (usage: make migrate-force version=1)
	migrate -path migrations -database "postgresql://postgres:postgres@localhost:5432/protocring?sslmode=disable" force $(version)

migrate-version: ## Show current migration version
	migrate -path migrations -database "postgresql://postgres:postgres@localhost:5432/protocring?sslmode=disable" version

db-reset: docker-down docker-up ## Reset database (recreate containers)
	@echo "Waiting for database to be ready..."
	@sleep 3
	@$(MAKE) migrate-up
