.PHONY: build test test-race run fmt lint up down logs

build:
	go build -o bin/ ./...

test:
	go test ./...

test-race:
	go test -race ./...

run:
	go run ./cmd/server

fmt:
	go fmt ./...

lint:
	golangci-lint run ./...

up:
	docker compose up -d --build

down:
	docker compose down

logs:
	docker compose logs -f
