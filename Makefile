.PHONY: build test run fmt up down logs

build:
	go build -o bin/ ./...

test:
	go test ./...

run:
	go run ./cmd/server

fmt:
	go fmt ./...

up:
	docker compose up -d --build

down:
	docker compose down

logs:
	docker compose logs -f
