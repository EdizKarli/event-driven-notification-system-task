.PHONY: build test run fmt

build:
	go build -o bin/ ./...

test:
	go test ./...

run:
	go run ./cmd/server

fmt:
	go fmt ./...
