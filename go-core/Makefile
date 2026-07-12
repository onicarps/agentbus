.PHONY: test build tidy

export PATH := $(HOME)/.local/go/bin:$(PATH)

tidy:
	go mod tidy

test:
	go test ./...

build:
	mkdir -p bin
	go build -o bin/agentbus-go-serve ./cmd/agentbus-go-serve
	go build -o bin/agentbus-go-worker ./cmd/agentbus-go-worker
