.PHONY: build test test-race vet check

build:
	go build -o backlog ./cmd/backlog

test:
	go test ./...

test-race:
	go test -race ./...

vet:
	go vet ./...

check: test test-race vet build
