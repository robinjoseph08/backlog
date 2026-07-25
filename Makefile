.PHONY: build test test-race lint vet check ci

build:
	mise run build

test:
	go test ./...

test-race:
	go test -race ./...

lint:
	mise run lint

vet:
	go vet ./...

check:
	mise run check

ci:
	mise run ci
