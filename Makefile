.PHONY: lint fmt check test build

lint:
	golangci-lint run ./...

fmt:
	go fmt ./...

vet:
	go vet ./...

check: vet lint

test:
	go test ./...

build:
	go build ./...
