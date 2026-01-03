.PHONY: lint fmt check test build

lint:
	golangci-lint run ./...

fmt:
	gofmt -s -w .

vet:
	go vet ./...

check: vet lint

test:
	go test ./...

build:
	go build ./...
