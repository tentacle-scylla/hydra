.PHONY: lint fmt check test build release

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

# Release target - runs all validation before creating a release
# Usage: make release VERSION=v0.1.4
release: fmt vet lint test
	@echo "All checks passed!"
	@if [ -z "$(VERSION)" ]; then \
		echo "Error: VERSION not specified. Usage: make release VERSION=v0.1.4"; \
		exit 1; \
	fi
	@echo "Creating release $(VERSION)..."
	@if git diff --quiet; then \
		echo "Working directory is clean"; \
	else \
		echo "Error: Working directory has uncommitted changes."; \
		exit 1; \
	fi
	git tag $(VERSION)
	git push origin main --tags
	@echo "Release $(VERSION) created and pushed!"
