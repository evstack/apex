# Apex — Celestia namespace indexer

version := `git describe --tags --always --dirty 2>/dev/null || echo "dev"`
ldflags := "-s -w -X main.version=" + version

# Build the binary
build:
    go build -trimpath -ldflags '{{ldflags}}' -o bin/apex ./cmd/apex

# Run all tests with race detection
test:
    go test -race -count=1 -timeout 5m ./...

# Run linter
lint:
    golangci-lint run ./...

# Format code
fmt:
    gofumpt -w .

# Run the indexer
run *args: build
    ./bin/apex {{args}}

# Remove build artifacts
clean:
    rm -rf bin/

# Tidy dependencies
tidy:
    go mod tidy

# Verify go.mod/go.sum are tidy (for CI)
tidy-check:
    go mod tidy
    git diff --exit-code go.mod go.sum

# Run all checks (CI equivalent)
check: tidy-check lint test build
