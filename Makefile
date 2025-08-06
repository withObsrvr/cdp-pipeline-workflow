# Makefile for CDP Pipeline Workflow

# Variables
BINARY_NAME=flowctl
GO=go
GOFLAGS=-v

# Version information
VERSION ?= $(shell git describe --tags --always --dirty 2>/dev/null || echo "dev")
COMMIT := $(shell git rev-parse --short HEAD 2>/dev/null || echo "unknown")
BUILD_DATE := $(shell date -u '+%Y-%m-%d_%H:%M:%S')

# Build flags
LDFLAGS := -X main.Version=$(VERSION) \
           -X main.GitCommit=$(COMMIT) \
           -X main.BuildDate=$(BUILD_DATE)

# Default target
.PHONY: all
all: build

# Build the flowctl binary
.PHONY: build
build:
	CGO_ENABLED=1 $(GO) build $(GOFLAGS) -ldflags "$(LDFLAGS)" -o $(BINARY_NAME)

# Install the binary to /usr/local/bin
.PHONY: install
install: build
	cp $(BINARY_NAME) /usr/local/bin/

# Clean build artifacts
.PHONY: clean
clean:
	rm -f $(BINARY_NAME)

# Legacy compatibility target
.PHONY: cdp-pipeline-workflow
cdp-pipeline-workflow: build
	@echo "Note: Binary is now named 'flowctl'"

# Run tests
.PHONY: test
test:
	$(GO) test ./...

# Format code
.PHONY: fmt
fmt:
	$(GO) fmt ./...

# Run linters
.PHONY: lint
lint:
	golangci-lint run

# Get dependencies
.PHONY: deps
deps:
	$(GO) mod download
	$(GO) mod tidy

# Build for multiple platforms
.PHONY: build-all
build-all:
	GOOS=linux GOARCH=amd64 CGO_ENABLED=1 $(GO) build -ldflags "$(LDFLAGS)" -o $(BINARY_NAME)-linux-amd64
	GOOS=darwin GOARCH=amd64 CGO_ENABLED=1 $(GO) build -ldflags "$(LDFLAGS)" -o $(BINARY_NAME)-darwin-amd64
	GOOS=darwin GOARCH=arm64 CGO_ENABLED=1 $(GO) build -ldflags "$(LDFLAGS)" -o $(BINARY_NAME)-darwin-arm64

# Run the pipeline with a config file (for testing)
.PHONY: run
run:
	./$(BINARY_NAME) run config/base/pipeline_config.yaml

# Run with legacy interface (for testing)
.PHONY: run-legacy
run-legacy:
	./$(BINARY_NAME) -config config/base/pipeline_config.yaml

# Help target
.PHONY: help
help:
	@echo "Available targets:"
	@echo "  build        - Build the flowctl binary"
	@echo "  install      - Install flowctl to /usr/local/bin"
	@echo "  clean        - Remove build artifacts"
	@echo "  test         - Run tests"
	@echo "  fmt          - Format code"
	@echo "  lint         - Run linters"
	@echo "  deps         - Download and tidy dependencies"
	@echo "  build-all    - Build for multiple platforms"
	@echo "  run          - Run flowctl with new interface"
	@echo "  run-legacy   - Run flowctl with legacy interface"
	@echo "  help         - Show this help message"