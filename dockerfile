# Build stage
FROM golang:1.22.5 AS builder

WORKDIR /app

# Install required packages using apt-get
RUN apt-get update && \
    apt-get install -y \
    git \
    make \
    build-essential \
    libzmq3-dev \
    libczmq-dev \
    libsodium-dev \
    pkg-config \
    gcc \
    libc-dev \
    cmake \
    python3 \
    libssl-dev

# Copy go mod files first to leverage caching
COPY go.mod go.sum ./
RUN go mod download

# Copy source code
COPY . ./

# Build the application with CGO enabled and dynamic linking
RUN CGO_ENABLED=1 GOOS=linux go build -o obsrvr-flow-pipeline

# Final stage
FROM ubuntu:22.04

WORKDIR /app

# Install runtime dependencies
RUN apt-get update && apt-get install -y \
    libzmq5 \
    libczmq4 \
    libsodium23 \
    ca-certificates \
    tzdata \
    && rm -rf /var/lib/apt/lists/*

# Create non-root user
RUN useradd -m -s /bin/bash appuser
# Copy binary from builder
COPY --from=builder /app/obsrvr-flow-pipeline /usr/local/bin/

# Set ownership
RUN chown -R appuser:appuser /usr/local/bin/obsrvr-flow-pipeline

# Switch to non-root user
USER appuser

# Command to run
ENTRYPOINT ["/usr/local/bin/obsrvr-flow-pipeline"]

