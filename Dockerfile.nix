# Nix builder stage
FROM nixos/nix:latest AS builder

# Copy our source and setup working directory
COPY . /tmp/build
WORKDIR /tmp/build

# Build our Nix application
RUN nix \
    --extra-experimental-features "nix-command flakes" \
    --option filter-syscalls false \
    build

# Copy the Nix store closure - all dependencies our app needs
RUN mkdir /tmp/nix-store-closure
RUN cp -R $(nix-store -qR result/) /tmp/nix-store-closure

# Final debugging-enabled image with Alpine Linux
FROM alpine:latest

# Install debugging tools and utilities
RUN apk --no-cache add \
    ca-certificates \
    bash \
    coreutils \
    util-linux \
    procps \
    net-tools \
    curl \
    wget \
    vim \
    nano \
    htop \
    strace \
    lsof \
    tree \
    file \
    findutils \
    grep \
    sed \
    gawk \
    jq \
    && rm -rf /var/cache/apk/*

WORKDIR /app

# Copy the Nix store closure and our built application
COPY --from=builder /tmp/nix-store-closure /nix/store
COPY --from=builder /tmp/build/result /app

# Set SSL certificate path
ENV SSL_CERT_FILE=/etc/ssl/certs/ca-certificates.crt

# Create useful directories for debugging
RUN mkdir -p /app/logs /app/data /app/tmp

# Set the entrypoint
CMD ["/app/bin/cdp-pipeline-workflow"]