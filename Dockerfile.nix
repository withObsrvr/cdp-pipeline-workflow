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

# Final minimal image with CA certificates
FROM alpine:latest AS certs
RUN apk --no-cache add ca-certificates

FROM scratch
WORKDIR /app

# Copy CA certificates for TLS
COPY --from=certs /etc/ssl/certs/ca-certificates.crt /etc/ssl/certs/

# Copy the Nix store closure and our built application
COPY --from=builder /tmp/nix-store-closure /nix/store
COPY --from=builder /tmp/build/result /app

# Set SSL certificate path
ENV SSL_CERT_FILE=/etc/ssl/certs/ca-certificates.crt

# Set the entrypoint
CMD ["/app/bin/cdp-pipeline-workflow"]