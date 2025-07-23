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

# Final minimal image
FROM scratch
WORKDIR /app

# Copy the Nix store closure and our built application
COPY --from=builder /tmp/nix-store-closure /nix/store
COPY --from=builder /tmp/build/result /app

# Set the entrypoint
CMD ["/app/bin/cdp-pipeline-workflow"]