#!/bin/bash
set -e

# Default values
CONFIG_FILE="${1:-config/base/pipeline_config.yaml}"
IMAGE_NAME="docker.io/withobsrvr/cdp-pipeline-dev:latest"

echo "Running CDP Pipeline with Docker container..."
echo "Config file: $CONFIG_FILE"
echo "Image: $IMAGE_NAME"

# Run the container with volume mounts
docker run --rm \
  -v "$(pwd)/config:/app/config" \
  -v "$(pwd)/output:/app/output" \
  -v "$(pwd)/data:/app/data" \
  -e "CONFIG_FILE=/app/config/$CONFIG_FILE" \
  "$IMAGE_NAME" \
  /app/bin/cdp-pipeline-workflow -config "/app/config/$CONFIG_FILE"