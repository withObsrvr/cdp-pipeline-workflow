#!/bin/bash
set -e

IMAGE_NAME="obsrvr-flow-pipeline"
TAG="${1:-latest}"
USERNAME="${2:-withobsrvr}"

echo "Building production container with Docker..."
docker build -f Dockerfile.prod -t "docker.io/$USERNAME/$IMAGE_NAME:$TAG" .

echo "Pushing to DockerHub..."
docker push "docker.io/$USERNAME/$IMAGE_NAME:$TAG"

echo "Successfully pushed docker.io/$USERNAME/$IMAGE_NAME:$TAG"