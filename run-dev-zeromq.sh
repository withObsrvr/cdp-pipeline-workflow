#!/bin/bash

CONFIG_FILE=${1:-"config/base/pipeline_config.yaml"}
GOOGLE_CREDS=${2:-"$HOME/.config/gcloud"}

# Ensure ZeroMQ port is available
if ! netstat -an | grep -q "127.0.0.1:5555.*LISTEN"; then
    echo "Warning: No listener found on port 5555"
    echo "Make sure your ZeroMQ subscriber is running"
    read -p "Continue anyway? (y/n) " -n 1 -r
    echo
    if [[ ! $REPLY =~ ^[Yy]$ ]]; then
        exit 1
    fi
fi

# Run the container
docker run -it --rm \
    --network host \
    -v "$(pwd)/config:/app/config:ro" \
    -v "$GOOGLE_CREDS:/app/.config/gcloud:ro" \
    -e GOOGLE_APPLICATION_CREDENTIALS="/app/.config/gcloud/application_default_credentials.json" \
    -e LOCAL_CONFIG="/app/$CONFIG_FILE" \
    -e DEV_MODE=true \
    -e LOG_LEVEL=debug \
    cdp-pipeline-dev


