#!/bin/bash

# Default values
IMAGE_NAME="cdp-pipeline-dev"
CONFIG_DIR="$(pwd)/config"
DATA_DIR="$(pwd)/data"

# Help message
show_help() {
    echo "Usage: ./run-local.sh [OPTIONS] CONFIG_FILE"
    echo
    echo "Run CDP pipeline locally with different configurations"
    echo
    echo "Options:"
    echo "  -i, --image NAME     Docker image name (default: cdp-pipeline-dev)"
    echo "  -d, --data DIR       Data directory to mount (default: ./data)"
    echo "  -h, --help           Show this help message"
    echo
    echo "Example:"
    echo "  ./run-local.sh config/customer1.yaml"
    echo "  ./run-local.sh -i custom-image config/test.yaml"
}

# Parse arguments
while [[ $# -gt 0 ]]; do
    case $1 in
        -i|--image)
            IMAGE_NAME="$2"
            shift 2
            ;;
        -d|--data)
            DATA_DIR="$2"
            shift 2
            ;;
        -h|--help)
            show_help
            exit 0
            ;;
        *)
            CONFIG_FILE="$1"
            shift
            ;;
    esac
done

# Validate inputs
if [ -z "$CONFIG_FILE" ]; then
    echo "Error: Config file must be specified"
    show_help
    exit 1
fi

if [ ! -f "$CONFIG_FILE" ]; then
    echo "Error: Config file does not exist: $CONFIG_FILE"
    exit 1
fi

# Create data directory if it doesn't exist
mkdir -p "$DATA_DIR"

# Run the container
docker run -it --rm \
    -v "$(realpath "$CONFIG_FILE"):/app/config/local.yaml:ro" \
    -v "$(realpath "$DATA_DIR"):/app/data" \
    -e LOCAL_CONFIG=/app/config/local.yaml \
    -e DEV_MODE=true \
    "$IMAGE_NAME"