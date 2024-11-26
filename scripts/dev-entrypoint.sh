#!/bin/bash
set -e

# Function to setup GCP credentials
setup_gcp_auth() {
    echo "Setting up GCP authentication..."
    
    # Check for directly mounted credentials
    if [ -n "$GOOGLE_APPLICATION_CREDENTIALS" ] && [ -f "$GOOGLE_APPLICATION_CREDENTIALS" ]; then
        echo "Using mounted Google credentials from: $GOOGLE_APPLICATION_CREDENTIALS"
        return
    fi

    # Check for credentials in environment variable
    if [ -n "$GOOGLE_CREDENTIALS_JSON" ]; then
        echo "Using Google credentials from environment variable"
        echo "$GOOGLE_CREDENTIALS_JSON" > /app/secrets/google-credentials.json
        export GOOGLE_APPLICATION_CREDENTIALS=/app/secrets/google-credentials.json
        return
    fi

    # Check for Vault credentials if VAULT_ADDR is set
    if [ -n "$VAULT_ADDR" ] && [ -n "$VAULT_TOKEN" ]; then
        echo "Fetching Google credentials from Vault..."
        CREDENTIALS=$(curl -s -H "X-Vault-Token: $VAULT_TOKEN" \
            "$VAULT_ADDR/v1/secret/data/gcp/credentials" | jq -r '.data.data.credentials')
        echo "$CREDENTIALS" > /app/secrets/google-credentials.json
        export GOOGLE_APPLICATION_CREDENTIALS=/app/secrets/google-credentials.json
        return
    fi

    echo "Warning: No Google credentials found"
}

# Function to validate yaml config
validate_config() {
    local config_file=$1
    if ! yq eval '.' "$config_file" > /dev/null 2>&1; then
        echo "Error: Invalid YAML in config file: $config_file"
        exit 1
    fi
}

# Function to setup development environment
setup_dev_env() {
    echo "Setting up development environment..."
    
    # Setup GCP authentication
    setup_gcp_auth
    
    # Create symbolic link to mounted config if specified
    if [ -n "$LOCAL_CONFIG" ] && [ -f "$LOCAL_CONFIG" ]; then
        echo "Using local config file: $LOCAL_CONFIG"
        validate_config "$LOCAL_CONFIG"
        export CONFIG_PATH="$LOCAL_CONFIG"
    else
        echo "Using default config from: $CONFIG_PATH"
    fi

    # Set up development-specific environment variables
    if [ "$DEV_MODE" = "true" ]; then
        echo "Development mode enabled"
        export LOG_LEVEL=${LOG_LEVEL:-debug}
    fi
}

# Main execution
echo "Starting CDP Pipeline in development mode..."
setup_dev_env

echo "Using configuration from: $CONFIG_PATH"
echo "Current configuration:"
cat "$CONFIG_PATH"

# Execute the pipeline with any additional arguments
exec /app/cdp-pipeline-workflow -config="$CONFIG_PATH" "$@"