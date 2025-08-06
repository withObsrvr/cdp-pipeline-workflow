#!/bin/sh
# FlowCTL Installer Script
# Usage: curl -sSL https://get.flowctl.dev | sh

set -e

# Configuration
FLOWCTL_VERSION="${FLOWCTL_VERSION:-latest}"
FLOWCTL_INSTALL_DIR="${FLOWCTL_INSTALL_DIR:-$HOME/.flowctl}"
GITHUB_REPO="withObsrvr/cdp-pipeline-workflow"
BINARY_NAME="flowctl"

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

# Helper functions
info() {
    printf "${GREEN}[INFO]${NC} %s\n" "$1"
}

warn() {
    printf "${YELLOW}[WARN]${NC} %s\n" "$1"
}

error() {
    printf "${RED}[ERROR]${NC} %s\n" "$1"
    exit 1
}

# Detect OS and Architecture
detect_platform() {
    OS=$(uname -s | tr '[:upper:]' '[:lower:]')
    ARCH=$(uname -m)
    
    case "$OS" in
        linux) 
            OS="linux" 
            ;;
        darwin) 
            OS="darwin" 
            ;;
        mingw*|msys*|cygwin*) 
            error "Windows detected. Please use install.ps1 or WSL"
            ;;
        *) 
            error "Unsupported OS: $OS" 
            ;;
    esac
    
    case "$ARCH" in
        x86_64|amd64) 
            ARCH="amd64" 
            ;;
        aarch64|arm64) 
            ARCH="arm64" 
            ;;
        armv7l|armv6l)
            warn "32-bit ARM detected. This may not be supported."
            ARCH="arm"
            ;;
        *) 
            error "Unsupported architecture: $ARCH" 
            ;;
    esac
    
    PLATFORM="${OS}-${ARCH}"
    info "Detected platform: $PLATFORM"
}

# Check for required tools
check_requirements() {
    if ! command -v curl >/dev/null 2>&1 && ! command -v wget >/dev/null 2>&1; then
        error "curl or wget is required but not installed."
    fi
}

# Get latest version from GitHub
get_latest_version() {
    if [ "$FLOWCTL_VERSION" = "latest" ]; then
        info "Fetching latest version..."
        LATEST_URL="https://api.github.com/repos/${GITHUB_REPO}/releases/latest"
        
        if command -v curl >/dev/null 2>&1; then
            FLOWCTL_VERSION=$(curl -s "$LATEST_URL" | grep '"tag_name":' | sed -E 's/.*"([^"]+)".*/\1/')
        else
            FLOWCTL_VERSION=$(wget -qO- "$LATEST_URL" | grep '"tag_name":' | sed -E 's/.*"([^"]+)".*/\1/')
        fi
        
        if [ -z "$FLOWCTL_VERSION" ]; then
            error "Could not determine latest version"
        fi
        
        info "Latest version: $FLOWCTL_VERSION"
    fi
}

# Download binary
download_flowctl() {
    DOWNLOAD_URL="https://github.com/${GITHUB_REPO}/releases/download/${FLOWCTL_VERSION}/${BINARY_NAME}-${PLATFORM}"
    
    # Add .exe extension for Windows binaries
    if [ "$OS" = "windows" ]; then
        DOWNLOAD_URL="${DOWNLOAD_URL}.exe"
    fi
    
    info "Downloading flowctl ${FLOWCTL_VERSION} for ${PLATFORM}..."
    info "URL: $DOWNLOAD_URL"
    
    # Create temporary directory
    TMP_DIR=$(mktemp -d)
    TMP_FILE="$TMP_DIR/$BINARY_NAME"
    
    # Download file
    if command -v curl >/dev/null 2>&1; then
        if ! curl -fsSL "$DOWNLOAD_URL" -o "$TMP_FILE"; then
            error "Failed to download flowctl. Please check if the release exists."
        fi
    else
        if ! wget -qO "$TMP_FILE" "$DOWNLOAD_URL"; then
            error "Failed to download flowctl. Please check if the release exists."
        fi
    fi
    
    # Verify download
    if [ ! -f "$TMP_FILE" ] || [ ! -s "$TMP_FILE" ]; then
        error "Downloaded file is empty or missing"
    fi
    
    # Move to install directory
    mkdir -p "$FLOWCTL_INSTALL_DIR/bin"
    mv "$TMP_FILE" "$FLOWCTL_INSTALL_DIR/bin/$BINARY_NAME"
    chmod +x "$FLOWCTL_INSTALL_DIR/bin/$BINARY_NAME"
    
    # Cleanup
    rm -rf "$TMP_DIR"
    
    info "Binary installed to: $FLOWCTL_INSTALL_DIR/bin/$BINARY_NAME"
}

# Download and verify checksums
verify_checksum() {
    CHECKSUM_URL="https://github.com/${GITHUB_REPO}/releases/download/${FLOWCTL_VERSION}/checksums.txt"
    
    info "Verifying checksum..."
    
    TMP_CHECKSUM=$(mktemp)
    if command -v curl >/dev/null 2>&1; then
        curl -fsSL "$CHECKSUM_URL" -o "$TMP_CHECKSUM" 2>/dev/null || warn "Checksums not available"
    else
        wget -qO "$TMP_CHECKSUM" "$CHECKSUM_URL" 2>/dev/null || warn "Checksums not available"
    fi
    
    if [ -f "$TMP_CHECKSUM" ] && [ -s "$TMP_CHECKSUM" ]; then
        EXPECTED_SUM=$(grep "${BINARY_NAME}-${PLATFORM}" "$TMP_CHECKSUM" | cut -d' ' -f1)
        if [ -n "$EXPECTED_SUM" ]; then
            ACTUAL_SUM=$(sha256sum "$FLOWCTL_INSTALL_DIR/bin/$BINARY_NAME" | cut -d' ' -f1)
            if [ "$EXPECTED_SUM" = "$ACTUAL_SUM" ]; then
                info "Checksum verified"
            else
                error "Checksum verification failed"
            fi
        fi
    fi
    
    rm -f "$TMP_CHECKSUM"
}

# Add to PATH
configure_path() {
    FLOWCTL_BIN="$FLOWCTL_INSTALL_DIR/bin"
    
    # Check if already in PATH
    if echo "$PATH" | grep -q "$FLOWCTL_BIN"; then
        info "flowctl is already in your PATH"
        return
    fi
    
    info "Configuring PATH..."
    
    # Detect shell and profile file
    SHELL_NAME=$(basename "$SHELL")
    case "$SHELL_NAME" in
        bash)
            PROFILE_FILES="$HOME/.bashrc $HOME/.bash_profile"
            ;;
        zsh)
            PROFILE_FILES="$HOME/.zshrc"
            ;;
        fish)
            PROFILE_FILES="$HOME/.config/fish/config.fish"
            ;;
        *)
            PROFILE_FILES="$HOME/.profile"
            ;;
    esac
    
    # Find the first existing profile file
    SHELL_PROFILE=""
    for file in $PROFILE_FILES; do
        if [ -f "$file" ]; then
            SHELL_PROFILE="$file"
            break
        fi
    done
    
    # Create profile file if none exists
    if [ -z "$SHELL_PROFILE" ]; then
        SHELL_PROFILE="$HOME/.profile"
        touch "$SHELL_PROFILE"
    fi
    
    # Add to PATH
    if [ -n "$SHELL_PROFILE" ]; then
        echo "" >> "$SHELL_PROFILE"
        echo "# FlowCTL" >> "$SHELL_PROFILE"
        
        if [ "$SHELL_NAME" = "fish" ]; then
            echo "set -gx PATH \$PATH $FLOWCTL_BIN" >> "$SHELL_PROFILE"
        else
            echo "export PATH=\"\$PATH:$FLOWCTL_BIN\"" >> "$SHELL_PROFILE"
        fi
        
        info "Added flowctl to PATH in $SHELL_PROFILE"
        warn "Run 'source $SHELL_PROFILE' or start a new shell to use flowctl"
    fi
    
    # Try to create symlink in system directories (optional)
    for dir in /usr/local/bin /opt/bin; do
        if [ -d "$dir" ] && [ -w "$dir" ]; then
            ln -sf "$FLOWCTL_BIN/$BINARY_NAME" "$dir/$BINARY_NAME" 2>/dev/null && \
                info "Created symlink in $dir" && \
                break
        fi
    done
}

# Print usage information
print_usage() {
    cat << EOF

${GREEN}FlowCTL installed successfully!${NC}

To get started:

1. Add flowctl to your PATH (if not done automatically):
   ${YELLOW}export PATH="\$PATH:$FLOWCTL_INSTALL_DIR/bin"${NC}

2. Verify installation:
   ${YELLOW}flowctl version${NC}

3. Run your first pipeline:
   ${YELLOW}flowctl run "captive-core://localhost | latest-ledger | stdout"${NC}

4. View help:
   ${YELLOW}flowctl run --help${NC}

For more information, visit:
https://github.com/${GITHUB_REPO}

EOF
}

# Cleanup function
cleanup() {
    if [ -n "$TMP_DIR" ] && [ -d "$TMP_DIR" ]; then
        rm -rf "$TMP_DIR"
    fi
}

# Set trap for cleanup
trap cleanup EXIT

# Main installation flow
main() {
    echo ""
    echo "Installing FlowCTL..."
    echo ""
    
    check_requirements
    detect_platform
    get_latest_version
    download_flowctl
    verify_checksum
    configure_path
    
    # Test installation
    if "$FLOWCTL_INSTALL_DIR/bin/$BINARY_NAME" version >/dev/null 2>&1; then
        VERSION_OUTPUT=$("$FLOWCTL_INSTALL_DIR/bin/$BINARY_NAME" version 2>&1)
        info "Installation successful!"
        info "Version: $VERSION_OUTPUT"
        print_usage
    else
        error "Installation completed but flowctl is not working properly"
    fi
}

# Run main function
main "$@"