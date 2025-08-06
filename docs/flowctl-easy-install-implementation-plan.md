# FlowCTL Easy Installation Implementation Plan

## Overview
Implement a one-line installation process for flowctl that allows users to get started in under a minute, following the pattern: `curl -sSL https://get.flowctl.dev | sh`

## Goals
1. Zero-friction installation experience
2. Cross-platform support (Linux, macOS, Windows)
3. Automatic architecture detection
4. No prerequisites except curl/wget
5. Smart PATH configuration
6. Version management support

## Implementation Phases

### Phase 1: Build and Release Infrastructure

#### 1.1 Multi-Platform Builds
```yaml
# .github/workflows/release.yml
name: Release
on:
  push:
    tags:
      - 'v*'

jobs:
  build:
    strategy:
      matrix:
        include:
          - os: ubuntu-latest
            target: x86_64-unknown-linux-gnu
            asset_name: flowctl-linux-amd64
          - os: ubuntu-latest
            target: aarch64-unknown-linux-gnu
            asset_name: flowctl-linux-arm64
          - os: macos-latest
            target: x86_64-apple-darwin
            asset_name: flowctl-darwin-amd64
          - os: macos-latest
            target: aarch64-apple-darwin
            asset_name: flowctl-darwin-arm64
          - os: windows-latest
            target: x86_64-pc-windows-msvc
            asset_name: flowctl-windows-amd64.exe
```

#### 1.2 Binary Naming Convention
```
flowctl-<os>-<arch>
flowctl-linux-amd64
flowctl-linux-arm64
flowctl-darwin-amd64
flowctl-darwin-arm64
flowctl-windows-amd64.exe
```

### Phase 2: Installation Script

#### 2.1 Core Install Script (`install.sh`)
```bash
#!/bin/sh
set -e

# FlowCTL Installer Script
# Usage: curl -sSL https://get.flowctl.dev | sh

FLOWCTL_VERSION="${FLOWCTL_VERSION:-latest}"
FLOWCTL_INSTALL_DIR="${FLOWCTL_INSTALL_DIR:-$HOME/.flowctl}"
GITHUB_REPO="withObsrvr/cdp-pipeline-workflow"

# Detect OS and Architecture
detect_platform() {
    OS=$(uname -s | tr '[:upper:]' '[:lower:]')
    ARCH=$(uname -m)
    
    case "$OS" in
        linux) OS="linux" ;;
        darwin) OS="darwin" ;;
        mingw*|msys*|cygwin*) OS="windows" ;;
        *) echo "Unsupported OS: $OS"; exit 1 ;;
    esac
    
    case "$ARCH" in
        x86_64|amd64) ARCH="amd64" ;;
        aarch64|arm64) ARCH="arm64" ;;
        *) echo "Unsupported architecture: $ARCH"; exit 1 ;;
    esac
    
    PLATFORM="${OS}-${ARCH}"
}

# Download binary
download_flowctl() {
    if [ "$FLOWCTL_VERSION" = "latest" ]; then
        DOWNLOAD_URL="https://github.com/${GITHUB_REPO}/releases/latest/download/flowctl-${PLATFORM}"
    else
        DOWNLOAD_URL="https://github.com/${GITHUB_REPO}/releases/download/${FLOWCTL_VERSION}/flowctl-${PLATFORM}"
    fi
    
    echo "Downloading flowctl for ${PLATFORM}..."
    
    if command -v curl >/dev/null 2>&1; then
        curl -fsSL "$DOWNLOAD_URL" -o "$FLOWCTL_INSTALL_DIR/bin/flowctl"
    elif command -v wget >/dev/null 2>&1; then
        wget -qO "$FLOWCTL_INSTALL_DIR/bin/flowctl" "$DOWNLOAD_URL"
    else
        echo "Error: curl or wget is required"
        exit 1
    fi
}

# Setup installation directory
setup_install_dir() {
    mkdir -p "$FLOWCTL_INSTALL_DIR/bin"
    chmod +x "$FLOWCTL_INSTALL_DIR/bin/flowctl"
}

# Add to PATH
configure_path() {
    FLOWCTL_BIN="$FLOWCTL_INSTALL_DIR/bin"
    
    # Detect shell
    SHELL_PROFILE=""
    if [ -n "$BASH_VERSION" ]; then
        SHELL_PROFILE="$HOME/.bashrc"
    elif [ -n "$ZSH_VERSION" ]; then
        SHELL_PROFILE="$HOME/.zshrc"
    elif [ -f "$HOME/.profile" ]; then
        SHELL_PROFILE="$HOME/.profile"
    fi
    
    # Add to PATH if not already present
    if [ -n "$SHELL_PROFILE" ] && ! grep -q "$FLOWCTL_BIN" "$SHELL_PROFILE"; then
        echo "" >> "$SHELL_PROFILE"
        echo "# FlowCTL" >> "$SHELL_PROFILE"
        echo "export PATH=\"\$PATH:$FLOWCTL_BIN\"" >> "$SHELL_PROFILE"
        echo "Added flowctl to PATH in $SHELL_PROFILE"
        echo "Run 'source $SHELL_PROFILE' or start a new shell to use flowctl"
    fi
    
    # Also create symlink in common locations
    if [ -w "/usr/local/bin" ]; then
        ln -sf "$FLOWCTL_BIN/flowctl" "/usr/local/bin/flowctl" 2>/dev/null || true
    fi
}

# Main installation flow
main() {
    echo "Installing FlowCTL..."
    
    detect_platform
    setup_install_dir
    download_flowctl
    configure_path
    
    # Verify installation
    if "$FLOWCTL_INSTALL_DIR/bin/flowctl" version >/dev/null 2>&1; then
        echo ""
        echo "FlowCTL installed successfully!"
        echo "Version: $("$FLOWCTL_INSTALL_DIR/bin/flowctl" version)"
        echo ""
        echo "To get started, run:"
        echo "  flowctl run --help"
    else
        echo "Installation failed. Please check the error messages above."
        exit 1
    fi
}

main "$@"
```

### Phase 3: Alternative Installation Methods

#### 3.1 PowerShell Script for Windows
```powershell
# install.ps1
$ErrorActionPreference = 'Stop'

$FlowctlVersion = if ($env:FLOWCTL_VERSION) { $env:FLOWCTL_VERSION } else { "latest" }
$InstallDir = if ($env:FLOWCTL_INSTALL_DIR) { $env:FLOWCTL_INSTALL_DIR } else { "$env:USERPROFILE\.flowctl" }

# Detect architecture
$Arch = if ([Environment]::Is64BitProcess) { "amd64" } else { "386" }
$Platform = "windows-$Arch"

# Download
$DownloadUrl = "https://github.com/withObsrvr/cdp-pipeline-workflow/releases/latest/download/flowctl-$Platform.exe"
$BinDir = "$InstallDir\bin"
New-Item -ItemType Directory -Force -Path $BinDir | Out-Null

Write-Host "Downloading flowctl for Windows..."
Invoke-WebRequest -Uri $DownloadUrl -OutFile "$BinDir\flowctl.exe"

# Add to PATH
$CurrentPath = [Environment]::GetEnvironmentVariable("Path", "User")
if ($CurrentPath -notlike "*$BinDir*") {
    [Environment]::SetEnvironmentVariable("Path", "$CurrentPath;$BinDir", "User")
    Write-Host "Added flowctl to PATH. Restart your terminal to use it."
}

Write-Host "FlowCTL installed successfully!"
```

#### 3.2 Homebrew Formula (macOS/Linux)
```ruby
# flowctl.rb
class Flowctl < Formula
  desc "Stellar CDP Pipeline Workflow CLI"
  homepage "https://github.com/withObsrvr/cdp-pipeline-workflow"
  version "0.1.0"
  
  if OS.mac? && Hardware::CPU.intel?
    url "https://github.com/withObsrvr/cdp-pipeline-workflow/releases/download/v#{version}/flowctl-darwin-amd64"
    sha256 "..."
  elsif OS.mac? && Hardware::CPU.arm?
    url "https://github.com/withObsrvr/cdp-pipeline-workflow/releases/download/v#{version}/flowctl-darwin-arm64"
    sha256 "..."
  elsif OS.linux? && Hardware::CPU.intel?
    url "https://github.com/withObsrvr/cdp-pipeline-workflow/releases/download/v#{version}/flowctl-linux-amd64"
    sha256 "..."
  end
  
  def install
    bin.install "flowctl"
  end
  
  test do
    system "#{bin}/flowctl", "version"
  end
end
```

### Phase 4: Hosting and Distribution

#### 4.1 GitHub Pages Setup
```yaml
# .github/workflows/deploy-installer.yml
name: Deploy Installer
on:
  push:
    branches: [main]
    paths:
      - 'scripts/install.sh'
      - 'scripts/install.ps1'

jobs:
  deploy:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v3
      
      - name: Setup Pages
        run: |
          mkdir -p public
          cp scripts/install.sh public/
          cp scripts/install.ps1 public/
          
          # Create index.html with redirect
          cat > public/index.html << 'EOF'
          <!DOCTYPE html>
          <html>
          <head>
            <meta charset="utf-8">
            <title>FlowCTL Installer</title>
            <script>
              // Detect OS and redirect
              var userAgent = navigator.platform;
              if (userAgent.indexOf('Win') !== -1) {
                window.location.href = '/install.ps1';
              } else {
                window.location.href = '/install.sh';
              }
            </script>
          </head>
          <body>
            <h1>Installing FlowCTL...</h1>
          </body>
          </html>
          EOF
      
      - name: Deploy to GitHub Pages
        uses: peaceiris/actions-gh-pages@v3
        with:
          github_token: ${{ secrets.GITHUB_TOKEN }}
          publish_dir: ./public
          cname: get.flowctl.dev
```

#### 4.2 CDN Configuration
- Set up CloudFlare or similar CDN
- Point get.flowctl.dev to GitHub Pages
- Enable HTTPS
- Set cache headers appropriately

### Phase 5: Version Management

#### 5.1 Version Check Command
```go
// cmd/version.go
package cmd

import (
    "fmt"
    "runtime"
    "github.com/spf13/cobra"
)

var (
    Version   = "dev"
    GitCommit = "none"
    BuildDate = "unknown"
)

var versionCmd = &cobra.Command{
    Use:   "version",
    Short: "Print version information",
    Run: func(cmd *cobra.Command, args []string) {
        fmt.Printf("FlowCTL %s\n", Version)
        fmt.Printf("  Git commit: %s\n", GitCommit)
        fmt.Printf("  Built:      %s\n", BuildDate)
        fmt.Printf("  Go version: %s\n", runtime.Version())
        fmt.Printf("  OS/Arch:    %s/%s\n", runtime.GOOS, runtime.GOARCH)
    },
}
```

#### 5.2 Self-Update Command
```go
// cmd/update.go
var updateCmd = &cobra.Command{
    Use:   "update",
    Short: "Update flowctl to the latest version",
    Run: func(cmd *cobra.Command, args []string) {
        // Check current version
        // Fetch latest release from GitHub
        // Download and replace binary
        // Verify checksum
    },
}
```

### Phase 6: Build Script Updates

#### 6.1 Makefile Additions
```makefile
# Version info
VERSION ?= $(shell git describe --tags --always --dirty)
COMMIT := $(shell git rev-parse --short HEAD)
BUILD_DATE := $(shell date -u '+%Y-%m-%d_%H:%M:%S')

# Build flags
LDFLAGS := -X main.Version=$(VERSION) \
           -X main.GitCommit=$(COMMIT) \
           -X main.BuildDate=$(BUILD_DATE)

# Build targets
.PHONY: build-all
build-all: build-linux build-darwin build-windows

.PHONY: build-linux
build-linux:
	GOOS=linux GOARCH=amd64 go build -ldflags "$(LDFLAGS)" -o dist/flowctl-linux-amd64
	GOOS=linux GOARCH=arm64 go build -ldflags "$(LDFLAGS)" -o dist/flowctl-linux-arm64

.PHONY: build-darwin
build-darwin:
	GOOS=darwin GOARCH=amd64 go build -ldflags "$(LDFLAGS)" -o dist/flowctl-darwin-amd64
	GOOS=darwin GOARCH=arm64 go build -ldflags "$(LDFLAGS)" -o dist/flowctl-darwin-arm64

.PHONY: build-windows
build-windows:
	GOOS=windows GOARCH=amd64 go build -ldflags "$(LDFLAGS)" -o dist/flowctl-windows-amd64.exe

# Create checksums
.PHONY: checksums
checksums:
	cd dist && sha256sum flowctl-* > checksums.txt
```

## Testing Strategy

1. **Installation Testing**
   - Test on fresh VMs/containers
   - Multiple OS/arch combinations
   - Behind corporate proxies
   - With/without sudo access

2. **Update Testing**
   - Version upgrades
   - Rollback scenarios
   - Checksum verification

3. **CI Testing**
   ```yaml
   test-installer:
     strategy:
       matrix:
         os: [ubuntu-latest, macos-latest, windows-latest]
     steps:
       - name: Test installer
         run: |
           curl -sSL https://get.flowctl.dev | sh
           flowctl version
   ```

## Security Considerations

1. **Checksum Verification**
   - Publish SHA256 checksums
   - Verify in install script
   - Use HTTPS only

2. **Code Signing**
   - Sign macOS binaries
   - Sign Windows executables
   - GPG sign releases

3. **Permissions**
   - Never require sudo for user installs
   - Respect XDG base directories
   - Clear permission requirements

## Documentation

1. **Installation Guide**
   ```markdown
   # Installing FlowCTL
   
   ## Quick Install (Recommended)
   ```bash
   curl -sSL https://get.flowctl.dev | sh
   ```
   
   ## Manual Installation
   Download from: https://github.com/withObsrvr/cdp-pipeline-workflow/releases
   
   ## Package Managers
   - Homebrew: `brew install flowctl`
   - Snap: `snap install flowctl`
   - Docker: `docker pull obsrvr/flowctl`
   ```

2. **Troubleshooting**
   - Common installation issues
   - Proxy configuration
   - PATH problems
   - Architecture detection

## Success Metrics

1. **Installation Success Rate**: >95%
2. **Time to First Command**: <1 minute
3. **Cross-platform Coverage**: Linux, macOS, Windows
4. **Download Size**: <50MB per binary
5. **Update Success Rate**: >99%

## Timeline

- Week 1: Build infrastructure and release process
- Week 2: Install scripts and testing
- Week 3: Package manager integration
- Week 4: Documentation and polish

This implementation would provide a professional, user-friendly installation experience that gets users from zero to running flowctl in under a minute.