# Installing FlowCTL

FlowCTL is the command-line interface for the CDP Pipeline Workflow. This guide covers all installation methods.

## Quick Install (Recommended)

### Linux/macOS

```bash
curl -sSL https://raw.githubusercontent.com/withObsrvr/cdp-pipeline-workflow/main/scripts/install.sh | sh
```

### Windows (PowerShell)

```powershell
iwr -useb https://raw.githubusercontent.com/withObsrvr/cdp-pipeline-workflow/main/scripts/install.ps1 | iex
```

## Manual Installation

1. Download the appropriate binary for your platform from the [releases page](https://github.com/withObsrvr/cdp-pipeline-workflow/releases)

2. Extract and move to your PATH:
   ```bash
   # Linux/macOS
   chmod +x flowctl-*
   sudo mv flowctl-* /usr/local/bin/flowctl
   
   # Windows
   # Move flowctl-windows-amd64.exe to a directory in your PATH
   ```

3. Verify installation:
   ```bash
   flowctl version
   ```

## Installation Options

### Custom Installation Directory

```bash
# Linux/macOS
FLOWCTL_INSTALL_DIR=/opt/flowctl curl -sSL https://raw.githubusercontent.com/withObsrvr/cdp-pipeline-workflow/main/scripts/install.sh | sh

# Windows
$env:FLOWCTL_INSTALL_DIR = "C:\Program Files\flowctl"; iwr -useb https://raw.githubusercontent.com/withObsrvr/cdp-pipeline-workflow/main/scripts/install.ps1 | iex
```

### Specific Version

```bash
# Linux/macOS
FLOWCTL_VERSION=v0.1.0 curl -sSL https://raw.githubusercontent.com/withObsrvr/cdp-pipeline-workflow/main/scripts/install.sh | sh

# Windows
$env:FLOWCTL_VERSION = "v0.1.0"; iwr -useb https://raw.githubusercontent.com/withObsrvr/cdp-pipeline-workflow/main/scripts/install.ps1 | iex
```

## Docker

```bash
# Pull the image
docker pull ghcr.io/withobsrvr/flowctl:latest

# Run with local config
docker run -v $(pwd)/config:/workspace/config ghcr.io/withobsrvr/flowctl run config/pipeline.yaml

# Create an alias for convenience
alias flowctl='docker run -v $(pwd):/workspace ghcr.io/withobsrvr/flowctl'
```

## Building from Source

### Prerequisites
- Go 1.21 or later
- CGO enabled
- ZeroMQ libraries (libzmq3-dev, libczmq-dev, libsodium-dev)

### Build Steps

```bash
# Clone the repository
git clone https://github.com/withObsrvr/cdp-pipeline-workflow.git
cd cdp-pipeline-workflow

# Install dependencies
make deps

# Build the binary
make build

# Install to system
make install
```

## Supported Platforms

| Platform | Architecture | Binary Name |
|----------|-------------|-------------|
| Linux | x86_64 | flowctl-linux-amd64 |
| Linux | ARM64 | flowctl-linux-arm64 |
| macOS | x86_64 | flowctl-darwin-amd64 |
| macOS | Apple Silicon | flowctl-darwin-arm64 |
| Windows | x86_64 | flowctl-windows-amd64.exe |

## Environment Variables

- `FLOWCTL_INSTALL_DIR`: Custom installation directory (default: `~/.flowctl`)
- `FLOWCTL_VERSION`: Specific version to install (default: latest)

## Troubleshooting

### Command not found

If you get "command not found" after installation:

1. **Add to PATH manually**:
   ```bash
   # Linux/macOS
   export PATH="$PATH:$HOME/.flowctl/bin"
   
   # Windows
   set PATH=%PATH%;%USERPROFILE%\.flowctl\bin
   ```

2. **Reload your shell configuration**:
   ```bash
   # Bash
   source ~/.bashrc
   
   # Zsh
   source ~/.zshrc
   
   # Or just open a new terminal
   ```

### Permission denied

If you get permission errors:
- Use the user installation directory (default)
- Don't use sudo with the install script
- Check file permissions: `chmod +x flowctl`

### Behind a proxy

Set your proxy environment variables before running the installer:
```bash
export HTTP_PROXY=http://proxy.example.com:8080
export HTTPS_PROXY=http://proxy.example.com:8080
curl -sSL https://raw.githubusercontent.com/withObsrvr/cdp-pipeline-workflow/main/scripts/install.sh | sh
```

### Verify checksums

The installer automatically verifies checksums. To manually verify:
```bash
# Download checksums
curl -L https://github.com/withObsrvr/cdp-pipeline-workflow/releases/latest/download/checksums.txt

# Verify your binary
sha256sum -c checksums.txt --ignore-missing
```

## Uninstalling

### Linux/macOS
```bash
# If installed with the script
rm -rf ~/.flowctl
# Remove from PATH in your shell config file

# If installed manually
sudo rm /usr/local/bin/flowctl
```

### Windows
```powershell
# Run the installer with --uninstall flag
powershell -ExecutionPolicy Bypass -File install.ps1 --uninstall

# Or manually
Remove-Item -Recurse -Force "$env:USERPROFILE\.flowctl"
# Remove from PATH via System Properties
```

## Next Steps

After installation, you can:

1. Check the version:
   ```bash
   flowctl version
   ```

2. View available commands:
   ```bash
   flowctl --help
   ```

3. Run your first pipeline:
   ```bash
   flowctl run "captive-core://localhost | latest-ledger | stdout"
   ```

4. Use a configuration file:
   ```bash
   flowctl run config/pipeline.yaml
   ```

For more information, see the [FlowCTL documentation](https://github.com/withObsrvr/cdp-pipeline-workflow).