# Nix Setup Guide for CDP Pipeline Workflow

This guide explains how to use the Nix configuration for building the CDP Pipeline Workflow application and creating Docker containers.

## Prerequisites

### Install Nix
If you don't have Nix installed, install it with flakes support:

```bash
# Install Nix with the installer
curl --proto '=https' --tlsv1.2 -sSf -L https://install.determinate.systems/nix | sh -s -- install

# Or use the official installer with flakes enabled
sh <(curl -L https://nixos.org/nix/install) --daemon
echo "experimental-features = nix-command flakes" >> ~/.config/nix/nix.conf
```

### Install containerd and nerdctl (for DockerHub operations)
```bash
# On NixOS, add to your configuration.nix:
virtualisation.containerd.enable = true;

# On other systems with Nix:
nix profile install nixpkgs#containerd nixpkgs#nerdctl
```

## Quick Start

### Enter Development Environment
```bash
# Clone the repository (if not already done)
cd cdp-pipeline-workflow

# Enter the Nix development shell
nix develop

# The shell will automatically set up all dependencies and environment variables
```

### Build the Application
```bash
# Build the Go application
nix build

# The binary will be available at ./result/bin/cdp-pipeline-workflow
./result/bin/cdp-pipeline-workflow --help
```

### Run the Application
```bash
# Run with a configuration file
./result/bin/cdp-pipeline-workflow -config config/base/pipeline_config.yaml
```

## Container Operations

### Building Containers

#### Production Container
```bash
# Build the production container (minimal, optimized)
nix build .#container-prod

# The container image will be available as ./result
```

#### Development Container
```bash
# Build the development container (includes debugging tools)
nix build .#container-dev

# The container image will be available as ./result
```

### Loading Containers Locally
```bash
# Load the production container into containerd
nerdctl load < result

# List loaded images
nerdctl images
```

### Pushing to DockerHub

#### Setup DockerHub Authentication
```bash
# Login to DockerHub
nerdctl login docker.io
# Enter your DockerHub username and password/token
```

#### Push Production Image
```bash
# Push production image with default tag (latest)
push-to-dockerhub-prod

# Push with custom tag
push-to-dockerhub-prod v1.0.0

# Push to custom registry
push-to-dockerhub-prod latest your-registry.com
```

#### Push Development Image
```bash
# Push development image
push-to-dockerhub-dev

# Push with custom tag
push-to-dockerhub-dev v1.0.0-dev
```

## Development Workflow

### Day-to-Day Development

1. **Enter development environment:**
   ```bash
   nix develop
   ```

2. **Make code changes** using your preferred editor

3. **Build and test locally:**
   ```bash
   # Quick build
   go build -o cdp-pipeline-workflow
   
   # Or full Nix build
   nix build
   ```

4. **Build containers when ready:**
   ```bash
   nix build .#container-prod
   nix build .#container-dev
   ```

5. **Push to registry:**
   ```bash
   push-to-dockerhub-prod
   ```

### Environment Variables

The development shell automatically sets up:
- `CGO_ENABLED=1`
- `CGO_CFLAGS` - pointing to ZeroMQ, CZMQ, and libsodium headers
- `CGO_LDFLAGS` - pointing to required libraries
- `PKG_CONFIG_PATH` - for pkg-config to find dependencies

### Available Tools in Development Shell

- **Go 1.23** - Latest Go version with CGO support
- **System Libraries**: zeromq, czmq, libsodium, openssl
- **Build Tools**: gcc, make, cmake, pkg-config
- **Container Tools**: nerdctl, containerd, skopeo
- **Development Utilities**: yq, jq, curl, vim, htop, networking tools
- **Go Tools**: gotools, gopls (Language Server), delve (debugger)

## Container Details

### Production Container (`obsrvr-flow-pipeline`)
- **Base**: Minimal Nix-built environment
- **Size**: Optimized for production use
- **Contents**: 
  - CDP Pipeline binary
  - Required runtime libraries
  - CA certificates
  - Timezone data
- **User**: Non-root user (1000:1000)
- **Exposed Ports**: 8080

### Development Container (`cdp-pipeline-dev`)
- **Base**: Feature-rich development environment
- **Size**: Larger, includes debugging tools
- **Contents**:
  - Everything from production container
  - Development and debugging tools
  - Shell utilities
- **User**: Non-root user (1000:1000)
- **Exposed Ports**: 8080, 5555 (ZeroMQ)
- **Volumes**: `/app/config`, `/app/data`
- **Environment**: `DEV_MODE=true`, `LOG_LEVEL=debug`

## Configuration Management

### Using Existing Configurations
The Nix setup works seamlessly with existing YAML configurations:

```bash
# Run with existing configuration
./result/bin/cdp-pipeline-workflow -config config/base/pipeline_config.yaml

# Or in container
nerdctl run -v $(pwd)/config:/app/config obsrvr-flow-pipeline -config /app/config/base/pipeline_config.yaml
```

### Environment Variable Overrides
```bash
# Set environment variables for configuration
export AWS_ACCESS_KEY_ID=your-key
export GOOGLE_APPLICATION_CREDENTIALS=/path/to/creds.json

# Run the application
./result/bin/cdp-pipeline-workflow -config config/base/pipeline_config.yaml
```

## Troubleshooting

### Common Issues

#### CGO Build Failures
If you encounter CGO-related build errors:

```bash
# Verify CGO environment in development shell
echo $CGO_ENABLED
echo $CGO_CFLAGS
echo $CGO_LDFLAGS

# Rebuild with verbose output
CGO_ENABLED=1 go build -v .
```

#### Container Loading Issues
```bash
# Check if containerd is running
sudo systemctl status containerd

# Restart containerd if needed
sudo systemctl restart containerd

# Check nerdctl version
nerdctl version
```

#### Missing Dependencies
If you encounter missing system dependencies:

```bash
# Exit and re-enter the development shell
exit
nix develop

# Or rebuild the development environment
nix develop --recreate-lock-file
```

### Vendor Hash Updates

When Go dependencies change, you may need to update the vendor hash in `flake.nix`:

```bash
# Try to build and get the correct hash
nix build 2>&1 | grep "got:" | awk '{print $2}'

# Update the vendorHash in flake.nix with the correct value
```

## Integration with Existing Workflow

### Compatibility with Docker Scripts
The Nix setup maintains compatibility with existing Docker workflows:

- `run-local.sh` can be adapted to use Nix-built containers
- Configuration files remain unchanged
- Environment variables work the same way

### Migration Tips

1. **Gradual Migration**: Start using `nix develop` for local development
2. **Container Testing**: Compare Nix-built containers with existing Docker builds
3. **CI/CD Integration**: Update CI/CD pipelines to use Nix builds
4. **Team Adoption**: Share the development shell for consistent environments

## Performance Benefits

### Reproducible Builds
- Exact dependency versions
- Consistent build environment
- Cacheable build steps

### Development Experience
- Instant environment setup
- No Docker daemon required for development
- Isolated dependencies per project

### Container Optimization
- Minimal production images
- Layer deduplication
- Reproducible container builds

## Next Steps

1. **Try the development environment**: `nix develop`
2. **Build your first container**: `nix build .#container-prod`
3. **Push to your registry**: `push-to-dockerhub-prod your-tag`
4. **Integrate with CI/CD**: Use Nix commands in your build pipelines
5. **Share with team**: Commit the flake files for team-wide consistency

For questions or issues, refer to the [Nix documentation](https://nixos.org/manual/nix/stable/) or the project's issue tracker.