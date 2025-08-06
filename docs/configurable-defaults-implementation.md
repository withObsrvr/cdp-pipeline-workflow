# Configurable Defaults Implementation

## Overview

This document outlines the implementation plan for making pipeline component defaults configurable by users, which is a prerequisite for making one-liner syntax practical.

## Problem Statement

Currently, defaults are hardcoded in `internal/config/v2/defaults.go`:
- `BufferedStorageSourceAdapter`: 20 workers
- `SaveToParquet`: 10,000 buffer size
- `SaveToPostgreSQL`: 10 connection pool size

Users need different defaults based on:
- Environment (dev vs production)
- Hardware capabilities
- Network characteristics
- Data volume

## Solution: Layered Configuration

### Configuration Hierarchy (highest to lowest precedence)

1. **Explicit values** in pipeline config
2. **Environment variables** (runtime overrides)
3. **Project defaults** (`.flowctl/defaults.yaml`)
4. **User defaults** (`~/.flowctl/defaults.yaml`)
5. **System defaults** (current hardcoded values)

### Example Configuration Flow

```yaml
# ~/.flowctl/defaults.yaml (user level)
defaults:
  sources:
    BufferedStorageSourceAdapter:
      num_workers: 10  # Lower for development machine
      buffer_size: 50
    
  consumers:
    SaveToParquet:
      buffer_size: 5000  # Smaller for local testing
```

```yaml
# .flowctl/defaults.yaml (project level)
defaults:
  sources:
    BufferedStorageSourceAdapter:
      num_workers: 30  # Higher for this specific project
      
  # Project-specific network defaults
  networks:
    testnet:
      default_workers: 10
      default_buffer_size: 100
    mainnet:
      default_workers: 50
      default_buffer_size: 1000
```

```bash
# Environment variables (runtime)
export FLOWCTL_DEFAULT_WORKERS=5
export FLOWCTL_DEFAULT_BUFFER_SIZE=25
export FLOWCTL_DEFAULTS_FILE=/custom/path/to/defaults.yaml
```

## Implementation Details

### 1. Enhanced DefaultsEngine

```go
// internal/config/v2/defaults.go

type DefaultsEngine struct {
    systemDefaults    map[string]map[string]interface{}  // Current hardcoded
    userDefaults      map[string]map[string]interface{}  // From ~/.flowctl/defaults.yaml
    projectDefaults   map[string]map[string]interface{}  // From .flowctl/defaults.yaml
    envDefaults       map[string]interface{}             // From environment variables
}

func NewDefaultsEngine() *DefaultsEngine {
    engine := &DefaultsEngine{
        systemDefaults: getSystemDefaults(),
    }
    
    // Load user defaults
    if userConfig := loadUserDefaults(); userConfig != nil {
        engine.userDefaults = userConfig
    }
    
    // Load project defaults
    if projectConfig := loadProjectDefaults(); projectConfig != nil {
        engine.projectDefaults = projectConfig
    }
    
    // Load environment defaults
    engine.envDefaults = loadEnvDefaults()
    
    return engine
}

// ApplyDefaults with layered precedence
func (e *DefaultsEngine) ApplyDefaults(componentType string, config map[string]interface{}) map[string]interface{} {
    result := make(map[string]interface{})
    
    // Layer 1: System defaults (lowest precedence)
    if defaults, ok := e.systemDefaults[componentType]; ok {
        for k, v := range defaults {
            result[k] = v
        }
    }
    
    // Layer 2: User defaults
    if defaults, ok := e.userDefaults[componentType]; ok {
        for k, v := range defaults {
            result[k] = v
        }
    }
    
    // Layer 3: Project defaults
    if defaults, ok := e.projectDefaults[componentType]; ok {
        for k, v := range defaults {
            result[k] = v
        }
    }
    
    // Layer 4: Environment variable defaults
    for k, v := range e.envDefaults {
        if key := mapEnvToConfig(componentType, k); key != "" {
            result[key] = v
        }
    }
    
    // Layer 5: Explicit config (highest precedence)
    for k, v := range config {
        result[k] = v
    }
    
    return result
}
```

### 2. Defaults Configuration File Format

```yaml
# ~/.flowctl/defaults.yaml
version: 1
defaults:
  # Global defaults (apply to all components)
  global:
    num_workers: "${FLOWCTL_WORKERS:-10}"
    buffer_size: "${FLOWCTL_BUFFER_SIZE:-100}"
    
  # Source-specific defaults
  sources:
    BufferedStorageSourceAdapter:
      num_workers: 10
      retry_limit: 3
      buffer_size: 100
      
    # Network-aware defaults
    CaptiveCoreInboundAdapter:
      networks:
        testnet:
          binary_path: "/usr/local/bin/stellar-core-testnet"
          storage_path: "/tmp/stellar-core-testnet"
        mainnet:
          binary_path: "/usr/local/bin/stellar-core"
          storage_path: "/var/lib/stellar-core"
          
  # Processor defaults
  processors:
    FilterPayments:
      include_failed: false
      include_muxed: true
      
  # Consumer defaults
  consumers:
    SaveToParquet:
      compression: "snappy"
      buffer_size: 5000
      max_file_size_mb: 128
      
    SaveToPostgreSQL:
      connection_pool_size: "${DB_POOL_SIZE:-10}"
      batch_size: 1000

# Conditional defaults based on environment
environments:
  development:
    when: "${ENV:-dev}" == "dev"
    defaults:
      global:
        num_workers: 5
        
  production:
    when: "${ENV:-dev}" == "prod"
    defaults:
      global:
        num_workers: 50
        buffer_size: 10000
```

### 3. Environment Variable Mapping

```bash
# Global defaults
FLOWCTL_DEFAULT_WORKERS=10
FLOWCTL_DEFAULT_BUFFER_SIZE=1000

# Component-specific
FLOWCTL_SOURCE_WORKERS=20
FLOWCTL_PARQUET_BUFFER_SIZE=5000
FLOWCTL_POSTGRES_POOL_SIZE=20

# Network-specific
FLOWCTL_TESTNET_WORKERS=10
FLOWCTL_MAINNET_WORKERS=50

# Special overrides
FLOWCTL_DEFAULTS_FILE=/path/to/custom/defaults.yaml
FLOWCTL_NO_USER_DEFAULTS=true  # Disable user defaults
FLOWCTL_NO_PROJECT_DEFAULTS=true  # Disable project defaults
```

### 4. CLI Commands for Managing Defaults

```bash
# View current defaults
flowctl config defaults

# Show where defaults are coming from
flowctl config defaults --explain num_workers
> num_workers: 30
>   From: .flowctl/defaults.yaml (project)
>   Overrides: 
>     - ~/.flowctl/defaults.yaml (user): 10
>     - system default: 20

# Set a user default
flowctl config set-default BufferedStorageSourceAdapter.num_workers 15

# Set a project default
flowctl config set-default --project BufferedStorageSourceAdapter.num_workers 30

# Reset to system defaults
flowctl config reset-defaults

# Validate defaults file
flowctl config validate-defaults ~/.flowctl/defaults.yaml
```

## Integration with One-Liner Syntax

With configurable defaults, one-liners become much more practical:

```bash
# Set your preferred defaults once
cat > ~/.flowctl/defaults.yaml << EOF
defaults:
  sources:
    BufferedStorageSourceAdapter:
      num_workers: 10
      network: testnet  # Default network
      
  consumers:
    SaveToZeroMQ:
      address: "tcp://127.0.0.1:5555"  # Your default ZMQ address
EOF

# Now one-liners use your defaults
flowctl run "gs://obsrvr-stellar-ledger-data-testnet-data/landing/ledgers/testnet | latest-ledger | zmq"
# Uses your defaults: 10 workers, testnet network, tcp://127.0.0.1:5555
```

## Benefits

1. **Personalization**: Users can set defaults that match their environment
2. **Project Consistency**: Teams can share project-level defaults
3. **Environment Adaptation**: Different defaults for dev/staging/production
4. **Simplified One-Liners**: Less need to specify common parameters
5. **Backward Compatible**: Existing configs continue to work

## Implementation Phases

### Phase 1: Core Infrastructure (Week 1-2)
- Extend DefaultsEngine to support layered defaults
- Implement defaults file loading
- Add environment variable support

### Phase 2: CLI Commands (Week 3)
- Add `flowctl config` subcommands
- Implement defaults management commands
- Add validation and explanation features

### Phase 3: Integration (Week 4)
- Update v2 config loader to use new defaults system
- Update documentation
- Add migration guide for existing users

### Phase 4: Testing & Polish (Week 5)
- Comprehensive testing of precedence rules
- Performance optimization
- User documentation and examples

## Success Metrics

- **Configuration Time**: Reduce repeated configuration by 80%
- **One-Liner Adoption**: 70% of one-liners work without parameters
- **User Satisfaction**: Less frustration with repetitive settings
- **Team Adoption**: 50% of teams use project-level defaults

## Future Enhancements

1. **Context-Aware Defaults**
   ```yaml
   contexts:
     high-volume:
       when: "estimated_volume > 1M"
       defaults:
         num_workers: 100
   ```

2. **Dynamic Defaults**
   ```yaml
   defaults:
     sources:
       BufferedStorageSourceAdapter:
         num_workers: "${auto:cpu_cores * 2}"
   ```

3. **Default Profiles**
   ```bash
   flowctl config use-profile high-performance
   flowctl config use-profile development
   ```

4. **Cloud Sync**
   ```bash
   flowctl config sync  # Sync defaults across machines
   ```