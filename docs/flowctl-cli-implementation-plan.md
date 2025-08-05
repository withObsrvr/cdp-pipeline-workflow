# FlowCTL CLI Implementation Plan

## Executive Summary

This document outlines the implementation plan for evolving the CDP Pipeline Workflow CLI. The goal is to rename the binary to `flowctl` and support both the legacy flag-based interface (`flowctl -config config.yaml`) and a new command-based interface (`flowctl run config.yaml`) using Cobra and Viper.

## Current State vs Desired State

### Current State
```bash
# Current usage
./cdp-pipeline-workflow -config config/base/contract_data.yaml
# OR (if built as flowctl)
./flowctl -config config/base/contract_data.yaml

# Limited to single command with flags
# No sub-commands
# No configuration management
```

### Desired State
```bash
# New usage
flowctl run config/base/contract_data.yaml

# Clean command-based interface
# Better error messages
# Improved configuration handling with Viper
```

## Implementation Architecture

### Compatibility Note

The current tool's entry point is `main.go` in the root directory. During the transition, we'll modify the build process to create a `flowctl` binary that supports both the legacy flag-based interface (`flowctl -config`) and the new command-based interface (`flowctl run`). This approach ensures backward compatibility while introducing the new CLI structure.

### 1. Project Structure

```
cdp-pipeline-workflow/
├── main.go                       # Modified to support both legacy and new CLI
├── internal/
│   └── cli/                      # CLI implementation
│       ├── cmd/
│       │   ├── root.go          # Root command
│       │   └── run.go           # Run command
│       ├── config/
│       │   └── config.go        # Viper configuration
│       ├── runner/
│       │   └── runner.go        # Pipeline execution logic
│       └── utils/
│           └── helpers.go       # Shared utilities
```

### 2. Core Dependencies

```go
// go.mod additions
require (
    github.com/spf13/cobra v1.8.0
    github.com/spf13/viper v1.18.2
    github.com/fatih/color v1.16.0    // Colored output
)
```

### 3. Command Structure

#### Modified Main Entry Point (`main.go`)
```go
package main

import (
    "flag"
    "os"
    "github.com/withObsrvr/cdp-pipeline-workflow/internal/cli/cmd"
)

func main() {
    // Check if running in legacy mode (flowctl -config ...)
    if len(os.Args) > 1 && os.Args[1] == "-config" {
        // Run legacy flag-based CLI
        runLegacyCLI()
        return
    }
    
    // Run new Cobra-based CLI
    if err := cmd.Execute(); err != nil {
        os.Exit(1)
    }
}

func runLegacyCLI() {
    // Existing flag-based logic from current main.go
    var configPath string
    flag.StringVar(&configPath, "config", "", "Path to the config file")
    flag.Parse()
    
    // ... existing pipeline execution logic ...
}
```

#### Root Command Implementation (`internal/cli/cmd/root.go`)
```go
package cmd

import (
    "fmt"
    "os"
    
    "github.com/spf13/cobra"
    "github.com/spf13/viper"
    "github.com/fatih/color"
)

var (
    cfgFile string
    verbose bool
    
    rootCmd = &cobra.Command{
        Use:   "flowctl",
        Short: "CDP Pipeline Workflow CLI",
        Long: color.CyanString(`CDP Pipeline Workflow - Run data pipelines with ease`),
    }
)

func Execute() error {
    return rootCmd.Execute()
}

func init() {
    cobra.OnInitialize(initConfig)
    
    rootCmd.PersistentFlags().StringVar(&cfgFile, "config", "", "config file (default: $HOME/.flowctl.yaml)")
    rootCmd.PersistentFlags().BoolVarP(&verbose, "verbose", "v", false, "verbose output")
    
    // Bind flags to viper
    viper.BindPFlag("verbose", rootCmd.PersistentFlags().Lookup("verbose"))
}

func initConfig() {
    if cfgFile != "" {
        viper.SetConfigFile(cfgFile)
    } else {
        home, _ := os.UserHomeDir()
        viper.AddConfigPath(home)
        viper.SetConfigName(".flowctl")
    }
    
    viper.AutomaticEnv()
    viper.SetEnvPrefix("FLOWCTL")
    
    if err := viper.ReadInConfig(); err == nil && verbose {
        fmt.Println("Using config file:", viper.ConfigFileUsed())
    }
}
```

#### Run Command (`internal/cli/cmd/run.go`)
```go
package cmd

import (
    "context"
    "fmt"
    "os"
    "os/signal"
    
    "github.com/spf13/cobra"
    "github.com/fatih/color"
    "github.com/withObsrvr/cdp-pipeline-workflow/internal/cli/runner"
)

var (
    runCmd = &cobra.Command{
        Use:   "run [config file]",
        Short: "Run a pipeline from configuration",
        Long:  "Execute a CDP pipeline using the specified configuration file",
        Args:  cobra.ExactArgs(1),
        Example: `  flowctl run pipeline.yaml
  flowctl run config/production.yaml
  flowctl run stellar-pipeline.yaml`,
        RunE: runPipeline,
    }
)

func init() {
    rootCmd.AddCommand(runCmd)
}

func runPipeline(cmd *cobra.Command, args []string) error {
    configFile := args[0]
    
    // Check if file exists
    if _, err := os.Stat(configFile); os.IsNotExist(err) {
        return fmt.Errorf("configuration file not found: %s", configFile)
    }
    
    // Pretty print startup
    fmt.Println(color.GreenString("🚀 Starting pipeline from %s", configFile))
    
    // Create context with signal handling
    ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, os.Kill)
    defer cancel()
    
    // Create and run pipeline
    runner := runner.New(runner.Options{
        ConfigFile: configFile,
        Verbose:    verbose,
    })
    
    if err := runner.Run(ctx); err != nil {
        return fmt.Errorf("pipeline failed: %w", err)
    }
    
    fmt.Println(color.GreenString("✅ Pipeline completed successfully"))
    return nil
}
```

### 4. Pipeline Runner Implementation

#### Runner Interface (`internal/cli/runner/runner.go`)
```go
package runner

import (
    "context"
    "fmt"
    "os"
    
    "github.com/withObsrvr/cdp-pipeline-workflow/consumer"
    "github.com/withObsrvr/cdp-pipeline-workflow/processor"
    "gopkg.in/yaml.v2"
)

type Options struct {
    ConfigFile string
    Verbose    bool
}

type Runner struct {
    opts     Options
    pipeline *Pipeline
}

type Pipeline struct {
    source     SourceAdapter
    processors []processor.Processor
    consumers  []processor.Processor
}

func New(opts Options) *Runner {
    return &Runner{
        opts: opts,
    }
}

func (r *Runner) Run(ctx context.Context) error {
    // Load configuration
    if err := r.loadConfig(); err != nil {
        return fmt.Errorf("failed to load config: %w", err)
    }
    
    // Run pipeline
    return r.runPipeline(ctx)
}

func (r *Runner) loadConfig() error {
    // Read config file
    configBytes, err := os.ReadFile(r.opts.ConfigFile)
    if err != nil {
        return err
    }
    
    // Parse configuration
    var config Config
    if err := yaml.Unmarshal(configBytes, &config); err != nil {
        return err
    }
    
    // Build pipeline from config
    // This will reuse the existing factory functions from main.go
    pipeline, err := r.buildPipeline(config)
    if err != nil {
        return err
    }
    
    r.pipeline = pipeline
    return nil
}

// buildPipeline leverages existing logic from main.go
// to create sources, processors, and consumers
```

### 5. Configuration Management with Viper

```go
// internal/cli/config/config.go
package config

import (
    "github.com/spf13/viper"
)

type FlowctlConfig struct {
    DefaultSource     string            `mapstructure:"default_source"`
    DefaultConsumer   string            `mapstructure:"default_consumer"`
    Environments      map[string]EnvConfig `mapstructure:"environments"`
}

type EnvConfig struct {
    Name      string            `mapstructure:"name"`
    Variables map[string]string `mapstructure:"variables"`
}

func Load() (*FlowctlConfig, error) {
    var cfg FlowctlConfig
    
    // Set defaults
    viper.SetDefault("default_source", "BufferedStorageSourceAdapter")
    viper.SetDefault("default_consumer", "SaveToParquet")
    
    if err := viper.Unmarshal(&cfg); err != nil {
        return nil, err
    }
    
    return &cfg, nil
}

// Helper functions
func SetDefault(key, value string) {
    viper.Set(key, value)
    viper.WriteConfig()
}

func GetString(key string) string {
    return viper.GetString(key)
}
```

### 6. Migration Strategy

#### Phase 1: Implementation (Week 1)
1. Create new `internal/cli` directory structure
2. Implement run command with Cobra/Viper
3. Modify `main.go` to support both legacy (`flowctl -config`) and new (`flowctl run`) interfaces
4. Add tests for new CLI

#### Phase 2: Testing and Documentation (Week 2)
1. Test both legacy and new command interfaces
2. Update documentation to show both usage patterns
3. Add better error messages
4. Create migration guide

#### Phase 3: Gradual Adoption (Week 3+)
1. Update examples to use `flowctl run`
2. Add deprecation notice for `-config` flag usage
3. Update CI/CD pipelines to build `flowctl` binary
4. Monitor adoption and gather feedback

### 7. Installation and Distribution

#### Binary Distribution
```bash
# Install script
curl -sSL https://get.flowctl.dev | sh

# Or via go install
go install github.com/withObsrvr/cdp-pipeline-workflow@latest
```

#### Makefile Updates
```makefile
# Makefile
.PHONY: build
build:
	go build -o flowctl main.go

.PHONY: install
install: build
	cp flowctl /usr/local/bin/

.PHONY: clean
clean:
	rm -f flowctl

# Legacy compatibility
.PHONY: cdp-pipeline-workflow
cdp-pipeline-workflow: build
	@echo "Note: Binary is now named 'flowctl'"
```

### 8. Testing Strategy

```go
// internal/cli/cmd/run_test.go
func TestRunCommand(t *testing.T) {
    tests := []struct {
        name    string
        args    []string
        wantErr bool
    }{
        {
            name:    "valid config",
            args:    []string{"testdata/valid.yaml"},
            wantErr: false,
        },
        {
            name:    "missing config",
            args:    []string{"nonexistent.yaml"},
            wantErr: true,
        },
    }
    
    for _, tt := range tests {
        t.Run(tt.name, func(t *testing.T) {
            cmd := NewRunCommand()
            cmd.SetArgs(tt.args)
            err := cmd.Execute()
            if (err != nil) != tt.wantErr {
                t.Errorf("Execute() error = %v, wantErr %v", err, tt.wantErr)
            }
        })
    }
}
```

### 9. Documentation Updates

#### New README section
```markdown
## Quick Start with FlowCTL

```bash
# Install flowctl
go install github.com/withObsrvr/cdp-pipeline-workflow@latest

# Run your pipeline (new command-based interface)
flowctl run pipeline.yaml

# Legacy flag-based interface (still supported)
flowctl -config pipeline.yaml
```

### Migration Guide

Users currently running:
```bash
./cdp-pipeline-workflow -config config.yaml
```

Should update to:
```bash
flowctl run config.yaml
```

During the transition period, both interfaces are supported:
- `flowctl -config config.yaml` (legacy)
- `flowctl run config.yaml` (recommended)
```

## Benefits

1. **Improved Developer Experience**
   - Intuitive command structure
   - Better error messages with colors
   - Clean command-line interface

2. **Better Configuration Management**
   - Environment-specific configs with Viper
   - Environment variable support
   - Configuration file flexibility

3. **Extensibility**
   - Clean architecture for future enhancements
   - Better testing framework
   - Modular design

4. **Compatibility**
   - Maintains backward compatibility
   - Gradual migration path
   - Same underlying pipeline engine

## Timeline

- **Week 1**: Basic CLI structure with run command
- **Week 2**: Testing and documentation
- **Week 3**: Migration guide and adoption

## Conclusion

This implementation renames the binary to `flowctl` and provides both backward compatibility with the legacy flag-based interface and a cleaner, modern command-based interface using Cobra and Viper. Users can transition gradually from `flowctl -config` to `flowctl run` while maintaining all existing functionality.