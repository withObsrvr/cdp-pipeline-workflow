# CDP Pipeline Workflow Plugin System Implementation Guide

## Executive Summary

This document presents a comprehensive plugin system for CDP Pipeline Workflow that addresses the limitations of previous implementations while enabling language-agnostic plugin development. By combining gRPC for cross-language support with WebAssembly for sandboxed execution, this system provides flexibility, security, and maintainability without the dependency management issues of Go's native plugin system.

## Background & Lessons Learned

### Previous Implementation Issues

The original Flow plugin system used Go's native plugin mechanism (`plugin.so`), which led to:
- **Dependency Hell**: Plugins had to match the exact Go version and dependencies of the host
- **Platform Limitations**: `.so` files are platform-specific (Linux only)
- **Maintenance Burden**: Any dependency update required rebuilding all plugins
- **Version Conflicts**: Incompatible module versions between host and plugins

### Key Requirements

1. **Language Agnostic**: Support plugins written in any language
2. **Dependency Isolation**: Plugins manage their own dependencies
3. **Hot Reload**: Update plugins without restarting the pipeline
4. **Performance**: Minimal overhead for high-throughput processing
5. **Security**: Sandboxed execution for untrusted plugins
6. **Developer Experience**: Simple to create and deploy plugins

## Architecture Overview

### Hybrid Plugin System

```
┌─────────────────────────────────────────────────────────────┐
│                   CDP Pipeline Host                          │
├─────────────────────────────────────────────────────────────┤
│                    Plugin Manager                            │
│  ┌─────────────┐  ┌─────────────┐  ┌─────────────┐        │
│  │ gRPC Bridge │  │ WASM Runtime│  │ Native API  │        │
│  └──────┬──────┘  └──────┬──────┘  └──────┬──────┘        │
└─────────┼─────────────────┼─────────────────┼──────────────┘
          │                 │                 │
    ┌─────┴──────┐   ┌─────┴──────┐   ┌─────┴──────┐
    │ External   │   │ WASM       │   │ Built-in   │
    │ Services   │   │ Plugins    │   │ Processors │
    │ (Any Lang) │   │ (Rust/Go)  │   │ (Go)       │
    └────────────┘   └────────────┘   └────────────┘
```

### Plugin Types

1. **gRPC Plugins** (Recommended for most use cases)
   - Run as separate processes/containers
   - Complete language freedom
   - Easy to scale and deploy
   - Network overhead (minimal with local gRPC)

2. **WebAssembly Plugins** (For performance-critical, trusted code)
   - Run in-process with near-native speed
   - Sandboxed execution
   - Limited language support (Rust, Go, C++)
   - Memory safety guarantees

3. **Native Plugins** (Built-in processors)
   - Compiled into the main binary
   - Best performance
   - No isolation

## Implementation Details

### 1. Plugin Interface Definition

Create `pkg/plugin/interface.go`:

```go
package plugin

import (
    "context"
    "encoding/json"
)

// PluginType defines the type of plugin
type PluginType string

const (
    PluginTypeProcessor PluginType = "processor"
    PluginTypeConsumer  PluginType = "consumer"
    PluginTypeSource    PluginType = "source"
)

// PluginMetadata describes a plugin
type PluginMetadata struct {
    Name        string            `json:"name"`
    Version     string            `json:"version"`
    Type        PluginType        `json:"type"`
    Description string            `json:"description"`
    Author      string            `json:"author"`
    
    // Capabilities
    InputTypes  []string          `json:"input_types"`
    OutputTypes []string          `json:"output_types"`
    
    // Configuration schema
    ConfigSchema json.RawMessage   `json:"config_schema"`
    
    // Resource requirements
    Resources   ResourceRequirements `json:"resources"`
}

// ResourceRequirements defines plugin resource needs
type ResourceRequirements struct {
    MinMemoryMB int    `json:"min_memory_mb"`
    MaxMemoryMB int    `json:"max_memory_mb"`
    CPUShares   int    `json:"cpu_shares"`
    GPURequired bool   `json:"gpu_required"`
}

// Plugin is the base interface all plugins must implement
type Plugin interface {
    // Metadata returns plugin information
    Metadata() PluginMetadata
    
    // Configure initializes the plugin with configuration
    Configure(ctx context.Context, config map[string]interface{}) error
    
    // Start begins plugin operation
    Start(ctx context.Context) error
    
    // Stop gracefully shuts down the plugin
    Stop(ctx context.Context) error
    
    // Health returns the plugin's health status
    Health(ctx context.Context) HealthStatus
}

// ProcessorPlugin extends Plugin for data processing
type ProcessorPlugin interface {
    Plugin
    
    // Process handles a single message
    Process(ctx context.Context, input Message) (Message, error)
    
    // ProcessBatch handles multiple messages (optional optimization)
    ProcessBatch(ctx context.Context, inputs []Message) ([]Message, error)
}

// Message represents data flowing through the pipeline
type Message struct {
    ID       string                 `json:"id"`
    Type     string                 `json:"type"`
    Payload  json.RawMessage        `json:"payload"`
    Metadata map[string]interface{} `json:"metadata"`
    
    // Provenance tracking
    Source   SourceInfo             `json:"source"`
    TraceID  string                 `json:"trace_id"`
}
```

### 2. Plugin Manager

Create `pkg/plugin/manager.go`:

```go
package plugin

import (
    "context"
    "fmt"
    "sync"
    "time"
    
    "github.com/fsnotify/fsnotify"
)

// Manager handles plugin lifecycle
type Manager struct {
    mu          sync.RWMutex
    plugins     map[string]LoadedPlugin
    loaders     map[string]PluginLoader
    config      ManagerConfig
    
    // Hot reload
    watcher     *fsnotify.Watcher
    
    // Metrics
    metrics     *PluginMetrics
}

// LoadedPlugin represents a loaded plugin instance
type LoadedPlugin struct {
    Plugin      Plugin
    Loader      PluginLoader
    Metadata    PluginMetadata
    LoadedAt    time.Time
    Config      map[string]interface{}
    
    // Runtime state
    Running     bool
    Health      HealthStatus
    LastHealthCheck time.Time
}

// ManagerConfig configures the plugin manager
type ManagerConfig struct {
    // Plugin directories
    PluginDirs      []string
    
    // Hot reload
    EnableHotReload bool
    ReloadDebounce  time.Duration
    
    // Health checks
    HealthCheckInterval time.Duration
    UnhealthyThreshold  int
    
    // Resource limits
    MaxPlugins      int
    MaxMemoryPerPlugin int
}

// NewManager creates a new plugin manager
func NewManager(config ManagerConfig) (*Manager, error) {
    m := &Manager{
        plugins: make(map[string]LoadedPlugin),
        loaders: make(map[string]PluginLoader),
        config:  config,
        metrics: NewPluginMetrics(),
    }
    
    // Register built-in loaders
    m.RegisterLoader("grpc", NewGRPCLoader())
    m.RegisterLoader("wasm", NewWASMLoader())
    m.RegisterLoader("native", NewNativeLoader())
    
    // Setup hot reload if enabled
    if config.EnableHotReload {
        if err := m.setupHotReload(); err != nil {
            return nil, fmt.Errorf("failed to setup hot reload: %w", err)
        }
    }
    
    // Start health checker
    go m.healthCheckLoop()
    
    return m, nil
}

// LoadPlugin loads a plugin by path or identifier
func (m *Manager) LoadPlugin(ctx context.Context, path string, config map[string]interface{}) error {
    m.mu.Lock()
    defer m.mu.Unlock()
    
    // Detect plugin type
    loader, err := m.detectLoader(path)
    if err != nil {
        return fmt.Errorf("failed to detect plugin type: %w", err)
    }
    
    // Load plugin
    plugin, err := loader.Load(ctx, path)
    if err != nil {
        return fmt.Errorf("failed to load plugin: %w", err)
    }
    
    // Get metadata
    metadata := plugin.Metadata()
    
    // Validate configuration
    if err := m.validateConfig(metadata, config); err != nil {
        return fmt.Errorf("invalid configuration: %w", err)
    }
    
    // Configure plugin
    if err := plugin.Configure(ctx, config); err != nil {
        return fmt.Errorf("failed to configure plugin: %w", err)
    }
    
    // Store loaded plugin
    loaded := LoadedPlugin{
        Plugin:   plugin,
        Loader:   loader,
        Metadata: metadata,
        LoadedAt: time.Now(),
        Config:   config,
        Running:  false,
    }
    
    m.plugins[metadata.Name] = loaded
    m.metrics.PluginLoaded(metadata.Name)
    
    return nil
}

// StartPlugin starts a loaded plugin
func (m *Manager) StartPlugin(ctx context.Context, name string) error {
    m.mu.Lock()
    defer m.mu.Unlock()
    
    loaded, ok := m.plugins[name]
    if !ok {
        return fmt.Errorf("plugin %s not found", name)
    }
    
    if loaded.Running {
        return fmt.Errorf("plugin %s already running", name)
    }
    
    // Check resource limits
    if err := m.checkResourceLimits(loaded.Metadata.Resources); err != nil {
        return fmt.Errorf("resource limits exceeded: %w", err)
    }
    
    // Start plugin
    if err := loaded.Plugin.Start(ctx); err != nil {
        return fmt.Errorf("failed to start plugin: %w", err)
    }
    
    loaded.Running = true
    m.plugins[name] = loaded
    m.metrics.PluginStarted(name)
    
    return nil
}

// StopPlugin stops a running plugin
func (m *Manager) StopPlugin(ctx context.Context, name string) error {
    m.mu.Lock()
    defer m.mu.Unlock()
    
    loaded, ok := m.plugins[name]
    if !ok {
        return fmt.Errorf("plugin %s not found", name)
    }
    
    if !loaded.Running {
        return nil
    }
    
    // Stop plugin with timeout
    stopCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
    defer cancel()
    
    if err := loaded.Plugin.Stop(stopCtx); err != nil {
        return fmt.Errorf("failed to stop plugin: %w", err)
    }
    
    loaded.Running = false
    m.plugins[name] = loaded
    m.metrics.PluginStopped(name)
    
    return nil
}

// ReloadPlugin reloads a plugin (hot reload)
func (m *Manager) ReloadPlugin(ctx context.Context, name string) error {
    m.mu.Lock()
    loaded, ok := m.plugins[name]
    m.mu.Unlock()
    
    if !ok {
        return fmt.Errorf("plugin %s not found", name)
    }
    
    // Stop old instance
    if err := m.StopPlugin(ctx, name); err != nil {
        return fmt.Errorf("failed to stop old plugin: %w", err)
    }
    
    // Reload from same path with same config
    if err := m.LoadPlugin(ctx, loaded.Loader.GetPath(name), loaded.Config); err != nil {
        return fmt.Errorf("failed to reload plugin: %w", err)
    }
    
    // Start new instance
    if err := m.StartPlugin(ctx, name); err != nil {
        return fmt.Errorf("failed to start reloaded plugin: %w", err)
    }
    
    m.metrics.PluginReloaded(name)
    return nil
}

// GetProcessor returns a processor plugin wrapped in the Processor interface
func (m *Manager) GetProcessor(name string) (processor.Processor, error) {
    m.mu.RLock()
    defer m.mu.RUnlock()
    
    loaded, ok := m.plugins[name]
    if !ok {
        return nil, fmt.Errorf("plugin %s not found", name)
    }
    
    if !loaded.Running {
        return nil, fmt.Errorf("plugin %s not running", name)
    }
    
    // Wrap plugin in processor adapter
    if proc, ok := loaded.Plugin.(ProcessorPlugin); ok {
        return NewProcessorAdapter(proc), nil
    }
    
    return nil, fmt.Errorf("plugin %s is not a processor", name)
}
```

### 3. gRPC Plugin Loader

Create `pkg/plugin/loader_grpc.go`:

```go
package plugin

import (
    "context"
    "fmt"
    "net"
    "os"
    "os/exec"
    "path/filepath"
    "time"
    
    "google.golang.org/grpc"
    "gopkg.in/yaml.v3"
    
    pb "github.com/withObsrvr/cdp-pipeline-workflow/gen/plugin/v1"
)

// GRPCLoader loads external gRPC plugins
type GRPCLoader struct {
    processes map[string]*exec.Cmd
    clients   map[string]pb.PluginServiceClient
}

// GRPCPluginManifest describes an external plugin
type GRPCPluginManifest struct {
    Name       string   `yaml:"name"`
    Version    string   `yaml:"version"`
    Executable string   `yaml:"executable"`
    Args       []string `yaml:"args"`
    Port       int      `yaml:"port"`
    
    // Optional Docker configuration
    Docker     *DockerConfig `yaml:"docker,omitempty"`
}

type DockerConfig struct {
    Image      string            `yaml:"image"`
    Ports      map[string]string `yaml:"ports"`
    Volumes    []string          `yaml:"volumes"`
    Env        map[string]string `yaml:"env"`
}

func NewGRPCLoader() *GRPCLoader {
    return &GRPCLoader{
        processes: make(map[string]*exec.Cmd),
        clients:   make(map[string]pb.PluginServiceClient),
    }
}

func (l *GRPCLoader) Load(ctx context.Context, path string) (Plugin, error) {
    // Load manifest
    manifestPath := filepath.Join(path, "plugin.yaml")
    manifestData, err := os.ReadFile(manifestPath)
    if err != nil {
        return nil, fmt.Errorf("failed to read manifest: %w", err)
    }
    
    var manifest GRPCPluginManifest
    if err := yaml.Unmarshal(manifestData, &manifest); err != nil {
        return nil, fmt.Errorf("failed to parse manifest: %w", err)
    }
    
    // Start plugin process
    var cmd *exec.Cmd
    var address string
    
    if manifest.Docker != nil {
        // Run as Docker container
        cmd, address, err = l.startDockerPlugin(ctx, path, manifest)
    } else {
        // Run as local process
        cmd, address, err = l.startLocalPlugin(ctx, path, manifest)
    }
    
    if err != nil {
        return nil, fmt.Errorf("failed to start plugin: %w", err)
    }
    
    l.processes[manifest.Name] = cmd
    
    // Connect to plugin
    conn, err := l.connectWithRetry(ctx, address, 30*time.Second)
    if err != nil {
        cmd.Process.Kill()
        return nil, fmt.Errorf("failed to connect to plugin: %w", err)
    }
    
    client := pb.NewPluginServiceClient(conn)
    l.clients[manifest.Name] = client
    
    // Create plugin wrapper
    return &GRPCPlugin{
        name:    manifest.Name,
        client:  client,
        conn:    conn,
        process: cmd,
    }, nil
}

func (l *GRPCLoader) startLocalPlugin(ctx context.Context, path string, manifest GRPCPluginManifest) (*exec.Cmd, string, error) {
    execPath := filepath.Join(path, manifest.Executable)
    
    // Ensure executable permissions
    if err := os.Chmod(execPath, 0755); err != nil {
        return nil, "", fmt.Errorf("failed to set executable permissions: %w", err)
    }
    
    // Prepare command
    args := append([]string{}, manifest.Args...)
    cmd := exec.CommandContext(ctx, execPath, args...)
    cmd.Dir = path
    cmd.Env = append(os.Environ(), 
        fmt.Sprintf("PLUGIN_PORT=%d", manifest.Port),
        "PLUGIN_MODE=grpc",
    )
    
    // Capture output for debugging
    cmd.Stdout = os.Stdout
    cmd.Stderr = os.Stderr
    
    // Start process
    if err := cmd.Start(); err != nil {
        return nil, "", fmt.Errorf("failed to start process: %w", err)
    }
    
    address := fmt.Sprintf("localhost:%d", manifest.Port)
    return cmd, address, nil
}

func (l *GRPCLoader) startDockerPlugin(ctx context.Context, path string, manifest GRPCPluginManifest) (*exec.Cmd, string, error) {
    // Build docker run command
    args := []string{"run", "-d", "--rm", "--name", manifest.Name}
    
    // Port mapping
    for host, container := range manifest.Docker.Ports {
        args = append(args, "-p", fmt.Sprintf("%s:%s", host, container))
    }
    
    // Volumes
    for _, volume := range manifest.Docker.Volumes {
        args = append(args, "-v", volume)
    }
    
    // Environment variables
    for key, value := range manifest.Docker.Env {
        args = append(args, "-e", fmt.Sprintf("%s=%s", key, value))
    }
    
    args = append(args, manifest.Docker.Image)
    
    cmd := exec.CommandContext(ctx, "docker", args...)
    
    if err := cmd.Start(); err != nil {
        return nil, "", fmt.Errorf("failed to start docker container: %w", err)
    }
    
    // Get container IP
    time.Sleep(2 * time.Second) // Give container time to start
    
    inspectCmd := exec.Command("docker", "inspect", "-f", 
        "{{range .NetworkSettings.Networks}}{{.IPAddress}}{{end}}", manifest.Name)
    output, err := inspectCmd.Output()
    if err != nil {
        cmd.Process.Kill()
        return nil, "", fmt.Errorf("failed to get container IP: %w", err)
    }
    
    containerIP := string(output)
    address := fmt.Sprintf("%s:%d", containerIP, manifest.Port)
    
    return cmd, address, nil
}

func (l *GRPCLoader) connectWithRetry(ctx context.Context, address string, timeout time.Duration) (*grpc.ClientConn, error) {
    deadline := time.Now().Add(timeout)
    
    for time.Now().Before(deadline) {
        conn, err := grpc.DialContext(ctx, address,
            grpc.WithInsecure(),
            grpc.WithBlock(),
            grpc.WithTimeout(1*time.Second),
        )
        
        if err == nil {
            return conn, nil
        }
        
        select {
        case <-ctx.Done():
            return nil, ctx.Err()
        case <-time.After(500 * time.Millisecond):
            // Retry
        }
    }
    
    return nil, fmt.Errorf("timeout connecting to plugin at %s", address)
}
```

### 4. WebAssembly Plugin Loader

Create `pkg/plugin/loader_wasm.go`:

```go
package plugin

import (
    "context"
    "fmt"
    "os"
    
    "github.com/tetratelabs/wazero"
    "github.com/tetratelabs/wazero/api"
    "github.com/tetratelabs/wazero/imports/wasi_snapshot_preview1"
)

// WASMLoader loads WebAssembly plugins
type WASMLoader struct {
    runtime wazero.Runtime
    modules map[string]api.Module
}

func NewWASMLoader() *WASMLoader {
    return &WASMLoader{
        runtime: wazero.NewRuntime(context.Background()),
        modules: make(map[string]api.Module),
    }
}

func (l *WASMLoader) Load(ctx context.Context, path string) (Plugin, error) {
    // Read WASM binary
    wasmBytes, err := os.ReadFile(path)
    if err != nil {
        return nil, fmt.Errorf("failed to read WASM file: %w", err)
    }
    
    // Configure WASM runtime
    config := wazero.NewModuleConfig().
        WithStdout(os.Stdout).
        WithStderr(os.Stderr).
        WithFS(os.DirFS("."))
    
    // Instantiate WASI (WebAssembly System Interface)
    wasi_snapshot_preview1.MustInstantiate(ctx, l.runtime)
    
    // Compile module
    compiledModule, err := l.runtime.CompileModule(ctx, wasmBytes)
    if err != nil {
        return nil, fmt.Errorf("failed to compile WASM module: %w", err)
    }
    
    // Instantiate module
    module, err := l.runtime.InstantiateModule(ctx, compiledModule, config)
    if err != nil {
        return nil, fmt.Errorf("failed to instantiate WASM module: %w", err)
    }
    
    // Create plugin wrapper
    plugin := &WASMPlugin{
        module:  module,
        runtime: l.runtime,
    }
    
    // Initialize plugin
    if err := plugin.initialize(ctx); err != nil {
        return nil, fmt.Errorf("failed to initialize plugin: %w", err)
    }
    
    return plugin, nil
}

// WASMPlugin wraps a WebAssembly module as a plugin
type WASMPlugin struct {
    module   api.Module
    runtime  wazero.Runtime
    metadata PluginMetadata
    
    // Exported functions
    configureFn api.Function
    processFn   api.Function
    healthFn    api.Function
}

func (p *WASMPlugin) initialize(ctx context.Context) error {
    // Get exported functions
    p.configureFn = p.module.ExportedFunction("configure")
    p.processFn = p.module.ExportedFunction("process")
    p.healthFn = p.module.ExportedFunction("health")
    
    if p.configureFn == nil || p.processFn == nil {
        return fmt.Errorf("required functions not exported")
    }
    
    // Get metadata
    metadataFn := p.module.ExportedFunction("metadata")
    if metadataFn == nil {
        return fmt.Errorf("metadata function not exported")
    }
    
    // Call metadata function
    results, err := metadataFn.Call(ctx)
    if err != nil {
        return fmt.Errorf("failed to get metadata: %w", err)
    }
    
    // Parse metadata from memory
    // (Implementation depends on WASM ABI)
    
    return nil
}

func (p *WASMPlugin) Metadata() PluginMetadata {
    return p.metadata
}

func (p *WASMPlugin) Configure(ctx context.Context, config map[string]interface{}) error {
    // Serialize config to JSON
    configBytes, err := json.Marshal(config)
    if err != nil {
        return err
    }
    
    // Write to WASM memory
    ptr, err := p.writeToMemory(configBytes)
    if err != nil {
        return err
    }
    
    // Call configure function
    results, err := p.configureFn.Call(ctx, ptr, uint64(len(configBytes)))
    if err != nil {
        return fmt.Errorf("configure failed: %w", err)
    }
    
    if results[0] != 0 {
        return fmt.Errorf("configure returned error code: %d", results[0])
    }
    
    return nil
}

func (p *WASMPlugin) Process(ctx context.Context, input Message) (Message, error) {
    // Serialize input
    inputBytes, err := json.Marshal(input)
    if err != nil {
        return Message{}, err
    }
    
    // Write to WASM memory
    inputPtr, err := p.writeToMemory(inputBytes)
    if err != nil {
        return Message{}, err
    }
    
    // Allocate output buffer
    outputPtr, err := p.allocateMemory(1024 * 1024) // 1MB buffer
    if err != nil {
        return Message{}, err
    }
    
    // Call process function
    results, err := p.processFn.Call(ctx, 
        inputPtr, uint64(len(inputBytes)),
        outputPtr, uint64(1024*1024))
    if err != nil {
        return Message{}, fmt.Errorf("process failed: %w", err)
    }
    
    outputLen := results[0]
    if outputLen == 0 {
        return Message{}, fmt.Errorf("process returned no output")
    }
    
    // Read output from memory
    outputBytes, err := p.readFromMemory(outputPtr, outputLen)
    if err != nil {
        return Message{}, err
    }
    
    // Deserialize output
    var output Message
    if err := json.Unmarshal(outputBytes, &output); err != nil {
        return Message{}, fmt.Errorf("failed to parse output: %w", err)
    }
    
    return output, nil
}
```

### 5. Plugin Development SDKs

#### Go SDK

Create `sdk/go/plugin.go`:

```go
package cdpplugin

import (
    "context"
    "encoding/json"
    "fmt"
    "net"
    "os"
    
    "google.golang.org/grpc"
    pb "github.com/withObsrvr/cdp-pipeline-workflow/gen/plugin/v1"
)

// ProcessorFunc is a simple function that processes messages
type ProcessorFunc func(context.Context, Message) (Message, error)

// ServeProcessor starts a gRPC server for a processor function
func ServeProcessor(metadata PluginMetadata, processor ProcessorFunc) error {
    port := os.Getenv("PLUGIN_PORT")
    if port == "" {
        port = "50051"
    }
    
    lis, err := net.Listen("tcp", fmt.Sprintf(":%s", port))
    if err != nil {
        return fmt.Errorf("failed to listen: %w", err)
    }
    
    server := grpc.NewServer()
    pb.RegisterPluginServiceServer(server, &processorServer{
        metadata:  metadata,
        processor: processor,
    })
    
    fmt.Printf("Plugin %s listening on %v\n", metadata.Name, lis.Addr())
    return server.Serve(lis)
}

// Example plugin implementation
func ExamplePlugin() {
    metadata := PluginMetadata{
        Name:        "example-processor",
        Version:     "1.0.0",
        Type:        PluginTypeProcessor,
        Description: "Example processor plugin",
        InputTypes:  []string{"LedgerCloseMeta"},
        OutputTypes: []string{"ProcessedData"},
    }
    
    processor := func(ctx context.Context, msg Message) (Message, error) {
        // Process the message
        var ledgerData map[string]interface{}
        if err := json.Unmarshal(msg.Payload, &ledgerData); err != nil {
            return Message{}, err
        }
        
        // Transform data
        processed := map[string]interface{}{
            "ledger_seq": ledgerData["sequence"],
            "op_count":   len(ledgerData["operations"].([]interface{})),
            "processed":  true,
        }
        
        // Create output message
        outputPayload, _ := json.Marshal(processed)
        output := Message{
            ID:       GenerateID(),
            Type:     "ProcessedData",
            Payload:  outputPayload,
            Metadata: msg.Metadata,
            TraceID:  msg.TraceID,
        }
        
        return output, nil
    }
    
    if err := ServeProcessor(metadata, processor); err != nil {
        log.Fatal(err)
    }
}
```

#### Python SDK

Create `sdk/python/cdpplugin/__init__.py`:

```python
import os
import json
import grpc
from concurrent import futures
from typing import Dict, Any, Callable

from . import plugin_pb2
from . import plugin_pb2_grpc

class PluginMetadata:
    def __init__(self, name: str, version: str, type: str, **kwargs):
        self.name = name
        self.version = version
        self.type = type
        self.description = kwargs.get('description', '')
        self.input_types = kwargs.get('input_types', [])
        self.output_types = kwargs.get('output_types', [])

class Message:
    def __init__(self, id: str, type: str, payload: Any, metadata: Dict = None):
        self.id = id
        self.type = type
        self.payload = payload
        self.metadata = metadata or {}
        self.trace_id = metadata.get('trace_id', '')

def serve_processor(metadata: PluginMetadata, processor: Callable):
    """Start a gRPC server for a processor function"""
    
    class ProcessorService(plugin_pb2_grpc.PluginServiceServicer):
        def GetMetadata(self, request, context):
            return plugin_pb2.MetadataResponse(
                name=metadata.name,
                version=metadata.version,
                type=metadata.type,
                description=metadata.description,
                input_types=metadata.input_types,
                output_types=metadata.output_types
            )
        
        def Process(self, request, context):
            # Deserialize input
            input_data = json.loads(request.payload)
            input_msg = Message(
                id=request.id,
                type=request.type,
                payload=input_data,
                metadata=dict(request.metadata)
            )
            
            # Process
            try:
                output_msg = processor(input_msg)
                
                # Serialize output
                output_payload = json.dumps(output_msg.payload).encode()
                
                return plugin_pb2.ProcessResponse(
                    id=output_msg.id,
                    type=output_msg.type,
                    payload=output_payload,
                    metadata=output_msg.metadata
                )
            except Exception as e:
                context.set_code(grpc.StatusCode.INTERNAL)
                context.set_details(str(e))
                return plugin_pb2.ProcessResponse()
    
    # Start server
    port = os.environ.get('PLUGIN_PORT', '50051')
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
    plugin_pb2_grpc.add_PluginServiceServicer_to_server(
        ProcessorService(), server
    )
    server.add_insecure_port(f'[::]:{port}')
    server.start()
    
    print(f"Plugin {metadata.name} listening on port {port}")
    server.wait_for_termination()

# Example usage
def example_plugin():
    metadata = PluginMetadata(
        name="python-processor",
        version="1.0.0",
        type="processor",
        description="Example Python processor",
        input_types=["LedgerCloseMeta"],
        output_types=["ProcessedData"]
    )
    
    def process_message(msg: Message) -> Message:
        # Process the ledger data
        ledger_data = msg.payload
        
        processed = {
            'ledger_seq': ledger_data.get('sequence'),
            'op_count': len(ledger_data.get('operations', [])),
            'processed_by': 'python-processor'
        }
        
        return Message(
            id=generate_id(),
            type='ProcessedData',
            payload=processed,
            metadata=msg.metadata
        )
    
    serve_processor(metadata, process_message)
```

### 6. Plugin Configuration

Create `pkg/plugin/config.go`:

```go
package plugin

import (
    "fmt"
    "gopkg.in/yaml.v3"
)

// PipelineWithPlugins extends pipeline config with plugin support
type PipelineWithPlugins struct {
    Name    string                 `yaml:"name"`
    Plugins []PluginReference      `yaml:"plugins"`
    Source  SourceConfig           `yaml:"source"`
    Pipeline []PipelineStage       `yaml:"pipeline"`
}

// PluginReference references a plugin to load
type PluginReference struct {
    Name    string                 `yaml:"name"`
    Type    string                 `yaml:"type"` // grpc, wasm, native
    Path    string                 `yaml:"path"`
    Config  map[string]interface{} `yaml:"config"`
    
    // Optional version constraints
    Version VersionConstraint      `yaml:"version,omitempty"`
}

// PipelineStage can be a built-in processor or plugin
type PipelineStage struct {
    Type   string                  `yaml:"type"`
    Plugin string                  `yaml:"plugin,omitempty"` // Reference to loaded plugin
    Config map[string]interface{}  `yaml:"config,omitempty"`
}

// Example configuration
const exampleConfig = `
name: stellar-processing-with-plugins

# Load plugins
plugins:
  - name: fraud-detector
    type: grpc
    path: ./plugins/fraud-detector
    config:
      threshold: 0.8
      model_path: /models/fraud_v2.pkl
    
  - name: fast-filter
    type: wasm
    path: ./plugins/fast-filter/filter.wasm
    config:
      rules:
        - field: amount
          op: ">"
          value: 1000000

# Pipeline definition
source:
  type: captive-core
  config:
    network: mainnet

pipeline:
  # Built-in processor
  - type: latest-ledger
  
  # WASM plugin for fast filtering
  - plugin: fast-filter
  
  # gRPC plugin for fraud detection
  - plugin: fraud-detector
  
  # Built-in consumer
  - type: save-to-parquet
    config:
      path: /data/processed
`
```

### 7. Plugin Marketplace & Registry

Create `pkg/plugin/registry.go`:

```go
package plugin

import (
    "context"
    "fmt"
    "io"
    "net/http"
    "os"
    "path/filepath"
)

// Registry manages plugin discovery and installation
type Registry struct {
    baseURL    string
    cacheDir   string
    httpClient *http.Client
}

// PluginInfo describes a plugin in the registry
type PluginInfo struct {
    Name        string            `json:"name"`
    Version     string            `json:"version"`
    Description string            `json:"description"`
    Author      string            `json:"author"`
    License     string            `json:"license"`
    
    // Download information
    Downloads   map[string]string `json:"downloads"` // platform -> URL
    Checksum    string            `json:"checksum"`
    
    // Compatibility
    MinCDPVersion string          `json:"min_cdp_version"`
    MaxCDPVersion string          `json:"max_cdp_version"`
    
    // Stats
    Stars       int               `json:"stars"`
    Downloads   int               `json:"downloads"`
}

// Search searches for plugins
func (r *Registry) Search(ctx context.Context, query string) ([]PluginInfo, error) {
    url := fmt.Sprintf("%s/search?q=%s", r.baseURL, query)
    
    resp, err := r.httpClient.Get(url)
    if err != nil {
        return nil, err
    }
    defer resp.Body.Close()
    
    var results []PluginInfo
    if err := json.NewDecoder(resp.Body).Decode(&results); err != nil {
        return nil, err
    }
    
    return results, nil
}

// Install downloads and installs a plugin
func (r *Registry) Install(ctx context.Context, name, version string) error {
    // Get plugin info
    info, err := r.getPluginInfo(name, version)
    if err != nil {
        return err
    }
    
    // Detect platform
    platform := detectPlatform()
    downloadURL, ok := info.Downloads[platform]
    if !ok {
        return fmt.Errorf("plugin not available for platform %s", platform)
    }
    
    // Download plugin
    pluginPath := filepath.Join(r.cacheDir, name, version)
    if err := os.MkdirAll(pluginPath, 0755); err != nil {
        return err
    }
    
    if err := r.downloadPlugin(ctx, downloadURL, pluginPath); err != nil {
        return err
    }
    
    // Verify checksum
    if err := r.verifyChecksum(pluginPath, info.Checksum); err != nil {
        return err
    }
    
    fmt.Printf("Successfully installed %s@%s to %s\n", name, version, pluginPath)
    return nil
}
```

## Benefits

### 1. Language Independence
- **Any Language**: Developers can write plugins in their preferred language
- **No Dependency Conflicts**: Each plugin manages its own dependencies
- **Ecosystem Growth**: Easier for community contributions

### 2. Operational Excellence
- **Hot Reload**: Update plugins without pipeline restart
- **Fault Isolation**: Plugin crashes don't affect the pipeline
- **Independent Scaling**: Scale plugins separately from the core

### 3. Performance Options
- **gRPC**: Network overhead but maximum flexibility
- **WASM**: Near-native performance with sandboxing
- **Native**: Best performance for trusted code

### 4. Developer Experience
- **Simple SDKs**: Easy to create plugins in any language
- **Clear Interfaces**: Well-defined plugin contracts
- **Testing Tools**: Built-in testing support

### 5. Security
- **Sandboxing**: WASM plugins run in isolated environment
- **Process Isolation**: gRPC plugins run in separate processes
- **Resource Limits**: Prevent plugins from consuming excessive resources

## Implementation Roadmap

### Phase 1: Core Infrastructure (2 weeks)
1. **Week 1**:
   - Implement plugin interface and manager
   - Create gRPC proto definitions
   - Basic gRPC loader implementation

2. **Week 2**:
   - WASM loader implementation
   - Plugin lifecycle management
   - Health checking system

### Phase 2: Developer Tools (2 weeks)
3. **Week 3**:
   - Go SDK with examples
   - Python SDK with examples
   - Plugin testing framework

4. **Week 4**:
   - Plugin scaffolding CLI
   - Documentation and tutorials
   - Example plugins repository

### Phase 3: Production Features (2 weeks)
5. **Week 5**:
   - Hot reload implementation
   - Resource management
   - Monitoring and metrics

6. **Week 6**:
   - Plugin registry/marketplace
   - Security hardening
   - Performance optimization

### Phase 4: Advanced Features (2 weeks)
7. **Week 7**:
   - Plugin versioning system
   - Dependency resolution
   - Multi-version support

8. **Week 8**:
   - Advanced routing (A/B testing)
   - Plugin composition
   - Integration testing

## Migration Guide

### From Go Native Plugins

```go
// Old: Native Go plugin
type MyProcessor struct{}

func (p *MyProcessor) Process(msg Message) error {
    // Process message
    return nil
}

// New: gRPC plugin (main.go)
func main() {
    metadata := PluginMetadata{
        Name:    "my-processor",
        Version: "1.0.0",
        Type:    PluginTypeProcessor,
    }
    
    processor := func(ctx context.Context, msg Message) (Message, error) {
        // Same processing logic
        return msg, nil
    }
    
    cdpplugin.ServeProcessor(metadata, processor)
}
```

### Plugin Manifest

```yaml
# plugin.yaml
name: my-processor
version: 1.0.0
executable: my-processor
port: 50051

# Optional Docker deployment
docker:
  image: myorg/my-processor:1.0.0
  ports:
    "50051": "50051"
```

## Example Plugins

### 1. Fraud Detection (Python + ML)

```python
import cdpplugin
import joblib
import numpy as np

# Load ML model
model = joblib.load('fraud_model.pkl')

def detect_fraud(msg: cdpplugin.Message) -> cdpplugin.Message:
    # Extract features
    features = extract_features(msg.payload)
    
    # Predict
    fraud_score = model.predict_proba([features])[0][1]
    
    # Add to message
    msg.metadata['fraud_score'] = float(fraud_score)
    msg.metadata['fraud_detected'] = fraud_score > 0.8
    
    return msg

metadata = cdpplugin.PluginMetadata(
    name="fraud-detector",
    version="2.0.0",
    type="processor"
)

cdpplugin.serve_processor(metadata, detect_fraud)
```

### 2. High-Performance Filter (Rust/WASM)

```rust
use wasm_bindgen::prelude::*;
use serde::{Deserialize, Serialize};

#[derive(Deserialize)]
struct Config {
    min_amount: u64,
    asset_filter: Vec<String>,
}

#[derive(Deserialize, Serialize)]
struct Message {
    payload: serde_json::Value,
    metadata: std::collections::HashMap<String, String>,
}

static mut CONFIG: Option<Config> = None;

#[wasm_bindgen]
pub fn configure(config_json: &str) -> i32 {
    match serde_json::from_str(config_json) {
        Ok(config) => {
            unsafe { CONFIG = Some(config); }
            0
        }
        Err(_) => 1
    }
}

#[wasm_bindgen]
pub fn process(input_json: &str) -> String {
    let msg: Message = serde_json::from_str(input_json).unwrap();
    let config = unsafe { CONFIG.as_ref().unwrap() };
    
    // Fast filtering logic
    if let Some(amount) = msg.payload["amount"].as_u64() {
        if amount < config.min_amount {
            return "".to_string(); // Skip
        }
    }
    
    serde_json::to_string(&msg).unwrap()
}
```

### 3. Notification Service (Node.js)

```javascript
const grpc = require('@grpc/grpc-js');
const protoLoader = require('@grpc/proto-loader');
const nodemailer = require('nodemailer');

// Load proto
const packageDefinition = protoLoader.loadSync('plugin.proto');
const pluginProto = grpc.loadPackageDefinition(packageDefinition).plugin.v1;

// Email configuration
const transporter = nodemailer.createTransport({
  service: 'gmail',
  auth: {
    user: process.env.EMAIL_USER,
    pass: process.env.EMAIL_PASS
  }
});

// Plugin implementation
const server = new grpc.Server();

server.addService(pluginProto.PluginService.service, {
  GetMetadata: (call, callback) => {
    callback(null, {
      name: 'email-notifier',
      version: '1.0.0',
      type: 'consumer'
    });
  },
  
  Process: async (call, callback) => {
    const message = JSON.parse(call.request.payload);
    
    // Check if notification needed
    if (message.fraud_detected) {
      await transporter.sendMail({
        to: 'security@example.com',
        subject: 'Fraud Alert',
        text: `Fraud detected in transaction ${message.id}`
      });
    }
    
    callback(null, {
      id: call.request.id,
      type: 'notification_sent'
    });
  }
});

// Start server
const port = process.env.PLUGIN_PORT || 50051;
server.bindAsync(
  `0.0.0.0:${port}`,
  grpc.ServerCredentials.createInsecure(),
  () => {
    console.log(`Email notifier plugin listening on port ${port}`);
    server.start();
  }
);
```

## Testing Plugins

### Unit Testing

```go
func TestPlugin(t *testing.T) {
    // Start test plugin
    plugin := startTestPlugin(t)
    defer plugin.Stop()
    
    // Create test message
    input := Message{
        Type: "TestData",
        Payload: json.RawMessage(`{"value": 100}`),
    }
    
    // Process
    output, err := plugin.Process(context.Background(), input)
    require.NoError(t, err)
    
    // Verify
    assert.Equal(t, "ProcessedData", output.Type)
}
```

### Integration Testing

```yaml
# test-pipeline.yaml
plugins:
  - name: test-processor
    type: grpc
    path: ./test-plugins/processor

pipeline:
  - plugin: test-processor
    
test_cases:
  - name: "Process valid transaction"
    input:
      type: "Transaction"
      payload:
        amount: 1000
        asset: "USDC"
    expect:
      type: "ProcessedTransaction"
      metadata:
        processed: true
```

## Security Best Practices

### 1. Plugin Signing
```bash
# Sign plugin
flowctl plugin sign --key mykey.pem --plugin ./my-plugin

# Verify signature
flowctl plugin verify --plugin ./my-plugin
```

### 2. Resource Limits
```yaml
plugins:
  - name: untrusted-plugin
    type: wasm
    path: ./plugins/untrusted
    limits:
      memory: 100MB
      cpu: 0.5
      timeout: 30s
```

### 3. Network Policies
```yaml
# Kubernetes NetworkPolicy
apiVersion: networking.k8s.io/v1
kind: NetworkPolicy
metadata:
  name: plugin-network-policy
spec:
  podSelector:
    matchLabels:
      app: plugin
  policyTypes:
  - Ingress
  - Egress
  ingress:
  - from:
    - podSelector:
        matchLabels:
          app: cdp-pipeline
  egress:
  - to:
    - podSelector:
        matchLabels:
          app: cdp-pipeline
```

## Monitoring & Observability

### Plugin Metrics
```go
type PluginMetrics struct {
    // Counters
    ProcessedMessages  prometheus.Counter
    ProcessingErrors   prometheus.Counter
    
    // Histograms
    ProcessingDuration prometheus.Histogram
    MessageSize        prometheus.Histogram
    
    // Gauges
    HealthStatus       prometheus.Gauge
    MemoryUsage        prometheus.Gauge
}
```

### Dashboard Example
```json
{
  "dashboard": {
    "title": "CDP Plugin Performance",
    "panels": [
      {
        "title": "Plugin Processing Rate",
        "targets": [
          {
            "expr": "rate(cdp_plugin_processed_messages_total[5m])"
          }
        ]
      },
      {
        "title": "Plugin Errors",
        "targets": [
          {
            "expr": "rate(cdp_plugin_processing_errors_total[5m])"
          }
        ]
      }
    ]
  }
}
```

## Conclusion

This plugin system provides CDP Pipeline Workflow with a robust, language-agnostic extension mechanism that avoids the pitfalls of previous implementations. By supporting both gRPC and WebAssembly plugins, developers can choose the right tool for their use case while maintaining system stability and performance.

The clear benefits include:
- **No dependency conflicts** - Each plugin manages its own dependencies
- **Language freedom** - Write plugins in any language
- **Production ready** - Hot reload, health checks, and monitoring
- **Developer friendly** - Simple SDKs and clear documentation
- **Secure by default** - Sandboxing and resource limits

With this architecture, CDP Pipeline Workflow can grow a rich ecosystem of plugins while maintaining its core focus on Stellar blockchain data processing.