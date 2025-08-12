# CDP Pipeline - Flowctl Control Plane Integration Guide

## Table of Contents
1. [Overview](#overview)
2. [Architecture](#architecture)
3. [Implementation Plan](#implementation-plan)
4. [CDP Pipeline Changes](#cdp-pipeline-changes)
5. [Flowctl Integration](#flowctl-integration)
6. [Configuration](#configuration)
7. [Testing](#testing)
8. [Deployment](#deployment)
9. [Monitoring and Operations](#monitoring-and-operations)

## Overview

This document outlines the implementation of control plane capabilities for CDP Pipeline by integrating with flowctl. The approach treats each CDP pipeline as a single data plane unit that registers with flowctl's control plane for monitoring, health checks, and status reporting.

### Goals
- Enable centralized monitoring of CDP pipelines via flowctl
- Maintain CDP's high-performance, zero-overhead internal architecture
- Provide operational visibility without significant code changes
- Support status queries and health monitoring for running pipelines

### Non-Goals
- Rewriting CDP components as individual flowctl services
- Breaking apart CDP's monolithic architecture
- Implementing distributed processing within CDP pipelines

## Architecture

### High-Level Design

```
┌─────────────────────────────────────────────────────────────┐
│                    Flowctl Control Plane                     │
│                                                              │
│  ┌─────────────┐  ┌──────────────┐  ┌─────────────────┐    │
│  │  Registry   │  │Health Monitor│  │  Status API     │    │
│  └─────────────┘  └──────────────┘  └─────────────────┘    │
└────────────────────────┬───────────────────────────────────┘
                         │ gRPC
     ┌───────────────────┴────────────────────┐
     │                                        │
┌────▼──────────────┐            ┌───────────▼──────────────┐
│  CDP Pipeline #1  │            │    CDP Pipeline #2       │
│                   │            │                          │
│ ┌───────────────┐ │            │ ┌────────────────────┐  │
│ │Control Client │ │            │ │ Control Client     │  │
│ └───────┬───────┘ │            │ └────────┬───────────┘  │
│         │         │            │          │              │
│  Source→Proc→Sink │            │  Source→Proc→Sink       │
└───────────────────┘            └──────────────────────────┘
```

### Component Interactions

1. **CDP Pipeline** starts up and creates a control plane client
2. **Control Client** registers the pipeline with flowctl
3. **Heartbeat Loop** sends periodic health updates and metrics
4. **Status Queries** through flowctl CLI return aggregated pipeline stats

## Implementation Plan

### Phase 1: Core Integration (Week 1)
- [ ] Create control plane client package in CDP
- [ ] Implement service registration
- [ ] Add heartbeat mechanism
- [ ] Create stats aggregation interface

### Phase 2: Metrics Collection (Week 2)
- [ ] Standardize stats interface across components
- [ ] Implement pipeline-wide stats aggregation
- [ ] Add health check logic
- [ ] Create metrics transformation for flowctl

### Phase 3: Testing & Documentation (Week 3)
- [ ] Integration tests with flowctl
- [ ] Performance impact testing
- [ ] Operational documentation
- [ ] Example configurations

## CDP Pipeline Changes

### 1. Control Plane Client Package

Create `pkg/control/client.go`:

```go
package control

import (
    "context"
    "fmt"
    "time"
    
    "google.golang.org/grpc"
    "google.golang.org/grpc/credentials/insecure"
    pb "github.com/withObsrvr/flowctl/api/v1"
)

type Client struct {
    conn            *grpc.ClientConn
    client          pb.ControlPlaneClient
    serviceInfo     *pb.ServiceInfo
    metricsProvider MetricsProvider
    healthChecker   HealthChecker
}

type MetricsProvider interface {
    GetMetrics() map[string]float64
}

type HealthChecker interface {
    IsHealthy() bool
    GetHealthDetails() map[string]string
}

func NewClient(endpoint string, serviceID string, serviceName string) (*Client, error) {
    conn, err := grpc.Dial(endpoint, grpc.WithTransportCredentials(insecure.NewCredentials()))
    if err != nil {
        return nil, fmt.Errorf("failed to connect to control plane: %w", err)
    }
    
    return &Client{
        conn:   conn,
        client: pb.NewControlPlaneClient(conn),
        serviceInfo: &pb.ServiceInfo{
            Id:   serviceID,
            Name: serviceName,
            Type: "cdp-pipeline",
        },
    }, nil
}

func (c *Client) Register(ctx context.Context, metadata map[string]string) error {
    c.serviceInfo.Metadata = metadata
    
    ack, err := c.client.Register(ctx, c.serviceInfo)
    if err != nil {
        return fmt.Errorf("registration failed: %w", err)
    }
    
    if !ack.Success {
        return fmt.Errorf("registration rejected: %s", ack.Message)
    }
    
    return nil
}

func (c *Client) StartHeartbeat(ctx context.Context, interval time.Duration) {
    ticker := time.NewTicker(interval)
    defer ticker.Stop()
    
    for {
        select {
        case <-ctx.Done():
            return
        case <-ticker.C:
            c.sendHeartbeat(ctx)
        }
    }
}

func (c *Client) sendHeartbeat(ctx context.Context) {
    status := "healthy"
    if c.healthChecker != nil && !c.healthChecker.IsHealthy() {
        status = "unhealthy"
    }
    
    var metrics []*pb.Metric
    if c.metricsProvider != nil {
        for name, value := range c.metricsProvider.GetMetrics() {
            metrics = append(metrics, &pb.Metric{
                Name:  name,
                Value: value,
            })
        }
    }
    
    heartbeat := &pb.ServiceHeartbeat{
        ServiceId: c.serviceInfo.Id,
        Timestamp: time.Now().Unix(),
        Status:    status,
        Metrics:   metrics,
    }
    
    _, err := c.client.Heartbeat(ctx, heartbeat)
    if err != nil {
        // Log error but don't stop heartbeat loop
        fmt.Printf("Failed to send heartbeat: %v\n", err)
    }
}

func (c *Client) Close() error {
    return c.conn.Close()
}
```

### 2. Stats Aggregation Interface

Create `pkg/control/stats.go`:

```go
package control

import (
    "sync"
    "time"
)

// StatsProvider is implemented by components that track statistics
type StatsProvider interface {
    GetStats() ComponentStats
}

// ComponentStats represents statistics from a single component
type ComponentStats struct {
    ComponentType string
    ComponentName string
    Stats         map[string]interface{}
    LastUpdated   time.Time
}

// PipelineStats aggregates stats from all components
type PipelineStats struct {
    mu         sync.RWMutex
    PipelineID string
    StartTime  time.Time
    Components []ComponentStats
}

func NewPipelineStats(pipelineID string) *PipelineStats {
    return &PipelineStats{
        PipelineID: pipelineID,
        StartTime:  time.Now(),
        Components: make([]ComponentStats, 0),
    }
}

func (ps *PipelineStats) UpdateComponentStats(stats ComponentStats) {
    ps.mu.Lock()
    defer ps.mu.Unlock()
    
    // Update existing or append new
    found := false
    for i, cs := range ps.Components {
        if cs.ComponentName == stats.ComponentName {
            ps.Components[i] = stats
            found = true
            break
        }
    }
    
    if !found {
        ps.Components = append(ps.Components, stats)
    }
}

func (ps *PipelineStats) GetMetrics() map[string]float64 {
    ps.mu.RLock()
    defer ps.mu.RUnlock()
    
    metrics := make(map[string]float64)
    
    // Pipeline-level metrics
    metrics["pipeline.uptime_seconds"] = time.Since(ps.StartTime).Seconds()
    metrics["pipeline.component_count"] = float64(len(ps.Components))
    
    // Component-level metrics
    for _, comp := range ps.Components {
        prefix := comp.ComponentType + "." + comp.ComponentName
        for key, value := range comp.Stats {
            if v, ok := value.(float64); ok {
                metrics[prefix+"."+key] = v
            } else if v, ok := value.(int); ok {
                metrics[prefix+"."+key] = float64(v)
            } else if v, ok := value.(uint32); ok {
                metrics[prefix+"."+key] = float64(v)
            } else if v, ok := value.(uint64); ok {
                metrics[prefix+"."+key] = float64(v)
            }
        }
    }
    
    return metrics
}

func (ps *PipelineStats) IsHealthy() bool {
    ps.mu.RLock()
    defer ps.mu.RUnlock()
    
    // Pipeline is healthy if all components reported stats recently
    for _, comp := range ps.Components {
        if time.Since(comp.LastUpdated) > 30*time.Second {
            return false
        }
    }
    
    return len(ps.Components) > 0
}

func (ps *PipelineStats) GetHealthDetails() map[string]string {
    ps.mu.RLock()
    defer ps.mu.RUnlock()
    
    details := make(map[string]string)
    details["pipeline_id"] = ps.PipelineID
    details["uptime"] = time.Since(ps.StartTime).String()
    details["component_count"] = fmt.Sprintf("%d", len(ps.Components))
    
    for _, comp := range ps.Components {
        status := "healthy"
        if time.Since(comp.LastUpdated) > 30*time.Second {
            status = "stale"
        }
        details[comp.ComponentName+"_status"] = status
    }
    
    return details
}
```

### 3. Update Main Pipeline Runner

Modify `main.go` to include control plane integration:

```go
package main

import (
    "context"
    "fmt"
    "os"
    "time"
    
    "github.com/withObsrvr/cdp-pipeline-workflow/pkg/control"
    // ... other imports
)

func main() {
    // Existing configuration loading...
    config, err := loadConfig(*configFile)
    if err != nil {
        log.Fatal(err)
    }
    
    // Create pipeline ID
    pipelineID := fmt.Sprintf("cdp-%s-%d", config.Name, time.Now().Unix())
    
    // Initialize pipeline stats
    pipelineStats := control.NewPipelineStats(pipelineID)
    
    // Setup control plane client if endpoint is provided
    var controlClient *control.Client
    if endpoint := os.Getenv("FLOWCTL_ENDPOINT"); endpoint != "" {
        controlClient, err = control.NewClient(endpoint, pipelineID, config.Name)
        if err != nil {
            log.Printf("Failed to create control plane client: %v", err)
            // Continue without control plane
        } else {
            // Set metrics and health providers
            controlClient.SetMetricsProvider(pipelineStats)
            controlClient.SetHealthChecker(pipelineStats)
            
            // Register with control plane
            metadata := map[string]string{
                "config_file": *configFile,
                "version":     VERSION,
                "network":     config.Network,
            }
            
            if err := controlClient.Register(ctx, metadata); err != nil {
                log.Printf("Failed to register with control plane: %v", err)
            } else {
                // Start heartbeat loop
                go controlClient.StartHeartbeat(ctx, 10*time.Second)
                defer controlClient.Close()
            }
        }
    }
    
    // Create pipeline components with stats collection
    pipeline, err := createPipeline(config, pipelineStats)
    if err != nil {
        log.Fatal(err)
    }
    
    // Run pipeline
    if err := pipeline.Run(ctx); err != nil {
        log.Fatal(err)
    }
}

func createPipeline(config *Config, stats *control.PipelineStats) (*Pipeline, error) {
    // Create source with stats wrapper
    source, err := createSourceAdapter(config.Source)
    if err != nil {
        return nil, err
    }
    
    // Wrap source with stats collector
    if sp, ok := source.(control.StatsProvider); ok {
        go collectStats(ctx, "source", config.Source.Type, sp, stats)
    }
    
    // Similar for processors and consumers...
    
    return &Pipeline{
        Source:     source,
        Processors: processors,
        Consumers:  consumers,
    }, nil
}

func collectStats(ctx context.Context, componentType, componentName string, 
    provider control.StatsProvider, pipelineStats *control.PipelineStats) {
    
    ticker := time.NewTicker(5 * time.Second)
    defer ticker.Stop()
    
    for {
        select {
        case <-ctx.Done():
            return
        case <-ticker.C:
            stats := provider.GetStats()
            compStats := control.ComponentStats{
                ComponentType: componentType,
                ComponentName: componentName,
                Stats:         stats.ToMap(), // Convert to map[string]interface{}
                LastUpdated:   time.Now(),
            }
            pipelineStats.UpdateComponentStats(compStats)
        }
    }
}
```

### 4. Update Existing Components

Add stats interface to components. Example for `ContractEventProcessor`:

```go
// Add interface implementation
func (p *ContractEventProcessor) GetStats() control.ComponentStats {
    p.mu.RLock()
    defer p.mu.RUnlock()
    
    return control.ComponentStats{
        ComponentType: "processor",
        ComponentName: "ContractEventProcessor",
        Stats: map[string]interface{}{
            "processed_ledgers":   p.stats.ProcessedLedgers,
            "events_found":        p.stats.EventsFound,
            "successful_events":   p.stats.SuccessfulEvents,
            "failed_events":       p.stats.FailedEvents,
            "last_ledger":         p.stats.LastLedger,
            "last_processed_time": p.stats.LastProcessedTime.Unix(),
        },
        LastUpdated: time.Now(),
    }
}
```

## Flowctl Integration

### 1. Ensure CDP Pipeline Type is Recognized

Add to flowctl's service type definitions:

```go
// In flowctl service types
const (
    ServiceTypeProcessor = "processor"
    ServiceTypeSink      = "sink"
    ServiceTypeSource    = "source"
    ServiceTypeCDP       = "cdp-pipeline" // Add this
)
```

### 2. Update Status Display

Enhance flowctl's status command to handle CDP pipelines:

```go
func formatServiceStatus(service *ServiceInfo) string {
    if service.Type == "cdp-pipeline" {
        // Special formatting for CDP pipelines
        return formatCDPPipelineStatus(service)
    }
    // Default formatting
    return formatDefaultStatus(service)
}

func formatCDPPipelineStatus(service *ServiceInfo) string {
    // Extract CDP-specific metadata
    configFile := service.Metadata["config_file"]
    network := service.Metadata["network"]
    
    // Format with additional details
    return fmt.Sprintf("CDP Pipeline: %s (config: %s, network: %s)",
        service.Name, configFile, network)
}
```

## Configuration

### 1. CDP Pipeline Configuration

No changes needed to existing CDP YAML files. Control plane integration is configured via environment variables:

```bash
# Enable control plane integration
export FLOWCTL_ENDPOINT=localhost:8080

# Optional: Override pipeline name
export CDP_PIPELINE_NAME=contract-events-prod

# Run pipeline as normal
./cdp-pipeline-workflow -config contract_events.yaml
```

### 2. Docker Compose Example

```yaml
version: '3.8'

services:
  flowctl:
    image: withobsrvr/flowctl:latest
    ports:
      - "8080:8080"
    command: ["server", "--storage-type", "boltdb"]
    
  cdp-pipeline-events:
    image: withobsrvr/cdp-pipeline:latest
    environment:
      - FLOWCTL_ENDPOINT=flowctl:8080
      - CDP_PIPELINE_NAME=contract-events
    volumes:
      - ./config/contract_events.yaml:/config/pipeline.yaml
    command: ["-config", "/config/pipeline.yaml"]
    depends_on:
      - flowctl
      
  cdp-pipeline-payments:
    image: withobsrvr/cdp-pipeline:latest
    environment:
      - FLOWCTL_ENDPOINT=flowctl:8080
      - CDP_PIPELINE_NAME=payment-filter
    volumes:
      - ./config/payment_filter.yaml:/config/pipeline.yaml
    command: ["-config", "/config/pipeline.yaml"]
    depends_on:
      - flowctl
```

### 3. Kubernetes Deployment

```yaml
apiVersion: apps/v1
kind: Deployment
metadata:
  name: cdp-contract-events
spec:
  replicas: 1
  selector:
    matchLabels:
      app: cdp-contract-events
  template:
    metadata:
      labels:
        app: cdp-contract-events
    spec:
      containers:
      - name: cdp-pipeline
        image: withobsrvr/cdp-pipeline:latest
        env:
        - name: FLOWCTL_ENDPOINT
          value: "flowctl-service:8080"
        - name: CDP_PIPELINE_NAME
          value: "contract-events"
        volumeMounts:
        - name: config
          mountPath: /config
        args: ["-config", "/config/pipeline.yaml"]
      volumes:
      - name: config
        configMap:
          name: cdp-pipeline-config
```

## Testing

### 1. Unit Tests

```go
// pkg/control/client_test.go
func TestControlPlaneClient(t *testing.T) {
    // Test registration
    client, _ := NewClient("localhost:8080", "test-id", "test-pipeline")
    
    ctx := context.Background()
    err := client.Register(ctx, map[string]string{"test": "true"})
    assert.NoError(t, err)
    
    // Test heartbeat
    client.SetMetricsProvider(&mockMetricsProvider{})
    client.sendHeartbeat(ctx)
    // Verify heartbeat was sent
}

// Test stats aggregation
func TestPipelineStats(t *testing.T) {
    stats := NewPipelineStats("test-pipeline")
    
    // Add component stats
    stats.UpdateComponentStats(ComponentStats{
        ComponentType: "source",
        ComponentName: "TestSource",
        Stats: map[string]interface{}{
            "processed": 100,
            "errors":    2,
        },
    })
    
    metrics := stats.GetMetrics()
    assert.Equal(t, float64(100), metrics["source.TestSource.processed"])
    assert.Equal(t, float64(2), metrics["source.TestSource.errors"])
}
```

### 2. Integration Tests

```bash
#!/bin/bash
# test/integration/test_control_plane.sh

# Start flowctl
docker run -d --name flowctl-test -p 8080:8080 withobsrvr/flowctl:latest

# Wait for startup
sleep 5

# Run CDP pipeline with control plane
FLOWCTL_ENDPOINT=localhost:8080 ./cdp-pipeline-workflow \
    -config test/fixtures/test_pipeline.yaml &
CDP_PID=$!

# Wait for registration
sleep 10

# Check status
flowctl status | grep -q "test-pipeline"
if [ $? -eq 0 ]; then
    echo "✓ Pipeline registered successfully"
else
    echo "✗ Pipeline registration failed"
    exit 1
fi

# Check metrics
flowctl status test-pipeline --json | jq '.metrics' | grep -q "processed_ledgers"
if [ $? -eq 0 ]; then
    echo "✓ Metrics are being reported"
else
    echo "✗ Metrics not found"
    exit 1
fi

# Cleanup
kill $CDP_PID
docker stop flowctl-test
docker rm flowctl-test
```

### 3. Performance Testing

```go
// Benchmark control plane overhead
func BenchmarkWithControlPlane(b *testing.B) {
    // Run pipeline with control plane
    // Measure throughput and latency
}

func BenchmarkWithoutControlPlane(b *testing.B) {
    // Run pipeline without control plane
    // Compare metrics
}
```

## Deployment

### 1. Rolling Update Strategy

Since CDP pipelines are stateful (processing sequential ledgers), use careful deployment:

```yaml
# Kubernetes rolling update
spec:
  strategy:
    type: RollingUpdate
    rollingUpdate:
      maxUnavailable: 0
      maxSurge: 1
```

### 2. Environment-Specific Configuration

```bash
# Development
export FLOWCTL_ENDPOINT=localhost:8080

# Staging
export FLOWCTL_ENDPOINT=flowctl-staging.internal:8080

# Production
export FLOWCTL_ENDPOINT=flowctl-prod.internal:8080
export FLOWCTL_TLS_CERT=/certs/client.crt
export FLOWCTL_TLS_KEY=/certs/client.key
```

### 3. Monitoring Setup

Configure Prometheus to scrape flowctl metrics:

```yaml
# prometheus.yml
scrape_configs:
  - job_name: 'flowctl'
    static_configs:
      - targets: ['flowctl:9090']
    
  - job_name: 'cdp-pipelines'
    metric_relabel_configs:
      - source_labels: [__name__]
        regex: 'cdp_pipeline_.*'
        action: keep
```

## Monitoring and Operations

### 1. Status Commands

```bash
# List all pipelines
$ flowctl status
ID                          TYPE           STATUS    UPTIME
cdp-contract-events-123     cdp-pipeline   healthy   4h 23m
cdp-payments-456            cdp-pipeline   healthy   4h 23m
ttp-processor-789           processor      healthy   2h 10m

# Get detailed pipeline status
$ flowctl status cdp-contract-events-123
Pipeline: cdp-contract-events-123
Type: cdp-pipeline
Status: healthy
Config: contract_events.yaml
Network: mainnet
Uptime: 4h 23m

Metrics:
  pipeline.uptime_seconds: 15780
  pipeline.component_count: 3
  source.BufferedStorageSourceAdapter.processed_ledgers: 125000
  source.BufferedStorageSourceAdapter.current_ledger: 52125000
  processor.ContractEventProcessor.events_found: 45678
  processor.ContractEventProcessor.successful_events: 45600
  consumer.SaveToParquet.files_written: 234
  consumer.SaveToParquet.total_bytes: 48318382080

Health Details:
  BufferedStorageSourceAdapter_status: healthy
  ContractEventProcessor_status: healthy
  SaveToParquet_status: healthy

# Get metrics in JSON format
$ flowctl status cdp-contract-events-123 --json | jq '.metrics'
```

### 2. Health Checks

```bash
# Check if pipeline is healthy
$ flowctl health cdp-contract-events-123
✓ cdp-contract-events-123 is healthy

# Set up health check alerts
$ flowctl alert create \
    --service cdp-contract-events-123 \
    --condition "status != healthy" \
    --webhook https://alerts.example.com/webhook
```

### 3. Troubleshooting

Common issues and solutions:

**Pipeline not appearing in flowctl status:**
- Check FLOWCTL_ENDPOINT is set correctly
- Verify network connectivity to flowctl
- Check CDP pipeline logs for registration errors

**Metrics not updating:**
- Ensure components implement StatsProvider interface
- Check stats collection goroutines are running
- Verify no panics in stats collection

**Health showing as unhealthy:**
- Check component last update times
- Verify all expected components are reporting
- Review pipeline logs for processing errors

### 4. Operational Runbook

**Starting a CDP Pipeline with Monitoring:**
```bash
# 1. Ensure flowctl is running
flowctl health

# 2. Set environment
export FLOWCTL_ENDPOINT=flowctl:8080

# 3. Start pipeline
./cdp-pipeline-workflow -config pipeline.yaml

# 4. Verify registration (within 30s)
flowctl status | grep <pipeline-name>

# 5. Monitor metrics
watch -n 5 'flowctl status <pipeline-id> | grep processed_ledgers'
```

**Investigating Issues:**
```bash
# 1. Check pipeline health
flowctl health <pipeline-id>

# 2. Get detailed status
flowctl status <pipeline-id> --verbose

# 3. Check pipeline logs
kubectl logs <pod-name> | grep ERROR

# 4. Compare metrics over time
flowctl metrics <pipeline-id> --duration 1h
```

## Next Steps

After implementing this integration:

1. **Add Custom Metrics**: Extend components to expose business-specific metrics
2. **Historical Metrics**: Integrate with Prometheus for long-term storage
3. **Alerting**: Set up PagerDuty/Slack alerts based on pipeline health
4. **Dashboard**: Create Grafana dashboards using flowctl metrics
5. **Multi-Region**: Support for cross-region control plane communication

## Conclusion

This integration provides comprehensive monitoring and control capabilities for CDP pipelines while maintaining their high-performance characteristics. The implementation is non-invasive, backward-compatible, and can be rolled out gradually across existing pipelines.