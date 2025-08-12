# Control Plane Integration for CDP Pipeline

## Overview

CDP Pipeline now supports integration with flowctl's control plane for centralized monitoring and status reporting. This integration allows you to:

- Monitor multiple CDP pipelines from a single control plane
- View real-time metrics and health status
- Query pipeline status via flowctl CLI
- Track component-level statistics

## Quick Start

### 1. Start flowctl server

```bash
flowctl server --storage-type boltdb
```

### 2. Run CDP pipeline with control plane

```bash
export FLOWCTL_ENDPOINT=localhost:8080
./cdp-pipeline-workflow -config config/contract_events.yaml
```

### 3. Check pipeline status

```bash
# List all registered services
flowctl status

# Get detailed pipeline status
flowctl status cdp-contract-events-1234567890
```

## How It Works

1. **Automatic Registration**: When `FLOWCTL_ENDPOINT` is set, CDP pipeline automatically registers with the control plane
2. **Heartbeat Loop**: Sends health updates every 10 seconds
3. **Metrics Collection**: Components that implement `StatsProvider` interface report metrics
4. **No Performance Impact**: Control plane communication runs in separate goroutines

## Configuration

Control plane integration is configured via environment variables:

```bash
# Required: Control plane endpoint
export FLOWCTL_ENDPOINT=localhost:8080

# Optional: Override pipeline name
export CDP_PIPELINE_NAME=my-custom-pipeline
```

## Implementation Details

### Components Modified

1. **main.go**: Added control plane client initialization in `setupPipeline()`
2. **pkg/control/client.go**: gRPC client for flowctl communication
3. **pkg/control/stats.go**: Stats aggregation and health checking
4. **processor/processor_contract_events.go**: Example StatsProvider implementation

### Adding Stats to Components

To make a component report stats, implement the `control.StatsProvider` interface:

```go
func (c *MyComponent) GetStats() control.ComponentStats {
    return control.ComponentStats{
        ComponentType: "processor",
        ComponentName: "MyComponent",
        Stats: map[string]interface{}{
            "processed": c.processedCount,
            "errors": c.errorCount,
        },
        LastUpdated: time.Now(),
    }
}
```

## Docker Example

```yaml
version: '3.8'

services:
  flowctl:
    image: withobsrvr/flowctl:latest
    ports:
      - "8080:8080"
    command: ["server", "--storage-type", "boltdb"]
    
  cdp-pipeline:
    image: withobsrvr/cdp-pipeline:latest
    environment:
      - FLOWCTL_ENDPOINT=flowctl:8080
    volumes:
      - ./config:/config
    command: ["-config", "/config/pipeline.yaml"]
    depends_on:
      - flowctl
```

## Metrics Available

When integrated with flowctl, CDP pipelines report:

- **Pipeline-level metrics**:
  - `pipeline.uptime_seconds`: Time since pipeline started
  - `pipeline.component_count`: Number of active components

- **Component-level metrics** (example from ContractEventProcessor):
  - `processor.ContractEventProcessor.processed_ledgers`: Total ledgers processed
  - `processor.ContractEventProcessor.events_found`: Total events found
  - `processor.ContractEventProcessor.successful_events`: Events in successful transactions
  - `processor.ContractEventProcessor.failed_events`: Events in failed transactions

## Troubleshooting

### Pipeline not appearing in flowctl status

1. Check `FLOWCTL_ENDPOINT` is set correctly
2. Verify network connectivity to flowctl
3. Check CDP pipeline logs for registration errors

### No metrics showing

1. Ensure components implement `StatsProvider` interface
2. Check that stats collection goroutines are running
3. Wait at least 10 seconds for first heartbeat

### Registration fails

1. Verify flowctl is running and accessible
2. Check firewall/network settings
3. Review CDP pipeline logs for specific error messages

## Next Steps

- Add StatsProvider to more components (sources, consumers)
- Implement custom business metrics
- Set up alerts based on pipeline health
- Create Grafana dashboards using flowctl metrics