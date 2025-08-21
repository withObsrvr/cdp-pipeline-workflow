# Flowctl as AI-Native Control Plane for CDP Pipelines

## Executive Summary

This document outlines how to leverage and enhance flowctl as the AI-native control plane for CDP Pipeline Workflow, avoiding duplication of functionality and creating a clean separation of concerns. Instead of implementing AI-native features directly in CDP pipelines, we use flowctl's existing infrastructure and extend it with CDP-specific capabilities.

## Architecture Overview

```
┌────────────────────────────────────────────────────────────────┐
│                        AI Agents                                │
│              (Claude, GPT-4, Custom Agents)                     │
└────────────────────────┬───────────────────────────────────────┘
                         │
                    REST/gRPC API
                         │
┌────────────────────────┴───────────────────────────────────────┐
│                   Flowctl Control Plane                         │
│  ┌─────────────┐  ┌──────────────┐  ┌─────────────────┐      │
│  │ Tool Cards  │  │ Plan/Apply   │  │ Policy Engine   │      │
│  └─────────────┘  └──────────────┘  └─────────────────┘      │
│  ┌─────────────┐  ┌──────────────┐  ┌─────────────────┐      │
│  │ DAG Engine  │  │ Embedded CP  │  │ Cost Tracking   │      │
│  └─────────────┘  └──────────────┘  └─────────────────┘      │
│  ┌─────────────┐  ┌──────────────┐  ┌─────────────────┐      │
│  │Orchestrator │  │ Storage API  │  │ Monitoring      │      │
│  └─────────────┘  └──────────────┘  └─────────────────┘      │
└────────────────────────────────────────────────────────────────┘
                         │
                    gRPC Protocol
                         │
┌────────────────────────┴───────────────────────────────────────┐
│                 CDP Pipeline Components                         │
│  ┌─────────────┐  ┌──────────────┐  ┌─────────────────┐      │
│  │  Sources    │  │  Processors  │  │   Consumers     │      │
│  │  (S3, GCS)  │  │ (Transform)  │  │  (Database)     │      │
│  └─────────────┘  └──────────────┘  └─────────────────┘      │
└────────────────────────────────────────────────────────────────┘
```

## Key Advantages of Using Flowctl

### Already Implemented in Flowctl:
1. **gRPC Control Plane Protocol** - Service registration, heartbeat, health checks
2. **Embedded Control Plane** - Can run standalone or embedded
3. **DAG Processing** - Complex pipeline orchestration
4. **Multiple Execution Drivers** - Docker, Kubernetes, Nomad support
5. **Storage Abstraction** - BoltDB and memory implementations
6. **Component Registry** - Service discovery and management
7. **Pipeline Runner** - Lifecycle management
8. **AI Agent Implementation Guide** - Comprehensive AI integration plan

### CDP Pipeline Focus:
1. **Stellar-Specific Processing** - Ledger processing, contract events
2. **Domain Logic** - Payment filtering, account monitoring
3. **Data Transformations** - Stellar XDR to application formats
4. **Storage Adapters** - S3, GCS, filesystem sources

## Implementation Plan

### Phase 1: CDP Component Tool Cards (Week 1)

Create Tool Cards for all CDP pipeline components in flowctl:

```go
// flowctl/internal/cdp/toolcards.go
package cdp

import (
    "github.com/withobsrvr/flowctl/internal/ai/tools"
)

func RegisterCDPTools() []tools.Tool {
    return []tools.Tool{
        {
            Type: "function",
            Function: tools.Function{
                Name:        "create_stellar_pipeline",
                Description: "Create a Stellar blockchain data processing pipeline",
                Parameters: tools.Parameters{
                    Type: "object",
                    Properties: map[string]tools.Property{
                        "name": {
                            Type:        "string",
                            Description: "Pipeline name",
                        },
                        "source": {
                            Type:        "object",
                            Description: "Stellar data source configuration",
                            Properties: map[string]tools.Property{
                                "type": {
                                    Type: "string",
                                    Enum: []string{
                                        "BufferedStorageSourceAdapter",
                                        "CaptiveCoreInboundAdapter",
                                        "RPCSourceAdapter",
                                    },
                                },
                                "bucket_name": {
                                    Type:        "string",
                                    Description: "S3/GCS bucket containing Stellar ledgers",
                                },
                                "network": {
                                    Type: "string",
                                    Enum: []string{"mainnet", "testnet"},
                                },
                                "start_ledger": {
                                    Type:        "integer",
                                    Description: "Starting ledger number",
                                },
                            },
                        },
                        "processors": {
                            Type:        "array",
                            Description: "Stellar data processors",
                            Items: {
                                Type: "object",
                                Properties: map[string]tools.Property{
                                    "type": {
                                        Type: "string",
                                        Enum: []string{
                                            "ContractEvent",
                                            "FilterPayments",
                                            "AccountData",
                                            "TransformToAppPayment",
                                        },
                                    },
                                },
                            },
                        },
                    },
                    Required: []string{"name", "source"},
                },
            },
        },
    }
}
```

### Phase 2: CDP Pipeline Registry (Week 2)

Extend flowctl's component registry with CDP-specific metadata:

```go
// flowctl/internal/cdp/registry.go
type CDPComponentRegistry struct {
    base *ComponentRegistry
}

type CDPComponentMetadata struct {
    ComponentMetadata
    
    // CDP-specific fields
    NetworkSupport    []string `json:"network_support"`    // mainnet, testnet
    StellarVersion    string   `json:"stellar_version"`    
    ProcessingMode    string   `json:"processing_mode"`    // streaming, batch
    DataTypes         []string `json:"data_types"`         // ledgers, transactions, events
    ResourceEstimates struct {
        MemoryMB      int     `json:"memory_mb"`
        CPUCores      float64 `json:"cpu_cores"`
        ThroughputTPS int     `json:"throughput_tps"`
    } `json:"resource_estimates"`
}

// Register all CDP components
func (r *CDPComponentRegistry) RegisterDefaults() error {
    components := []CDPComponentMetadata{
        {
            ComponentMetadata: ComponentMetadata{
                ID:          "buffered-storage-source",
                Type:        "source",
                Name:        "Buffered Storage Source",
                Description: "Reads Stellar ledgers from S3/GCS",
                Version:     "1.0.0",
            },
            NetworkSupport: []string{"mainnet", "testnet"},
            StellarVersion: "v20.0.0",
            ProcessingMode: "batch",
            DataTypes:      []string{"ledgers"},
            ResourceEstimates: ResourceEstimates{
                MemoryMB:      512,
                CPUCores:      1.0,
                ThroughputTPS: 1000,
            },
        },
        // ... more components
    }
    
    for _, comp := range components {
        if err := r.Register(comp); err != nil {
            return err
        }
    }
    
    return nil
}
```

### Phase 3: Plan/Apply for CDP Pipelines (Week 3)

Implement CDP-specific plan/apply logic in flowctl:

```go
// flowctl/internal/cdp/planner.go
type CDPPipelinePlanner struct {
    validator  CDPValidator
    estimator  CostEstimator
    optimizer  PipelineOptimizer
}

type CDPPipelinePlan struct {
    PipelinePlan
    
    // CDP-specific planning
    LedgerRange    LedgerRange           `json:"ledger_range"`
    EstimatedCost  CostEstimate          `json:"estimated_cost"`
    ProcessingTime time.Duration         `json:"processing_time"`
    DataVolume     DataVolumeEstimate    `json:"data_volume"`
    Optimizations  []OptimizationSuggestion `json:"optimizations"`
}

func (p *CDPPipelinePlanner) CreatePlan(config PipelineConfig) (*CDPPipelinePlan, error) {
    // Validate CDP-specific configuration
    if err := p.validator.ValidateStellarConfig(config); err != nil {
        return nil, fmt.Errorf("invalid Stellar configuration: %w", err)
    }
    
    // Calculate ledger processing estimates
    ledgerRange := p.extractLedgerRange(config)
    dataVolume := p.estimateDataVolume(ledgerRange)
    
    // Estimate costs based on cloud provider
    costEstimate := p.estimator.EstimateCost(CostRequest{
        DataVolume:    dataVolume,
        ProcessingTime: p.estimateProcessingTime(dataVolume),
        CloudProvider: p.detectCloudProvider(config),
    })
    
    // Generate optimizations
    optimizations := p.optimizer.SuggestOptimizations(config, costEstimate)
    
    return &CDPPipelinePlan{
        PipelinePlan: PipelinePlan{
            Config:  config,
            Actions: p.generateActions(config),
        },
        LedgerRange:    ledgerRange,
        EstimatedCost:  costEstimate,
        ProcessingTime: p.estimateProcessingTime(dataVolume),
        DataVolume:     dataVolume,
        Optimizations:  optimizations,
    }, nil
}
```

### Phase 4: Enhanced Monitoring for CDP (Week 4)

Add CDP-specific metrics to flowctl's monitoring:

```go
// flowctl/internal/cdp/metrics.go
type CDPMetricsCollector struct {
    base MetricsCollector
}

func (c *CDPMetricsCollector) RegisterMetrics() {
    // Ledger processing metrics
    c.RegisterCounter("cdp_ledgers_processed_total", 
        "Total number of Stellar ledgers processed",
        []string{"network", "pipeline", "source"})
    
    c.RegisterHistogram("cdp_ledger_processing_duration_seconds",
        "Time to process a single ledger",
        []string{"network", "pipeline"})
    
    // Transaction metrics
    c.RegisterCounter("cdp_transactions_processed_total",
        "Total number of transactions processed",
        []string{"network", "type", "pipeline"})
    
    // Contract event metrics
    c.RegisterCounter("cdp_contract_events_total",
        "Total contract events processed",
        []string{"network", "contract", "event_type"})
    
    // Data volume metrics
    c.RegisterCounter("cdp_bytes_processed_total",
        "Total bytes of Stellar data processed",
        []string{"network", "source"})
    
    // Cost tracking
    c.RegisterGauge("cdp_estimated_cost_dollars",
        "Estimated cost in dollars",
        []string{"pipeline", "provider"})
}

// CDP-specific health checks
func (c *CDPMetricsCollector) RegisterHealthChecks() {
    c.RegisterHealthCheck("stellar_network_sync", func() error {
        // Check if pipeline is keeping up with network
        latestLedger := c.getLatestNetworkLedger()
        processedLedger := c.getLatestProcessedLedger()
        
        lag := latestLedger - processedLedger
        if lag > 1000 {
            return fmt.Errorf("pipeline lagging by %d ledgers", lag)
        }
        
        return nil
    })
}
```

### Phase 5: AI-Powered CDP Operations (Week 5-6)

Implement CDP-specific AI operations in flowctl:

```go
// flowctl/internal/cdp/ai/analyzer.go
type CDPAnalyzer struct {
    llm LLMClient
}

func (a *CDPAnalyzer) AnalyzePipelinePerformance(pipelineID string) (*Analysis, error) {
    metrics := a.collectPipelineMetrics(pipelineID)
    
    prompt := fmt.Sprintf(`
        Analyze this Stellar pipeline performance:
        
        Pipeline: %s
        Metrics:
        - Ledgers processed: %d
        - Processing rate: %.2f ledgers/sec
        - Error rate: %.2f%%
        - Memory usage: %d MB
        - Cost/ledger: $%.4f
        
        Recent errors:
        %s
        
        Provide:
        1. Performance assessment
        2. Bottleneck identification
        3. Cost optimization suggestions
        4. Scaling recommendations
    `, pipelineID, metrics.LedgersProcessed, metrics.ProcessingRate,
       metrics.ErrorRate, metrics.MemoryMB, metrics.CostPerLedger,
       metrics.RecentErrors)
    
    return a.llm.Analyze(prompt)
}

// Natural language to CDP pipeline
func (a *CDPAnalyzer) GeneratePipelineFromDescription(description string) (*PipelineConfig, error) {
    prompt := fmt.Sprintf(`
        Generate a Stellar CDP pipeline configuration from this description:
        "%s"
        
        Available components:
        Sources: BufferedStorageSource (S3/GCS), CaptiveCore, RPC
        Processors: ContractEvent, FilterPayments, AccountData, TransformToAppPayment
        Consumers: PostgreSQL, MongoDB, Redis, Webhook
        
        Return a valid pipeline configuration.
    `, description)
    
    response, err := a.llm.Generate(prompt)
    if err != nil {
        return nil, err
    }
    
    return a.parsePipelineConfig(response)
}
```

### Phase 6: CDP-Specific Safety Policies (Week 7)

Add CDP-specific safety policies to flowctl:

```go
// flowctl/internal/cdp/policies.go
type CDPPolicyEngine struct {
    base PolicyEngine
}

func (e *CDPPolicyEngine) RegisterDefaultPolicies() {
    // Ledger range limits
    e.RegisterPolicy(Policy{
        Name: "max_ledger_range",
        Description: "Limit maximum ledger range per pipeline",
        Rule: func(config PipelineConfig) error {
            source := config.Source
            if source.Type == "BufferedStorageSource" {
                start := source.Config["start_ledger"].(int)
                end := source.Config["end_ledger"].(int)
                
                if end - start > 1_000_000 {
                    return fmt.Errorf("ledger range too large: %d ledgers (max 1M)", end-start)
                }
            }
            return nil
        },
    })
    
    // Resource limits based on network
    e.RegisterPolicy(Policy{
        Name: "mainnet_resource_limits",
        Description: "Enforce stricter limits for mainnet",
        Rule: func(config PipelineConfig) error {
            if config.Source.Config["network"] == "mainnet" {
                // Check worker count
                if workers := config.Source.Config["num_workers"].(int); workers > 10 {
                    return fmt.Errorf("mainnet limited to 10 workers")
                }
                
                // Check buffer size
                if buffer := config.Source.Config["buffer_size"].(int); buffer > 1000 {
                    return fmt.Errorf("mainnet buffer size limited to 1000")
                }
            }
            return nil
        },
    })
    
    // Cost controls
    e.RegisterPolicy(Policy{
        Name: "cost_limit",
        Description: "Prevent excessive costs",
        Rule: func(config PipelineConfig) error {
            estimatedCost := e.estimatePipelineCost(config)
            if estimatedCost > 1000 { // $1000 limit
                return fmt.Errorf("estimated cost $%.2f exceeds limit", estimatedCost)
            }
            return nil
        },
    })
}
```

### Phase 7: CDP Pipeline Templates (Week 8)

Create reusable templates for common CDP use cases:

```yaml
# flowctl/templates/cdp/contract-monitoring.yaml
apiVersion: v1
kind: PipelineTemplate
metadata:
  name: stellar-contract-monitoring
  labels:
    category: cdp
    use-case: monitoring
spec:
  parameters:
    - name: contract_id
      description: "Stellar contract ID to monitor"
      type: string
      required: true
    
    - name: event_types
      description: "Event types to filter"
      type: array
      default: ["*"]
    
    - name: network
      description: "Stellar network"
      type: string
      default: "mainnet"
      enum: ["mainnet", "testnet"]
    
    - name: alert_webhook
      description: "Webhook URL for alerts"
      type: string
      required: true
  
  template: |
    apiVersion: v1
    kind: Pipeline
    metadata:
      name: contract-monitor-{{ .Values.contract_id | trunc 8 }}
    spec:
      source:
        type: BufferedStorageSourceAdapter
        config:
          bucket_name: stellar-{{ .Values.network }}-ledgers
          network: {{ .Values.network }}
          num_workers: 4
          buffer_size: 100
      
      processors:
        - type: ContractEvent
          config:
            network_passphrase: {{ .Values.network | networkPassphrase }}
            contract_id: {{ .Values.contract_id }}
            event_types: {{ .Values.event_types | toJson }}
        
        - type: AlertFilter
          config:
            conditions:
              - field: "value"
                operator: ">"
                threshold: 1000000
      
      consumers:
        - type: WebhookNotifier
          config:
            url: {{ .Values.alert_webhook }}
            format: "json"
```

## Integration with CDP Pipeline

### 1. Minimal Changes to CDP Pipeline

```go
// cdp-pipeline-workflow/pkg/control/client.go
type EnhancedControlClient struct {
    *control.Client
    
    // Additional CDP-specific fields
    pipelineType string
    cdpMetadata  map[string]string
}

func (c *EnhancedControlClient) RegisterWithCDP(ctx context.Context) error {
    // Register as CDP pipeline with additional metadata
    metadata := map[string]string{
        "pipeline_type":    "cdp",
        "stellar_network":  c.cdpMetadata["network"],
        "processing_mode":  c.cdpMetadata["mode"],
        "component_version": Version,
    }
    
    return c.Register(ctx, metadata)
}

// Report CDP-specific metrics
func (c *EnhancedControlClient) ReportCDPMetrics(metrics CDPMetrics) error {
    return c.ReportMetrics(map[string]float64{
        "cdp.ledgers_processed":    float64(metrics.LedgersProcessed),
        "cdp.transactions_total":   float64(metrics.TransactionsTotal),
        "cdp.events_processed":     float64(metrics.EventsProcessed),
        "cdp.processing_rate_tps":  metrics.ProcessingRateTPS,
        "cdp.error_rate":          metrics.ErrorRate,
    })
}
```

### 2. CDP Component Wrapper

```go
// cdp-pipeline-workflow/pkg/wrapper/component.go
type FlowctlComponentWrapper struct {
    component interface{}
    client    *control.Client
    stats     *StatsCollector
}

func (w *FlowctlComponentWrapper) WrapSource(source SourceAdapter) SourceAdapter {
    return &wrappedSource{
        base:   source,
        client: w.client,
        stats:  w.stats,
    }
}

type wrappedSource struct {
    base   SourceAdapter
    client *control.Client
    stats  *StatsCollector
}

func (s *wrappedSource) Run(ctx context.Context) error {
    // Report start
    s.client.UpdateStatus("running")
    
    // Run with metrics collection
    err := s.base.Run(ctx)
    
    // Report completion
    status := "completed"
    if err != nil {
        status = "failed"
    }
    s.client.UpdateStatus(status)
    
    return err
}
```

## Benefits of This Approach

1. **No Duplication**: AI-native features live in flowctl, not CDP pipeline
2. **Clean Separation**: CDP focuses on Stellar processing, flowctl on orchestration
3. **Reusability**: Other pipelines can use the same control plane
4. **Unified Management**: Single control plane for all pipeline types
5. **Better Scaling**: Control plane can manage thousands of pipelines
6. **Cost Efficiency**: Shared infrastructure reduces overhead

## Migration Path

### Step 1: Update CDP Pipeline Dependencies
```bash
# Add flowctl as a dependency
go get github.com/withobsrvr/flowctl
```

### Step 2: Configure Flowctl Endpoint
```yaml
# pipeline_config.yaml
control_plane:
  endpoint: "localhost:8080"  # or remote flowctl server
  registration:
    type: "cdp_pipeline"
    metadata:
      network: "mainnet"
      version: "1.0.0"
```

### Step 3: Launch with Flowctl
```bash
# Start flowctl server with CDP support
flowctl server --enable-cdp

# Run CDP pipeline connected to flowctl
./cdp-pipeline-workflow -config pipeline_config.yaml
```

### Step 4: Use AI Features
```bash
# Generate pipeline from natural language
flowctl ai generate "Monitor Soroswap contract events and alert on large swaps"

# Analyze performance
flowctl ai analyze --pipeline cdp-soroswap-monitor

# Optimize costs
flowctl ai optimize --pipeline cdp-mainnet-processor --target-cost 500
```

## Conclusion

By leveraging flowctl as the AI-native control plane for CDP pipelines, we achieve:

1. **Better Architecture**: Clear separation between control and data planes
2. **Faster Development**: Reuse existing flowctl capabilities
3. **Enhanced Features**: Get AI, monitoring, and orchestration for free
4. **Future Proof**: Easy to add new AI capabilities to flowctl
5. **Community Benefits**: Other Stellar projects can use the same infrastructure

The CDP pipeline remains focused on its core competency - processing Stellar blockchain data - while flowctl handles all the AI-native control plane features.