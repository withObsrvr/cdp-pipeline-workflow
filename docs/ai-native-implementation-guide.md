# CDP Pipeline AI-Native Implementation Guide

## Executive Summary

This document outlines the transformation of the CDP (Composable Data Platform) Pipeline Workflow into an AI-native system that can be operated end-to-end by AI agents while maintaining human oversight and control. The implementation follows Obsrvr's AI-Native Design Guide principles to achieve Level 2 (Closed-loop) maturity with some Level 3 (Self-healing) capabilities.

## Table of Contents

1. [Current State Assessment](#current-state-assessment)
2. [AI-Native Architecture](#ai-native-architecture)
3. [Implementation Phases](#implementation-phases)
4. [Tool Card System](#tool-card-system)
5. [Plan/Apply Pattern](#planapply-pattern)
6. [API Layer](#api-layer)
7. [Enhanced Observability](#enhanced-observability)
8. [Safety & Governance](#safety--governance)
9. [Streaming Capabilities](#streaming-capabilities)
10. [Evaluators & Success Criteria](#evaluators--success-criteria)
11. [Cost Awareness](#cost-awareness)
12. [Migration Guide](#migration-guide)
13. [Testing Strategy](#testing-strategy)
14. [Success Metrics](#success-metrics)

## Current State Assessment

### Existing Capabilities
- ✅ Modular pipeline architecture with sources, processors, and consumers
- ✅ YAML-based configuration
- ✅ Basic statistics tracking
- ✅ Multiple data source support (GCS, S3, filesystem)
- ✅ Extensible processor/consumer system

### AI-Native Gaps
- ❌ No JSON Schema definitions for configurations
- ❌ No programmatic API for control
- ❌ Limited observability (no distributed tracing)
- ❌ No plan/apply pattern for changes
- ❌ No cost hints or resource tracking
- ❌ No safety policies or approval mechanisms
- ❌ No streaming progress updates
- ❌ No machine-readable documentation

## AI-Native Architecture

### Design Principles

1. **Every feature is a tool** - All functionality exposed via JSON Schema APIs
2. **Deterministic responses** - Stable, machine-parseable outputs
3. **Observable by default** - OpenTelemetry traces on every operation
4. **Safe operations** - Policy controls, dry-run, and approvals
5. **Cost aware** - Resource usage hints in every response
6. **Streaming first** - Real-time progress and partial results

### High-Level Architecture

```
┌─────────────────────────────────────────────────────────────┐
│                        AI Agent                              │
│                 (Claude, GPT, Custom)                        │
└────────────────────────┬────────────────────────────────────┘
                         │
                    JSON/REST API
                         │
┌─────────────────────────┴────────────────────────────────────┐
│                   CDP Pipeline API Layer                      │
│  ┌─────────────┐  ┌──────────────┐  ┌─────────────────┐    │
│  │ Tool Cards  │  │ Plan/Apply   │  │ Policy Engine   │    │
│  └─────────────┘  └──────────────┘  └─────────────────┘    │
│  ┌─────────────┐  ┌──────────────┐  ┌─────────────────┐    │
│  │ Observability│ │ Evaluators   │  │ Cost Calculator │    │
│  └─────────────┘  └──────────────┘  └─────────────────┘    │
└──────────────────────────────────────────────────────────────┘
                         │
┌─────────────────────────┴────────────────────────────────────┐
│              Existing CDP Pipeline Core                       │
│  ┌─────────────┐  ┌──────────────┐  ┌─────────────────┐    │
│  │  Sources    │  │  Processors  │  │   Consumers     │    │
│  └─────────────┘  └──────────────┘  └─────────────────┘    │
└──────────────────────────────────────────────────────────────┘
```

## Implementation Phases

### Phase 1: Tool Cards & API Foundation (Weeks 1-2)

#### 1.1 Tool Card Schema Definition

Create `pkg/toolcards/schema.go`:

```go
package toolcards

import (
    "encoding/json"
    "time"
)

// ToolCard represents a callable tool with full metadata
type ToolCard struct {
    // Identification
    ID          string `json:"id"`
    Name        string `json:"name"`
    Type        string `json:"type"` // source, processor, consumer
    Version     string `json:"version"`
    Description string `json:"description"`
    
    // Schemas
    InputSchema  json.RawMessage `json:"input_schema"`  // JSON Schema
    OutputSchema json.RawMessage `json:"output_schema"` // JSON Schema
    ConfigSchema json.RawMessage `json:"config_schema"` // JSON Schema
    
    // Behavior
    Idempotent      bool              `json:"idempotent"`
    StreamingOutput bool              `json:"streaming_output"`
    RetryPolicy     RetryPolicy       `json:"retry_policy"`
    
    // Cost & Performance
    CostHints       CostHints         `json:"cost_hints"`
    PerformanceHints PerformanceHints `json:"performance_hints"`
    
    // Safety
    RequiredScopes  []string          `json:"required_scopes"`
    DangerLevel     string            `json:"danger_level"` // safe, warning, dangerous
    
    // Documentation
    Examples        []Example         `json:"examples"`
    ErrorTaxonomy   []ErrorType       `json:"error_taxonomy"`
}

type RetryPolicy struct {
    MaxRetries      int           `json:"max_retries"`
    RetryableErrors []string      `json:"retryable_errors"`
    BackoffStrategy string        `json:"backoff_strategy"`
    InitialDelay    time.Duration `json:"initial_delay"`
}

type CostHints struct {
    CPUMillicores    int     `json:"cpu_millicores"`
    MemoryMB         int     `json:"memory_mb"`
    NetworkBytesPerOp int64  `json:"network_bytes_per_op"`
    StorageBytesPerOp int64  `json:"storage_bytes_per_op"`
    CostPerThousandOps float64 `json:"cost_per_thousand_ops"`
}

type PerformanceHints struct {
    AvgLatencyMs     int `json:"avg_latency_ms"`
    P99LatencyMs     int `json:"p99_latency_ms"`
    ThroughputOpsPerSec int `json:"throughput_ops_per_sec"`
}

type Example struct {
    Name        string          `json:"name"`
    Description string          `json:"description"`
    Input       json.RawMessage `json:"input"`
    Output      json.RawMessage `json:"output"`
    Config      json.RawMessage `json:"config,omitempty"`
}

type ErrorType struct {
    Code        string `json:"code"`
    Description string `json:"description"`
    Retryable   bool   `json:"retryable"`
    UserAction  string `json:"user_action"`
}
```

#### 1.2 Tool Card Registry

Create `pkg/toolcards/registry.go`:

```go
package toolcards

import (
    "encoding/json"
    "fmt"
    "sync"
)

// Registry manages all available tool cards
type Registry struct {
    mu    sync.RWMutex
    cards map[string]*ToolCard
}

var defaultRegistry = &Registry{
    cards: make(map[string]*ToolCard),
}

// Register adds a tool card to the registry
func Register(card *ToolCard) error {
    defaultRegistry.mu.Lock()
    defer defaultRegistry.mu.Unlock()
    
    if _, exists := defaultRegistry.cards[card.ID]; exists {
        return fmt.Errorf("tool card %s already registered", card.ID)
    }
    
    defaultRegistry.cards[card.ID] = card
    return nil
}

// Get retrieves a tool card by ID
func Get(id string) (*ToolCard, error) {
    defaultRegistry.mu.RLock()
    defer defaultRegistry.mu.RUnlock()
    
    card, exists := defaultRegistry.cards[id]
    if !exists {
        return nil, fmt.Errorf("tool card %s not found", id)
    }
    
    return card, nil
}

// List returns all registered tool cards
func List() []*ToolCard {
    defaultRegistry.mu.RLock()
    defer defaultRegistry.mu.RUnlock()
    
    cards := make([]*ToolCard, 0, len(defaultRegistry.cards))
    for _, card := range defaultRegistry.cards {
        cards = append(cards, card)
    }
    
    return cards
}

// ExportOpenAPI generates OpenAPI spec from tool cards
func ExportOpenAPI() ([]byte, error) {
    // Implementation for OpenAPI generation
    return nil, nil
}
```

#### 1.3 Example Tool Card

Create `toolcards/processor_contract_events.json`:

```json
{
  "id": "processor.contract_events",
  "name": "Contract Events Processor",
  "type": "processor",
  "version": "1.0.0",
  "description": "Processes Stellar smart contract events from ledger data",
  
  "config_schema": {
    "$schema": "http://json-schema.org/draft-07/schema#",
    "type": "object",
    "required": ["network_passphrase"],
    "properties": {
      "network_passphrase": {
        "type": "string",
        "description": "Stellar network passphrase",
        "enum": [
          "Public Global Stellar Network ; September 2015",
          "Test SDF Network ; September 2015"
        ]
      }
    }
  },
  
  "input_schema": {
    "$schema": "http://json-schema.org/draft-07/schema#",
    "type": "object",
    "description": "XDR LedgerCloseMeta",
    "properties": {
      "type": {
        "const": "xdr.LedgerCloseMeta"
      }
    }
  },
  
  "output_schema": {
    "$schema": "http://json-schema.org/draft-07/schema#",
    "type": "object",
    "properties": {
      "timestamp": {
        "type": "string",
        "format": "date-time"
      },
      "ledger_sequence": {
        "type": "integer"
      },
      "contract_id": {
        "type": "string",
        "pattern": "^C[A-Z2-7]{55}$"
      },
      "event_type": {
        "type": "string"
      },
      "topics": {
        "type": "array"
      },
      "data": {
        "type": "object"
      }
    }
  },
  
  "idempotent": true,
  "streaming_output": true,
  
  "retry_policy": {
    "max_retries": 3,
    "retryable_errors": ["TIMEOUT", "NETWORK_ERROR"],
    "backoff_strategy": "exponential",
    "initial_delay": "1s"
  },
  
  "cost_hints": {
    "cpu_millicores": 100,
    "memory_mb": 256,
    "network_bytes_per_op": 1024,
    "storage_bytes_per_op": 0,
    "cost_per_thousand_ops": 0.001
  },
  
  "performance_hints": {
    "avg_latency_ms": 50,
    "p99_latency_ms": 200,
    "throughput_ops_per_sec": 1000
  },
  
  "required_scopes": ["read:ledgers"],
  "danger_level": "safe",
  
  "examples": [{
    "name": "Process DEX swap event",
    "description": "Extract a swap event from a Soroban DEX",
    "config": {
      "network_passphrase": "Public Global Stellar Network ; September 2015"
    },
    "output": {
      "timestamp": "2025-01-15T10:30:45Z",
      "ledger_sequence": 58466921,
      "contract_id": "CAS3J7GYLGXMF6TDJBBYYSE3HQ6BBSMLNUQ34T6TZMYMW2EVH34XOWMA",
      "event_type": "swap",
      "topics": ["swap", "GBGWQFSJSOMJ2BTOH5RLZUTZPV544YR2DF5CGYL7WDZ2Y6OSRHR6TUBE"],
      "data": {
        "amount_in": "1000000",
        "amount_out": "950000"
      }
    }
  }],
  
  "error_taxonomy": [
    {
      "code": "INVALID_XDR",
      "description": "Input is not valid XDR LedgerCloseMeta",
      "retryable": false,
      "user_action": "Ensure input is properly formatted XDR data"
    },
    {
      "code": "NETWORK_MISMATCH",
      "description": "Network passphrase doesn't match ledger data",
      "retryable": false,
      "user_action": "Check network_passphrase configuration"
    }
  ]
}
```

### Phase 2: Plan/Apply Pattern (Weeks 3-4)

#### 2.1 Configuration Planner

Create `pkg/planner/planner.go`:

```go
package planner

import (
    "context"
    "fmt"
    "time"
)

// Plan represents a planned configuration change
type Plan struct {
    ID          string           `json:"id"`
    Created     time.Time        `json:"created"`
    Description string           `json:"description"`
    CurrentState *PipelineConfig `json:"current_state"`
    DesiredState *PipelineConfig `json:"desired_state"`
    Changes     []Change         `json:"changes"`
    CostEstimate *CostEstimate   `json:"cost_estimate"`
    SafetyChecks []SafetyCheck   `json:"safety_checks"`
    Approved    bool             `json:"approved"`
    ApprovedBy  string           `json:"approved_by,omitempty"`
    ApprovedAt  *time.Time       `json:"approved_at,omitempty"`
}

// Change represents a single configuration change
type Change struct {
    Type        string      `json:"type"` // add, remove, modify
    Path        string      `json:"path"` // JSON path
    OldValue    interface{} `json:"old_value,omitempty"`
    NewValue    interface{} `json:"new_value,omitempty"`
    Impact      string      `json:"impact"` // high, medium, low
    Description string      `json:"description"`
}

// CostEstimate provides resource usage estimates
type CostEstimate struct {
    CPUCores        float64 `json:"cpu_cores"`
    MemoryGB        float64 `json:"memory_gb"`
    NetworkGBPerDay float64 `json:"network_gb_per_day"`
    StorageGB       float64 `json:"storage_gb"`
    EstimatedCostPerDay float64 `json:"estimated_cost_per_day"`
}

// SafetyCheck represents a validation check
type SafetyCheck struct {
    Name        string `json:"name"`
    Passed      bool   `json:"passed"`
    Message     string `json:"message"`
    Severity    string `json:"severity"` // error, warning, info
}

// Planner creates and manages configuration plans
type Planner struct {
    validator    *Validator
    differ       *Differ
    costEstimator *CostEstimator
}

// CreatePlan generates a plan for configuration changes
func (p *Planner) CreatePlan(ctx context.Context, current, desired *PipelineConfig) (*Plan, error) {
    // Generate diff
    changes, err := p.differ.Diff(current, desired)
    if err != nil {
        return nil, fmt.Errorf("failed to generate diff: %w", err)
    }
    
    // Run safety checks
    safetyChecks := p.validator.Validate(desired, changes)
    
    // Estimate costs
    costEstimate, err := p.costEstimator.Estimate(desired)
    if err != nil {
        return nil, fmt.Errorf("failed to estimate costs: %w", err)
    }
    
    plan := &Plan{
        ID:           generatePlanID(),
        Created:      time.Now(),
        Description:  generateDescription(changes),
        CurrentState: current,
        DesiredState: desired,
        Changes:      changes,
        CostEstimate: costEstimate,
        SafetyChecks: safetyChecks,
        Approved:     false,
    }
    
    return plan, nil
}

// ApplyPlan executes an approved plan
func (p *Planner) ApplyPlan(ctx context.Context, plan *Plan) (*ApplyResult, error) {
    if !plan.Approved {
        return nil, fmt.Errorf("plan %s is not approved", plan.ID)
    }
    
    result := &ApplyResult{
        PlanID:    plan.ID,
        StartTime: time.Now(),
        Steps:     make([]ApplyStep, 0),
    }
    
    // Apply changes with rollback capability
    for _, change := range plan.Changes {
        step := p.applyChange(ctx, change)
        result.Steps = append(result.Steps, step)
        
        if !step.Success {
            // Trigger rollback
            p.rollback(ctx, result)
            return result, fmt.Errorf("apply failed at step %d: %s", len(result.Steps), step.Error)
        }
    }
    
    result.EndTime = time.Now()
    result.Success = true
    
    return result, nil
}
```

#### 2.2 Configuration Differ

Create `pkg/planner/diff.go`:

```go
package planner

import (
    "encoding/json"
    "reflect"
    "strings"
)

// Differ generates configuration diffs
type Differ struct{}

// Diff compares two configurations and returns changes
func (d *Differ) Diff(current, desired *PipelineConfig) ([]Change, error) {
    changes := []Change{}
    
    // Convert to JSON for easier comparison
    currentJSON, _ := json.Marshal(current)
    desiredJSON, _ := json.Marshal(desired)
    
    var currentMap, desiredMap map[string]interface{}
    json.Unmarshal(currentJSON, &currentMap)
    json.Unmarshal(desiredJSON, &desiredMap)
    
    // Recursive diff
    d.diffMaps("", currentMap, desiredMap, &changes)
    
    return changes, nil
}

func (d *Differ) diffMaps(path string, current, desired map[string]interface{}, changes *[]Change) {
    // Check for removed keys
    for key, currentValue := range current {
        fullPath := d.joinPath(path, key)
        if _, exists := desired[key]; !exists {
            *changes = append(*changes, Change{
                Type:        "remove",
                Path:        fullPath,
                OldValue:    currentValue,
                Impact:      d.assessImpact(fullPath),
                Description: fmt.Sprintf("Remove %s", fullPath),
            })
        }
    }
    
    // Check for added/modified keys
    for key, desiredValue := range desired {
        fullPath := d.joinPath(path, key)
        currentValue, exists := current[key]
        
        if !exists {
            *changes = append(*changes, Change{
                Type:        "add",
                Path:        fullPath,
                NewValue:    desiredValue,
                Impact:      d.assessImpact(fullPath),
                Description: fmt.Sprintf("Add %s", fullPath),
            })
        } else if !reflect.DeepEqual(currentValue, desiredValue) {
            *changes = append(*changes, Change{
                Type:        "modify",
                Path:        fullPath,
                OldValue:    currentValue,
                NewValue:    desiredValue,
                Impact:      d.assessImpact(fullPath),
                Description: fmt.Sprintf("Modify %s", fullPath),
            })
        }
    }
}

func (d *Differ) assessImpact(path string) string {
    // Assess impact based on path
    if strings.Contains(path, "source") {
        return "high"
    } else if strings.Contains(path, "processor") {
        return "medium"
    }
    return "low"
}
```

### Phase 3: API Layer (Weeks 3-4)

#### 3.1 API Server

Create `pkg/api/server.go`:

```go
package api

import (
    "context"
    "encoding/json"
    "net/http"
    "time"
    
    "github.com/gorilla/mux"
    "github.com/rs/cors"
    "go.opentelemetry.io/otel"
    "go.opentelemetry.io/otel/trace"
)

// Server provides the HTTP API for the CDP Pipeline
type Server struct {
    router       *mux.Router
    pipelineManager *PipelineManager
    planner      *planner.Planner
    tracer       trace.Tracer
}

// NewServer creates a new API server
func NewServer(pm *PipelineManager, p *planner.Planner) *Server {
    s := &Server{
        router:          mux.NewRouter(),
        pipelineManager: pm,
        planner:         p,
        tracer:          otel.Tracer("cdp-api"),
    }
    
    s.setupRoutes()
    return s
}

func (s *Server) setupRoutes() {
    // Tool discovery
    s.router.HandleFunc("/tools", s.handleListTools).Methods("GET")
    s.router.HandleFunc("/tools/{id}", s.handleGetTool).Methods("GET")
    
    // Pipeline management
    s.router.HandleFunc("/pipelines", s.handleListPipelines).Methods("GET")
    s.router.HandleFunc("/pipelines/{id}", s.handleGetPipeline).Methods("GET")
    s.router.HandleFunc("/pipelines/{id}/status", s.handleGetPipelineStatus).Methods("GET")
    s.router.HandleFunc("/pipelines/{id}/metrics", s.handleGetPipelineMetrics).Methods("GET")
    
    // Plan/Apply pattern
    s.router.HandleFunc("/pipelines/plan", s.handlePlanPipeline).Methods("POST")
    s.router.HandleFunc("/pipelines/apply", s.handleApplyPlan).Methods("POST")
    s.router.HandleFunc("/plans/{id}", s.handleGetPlan).Methods("GET")
    s.router.HandleFunc("/plans/{id}/approve", s.handleApprovePlan).Methods("POST")
    
    // Control operations
    s.router.HandleFunc("/pipelines/{id}/start", s.handleStartPipeline).Methods("POST")
    s.router.HandleFunc("/pipelines/{id}/stop", s.handleStopPipeline).Methods("POST")
    s.router.HandleFunc("/pipelines/{id}/pause", s.handlePausePipeline).Methods("POST")
    
    // Streaming endpoints
    s.router.HandleFunc("/pipelines/{id}/stream/metrics", s.handleStreamMetrics).Methods("GET")
    s.router.HandleFunc("/pipelines/{id}/stream/logs", s.handleStreamLogs).Methods("GET")
    
    // Health and metrics
    s.router.HandleFunc("/health", s.handleHealth).Methods("GET")
    s.router.HandleFunc("/metrics", s.handlePrometheusMetrics).Methods("GET")
}

// Start runs the API server
func (s *Server) Start(addr string) error {
    handler := cors.Default().Handler(s.router)
    
    srv := &http.Server{
        Addr:         addr,
        Handler:      handler,
        ReadTimeout:  15 * time.Second,
        WriteTimeout: 15 * time.Second,
        IdleTimeout:  60 * time.Second,
    }
    
    return srv.ListenAndServe()
}

// API Response Types

type ToolsResponse struct {
    Tools []ToolCard `json:"tools"`
    Count int        `json:"count"`
}

type PlanRequest struct {
    Pipeline     PipelineConfig `json:"pipeline"`
    DryRun       bool          `json:"dry_run"`
    AutoApprove  bool          `json:"auto_approve"`
}

type PlanResponse struct {
    Plan      *Plan         `json:"plan"`
    Warnings  []string      `json:"warnings"`
    RequiresApproval bool   `json:"requires_approval"`
}

type ApplyRequest struct {
    PlanID       string        `json:"plan_id"`
    RunID        string        `json:"run_id,omitempty"`
    StreamEvents bool          `json:"stream_events"`
}

type ApplyResponse struct {
    RunID        string        `json:"run_id"`
    Status       string        `json:"status"`
    StreamURL    string        `json:"stream_url,omitempty"`
    Result       *ApplyResult  `json:"result,omitempty"`
}
```

#### 3.2 Pipeline Status Handler

Create `pkg/api/handlers/pipeline_status.go`:

```go
package handlers

import (
    "encoding/json"
    "net/http"
    "time"
    
    "github.com/gorilla/mux"
    "go.opentelemetry.io/otel/attribute"
)

// PipelineStatus represents the current state of a pipeline
type PipelineStatus struct {
    ID          string                 `json:"id"`
    State       string                 `json:"state"` // running, paused, stopped, error
    StartTime   *time.Time             `json:"start_time,omitempty"`
    LastActive  time.Time              `json:"last_active"`
    Metrics     map[string]interface{} `json:"metrics"`
    Components  []ComponentStatus      `json:"components"`
    Health      HealthStatus           `json:"health"`
    TraceID     string                 `json:"trace_id"`
}

type ComponentStatus struct {
    Type        string                 `json:"type"`
    Name        string                 `json:"name"`
    State       string                 `json:"state"`
    LastUpdate  time.Time              `json:"last_update"`
    Metrics     map[string]interface{} `json:"metrics"`
    Errors      []string               `json:"errors,omitempty"`
}

type HealthStatus struct {
    Healthy     bool              `json:"healthy"`
    Checks      []HealthCheck     `json:"checks"`
    LastCheck   time.Time         `json:"last_check"`
}

func (s *Server) handleGetPipelineStatus(w http.ResponseWriter, r *http.Request) {
    ctx, span := s.tracer.Start(r.Context(), "getPipelineStatus")
    defer span.End()
    
    vars := mux.Vars(r)
    pipelineID := vars["id"]
    
    span.SetAttributes(attribute.String("pipeline.id", pipelineID))
    
    // Get pipeline from manager
    pipeline, err := s.pipelineManager.GetPipeline(ctx, pipelineID)
    if err != nil {
        span.RecordError(err)
        http.Error(w, err.Error(), http.StatusNotFound)
        return
    }
    
    // Build status response
    status := &PipelineStatus{
        ID:         pipelineID,
        State:      pipeline.GetState(),
        StartTime:  pipeline.GetStartTime(),
        LastActive: pipeline.GetLastActiveTime(),
        Metrics:    pipeline.GetMetrics(),
        Components: s.getComponentStatuses(ctx, pipeline),
        Health:     s.getHealthStatus(ctx, pipeline),
        TraceID:    span.SpanContext().TraceID().String(),
    }
    
    // Add cost hints to response
    w.Header().Set("X-Cost-CPU-Millicores", fmt.Sprintf("%d", status.Metrics["cpu_millicores"]))
    w.Header().Set("X-Cost-Memory-MB", fmt.Sprintf("%d", status.Metrics["memory_mb"]))
    
    w.Header().Set("Content-Type", "application/json")
    json.NewEncoder(w).Encode(status)
}
```

### Phase 4: Enhanced Observability (Weeks 5-6)

#### 4.1 OpenTelemetry Integration

Create `pkg/telemetry/tracer.go`:

```go
package telemetry

import (
    "context"
    "fmt"
    
    "go.opentelemetry.io/otel"
    "go.opentelemetry.io/otel/attribute"
    "go.opentelemetry.io/otel/exporters/otlp/otlptrace"
    "go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracegrpc"
    "go.opentelemetry.io/otel/propagation"
    "go.opentelemetry.io/otel/sdk/resource"
    sdktrace "go.opentelemetry.io/otel/sdk/trace"
    semconv "go.opentelemetry.io/otel/semconv/v1.4.0"
    "go.opentelemetry.io/otel/trace"
)

// InitTracing initializes OpenTelemetry tracing
func InitTracing(serviceName string, endpoint string) (func(), error) {
    ctx := context.Background()
    
    // Create OTLP exporter
    exporter, err := otlptrace.New(
        ctx,
        otlptracegrpc.NewClient(
            otlptracegrpc.WithEndpoint(endpoint),
            otlptracegrpc.WithInsecure(),
        ),
    )
    if err != nil {
        return nil, fmt.Errorf("failed to create exporter: %w", err)
    }
    
    // Create resource
    res, err := resource.New(ctx,
        resource.WithAttributes(
            semconv.ServiceNameKey.String(serviceName),
            semconv.ServiceVersionKey.String("1.0.0"),
        ),
    )
    if err != nil {
        return nil, fmt.Errorf("failed to create resource: %w", err)
    }
    
    // Create tracer provider
    tp := sdktrace.NewTracerProvider(
        sdktrace.WithBatcher(exporter),
        sdktrace.WithResource(res),
        sdktrace.WithSampler(sdktrace.AlwaysSample()),
    )
    
    otel.SetTracerProvider(tp)
    otel.SetTextMapPropagator(propagation.TraceContext{})
    
    return func() {
        if err := tp.Shutdown(ctx); err != nil {
            fmt.Printf("Error shutting down tracer provider: %v\n", err)
        }
    }, nil
}

// TracedProcessor wraps a processor with tracing
type TracedProcessor struct {
    processor Processor
    tracer    trace.Tracer
}

func NewTracedProcessor(p Processor) *TracedProcessor {
    return &TracedProcessor{
        processor: p,
        tracer:    otel.Tracer("cdp-processor"),
    }
}

func (tp *TracedProcessor) Process(ctx context.Context, msg Message) error {
    ctx, span := tp.tracer.Start(ctx, tp.processor.Name(),
        trace.WithAttributes(
            attribute.String("processor.type", tp.processor.Type()),
            attribute.String("message.type", fmt.Sprintf("%T", msg.Payload)),
        ),
    )
    defer span.End()
    
    // Add cost attributes
    span.SetAttributes(
        attribute.Int("cost.cpu_millicores", tp.processor.GetCostHints().CPUMillicores),
        attribute.Int("cost.memory_mb", tp.processor.GetCostHints().MemoryMB),
    )
    
    // Process with timing
    start := time.Now()
    err := tp.processor.Process(ctx, msg)
    duration := time.Since(start)
    
    span.SetAttributes(
        attribute.Int64("duration_ms", duration.Milliseconds()),
    )
    
    if err != nil {
        span.RecordError(err)
        span.SetStatus(codes.Error, err.Error())
    } else {
        span.SetStatus(codes.Ok, "")
    }
    
    return err
}
```

#### 4.2 Metrics Collection

Create `pkg/telemetry/metrics.go`:

```go
package telemetry

import (
    "context"
    
    "github.com/prometheus/client_golang/prometheus"
    "github.com/prometheus/client_golang/prometheus/promauto"
)

var (
    // Pipeline metrics
    pipelinesRunning = promauto.NewGauge(prometheus.GaugeOpts{
        Name: "cdp_pipelines_running_total",
        Help: "Total number of running pipelines",
    })
    
    pipelinesProcessed = promauto.NewCounterVec(prometheus.CounterOpts{
        Name: "cdp_pipelines_processed_total",
        Help: "Total number of messages processed by pipeline",
    }, []string{"pipeline", "processor"})
    
    pipelineErrors = promauto.NewCounterVec(prometheus.CounterOpts{
        Name: "cdp_pipeline_errors_total",
        Help: "Total number of errors by pipeline and type",
    }, []string{"pipeline", "processor", "error_type"})
    
    // Performance metrics
    processingDuration = promauto.NewHistogramVec(prometheus.HistogramOpts{
        Name: "cdp_processing_duration_seconds",
        Help: "Processing duration in seconds",
        Buckets: prometheus.DefBuckets,
    }, []string{"pipeline", "processor"})
    
    // Resource metrics
    resourceUsage = promauto.NewGaugeVec(prometheus.GaugeOpts{
        Name: "cdp_resource_usage",
        Help: "Resource usage by type",
    }, []string{"pipeline", "resource_type"})
    
    // Cost metrics
    estimatedCost = promauto.NewGaugeVec(prometheus.GaugeOpts{
        Name: "cdp_estimated_cost_dollars_per_hour",
        Help: "Estimated cost in dollars per hour",
    }, []string{"pipeline", "component"})
)

// MetricsCollector collects and exposes metrics
type MetricsCollector struct {
    pipelines map[string]*PipelineMetrics
}

type PipelineMetrics struct {
    ID              string
    ProcessedCount  uint64
    ErrorCount      uint64
    LastProcessed   time.Time
    ResourceUsage   ResourceMetrics
}

type ResourceMetrics struct {
    CPUCores    float64
    MemoryMB    float64
    NetworkMBps float64
    StorageMB   float64
}

func (mc *MetricsCollector) RecordProcessed(pipeline, processor string) {
    pipelinesProcessed.WithLabelValues(pipeline, processor).Inc()
}

func (mc *MetricsCollector) RecordError(pipeline, processor, errorType string) {
    pipelineErrors.WithLabelValues(pipeline, processor, errorType).Inc()
}

func (mc *MetricsCollector) RecordDuration(pipeline, processor string, duration time.Duration) {
    processingDuration.WithLabelValues(pipeline, processor).Observe(duration.Seconds())
}

func (mc *MetricsCollector) UpdateResourceUsage(pipeline string, resources ResourceMetrics) {
    resourceUsage.WithLabelValues(pipeline, "cpu_cores").Set(resources.CPUCores)
    resourceUsage.WithLabelValues(pipeline, "memory_mb").Set(resources.MemoryMB)
    resourceUsage.WithLabelValues(pipeline, "network_mbps").Set(resources.NetworkMBps)
    resourceUsage.WithLabelValues(pipeline, "storage_mb").Set(resources.StorageMB)
}

func (mc *MetricsCollector) UpdateCostEstimate(pipeline, component string, costPerHour float64) {
    estimatedCost.WithLabelValues(pipeline, component).Set(costPerHour)
}
```

### Phase 5: Safety & Governance (Weeks 7-8)

#### 5.1 Policy Engine

Create `pkg/policy/engine.go`:

```go
package policy

import (
    "context"
    "fmt"
    "time"
)

// Policy defines constraints for pipeline operations
type Policy struct {
    ID          string           `json:"id"`
    Name        string           `json:"name"`
    Description string           `json:"description"`
    Rules       []Rule           `json:"rules"`
    Actions     []string         `json:"actions"` // Actions this policy applies to
    Enabled     bool             `json:"enabled"`
}

// Rule defines a single policy rule
type Rule struct {
    Type        string                 `json:"type"`
    Condition   string                 `json:"condition"`
    Parameters  map[string]interface{} `json:"parameters"`
    EnforceMode string                 `json:"enforce_mode"` // block, warn, audit
}

// PolicyEngine evaluates policies
type PolicyEngine struct {
    policies map[string]*Policy
    auditor  *Auditor
}

// Evaluate checks if an action is allowed
func (pe *PolicyEngine) Evaluate(ctx context.Context, action Action) (*EvaluationResult, error) {
    result := &EvaluationResult{
        Action:     action,
        Allowed:    true,
        Violations: []Violation{},
        EvaluatedAt: time.Now(),
    }
    
    // Find applicable policies
    applicablePolicies := pe.findApplicablePolicies(action.Type)
    
    // Evaluate each policy
    for _, policy := range applicablePolicies {
        if !policy.Enabled {
            continue
        }
        
        policyResult := pe.evaluatePolicy(ctx, policy, action)
        result.PolicyResults = append(result.PolicyResults, policyResult)
        
        if !policyResult.Passed {
            result.Allowed = false
            result.Violations = append(result.Violations, policyResult.Violations...)
        }
    }
    
    // Audit the evaluation
    pe.auditor.RecordEvaluation(result)
    
    return result, nil
}

// Built-in policy rules
var builtInRules = map[string]RuleEvaluator{
    "resource_limit": &ResourceLimitRule{},
    "time_window": &TimeWindowRule{},
    "approval_required": &ApprovalRequiredRule{},
    "rate_limit": &RateLimitRule{},
    "cost_limit": &CostLimitRule{},
}

// ResourceLimitRule enforces resource constraints
type ResourceLimitRule struct{}

func (r *ResourceLimitRule) Evaluate(ctx context.Context, action Action, params map[string]interface{}) (bool, string) {
    maxCPU, _ := params["max_cpu_cores"].(float64)
    maxMemory, _ := params["max_memory_gb"].(float64)
    
    if action.ResourceEstimate.CPUCores > maxCPU {
        return false, fmt.Sprintf("CPU usage %.2f exceeds limit %.2f", action.ResourceEstimate.CPUCores, maxCPU)
    }
    
    if action.ResourceEstimate.MemoryGB > maxMemory {
        return false, fmt.Sprintf("Memory usage %.2f GB exceeds limit %.2f GB", action.ResourceEstimate.MemoryGB, maxMemory)
    }
    
    return true, ""
}

// TimeWindowRule enforces time-based restrictions
type TimeWindowRule struct{}

func (r *TimeWindowRule) Evaluate(ctx context.Context, action Action, params map[string]interface{}) (bool, string) {
    allowedStart, _ := params["start_time"].(string)
    allowedEnd, _ := params["end_time"].(string)
    allowedDays, _ := params["allowed_days"].([]string)
    
    now := time.Now()
    currentDay := now.Weekday().String()
    
    // Check day of week
    dayAllowed := false
    for _, day := range allowedDays {
        if day == currentDay {
            dayAllowed = true
            break
        }
    }
    
    if !dayAllowed {
        return false, fmt.Sprintf("Action not allowed on %s", currentDay)
    }
    
    // Check time window
    startTime, _ := time.Parse("15:04", allowedStart)
    endTime, _ := time.Parse("15:04", allowedEnd)
    currentTime, _ := time.Parse("15:04", now.Format("15:04"))
    
    if currentTime.Before(startTime) || currentTime.After(endTime) {
        return false, fmt.Sprintf("Action not allowed outside time window %s-%s", allowedStart, allowedEnd)
    }
    
    return true, ""
}
```

#### 5.2 Approval System

Create `pkg/approval/webhook.go`:

```go
package approval

import (
    "bytes"
    "context"
    "encoding/json"
    "fmt"
    "net/http"
    "time"
)

// ApprovalRequest represents a request for approval
type ApprovalRequest struct {
    ID          string                 `json:"id"`
    Type        string                 `json:"type"`
    RequestedBy string                 `json:"requested_by"`
    RequestedAt time.Time              `json:"requested_at"`
    Details     map[string]interface{} `json:"details"`
    Plan        *Plan                  `json:"plan,omitempty"`
    Urgency     string                 `json:"urgency"` // low, medium, high, critical
    ExpiresAt   time.Time              `json:"expires_at"`
}

// ApprovalResponse represents the approval decision
type ApprovalResponse struct {
    RequestID   string    `json:"request_id"`
    Approved    bool      `json:"approved"`
    ApprovedBy  string    `json:"approved_by"`
    ApprovedAt  time.Time `json:"approved_at"`
    Reason      string    `json:"reason"`
    Conditions  []string  `json:"conditions,omitempty"`
}

// WebhookApprover sends approval requests via webhook
type WebhookApprover struct {
    webhookURL string
    client     *http.Client
    timeout    time.Duration
}

func NewWebhookApprover(webhookURL string) *WebhookApprover {
    return &WebhookApprover{
        webhookURL: webhookURL,
        client: &http.Client{
            Timeout: 30 * time.Second,
        },
        timeout: 5 * time.Minute,
    }
}

// RequestApproval sends an approval request
func (w *WebhookApprover) RequestApproval(ctx context.Context, request *ApprovalRequest) (*ApprovalResponse, error) {
    // Send webhook
    payload, err := json.Marshal(request)
    if err != nil {
        return nil, fmt.Errorf("failed to marshal request: %w", err)
    }
    
    req, err := http.NewRequestWithContext(ctx, "POST", w.webhookURL, bytes.NewBuffer(payload))
    if err != nil {
        return nil, fmt.Errorf("failed to create request: %w", err)
    }
    
    req.Header.Set("Content-Type", "application/json")
    req.Header.Set("X-Approval-ID", request.ID)
    req.Header.Set("X-Approval-Urgency", request.Urgency)
    
    resp, err := w.client.Do(req)
    if err != nil {
        return nil, fmt.Errorf("webhook request failed: %w", err)
    }
    defer resp.Body.Close()
    
    if resp.StatusCode != http.StatusOK {
        return nil, fmt.Errorf("webhook returned status %d", resp.StatusCode)
    }
    
    // For async approvals, return pending status
    if resp.Header.Get("X-Approval-Async") == "true" {
        return &ApprovalResponse{
            RequestID: request.ID,
            Approved:  false,
            Reason:    "Pending async approval",
        }, nil
    }
    
    // Parse immediate response
    var approvalResp ApprovalResponse
    if err := json.NewDecoder(resp.Body).Decode(&approvalResp); err != nil {
        return nil, fmt.Errorf("failed to decode response: %w", err)
    }
    
    return &approvalResp, nil
}

// WaitForApproval polls for async approval
func (w *WebhookApprover) WaitForApproval(ctx context.Context, requestID string) (*ApprovalResponse, error) {
    ticker := time.NewTicker(5 * time.Second)
    defer ticker.Stop()
    
    deadline := time.Now().Add(w.timeout)
    
    for {
        select {
        case <-ctx.Done():
            return nil, ctx.Err()
        case <-ticker.C:
            if time.Now().After(deadline) {
                return nil, fmt.Errorf("approval timeout exceeded")
            }
            
            // Check approval status
            resp, err := w.checkApprovalStatus(ctx, requestID)
            if err != nil {
                continue // Retry on error
            }
            
            if resp.Approved || resp.Reason != "Pending async approval" {
                return resp, nil
            }
        }
    }
}
```

### Phase 6: Streaming Capabilities (Weeks 5-6)

#### 6.1 Server-Sent Events

Create `pkg/stream/sse.go`:

```go
package stream

import (
    "encoding/json"
    "fmt"
    "net/http"
    "time"
)

// SSEStreamer provides server-sent events streaming
type SSEStreamer struct {
    clients map[string]chan Event
}

// Event represents a server-sent event
type Event struct {
    ID      string      `json:"id"`
    Type    string      `json:"type"`
    Data    interface{} `json:"data"`
    Retry   int         `json:"retry,omitempty"`
}

// StreamHandler creates an SSE stream endpoint
func (s *SSEStreamer) StreamHandler(w http.ResponseWriter, r *http.Request) {
    // Set headers for SSE
    w.Header().Set("Content-Type", "text/event-stream")
    w.Header().Set("Cache-Control", "no-cache")
    w.Header().Set("Connection", "keep-alive")
    w.Header().Set("Access-Control-Allow-Origin", "*")
    
    // Create client channel
    clientID := r.URL.Query().Get("client_id")
    if clientID == "" {
        clientID = generateClientID()
    }
    
    clientChan := make(chan Event, 100)
    s.clients[clientID] = clientChan
    defer func() {
        delete(s.clients, clientID)
        close(clientChan)
    }()
    
    // Send initial connection event
    s.sendEvent(w, Event{
        Type: "connected",
        Data: map[string]string{"client_id": clientID},
    })
    
    // Create ticker for keepalive
    ticker := time.NewTicker(30 * time.Second)
    defer ticker.Stop()
    
    // Stream events
    for {
        select {
        case event := <-clientChan:
            if err := s.sendEvent(w, event); err != nil {
                return
            }
            
        case <-ticker.C:
            // Send keepalive
            if err := s.sendEvent(w, Event{Type: "ping"}); err != nil {
                return
            }
            
        case <-r.Context().Done():
            return
        }
    }
}

func (s *SSEStreamer) sendEvent(w http.ResponseWriter, event Event) error {
    data, err := json.Marshal(event.Data)
    if err != nil {
        return err
    }
    
    // Format SSE message
    fmt.Fprintf(w, "id: %s\n", event.ID)
    fmt.Fprintf(w, "event: %s\n", event.Type)
    fmt.Fprintf(w, "data: %s\n\n", data)
    
    if f, ok := w.(http.Flusher); ok {
        f.Flush()
    }
    
    return nil
}

// BroadcastProgress sends progress updates to all connected clients
func (s *SSEStreamer) BroadcastProgress(pipelineID string, progress ProgressUpdate) {
    event := Event{
        ID:   generateEventID(),
        Type: "progress",
        Data: progress,
    }
    
    for _, clientChan := range s.clients {
        select {
        case clientChan <- event:
        default:
            // Client channel full, skip
        }
    }
}

// ProgressUpdate represents pipeline progress
type ProgressUpdate struct {
    PipelineID      string    `json:"pipeline_id"`
    Component       string    `json:"component"`
    Progress        float64   `json:"progress"` // 0-100
    CurrentItem     string    `json:"current_item"`
    ItemsProcessed  int       `json:"items_processed"`
    ItemsTotal      int       `json:"items_total"`
    TimeElapsed     string    `json:"time_elapsed"`
    TimeRemaining   string    `json:"time_remaining"`
    ProcessingRate  float64   `json:"processing_rate"`
    Timestamp       time.Time `json:"timestamp"`
}
```

### Phase 7: Evaluators & Success Criteria (Weeks 7-8)

#### 7.1 Evaluation Engine

Create `pkg/evaluator/engine.go`:

```go
package evaluator

import (
    "context"
    "fmt"
    "time"
)

// Evaluator checks if pipeline operations meet success criteria
type Evaluator struct {
    predicates map[string]Predicate
    rollback   *RollbackManager
}

// Predicate defines a success/failure condition
type Predicate interface {
    Evaluate(ctx context.Context, data interface{}) (bool, string)
    Name() string
    Type() string // success, failure, warning
}

// EvaluationResult contains the outcome of evaluations
type EvaluationResult struct {
    PipelineID   string              `json:"pipeline_id"`
    RunID        string              `json:"run_id"`
    Timestamp    time.Time           `json:"timestamp"`
    Success      bool                `json:"success"`
    Predicates   []PredicateResult   `json:"predicates"`
    Recommendation string            `json:"recommendation"`
    AutoActions  []string            `json:"auto_actions"`
}

// PredicateResult is the result of a single predicate
type PredicateResult struct {
    Name    string `json:"name"`
    Type    string `json:"type"`
    Passed  bool   `json:"passed"`
    Message string `json:"message"`
}

// Built-in predicates
type LatencyPredicate struct {
    MaxLatencyMs int
}

func (p *LatencyPredicate) Evaluate(ctx context.Context, data interface{}) (bool, string) {
    metrics, ok := data.(PipelineMetrics)
    if !ok {
        return false, "invalid data type"
    }
    
    if metrics.AvgLatencyMs > p.MaxLatencyMs {
        return false, fmt.Sprintf("latency %dms exceeds threshold %dms", metrics.AvgLatencyMs, p.MaxLatencyMs)
    }
    
    return true, fmt.Sprintf("latency %dms within threshold", metrics.AvgLatencyMs)
}

func (p *LatencyPredicate) Name() string { return "latency_check" }
func (p *LatencyPredicate) Type() string { return "success" }

// ErrorRatePredicate checks error rates
type ErrorRatePredicate struct {
    MaxErrorRate float64
}

func (p *ErrorRatePredicate) Evaluate(ctx context.Context, data interface{}) (bool, string) {
    metrics, ok := data.(PipelineMetrics)
    if !ok {
        return false, "invalid data type"
    }
    
    errorRate := float64(metrics.ErrorCount) / float64(metrics.ProcessedCount)
    if errorRate > p.MaxErrorRate {
        return false, fmt.Sprintf("error rate %.2f%% exceeds threshold %.2f%%", errorRate*100, p.MaxErrorRate*100)
    }
    
    return true, fmt.Sprintf("error rate %.2f%% within threshold", errorRate*100)
}

// ThroughputPredicate checks processing throughput
type ThroughputPredicate struct {
    MinThroughput int // items per second
}

func (p *ThroughputPredicate) Evaluate(ctx context.Context, data interface{}) (bool, string) {
    metrics, ok := data.(PipelineMetrics)
    if !ok {
        return false, "invalid data type"
    }
    
    if metrics.Throughput < p.MinThroughput {
        return false, fmt.Sprintf("throughput %d/s below minimum %d/s", metrics.Throughput, p.MinThroughput)
    }
    
    return true, fmt.Sprintf("throughput %d/s meets requirement", metrics.Throughput)
}

// Evaluate runs all predicates against pipeline data
func (e *Evaluator) Evaluate(ctx context.Context, pipelineID, runID string) (*EvaluationResult, error) {
    // Get pipeline metrics
    metrics, err := e.getMetrics(ctx, pipelineID)
    if err != nil {
        return nil, fmt.Errorf("failed to get metrics: %w", err)
    }
    
    result := &EvaluationResult{
        PipelineID: pipelineID,
        RunID:      runID,
        Timestamp:  time.Now(),
        Success:    true,
        Predicates: []PredicateResult{},
    }
    
    // Run all predicates
    for name, predicate := range e.predicates {
        passed, message := predicate.Evaluate(ctx, metrics)
        
        predResult := PredicateResult{
            Name:    name,
            Type:    predicate.Type(),
            Passed:  passed,
            Message: message,
        }
        
        result.Predicates = append(result.Predicates, predResult)
        
        // Update overall success
        if !passed && predicate.Type() == "success" {
            result.Success = false
        }
    }
    
    // Generate recommendation
    result.Recommendation = e.generateRecommendation(result)
    
    // Determine auto-actions
    if !result.Success {
        result.AutoActions = e.determineAutoActions(result)
    }
    
    return result, nil
}

func (e *Evaluator) generateRecommendation(result *EvaluationResult) string {
    if result.Success {
        return "Pipeline operating within normal parameters"
    }
    
    // Analyze failures and provide recommendations
    var recommendations []string
    
    for _, pred := range result.Predicates {
        if !pred.Passed {
            switch pred.Name {
            case "latency_check":
                recommendations = append(recommendations, "Consider scaling up processing resources")
            case "error_rate_check":
                recommendations = append(recommendations, "Investigate error sources and add retry logic")
            case "throughput_check":
                recommendations = append(recommendations, "Increase parallelism or optimize processing logic")
            }
        }
    }
    
    return strings.Join(recommendations, "; ")
}
```

### Phase 8: Cost Awareness (Weeks 7-8)

#### 8.1 Cost Calculator

Create `pkg/cost/calculator.go`:

```go
package cost

import (
    "fmt"
    "time"
)

// CostCalculator estimates resource costs
type CostCalculator struct {
    rates ResourceRates
}

// ResourceRates defines cost per unit of resource
type ResourceRates struct {
    CPUCoreHour      float64 // $ per CPU core hour
    MemoryGBHour     float64 // $ per GB memory hour
    NetworkGBTransfer float64 // $ per GB transferred
    StorageGBMonth   float64 // $ per GB stored per month
    RequestCost      float64 // $ per 1000 requests
}

// DefaultRates provides typical cloud pricing
var DefaultRates = ResourceRates{
    CPUCoreHour:      0.04,    // ~$30/month per core
    MemoryGBHour:     0.005,   // ~$3.60/month per GB
    NetworkGBTransfer: 0.09,   // Typical egress cost
    StorageGBMonth:   0.023,   // S3 standard pricing
    RequestCost:      0.0004,  // Per 1000 requests
}

// ComponentCost tracks costs for a component
type ComponentCost struct {
    Component    string        `json:"component"`
    Type         string        `json:"type"`
    CPUCost      float64       `json:"cpu_cost"`
    MemoryCost   float64       `json:"memory_cost"`
    NetworkCost  float64       `json:"network_cost"`
    StorageCost  float64       `json:"storage_cost"`
    RequestCost  float64       `json:"request_cost"`
    TotalCost    float64       `json:"total_cost"`
    Period       time.Duration `json:"period"`
    CostPerHour  float64       `json:"cost_per_hour"`
}

// PipelineCost aggregates all component costs
type PipelineCost struct {
    PipelineID     string          `json:"pipeline_id"`
    Components     []ComponentCost `json:"components"`
    TotalCostPerHour float64       `json:"total_cost_per_hour"`
    TotalCostPerDay  float64       `json:"total_cost_per_day"`
    TotalCostPerMonth float64      `json:"total_cost_per_month"`
    Breakdown      CostBreakdown   `json:"breakdown"`
}

// CostBreakdown shows cost by resource type
type CostBreakdown struct {
    CPU      float64 `json:"cpu"`
    Memory   float64 `json:"memory"`
    Network  float64 `json:"network"`
    Storage  float64 `json:"storage"`
    Requests float64 `json:"requests"`
}

// EstimateComponentCost calculates cost for a single component
func (c *CostCalculator) EstimateComponentCost(hints CostHints, duration time.Duration) ComponentCost {
    hours := duration.Hours()
    
    cpuCost := float64(hints.CPUMillicores) / 1000.0 * c.rates.CPUCoreHour * hours
    memoryCost := float64(hints.MemoryMB) / 1024.0 * c.rates.MemoryGBHour * hours
    
    // Estimate based on throughput
    gbTransferred := float64(hints.NetworkBytesPerOp) / 1e9 * float64(hints.OpsPerHour) * hours
    networkCost := gbTransferred * c.rates.NetworkGBTransfer
    
    // Storage cost (monthly rate converted to hourly)
    gbStored := float64(hints.StorageBytesPerOp) / 1e9 * float64(hints.TotalOps)
    storageCost := gbStored * c.rates.StorageGBMonth / 730.0 * hours
    
    // Request cost
    requestCost := float64(hints.OpsPerHour) * hours / 1000.0 * c.rates.RequestCost
    
    totalCost := cpuCost + memoryCost + networkCost + storageCost + requestCost
    
    return ComponentCost{
        Component:    hints.ComponentName,
        Type:         hints.ComponentType,
        CPUCost:      cpuCost,
        MemoryCost:   memoryCost,
        NetworkCost:  networkCost,
        StorageCost:  storageCost,
        RequestCost:  requestCost,
        TotalCost:    totalCost,
        Period:       duration,
        CostPerHour:  totalCost / hours,
    }
}

// EstimatePipelineCost calculates total pipeline cost
func (c *CostCalculator) EstimatePipelineCost(config PipelineConfig) (*PipelineCost, error) {
    cost := &PipelineCost{
        PipelineID: config.ID,
        Components: []ComponentCost{},
    }
    
    // Calculate source cost
    sourceHints := c.getSourceCostHints(config.Source)
    sourceCost := c.EstimateComponentCost(sourceHints, time.Hour)
    cost.Components = append(cost.Components, sourceCost)
    
    // Calculate processor costs
    for _, proc := range config.Processors {
        procHints := c.getProcessorCostHints(proc)
        procCost := c.EstimateComponentCost(procHints, time.Hour)
        cost.Components = append(cost.Components, procCost)
    }
    
    // Calculate consumer costs
    for _, cons := range config.Consumers {
        consHints := c.getConsumerCostHints(cons)
        consCost := c.EstimateComponentCost(consHints, time.Hour)
        cost.Components = append(cost.Components, consCost)
    }
    
    // Aggregate totals
    for _, comp := range cost.Components {
        cost.TotalCostPerHour += comp.CostPerHour
        cost.Breakdown.CPU += comp.CPUCost
        cost.Breakdown.Memory += comp.MemoryCost
        cost.Breakdown.Network += comp.NetworkCost
        cost.Breakdown.Storage += comp.StorageCost
        cost.Breakdown.Requests += comp.RequestCost
    }
    
    cost.TotalCostPerDay = cost.TotalCostPerHour * 24
    cost.TotalCostPerMonth = cost.TotalCostPerDay * 30
    
    return cost, nil
}

// AddCostHeaders adds cost information to HTTP response headers
func AddCostHeaders(w http.ResponseWriter, cost ComponentCost) {
    w.Header().Set("X-Cost-CPU-Dollars", fmt.Sprintf("%.4f", cost.CPUCost))
    w.Header().Set("X-Cost-Memory-Dollars", fmt.Sprintf("%.4f", cost.MemoryCost))
    w.Header().Set("X-Cost-Network-Dollars", fmt.Sprintf("%.4f", cost.NetworkCost))
    w.Header().Set("X-Cost-Total-Dollars", fmt.Sprintf("%.4f", cost.TotalCost))
    w.Header().Set("X-Cost-Per-Hour-Dollars", fmt.Sprintf("%.4f", cost.CostPerHour))
}
```

## llms.txt Generation

Create `llms.txt`:

```
# CDP Pipeline - AI-Native Interface

## Overview
The CDP (Composable Data Platform) Pipeline is an AI-native system for processing Stellar blockchain data. All operations are exposed as callable tools with JSON Schema definitions.

## Base URL
https://api.cdp-pipeline.example.com/v1

## Authentication
Bearer token required in Authorization header

## Tool Discovery
GET /tools - List all available tools with schemas
GET /tools/{id} - Get detailed tool card for a specific tool

## Pipeline Operations

### Plan Pipeline Changes
POST /pipelines/plan
Creates a plan for pipeline configuration changes with diff, cost estimates, and safety checks.

Request:
{
  "pipeline": <PipelineConfig>,
  "dry_run": boolean,
  "auto_approve": boolean
}

Response includes plan ID, changes, cost estimates, and required approvals.

### Apply Plan
POST /pipelines/apply
Executes an approved plan with streaming progress updates.

Request:
{
  "plan_id": string,
  "stream_events": boolean
}

### Pipeline Control
POST /pipelines/{id}/start - Start a pipeline
POST /pipelines/{id}/stop - Stop a pipeline
POST /pipelines/{id}/pause - Pause processing

### Pipeline Status
GET /pipelines/{id}/status - Get current pipeline status with health checks
GET /pipelines/{id}/metrics - Get real-time metrics

## Streaming Endpoints
GET /pipelines/{id}/stream/metrics - Server-sent events for metrics
GET /pipelines/{id}/stream/logs - Server-sent events for logs

## Cost Information
All responses include cost hints in headers:
- X-Cost-CPU-Millicores
- X-Cost-Memory-MB
- X-Cost-Total-Dollars
- X-Cost-Per-Hour-Dollars

## Safety Features
- All destructive operations require plan/apply pattern
- Configurable approval webhooks for sensitive changes
- Policy engine enforces resource limits and time windows
- Automatic rollback on failure

## Example: Configure Contract Events Pipeline

1. Get tool card:
GET /tools/processor.contract_events

2. Create plan:
POST /pipelines/plan
{
  "pipeline": {
    "id": "contract-events-mainnet",
    "source": {
      "type": "BufferedStorageSourceAdapter",
      "config": {
        "bucket_name": "stellar-ledgers",
        "network": "mainnet"
      }
    },
    "processors": [{
      "type": "ContractEvents",
      "config": {
        "network_passphrase": "Public Global Stellar Network ; September 2015"
      }
    }],
    "consumers": [{
      "type": "SaveToPostgreSQL",
      "config": {
        "connection_string": "postgresql://..."
      }
    }]
  }
}

3. Review plan and approve if needed

4. Apply plan:
POST /pipelines/apply
{
  "plan_id": "plan_xyz",
  "stream_events": true
}

5. Monitor progress via SSE stream
```

## Migration Guide

### Phase 1: Add API Layer (No Breaking Changes)
1. Deploy new API server alongside existing CLI
2. Existing YAML configs continue to work
3. New API provides programmatic access

### Phase 2: Gradual Tool Card Adoption
1. Generate tool cards for existing components
2. Update documentation to reference tool cards
3. CLI reads tool cards for validation

### Phase 3: Enhanced Features
1. Add telemetry to all components
2. Implement plan/apply for config changes
3. Add streaming progress updates

### Phase 4: Full AI-Native
1. Deprecate direct YAML editing
2. All changes go through plan/apply
3. Policy engine enforces all operations

## Testing Strategy

### Unit Tests
- Tool card validation
- Policy engine rules
- Cost calculations
- Diff generation

### Integration Tests
- End-to-end pipeline operations
- API endpoint testing
- Streaming functionality
- Rollback scenarios

### AI Agent Testing
- Validate tool discovery
- Test plan/apply workflows
- Verify streaming updates
- Check error handling

## Success Metrics

### Technical Metrics
- API response time < 100ms (p99)
- Streaming latency < 50ms
- Plan generation < 1s
- Tool discovery < 200ms

### Business Metrics
- Reduction in configuration errors
- Faster pipeline deployment
- Improved resource utilization
- Lower operational costs

### AI-Native Metrics
- Tool invocation success rate > 95%
- Plan approval rate > 80%
- Automatic rollback success > 99%
- Agent task completion > 90%

## Conclusion

This implementation guide provides a comprehensive roadmap for transforming the CDP Pipeline into an AI-native system. The phased approach ensures backward compatibility while progressively adding AI-native capabilities. The result is a system that can be operated end-to-end by AI agents while maintaining human oversight and control.