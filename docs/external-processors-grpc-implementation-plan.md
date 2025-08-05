# External Processors and Consumers via gRPC Implementation Plan

## Executive Summary

This document outlines the implementation plan for enabling cdp-pipeline-workflow to support external processors and consumers as separate services communicating via gRPC. This architecture will allow the core pipeline to remain lightweight while enabling language-agnostic processor development and independent scaling.

## Design Goals

1. **Minimal Core Changes**: The existing processor interface remains unchanged
2. **Language Agnostic**: External processors can be written in any language
3. **Static Configuration First**: Start with simple endpoint configuration
4. **Backward Compatible**: All existing processors continue to work
5. **Performance Conscious**: Support both unary and streaming gRPC

## Architecture Overview

### Current State
```
[Source] → [Processor1] → [Processor2] → [Consumer]
          (in-process)   (in-process)   (in-process)
```

### Future State
```
[Source] → [Processor1] → [GRPCBridge] → [External] → [GRPCBridge] → [Consumer]
          (in-process)   (proxy)       (service)    (proxy)       (in-process)
```

## Protocol Buffer Definitions

### 1. Core Pipeline Service Proto

Create `protos/pipeline/v1/pipeline_service.proto`:

```proto
syntax = "proto3";

package pipeline.v1;

option go_package = "github.com/withObsrvr/cdp-pipeline-workflow/gen/pipeline/v1";

import "google/protobuf/timestamp.proto";
import "google/protobuf/struct.proto";

// Generic processor service that any external processor must implement
service ProcessorService {
  // Process a single message (unary)
  rpc Process(ProcessRequest) returns (ProcessResponse);
  
  // Process a stream of messages (streaming)
  rpc StreamProcess(stream ProcessRequest) returns (stream ProcessResponse);
  
  // Health check
  rpc HealthCheck(HealthCheckRequest) returns (HealthCheckResponse);
}

// Generic consumer service that any external consumer must implement
service ConsumerService {
  // Consume a single message
  rpc Consume(ConsumeRequest) returns (ConsumeResponse);
  
  // Consume a stream of messages
  rpc StreamConsume(stream ConsumeRequest) returns (stream ConsumeResponse);
  
  // Health check
  rpc HealthCheck(HealthCheckRequest) returns (HealthCheckResponse);
}

// Process request wraps the pipeline Message
message ProcessRequest {
  // Unique ID for tracing
  string trace_id = 1;
  
  // The actual payload (serialized as JSON for flexibility)
  bytes payload = 2;
  
  // Metadata as key-value pairs
  map<string, string> metadata = 3;
  
  // Optional: Archive source metadata for data provenance
  ArchiveSourceMetadata archive_source = 4;
  
  // Timestamp when message was sent
  google.protobuf.Timestamp sent_at = 5;
}

message ProcessResponse {
  // Echo back trace ID for correlation
  string trace_id = 1;
  
  // Processed payload
  bytes payload = 2;
  
  // Updated or new metadata
  map<string, string> metadata = 3;
  
  // Processing status
  ProcessingStatus status = 4;
  
  // Error details if status is ERROR
  string error_message = 5;
  
  // Processing duration in milliseconds
  int64 processing_time_ms = 6;
}

message ConsumeRequest {
  string trace_id = 1;
  bytes payload = 2;
  map<string, string> metadata = 3;
  google.protobuf.Timestamp sent_at = 4;
}

message ConsumeResponse {
  string trace_id = 1;
  ConsumptionStatus status = 2;
  string error_message = 3;
  int64 processing_time_ms = 4;
}

// Archive metadata (matching cdp-pipeline-workflow structure)
message ArchiveSourceMetadata {
  string source_type = 1;        // "S3", "GCS", "FS"
  string bucket_name = 2;        // for cloud storage
  string file_path = 3;          // datastore schema path
  string file_name = 4;          // just filename
  uint32 start_ledger = 5;       // first ledger in file
  uint32 end_ledger = 6;         // last ledger in file
  google.protobuf.Timestamp processed_at = 7;
  int64 file_size = 8;
  uint32 partition = 9;
  string full_cloud_url = 10;
  string history_archive_path = 11;
}

enum ProcessingStatus {
  PROCESSING_STATUS_UNSPECIFIED = 0;
  PROCESSING_STATUS_SUCCESS = 1;
  PROCESSING_STATUS_ERROR = 2;
  PROCESSING_STATUS_SKIPPED = 3;
}

enum ConsumptionStatus {
  CONSUMPTION_STATUS_UNSPECIFIED = 0;
  CONSUMPTION_STATUS_SUCCESS = 1;
  CONSUMPTION_STATUS_ERROR = 2;
  CONSUMPTION_STATUS_RETRY = 3;
}

// Health check messages
message HealthCheckRequest {}

message HealthCheckResponse {
  HealthStatus status = 1;
  string message = 2;
  map<string, string> metadata = 3;
}

enum HealthStatus {
  HEALTH_STATUS_UNSPECIFIED = 0;
  HEALTH_STATUS_HEALTHY = 1;
  HEALTH_STATUS_UNHEALTHY = 2;
  HEALTH_STATUS_DEGRADED = 3;
}
```

### 2. Specialized Processor Protos (Optional)

For processors that need strongly-typed interfaces, create specialized protos:

```proto
// protos/stellar/v1/stellar_processor.proto
syntax = "proto3";

package stellar.v1;

import "pipeline/v1/pipeline_service.proto";

// Specialized service for Stellar data processing
service StellarProcessorService {
  // Process ledger close meta
  rpc ProcessLedger(LedgerRequest) returns (LedgerResponse);
  
  // Generic pipeline interface (must implement)
  rpc Process(pipeline.v1.ProcessRequest) returns (pipeline.v1.ProcessResponse);
}

message LedgerRequest {
  uint32 sequence = 1;
  bytes ledger_close_meta_xdr = 2;
}

message LedgerResponse {
  uint32 sequence = 1;
  repeated ContractInvocation invocations = 2;
}

// Domain-specific types...
```

## Implementation Components

### 1. gRPC Bridge Processor

Create `internal/bridge/grpc_processor.go`:

```go
package bridge

import (
    "context"
    "encoding/json"
    "fmt"
    "time"
    
    "google.golang.org/grpc"
    "google.golang.org/grpc/credentials/insecure"
    "google.golang.org/grpc/health/grpc_health_v1"
    
    "github.com/withObsrvr/cdp-pipeline-workflow/processor"
    pb "github.com/withObsrvr/cdp-pipeline-workflow/gen/pipeline/v1"
)

// GRPCProcessorConfig holds configuration for external processor
type GRPCProcessorConfig struct {
    Endpoint        string        `yaml:"endpoint"`
    Timeout         time.Duration `yaml:"timeout"`
    MaxRetries      int          `yaml:"max_retries"`
    UseStreaming    bool         `yaml:"use_streaming"`
    HealthCheckInterval time.Duration `yaml:"health_check_interval"`
}

// GRPCProcessor bridges internal processors to external gRPC services
type GRPCProcessor struct {
    config     GRPCProcessorConfig
    client     pb.ProcessorServiceClient
    conn       *grpc.ClientConn
    downstream []processor.Processor
    healthy    bool
}

// NewGRPCProcessor creates a new gRPC processor bridge
func NewGRPCProcessor(config map[string]interface{}) (*GRPCProcessor, error) {
    // Parse configuration
    cfg := GRPCProcessorConfig{
        Timeout:    30 * time.Second,
        MaxRetries: 3,
        HealthCheckInterval: 10 * time.Second,
    }
    
    if endpoint, ok := config["endpoint"].(string); ok {
        cfg.Endpoint = endpoint
    } else {
        return nil, fmt.Errorf("endpoint is required")
    }
    
    if timeout, ok := config["timeout"].(string); ok {
        if d, err := time.ParseDuration(timeout); err == nil {
            cfg.Timeout = d
        }
    }
    
    if streaming, ok := config["use_streaming"].(bool); ok {
        cfg.UseStreaming = streaming
    }
    
    // Connect to external service
    conn, err := grpc.Dial(cfg.Endpoint, 
        grpc.WithTransportCredentials(insecure.NewCredentials()),
        grpc.WithDefaultCallOptions(
            grpc.MaxCallRecvMsgSize(100*1024*1024), // 100MB
            grpc.MaxCallSendMsgSize(100*1024*1024),
        ),
    )
    if err != nil {
        return nil, fmt.Errorf("failed to connect to %s: %w", cfg.Endpoint, err)
    }
    
    client := pb.NewProcessorServiceClient(conn)
    
    p := &GRPCProcessor{
        config: cfg,
        client: client,
        conn:   conn,
        healthy: true,
    }
    
    // Start health check routine
    go p.healthCheckLoop()
    
    return p, nil
}

// Process implements processor.Processor interface
func (p *GRPCProcessor) Process(ctx context.Context, msg processor.Message) error {
    if !p.healthy {
        return fmt.Errorf("processor at %s is unhealthy", p.config.Endpoint)
    }
    
    // Serialize payload
    payload, err := json.Marshal(msg.Payload)
    if err != nil {
        return fmt.Errorf("failed to serialize payload: %w", err)
    }
    
    // Convert metadata
    metadata := make(map[string]string)
    for k, v := range msg.Metadata {
        metadata[k] = fmt.Sprintf("%v", v)
    }
    
    // Create request
    req := &pb.ProcessRequest{
        TraceId:  generateTraceID(),
        Payload:  payload,
        Metadata: metadata,
        SentAt:   timestamppb.Now(),
    }
    
    // Add archive metadata if present
    if archiveMeta, ok := msg.GetArchiveMetadata(); ok {
        req.ArchiveSource = convertArchiveMetadata(archiveMeta)
    }
    
    // Call external service with timeout and retries
    ctx, cancel := context.WithTimeout(ctx, p.config.Timeout)
    defer cancel()
    
    var resp *pb.ProcessResponse
    for attempt := 0; attempt <= p.config.MaxRetries; attempt++ {
        resp, err = p.client.Process(ctx, req)
        if err == nil {
            break
        }
        
        if attempt < p.config.MaxRetries {
            time.Sleep(time.Duration(attempt+1) * time.Second)
        }
    }
    
    if err != nil {
        return fmt.Errorf("failed to process after %d attempts: %w", p.config.MaxRetries, err)
    }
    
    if resp.Status != pb.ProcessingStatus_PROCESSING_STATUS_SUCCESS {
        return fmt.Errorf("processing failed: %s", resp.ErrorMessage)
    }
    
    // Deserialize response
    var newPayload interface{}
    if err := json.Unmarshal(resp.Payload, &newPayload); err != nil {
        return fmt.Errorf("failed to deserialize response: %w", err)
    }
    
    // Create new message
    newMsg := processor.Message{
        Payload:  newPayload,
        Metadata: convertMetadataBack(resp.Metadata),
    }
    
    // Forward to downstream processors
    for _, downstream := range p.downstream {
        if err := downstream.Process(ctx, newMsg); err != nil {
            return fmt.Errorf("downstream processing failed: %w", err)
        }
    }
    
    return nil
}

// Subscribe implements processor.Processor interface
func (p *GRPCProcessor) Subscribe(downstream processor.Processor) {
    p.downstream = append(p.downstream, downstream)
}

// healthCheckLoop monitors the health of external service
func (p *GRPCProcessor) healthCheckLoop() {
    ticker := time.NewTicker(p.config.HealthCheckInterval)
    defer ticker.Stop()
    
    for range ticker.C {
        ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
        resp, err := p.client.HealthCheck(ctx, &pb.HealthCheckRequest{})
        cancel()
        
        if err != nil || resp.Status != pb.HealthStatus_HEALTH_STATUS_HEALTHY {
            p.healthy = false
        } else {
            p.healthy = true
        }
    }
}

// Close cleans up resources
func (p *GRPCProcessor) Close() error {
    return p.conn.Close()
}
```

### 2. gRPC Bridge Consumer

Create `internal/bridge/grpc_consumer.go`:

```go
package bridge

import (
    "context"
    "encoding/json"
    "fmt"
    "time"
    
    "google.golang.org/grpc"
    
    "github.com/withObsrvr/cdp-pipeline-workflow/processor"
    pb "github.com/withObsrvr/cdp-pipeline-workflow/gen/pipeline/v1"
)

// GRPCConsumer bridges internal consumers to external gRPC services
type GRPCConsumer struct {
    config     GRPCConsumerConfig
    client     pb.ConsumerServiceClient
    conn       *grpc.ClientConn
    downstream []processor.Processor
}

// Similar implementation to GRPCProcessor but calls ConsumerService
```

### 3. Factory Registration

Update `main.go` to register the new bridge processors:

```go
// In createProcessor function, add:
case "ExternalProcessor":
    return bridge.NewGRPCProcessor(processorConfig.Config)

// In createConsumer function, add:
case "ExternalConsumer":
    return bridge.NewGRPCConsumer(consumerConfig.Config)
```

## Configuration Examples

### 1. Basic External Processor

```yaml
pipelines:
  stellar-processing:
    name: "Stellar with External TTP Processor"
    source:
      type: "BufferedStorageSourceAdapter"
      config:
        storage_type: "GCS"
        bucket_name: "stellar-ledgers"
        
    processors:
      # Internal processor
      - type: "LedgerReader"
        config:
          network: "pubnet"
          
      # External gRPC processor
      - type: "ExternalProcessor"
        config:
          endpoint: "ttp-processor:50051"
          timeout: "30s"
          max_retries: 3
          use_streaming: false
          
    consumers:
      # External gRPC consumer
      - type: "ExternalConsumer"
        config:
          endpoint: "analytics-sink:50052"
          timeout: "60s"
          use_streaming: true
```

### 2. Multiple External Services

```yaml
pipelines:
  complex-pipeline:
    name: "Multi-Service Pipeline"
    source:
      type: "StellarRPC"
      config:
        endpoint: "https://horizon-testnet.stellar.org"
        
    processors:
      # Chain multiple external processors
      - type: "ExternalProcessor"
        config:
          endpoint: "ledger-validator:50051"
          timeout: "10s"
          
      - type: "ExternalProcessor"
        config:
          endpoint: "contract-extractor:50052"
          timeout: "20s"
          
      - type: "ExternalProcessor"
        config:
          endpoint: "enrichment-service:50053"
          timeout: "15s"
          
    consumers:
      # Multiple consumers
      - type: "SaveToPostgreSQL"
        config:
          connection_string: "postgres://..."
          
      - type: "ExternalConsumer"
        config:
          endpoint: "notification-service:50054"
```

## Example External Processor Implementation

### Go Implementation

```go
// external/ttp-processor/main.go
package main

import (
    "context"
    "encoding/json"
    "net"
    
    "google.golang.org/grpc"
    pb "github.com/withObsrvr/cdp-pipeline-workflow/gen/pipeline/v1"
)

type server struct {
    pb.UnimplementedProcessorServiceServer
}

func (s *server) Process(ctx context.Context, req *pb.ProcessRequest) (*pb.ProcessResponse, error) {
    // Deserialize the Stellar ledger data
    var ledgerData LedgerCloseMeta
    if err := json.Unmarshal(req.Payload, &ledgerData); err != nil {
        return &pb.ProcessResponse{
            TraceId:      req.TraceId,
            Status:       pb.ProcessingStatus_PROCESSING_STATUS_ERROR,
            ErrorMessage: err.Error(),
        }, nil
    }
    
    // Process and extract TTP events
    ttpEvents := extractTTPEvents(ledgerData)
    
    // Serialize response
    responseData, err := json.Marshal(ttpEvents)
    if err != nil {
        return &pb.ProcessResponse{
            TraceId:      req.TraceId,
            Status:       pb.ProcessingStatus_PROCESSING_STATUS_ERROR,
            ErrorMessage: err.Error(),
        }, nil
    }
    
    return &pb.ProcessResponse{
        TraceId:  req.TraceId,
        Payload:  responseData,
        Metadata: req.Metadata, // Pass through metadata
        Status:   pb.ProcessingStatus_PROCESSING_STATUS_SUCCESS,
    }, nil
}

func (s *server) HealthCheck(ctx context.Context, req *pb.HealthCheckRequest) (*pb.HealthCheckResponse, error) {
    return &pb.HealthCheckResponse{
        Status: pb.HealthStatus_HEALTH_STATUS_HEALTHY,
        Message: "TTP Processor is healthy",
    }, nil
}

func main() {
    lis, err := net.Listen("tcp", ":50051")
    if err != nil {
        log.Fatalf("failed to listen: %v", err)
    }
    
    s := grpc.NewServer()
    pb.RegisterProcessorServiceServer(s, &server{})
    
    log.Printf("TTP Processor listening on %v", lis.Addr())
    if err := s.Serve(lis); err != nil {
        log.Fatalf("failed to serve: %v", err)
    }
}
```

### Python Implementation

```python
# external/fraud-detector/server.py
import grpc
from concurrent import futures
import json

import pipeline_pb2
import pipeline_pb2_grpc

class FraudDetectorService(pipeline_pb2_grpc.ProcessorServiceServicer):
    def Process(self, request, context):
        # Deserialize payload
        data = json.loads(request.payload)
        
        # Detect fraud patterns
        fraud_score = self.calculate_fraud_score(data)
        
        # Add fraud metadata
        response_data = {
            **data,
            "fraud_score": fraud_score,
            "fraud_detected": fraud_score > 0.7
        }
        
        return pipeline_pb2.ProcessResponse(
            trace_id=request.trace_id,
            payload=json.dumps(response_data).encode(),
            status=pipeline_pb2.PROCESSING_STATUS_SUCCESS
        )
    
    def HealthCheck(self, request, context):
        return pipeline_pb2.HealthCheckResponse(
            status=pipeline_pb2.HEALTH_STATUS_HEALTHY
        )

def serve():
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
    pipeline_pb2_grpc.add_ProcessorServiceServicer_to_server(
        FraudDetectorService(), server
    )
    server.add_insecure_port('[::]:50051')
    server.start()
    server.wait_for_termination()
```

## Deployment Patterns

### 1. Docker Compose Development

```yaml
version: '3.8'
services:
  cdp-pipeline:
    image: cdp-pipeline-workflow:latest
    volumes:
      - ./config:/config
    command: ["-config", "/config/pipeline.yaml"]
    
  ttp-processor:
    image: ttp-processor:latest
    ports:
      - "50051:50051"
      
  analytics-sink:
    image: analytics-sink:latest
    ports:
      - "50052:50052"
```

### 2. Kubernetes Production

```yaml
apiVersion: apps/v1
kind: Deployment
metadata:
  name: external-ttp-processor
spec:
  replicas: 3
  selector:
    matchLabels:
      app: ttp-processor
  template:
    metadata:
      labels:
        app: ttp-processor
    spec:
      containers:
      - name: processor
        image: ttp-processor:v1.0
        ports:
        - containerPort: 50051
        resources:
          requests:
            cpu: "500m"
            memory: "1Gi"
          limits:
            cpu: "2"
            memory: "4Gi"
---
apiVersion: v1
kind: Service
metadata:
  name: ttp-processor
spec:
  selector:
    app: ttp-processor
  ports:
  - port: 50051
    targetPort: 50051
```

## Performance Considerations

### 1. Batching Support

For high-throughput scenarios, implement batching in the bridge:

```go
type BatchingGRPCProcessor struct {
    *GRPCProcessor
    batchSize    int
    batchTimeout time.Duration
    buffer       []processor.Message
}

func (b *BatchingGRPCProcessor) Process(ctx context.Context, msg processor.Message) error {
    b.buffer = append(b.buffer, msg)
    
    if len(b.buffer) >= b.batchSize {
        return b.flushBatch(ctx)
    }
    
    // Set timeout for partial batch
    return nil
}
```

### 2. Connection Pooling

For better performance with multiple external services:

```go
type ConnectionPool struct {
    connections []*grpc.ClientConn
    current     int
    mu          sync.Mutex
}

func (p *ConnectionPool) GetConnection() *grpc.ClientConn {
    p.mu.Lock()
    defer p.mu.Unlock()
    
    conn := p.connections[p.current]
    p.current = (p.current + 1) % len(p.connections)
    return conn
}
```

## Migration Strategy

### Phase 1: Proof of Concept (Week 1)
1. Implement basic GRPCProcessor bridge
2. Create example external processor
3. Test with simple pipeline
4. Measure performance overhead

### Phase 2: Production Features (Week 2-3)
1. Add streaming support
2. Implement health checks
3. Add metrics and tracing
4. Create deployment templates

### Phase 3: Advanced Features (Week 4+)
1. Service discovery integration
2. Circuit breaker implementation
3. Advanced load balancing
4. Multi-version support

## Monitoring and Observability

### 1. Metrics to Track
- Request latency per external service
- Success/failure rates
- Message throughput
- Connection pool utilization
- Health check status

### 2. Distributed Tracing
- Use trace_id for request correlation
- Integrate with OpenTelemetry
- Track processing time across services

## Security Considerations

### 1. Transport Security
- Use TLS for production deployments
- Implement mutual TLS for service-to-service
- Rotate certificates regularly

### 2. Authentication
- Add service authentication tokens
- Implement request signing
- Use service mesh for advanced security

## Testing Strategy

### 1. Unit Tests
- Mock gRPC clients for bridge testing
- Test serialization/deserialization
- Test retry logic

### 2. Integration Tests
- Use testcontainers for external services
- Test full pipeline with external processors
- Benchmark performance impact

### 3. Load Tests
- Measure throughput with external services
- Test scaling characteristics
- Identify bottlenecks

## Conclusion

This implementation plan provides a clear path to adding external processor support to cdp-pipeline-workflow. Starting with static configuration and basic gRPC bridges, the system can evolve to support advanced features like service discovery and dynamic scaling while maintaining backward compatibility with existing processors.

The key to success is the bridge pattern that makes external services transparent to the existing pipeline architecture, allowing gradual migration and testing of external processors without disrupting current functionality.