# Parquet Archival Consumer Implementation Plan

## Overview

This document outlines the phased implementation plan for adding a Parquet-based archival consumer to the CDP Pipeline Workflow. The consumer will convert streaming Protocol Buffer data to columnar Parquet format for efficient long-term storage and analytics.

## Architecture Summary

The Parquet archival consumer will:
- Convert streaming blockchain data to columnar Parquet format
- Support multiple storage backends (Local FS, GCS, S3)
- Implement intelligent batching and file rotation
- Maintain compatibility with existing CDP pipeline patterns
- Provide schema evolution and compression options

## Implementation Phases

### Phase 1: Core Consumer Implementation
**Duration**: 2-3 days  
**Priority**: Critical

#### Objectives
- Create basic consumer structure implementing the Processor interface
- Set up buffering and batch processing logic
- Implement core Process() method
- Add configuration parsing and validation

#### Deliverables
1. `/consumer/consumer_save_to_parquet.go` with:
   - SaveToParquetConfig struct
   - SaveToParquet consumer struct
   - Basic Process() method with buffering
   - Configuration parsing logic
   - Placeholder storage interface

2. Basic unit tests for configuration parsing

#### Technical Requirements
- Implement processor.Processor interface
- Handle message buffering with thread safety
- Support configurable buffer sizes and rotation intervals
- Parse both map[string]interface{} and typed config

#### Dependencies
- github.com/apache/arrow/go/v17
- Existing processor package interfaces

---

### Phase 2: Storage Backend Implementation
**Duration**: 2-3 days  
**Priority**: Critical

#### Objectives
- Implement storage abstraction layer
- Create storage clients for Local FS, GCS, and S3
- Handle authentication and connection management
- Implement atomic writes and error handling

#### Deliverables
1. `/consumer/parquet_storage_clients.go` with:
   - StorageClient interface
   - LocalFSClient implementation
   - GCSClient implementation
   - S3Client implementation
   - Factory function for client creation

2. Unit tests for each storage backend

#### Technical Requirements
- Atomic file writes for local filesystem
- Proper credential handling for cloud providers
- Retry logic for transient failures
- Resource cleanup on shutdown

#### Dependencies
- cloud.google.com/go/storage
- github.com/aws/aws-sdk-go-v2
- Standard library for local FS

---

### Phase 3: Schema Registry and Arrow Integration
**Duration**: 3-4 days  
**Priority**: High

#### Objectives
- Implement Arrow schema management
- Create data type conversions from protobuf to Arrow
- Support schema evolution
- Handle multiple message types

#### Deliverables
1. `/consumer/parquet_schema_registry.go` with:
   - ParquetSchemaRegistry struct
   - Schema inference from messages
   - Predefined schemas for known types
   - Schema evolution support

2. Arrow conversion logic in main consumer:
   - Message to Arrow record batch conversion
   - Type mapping and null handling
   - Compression configuration

3. Unit tests for schema operations

#### Technical Requirements
- Support ContractDataOutput, Event, and AssetDetails types
- Handle nullable fields correctly
- Preserve type information during conversion
- Support nested and complex types

#### Dependencies
- github.com/apache/arrow/go/v17/parquet
- Existing CDP data structures

---

### Phase 4: Configuration and Pipeline Integration
**Duration**: 2 days  
**Priority**: High

#### Objectives
- Integrate consumer into main pipeline
- Create configuration examples
- Add factory registration
- Document configuration options

#### Deliverables
1. Updates to `/main.go`:
   - Add SaveToParquet case in createConsumer()
   - Import new consumer package

2. Configuration examples:
   - `/config/examples/parquet-archival.yaml`
   - Multi-consumer pipeline examples
   - Environment variable documentation

3. Integration tests with mock pipeline

#### Technical Requirements
- Maintain backward compatibility
- Support existing configuration patterns
- Handle missing dependencies gracefully
- Validate all configuration options

---

### Phase 5: Testing and Performance Optimization
**Duration**: 3-4 days  
**Priority**: High

#### Objectives
- Comprehensive testing of all components
- Performance benchmarking
- Memory usage optimization
- Build verification

#### Deliverables
1. Complete test suite:
   - Unit tests for all components
   - Integration tests with test data
   - Performance benchmarks
   - Memory leak tests

2. Performance optimizations:
   - Buffer size tuning
   - Compression benchmarks
   - Batch size optimization
   - Concurrent write testing

3. Documentation updates:
   - Performance tuning guide
   - Troubleshooting section
   - Operational procedures

#### Technical Requirements
- Test with realistic data volumes
- Verify no memory leaks
- Ensure graceful shutdown
- Test all error conditions

---

## Implementation Guidelines

### Code Organization
```
consumer/
├── consumer_save_to_parquet.go      # Main consumer implementation
├── parquet_storage_clients.go       # Storage backend implementations
├── parquet_schema_registry.go       # Schema management
└── consumer_save_to_parquet_test.go # Unit tests
```

### Error Handling Strategy
- Use structured logging for all errors
- Implement retry logic for transient failures
- Buffer failed messages for retry
- Graceful degradation on non-critical errors

### Performance Considerations
- Target: 50k-100k records/second throughput
- Memory usage: < 500MB for 50k record buffer
- File size: 128-512MB compressed Parquet files
- Compression ratio: 10:1 to 20:1

### Testing Strategy
1. **Unit Tests**: Each component in isolation
2. **Integration Tests**: Full pipeline with test data
3. **Performance Tests**: Benchmark throughput and latency
4. **Reliability Tests**: Error injection and recovery
5. **Build Tests**: Verify compilation with all dependencies

---

## Risk Mitigation

### Technical Risks
1. **Memory Usage**: Mitigate with configurable buffer limits
2. **Schema Evolution**: Use nullable fields and versioning
3. **Storage Failures**: Implement retry and dead letter queue
4. **Performance**: Profile and optimize hot paths

### Operational Risks
1. **Data Loss**: Implement write-ahead logging if needed
2. **Monitoring**: Add comprehensive metrics
3. **Debugging**: Detailed logging and dry-run mode
4. **Rollback**: Feature flag for gradual rollout

---

## Success Criteria

### Functional Requirements
- [ ] Successfully processes all message types
- [ ] Writes valid Parquet files readable by standard tools
- [ ] Supports all three storage backends
- [ ] Handles configuration changes gracefully
- [ ] Integrates with existing pipeline

### Performance Requirements
- [ ] Processes 50k+ messages/second
- [ ] Memory usage < 500MB under normal load
- [ ] File compression ratio > 10:1
- [ ] No memory leaks over 24-hour run

### Operational Requirements
- [ ] Comprehensive logging and metrics
- [ ] Graceful shutdown handling
- [ ] Configuration validation
- [ ] Documentation complete

---

## Timeline Summary

**Total Duration**: 12-16 days

1. Phase 1 (Core Implementation): Days 1-3
2. Phase 2 (Storage Backends): Days 4-6
3. Phase 3 (Schema/Arrow): Days 7-10
4. Phase 4 (Integration): Days 11-12
5. Phase 5 (Testing/Optimization): Days 13-16

**Developer Handoff**: After each phase completion

---

## Next Steps

1. Review and approve implementation plan
2. Set up development environment with Arrow dependencies
3. Create feature branch for development
4. Begin Phase 1 implementation
5. Schedule daily progress reviews

---

## Appendix: Key Design Decisions

### Why Arrow/Parquet?
- Industry standard for analytical workloads
- Excellent compression and query performance
- Wide ecosystem support (DuckDB, Spark, etc.)
- Schema evolution capabilities

### Storage Backend Choice
- Local FS: Development and testing
- GCS: Primary production storage
- S3: Alternative cloud deployment

### Partitioning Strategy
- Time-based: Optimal for most queries
- Ledger-based: For blockchain-specific analysis
- Configurable: Adapt to use case

### Buffer Management
- In-memory buffering for performance
- Configurable size limits
- Time-based rotation
- Graceful overflow handling