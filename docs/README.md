# CDP Pipeline Workflow Documentation

This directory contains comprehensive documentation for the CDP Pipeline Workflow project and its evolution to Obsrvr Lake.

## Quick Start

- [Installation Guide](installation.md) - Get started with CDP Pipeline
- [Testing Guide](testing-guide.md) - How to test pipelines
- [Lake Customer Quick Start](lake-customer-quickstart.md) - For customers using Obsrvr Lake

## Architecture Evolution

### Current Architecture (CDP Pipeline)
- [Hosted Data Service Architecture](hosted-data-service-architecture.md) - Multi-tenant database approach
- [Wallet Backend Integration](wallet-backend-integration.md) - Stellar wallet backend compatibility
- [Control Plane Integration](control-plane-integration.md) - Pipeline monitoring and management

### Future Architecture (Obsrvr Lake)
- [Obsrvr Lake + Actions Architecture](obsrvr-lake-actions-architecture.md) - Modern lakehouse with Iceberg
- [CDP to Lakehouse Migration](cdp-to-lakehouse-migration.md) - Processor migration guide
- [CDP to Iceberg Migration Guide](cdp-to-iceberg-migration-guide.md) - Technical migration details

## Stellar Integration

### Critical SDK Guidelines
- [Stellar SDK Helper Methods](stellar-go-sdk-helper-methods.md) - **MUST READ** - Avoid protocol version issues
- [Stellar SDK Quick Reference](stellar-sdk-quick-reference.md) - Quick lookup for common operations
- [Stellar Processor Migration Plan](stellar-processor-migration-plan.md) - Update processors to use SDK helpers

### Processors and Patterns
- [Stellar Token Processor Patterns](stellar-token-processor-patterns.md) - Token transfer processing
- [Contract Events Dual Representation](contract-events-dual-representation-plan.md) - Soroban event handling
- [Protocol 23 Migration Plan](protocol-23-migration-plan.md) - Unified events preparation

## CLI and Tools

### FlowCTL
- [FlowCTL Commands](flowctl-commands.md) - Command reference
- [FlowCTL CLI Implementation](flowctl-cli-implementation-plan.md) - Architecture details
- [FlowCTL Integration Summary](flowctl-integration-summary.md) - Integration overview

### Development Tools
- [Nix Setup](nix-setup.md) - Development environment setup
- [Plugin System Implementation](plugin-system-implementation-guide.md) - Extending CDP
- [External Processors gRPC](external-processors-grpc-implementation-plan.md) - Remote processors

## Consumer Documentation

### Storage Consumers
- [Parquet Archival Consumer](parquet-archival-consumer.md) - Efficient data archival
- [GCS Parquet Setup Guide](gcs-parquet-setup-guide.md) - Google Cloud Storage setup
- [Contract Data PostgreSQL Consumer](contract-data-postgresql-consumer-implementation-plan.md) - Contract storage

### Data Flow Guides
- [Parquet Consumer Data Flow](parquet-consumer-data-flow.md) - Understanding data pipelines
- [Parquet Consumer Processor Integration](parquet-consumer-processor-integration.md) - Processor compatibility

## Phase Documentation

### Phase 1
- [Phase 1 Completion Report](phase1-completion-report.md)
- [Phase 1 Parquet Consumer Handoff](phase1-parquet-consumer-handoff.md)

### Phase 2
- [Phase 2 Executive Summary](phase-2-executive-summary.md)
- [Phase 2 Developer Handoff](phase-2-developer-handoff.md)
- [Phase 2 Migration Guide](phase-2-migration-guide.md)
- [Phase 2 Implementation Summary](phase-2-implementation-summary.md)

### Additional Phases
- [Phase 3 Parquet Consumer Handoff](phase3-parquet-consumer-handoff.md)
- [Phase 4 Parquet Consumer Handoff](phase4-parquet-consumer-handoff.md)
- [Phase 5 Testing Handoff](phase5-testing-handoff.md)

## Technical Deep Dives

### Performance and Optimization
- [Time-Based Ledger Processing](time-based-ledger-processing.md) - Efficient processing strategies
- [Time-Bounded Processing Plan](time-bounded-processing-plan.md) - Resource management
- [AI Native Implementation Guide](ai-native-implementation-guide.md) - Modern patterns

### Contract Processing
- [Contract Invocation Examples](contract-invocation-examples.md) - Real-world examples
- [Contract State Indexer Implementation](contract-state-indexer-implementation-guide.md) - State tracking
- [Contract Data Schema Mapping](contract-data-schema-mapping.md) - Data structures

## Comparisons and Analysis

- [CDP vs Benthos Comparison](cdp-vs-benthos-comparison.md) - Alternative approaches
- [CDP vs FlowCTL Implementation](cdp-vs-flowctl-implementation-comparison.md) - Architecture comparison
- [Effects Processor Comparison](effects-processor-comparison.md) - Processing strategies

## Future Roadmap

- [Future Best Practices](future-best-practices-for-cdp.md) - Recommended patterns
- [Future Inspired Enhancements](future-inspired-enhancements.md) - Planned improvements
- [FlowCTL as AI Native Control Plane](flowctl-as-ai-native-control-plane.md) - Vision document

## Example Configurations

- [Plugin Pipeline Examples](plugin-pipeline-examples.md) - Sample configurations
- [One-Liner Pipeline Implementation](one-liner-pipeline-implementation.md) - Simple setups
- [Contract Invocation Enhancement Examples](contract-invocation-examples.md) - Complex scenarios

## Reference Documentation

- [Codebase Investigation Report](codebase-investigation-report.md) - Code analysis
- [SCF Validation Guide](scf-validation-guide.md) - Validation procedures
- [PR Review Changes Summary](pr-review-changes-summary.md) - Recent updates