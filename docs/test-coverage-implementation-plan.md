# Test Coverage Implementation Plan

## Executive Summary

Current test coverage is critically low at ~5.5% (8 test files for 145 Go files). This plan outlines a systematic approach to reach 70% coverage within 8 weeks while establishing sustainable testing practices.

**Goals**:
- Week 4: 30% coverage (critical paths)
- Week 8: 70% coverage (comprehensive)
- Ongoing: Maintain 70%+ with CI enforcement

## Current State Analysis

### Coverage Breakdown
```
Component Type          Files    Tests    Coverage
Source Adapters         6        0        0%
Processors (root)       43       2        4.7%
Processors (pkg)        15       0        0%
Consumers (root)        31       1        3.2%
Consumers (pkg)         10       0        0%
Core Libraries          40       5        12.5%
TOTAL                   145      8        5.5%
```

### Risk Assessment
- **Critical Risk**: Parquet consumer (new feature, handles data archival)
- **High Risk**: Source adapters (data ingestion points)
- **High Risk**: Database consumers (data persistence)
- **Medium Risk**: Processors (data transformation)

## Implementation Strategy

### Phase 1: Critical Path Coverage (Weeks 1-2) - Target: 20%

**Week 1: Data Flow Foundations**
1. **Source Adapters** (highest priority)
   - `TestBufferedStorageSourceAdapter` - Core ingestion
   - `TestCaptiveCoreInboundAdapter` - Stellar connectivity
   - `TestRPCSourceAdapter` - RPC integration

2. **Parquet Consumer** (new feature)
   - `TestSaveToParquet` - Basic functionality
   - `TestParquetBuffering` - Buffer management
   - `TestParquetSchemaEvolution` - Schema changes
   - `TestParquetErrorRecovery` - Data integrity

**Week 2: Database Consumers**
3. **PostgreSQL Consumer**
   - `TestSaveToPostgreSQL` - Connection and writes
   - `TestPostgreSQLBatching` - Batch operations
   - `TestPostgreSQLErrorHandling` - Failure scenarios

4. **Core Processors**
   - `TestLatestLedgerProcessor` - Used in example
   - `TestFilterPaymentsProcessor` - Common use case
   - `TestContractDataProcessor` - Business logic

### Phase 2: Comprehensive Coverage (Weeks 3-4) - Target: 45%

**Week 3: Remaining Consumers**
- MongoDB, DuckDB, Redis consumers
- Messaging consumers (ZeroMQ, WebSocket)
- File-based consumers (Excel, GCS)

**Week 4: Processor Coverage**
- Payment processors
- Account processors
- Contract processors
- Transform processors

### Phase 3: Integration & Edge Cases (Weeks 5-6) - Target: 60%

**Week 5: Integration Tests**
- End-to-end pipeline tests
- Multi-processor chains
- Error propagation tests
- Performance benchmarks

**Week 6: Edge Cases**
- Malformed input handling
- Resource exhaustion scenarios
- Concurrent access patterns
- Network failure simulation

### Phase 4: Polish & Maintenance (Weeks 7-8) - Target: 70%

**Week 7: Test Infrastructure**
- Test utilities and helpers
- Mock implementations
- Test data generators
- CI/CD integration

**Week 8: Documentation & Training**
- Testing guide completion
- Team training
- Code review standards
- Maintenance procedures

## Testing Patterns

### 1. Table-Driven Tests (Preferred)
```go
func TestProcessMessage(t *testing.T) {
    tests := []struct {
        name    string
        input   processor.Message
        want    processor.Message
        wantErr bool
    }{
        {
            name:  "valid payment",
            input: processor.Message{Payload: validPayment},
            want:  processor.Message{Payload: transformedPayment},
        },
        {
            name:    "invalid amount",
            input:   processor.Message{Payload: invalidPayment},
            wantErr: true,
        },
    }
    
    for _, tt := range tests {
        t.Run(tt.name, func(t *testing.T) {
            p := NewPaymentProcessor()
            err := p.Process(context.Background(), tt.input)
            
            if (err != nil) != tt.wantErr {
                t.Errorf("Process() error = %v, wantErr %v", err, tt.wantErr)
            }
            // Additional assertions...
        })
    }
}
```

### 2. Interface Mocking
```go
// mocks/mock_storage.go
type MockStorage struct {
    mock.Mock
}

func (m *MockStorage) Save(ctx context.Context, data []byte) error {
    args := m.Called(ctx, data)
    return args.Error(0)
}

// In tests
func TestConsumerWithStorage(t *testing.T) {
    mockStorage := new(MockStorage)
    mockStorage.On("Save", mock.Anything, mock.Anything).Return(nil)
    
    consumer := NewConsumer(mockStorage)
    err := consumer.Process(ctx, testMessage)
    
    assert.NoError(t, err)
    mockStorage.AssertExpectations(t)
}
```

### 3. Integration Test Pattern
```go
func TestPipelineIntegration(t *testing.T) {
    if testing.Short() {
        t.Skip("skipping integration test")
    }
    
    // Setup test environment
    ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
    defer cancel()
    
    // Create pipeline components
    source := setupTestSource(t)
    processor := processor.NewFilterPayments(testConfig)
    consumer := setupTestConsumer(t)
    
    // Wire pipeline
    source.Subscribe(processor)
    processor.Subscribe(consumer)
    
    // Run pipeline
    go source.Run(ctx)
    
    // Verify results
    eventually(t, func() bool {
        return consumer.MessageCount() > 0
    }, 10*time.Second, "expected messages to be processed")
}
```

## Test Categories

### 1. Unit Tests (60% of tests)
- Fast, isolated, no external dependencies
- Mock all I/O operations
- Focus on business logic
- Run on every commit

### 2. Integration Tests (30% of tests)
- Test component interactions
- Use test containers for databases
- Test real protocol implementations
- Run on PR creation

### 3. End-to-End Tests (10% of tests)
- Full pipeline execution
- Real data sources (test environment)
- Performance benchmarks
- Run nightly

## Coverage Standards

### Minimum Coverage Requirements

| Component Type | Minimum Coverage | Rationale |
|---------------|-----------------|-----------|
| Source Adapters | 80% | Critical data ingestion |
| Processors | 70% | Business logic |
| Consumers | 80% | Data persistence |
| Utilities | 90% | Shared functionality |
| Config/CLI | 60% | User interaction |

### What to Test

**Must Test**:
- Happy path functionality
- Error conditions and edge cases
- Resource cleanup (Close, cancel)
- Concurrent access
- Configuration validation
- Data integrity

**Can Skip**:
- Simple getters/setters
- Generated code
- Third-party library calls (unless wrapped)
- Logging statements

## Tooling & Infrastructure

### 1. Testing Tools
```bash
# Install required tools
go install github.com/stretchr/testify
go install github.com/golang/mock/mockgen
go install gotest.tools/gotestsum

# Coverage tools
go install github.com/axw/gocov/gocov
go install github.com/AlekSi/gocov-xml
```

### 2. Makefile Targets
```makefile
# Add to Makefile
.PHONY: test
test:
	go test -v -race ./...

.PHONY: test-coverage
test-coverage:
	go test -v -race -coverprofile=coverage.out ./...
	go tool cover -html=coverage.out -o coverage.html

.PHONY: test-integration
test-integration:
	go test -v -tags=integration -timeout=30m ./...

.PHONY: test-benchmark
test-benchmark:
	go test -bench=. -benchmem ./...
```

### 3. CI Configuration
```yaml
# .github/workflows/test.yml
name: Tests
on: [push, pull_request]

jobs:
  test:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v3
      - uses: actions/setup-go@v4
        with:
          go-version: '1.21'
      
      - name: Run tests
        run: make test-coverage
      
      - name: Check coverage
        run: |
          coverage=$(go tool cover -func=coverage.out | grep total | awk '{print $3}' | sed 's/%//')
          echo "Coverage: $coverage%"
          if (( $(echo "$coverage < 70" | bc -l) )); then
            echo "Coverage is below 70%"
            exit 1
          fi
```

## Quick Start Guide

### Creating Your First Test

1. **Create test file** alongside implementation:
   ```
   processor_payment.go
   processor_payment_test.go
   ```

2. **Use the standard template**:
   ```go
   package processor_test

   import (
       "testing"
       "context"
       
       "github.com/stretchr/testify/assert"
       "github.com/stretchr/testify/require"
       
       "github.com/withObsrvr/cdp-pipeline-workflow/processor"
   )

   func TestPaymentProcessor_Process(t *testing.T) {
       // Setup
       ctx := context.Background()
       p := processor.NewPaymentProcessor(testConfig)
       
       // Execute
       err := p.Process(ctx, testMessage)
       
       // Assert
       require.NoError(t, err)
       assert.Equal(t, expected, actual)
   }
   ```

3. **Run test**:
   ```bash
   go test -v ./processor/...
   ```

## Success Metrics

### Week 4 Checkpoint
- [ ] 30% overall coverage
- [ ] 100% coverage on Parquet consumer
- [ ] 80% coverage on source adapters
- [ ] CI enforcing coverage on new code

### Week 8 Goals
- [ ] 70% overall coverage
- [ ] All critical paths tested
- [ ] Integration test suite running
- [ ] Team trained on testing practices

### Ongoing Metrics
- Coverage never drops below 70%
- New features require tests (enforced by CI)
- Test execution time < 5 minutes
- Zero flaky tests

## Common Pitfalls to Avoid

1. **Testing Implementation, Not Behavior**
   ```go
   // Bad: Tests internal implementation
   assert.Equal(t, 5, processor.bufferSize)
   
   // Good: Tests observable behavior
   assert.Equal(t, 5, len(processor.GetResults()))
   ```

2. **Ignoring Error Cases**
   ```go
   // Bad: Only happy path
   result, _ := processor.Process(msg)
   
   // Good: Test error conditions
   _, err := processor.Process(invalidMsg)
   assert.ErrorIs(t, err, ErrInvalidMessage)
   ```

3. **Not Cleaning Up Resources**
   ```go
   // Always cleanup
   defer func() {
       consumer.Close()
       os.RemoveAll(testDir)
   }()
   ```

## Team Responsibilities

### During Implementation
- **Lead Developer**: Owns test strategy and templates
- **Each Developer**: Tests their assigned components
- **Code Review**: Enforces test requirements

### Ongoing
- **New Features**: Must include tests (no exceptions)
- **Bug Fixes**: Must include regression tests
- **Refactoring**: Must maintain or improve coverage

## Budget & Resources

### Time Investment
- 2 developers × 8 weeks × 50% time = 320 hours
- Additional 80 hours for infrastructure and tooling

### Expected ROI
- 50% reduction in production bugs
- 75% faster debugging with good tests
- 90% confidence in refactoring
- Improved onboarding for new developers

## Conclusion

Improving test coverage from 5.5% to 70% is ambitious but achievable with focused effort. The key is to:

1. Start with critical paths
2. Use consistent patterns
3. Make testing easy with good tools
4. Enforce standards through CI
5. Celebrate progress

This investment will pay dividends in reliability, maintainability, and developer confidence.