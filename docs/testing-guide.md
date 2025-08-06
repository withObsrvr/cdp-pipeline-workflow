# CDP Pipeline Workflow Testing Guide

## Table of Contents
1. [Testing Philosophy](#testing-philosophy)
2. [Testing Patterns](#testing-patterns)
3. [Component Testing Guide](#component-testing-guide)
4. [Test Data Management](#test-data-management)
5. [Performance Testing](#performance-testing)
6. [Best Practices](#best-practices)
7. [Troubleshooting Tests](#troubleshooting-tests)

## Testing Philosophy

### Core Principles

1. **Test Behavior, Not Implementation**
   - Test what the component does, not how it does it
   - Focus on inputs, outputs, and side effects
   - Allow refactoring without breaking tests

2. **Fast Feedback Loop**
   - Unit tests should run in milliseconds
   - Integration tests in seconds
   - E2E tests in minutes

3. **Deterministic & Reliable**
   - No flaky tests allowed
   - Control time, randomness, and external dependencies
   - Tests must pass consistently

4. **Clear Test Names**
   ```go
   // Bad
   func TestProcess(t *testing.T)
   
   // Good
   func TestPaymentProcessor_Process_FiltersPaymentsBelowMinimumAmount(t *testing.T)
   ```

## Testing Patterns

### 1. Table-Driven Tests (Default Pattern)

Table-driven tests are the preferred pattern for most scenarios:

```go
func TestFilterPayments_Process(t *testing.T) {
    tests := []struct {
        name    string
        config  map[string]interface{}
        input   processor.Message
        want    processor.Message
        wantErr error
        wantSent bool
    }{
        {
            name: "filters payment below minimum",
            config: map[string]interface{}{
                "min_amount": "100",
            },
            input: processor.Message{
                Payload: []byte(`{"amount": "50", "type": "payment"}`),
            },
            wantSent: false,
        },
        {
            name: "passes payment above minimum",
            config: map[string]interface{}{
                "min_amount": "100",
            },
            input: processor.Message{
                Payload: []byte(`{"amount": "150", "type": "payment"}`),
            },
            want: processor.Message{
                Payload: []byte(`{"amount": "150", "type": "payment"}`),
            },
            wantSent: true,
        },
        {
            name: "handles invalid JSON",
            config: map[string]interface{}{
                "min_amount": "100",
            },
            input: processor.Message{
                Payload: []byte(`{invalid json`),
            },
            wantErr: processor.ErrInvalidJSON,
            wantSent: false,
        },
    }
    
    for _, tt := range tests {
        t.Run(tt.name, func(t *testing.T) {
            // Setup
            ctx := context.Background()
            p, err := processor.NewFilterPayments(tt.config)
            require.NoError(t, err)
            
            // Create mock subscriber
            mockSub := &MockProcessor{}
            p.Subscribe(mockSub)
            
            // Execute
            err = p.Process(ctx, tt.input)
            
            // Assert
            if tt.wantErr != nil {
                assert.ErrorIs(t, err, tt.wantErr)
            } else {
                assert.NoError(t, err)
            }
            
            if tt.wantSent {
                assert.Equal(t, 1, mockSub.CallCount())
                assert.Equal(t, tt.want, mockSub.LastMessage())
            } else {
                assert.Equal(t, 0, mockSub.CallCount())
            }
        })
    }
}
```

### 2. Mock Implementations

Create focused mocks for testing interactions:

```go
// mocks/processor.go
type MockProcessor struct {
    mu       sync.Mutex
    messages []processor.Message
    errors   []error
    delay    time.Duration
}

func (m *MockProcessor) Process(ctx context.Context, msg processor.Message) error {
    m.mu.Lock()
    defer m.mu.Unlock()
    
    // Simulate processing delay if configured
    if m.delay > 0 {
        select {
        case <-time.After(m.delay):
        case <-ctx.Done():
            return ctx.Err()
        }
    }
    
    m.messages = append(m.messages, msg)
    
    // Return configured error for this call
    if len(m.errors) > len(m.messages)-1 {
        return m.errors[len(m.messages)-1]
    }
    
    return nil
}

func (m *MockProcessor) CallCount() int {
    m.mu.Lock()
    defer m.mu.Unlock()
    return len(m.messages)
}

func (m *MockProcessor) LastMessage() processor.Message {
    m.mu.Lock()
    defer m.mu.Unlock()
    if len(m.messages) == 0 {
        return processor.Message{}
    }
    return m.messages[len(m.messages)-1]
}
```

### 3. Test Fixtures

Organize test data in a maintainable way:

```go
// testdata/fixtures.go
package testdata

import (
    "encoding/json"
    "time"
)

var (
    // Valid ledger close meta for testing
    ValidLedgerCloseMeta = createTestLedgerCloseMeta()
    
    // Valid payment operation
    ValidPayment = Payment{
        SourceAccount: "GABC...",
        Destination:   "GXYZ...",
        Amount:        "1000.0",
        Asset:         Asset{Code: "XLM"},
        Timestamp:     time.Now(),
    }
    
    // Invalid payment (missing required fields)
    InvalidPayment = Payment{
        Amount: "-100", // Invalid negative amount
    }
)

// Helper to create test data with variations
func PaymentWithAmount(amount string) Payment {
    p := ValidPayment
    p.Amount = amount
    return p
}

// Load test fixtures from files
func LoadTestData(filename string) ([]byte, error) {
    return os.ReadFile(filepath.Join("testdata", filename))
}
```

## Component Testing Guide

### Testing Source Adapters

Source adapters require special attention as they're entry points:

```go
func TestBufferedStorageSourceAdapter_Run(t *testing.T) {
    tests := []struct {
        name           string
        config         map[string]interface{}
        mockFiles      []MockFile
        expectedCount  int
        expectedError  error
    }{
        {
            name: "processes all files successfully",
            config: map[string]interface{}{
                "bucket_name": "test-bucket",
                "start_ledger": 1000,
                "end_ledger":   1002,
            },
            mockFiles: []MockFile{
                {Name: "1000.xdr", Content: validLedgerXDR},
                {Name: "1001.xdr", Content: validLedgerXDR},
                {Name: "1002.xdr", Content: validLedgerXDR},
            },
            expectedCount: 3,
        },
        {
            name: "handles missing files gracefully",
            config: map[string]interface{}{
                "bucket_name": "test-bucket",
                "start_ledger": 1000,
                "end_ledger":   1002,
            },
            mockFiles: []MockFile{
                {Name: "1000.xdr", Content: validLedgerXDR},
                // 1001 missing
                {Name: "1002.xdr", Content: validLedgerXDR},
            },
            expectedCount: 2,
        },
    }
    
    for _, tt := range tests {
        t.Run(tt.name, func(t *testing.T) {
            // Setup mock storage
            mockStorage := NewMockStorage()
            for _, file := range tt.mockFiles {
                mockStorage.AddFile(file.Name, file.Content)
            }
            
            // Create adapter with mock
            adapter := NewBufferedStorageSourceAdapter(tt.config, mockStorage)
            
            // Setup subscriber
            collector := NewMessageCollector()
            adapter.Subscribe(collector)
            
            // Run with timeout
            ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
            defer cancel()
            
            // Execute
            err := adapter.Run(ctx)
            
            // Assert
            if tt.expectedError != nil {
                assert.ErrorIs(t, err, tt.expectedError)
            } else {
                assert.NoError(t, err)
            }
            
            assert.Equal(t, tt.expectedCount, collector.Count())
        })
    }
}
```

### Testing Processors

Processors transform data and should be tested for correctness:

```go
func TestContractDataProcessor_Process(t *testing.T) {
    // Setup test cases
    tests := []struct {
        name          string
        config        map[string]interface{}
        ledgerMeta    xdr.LedgerCloseMeta
        expectedCount int
        expectedData  []ContractData
    }{
        {
            name: "extracts contract data from ledger",
            config: map[string]interface{}{
                "network_passphrase": "Test SDF Network ; September 2015",
                "include_expired": false,
            },
            ledgerMeta: createLedgerWithContractData(
                ContractDataEntry{Key: "key1", Value: "value1"},
                ContractDataEntry{Key: "key2", Value: "value2", Expired: true},
            ),
            expectedCount: 1, // Only non-expired
            expectedData: []ContractData{
                {Key: "key1", Value: "value1"},
            },
        },
    }
    
    for _, tt := range tests {
        t.Run(tt.name, func(t *testing.T) {
            // Create processor
            p, err := processor.NewContractData(tt.config)
            require.NoError(t, err)
            
            // Create collector
            collector := NewContractDataCollector()
            p.Subscribe(collector)
            
            // Process
            err = p.Process(context.Background(), processor.Message{
                Payload: tt.ledgerMeta,
            })
            
            // Assert
            require.NoError(t, err)
            assert.Equal(t, tt.expectedCount, collector.Count())
            assert.Equal(t, tt.expectedData, collector.Data())
        })
    }
}
```

### Testing Consumers

Consumers need careful testing of I/O operations:

```go
func TestSaveToParquet_Process(t *testing.T) {
    tests := []struct {
        name          string
        config        map[string]interface{}
        messages      []processor.Message
        expectedFiles []string
        validateFunc  func(t *testing.T, files map[string][]byte)
    }{
        {
            name: "buffers and writes on threshold",
            config: map[string]interface{}{
                "path": "/tmp/test",
                "buffer_size": 2,
                "compression": "snappy",
            },
            messages: []processor.Message{
                {Payload: []byte(`{"id": 1, "amount": 100}`)},
                {Payload: []byte(`{"id": 2, "amount": 200}`)},
                {Payload: []byte(`{"id": 3, "amount": 300}`)},
            },
            expectedFiles: []string{
                "data_20240105_150405_0.parquet", // First 2 records
                "data_20240105_150406_0.parquet", // Third record
            },
            validateFunc: func(t *testing.T, files map[string][]byte) {
                // Validate first file has 2 records
                records := readParquetFile(t, files["data_20240105_150405_0.parquet"])
                assert.Len(t, records, 2)
                assert.Equal(t, int64(1), records[0]["id"])
                assert.Equal(t, int64(2), records[1]["id"])
            },
        },
    }
    
    for _, tt := range tests {
        t.Run(tt.name, func(t *testing.T) {
            // Mock file system
            mockFS := NewMockFileSystem()
            
            // Create consumer with mock
            consumer := NewSaveToParquet(tt.config, mockFS)
            
            // Process messages
            ctx := context.Background()
            for _, msg := range tt.messages {
                err := consumer.Process(ctx, msg)
                require.NoError(t, err)
            }
            
            // Close to flush
            err := consumer.Close()
            require.NoError(t, err)
            
            // Validate files
            files := mockFS.GetFiles()
            assert.Len(t, files, len(tt.expectedFiles))
            
            if tt.validateFunc != nil {
                tt.validateFunc(t, files)
            }
        })
    }
}
```

### Integration Testing

Test complete pipelines end-to-end:

```go
func TestPaymentProcessingPipeline(t *testing.T) {
    if testing.Short() {
        t.Skip("skipping integration test")
    }
    
    // Setup test database
    db := setupTestPostgres(t)
    defer db.Close()
    
    // Create pipeline components
    source := createTestStellarSource(t, "testnet")
    filter := processor.NewFilterPayments(map[string]interface{}{
        "min_amount": "100",
    })
    consumer := consumer.NewSaveToPostgreSQL(map[string]interface{}{
        "connection_string": db.ConnectionString(),
        "table": "payments",
    })
    
    // Wire pipeline
    source.Subscribe(filter)
    filter.Subscribe(consumer)
    
    // Run pipeline
    ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
    defer cancel()
    
    errCh := make(chan error, 1)
    go func() {
        errCh <- source.Run(ctx)
    }()
    
    // Wait for some data to be processed
    eventually(t, func() bool {
        count := getRowCount(t, db, "payments")
        return count >= 10
    }, 20*time.Second, "expected at least 10 payments")
    
    // Verify data integrity
    rows := queryPayments(t, db)
    for _, row := range rows {
        assert.True(t, row.Amount >= 100, "payment amount should be >= 100")
    }
    
    // Shutdown gracefully
    cancel()
    select {
    case err := <-errCh:
        assert.ErrorIs(t, err, context.Canceled)
    case <-time.After(5 * time.Second):
        t.Fatal("source did not shut down in time")
    }
}
```

## Test Data Management

### 1. Test Data Generators

Create realistic test data programmatically:

```go
// testdata/generators.go
package testdata

type LedgerGenerator struct {
    startSeq uint32
    network  string
}

func (g *LedgerGenerator) Next() xdr.LedgerCloseMeta {
    ledger := xdr.LedgerCloseMeta{
        V: 1,
        V1: &xdr.LedgerCloseMetaV1{
            LedgerHeader: xdr.LedgerHeaderHistoryEntry{
                Header: xdr.LedgerHeader{
                    LedgerSeq: xdr.Uint32(g.startSeq),
                    // ... other fields
                },
            },
        },
    }
    g.startSeq++
    return ledger
}

// Generate test transactions
func GeneratePaymentTransaction(from, to string, amount int64) xdr.TransactionEnvelope {
    // Implementation...
}

// Generate contract data
func GenerateContractData(count int) []xdr.ContractDataEntry {
    entries := make([]xdr.ContractDataEntry, count)
    for i := range entries {
        entries[i] = xdr.ContractDataEntry{
            Key: xdr.ScVal{/* ... */},
            Val: xdr.ScVal{/* ... */},
        }
    }
    return entries
}
```

### 2. Golden Files

Use golden files for complex test data:

```go
func TestComplexTransformation(t *testing.T) {
    // Load input from golden file
    input := loadGoldenFile(t, "complex_ledger_input.json")
    
    // Process
    processor := NewComplexProcessor()
    output, err := processor.Transform(input)
    require.NoError(t, err)
    
    // Compare with expected output
    if *updateGolden {
        saveGoldenFile(t, "complex_ledger_output.json", output)
    }
    
    expected := loadGoldenFile(t, "complex_ledger_output.json")
    assert.JSONEq(t, expected, output)
}

// Run with: go test -update-golden to update golden files
var updateGolden = flag.Bool("update-golden", false, "update golden files")
```

### 3. Test Containers

Use test containers for external dependencies:

```go
func setupTestPostgres(t *testing.T) *PostgresContainer {
    ctx := context.Background()
    
    req := testcontainers.ContainerRequest{
        Image:        "postgres:15",
        ExposedPorts: []string{"5432/tcp"},
        Env: map[string]string{
            "POSTGRES_PASSWORD": "test",
            "POSTGRES_DB":       "testdb",
        },
        WaitingFor: wait.ForSQL("5432/tcp", "postgres", func(port nat.Port) string {
            return fmt.Sprintf("postgres://postgres:test@localhost:%s/testdb?sslmode=disable", port.Port())
        }),
    }
    
    container, err := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{
        ContainerRequest: req,
        Started:          true,
    })
    require.NoError(t, err)
    
    t.Cleanup(func() {
        container.Terminate(ctx)
    })
    
    return &PostgresContainer{container: container}
}
```

## Performance Testing

### Benchmark Tests

Include benchmarks for performance-critical paths:

```go
func BenchmarkParquetWriter(b *testing.B) {
    // Setup
    writer := setupBenchmarkWriter(b)
    data := generateBenchmarkData(1000)
    
    b.ResetTimer()
    b.ReportAllocs()
    
    for i := 0; i < b.N; i++ {
        err := writer.Write(data)
        if err != nil {
            b.Fatal(err)
        }
    }
    
    b.StopTimer()
    
    // Report custom metrics
    b.ReportMetric(float64(len(data)*b.N), "records/op")
    b.ReportMetric(float64(len(data)*b.N)/b.Elapsed().Seconds(), "records/sec")
}

// Benchmark with different sizes
func BenchmarkProcessorThroughput(b *testing.B) {
    sizes := []int{1, 10, 100, 1000}
    
    for _, size := range sizes {
        b.Run(fmt.Sprintf("size-%d", size), func(b *testing.B) {
            processor := NewProcessor()
            messages := generateMessages(size)
            
            b.ResetTimer()
            for i := 0; i < b.N; i++ {
                for _, msg := range messages {
                    processor.Process(context.Background(), msg)
                }
            }
        })
    }
}
```

### Load Testing

Test system behavior under load:

```go
func TestHighLoadScenario(t *testing.T) {
    if testing.Short() {
        t.Skip("skipping load test")
    }
    
    processor := NewProcessor()
    
    // Metrics collection
    var (
        processed  int64
        errors     int64
        maxLatency int64
    )
    
    // Generate load
    const (
        workers    = 100
        duration   = 30 * time.Second
        targetRate = 10000 // messages per second
    )
    
    ctx, cancel := context.WithTimeout(context.Background(), duration)
    defer cancel()
    
    var wg sync.WaitGroup
    for i := 0; i < workers; i++ {
        wg.Add(1)
        go func() {
            defer wg.Done()
            
            ticker := time.NewTicker(time.Second / time.Duration(targetRate/workers))
            defer ticker.Stop()
            
            for {
                select {
                case <-ctx.Done():
                    return
                case <-ticker.C:
                    start := time.Now()
                    err := processor.Process(ctx, generateMessage())
                    latency := time.Since(start).Nanoseconds()
                    
                    if err != nil {
                        atomic.AddInt64(&errors, 1)
                    } else {
                        atomic.AddInt64(&processed, 1)
                    }
                    
                    // Update max latency
                    for {
                        current := atomic.LoadInt64(&maxLatency)
                        if latency <= current || atomic.CompareAndSwapInt64(&maxLatency, current, latency) {
                            break
                        }
                    }
                }
            }
        }()
    }
    
    wg.Wait()
    
    // Assert performance requirements
    errorRate := float64(errors) / float64(processed+errors)
    assert.Less(t, errorRate, 0.01, "error rate should be less than 1%")
    
    throughput := float64(processed) / duration.Seconds()
    assert.Greater(t, throughput, targetRate*0.95, "should achieve 95% of target throughput")
    
    assert.Less(t, time.Duration(maxLatency), 100*time.Millisecond, "max latency should be under 100ms")
}
```

## Best Practices

### 1. Test Organization

```
processor/
├── payment_processor.go
├── payment_processor_test.go
├── testdata/
│   ├── valid_payment.json
│   ├── invalid_payment.json
│   └── fixtures.go
└── benchmarks_test.go
```

### 2. Test Naming Convention

```go
// Format: Test{Type}_{Method}_{Scenario}
func TestPaymentProcessor_Process_ValidPaymentAboveMinimum(t *testing.T)
func TestPaymentProcessor_Process_InvalidJSONInput(t *testing.T)
func TestPaymentProcessor_Subscribe_MultipleSubscribers(t *testing.T)
```

### 3. Error Testing

Always test error conditions:

```go
func TestErrorConditions(t *testing.T) {
    tests := []struct {
        name      string
        setupFunc func() error
        wantErr   error
    }{
        {
            name: "connection timeout",
            setupFunc: func() error {
                return connectWithTimeout(1 * time.Nanosecond)
            },
            wantErr: ErrTimeout,
        },
        {
            name: "invalid credentials",
            setupFunc: func() error {
                return connectWithCredentials("invalid", "invalid")
            },
            wantErr: ErrAuthentication,
        },
    }
    
    for _, tt := range tests {
        t.Run(tt.name, func(t *testing.T) {
            err := tt.setupFunc()
            assert.ErrorIs(t, err, tt.wantErr)
        })
    }
}
```

### 4. Resource Cleanup

Always clean up resources:

```go
func TestWithResources(t *testing.T) {
    // Setup
    tmpDir := t.TempDir() // Automatically cleaned up
    
    conn := setupConnection(t)
    t.Cleanup(func() {
        conn.Close()
    })
    
    ctx, cancel := context.WithCancel(context.Background())
    t.Cleanup(cancel)
    
    // Test code...
}
```

### 5. Parallel Testing

Use parallel tests where appropriate:

```go
func TestParallelProcessing(t *testing.T) {
    t.Parallel() // Mark test as parallel
    
    tests := []struct{
        name string
        // ...
    }{
        // test cases
    }
    
    for _, tt := range tests {
        tt := tt // Capture range variable
        t.Run(tt.name, func(t *testing.T) {
            t.Parallel() // Mark subtest as parallel
            // Test code...
        })
    }
}
```

## Troubleshooting Tests

### Common Issues and Solutions

1. **Flaky Tests**
   ```go
   // Problem: Time-dependent test
   assert.Equal(t, time.Now(), record.Timestamp)
   
   // Solution: Use time injection
   now := time.Now()
   processor := NewProcessor(WithClock(fakeClock{now}))
   assert.Equal(t, now, record.Timestamp)
   ```

2. **Resource Leaks**
   ```go
   // Use go leak detection
   func TestNoGoroutineLeaks(t *testing.T) {
       defer goleak.VerifyNone(t)
       // Test code that creates goroutines
   }
   ```

3. **Race Conditions**
   ```bash
   # Always run tests with race detector in CI
   go test -race ./...
   ```

4. **Test Isolation**
   ```go
   // Problem: Tests affect each other
   var globalState = make(map[string]string)
   
   // Solution: Reset state in each test
   func TestWithState(t *testing.T) {
       oldState := globalState
       globalState = make(map[string]string)
       t.Cleanup(func() {
           globalState = oldState
       })
       // Test code...
   }
   ```

### Debugging Failed Tests

1. **Verbose Output**
   ```bash
   go test -v -run TestFailingTest ./package
   ```

2. **Debug Logging**
   ```go
   func TestWithDebugLogs(t *testing.T) {
       if testing.Verbose() {
           log.SetLevel(log.DebugLevel)
       }
       // Test code with debug logs
   }
   ```

3. **Test Artifacts**
   ```go
   func TestSaveArtifactsOnFailure(t *testing.T) {
       tmpDir := t.TempDir()
       
       // On failure, print location of artifacts
       t.Cleanup(func() {
           if t.Failed() {
               t.Logf("Test artifacts saved to: %s", tmpDir)
               // Don't delete tmpDir on failure
           }
       })
   }
   ```

## Testing Checklist

Before submitting code:

- [ ] Unit tests for all new code
- [ ] Integration tests for new features
- [ ] All tests pass locally
- [ ] Tests run with `-race` flag
- [ ] No deprecated testing patterns used
- [ ] Test coverage >= 70% for new code
- [ ] Benchmarks for performance-critical code
- [ ] No test files in production builds
- [ ] Clear test documentation

## Resources

- [Go Testing Package](https://pkg.go.dev/testing)
- [Testify Assertion Library](https://github.com/stretchr/testify)
- [Go Mock Framework](https://github.com/golang/mock)
- [Test Containers Go](https://golang.testcontainers.org/)
- [Go Test Patterns](https://github.com/golang/go/wiki/TestComments)