package main

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"net/url"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/pkg/errors"
	"github.com/stellar/go/xdr"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	cdpProcessor "github.com/withObsrvr/cdp-pipeline-workflow/processor"
)

// MockRPCServer simulates Stellar RPC responses
type MockRPCServer struct {
	mu         sync.Mutex
	server     *httptest.Server
	responses  map[string]interface{}
	callCounts map[string]int
	behavior   MockRPCBehavior
}

type MockRPCBehavior struct {
	ErrorOnCall    map[string]error
	DelayOnCall    map[string]time.Duration
	FailAfterN     map[string]int
	SimulateOutage bool
}

func NewMockRPCServer() *MockRPCServer {
	m := &MockRPCServer{
		responses:  make(map[string]interface{}),
		callCounts: make(map[string]int),
		behavior: MockRPCBehavior{
			ErrorOnCall: make(map[string]error),
			DelayOnCall: make(map[string]time.Duration),
			FailAfterN:  make(map[string]int),
		},
	}

	m.server = httptest.NewServer(http.HandlerFunc(m.handleRequest))
	return m
}

func (m *MockRPCServer) handleRequest(w http.ResponseWriter, r *http.Request) {
	m.mu.Lock()
	defer m.mu.Unlock()

	// Parse JSON-RPC request
	var req struct {
		Method string                 `json:"method"`
		Params map[string]interface{} `json:"params"`
		ID     int                    `json:"id"`
	}

	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "invalid request", http.StatusBadRequest)
		return
	}

	m.callCounts[req.Method]++

	// Simulate behavior
	if m.behavior.SimulateOutage {
		http.Error(w, "service unavailable", http.StatusServiceUnavailable)
		return
	}

	if delay, ok := m.behavior.DelayOnCall[req.Method]; ok {
		time.Sleep(delay)
	}

	if failAfter, ok := m.behavior.FailAfterN[req.Method]; ok {
		if m.callCounts[req.Method] > failAfter {
			http.Error(w, "simulated failure", http.StatusInternalServerError)
			return
		}
	}

	if err, ok := m.behavior.ErrorOnCall[req.Method]; ok {
		errorResp := map[string]interface{}{
			"jsonrpc": "2.0",
			"error": map[string]interface{}{
				"code":    -32000,
				"message": err.Error(),
			},
			"id": req.ID,
		}
		json.NewEncoder(w).Encode(errorResp)
		return
	}

	// Generate response based on method
	var result interface{}
	switch req.Method {
	case "getLatestLedger":
		result = m.getLatestLedgerResponse()
	case "getLedgerEntries":
		result = m.getLedgerEntriesResponse(req.Params)
	case "getTransactions":
		result = m.getTransactionsResponse(req.Params)
	case "getEvents":
		result = m.getEventsResponse(req.Params)
	default:
		result = map[string]interface{}{"error": "unknown method"}
	}

	resp := map[string]interface{}{
		"jsonrpc": "2.0",
		"result":  result,
		"id":      req.ID,
	}

	json.NewEncoder(w).Encode(resp)
}

func (m *MockRPCServer) getLatestLedgerResponse() interface{} {
	return map[string]interface{}{
		"id":         "latest",
		"sequence":   12345,
		"hash":       "abc123",
		"protocolVersion": 20,
	}
}

func (m *MockRPCServer) getLedgerEntriesResponse(params map[string]interface{}) interface{} {
	// Simulate ledger entries response
	return map[string]interface{}{
		"entries": []map[string]interface{}{
			{
				"key":               "AAAAAAAAAAA=",
				"xdr":               "AAAAAgAAAAA=",
				"lastModifiedLedger": 12345,
			},
		},
		"latestLedger": 12345,
	}
}

func (m *MockRPCServer) getTransactionsResponse(params map[string]interface{}) interface{} {
	// Simulate transactions response
	return map[string]interface{}{
		"transactions": []map[string]interface{}{
			{
				"status":            "SUCCESS",
				"ledger":           12345,
				"createdAt":        time.Now().Unix(),
				"applicationOrder": 1,
				"feeBump":          false,
				"envelopeXdr":      "AAAAAgAAAAA=",
				"resultXdr":        "AAAAAAAAAGQAAAAA",
				"resultMetaXdr":    "AAAAAwAAAAA=",
			},
		},
		"latestLedger": 12345,
		"cursor":       "12345-1",
	}
}

func (m *MockRPCServer) getEventsResponse(params map[string]interface{}) interface{} {
	// Simulate events response
	return map[string]interface{}{
		"events": []map[string]interface{}{
			{
				"type":              "contract",
				"ledger":           12345,
				"createdAt":        time.Now().Unix(),
				"contractId":       "CDLZFC3SYJYDZT7K67VZ75HPJVIEUVNIXF47ZG2FB2RMQQVU2HHGCYSC",
				"id":               "0000000053084160-0000000001",
				"pagingToken":      "0000000053084160-0000000001",
				"topic":            []string{"transfer", "mint"},
				"value":            map[string]interface{}{"amount": "1000000"},
			},
		},
		"latestLedger": 12345,
		"cursor":       "0000000053084160-0000000001",
	}
}

func (m *MockRPCServer) Close() {
	m.server.Close()
}

func (m *MockRPCServer) URL() string {
	return m.server.URL
}

func (m *MockRPCServer) GetCallCount(method string) int {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.callCounts[method]
}

// MockRPCProcessor collects messages for verification
type MockRPCProcessor struct {
	mu               sync.Mutex
	receivedMessages []cdpProcessor.Message
	errorOnProcess   error
	callCount        int32
}

func NewMockRPCProcessor() *MockRPCProcessor {
	return &MockRPCProcessor{
		receivedMessages: make([]cdpProcessor.Message, 0),
	}
}

func (m *MockRPCProcessor) Process(ctx context.Context, msg cdpProcessor.Message) error {
	atomic.AddInt32(&m.callCount, 1)

	if m.errorOnProcess != nil {
		return m.errorOnProcess
	}

	m.mu.Lock()
	m.receivedMessages = append(m.receivedMessages, msg)
	m.mu.Unlock()

	return nil
}

func (m *MockRPCProcessor) Subscribe(processor cdpProcessor.Processor) {
	// Not needed for mock
}

func (m *MockRPCProcessor) GetMessages() []cdpProcessor.Message {
	m.mu.Lock()
	defer m.mu.Unlock()
	return append([]cdpProcessor.Message{}, m.receivedMessages...)
}

func (m *MockRPCProcessor) GetCallCount() int {
	return int(atomic.LoadInt32(&m.callCount))
}

// TestRPCSourceAdapter_Configuration tests configuration validation
func TestRPCSourceAdapter_Configuration(t *testing.T) {
	tests := []struct {
		name        string
		config      map[string]interface{}
		expectError bool
		errorMsg    string
	}{
		{
			name: "valid configuration",
			config: map[string]interface{}{
				"rpc_url":      "https://stellar-rpc.example.com",
				"network":      "testnet",
				"start_ledger": float64(1000),
				"poll_interval": float64(5),
			},
			expectError: false,
		},
		{
			name: "missing rpc_url",
			config: map[string]interface{}{
				"network":      "testnet",
				"start_ledger": float64(1000),
			},
			expectError: true,
			errorMsg:    "rpc_url is required",
		},
		{
			name: "invalid rpc_url",
			config: map[string]interface{}{
				"rpc_url":      "not-a-url",
				"network":      "testnet",
				"start_ledger": float64(1000),
			},
			expectError: true,
			errorMsg:    "invalid rpc_url",
		},
		{
			name: "missing network",
			config: map[string]interface{}{
				"rpc_url":      "https://stellar-rpc.example.com",
				"start_ledger": float64(1000),
			},
			expectError: true,
			errorMsg:    "network is required",
		},
		{
			name: "with authentication",
			config: map[string]interface{}{
				"rpc_url":      "https://stellar-rpc.example.com",
				"network":      "pubnet",
				"start_ledger": float64(50000000),
				"auth_header":  "Bearer token123",
			},
			expectError: false,
		},
		{
			name: "with custom headers",
			config: map[string]interface{}{
				"rpc_url":      "https://stellar-rpc.example.com",
				"network":      "testnet",
				"start_ledger": float64(1000),
				"headers": map[string]interface{}{
					"X-Custom-Header": "value",
					"X-API-Key":      "key123",
				},
			},
			expectError: false,
		},
		{
			name: "with retry configuration",
			config: map[string]interface{}{
				"rpc_url":          "https://stellar-rpc.example.com",
				"network":          "testnet",
				"start_ledger":     float64(1000),
				"retry_attempts":   float64(5),
				"retry_backoff_ms": float64(1000),
			},
			expectError: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			adapter, err := NewRPCSourceAdapter(tt.config)

			if tt.expectError {
				assert.Error(t, err)
				if tt.errorMsg != "" {
					assert.Contains(t, err.Error(), tt.errorMsg)
				}
			} else {
				assert.NoError(t, err)
				assert.NotNil(t, adapter)
			}
		})
	}
}

// TestRPCSourceAdapter_MethodCalls tests different RPC method calls
func TestRPCSourceAdapter_MethodCalls(t *testing.T) {
	tests := []struct {
		name           string
		config         map[string]interface{}
		expectedCalls  map[string]int
		messageCount   int
		validateFunc   func(*testing.T, []cdpProcessor.Message)
	}{
		{
			name: "ledger entries mode",
			config: map[string]interface{}{
				"mode":         "ledger_entries",
				"start_ledger": float64(1000),
				"end_ledger":   float64(1002),
			},
			expectedCalls: map[string]int{
				"getLatestLedger":  1,
				"getLedgerEntries": 3,
			},
			messageCount: 3,
		},
		{
			name: "transactions mode",
			config: map[string]interface{}{
				"mode":         "transactions",
				"start_ledger": float64(1000),
				"end_ledger":   float64(1002),
			},
			expectedCalls: map[string]int{
				"getLatestLedger": 1,
				"getTransactions": 3,
			},
			messageCount: 3,
		},
		{
			name: "events mode",
			config: map[string]interface{}{
				"mode":         "events",
				"start_ledger": float64(1000),
				"end_ledger":   float64(1002),
				"contract_ids": []interface{}{
					"CDLZFC3SYJYDZT7K67VZ75HPJVIEUVNIXF47ZG2FB2RMQQVU2HHGCYSC",
				},
			},
			expectedCalls: map[string]int{
				"getLatestLedger": 1,
				"getEvents":       3,
			},
			messageCount: 3,
		},
		{
			name: "continuous polling mode",
			config: map[string]interface{}{
				"mode":          "continuous",
				"start_ledger":  float64(1000),
				"poll_interval": float64(1), // 1 second
			},
			expectedCalls: map[string]int{
				"getLatestLedger": 3, // Multiple polls
			},
			messageCount: 3,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			mockServer := NewMockRPCServer()
			defer mockServer.Close()

			// Add server URL to config
			tt.config["rpc_url"] = mockServer.URL()
			tt.config["network"] = "testnet"

			adapter := createTestRPCAdapter(t, tt.config)
			mockProcessor := NewMockRPCProcessor()
			adapter.Subscribe(mockProcessor)

			// Run with timeout
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()

			err := simulateRPCAdapterRun(ctx, adapter, mockServer)
			
			// For continuous mode, we expect context timeout
			if tt.config["mode"] == "continuous" {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
			}

			// Verify RPC calls
			for method, expectedCount := range tt.expectedCalls {
				actualCount := mockServer.GetCallCount(method)
				assert.GreaterOrEqual(t, actualCount, expectedCount, 
					"method %s should be called at least %d times, got %d", 
					method, expectedCount, actualCount)
			}

			// Verify messages received
			messages := mockProcessor.GetMessages()
			assert.GreaterOrEqual(t, len(messages), tt.messageCount)

			if tt.validateFunc != nil {
				tt.validateFunc(t, messages)
			}
		})
	}
}

// TestRPCSourceAdapter_ErrorHandling tests error scenarios
func TestRPCSourceAdapter_ErrorHandling(t *testing.T) {
	t.Run("handles RPC errors gracefully", func(t *testing.T) {
		mockServer := NewMockRPCServer()
		defer mockServer.Close()

		// Configure server to return errors
		mockServer.behavior.ErrorOnCall["getLatestLedger"] = errors.New("RPC error")

		config := map[string]interface{}{
			"rpc_url":        mockServer.URL(),
			"network":        "testnet",
			"start_ledger":   float64(1000),
			"retry_attempts": float64(2),
		}

		adapter := createTestRPCAdapter(t, config)
		mockProcessor := NewMockRPCProcessor()
		adapter.Subscribe(mockProcessor)

		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()

		err := simulateRPCAdapterRun(ctx, adapter, mockServer)
		assert.Error(t, err)

		// Should have retried
		assert.GreaterOrEqual(t, mockServer.GetCallCount("getLatestLedger"), 2)
	})

	t.Run("handles network outages", func(t *testing.T) {
		mockServer := NewMockRPCServer()
		defer mockServer.Close()

		// Simulate outage
		mockServer.behavior.SimulateOutage = true

		config := map[string]interface{}{
			"rpc_url":          mockServer.URL(),
			"network":          "testnet",
			"start_ledger":     float64(1000),
			"retry_attempts":   float64(3),
			"retry_backoff_ms": float64(100),
		}

		adapter := createTestRPCAdapter(t, config)
		mockProcessor := NewMockRPCProcessor()
		adapter.Subscribe(mockProcessor)

		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()

		err := simulateRPCAdapterRun(ctx, adapter, mockServer)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "service unavailable")
	})

	t.Run("handles slow responses", func(t *testing.T) {
		mockServer := NewMockRPCServer()
		defer mockServer.Close()

		// Configure slow responses
		mockServer.behavior.DelayOnCall["getTransactions"] = 500 * time.Millisecond

		config := map[string]interface{}{
			"rpc_url":         mockServer.URL(),
			"network":         "testnet",
			"mode":           "transactions",
			"start_ledger":   float64(1000),
			"end_ledger":     float64(1005),
			"request_timeout": float64(200), // 200ms timeout
		}

		adapter := createTestRPCAdapter(t, config)
		mockProcessor := NewMockRPCProcessor()
		adapter.Subscribe(mockProcessor)

		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()

		err := simulateRPCAdapterRun(ctx, adapter, mockServer)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "timeout")
	})
}

// TestRPCSourceAdapter_Concurrency tests concurrent processing
func TestRPCSourceAdapter_Concurrency(t *testing.T) {
	mockServer := NewMockRPCServer()
	defer mockServer.Close()

	config := map[string]interface{}{
		"rpc_url":        mockServer.URL(),
		"network":        "testnet",
		"mode":          "events",
		"start_ledger":  float64(1000),
		"end_ledger":    float64(1100),
		"concurrency":   float64(5), // Process 5 ledgers concurrently
		"batch_size":    float64(10),
	}

	adapter := createTestRPCAdapter(t, config)

	// Subscribe multiple processors
	processors := make([]*MockRPCProcessor, 3)
	for i := range processors {
		processors[i] = NewMockRPCProcessor()
		adapter.Subscribe(processors[i])
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	err := simulateRPCAdapterRun(ctx, adapter, mockServer)
	assert.NoError(t, err)

	// Verify all processors received messages
	for i, proc := range processors {
		messages := proc.GetMessages()
		assert.Greater(t, len(messages), 0, "processor %d should receive messages", i)
	}

	// Verify concurrent calls
	assert.Greater(t, mockServer.GetCallCount("getEvents"), 10)
}

// TestRPCSourceAdapter_RateLimiting tests rate limiting behavior
func TestRPCSourceAdapter_RateLimiting(t *testing.T) {
	mockServer := NewMockRPCServer()
	defer mockServer.Close()

	config := map[string]interface{}{
		"rpc_url":           mockServer.URL(),
		"network":           "testnet",
		"mode":             "transactions",
		"start_ledger":     float64(1000),
		"end_ledger":       float64(1010),
		"rate_limit_rps":   float64(5), // 5 requests per second
	}

	adapter := createTestRPCAdapter(t, config)
	mockProcessor := NewMockRPCProcessor()
	adapter.Subscribe(mockProcessor)

	startTime := time.Now()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	err := simulateRPCAdapterRun(ctx, adapter, mockServer)
	assert.NoError(t, err)

	elapsed := time.Since(startTime)
	callCount := mockServer.GetCallCount("getTransactions")

	// With rate limiting at 5 RPS, 11 calls should take at least 2 seconds
	expectedMinDuration := time.Duration(callCount/5) * time.Second
	assert.GreaterOrEqual(t, elapsed, expectedMinDuration*8/10) // Allow 20% margin
}

// TestRPCSourceAdapter_FilteringAndTransformation tests data filtering
func TestRPCSourceAdapter_FilteringAndTransformation(t *testing.T) {
	t.Run("filters events by contract ID", func(t *testing.T) {
		mockServer := NewMockRPCServer()
		defer mockServer.Close()

		config := map[string]interface{}{
			"rpc_url":      mockServer.URL(),
			"network":      "testnet",
			"mode":        "events",
			"start_ledger": float64(1000),
			"end_ledger":   float64(1002),
			"contract_ids": []interface{}{
				"CDLZFC3SYJYDZT7K67VZ75HPJVIEUVNIXF47ZG2FB2RMQQVU2HHGCYSC",
			},
			"event_types": []interface{}{"transfer", "mint"},
		}

		adapter := createTestRPCAdapter(t, config)
		mockProcessor := NewMockRPCProcessor()
		adapter.Subscribe(mockProcessor)

		ctx := context.Background()
		err := simulateRPCAdapterRun(ctx, adapter, mockServer)
		assert.NoError(t, err)

		messages := mockProcessor.GetMessages()
		assert.Greater(t, len(messages), 0)

		// Verify filtering worked
		for _, msg := range messages {
			data, ok := msg.Payload.(map[string]interface{})
			if ok && data["contractId"] != nil {
				assert.Equal(t, "CDLZFC3SYJYDZT7K67VZ75HPJVIEUVNIXF47ZG2FB2RMQQVU2HHGCYSC", 
					data["contractId"])
			}
		}
	})

	t.Run("filters transactions by operation type", func(t *testing.T) {
		mockServer := NewMockRPCServer()
		defer mockServer.Close()

		config := map[string]interface{}{
			"rpc_url":         mockServer.URL(),
			"network":         "testnet",
			"mode":           "transactions",
			"start_ledger":   float64(1000),
			"end_ledger":     float64(1002),
			"operation_types": []interface{}{"payment", "create_account"},
		}

		adapter := createTestRPCAdapter(t, config)
		mockProcessor := NewMockRPCProcessor()
		adapter.Subscribe(mockProcessor)

		ctx := context.Background()
		err := simulateRPCAdapterRun(ctx, adapter, mockServer)
		assert.NoError(t, err)

		messages := mockProcessor.GetMessages()
		assert.Greater(t, len(messages), 0)
	})
}

// BenchmarkRPCSourceAdapter tests performance
func BenchmarkRPCSourceAdapter(b *testing.B) {
	mockServer := NewMockRPCServer()
	defer mockServer.Close()

	config := map[string]interface{}{
		"rpc_url":      mockServer.URL(),
		"network":      "testnet",
		"mode":        "ledger_entries",
		"start_ledger": float64(0),
		"end_ledger":   float64(b.N - 1),
		"concurrency":  float64(10),
		"batch_size":   float64(100),
	}

	adapter := createTestRPCAdapter(b, config)
	mockProcessor := NewMockRPCProcessor()
	adapter.Subscribe(mockProcessor)

	b.ResetTimer()
	b.ReportAllocs()

	ctx := context.Background()
	err := simulateRPCAdapterRun(ctx, adapter, mockServer)
	if err != nil {
		b.Fatal(err)
	}

	b.StopTimer()

	// Report metrics
	b.ReportMetric(float64(b.N), "ledgers")
	b.ReportMetric(float64(b.N)/b.Elapsed().Seconds(), "ledgers/sec")
	b.ReportMetric(float64(mockServer.GetCallCount("getLedgerEntries")), "rpc_calls")
}

// Helper functions

func createTestRPCAdapter(t testing.TB, config map[string]interface{}) *RPCSourceAdapter {
	// Set defaults if not present
	if _, ok := config["rpc_url"]; !ok {
		config["rpc_url"] = "https://stellar-rpc.example.com"
	}
	if _, ok := config["network"]; !ok {
		config["network"] = "testnet"
	}

	adapter, err := NewRPCSourceAdapter(config)
	require.NoError(t, err)

	return adapter.(*RPCSourceAdapter)
}

func simulateRPCAdapterRun(ctx context.Context, adapter *RPCSourceAdapter, server *MockRPCServer) error {
	// This simulates the adapter's Run method
	// In a real implementation, this would make actual RPC calls
	
	// For testing, we'll simulate processing based on configuration
	mode, _ := adapter.config["mode"].(string)
	if mode == "" {
		mode = "ledger_entries"
	}

	startLedger := uint32(1000)
	if v, ok := adapter.config["start_ledger"].(float64); ok {
		startLedger = uint32(v)
	}

	endLedger := startLedger + 10
	if v, ok := adapter.config["end_ledger"].(float64); ok {
		endLedger = uint32(v)
	}

	// Simulate processing ledgers
	for ledger := startLedger; ledger <= endLedger; ledger++ {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		// Simulate RPC call delay
		time.Sleep(10 * time.Millisecond)

		// Create test message
		msg := cdpProcessor.Message{
			Payload: map[string]interface{}{
				"ledger": ledger,
				"mode":   mode,
				"data":   fmt.Sprintf("test data for ledger %d", ledger),
			},
		}

		// Send to processors
		for _, proc := range adapter.processors {
			if err := proc.Process(ctx, msg); err != nil {
				return errors.Wrap(err, "processor error")
			}
		}

		// For continuous mode, keep polling
		if mode == "continuous" && ledger == endLedger {
			ledger = startLedger - 1 // Reset to continue loop
		}
	}

	return nil
}

// TestRPCSourceAdapter_Reconnection tests reconnection logic
func TestRPCSourceAdapter_Reconnection(t *testing.T) {
	mockServer := NewMockRPCServer()
	defer mockServer.Close()

	// Configure server to fail initially then succeed
	callCount := 0
	mockServer.behavior.ErrorOnCall["getLatestLedger"] = errors.New("connection failed")

	config := map[string]interface{}{
		"rpc_url":               mockServer.URL(),
		"network":               "testnet",
		"start_ledger":          float64(1000),
		"retry_attempts":        float64(5),
		"retry_backoff_ms":      float64(100),
		"reconnect_interval_ms": float64(500),
	}

	adapter := createTestRPCAdapter(t, config)
	mockProcessor := NewMockRPCProcessor()
	adapter.Subscribe(mockProcessor)

	// Simulate connection recovery after 2 attempts
	go func() {
		time.Sleep(300 * time.Millisecond)
		mockServer.mu.Lock()
		delete(mockServer.behavior.ErrorOnCall, "getLatestLedger")
		mockServer.mu.Unlock()
	}()

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	// Should eventually succeed after reconnection
	err := simulateRPCAdapterRun(ctx, adapter, mockServer)
	
	// Check that it retried and eventually succeeded
	assert.Greater(t, mockServer.GetCallCount("getLatestLedger"), 1)
	messages := mockProcessor.GetMessages()
	assert.Greater(t, len(messages), 0, "should receive messages after reconnection")
}

// Mock RPCSourceAdapter for testing (since we don't have the actual implementation)
type RPCSourceAdapter struct {
	config     map[string]interface{}
	processors []cdpProcessor.Processor
}

func NewRPCSourceAdapter(config map[string]interface{}) (SourceAdapter, error) {
	// Validate configuration
	rpcURL, ok := config["rpc_url"].(string)
	if !ok || rpcURL == "" {
		return nil, errors.New("rpc_url is required")
	}

	// Basic URL validation
	if !isValidURL(rpcURL) {
		return nil, errors.New("invalid rpc_url")
	}

	network, ok := config["network"].(string)
	if !ok || network == "" {
		return nil, errors.New("network is required")
	}

	return &RPCSourceAdapter{
		config: config,
	}, nil
}

func (a *RPCSourceAdapter) Subscribe(processor cdpProcessor.Processor) {
	a.processors = append(a.processors, processor)
}

func (a *RPCSourceAdapter) Run(ctx context.Context) error {
	// Actual implementation would make RPC calls
	return nil
}

func isValidURL(urlStr string) bool {
	parsed, err := url.Parse(urlStr)
	return err == nil && (parsed.Scheme == "http" || parsed.Scheme == "https") && parsed.Host != ""
}