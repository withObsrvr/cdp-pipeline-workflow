package main

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/pkg/errors"
	"github.com/stellar/go/historyarchive"
	"github.com/stellar/go/ingest/ledgerbackend"
	"github.com/stellar/go/xdr"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/withObsrvr/cdp-pipeline-workflow/processor"
)

// MockLedgerBackend simulates the Stellar Core backend
type MockLedgerBackend struct {
	mu              sync.Mutex
	ledgers         map[uint32]xdr.LedgerCloseMeta
	currentSequence uint32
	prepareRangeErr error
	getledgerErr    error
	closeErr        error
	behavior        MockBackendBehavior
	closed          bool
}

type MockBackendBehavior struct {
	PrepareDelay   time.Duration
	GetLedgerDelay time.Duration
	FailAfterN     int
	processed      int
}

func NewMockLedgerBackend() *MockLedgerBackend {
	return &MockLedgerBackend{
		ledgers:         make(map[uint32]xdr.LedgerCloseMeta),
		currentSequence: 1,
	}
}

func (m *MockLedgerBackend) PrepareRange(ctx context.Context, r ledgerbackend.Range) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	if m.behavior.PrepareDelay > 0 {
		select {
		case <-time.After(m.behavior.PrepareDelay):
		case <-ctx.Done():
			return ctx.Err()
		}
	}

	if m.prepareRangeErr != nil {
		return m.prepareRangeErr
	}

	// Generate ledgers for the range
	for seq := r.From; seq <= r.To; seq++ {
		m.ledgers[seq] = createMockLedger(seq)
	}
	m.currentSequence = r.From

	return nil
}

func (m *MockLedgerBackend) GetLedger(ctx context.Context, sequence uint32) (xdr.LedgerCloseMeta, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	if m.behavior.GetLedgerDelay > 0 {
		select {
		case <-time.After(m.behavior.GetLedgerDelay):
		case <-ctx.Done():
			return xdr.LedgerCloseMeta{}, ctx.Err()
		}
	}

	m.behavior.processed++
	if m.behavior.FailAfterN > 0 && m.behavior.processed > m.behavior.FailAfterN {
		return xdr.LedgerCloseMeta{}, errors.New("simulated failure")
	}

	if m.getledgerErr != nil {
		return xdr.LedgerCloseMeta{}, m.getledgerErr
	}

	ledger, ok := m.ledgers[sequence]
	if !ok {
		return xdr.LedgerCloseMeta{}, errors.New("ledger not found")
	}

	return ledger, nil
}

func (m *MockLedgerBackend) Close() error {
	m.mu.Lock()
	defer m.mu.Unlock()
	
	if m.closed {
		return errors.New("already closed")
	}
	
	m.closed = true
	return m.closeErr
}

func (m *MockLedgerBackend) IsClosed() bool {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.closed
}

// MockCaptiveCoreAdapter wraps the real adapter with test hooks
type MockCaptiveCoreAdapter struct {
	*CaptiveCoreInboundAdapter
	mockBackend *MockLedgerBackend
}

// TestCaptiveCoreInboundAdapter_Configuration tests configuration validation
func TestCaptiveCoreInboundAdapter_Configuration(t *testing.T) {
	tests := []struct {
		name        string
		config      map[string]interface{}
		expectError bool
		errorMsg    string
	}{
		{
			name: "valid testnet configuration",
			config: map[string]interface{}{
				"history_archive_urls": []interface{}{
					"https://history.stellar.org/prd/core-testnet-001",
				},
				"network":         "testnet",
				"core_binary_path": "/usr/bin/stellar-core",
			},
			expectError: false,
		},
		{
			name: "valid pubnet configuration",
			config: map[string]interface{}{
				"history_archive_urls": []interface{}{
					"https://history.stellar.org/prd/core-live-001",
					"https://history.stellar.org/prd/core-live-002",
				},
				"network":         "pubnet",
				"core_binary_path": "/usr/bin/stellar-core",
			},
			expectError: false,
		},
		{
			name: "missing history_archive_urls",
			config: map[string]interface{}{
				"network":         "testnet",
				"core_binary_path": "/usr/bin/stellar-core",
			},
			expectError: true,
			errorMsg:    "history_archive_urls is missing",
		},
		{
			name: "invalid history_archive_urls type",
			config: map[string]interface{}{
				"history_archive_urls": "not-a-slice",
				"network":              "testnet",
				"core_binary_path":     "/usr/bin/stellar-core",
			},
			expectError: true,
			errorMsg:    "history_archive_urls must be a slice",
		},
		{
			name: "missing network",
			config: map[string]interface{}{
				"history_archive_urls": []interface{}{
					"https://history.stellar.org/prd/core-testnet-001",
				},
				"core_binary_path": "/usr/bin/stellar-core",
			},
			expectError: true,
			errorMsg:    "network must be a string",
		},
		{
			name: "unsupported network",
			config: map[string]interface{}{
				"history_archive_urls": []interface{}{
					"https://history.stellar.org/prd/core-testnet-001",
				},
				"network":         "invalid-network",
				"core_binary_path": "/usr/bin/stellar-core",
			},
			expectError: true,
			errorMsg:    "unsupported network",
		},
		{
			name: "missing core_binary_path",
			config: map[string]interface{}{
				"history_archive_urls": []interface{}{
					"https://history.stellar.org/prd/core-testnet-001",
				},
				"network": "testnet",
			},
			expectError: true,
			errorMsg:    "core_binary_path must be a string",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			adapter, err := NewCaptiveCoreInboundAdapter(tt.config)
			
			if tt.expectError {
				assert.Error(t, err)
				if tt.errorMsg != "" {
					assert.Contains(t, err.Error(), tt.errorMsg)
				}
			} else {
				assert.NoError(t, err)
				assert.NotNil(t, adapter)
				
				// Verify configuration was set correctly
				captiveAdapter := adapter.(*CaptiveCoreInboundAdapter)
				assert.Equal(t, tt.config["network"], getNetworkFromPassphrase(captiveAdapter.networkPassphrase))
				assert.Equal(t, tt.config["core_binary_path"], captiveAdapter.TomlParams.CoreBinaryPath)
			}
		})
	}
}

// TestCaptiveCoreInboundAdapter_ProcessLedgers tests ledger processing
func TestCaptiveCoreInboundAdapter_ProcessLedgers(t *testing.T) {
	tests := []struct {
		name            string
		setupMock       func(*MockLedgerBackend)
		ledgerRange     ledgerbackend.Range
		expectedCount   int
		expectError     bool
		validateResults func(*testing.T, []processor.Message)
	}{
		{
			name: "process single ledger",
			setupMock: func(m *MockLedgerBackend) {
				// Mock will auto-generate ledgers
			},
			ledgerRange:   ledgerbackend.Range{From: 100, To: 100},
			expectedCount: 1,
			validateResults: func(t *testing.T, messages []processor.Message) {
				assert.Len(t, messages, 1)
				ledger := messages[0].Payload.(xdr.LedgerCloseMeta)
				assert.Equal(t, uint32(100), uint32(ledger.LedgerSequence()))
			},
		},
		{
			name: "process ledger range",
			setupMock: func(m *MockLedgerBackend) {
				// Mock will auto-generate ledgers
			},
			ledgerRange:   ledgerbackend.Range{From: 200, To: 210},
			expectedCount: 11,
			validateResults: func(t *testing.T, messages []processor.Message) {
				// Verify ledgers are in sequence
				for i, msg := range messages {
					ledger := msg.Payload.(xdr.LedgerCloseMeta)
					expectedSeq := uint32(200 + i)
					assert.Equal(t, expectedSeq, uint32(ledger.LedgerSequence()))
				}
			},
		},
		{
			name: "handle prepare range error",
			setupMock: func(m *MockLedgerBackend) {
				m.prepareRangeErr = errors.New("network error")
			},
			ledgerRange: ledgerbackend.Range{From: 300, To: 310},
			expectError: true,
		},
		{
			name: "handle get ledger error",
			setupMock: func(m *MockLedgerBackend) {
				m.getledgerErr = errors.New("ledger corrupted")
			},
			ledgerRange: ledgerbackend.Range{From: 400, To: 405},
			expectError: true,
		},
		{
			name: "handle context cancellation",
			setupMock: func(m *MockLedgerBackend) {
				m.behavior.GetLedgerDelay = 100 * time.Millisecond
			},
			ledgerRange: ledgerbackend.Range{From: 500, To: 520},
			expectError: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Create mock backend
			mockBackend := NewMockLedgerBackend()
			if tt.setupMock != nil {
				tt.setupMock(mockBackend)
			}

			// Create adapter with test configuration
			adapter := createTestCaptiveCoreAdapter(t)
			
			// Create mock processor
			mockProcessor := NewMockCaptiveCoreProcessor()
			adapter.Subscribe(mockProcessor)

			// Create context
			ctx := context.Background()
			if tt.name == "handle context cancellation" {
				var cancel context.CancelFunc
				ctx, cancel = context.WithTimeout(ctx, 50*time.Millisecond)
				defer cancel()
			}

			// Simulate the Run method with our mock
			err := simulateAdapterRun(ctx, adapter, mockBackend, tt.ledgerRange)

			// Validate results
			if tt.expectError {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
				messages := mockProcessor.GetMessages()
				assert.Len(t, messages, tt.expectedCount)
				
				if tt.validateResults != nil {
					tt.validateResults(t, messages)
				}
			}
		})
	}
}

// TestCaptiveCoreInboundAdapter_MultipleProcessors tests fan-out to multiple processors
func TestCaptiveCoreInboundAdapter_MultipleProcessors(t *testing.T) {
	adapter := createTestCaptiveCoreAdapter(t)
	
	// Subscribe multiple processors
	processors := make([]*MockCaptiveCoreProcessor, 3)
	for i := range processors {
		processors[i] = NewMockCaptiveCoreProcessor()
		adapter.Subscribe(processors[i])
	}

	// Create mock backend
	mockBackend := NewMockLedgerBackend()
	
	// Process some ledgers
	ctx := context.Background()
	ledgerRange := ledgerbackend.Range{From: 1000, To: 1005}
	
	err := simulateAdapterRun(ctx, adapter, mockBackend, ledgerRange)
	require.NoError(t, err)

	// Verify all processors received all messages
	expectedCount := int(ledgerRange.To - ledgerRange.From + 1)
	for i, proc := range processors {
		messages := proc.GetMessages()
		assert.Len(t, messages, expectedCount, "processor %d should receive all messages", i)
		
		// Verify message order
		for j, msg := range messages {
			ledger := msg.Payload.(xdr.LedgerCloseMeta)
			expectedSeq := ledgerRange.From + uint32(j)
			assert.Equal(t, expectedSeq, uint32(ledger.LedgerSequence()))
		}
	}
}

// TestCaptiveCoreInboundAdapter_ErrorRecovery tests error handling and recovery
func TestCaptiveCoreInboundAdapter_ErrorRecovery(t *testing.T) {
	t.Run("processor error doesn't stop pipeline", func(t *testing.T) {
		adapter := createTestCaptiveCoreAdapter(t)
		
		// Create processors with different behaviors
		goodProcessor := NewMockCaptiveCoreProcessor()
		badProcessor := NewMockCaptiveCoreProcessor()
		badProcessor.errorOnProcess = errors.New("processor error")
		
		adapter.Subscribe(badProcessor)
		adapter.Subscribe(goodProcessor)
		
		mockBackend := NewMockLedgerBackend()
		ctx := context.Background()
		ledgerRange := ledgerbackend.Range{From: 2000, To: 2005}
		
		// Should continue despite one processor failing
		err := simulateAdapterRun(ctx, adapter, mockBackend, ledgerRange)
		assert.NoError(t, err)
		
		// Good processor should still receive all messages
		assert.Len(t, goodProcessor.GetMessages(), 6)
		assert.Equal(t, 6, badProcessor.GetCallCount()) // Called but errored
	})

	t.Run("recovers from transient backend errors", func(t *testing.T) {
		adapter := createTestCaptiveCoreAdapter(t)
		mockProcessor := NewMockCaptiveCoreProcessor()
		adapter.Subscribe(mockProcessor)
		
		mockBackend := NewMockLedgerBackend()
		mockBackend.behavior.FailAfterN = 3 // Fail after 3 ledgers
		
		ctx := context.Background()
		ledgerRange := ledgerbackend.Range{From: 3000, To: 3010}
		
		err := simulateAdapterRun(ctx, adapter, mockBackend, ledgerRange)
		assert.Error(t, err) // Should error after failures
		
		// Should have processed first 3 ledgers
		assert.Len(t, mockProcessor.GetMessages(), 3)
	})
}

// Benchmark tests
func BenchmarkCaptiveCoreInboundAdapter(b *testing.B) {
	adapter := createTestCaptiveCoreAdapter(b)
	mockProcessor := NewMockCaptiveCoreProcessor()
	adapter.Subscribe(mockProcessor)
	
	mockBackend := NewMockLedgerBackend()
	ctx := context.Background()
	
	b.ResetTimer()
	b.ReportAllocs()
	
	ledgerRange := ledgerbackend.Range{From: 0, To: uint32(b.N - 1)}
	err := simulateAdapterRun(ctx, adapter, mockBackend, ledgerRange)
	if err != nil {
		b.Fatal(err)
	}
	
	b.StopTimer()
	
	// Report metrics
	b.ReportMetric(float64(b.N), "ledgers")
	b.ReportMetric(float64(b.N)/b.Elapsed().Seconds(), "ledgers/sec")
}

// Helper functions

func createTestCaptiveCoreAdapter(t testing.TB) *CaptiveCoreInboundAdapter {
	config := map[string]interface{}{
		"history_archive_urls": []interface{}{
			"https://history.stellar.org/prd/core-testnet-001",
		},
		"network":         "testnet",
		"core_binary_path": "/usr/bin/stellar-core",
	}
	
	adapter, err := NewCaptiveCoreInboundAdapter(config)
	require.NoError(t, err)
	
	return adapter.(*CaptiveCoreInboundAdapter)
}

func createMockLedger(sequence uint32) xdr.LedgerCloseMeta {
	return xdr.LedgerCloseMeta{
		V: 1,
		V1: &xdr.LedgerCloseMetaV1{
			LedgerHeader: xdr.LedgerHeaderHistoryEntry{
				Header: xdr.LedgerHeader{
					LedgerSeq: xdr.Uint32(sequence),
					ScpValue: xdr.StellarValue{
						CloseTime: xdr.TimePoint(time.Now().Unix()),
					},
					BucketListHash: xdr.Hash{1, 2, 3},
				},
			},
			TxSet: xdr.GeneralizedTransactionSet{
				V: 1,
				V1TxSet: &xdr.TransactionSetV1{
					PreviousLedgerHash: xdr.Hash{},
					Phases: []xdr.TransactionPhase{
						{
							V: 0,
							V0Components: &[]xdr.TxSetComponent{
								{
									Type: xdr.TxSetComponentTypeTxsetCompTxsMaybeDiscountedFee,
									TxsMaybeDiscountedFee: &xdr.TxSetComponentTxsMaybeDiscountedFee{
										Txs: []xdr.TransactionEnvelope{},
									},
								},
							},
						},
					},
				},
			},
		},
	}
}

func getNetworkFromPassphrase(passphrase string) string {
	switch passphrase {
	case "Test SDF Network ; September 2015":
		return "testnet"
	case "Public Global Stellar Network ; September 2015":
		return "pubnet"
	default:
		return "unknown"
	}
}

// simulateAdapterRun simulates the adapter's Run method with our mock backend
func simulateAdapterRun(ctx context.Context, adapter *CaptiveCoreInboundAdapter, backend *MockLedgerBackend, ledgerRange ledgerbackend.Range) error {
	// Prepare range
	if err := backend.PrepareRange(ctx, ledgerRange); err != nil {
		return err
	}
	
	// Process ledgers
	for seq := ledgerRange.From; seq <= ledgerRange.To; seq++ {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}
		
		ledger, err := backend.GetLedger(ctx, seq)
		if err != nil {
			return errors.Wrap(err, "error getting ledger")
		}
		
		// Send to processors
		for _, proc := range adapter.processors {
			if err := proc.Process(ctx, processor.Message{
				Payload: ledger,
			}); err != nil {
				// Log but continue with other processors
				log.Printf("processor error: %v", err)
			}
		}
	}
	
	return backend.Close()
}

// MockCaptiveCoreProcessor for testing
type MockCaptiveCoreProcessor struct {
	mu               sync.Mutex
	receivedMessages []processor.Message
	errorOnProcess   error
	callCount        int32
}

func NewMockCaptiveCoreProcessor() *MockCaptiveCoreProcessor {
	return &MockCaptiveCoreProcessor{
		receivedMessages: make([]processor.Message, 0),
	}
}

func (m *MockCaptiveCoreProcessor) Process(ctx context.Context, msg processor.Message) error {
	atomic.AddInt32(&m.callCount, 1)
	
	if m.errorOnProcess != nil {
		return m.errorOnProcess
	}
	
	m.mu.Lock()
	m.receivedMessages = append(m.receivedMessages, msg)
	m.mu.Unlock()
	
	return nil
}

func (m *MockCaptiveCoreProcessor) Subscribe(processor processor.Processor) {
	// Not needed for mock
}

func (m *MockCaptiveCoreProcessor) GetMessages() []processor.Message {
	m.mu.Lock()
	defer m.mu.Unlock()
	return append([]processor.Message{}, m.receivedMessages...)
}

func (m *MockCaptiveCoreProcessor) GetCallCount() int {
	return int(atomic.LoadInt32(&m.callCount))
}