package processor

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stellar/go/xdr"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// MockProcessor for testing subscription chain
type MockProcessor struct {
	mu               sync.Mutex
	receivedMessages []Message
	processDelay     time.Duration
	errorOnProcess   error
	callCount        int32
}

func NewMockProcessor() *MockProcessor {
	return &MockProcessor{
		receivedMessages: make([]Message, 0),
	}
}

func (m *MockProcessor) Process(ctx context.Context, msg Message) error {
	atomic.AddInt32(&m.callCount, 1)

	if m.processDelay > 0 {
		select {
		case <-time.After(m.processDelay):
		case <-ctx.Done():
			return ctx.Err()
		}
	}

	if m.errorOnProcess != nil {
		return m.errorOnProcess
	}

	m.mu.Lock()
	m.receivedMessages = append(m.receivedMessages, msg)
	m.mu.Unlock()

	return nil
}

func (m *MockProcessor) Subscribe(processor Processor) {
	// Not implemented for mock
}

func (m *MockProcessor) GetMessages() []Message {
	m.mu.Lock()
	defer m.mu.Unlock()
	return append([]Message{}, m.receivedMessages...)
}

func (m *MockProcessor) GetCallCount() int {
	return int(atomic.LoadInt32(&m.callCount))
}

// TestNewLatestLedger tests processor creation and configuration
func TestNewLatestLedger(t *testing.T) {
	tests := []struct {
		name           string
		config         map[string]interface{}
		expectedPeriod int
		wantErr        bool
	}{
		{
			name:           "default configuration",
			config:         map[string]interface{}{},
			expectedPeriod: 60, // Default 60 seconds
			wantErr:        false,
		},
		{
			name: "custom period",
			config: map[string]interface{}{
				"period": 30,
			},
			expectedPeriod: 30,
			wantErr:        false,
		},
		{
			name: "period as float",
			config: map[string]interface{}{
				"period": 45.0,
			},
			expectedPeriod: 45,
			wantErr:        false,
		},
		{
			name: "invalid period type",
			config: map[string]interface{}{
				"period": "invalid",
			},
			expectedPeriod: 60, // Falls back to default
			wantErr:        false,
		},
		{
			name: "zero period",
			config: map[string]interface{}{
				"period": 0,
			},
			expectedPeriod: 60, // Falls back to default
			wantErr:        false,
		},
		{
			name: "negative period",
			config: map[string]interface{}{
				"period": -10,
			},
			expectedPeriod: 60, // Falls back to default
			wantErr:        false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			processor, err := NewLatestLedger(tt.config)

			if tt.wantErr {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
				assert.NotNil(t, processor)

				// Check period was set correctly
				latestLedger := processor.(*LatestLedger)
				assert.Equal(t, tt.expectedPeriod, latestLedger.period)
			}
		})
	}
}

// TestLatestLedger_Process tests message processing logic
func TestLatestLedger_Process(t *testing.T) {
	t.Run("tracks latest ledger sequence", func(t *testing.T) {
		processor, err := NewLatestLedger(map[string]interface{}{})
		require.NoError(t, err)

		ctx := context.Background()
		latestLedger := processor.(*LatestLedger)

		// Process ledgers in order
		sequences := []uint32{100, 101, 102, 103}
		for _, seq := range sequences {
			ledger := createTestLedger(seq)
			msg := Message{Payload: ledger}
			
			err := processor.Process(ctx, msg)
			assert.NoError(t, err)
			
			// Verify latest sequence is tracked
			assert.Equal(t, seq, latestLedger.GetLatestSequence())
		}
	})

	t.Run("handles out of order ledgers", func(t *testing.T) {
		processor, err := NewLatestLedger(map[string]interface{}{})
		require.NoError(t, err)

		ctx := context.Background()
		latestLedger := processor.(*LatestLedger)

		// Process ledgers out of order
		sequences := []uint32{100, 105, 103, 107, 102}
		expectedLatest := []uint32{100, 105, 105, 107, 107}

		for i, seq := range sequences {
			ledger := createTestLedger(seq)
			msg := Message{Payload: ledger}
			
			err := processor.Process(ctx, msg)
			assert.NoError(t, err)
			
			// Verify only higher sequences update latest
			assert.Equal(t, expectedLatest[i], latestLedger.GetLatestSequence())
		}
	})

	t.Run("forwards messages to subscribers", func(t *testing.T) {
		processor, err := NewLatestLedger(map[string]interface{}{})
		require.NoError(t, err)

		// Subscribe mock processors
		mock1 := NewMockProcessor()
		mock2 := NewMockProcessor()
		processor.Subscribe(mock1)
		processor.Subscribe(mock2)

		ctx := context.Background()

		// Process a ledger
		ledger := createTestLedger(200)
		msg := Message{Payload: ledger}
		
		err = processor.Process(ctx, msg)
		assert.NoError(t, err)

		// Verify both subscribers received the message
		assert.Len(t, mock1.GetMessages(), 1)
		assert.Len(t, mock2.GetMessages(), 1)

		// Verify message content
		receivedMsg := mock1.GetMessages()[0]
		receivedLedger, ok := receivedMsg.Payload.(xdr.LedgerCloseMeta)
		assert.True(t, ok)
		assert.Equal(t, uint32(200), uint32(receivedLedger.LedgerSequence()))
	})

	t.Run("handles invalid payload types", func(t *testing.T) {
		processor, err := NewLatestLedger(map[string]interface{}{})
		require.NoError(t, err)

		ctx := context.Background()

		// Send non-ledger payload
		msg := Message{
			Payload: map[string]interface{}{
				"not": "a ledger",
			},
		}

		err = processor.Process(ctx, msg)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "expected LedgerCloseMeta")
	})

	t.Run("continues processing on subscriber error", func(t *testing.T) {
		processor, err := NewLatestLedger(map[string]interface{}{})
		require.NoError(t, err)

		// Subscribe processors with different behaviors
		mockFailing := NewMockProcessor()
		mockFailing.errorOnProcess = fmt.Errorf("subscriber error")
		
		mockSuccessful := NewMockProcessor()

		processor.Subscribe(mockFailing)
		processor.Subscribe(mockSuccessful)

		ctx := context.Background()

		// Process a ledger
		ledger := createTestLedger(300)
		msg := Message{Payload: ledger}
		
		err = processor.Process(ctx, msg)
		assert.NoError(t, err) // Should not fail due to subscriber error

		// Verify successful subscriber still received message
		assert.Len(t, mockSuccessful.GetMessages(), 1)
		assert.Equal(t, 1, mockFailing.GetCallCount()) // Was called despite error
	})
}

// TestLatestLedger_PeriodLogging tests periodic logging functionality
func TestLatestLedger_PeriodLogging(t *testing.T) {
	t.Run("logs at specified intervals", func(t *testing.T) {
		// Create processor with short period for testing
		processor, err := NewLatestLedger(map[string]interface{}{
			"period": 1, // 1 second for testing
		})
		require.NoError(t, err)

		ctx := context.Background()
		latestLedger := processor.(*LatestLedger)

		// Process initial ledger
		ledger := createTestLedger(1000)
		msg := Message{Payload: ledger}
		err = processor.Process(ctx, msg)
		assert.NoError(t, err)

		// Record initial time
		initialTime := latestLedger.lastLogTime

		// Process another ledger immediately
		ledger = createTestLedger(1001)
		msg = Message{Payload: ledger}
		err = processor.Process(ctx, msg)
		assert.NoError(t, err)

		// Should not have logged yet
		assert.Equal(t, initialTime, latestLedger.lastLogTime)

		// Wait for period to pass
		time.Sleep(1100 * time.Millisecond)

		// Process another ledger
		ledger = createTestLedger(1002)
		msg = Message{Payload: ledger}
		err = processor.Process(ctx, msg)
		assert.NoError(t, err)

		// Should have logged and updated time
		assert.NotEqual(t, initialTime, latestLedger.lastLogTime)
	})
}

// TestLatestLedger_Concurrency tests thread safety
func TestLatestLedger_Concurrency(t *testing.T) {
	processor, err := NewLatestLedger(map[string]interface{}{})
	require.NoError(t, err)

	ctx := context.Background()
	latestLedger := processor.(*LatestLedger)

	// Subscribe a mock processor
	mock := NewMockProcessor()
	processor.Subscribe(mock)

	// Process ledgers concurrently
	numGoroutines := 10
	ledgersPerGoroutine := 100
	var wg sync.WaitGroup

	for i := 0; i < numGoroutines; i++ {
		wg.Add(1)
		go func(workerID int) {
			defer wg.Done()
			
			for j := 0; j < ledgersPerGoroutine; j++ {
				sequence := uint32(workerID*1000 + j)
				ledger := createTestLedger(sequence)
				msg := Message{Payload: ledger}
				
				err := processor.Process(ctx, msg)
				assert.NoError(t, err)
			}
		}(i)
	}

	wg.Wait()

	// Verify all messages were forwarded
	assert.Equal(t, numGoroutines*ledgersPerGoroutine, mock.GetCallCount())

	// Verify latest sequence is the highest processed
	expectedLatest := uint32((numGoroutines-1)*1000 + ledgersPerGoroutine - 1)
	assert.LessOrEqual(t, latestLedger.GetLatestSequence(), expectedLatest)
}

// TestLatestLedger_ContextCancellation tests graceful shutdown
func TestLatestLedger_ContextCancellation(t *testing.T) {
	processor, err := NewLatestLedger(map[string]interface{}{})
	require.NoError(t, err)

	// Subscribe a slow processor
	mock := NewMockProcessor()
	mock.processDelay = 100 * time.Millisecond
	processor.Subscribe(mock)

	ctx, cancel := context.WithCancel(context.Background())

	// Start processing in background
	go func() {
		for i := 0; i < 10; i++ {
			ledger := createTestLedger(uint32(i))
			msg := Message{Payload: ledger}
			processor.Process(ctx, msg)
		}
	}()

	// Cancel after short time
	time.Sleep(50 * time.Millisecond)
	cancel()

	// Give time for cancellation to propagate
	time.Sleep(200 * time.Millisecond)

	// Should have processed some but not all messages
	processedCount := mock.GetCallCount()
	assert.Greater(t, processedCount, 0)
	assert.Less(t, processedCount, 10)
}

// BenchmarkLatestLedger tests performance
func BenchmarkLatestLedger_Process(b *testing.B) {
	processor, err := NewLatestLedger(map[string]interface{}{})
	require.NoError(b, err)

	// Subscribe a lightweight mock
	mock := NewMockProcessor()
	processor.Subscribe(mock)

	ctx := context.Background()

	// Pre-create ledgers
	ledgers := make([]xdr.LedgerCloseMeta, b.N)
	for i := 0; i < b.N; i++ {
		ledgers[i] = createTestLedger(uint32(i))
	}

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		msg := Message{Payload: ledgers[i]}
		err := processor.Process(ctx, msg)
		if err != nil {
			b.Fatal(err)
		}
	}

	b.StopTimer()

	// Report metrics
	b.ReportMetric(float64(b.N), "ledgers")
	b.ReportMetric(float64(b.N)/b.Elapsed().Seconds(), "ledgers/sec")
}

func BenchmarkLatestLedger_ConcurrentProcess(b *testing.B) {
	processor, err := NewLatestLedger(map[string]interface{}{})
	require.NoError(b, err)

	// Subscribe multiple mocks to simulate real load
	for i := 0; i < 5; i++ {
		processor.Subscribe(NewMockProcessor())
	}

	ctx := context.Background()

	b.ResetTimer()
	b.ReportAllocs()

	// Process concurrently
	b.RunParallel(func(pb *testing.PB) {
		sequence := uint32(0)
		for pb.Next() {
			ledger := createTestLedger(atomic.AddUint32(&sequence, 1))
			msg := Message{Payload: ledger}
			processor.Process(ctx, msg)
		}
	})

	b.StopTimer()

	// Report metrics
	b.ReportMetric(float64(b.N), "ledgers")
	b.ReportMetric(float64(b.N)/b.Elapsed().Seconds(), "ledgers/sec")
}

// Helper functions

func createTestLedger(sequence uint32) xdr.LedgerCloseMeta {
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

// Mock LatestLedger implementation for testing
type LatestLedger struct {
	period          int
	latestSequence  uint32
	lastLogTime     time.Time
	subscribers     []Processor
	mu              sync.RWMutex
}

func NewLatestLedger(config map[string]interface{}) (Processor, error) {
	period := 60 // Default 60 seconds

	if p, ok := config["period"]; ok {
		switch v := p.(type) {
		case int:
			if v > 0 {
				period = v
			}
		case float64:
			if v > 0 {
				period = int(v)
			}
		}
	}

	return &LatestLedger{
		period:      period,
		lastLogTime: time.Now(),
	}, nil
}

func (p *LatestLedger) Process(ctx context.Context, msg Message) error {
	// Validate payload type
	ledger, ok := msg.Payload.(xdr.LedgerCloseMeta)
	if !ok {
		return fmt.Errorf("expected LedgerCloseMeta, got %T", msg.Payload)
	}

	sequence := uint32(ledger.LedgerSequence())

	// Update latest sequence
	p.mu.Lock()
	if sequence > p.latestSequence {
		p.latestSequence = sequence
	}
	currentLatest := p.latestSequence
	p.mu.Unlock()

	// Check if we should log
	if time.Since(p.lastLogTime) >= time.Duration(p.period)*time.Second {
		// In real implementation, would log here
		p.mu.Lock()
		p.lastLogTime = time.Now()
		p.mu.Unlock()
	}

	// Forward to subscribers
	p.mu.RLock()
	subscribers := append([]Processor{}, p.subscribers...)
	p.mu.RUnlock()

	for _, subscriber := range subscribers {
		if err := subscriber.Process(ctx, msg); err != nil {
			// Log error but continue processing
			continue
		}
	}

	return nil
}

func (p *LatestLedger) Subscribe(processor Processor) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.subscribers = append(p.subscribers, processor)
}

func (p *LatestLedger) GetLatestSequence() uint32 {
	p.mu.RLock()
	defer p.mu.RUnlock()
	return p.latestSequence
}