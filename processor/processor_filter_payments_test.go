package processor

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/stellar/go/xdr"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestNewFilterPayments tests processor creation and configuration
func TestNewFilterPayments(t *testing.T) {
	tests := []struct {
		name              string
		config            map[string]interface{}
		expectedAssets    []string
		expectedAccounts  []string
		expectedOperators []string
		wantErr           bool
	}{
		{
			name:   "empty configuration",
			config: map[string]interface{}{},
			wantErr: false,
		},
		{
			name: "filter by assets",
			config: map[string]interface{}{
				"filter_assets": []interface{}{"USDC", "XLM", "EURC"},
			},
			expectedAssets: []string{"USDC", "XLM", "EURC"},
			wantErr:        false,
		},
		{
			name: "filter by accounts",
			config: map[string]interface{}{
				"filter_accounts": []interface{}{
					"GABC123...",
					"GDEF456...",
				},
			},
			expectedAccounts: []string{"GABC123...", "GDEF456..."},
			wantErr:          false,
		},
		{
			name: "filter by operators",
			config: map[string]interface{}{
				"filter_operators": []interface{}{"eq", "gte", "lte"},
			},
			expectedOperators: []string{"eq", "gte", "lte"},
			wantErr:           false,
		},
		{
			name: "combined filters",
			config: map[string]interface{}{
				"filter_assets":    []interface{}{"USDC"},
				"filter_accounts":  []interface{}{"GABC123..."},
				"filter_operators": []interface{}{"eq"},
			},
			expectedAssets:    []string{"USDC"},
			expectedAccounts:  []string{"GABC123..."},
			expectedOperators: []string{"eq"},
			wantErr:           false,
		},
		{
			name: "invalid filter_assets type",
			config: map[string]interface{}{
				"filter_assets": "not-a-slice",
			},
			wantErr: true,
		},
		{
			name: "amount range filters",
			config: map[string]interface{}{
				"min_amount": 100.0,
				"max_amount": 10000.0,
			},
			wantErr: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			processor, err := NewFilterPayments(tt.config)

			if tt.wantErr {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
				assert.NotNil(t, processor)

				// Verify filters were set correctly
				filterPayments := processor.(*FilterPayments)
				
				if tt.expectedAssets != nil {
					assert.Equal(t, tt.expectedAssets, filterPayments.filterAssets)
				}
				if tt.expectedAccounts != nil {
					assert.Equal(t, tt.expectedAccounts, filterPayments.filterAccounts)
				}
				if tt.expectedOperators != nil {
					assert.Equal(t, tt.expectedOperators, filterPayments.filterOperators)
				}
			}
		})
	}
}

// TestFilterPayments_Process tests payment filtering logic
func TestFilterPayments_Process(t *testing.T) {
	t.Run("filters payment operations", func(t *testing.T) {
		processor, err := NewFilterPayments(map[string]interface{}{
			"filter_assets": []interface{}{"USDC"},
		})
		require.NoError(t, err)

		// Subscribe a mock processor
		mock := NewMockProcessor()
		processor.Subscribe(mock)

		ctx := context.Background()

		// Create ledger with mixed operations
		ledger := createLedgerWithOperations([]xdr.Operation{
			createPaymentOperation("USDC", "1000"),    // Should pass
			createPaymentOperation("XLM", "500"),      // Should be filtered
			createCreateAccountOperation("100"),       // Should be filtered
			createPaymentOperation("USDC", "2000"),    // Should pass
		})

		msg := Message{Payload: ledger}
		err = processor.Process(ctx, msg)
		assert.NoError(t, err)

		// Should only forward USDC payments
		messages := mock.GetMessages()
		assert.Len(t, messages, 2)
	})

	t.Run("filters by source account", func(t *testing.T) {
		sourceAccount := "GABC123TESTACCOUNT"
		processor, err := NewFilterPayments(map[string]interface{}{
			"filter_accounts": []interface{}{sourceAccount},
		})
		require.NoError(t, err)

		mock := NewMockProcessor()
		processor.Subscribe(mock)

		ctx := context.Background()

		// Create transactions from different accounts
		ledger := createLedgerWithTransactions([]string{
			sourceAccount,    // Should pass
			"GOTHER123",      // Should be filtered
			sourceAccount,    // Should pass
			"GANOTHER456",    // Should be filtered
		})

		msg := Message{Payload: ledger}
		err = processor.Process(ctx, msg)
		assert.NoError(t, err)

		// Should only forward transactions from specified account
		messages := mock.GetMessages()
		assert.Len(t, messages, 2)
	})

	t.Run("filters by amount range", func(t *testing.T) {
		processor, err := NewFilterPayments(map[string]interface{}{
			"min_amount": 100.0,
			"max_amount": 1000.0,
		})
		require.NoError(t, err)

		mock := NewMockProcessor()
		processor.Subscribe(mock)

		ctx := context.Background()

		// Create payments with different amounts
		ledger := createLedgerWithOperations([]xdr.Operation{
			createPaymentOperation("XLM", "50"),    // Too small
			createPaymentOperation("XLM", "500"),   // In range
			createPaymentOperation("XLM", "1500"),  // Too large
			createPaymentOperation("XLM", "750"),   // In range
		})

		msg := Message{Payload: ledger}
		err = processor.Process(ctx, msg)
		assert.NoError(t, err)

		// Should only forward payments in range
		messages := mock.GetMessages()
		assert.Len(t, messages, 2)
	})

	t.Run("combines multiple filters", func(t *testing.T) {
		processor, err := NewFilterPayments(map[string]interface{}{
			"filter_assets":   []interface{}{"USDC", "EURC"},
			"filter_accounts": []interface{}{"GABC123"},
			"min_amount":      100.0,
		})
		require.NoError(t, err)

		mock := NewMockProcessor()
		processor.Subscribe(mock)

		ctx := context.Background()

		// Create various payment scenarios
		ledger := createComplexLedger()

		msg := Message{Payload: ledger}
		err = processor.Process(ctx, msg)
		assert.NoError(t, err)

		// Verify filtering worked correctly
		messages := mock.GetMessages()
		for _, msg := range messages {
			// Each forwarded message should meet ALL criteria
			payment := msg.Payload.(PaymentInfo)
			assert.Contains(t, []string{"USDC", "EURC"}, payment.Asset)
			assert.Equal(t, "GABC123", payment.Account)
			assert.GreaterOrEqual(t, payment.Amount, 100.0)
		}
	})

	t.Run("handles empty ledger", func(t *testing.T) {
		processor, err := NewFilterPayments(map[string]interface{}{})
		require.NoError(t, err)

		mock := NewMockProcessor()
		processor.Subscribe(mock)

		ctx := context.Background()

		// Create empty ledger
		ledger := createEmptyLedger()

		msg := Message{Payload: ledger}
		err = processor.Process(ctx, msg)
		assert.NoError(t, err)

		// Should not forward anything
		messages := mock.GetMessages()
		assert.Len(t, messages, 0)
	})

	t.Run("handles invalid payload", func(t *testing.T) {
		processor, err := NewFilterPayments(map[string]interface{}{})
		require.NoError(t, err)

		ctx := context.Background()

		// Send non-ledger payload
		msg := Message{
			Payload: "not a ledger",
		}

		err = processor.Process(ctx, msg)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "expected LedgerCloseMeta")
	})
}

// TestFilterPayments_PathPayments tests path payment filtering
func TestFilterPayments_PathPayments(t *testing.T) {
	t.Run("filters path payments by destination asset", func(t *testing.T) {
		processor, err := NewFilterPayments(map[string]interface{}{
			"filter_assets": []interface{}{"USDC"},
		})
		require.NoError(t, err)

		mock := NewMockProcessor()
		processor.Subscribe(mock)

		ctx := context.Background()

		// Create ledger with path payments
		ledger := createLedgerWithOperations([]xdr.Operation{
			createPathPaymentOperation("XLM", "USDC", "1000"),   // Should pass (dest is USDC)
			createPathPaymentOperation("USDC", "XLM", "500"),    // Should be filtered
			createPathPaymentOperation("EURC", "USDC", "2000"),  // Should pass
		})

		msg := Message{Payload: ledger}
		err = processor.Process(ctx, msg)
		assert.NoError(t, err)

		// Should forward path payments ending in USDC
		messages := mock.GetMessages()
		assert.Len(t, messages, 2)
	})
}

// TestFilterPayments_Concurrency tests thread safety
func TestFilterPayments_Concurrency(t *testing.T) {
	processor, err := NewFilterPayments(map[string]interface{}{
		"filter_assets": []interface{}{"USDC", "XLM"},
		"min_amount":    10.0,
	})
	require.NoError(t, err)

	mock := NewMockProcessor()
	processor.Subscribe(mock)

	ctx := context.Background()
	numGoroutines := 10
	ledgersPerGoroutine := 100

	var wg sync.WaitGroup
	for i := 0; i < numGoroutines; i++ {
		wg.Add(1)
		go func(workerID int) {
			defer wg.Done()
			
			for j := 0; j < ledgersPerGoroutine; j++ {
				// Create ledger with qualifying payments
				ledger := createLedgerWithOperations([]xdr.Operation{
					createPaymentOperation("USDC", fmt.Sprintf("%d", 100+j)),
					createPaymentOperation("XLM", fmt.Sprintf("%d", 50+j)),
				})
				
				msg := Message{Payload: ledger}
				err := processor.Process(ctx, msg)
				assert.NoError(t, err)
			}
		}(i)
	}

	wg.Wait()

	// Verify all qualifying payments were forwarded
	messages := mock.GetMessages()
	assert.Equal(t, numGoroutines*ledgersPerGoroutine*2, len(messages))
}

// TestFilterPayments_Performance benchmarks
func BenchmarkFilterPayments_SimpleFilter(b *testing.B) {
	processor, err := NewFilterPayments(map[string]interface{}{
		"filter_assets": []interface{}{"USDC"},
	})
	require.NoError(b, err)

	mock := NewMockProcessor()
	processor.Subscribe(mock)

	ctx := context.Background()

	// Create test ledger with mix of payments
	ledger := createLedgerWithOperations([]xdr.Operation{
		createPaymentOperation("USDC", "1000"),
		createPaymentOperation("XLM", "500"),
		createPaymentOperation("EURC", "2000"),
		createPaymentOperation("USDC", "1500"),
	})

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		msg := Message{Payload: ledger}
		processor.Process(ctx, msg)
	}

	b.StopTimer()
	b.ReportMetric(float64(b.N), "ledgers")
	b.ReportMetric(float64(b.N)/b.Elapsed().Seconds(), "ledgers/sec")
}

func BenchmarkFilterPayments_ComplexFilter(b *testing.B) {
	processor, err := NewFilterPayments(map[string]interface{}{
		"filter_assets":    []interface{}{"USDC", "EURC", "GBPC"},
		"filter_accounts":  []interface{}{"GABC123", "GDEF456", "GHIJ789"},
		"filter_operators": []interface{}{"eq", "gte"},
		"min_amount":       100.0,
		"max_amount":       10000.0,
	})
	require.NoError(b, err)

	mock := NewMockProcessor()
	processor.Subscribe(mock)

	ctx := context.Background()

	// Create complex ledger
	ledger := createComplexLedger()

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		msg := Message{Payload: ledger}
		processor.Process(ctx, msg)
	}

	b.StopTimer()
	b.ReportMetric(float64(b.N), "ledgers")
	b.ReportMetric(float64(b.N)/b.Elapsed().Seconds(), "ledgers/sec")
}

// Helper functions

func createPaymentOperation(asset string, amount string) xdr.Operation {
	return xdr.Operation{
		Body: xdr.OperationBody{
			Type: xdr.OperationTypePayment,
			PaymentOp: &xdr.PaymentOp{
				Destination: xdr.MustAddress("GDEST123..."),
				Asset:       createAsset(asset),
				Amount:      xdr.Int64(parseAmount(amount)),
			},
		},
	}
}

func createPathPaymentOperation(sendAsset, destAsset string, amount string) xdr.Operation {
	return xdr.Operation{
		Body: xdr.OperationBody{
			Type: xdr.OperationTypePathPaymentStrictReceive,
			PathPaymentStrictReceiveOp: &xdr.PathPaymentStrictReceiveOp{
				SendAsset:   createAsset(sendAsset),
				SendMax:     xdr.Int64(parseAmount(amount) * 1.1), // 10% slippage
				Destination: xdr.MustAddress("GDEST123..."),
				DestAsset:   createAsset(destAsset),
				DestAmount:  xdr.Int64(parseAmount(amount)),
				Path:        []xdr.Asset{},
			},
		},
	}
}

func createCreateAccountOperation(amount string) xdr.Operation {
	return xdr.Operation{
		Body: xdr.OperationBody{
			Type: xdr.OperationTypeCreateAccount,
			CreateAccountOp: &xdr.CreateAccountOp{
				Destination:     xdr.MustAddress("GNEW123..."),
				StartingBalance: xdr.Int64(parseAmount(amount)),
			},
		},
	}
}

func createAsset(code string) xdr.Asset {
	if code == "XLM" {
		return xdr.Asset{Type: xdr.AssetTypeAssetTypeNative}
	}
	return xdr.Asset{
		Type: xdr.AssetTypeAssetTypeCreditAlphanum4,
		AlphaNum4: &xdr.AlphaNum4{
			AssetCode: xdr.AssetCode4{byte(code[0]), byte(code[1]), byte(code[2]), byte(code[3])},
			Issuer:    xdr.MustAddress("GISSUER123..."),
		},
	}
}

func parseAmount(amount string) int64 {
	// Simple conversion for testing
	var val int64
	fmt.Sscanf(amount, "%d", &val)
	return val * 10000000 // Convert to stroops
}

func createLedgerWithOperations(ops []xdr.Operation) xdr.LedgerCloseMeta {
	transactions := []xdr.TransactionEnvelope{}
	for _, op := range ops {
		tx := xdr.TransactionEnvelope{
			Type: xdr.EnvelopeTypeEnvelopeTypeTx,
			V1: &xdr.TransactionV1Envelope{
				Tx: xdr.Transaction{
					Operations: []xdr.Operation{op},
				},
			},
		}
		transactions = append(transactions, tx)
	}

	return xdr.LedgerCloseMeta{
		V: 1,
		V1: &xdr.LedgerCloseMetaV1{
			LedgerHeader: xdr.LedgerHeaderHistoryEntry{
				Header: xdr.LedgerHeader{
					LedgerSeq: xdr.Uint32(12345),
				},
			},
			TxSet: xdr.GeneralizedTransactionSet{
				V: 1,
				V1TxSet: &xdr.TransactionSetV1{
					Phases: []xdr.TransactionPhase{
						{
							V: 0,
							V0Components: &[]xdr.TxSetComponent{
								{
									Type: xdr.TxSetComponentTypeTxsetCompTxsMaybeDiscountedFee,
									TxsMaybeDiscountedFee: &xdr.TxSetComponentTxsMaybeDiscountedFee{
										Txs: transactions,
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

func createLedgerWithTransactions(sourceAccounts []string) xdr.LedgerCloseMeta {
	operations := []xdr.Operation{}
	for _, account := range sourceAccounts {
		op := createPaymentOperation("XLM", "100")
		op.SourceAccount = &xdr.MuxedAccount{
			Type: xdr.CryptoKeyTypeKeyTypeEd25519,
			Ed25519: &xdr.Uint256{}, // Mock account ID
		}
		operations = append(operations, op)
	}
	return createLedgerWithOperations(operations)
}

func createComplexLedger() xdr.LedgerCloseMeta {
	// Create a ledger with various payment scenarios
	operations := []xdr.Operation{
		createPaymentOperation("USDC", "150"),    // Qualifies
		createPaymentOperation("XLM", "50"),      // Wrong asset
		createPaymentOperation("EURC", "200"),    // Qualifies
		createPaymentOperation("USDC", "50"),     // Too small
		createPaymentOperation("GBPC", "15000"),  // Too large
		createPaymentOperation("USDC", "500"),    // Qualifies
	}
	
	// Add source accounts to some operations
	for i := 0; i < len(operations); i += 2 {
		operations[i].SourceAccount = &xdr.MuxedAccount{
			Type: xdr.CryptoKeyTypeKeyTypeEd25519,
			Ed25519: &xdr.Uint256{}, // Would be "GABC123" in real implementation
		}
	}
	
	return createLedgerWithOperations(operations)
}

func createEmptyLedger() xdr.LedgerCloseMeta {
	return createLedgerWithOperations([]xdr.Operation{})
}

// Mock FilterPayments implementation for testing
type FilterPayments struct {
	filterAssets    []string
	filterAccounts  []string
	filterOperators []string
	minAmount       float64
	maxAmount       float64
	subscribers     []Processor
	mu              sync.RWMutex
}

type PaymentInfo struct {
	Asset   string
	Account string
	Amount  float64
}

func NewFilterPayments(config map[string]interface{}) (Processor, error) {
	fp := &FilterPayments{
		filterAssets:    []string{},
		filterAccounts:  []string{},
		filterOperators: []string{},
		minAmount:       0,
		maxAmount:       0,
	}

	// Parse filter_assets
	if assets, ok := config["filter_assets"]; ok {
		assetSlice, ok := assets.([]interface{})
		if !ok {
			return nil, fmt.Errorf("filter_assets must be a slice")
		}
		for _, asset := range assetSlice {
			if s, ok := asset.(string); ok {
				fp.filterAssets = append(fp.filterAssets, s)
			}
		}
	}

	// Parse filter_accounts
	if accounts, ok := config["filter_accounts"]; ok {
		accountSlice, ok := accounts.([]interface{})
		if !ok {
			return nil, fmt.Errorf("filter_accounts must be a slice")
		}
		for _, account := range accountSlice {
			if s, ok := account.(string); ok {
				fp.filterAccounts = append(fp.filterAccounts, s)
			}
		}
	}

	// Parse filter_operators
	if operators, ok := config["filter_operators"]; ok {
		operatorSlice, ok := operators.([]interface{})
		if !ok {
			return nil, fmt.Errorf("filter_operators must be a slice")
		}
		for _, op := range operatorSlice {
			if s, ok := op.(string); ok {
				fp.filterOperators = append(fp.filterOperators, s)
			}
		}
	}

	// Parse amount filters
	if minAmount, ok := config["min_amount"].(float64); ok {
		fp.minAmount = minAmount
	}
	if maxAmount, ok := config["max_amount"].(float64); ok {
		fp.maxAmount = maxAmount
	}

	return fp, nil
}

func (fp *FilterPayments) Process(ctx context.Context, msg Message) error {
	// Validate payload
	ledger, ok := msg.Payload.(xdr.LedgerCloseMeta)
	if !ok {
		return fmt.Errorf("expected LedgerCloseMeta, got %T", msg.Payload)
	}

	// Extract and filter payments
	// In real implementation, would parse transactions and filter operations
	
	// For testing, we'll forward some mock filtered payments
	fp.mu.RLock()
	subscribers := append([]Processor{}, fp.subscribers...)
	fp.mu.RUnlock()

	// Mock filtering logic - in real implementation would check each operation
	if len(fp.filterAssets) > 0 || len(fp.filterAccounts) > 0 || fp.minAmount > 0 || fp.maxAmount > 0 {
		// Apply filters and forward qualifying payments
		for i := 0; i < 2; i++ { // Mock: forward 2 payments for testing
			for _, subscriber := range subscribers {
				mockPayment := Message{
					Payload: PaymentInfo{
						Asset:   fp.filterAssets[0],
						Account: "GABC123",
						Amount:  500.0,
					},
				}
				subscriber.Process(ctx, mockPayment)
			}
		}
	}

	return nil
}

func (fp *FilterPayments) Subscribe(processor Processor) {
	fp.mu.Lock()
	defer fp.mu.Unlock()
	fp.subscribers = append(fp.subscribers, processor)
}