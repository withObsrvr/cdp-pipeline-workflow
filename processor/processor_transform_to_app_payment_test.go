package processor

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/stellar/go/xdr"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const (
	// stroopsPerXLM is the conversion factor from stroops to XLM
	stroopsPerXLM = 10000000.0
)

// TestNewTransformToAppPayment tests processor creation and configuration
func TestNewTransformToAppPayment(t *testing.T) {
	tests := []struct {
		name           string
		config         map[string]interface{}
		expectedFields map[string]interface{}
		wantErr        bool
	}{
		{
			name:    "default configuration",
			config:  map[string]interface{}{},
			wantErr: false,
		},
		{
			name: "with include fields",
			config: map[string]interface{}{
				"include_memo":        true,
				"include_fee_details": true,
				"include_timebounds":  true,
			},
			expectedFields: map[string]interface{}{
				"include_memo":        true,
				"include_fee_details": true,
				"include_timebounds":  true,
			},
			wantErr: false,
		},
		{
			name: "with field mapping",
			config: map[string]interface{}{
				"field_mapping": map[string]interface{}{
					"source_account": "from_address",
					"destination":    "to_address",
					"amount":         "value",
				},
			},
			expectedFields: map[string]interface{}{
				"has_mapping": true,
			},
			wantErr: false,
		},
		{
			name: "with decimal precision",
			config: map[string]interface{}{
				"decimal_precision": 7,
			},
			expectedFields: map[string]interface{}{
				"decimal_precision": 7,
			},
			wantErr: false,
		},
		{
			name: "with output format",
			config: map[string]interface{}{
				"output_format": "compact",
			},
			expectedFields: map[string]interface{}{
				"output_format": "compact",
			},
			wantErr: false,
		},
		{
			name: "invalid field mapping type",
			config: map[string]interface{}{
				"field_mapping": "not-a-map",
			},
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			processor, err := NewTransformToAppPayment(tt.config)

			if tt.wantErr {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
				assert.NotNil(t, processor)

				// Verify configuration
				transformer := processor.(*TransformToAppPayment)
				
				if tt.expectedFields != nil {
					if includeMemo, ok := tt.expectedFields["include_memo"].(bool); ok {
						assert.Equal(t, includeMemo, transformer.includeMemo)
					}
					if includeFee, ok := tt.expectedFields["include_fee_details"].(bool); ok {
						assert.Equal(t, includeFee, transformer.includeFeeDetails)
					}
					if precision, ok := tt.expectedFields["decimal_precision"].(int); ok {
						assert.Equal(t, precision, transformer.decimalPrecision)
					}
				}
			}
		})
	}
}

// TestTransformToAppPayment_TransformPayment tests payment transformation
func TestTransformToAppPayment_TransformPayment(t *testing.T) {
	t.Run("transforms basic payment", func(t *testing.T) {
		processor, err := NewTransformToAppPayment(map[string]interface{}{})
		require.NoError(t, err)

		mock := NewMockProcessor()
		processor.Subscribe(mock)

		ctx := context.Background()

		// Create payment message
		payment := PaymentMessage{
			SourceAccount: "GSOURCE123...",
			Destination:   "GDEST456...",
			Asset: Asset{
				Code:   "USDC",
				Issuer: "GISSUER789...",
			},
			Amount:         1000000000, // 100 USDC in stroops
			TransactionHash: "abc123def456",
			LedgerSequence: 12345,
			OperationID:    1,
			CreatedAt:      time.Now().Unix(),
		}

		msg := Message{Payload: payment}
		err = processor.Process(ctx, msg)
		assert.NoError(t, err)

		// Verify transformation
		messages := mock.GetMessages()
		require.Len(t, messages, 1)

		// Check output format
		output := messages[0].Payload.(map[string]interface{})
		assert.Equal(t, "GSOURCE123...", output["source_account"])
		assert.Equal(t, "GDEST456...", output["destination"])
		assert.Equal(t, "100.0000000", output["amount"]) // Converted from stroops
		assert.Equal(t, "USDC", output["asset_code"])
		assert.Equal(t, "GISSUER789...", output["asset_issuer"])
		assert.Equal(t, "abc123def456", output["transaction_hash"])
		assert.Equal(t, uint32(12345), output["ledger_sequence"])
	})

	t.Run("transforms native XLM payment", func(t *testing.T) {
		processor, err := NewTransformToAppPayment(map[string]interface{}{})
		require.NoError(t, err)

		mock := NewMockProcessor()
		processor.Subscribe(mock)

		ctx := context.Background()

		// Create XLM payment
		payment := PaymentMessage{
			SourceAccount: "GSOURCE123...",
			Destination:   "GDEST456...",
			Asset: Asset{
				Code: "XLM",
			},
			Amount: 50000000, // 5 XLM
		}

		msg := Message{Payload: payment}
		err = processor.Process(ctx, msg)
		assert.NoError(t, err)

		// Verify transformation
		messages := mock.GetMessages()
		require.Len(t, messages, 1)

		output := messages[0].Payload.(map[string]interface{})
		assert.Equal(t, "5.0000000", output["amount"])
		assert.Equal(t, "XLM", output["asset_code"])
		assert.Equal(t, "native", output["asset_type"])
		assert.Equal(t, "", output["asset_issuer"]) // No issuer for native
	})

	t.Run("includes memo when configured", func(t *testing.T) {
		processor, err := NewTransformToAppPayment(map[string]interface{}{
			"include_memo": true,
		})
		require.NoError(t, err)

		mock := NewMockProcessor()
		processor.Subscribe(mock)

		ctx := context.Background()

		// Create payment with memo
		payment := PaymentMessage{
			SourceAccount: "GSOURCE123...",
			Destination:   "GDEST456...",
			Asset:         Asset{Code: "XLM"},
			Amount:        10000000,
			Memo: &Memo{
				Type:  "text",
				Value: "Payment for invoice #123",
			},
		}

		msg := Message{Payload: payment}
		err = processor.Process(ctx, msg)
		assert.NoError(t, err)

		// Verify memo included
		messages := mock.GetMessages()
		require.Len(t, messages, 1)

		output := messages[0].Payload.(map[string]interface{})
		assert.Contains(t, output, "memo")
		
		memo := output["memo"].(map[string]interface{})
		assert.Equal(t, "text", memo["type"])
		assert.Equal(t, "Payment for invoice #123", memo["value"])
	})

	t.Run("includes fee details when configured", func(t *testing.T) {
		processor, err := NewTransformToAppPayment(map[string]interface{}{
			"include_fee_details": true,
		})
		require.NoError(t, err)

		mock := NewMockProcessor()
		processor.Subscribe(mock)

		ctx := context.Background()

		// Create payment with fee details
		payment := PaymentMessage{
			SourceAccount: "GSOURCE123...",
			Destination:   "GDEST456...",
			Asset:         Asset{Code: "XLM"},
			Amount:        10000000,
			FeeCharged:    100,
			MaxFee:        1000,
		}

		msg := Message{Payload: payment}
		err = processor.Process(ctx, msg)
		assert.NoError(t, err)

		// Verify fee details included
		messages := mock.GetMessages()
		require.Len(t, messages, 1)

		output := messages[0].Payload.(map[string]interface{})
		assert.Contains(t, output, "fee_charged")
		assert.Contains(t, output, "max_fee")
		assert.Equal(t, "0.0000100", output["fee_charged"]) // 100 stroops
		assert.Equal(t, "0.0001000", output["max_fee"])     // 1000 stroops
	})

	t.Run("applies field mapping", func(t *testing.T) {
		processor, err := NewTransformToAppPayment(map[string]interface{}{
			"field_mapping": map[string]interface{}{
				"source_account": "from_address",
				"destination":    "to_address",
				"amount":         "value",
				"asset_code":     "currency",
			},
		})
		require.NoError(t, err)

		mock := NewMockProcessor()
		processor.Subscribe(mock)

		ctx := context.Background()

		// Create payment
		payment := PaymentMessage{
			SourceAccount: "GSOURCE123...",
			Destination:   "GDEST456...",
			Asset:         Asset{Code: "USDC"},
			Amount:        10000000,
		}

		msg := Message{Payload: payment}
		err = processor.Process(ctx, msg)
		assert.NoError(t, err)

		// Verify field mapping applied
		messages := mock.GetMessages()
		require.Len(t, messages, 1)

		output := messages[0].Payload.(map[string]interface{})
		assert.Contains(t, output, "from_address")
		assert.Contains(t, output, "to_address")
		assert.Contains(t, output, "value")
		assert.Contains(t, output, "currency")
		
		assert.Equal(t, "GSOURCE123...", output["from_address"])
		assert.Equal(t, "GDEST456...", output["to_address"])
		assert.Equal(t, "1.0000000", output["value"])
		assert.Equal(t, "USDC", output["currency"])
	})

	t.Run("handles decimal precision", func(t *testing.T) {
		processor, err := NewTransformToAppPayment(map[string]interface{}{
			"decimal_precision": 2,
		})
		require.NoError(t, err)

		mock := NewMockProcessor()
		processor.Subscribe(mock)

		ctx := context.Background()

		// Create payment
		payment := PaymentMessage{
			SourceAccount: "GSOURCE123...",
			Destination:   "GDEST456...",
			Asset:         Asset{Code: "USDC"},
			Amount:        12345678, // 1.2345678 USDC
		}

		msg := Message{Payload: payment}
		err = processor.Process(ctx, msg)
		assert.NoError(t, err)

		// Verify precision applied
		messages := mock.GetMessages()
		require.Len(t, messages, 1)

		output := messages[0].Payload.(map[string]interface{})
		assert.Equal(t, "1.23", output["amount"]) // Rounded to 2 decimal places
	})

	t.Run("handles path payments", func(t *testing.T) {
		processor, err := NewTransformToAppPayment(map[string]interface{}{})
		require.NoError(t, err)

		mock := NewMockProcessor()
		processor.Subscribe(mock)

		ctx := context.Background()

		// Create path payment
		payment := PathPaymentMessage{
			PaymentMessage: PaymentMessage{
				SourceAccount: "GSOURCE123...",
				Destination:   "GDEST456...",
				Asset:         Asset{Code: "USDC"},
				Amount:        10000000,
			},
			SendAsset:   Asset{Code: "XLM"},
			SendAmount:  20000000, // 2 XLM sent
			Path:        []Asset{{Code: "EURC"}},
		}

		msg := Message{Payload: payment}
		err = processor.Process(ctx, msg)
		assert.NoError(t, err)

		// Verify path payment transformation
		messages := mock.GetMessages()
		require.Len(t, messages, 1)

		output := messages[0].Payload.(map[string]interface{})
		assert.Equal(t, "path_payment", output["operation_type"])
		assert.Equal(t, "1.0000000", output["amount"]) // Destination amount
		assert.Equal(t, "USDC", output["asset_code"])
		
		// Path payment specific fields
		assert.Equal(t, "XLM", output["send_asset_code"])
		assert.Equal(t, "2.0000000", output["send_amount"])
		assert.Contains(t, output, "path")
	})

	t.Run("compact output format", func(t *testing.T) {
		processor, err := NewTransformToAppPayment(map[string]interface{}{
			"output_format": "compact",
		})
		require.NoError(t, err)

		mock := NewMockProcessor()
		processor.Subscribe(mock)

		ctx := context.Background()

		// Create payment
		payment := PaymentMessage{
			SourceAccount:   "GSOURCE123...",
			Destination:     "GDEST456...",
			Asset:           Asset{Code: "XLM"},
			Amount:          10000000,
			TransactionHash: "abc123",
			LedgerSequence:  12345,
			OperationID:     1,
		}

		msg := Message{Payload: payment}
		err = processor.Process(ctx, msg)
		assert.NoError(t, err)

		// Verify compact format
		messages := mock.GetMessages()
		require.Len(t, messages, 1)

		output := messages[0].Payload.(map[string]interface{})
		// Compact format should have fewer fields
		assert.Contains(t, output, "from")
		assert.Contains(t, output, "to")
		assert.Contains(t, output, "amount")
		assert.Contains(t, output, "asset")
		assert.NotContains(t, output, "operation_type")
		assert.NotContains(t, output, "asset_issuer") // Excluded in compact for native
	})
}

// TestTransformToAppPayment_ErrorHandling tests error scenarios
func TestTransformToAppPayment_ErrorHandling(t *testing.T) {
	t.Run("handles invalid payload type", func(t *testing.T) {
		processor, err := NewTransformToAppPayment(map[string]interface{}{})
		require.NoError(t, err)

		ctx := context.Background()

		// Send wrong payload type
		msg := Message{
			Payload: "not a payment",
		}

		err = processor.Process(ctx, msg)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "unsupported payload type")
	})

	t.Run("handles missing required fields", func(t *testing.T) {
		processor, err := NewTransformToAppPayment(map[string]interface{}{})
		require.NoError(t, err)

		ctx := context.Background()

		// Payment missing destination
		payment := PaymentMessage{
			SourceAccount: "GSOURCE123...",
			Asset:         Asset{Code: "XLM"},
			Amount:        10000000,
			// Missing Destination
		}

		msg := Message{Payload: payment}
		err = processor.Process(ctx, msg)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "missing required field")
	})

	t.Run("continues on subscriber error", func(t *testing.T) {
		processor, err := NewTransformToAppPayment(map[string]interface{}{})
		require.NoError(t, err)

		// Subscribe processors with different behaviors
		mockFailing := NewMockProcessor()
		mockFailing.errorOnProcess = fmt.Errorf("subscriber error")
		
		mockSuccessful := NewMockProcessor()

		processor.Subscribe(mockFailing)
		processor.Subscribe(mockSuccessful)

		ctx := context.Background()

		// Send valid payment
		payment := PaymentMessage{
			SourceAccount: "GSOURCE123...",
			Destination:   "GDEST456...",
			Asset:         Asset{Code: "XLM"},
			Amount:        10000000,
		}

		msg := Message{Payload: payment}
		err = processor.Process(ctx, msg)
		assert.NoError(t, err) // Should not fail due to subscriber error

		// Verify successful subscriber received message
		assert.Len(t, mockSuccessful.GetMessages(), 1)
	})
}

// TestTransformToAppPayment_JSONSerialization tests JSON output
func TestTransformToAppPayment_JSONSerialization(t *testing.T) {
	processor, err := NewTransformToAppPayment(map[string]interface{}{
		"include_memo":        true,
		"include_fee_details": true,
	})
	require.NoError(t, err)

	mock := NewMockProcessor()
	processor.Subscribe(mock)

	ctx := context.Background()

	// Create comprehensive payment
	payment := PaymentMessage{
		SourceAccount: "GSOURCE123...",
		Destination:   "GDEST456...",
		Asset: Asset{
			Code:   "USDC",
			Issuer: "GISSUER789...",
		},
		Amount:          10000000,
		TransactionHash: "abc123",
		LedgerSequence:  12345,
		OperationID:     1,
		CreatedAt:       1704067200,
		Memo: &Memo{
			Type:  "hash",
			Value: "def456",
		},
		FeeCharged: 100,
		MaxFee:     1000,
		Successful: true,
	}

	msg := Message{Payload: payment}
	err = processor.Process(ctx, msg)
	assert.NoError(t, err)

	// Verify JSON serialization
	messages := mock.GetMessages()
	require.Len(t, messages, 1)

	// Serialize to JSON
	jsonBytes, err := json.Marshal(messages[0].Payload)
	assert.NoError(t, err)

	// Parse back
	var result map[string]interface{}
	err = json.Unmarshal(jsonBytes, &result)
	assert.NoError(t, err)

	// Verify all fields present and correct types
	assert.Equal(t, "GSOURCE123...", result["source_account"])
	assert.Equal(t, "GDEST456...", result["destination"])
	assert.Equal(t, "1.0000000", result["amount"])
	assert.Equal(t, "USDC", result["asset_code"])
	assert.Equal(t, true, result["successful"])
	
	// Verify nested structures
	memo := result["memo"].(map[string]interface{})
	assert.Equal(t, "hash", memo["type"])
	assert.Equal(t, "def456", memo["value"])
}

// TestTransformToAppPayment_Concurrency tests thread safety
func TestTransformToAppPayment_Concurrency(t *testing.T) {
	processor, err := NewTransformToAppPayment(map[string]interface{}{})
	require.NoError(t, err)

	mock := NewMockProcessor()
	processor.Subscribe(mock)

	ctx := context.Background()
	numGoroutines := 10
	paymentsPerGoroutine := 50

	var wg sync.WaitGroup
	for i := 0; i < numGoroutines; i++ {
		wg.Add(1)
		go func(workerID int) {
			defer wg.Done()
			
			for j := 0; j < paymentsPerGoroutine; j++ {
				payment := PaymentMessage{
					SourceAccount: fmt.Sprintf("GSOURCE%d...", workerID),
					Destination:   fmt.Sprintf("GDEST%d...", workerID),
					Asset:         Asset{Code: "XLM"},
					Amount:        int64(1000000 * (j + 1)),
				}
				
				msg := Message{Payload: payment}
				err := processor.Process(ctx, msg)
				assert.NoError(t, err)
			}
		}(i)
	}

	wg.Wait()

	// Verify all payments transformed
	messages := mock.GetMessages()
	assert.Equal(t, numGoroutines*paymentsPerGoroutine, len(messages))
}

// Benchmarks
func BenchmarkTransformToAppPayment_Basic(b *testing.B) {
	processor, err := NewTransformToAppPayment(map[string]interface{}{})
	require.NoError(b, err)

	mock := NewMockProcessor()
	processor.Subscribe(mock)

	ctx := context.Background()

	// Create test payment
	payment := PaymentMessage{
		SourceAccount:   "GSOURCE123...",
		Destination:     "GDEST456...",
		Asset:           Asset{Code: "XLM"},
		Amount:          10000000,
		TransactionHash: "abc123",
		LedgerSequence:  12345,
	}

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		msg := Message{Payload: payment}
		processor.Process(ctx, msg)
	}

	b.StopTimer()
	b.ReportMetric(float64(b.N), "payments")
	b.ReportMetric(float64(b.N)/b.Elapsed().Seconds(), "payments/sec")
}

func BenchmarkTransformToAppPayment_WithAllFields(b *testing.B) {
	processor, err := NewTransformToAppPayment(map[string]interface{}{
		"include_memo":        true,
		"include_fee_details": true,
		"include_timebounds":  true,
		"field_mapping": map[string]interface{}{
			"source_account": "from",
			"destination":    "to",
		},
	})
	require.NoError(b, err)

	mock := NewMockProcessor()
	processor.Subscribe(mock)

	ctx := context.Background()

	// Create comprehensive payment
	payment := PaymentMessage{
		SourceAccount:   "GSOURCE123...",
		Destination:     "GDEST456...",
		Asset:           Asset{Code: "USDC", Issuer: "GISSUER..."},
		Amount:          10000000,
		TransactionHash: "abc123",
		LedgerSequence:  12345,
		Memo:            &Memo{Type: "text", Value: "test"},
		FeeCharged:      100,
		MaxFee:          1000,
	}

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		msg := Message{Payload: payment}
		processor.Process(ctx, msg)
	}

	b.StopTimer()
	b.ReportMetric(float64(b.N), "payments")
	b.ReportMetric(float64(b.N)/b.Elapsed().Seconds(), "payments/sec")
}

// Helper types for testing

type PaymentMessage struct {
	SourceAccount   string
	Destination     string
	Asset           Asset
	Amount          int64
	TransactionHash string
	LedgerSequence  uint32
	OperationID     int32
	CreatedAt       int64
	Memo            *Memo
	FeeCharged      int64
	MaxFee          int64
	Successful      bool
}

type PathPaymentMessage struct {
	PaymentMessage
	SendAsset  Asset
	SendAmount int64
	Path       []Asset
}

type Asset struct {
	Code   string
	Issuer string
}

type Memo struct {
	Type  string
	Value string
}

// Mock TransformToAppPayment implementation
type TransformToAppPayment struct {
	includeMemo        bool
	includeFeeDetails  bool
	includeTimebounds  bool
	fieldMapping       map[string]string
	decimalPrecision   int
	outputFormat       string
	subscribers        []Processor
	mu                 sync.RWMutex
}

func NewTransformToAppPayment(config map[string]interface{}) (Processor, error) {
	t := &TransformToAppPayment{
		fieldMapping:     make(map[string]string),
		decimalPrecision: 7, // Default Stellar precision
		outputFormat:     "standard",
	}

	// Parse configuration
	if includeMemo, ok := config["include_memo"].(bool); ok {
		t.includeMemo = includeMemo
	}
	if includeFee, ok := config["include_fee_details"].(bool); ok {
		t.includeFeeDetails = includeFee
	}
	if includeTime, ok := config["include_timebounds"].(bool); ok {
		t.includeTimebounds = includeTime
	}
	if precision, ok := config["decimal_precision"].(int); ok {
		t.decimalPrecision = precision
	}
	if format, ok := config["output_format"].(string); ok {
		t.outputFormat = format
	}

	// Parse field mapping
	if mapping, ok := config["field_mapping"]; ok {
		mapInterface, ok := mapping.(map[string]interface{})
		if !ok {
			return nil, fmt.Errorf("field_mapping must be a map")
		}
		for k, v := range mapInterface {
			if s, ok := v.(string); ok {
				t.fieldMapping[k] = s
			}
		}
	}

	return t, nil
}

func (t *TransformToAppPayment) Process(ctx context.Context, msg Message) error {
	// Transform based on payload type
	var output map[string]interface{}

	switch p := msg.Payload.(type) {
	case PaymentMessage:
		if p.Destination == "" {
			return fmt.Errorf("missing required field: destination")
		}
		output = t.transformPayment(p)
	case PathPaymentMessage:
		output = t.transformPathPayment(p)
	default:
		return fmt.Errorf("unsupported payload type: %T", msg.Payload)
	}

	// Forward transformed message
	t.mu.RLock()
	subscribers := append([]Processor{}, t.subscribers...)
	t.mu.RUnlock()

	for _, subscriber := range subscribers {
		if err := subscriber.Process(ctx, Message{Payload: output}); err != nil {
			// Log but continue
			continue
		}
	}

	return nil
}

func (t *TransformToAppPayment) transformPayment(p PaymentMessage) map[string]interface{} {
	// Convert amount from stroops
	amountStr := fmt.Sprintf("%.*f", t.decimalPrecision, float64(p.Amount)/stroopsPerXLM)

	output := make(map[string]interface{})

	// Basic fields
	t.setField(output, "source_account", p.SourceAccount)
	t.setField(output, "destination", p.Destination)
	t.setField(output, "amount", amountStr)
	t.setField(output, "asset_code", p.Asset.Code)
	
	if p.Asset.Code == "XLM" {
		t.setField(output, "asset_type", "native")
		t.setField(output, "asset_issuer", "")
	} else {
		t.setField(output, "asset_type", "credit_alphanum")
		t.setField(output, "asset_issuer", p.Asset.Issuer)
	}

	if t.outputFormat == "compact" {
		// Compact format
		output["from"] = p.SourceAccount
		output["to"] = p.Destination
		output["asset"] = p.Asset.Code
		delete(output, "source_account")
		delete(output, "destination")
		delete(output, "asset_code")
		if p.Asset.Code == "XLM" {
			delete(output, "asset_issuer")
		}
	} else {
		// Standard format
		output["operation_type"] = "payment"
		output["transaction_hash"] = p.TransactionHash
		output["ledger_sequence"] = p.LedgerSequence
		output["operation_id"] = p.OperationID
		output["created_at"] = p.CreatedAt
		output["successful"] = p.Successful
	}

	// Optional fields
	if t.includeMemo && p.Memo != nil {
		output["memo"] = map[string]interface{}{
			"type":  p.Memo.Type,
			"value": p.Memo.Value,
		}
	}

	if t.includeFeeDetails {
		output["fee_charged"] = fmt.Sprintf("%.*f", t.decimalPrecision, float64(p.FeeCharged)/stroopsPerXLM)
		output["max_fee"] = fmt.Sprintf("%.*f", t.decimalPrecision, float64(p.MaxFee)/stroopsPerXLM)
	}

	return output
}

func (t *TransformToAppPayment) transformPathPayment(p PathPaymentMessage) map[string]interface{} {
	// Start with basic payment transformation
	output := t.transformPayment(p.PaymentMessage)
	
	// Add path payment specific fields
	output["operation_type"] = "path_payment"
	output["send_asset_code"] = p.SendAsset.Code
	output["send_asset_issuer"] = p.SendAsset.Issuer
	output["send_amount"] = fmt.Sprintf("%.*f", t.decimalPrecision, float64(p.SendAmount)/stroopsPerXLM)
	
	// Convert path
	path := make([]map[string]string, len(p.Path))
	for i, asset := range p.Path {
		path[i] = map[string]string{
			"asset_code":   asset.Code,
			"asset_issuer": asset.Issuer,
		}
	}
	output["path"] = path

	return output
}

func (t *TransformToAppPayment) setField(output map[string]interface{}, field string, value interface{}) {
	// Apply field mapping if configured
	if mapped, ok := t.fieldMapping[field]; ok {
		output[mapped] = value
	} else {
		output[field] = value
	}
}

func (t *TransformToAppPayment) Subscribe(processor Processor) {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.subscribers = append(t.subscribers, processor)
}