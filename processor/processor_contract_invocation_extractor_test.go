package processor

import (
	"context"
	"encoding/json"
	"testing"
	"time"
)

func TestContractInvocationExtractor_ExtractCarbonSink(t *testing.T) {
	// Test data based on actual contract invocation
	invocation := &ContractInvocation{
		Timestamp:       time.Date(2025, 6, 23, 14, 24, 23, 0, time.UTC),
		LedgerSequence:  84191,
		TransactionHash: "edd9e4226df7c41432c186a8b7231ead112f6ca6c744e46598453fc5b9d7d1f4",
		ContractID:      "CASJKXVOKEBFC6HRNLLZKMEFJXYS3S5GOXM5DQRD7NDPIOQHCPAOLH7O",
		FunctionName:    "sink_carbon",
		InvokingAccount: "GAN4SL6DHOQO4POKWOUL4PPCIVJBSDX7SVOLL4GVM4CC27S6WCV7FQZL",
		ArgumentsDecoded: map[string]interface{}{
			"arg_0": map[string]interface{}{
				"type":    "account",
				"address": "GAN4SL6DHOQO4POKWOUL4PPCIVJBSDX7SVOLL4GVM4CC27S6WCV7FQZL",
			},
			"arg_1": map[string]interface{}{
				"type":    "account",
				"address": "GAN4SL6DHOQO4POKWOUL4PPCIVJBSDX7SVOLL4GVM4CC27S6WCV7FQZL",
			},
			"arg_2": float64(1000000),
			"arg_3": "VCS1360",
			"arg_4": "first",
			"arg_5": "account@domain.xyz",
		},
		Successful: true,
	}

	// Create extractor with schema
	extractor := &ContractInvocationExtractor{
		extractionSchemas: map[string]ExtractionSchema{
			"CASJKXVOKEBFC6HRNLLZKMEFJXYS3S5GOXM5DQRD7NDPIOQHCPAOLH7O:sink_carbon": {
				SchemaName:   "carbon_sink_v1",
				FunctionName: "sink_carbon",
				ContractIDs:  []string{"CASJKXVOKEBFC6HRNLLZKMEFJXYS3S5GOXM5DQRD7NDPIOQHCPAOLH7O"},
				Extractors: map[string]FieldExtractor{
					"funder": {
						ArgumentIndex: 0,
						FieldPath:     "address",
						FieldType:     "string",
						Required:      true,
					},
					"recipient": {
						ArgumentIndex: 1,
						FieldPath:     "address",
						FieldType:     "string",
						Required:      true,
					},
					"amount": {
						ArgumentIndex: 2,
						FieldPath:     "",
						FieldType:     "uint64",
						Required:      true,
					},
					"project_id": {
						ArgumentIndex: 3,
						FieldPath:     "",
						FieldType:     "string",
						Required:      true,
					},
					"memo_text": {
						ArgumentIndex: 4,
						FieldPath:     "",
						FieldType:     "string",
						Required:      false,
					},
					"email": {
						ArgumentIndex: 5,
						FieldPath:     "",
						FieldType:     "string",
						Required:      false,
					},
				},
			},
		},
	}

	// Extract data
	extracted, err := extractor.extractContractData(invocation)
	if err != nil {
		t.Fatalf("Failed to extract contract data: %v", err)
	}

	// Validate results  
	expectedTOID := (uint64(84191) << 32) | (uint64(0) << 20) | uint64(0)
	if extracted.Toid != expectedTOID {
		t.Errorf("Expected TOID %d, got %d", expectedTOID, extracted.Toid)
	}

	if extracted.Ledger != 84191 {
		t.Errorf("Expected ledger 84191, got %d", extracted.Ledger)
	}

	if extracted.Funder != "GAN4SL6DHOQO4POKWOUL4PPCIVJBSDX7SVOLL4GVM4CC27S6WCV7FQZL" {
		t.Errorf("Expected funder GAN4SL6DHOQO4POKWOUL4PPCIVJBSDX7SVOLL4GVM4CC27S6WCV7FQZL, got %s", extracted.Funder)
	}

	if extracted.Recipient != "GAN4SL6DHOQO4POKWOUL4PPCIVJBSDX7SVOLL4GVM4CC27S6WCV7FQZL" {
		t.Errorf("Expected recipient GAN4SL6DHOQO4POKWOUL4PPCIVJBSDX7SVOLL4GVM4CC27S6WCV7FQZL, got %s", extracted.Recipient)
	}

	if extracted.Amount != 1000000 {
		t.Errorf("Expected amount 1000000, got %d", extracted.Amount)
	}

	if extracted.ProjectID != "VCS1360" {
		t.Errorf("Expected project_id VCS1360, got %s", extracted.ProjectID)
	}

	if extracted.MemoText != "first" {
		t.Errorf("Expected memo_text 'first', got %s", extracted.MemoText)
	}

	if extracted.Email != "account@domain.xyz" {
		t.Errorf("Expected email 'account@domain.xyz', got %s", extracted.Email)
	}

	if extracted.SchemaName != "carbon_sink_v1" {
		t.Errorf("Expected schema_name 'carbon_sink_v1', got %s", extracted.SchemaName)
	}

	if !extracted.Successful {
		t.Errorf("Expected successful true, got %v", extracted.Successful)
	}
}

func TestContractInvocationExtractor_ValidationErrors(t *testing.T) {
	// Test validation failures
	tests := []struct {
		name    string
		field   string
		value   interface{}
		rule    ValidationRule
		wantErr bool
	}{
		{
			name:    "email_invalid_format",
			field:   "email",
			value:   "invalid-email",
			rule:    ValidationRule{Pattern: "^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\\.[a-zA-Z]{2,}$"},
			wantErr: true,
		},
		{
			name:    "email_valid_format",
			field:   "email",
			value:   "test@example.com",
			rule:    ValidationRule{Pattern: "^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\\.[a-zA-Z]{2,}$"},
			wantErr: false,
		},
		{
			name:    "amount_too_small",
			field:   "amount",
			value:   uint64(0),
			rule:    ValidationRule{MinValue: func() *float64 { v := float64(1); return &v }()},
			wantErr: true,
		},
		{
			name:    "amount_valid",
			field:   "amount",
			value:   uint64(1000),
			rule:    ValidationRule{MinValue: func() *float64 { v := float64(1); return &v }()},
			wantErr: false,
		},
		{
			name:    "project_id_empty",
			field:   "project_id",
			value:   "",
			rule:    ValidationRule{MinLength: 1},
			wantErr: true,
		},
		{
			name:    "project_id_valid",
			field:   "project_id",
			value:   "VCS1360",
			rule:    ValidationRule{MinLength: 1},
			wantErr: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			extractor := &ContractInvocationExtractor{}
			extracted := &ExtractedContractInvocation{}

			// Set the field value for testing
			switch tt.field {
			case "email":
				extracted.Email = tt.value.(string)
			case "amount":
				extracted.Amount = tt.value.(uint64)
			case "project_id":
				extracted.ProjectID = tt.value.(string)
			}

			err := extractor.validateField(extracted, tt.field, tt.rule)
			if (err != nil) != tt.wantErr {
				t.Errorf("validateField() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func TestContractInvocationExtractor_Process(t *testing.T) {
	// Test full processing pipeline
	invocation := &ContractInvocation{
		Timestamp:       time.Now(),
		LedgerSequence:  84191,
		TransactionHash: "test_hash",
		ContractID:      "CASJKXVOKEBFC6HRNLLZKMEFJXYS3S5GOXM5DQRD7NDPIOQHCPAOLH7O",
		FunctionName:    "sink_carbon",
		InvokingAccount: "GAN4SL6DHOQO4POKWOUL4PPCIVJBSDX7SVOLL4GVM4CC27S6WCV7FQZL",
		ArgumentsDecoded: map[string]interface{}{
			"arg_0": map[string]interface{}{"address": "GFUNDER"},
			"arg_1": map[string]interface{}{"address": "GRECIPIENT"},
			"arg_2": float64(1000000),
			"arg_3": "VCS1360",
			"arg_4": "test memo",
			"arg_5": "test@example.com",
		},
		Successful: true,
	}

	jsonBytes, err := json.Marshal(invocation)
	if err != nil {
		t.Fatalf("Failed to marshal invocation: %v", err)
	}

	// Create processor with mock consumer
	mockConsumer := &MockConsumer{}
	extractor := &ContractInvocationExtractor{
		processors: []Processor{mockConsumer},
		extractionSchemas: map[string]ExtractionSchema{
			"CASJKXVOKEBFC6HRNLLZKMEFJXYS3S5GOXM5DQRD7NDPIOQHCPAOLH7O:sink_carbon": {
				SchemaName:   "carbon_sink_v1",
				FunctionName: "sink_carbon",
				ContractIDs:  []string{"CASJKXVOKEBFC6HRNLLZKMEFJXYS3S5GOXM5DQRD7NDPIOQHCPAOLH7O"},
				Extractors: map[string]FieldExtractor{
					"funder":     {ArgumentIndex: 0, FieldPath: "address", FieldType: "string", Required: true},
					"recipient":  {ArgumentIndex: 1, FieldPath: "address", FieldType: "string", Required: true},
					"amount":     {ArgumentIndex: 2, FieldPath: "", FieldType: "uint64", Required: true},
					"project_id": {ArgumentIndex: 3, FieldPath: "", FieldType: "string", Required: true},
					"memo_text":  {ArgumentIndex: 4, FieldPath: "", FieldType: "string", Required: false},
					"email":      {ArgumentIndex: 5, FieldPath: "", FieldType: "string", Required: false},
				},
			},
		},
	}

	// Process message
	err = extractor.Process(context.Background(), Message{Payload: jsonBytes})
	if err != nil {
		t.Fatalf("Process() error = %v", err)
	}

	// Verify mock consumer was called
	if !mockConsumer.Called {
		t.Error("Expected mock consumer to be called")
	}

	// Verify extracted data
	if mockConsumer.ReceivedData == nil {
		t.Fatal("Expected mock consumer to receive data")
	}

	var extracted ExtractedContractInvocation
	if err := json.Unmarshal(mockConsumer.ReceivedData, &extracted); err != nil {
		t.Fatalf("Failed to unmarshal extracted data: %v", err)
	}

	if extracted.Funder != "GFUNDER" {
		t.Errorf("Expected funder 'GFUNDER', got %s", extracted.Funder)
	}

	if extracted.Recipient != "GRECIPIENT" {
		t.Errorf("Expected recipient 'GRECIPIENT', got %s", extracted.Recipient)
	}

	if extracted.Amount != 1000000 {
		t.Errorf("Expected amount 1000000, got %d", extracted.Amount)
	}
}

func TestContractInvocationExtractor_SchemaNotFound(t *testing.T) {
	// Test when no matching schema is found
	invocation := &ContractInvocation{
		Timestamp:       time.Now(),
		LedgerSequence:  84191,
		TransactionHash: "test_hash",
		ContractID:      "UNKNOWN_CONTRACT",
		FunctionName:    "unknown_function",
		InvokingAccount: "GAN4SL6DHOQO4POKWOUL4PPCIVJBSDX7SVOLL4GVM4CC27S6WCV7FQZL",
		ArgumentsDecoded: map[string]interface{}{
			"arg_0": "test",
		},
		Successful: true,
	}

	jsonBytes, err := json.Marshal(invocation)
	if err != nil {
		t.Fatalf("Failed to marshal invocation: %v", err)
	}

	// Create processor with no schemas
	extractor := &ContractInvocationExtractor{
		extractionSchemas: map[string]ExtractionSchema{},
	}

	// Process message - should not return error but log it
	err = extractor.Process(context.Background(), Message{Payload: jsonBytes})
	if err != nil {
		t.Errorf("Process() should not return error for missing schema, got: %v", err)
	}

	// Check stats
	stats := extractor.GetStats()
	if stats.ProcessedInvocations != 1 {
		t.Errorf("Expected 1 processed invocation, got %d", stats.ProcessedInvocations)
	}
	if stats.SchemaNotFound != 1 {
		t.Errorf("Expected 1 schema not found, got %d", stats.SchemaNotFound)
	}
	if stats.SuccessfulExtractions != 0 {
		t.Errorf("Expected 0 successful extractions, got %d", stats.SuccessfulExtractions)
	}
}

func TestContractInvocationExtractor_TOIDGeneration(t *testing.T) {
	tests := []struct {
		name     string
		ledger   uint32
		txIdx    uint32
		opIdx    uint32
		expected uint64
	}{
		{
			name:     "simple_case",
			ledger:   84191,
			txIdx:    0,
			opIdx:    0,
			expected: 361850667008, // (84191 << 32)
		},
		{
			name:     "with_tx_index",
			ledger:   100,
			txIdx:    5,
			opIdx:    0,
			expected: 429496734720, // (100 << 32) | (5 << 20)
		},
		{
			name:     "with_op_index",
			ledger:   100,
			txIdx:    0,
			opIdx:    3,
			expected: 429496729603, // (100 << 32) | 3
		},
		{
			name:     "full_case",
			ledger:   100,
			txIdx:    5,
			opIdx:    3,
			expected: 429496734723, // (100 << 32) | (5 << 20) | 3
		},
	}

	extractor := &ContractInvocationExtractor{}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := extractor.generateTOID(tt.ledger, tt.txIdx, tt.opIdx)
			if result != tt.expected {
				t.Errorf("generateTOID(%d, %d, %d) = %d, expected %d",
					tt.ledger, tt.txIdx, tt.opIdx, result, tt.expected)
			}
		})
	}
}

func TestContractInvocationExtractor_FieldTypeConversion(t *testing.T) {
	extractor := &ContractInvocationExtractor{}

	tests := []struct {
		name      string
		value     interface{}
		fieldType string
		expected  interface{}
		wantErr   bool
	}{
		{
			name:      "string_to_string",
			value:     "test",
			fieldType: "string",
			expected:  "test",
			wantErr:   false,
		},
		{
			name:      "number_to_string",
			value:     123,
			fieldType: "string",
			expected:  "123",
			wantErr:   false,
		},
		{
			name:      "float64_to_uint64",
			value:     float64(1000000),
			fieldType: "uint64",
			expected:  uint64(1000000),
			wantErr:   false,
		},
		{
			name:      "invalid_to_uint64",
			value:     "not_a_number",
			fieldType: "uint64",
			expected:  nil,
			wantErr:   true,
		},
		{
			name:      "string_to_address",
			value:     "GAN4SL6DHOQO4POKWOUL4PPCIVJBSDX7SVOLL4GVM4CC27S6WCV7FQZL",
			fieldType: "address",
			expected:  "GAN4SL6DHOQO4POKWOUL4PPCIVJBSDX7SVOLL4GVM4CC27S6WCV7FQZL",
			wantErr:   false,
		},
		{
			name:      "number_to_address",
			value:     123,
			fieldType: "address",
			expected:  nil,
			wantErr:   true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := extractor.convertFieldType(tt.value, tt.fieldType)
			if (err != nil) != tt.wantErr {
				t.Errorf("convertFieldType() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !tt.wantErr && result != tt.expected {
				t.Errorf("convertFieldType() = %v, expected %v", result, tt.expected)
			}
		})
	}
}

func TestContractInvocationExtractor_DefaultValues(t *testing.T) {
	invocation := &ContractInvocation{
		Timestamp:       time.Now(),
		LedgerSequence:  84191,
		TransactionHash: "test_hash",
		ContractID:      "CASJKXVOKEBFC6HRNLLZKMEFJXYS3S5GOXM5DQRD7NDPIOQHCPAOLH7O",
		FunctionName:    "sink_carbon",
		InvokingAccount: "GAN4SL6DHOQO4POKWOUL4PPCIVJBSDX7SVOLL4GVM4CC27S6WCV7FQZL",
		ArgumentsDecoded: map[string]interface{}{
			"arg_0": map[string]interface{}{"address": "GFUNDER"},
			"arg_1": map[string]interface{}{"address": "GRECIPIENT"},
			"arg_2": float64(1000000),
			"arg_3": "VCS1360",
			// arg_4 and arg_5 are missing - should use default values
		},
		Successful: true,
	}

	extractor := &ContractInvocationExtractor{
		extractionSchemas: map[string]ExtractionSchema{
			"CASJKXVOKEBFC6HRNLLZKMEFJXYS3S5GOXM5DQRD7NDPIOQHCPAOLH7O:sink_carbon": {
				SchemaName:   "carbon_sink_v1",
				FunctionName: "sink_carbon",
				ContractIDs:  []string{"CASJKXVOKEBFC6HRNLLZKMEFJXYS3S5GOXM5DQRD7NDPIOQHCPAOLH7O"},
				Extractors: map[string]FieldExtractor{
					"funder":     {ArgumentIndex: 0, FieldPath: "address", FieldType: "string", Required: true},
					"recipient":  {ArgumentIndex: 1, FieldPath: "address", FieldType: "string", Required: true},
					"amount":     {ArgumentIndex: 2, FieldPath: "", FieldType: "uint64", Required: true},
					"project_id": {ArgumentIndex: 3, FieldPath: "", FieldType: "string", Required: true},
					"memo_text":  {ArgumentIndex: 4, FieldPath: "", FieldType: "string", Required: false, DefaultValue: "default_memo"},
					"email":      {ArgumentIndex: 5, FieldPath: "", FieldType: "string", Required: false, DefaultValue: "default@example.com"},
				},
			},
		},
	}

	extracted, err := extractor.extractContractData(invocation)
	if err != nil {
		t.Fatalf("Failed to extract contract data: %v", err)
	}

	if extracted.MemoText != "default_memo" {
		t.Errorf("Expected memo_text 'default_memo', got %s", extracted.MemoText)
	}

	if extracted.Email != "default@example.com" {
		t.Errorf("Expected email 'default@example.com', got %s", extracted.Email)
	}
}

// MockConsumer for testing
type MockConsumer struct {
	Called       bool
	ReceivedData []byte
}

func (m *MockConsumer) Process(ctx context.Context, msg Message) error {
	m.Called = true
	if data, ok := msg.Payload.([]byte); ok {
		m.ReceivedData = data
	}
	return nil
}

func (m *MockConsumer) Subscribe(processor Processor) {
	// No-op for mock
}