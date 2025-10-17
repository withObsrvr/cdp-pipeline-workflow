package processor

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestContractEventProcessor_MetadataEmission tests that the processor
// emits correct metadata with each message
func TestContractEventProcessor_MetadataEmission(t *testing.T) {
	// Create processor
	processor, err := NewContractEventProcessor(map[string]interface{}{
		"network_passphrase": "Test SDF Network ; September 2015",
	})
	require.NoError(t, err, "Failed to create processor")

	// Create mock consumer to capture messages
	mockConsumer := &MockProcessor{}
	processor.Subscribe(mockConsumer)

	// Create a test event
	testEvent := &ContractEvent{
		Timestamp:         time.Now(),
		LedgerSequence:    12345,
		TransactionHash:   "test_event_tx_hash",
		ContractID:        "CCEVENT1234567890",
		Type:              "contract",
		EventType:         "transfer",
		InSuccessfulTx:    true,
		EventIndex:        0,
		OperationIndex:    1,
		NetworkPassphrase: "Test SDF Network ; September 2015",
	}

	// Forward to processors
	ctx := context.Background()
	err = processor.forwardToProcessors(ctx, testEvent)
	require.NoError(t, err, "Failed to forward to processors")

	// Verify message was captured
	require.Len(t, mockConsumer.Messages, 1, "Expected exactly 1 message")
	msg := mockConsumer.Messages[0]

	// Verify metadata exists
	require.NotNil(t, msg.Metadata, "Metadata should not be nil")

	// Verify processor_type
	processorType, ok := msg.Metadata["processor_type"].(string)
	require.True(t, ok, "processor_type should be a string")
	assert.Equal(t, string(ProcessorTypeContractEvent), processorType,
		"processor_type should be 'contract_event'")

	// Verify processor_name
	processorName, ok := msg.Metadata["processor_name"].(string)
	require.True(t, ok, "processor_name should be a string")
	assert.Equal(t, "ContractEventProcessor", processorName,
		"processor_name should be 'ContractEventProcessor'")

	// Verify version
	version, ok := msg.Metadata["version"].(string)
	require.True(t, ok, "version should be a string")
	assert.Equal(t, "1.0.0", version, "version should be '1.0.0'")

	// Verify timestamp
	timestamp, ok := msg.Metadata["timestamp"].(time.Time)
	require.True(t, ok, "timestamp should be a time.Time")
	assert.WithinDuration(t, time.Now(), timestamp, 5*time.Second,
		"timestamp should be recent")

	// Verify ledger_sequence
	ledgerSeq, ok := msg.Metadata["ledger_sequence"].(uint32)
	require.True(t, ok, "ledger_sequence should be a uint32")
	assert.Equal(t, uint32(12345), ledgerSeq,
		"ledger_sequence should match event")

	// Verify transaction_hash (specific to events)
	txHash, ok := msg.Metadata["transaction_hash"].(string)
	require.True(t, ok, "transaction_hash should be a string")
	assert.Equal(t, "test_event_tx_hash", txHash,
		"transaction_hash should match event")

	// Verify contract_id (specific to events)
	contractID, ok := msg.Metadata["contract_id"].(string)
	require.True(t, ok, "contract_id should be a string")
	assert.Equal(t, "CCEVENT1234567890", contractID,
		"contract_id should match event")

	// Verify event_type (specific to events)
	eventType, ok := msg.Metadata["event_type"].(string)
	require.True(t, ok, "event_type should be a string")
	assert.Equal(t, "transfer", eventType,
		"event_type should match event")

	// Verify payload is valid JSON
	var event ContractEvent
	err = json.Unmarshal(msg.Payload.([]byte), &event)
	require.NoError(t, err, "Payload should be valid JSON")
	assert.Equal(t, testEvent.ContractID, event.ContractID,
		"Payload should contain correct contract ID")
	assert.Equal(t, testEvent.EventType, event.EventType,
		"Payload should contain correct event type")
}

// TestContractEventProcessor_ProcessorTypeIsValid tests that the
// processor type constant is recognized as valid
func TestContractEventProcessor_ProcessorTypeIsValid(t *testing.T) {
	assert.True(t, ProcessorTypeContractEvent.IsValid(),
		"ProcessorTypeContractEvent should be a valid processor type")
}

// TestContractEventProcessor_AdditionalMetadataFields tests that events
// include additional context fields not present in invocations
func TestContractEventProcessor_AdditionalMetadataFields(t *testing.T) {
	// Create processor
	processor, err := NewContractEventProcessor(map[string]interface{}{
		"network_passphrase": "Test SDF Network ; September 2015",
	})
	require.NoError(t, err)

	// Create mock consumer
	mockConsumer := &MockProcessor{}
	processor.Subscribe(mockConsumer)

	// Create test event with various event types
	testCases := []struct {
		name          string
		eventType     string
		contractID    string
		txHash        string
		ledgerSeq     uint32
	}{
		{
			name:       "transfer event",
			eventType:  "transfer",
			contractID: "CCTRANSFER123",
			txHash:     "tx_transfer_abc",
			ledgerSeq:  1000,
		},
		{
			name:       "mint event",
			eventType:  "mint",
			contractID: "CCMINT456",
			txHash:     "tx_mint_def",
			ledgerSeq:  2000,
		},
		{
			name:       "burn event",
			eventType:  "burn",
			contractID: "CCBURN789",
			txHash:     "tx_burn_ghi",
			ledgerSeq:  3000,
		},
	}

	ctx := context.Background()

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			// Clear previous messages
			mockConsumer.Messages = []Message{}

			event := &ContractEvent{
				Timestamp:         time.Now(),
				LedgerSequence:    tc.ledgerSeq,
				TransactionHash:   tc.txHash,
				ContractID:        tc.contractID,
				Type:              "contract",
				EventType:         tc.eventType,
				InSuccessfulTx:    true,
				EventIndex:        0,
				OperationIndex:    0,
				NetworkPassphrase: "Test SDF Network ; September 2015",
			}

			err = processor.forwardToProcessors(ctx, event)
			require.NoError(t, err)

			require.Len(t, mockConsumer.Messages, 1)
			msg := mockConsumer.Messages[0]

			// Verify event-specific fields
			assert.Equal(t, tc.txHash, msg.Metadata["transaction_hash"],
				"transaction_hash should match")
			assert.Equal(t, tc.contractID, msg.Metadata["contract_id"],
				"contract_id should match")
			assert.Equal(t, tc.eventType, msg.Metadata["event_type"],
				"event_type should match")
			assert.Equal(t, tc.ledgerSeq, msg.Metadata["ledger_sequence"],
				"ledger_sequence should match")
		})
	}
}

// TestContractEventProcessor_MetadataStructure tests that metadata
// matches the ProcessorMetadata structure (base fields)
func TestContractEventProcessor_MetadataStructure(t *testing.T) {
	// Create processor
	processor, err := NewContractEventProcessor(map[string]interface{}{
		"network_passphrase": "Test SDF Network ; September 2015",
	})
	require.NoError(t, err)

	// Create mock consumer
	mockConsumer := &MockProcessor{}
	processor.Subscribe(mockConsumer)

	// Create test event
	testEvent := &ContractEvent{
		Timestamp:         time.Now(),
		LedgerSequence:    99999,
		TransactionHash:   "test_structure_event",
		ContractID:        "CCSTRUCTURE456",
		Type:              "contract",
		EventType:         "test",
		InSuccessfulTx:    true,
		EventIndex:        0,
		OperationIndex:    0,
		NetworkPassphrase: "Test SDF Network ; September 2015",
	}

	// Forward to processors
	ctx := context.Background()
	err = processor.forwardToProcessors(ctx, testEvent)
	require.NoError(t, err)

	// Get message
	require.Len(t, mockConsumer.Messages, 1)
	msg := mockConsumer.Messages[0]

	// Verify all required ProcessorMetadata fields are present
	requiredFields := []string{
		"processor_type",
		"processor_name",
		"version",
		"timestamp",
	}

	for _, field := range requiredFields {
		_, exists := msg.Metadata[field]
		assert.True(t, exists, "Required metadata field '%s' should exist", field)
	}

	// Verify event-specific additional fields
	additionalFields := []string{
		"ledger_sequence",
		"transaction_hash",
		"contract_id",
		"event_type",
	}

	for _, field := range additionalFields {
		_, exists := msg.Metadata[field]
		assert.True(t, exists, "Additional metadata field '%s' should exist", field)
	}

	// Verify we can construct a ProcessorMetadata struct from the metadata
	processorType := ProcessorType(msg.Metadata["processor_type"].(string))
	processorName := msg.Metadata["processor_name"].(string)
	version := msg.Metadata["version"].(string)
	timestamp := msg.Metadata["timestamp"].(time.Time)

	metadata := ProcessorMetadata{
		ProcessorType: processorType,
		ProcessorName: processorName,
		Version:       version,
		Timestamp:     timestamp,
	}

	assert.Equal(t, ProcessorTypeContractEvent, metadata.ProcessorType)
	assert.Equal(t, "ContractEventProcessor", metadata.ProcessorName)
	assert.Equal(t, "1.0.0", metadata.Version)
	assert.WithinDuration(t, time.Now(), metadata.Timestamp, 5*time.Second)
}

// TestContractEventProcessor_MultipleEvents tests that metadata
// is emitted correctly for multiple events
func TestContractEventProcessor_MultipleEvents(t *testing.T) {
	// Create processor
	processor, err := NewContractEventProcessor(map[string]interface{}{
		"network_passphrase": "Test SDF Network ; September 2015",
	})
	require.NoError(t, err)

	// Create mock consumer
	mockConsumer := &MockProcessor{}
	processor.Subscribe(mockConsumer)

	ctx := context.Background()

	// Process multiple events
	eventTypes := []string{"transfer", "mint", "burn", "swap", "deposit"}
	for i := 0; i < len(eventTypes); i++ {
		event := &ContractEvent{
			Timestamp:         time.Now(),
			LedgerSequence:    uint32(5000 + i),
			TransactionHash:   "test_event_" + string(rune(i)),
			ContractID:        "CCEVENT" + string(rune(i)),
			Type:              "contract",
			EventType:         eventTypes[i],
			InSuccessfulTx:    true,
			EventIndex:        i,
			OperationIndex:    0,
			NetworkPassphrase: "Test SDF Network ; September 2015",
		}

		err = processor.forwardToProcessors(ctx, event)
		require.NoError(t, err)
	}

	// Verify all messages have metadata
	require.Len(t, mockConsumer.Messages, len(eventTypes), "Should have %d messages", len(eventTypes))

	for i, msg := range mockConsumer.Messages {
		require.NotNil(t, msg.Metadata, "Message %d should have metadata", i)

		processorType := msg.Metadata["processor_type"].(string)
		assert.Equal(t, string(ProcessorTypeContractEvent), processorType,
			"Message %d should have correct processor_type", i)

		ledgerSeq := msg.Metadata["ledger_sequence"].(uint32)
		assert.Equal(t, uint32(5000+i), ledgerSeq,
			"Message %d should have correct ledger_sequence", i)

		eventType := msg.Metadata["event_type"].(string)
		assert.Equal(t, eventTypes[i], eventType,
			"Message %d should have correct event_type", i)
	}
}

// TestContractEventProcessor_DifferentEventTypes tests common event types
func TestContractEventProcessor_DifferentEventTypes(t *testing.T) {
	processor, err := NewContractEventProcessor(map[string]interface{}{
		"network_passphrase": "Test SDF Network ; September 2015",
	})
	require.NoError(t, err)

	mockConsumer := &MockProcessor{}
	processor.Subscribe(mockConsumer)

	commonEventTypes := []string{
		"transfer",
		"mint",
		"burn",
		"swap",
		"deposit",
		"withdraw",
		"approval",
		"stake",
		"unstake",
		"claim",
	}

	ctx := context.Background()

	for i, eventType := range commonEventTypes {
		mockConsumer.Messages = []Message{} // Clear messages

		event := &ContractEvent{
			Timestamp:         time.Now(),
			LedgerSequence:    uint32(10000 + i),
			TransactionHash:   "tx_" + eventType,
			ContractID:        "CC" + eventType,
			Type:              "contract",
			EventType:         eventType,
			InSuccessfulTx:    true,
			EventIndex:        0,
			OperationIndex:    0,
			NetworkPassphrase: "Test SDF Network ; September 2015",
		}

		err = processor.forwardToProcessors(ctx, event)
		require.NoError(t, err, "Failed to process %s event", eventType)

		require.Len(t, mockConsumer.Messages, 1)
		msg := mockConsumer.Messages[0]

		assert.Equal(t, eventType, msg.Metadata["event_type"],
			"Event type should be %s", eventType)
		assert.Equal(t, string(ProcessorTypeContractEvent), msg.Metadata["processor_type"],
			"Processor type should be contract_event")
	}
}
