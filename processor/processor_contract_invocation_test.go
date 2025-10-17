package processor

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// MockProcessor is a test processor that captures messages for verification
type MockProcessor struct {
	ProcessFunc func(ctx context.Context, msg Message) error
	Messages    []Message
}

func (m *MockProcessor) Process(ctx context.Context, msg Message) error {
	m.Messages = append(m.Messages, msg)
	if m.ProcessFunc != nil {
		return m.ProcessFunc(ctx, msg)
	}
	return nil
}

func (m *MockProcessor) Subscribe(processor Processor) {
	// No-op for mock
}

// TestContractInvocationProcessor_MetadataEmission tests that the processor
// emits correct metadata with each message
func TestContractInvocationProcessor_MetadataEmission(t *testing.T) {
	// Create processor
	processor, err := NewContractInvocationProcessor(map[string]interface{}{
		"network_passphrase": "Test SDF Network ; September 2015",
	})
	require.NoError(t, err, "Failed to create processor")

	// Create mock consumer to capture messages
	mockConsumer := &MockProcessor{}
	processor.Subscribe(mockConsumer)

	// Create a test invocation
	testInvocation := &ContractInvocation{
		LedgerSequence:  12345,
		TransactionHash: "test_tx_hash",
		ContractID:      "CCTEST1234567890",
		FunctionName:    "transfer",
		Successful:      true,
		ArchiveMetadata: &ArchiveSourceMetadata{
			SourceType:   "S3",
			BucketName:   "test-bucket",
			FilePath:     "ledger-00012345.xdr.gz",
			StartLedger:  12345,
			EndLedger:    12345,
			ProcessedAt:  time.Now(),
			FullCloudURL: "s3://test-bucket/ledger-00012345.xdr.gz",
		},
	}

	// Forward to processors (this is what triggers metadata emission)
	ctx := context.Background()
	err = processor.forwardToProcessors(ctx, testInvocation)
	require.NoError(t, err, "Failed to forward to processors")

	// Verify message was captured
	require.Len(t, mockConsumer.Messages, 1, "Expected exactly 1 message")
	msg := mockConsumer.Messages[0]

	// Verify metadata exists
	require.NotNil(t, msg.Metadata, "Metadata should not be nil")

	// Verify processor_type
	processorType, ok := msg.Metadata["processor_type"].(string)
	require.True(t, ok, "processor_type should be a string")
	assert.Equal(t, string(ProcessorTypeContractInvocation), processorType,
		"processor_type should be 'contract_invocation'")

	// Verify processor_name
	processorName, ok := msg.Metadata["processor_name"].(string)
	require.True(t, ok, "processor_name should be a string")
	assert.Equal(t, "ContractInvocationProcessor", processorName,
		"processor_name should be 'ContractInvocationProcessor'")

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
		"ledger_sequence should match invocation")

	// Verify archive_source is preserved
	archiveSource, ok := msg.Metadata["archive_source"]
	require.True(t, ok, "archive_source should be present")
	assert.NotNil(t, archiveSource, "archive_source should not be nil")

	// Verify payload is valid JSON
	var invocation ContractInvocation
	err = json.Unmarshal(msg.Payload.([]byte), &invocation)
	require.NoError(t, err, "Payload should be valid JSON")
	assert.Equal(t, testInvocation.ContractID, invocation.ContractID,
		"Payload should contain correct contract ID")
}

// TestContractInvocationProcessor_MetadataWithoutArchiveSource tests that
// metadata emission works even when ArchiveMetadata is nil
func TestContractInvocationProcessor_MetadataWithoutArchiveSource(t *testing.T) {
	// Create processor
	processor, err := NewContractInvocationProcessor(map[string]interface{}{
		"network_passphrase": "Test SDF Network ; September 2015",
	})
	require.NoError(t, err, "Failed to create processor")

	// Create mock consumer
	mockConsumer := &MockProcessor{}
	processor.Subscribe(mockConsumer)

	// Create invocation WITHOUT archive metadata
	testInvocation := &ContractInvocation{
		LedgerSequence:  54321,
		TransactionHash: "test_tx_hash_2",
		ContractID:      "CCTEST9876543210",
		FunctionName:    "mint",
		Successful:      true,
		ArchiveMetadata: nil, // No archive metadata
	}

	// Forward to processors
	ctx := context.Background()
	err = processor.forwardToProcessors(ctx, testInvocation)
	require.NoError(t, err, "Failed to forward to processors")

	// Verify message was captured
	require.Len(t, mockConsumer.Messages, 1, "Expected exactly 1 message")
	msg := mockConsumer.Messages[0]

	// Verify basic metadata exists
	require.NotNil(t, msg.Metadata, "Metadata should not be nil")

	// Verify processor_type is still present
	processorType, ok := msg.Metadata["processor_type"].(string)
	require.True(t, ok, "processor_type should be a string")
	assert.Equal(t, string(ProcessorTypeContractInvocation), processorType)

	// Verify ledger_sequence
	ledgerSeq, ok := msg.Metadata["ledger_sequence"].(uint32)
	require.True(t, ok, "ledger_sequence should be a uint32")
	assert.Equal(t, uint32(54321), ledgerSeq)

	// Verify archive_source is NOT present (since it was nil)
	_, hasArchiveSource := msg.Metadata["archive_source"]
	assert.False(t, hasArchiveSource,
		"archive_source should not be present when ArchiveMetadata is nil")
}

// TestContractInvocationProcessor_ProcessorTypeIsValid tests that the
// processor type constant is recognized as valid
func TestContractInvocationProcessor_ProcessorTypeIsValid(t *testing.T) {
	assert.True(t, ProcessorTypeContractInvocation.IsValid(),
		"ProcessorTypeContractInvocation should be a valid processor type")
}

// TestContractInvocationProcessor_MetadataStructure tests that metadata
// matches the ProcessorMetadata structure
func TestContractInvocationProcessor_MetadataStructure(t *testing.T) {
	// Create processor
	processor, err := NewContractInvocationProcessor(map[string]interface{}{
		"network_passphrase": "Test SDF Network ; September 2015",
	})
	require.NoError(t, err)

	// Create mock consumer
	mockConsumer := &MockProcessor{}
	processor.Subscribe(mockConsumer)

	// Create test invocation
	testInvocation := &ContractInvocation{
		LedgerSequence:  99999,
		TransactionHash: "test_structure",
		ContractID:      "CCSTRUCTURE123",
		FunctionName:    "test",
		Successful:      true,
	}

	// Forward to processors
	ctx := context.Background()
	err = processor.forwardToProcessors(ctx, testInvocation)
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

	assert.Equal(t, ProcessorTypeContractInvocation, metadata.ProcessorType)
	assert.Equal(t, "ContractInvocationProcessor", metadata.ProcessorName)
	assert.Equal(t, "1.0.0", metadata.Version)
	assert.WithinDuration(t, time.Now(), metadata.Timestamp, 5*time.Second)
}

// TestContractInvocationProcessor_MultipleInvocations tests that metadata
// is emitted correctly for multiple invocations
func TestContractInvocationProcessor_MultipleInvocations(t *testing.T) {
	// Create processor
	processor, err := NewContractInvocationProcessor(map[string]interface{}{
		"network_passphrase": "Test SDF Network ; September 2015",
	})
	require.NoError(t, err)

	// Create mock consumer
	mockConsumer := &MockProcessor{}
	processor.Subscribe(mockConsumer)

	ctx := context.Background()

	// Process multiple invocations
	for i := 0; i < 5; i++ {
		invocation := &ContractInvocation{
			LedgerSequence:  uint32(1000 + i),
			TransactionHash: "test_tx_" + string(rune(i)),
			ContractID:      "CCTEST" + string(rune(i)),
			FunctionName:    "transfer",
			Successful:      true,
		}

		err = processor.forwardToProcessors(ctx, invocation)
		require.NoError(t, err)
	}

	// Verify all messages have metadata
	require.Len(t, mockConsumer.Messages, 5, "Should have 5 messages")

	for i, msg := range mockConsumer.Messages {
		require.NotNil(t, msg.Metadata, "Message %d should have metadata", i)

		processorType := msg.Metadata["processor_type"].(string)
		assert.Equal(t, string(ProcessorTypeContractInvocation), processorType,
			"Message %d should have correct processor_type", i)

		ledgerSeq := msg.Metadata["ledger_sequence"].(uint32)
		assert.Equal(t, uint32(1000+i), ledgerSeq,
			"Message %d should have correct ledger_sequence", i)
	}
}
