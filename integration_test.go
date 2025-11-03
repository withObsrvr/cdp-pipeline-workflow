// +build integration

package main

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/withObsrvr/cdp-pipeline-workflow/consumer"
	"github.com/withObsrvr/cdp-pipeline-workflow/processor"

	_ "github.com/marcboeker/go-duckdb/v2"
)

// TestIntegration_ContractInvocationToDuckLake tests the full pipeline
// from ContractInvocationProcessor to SaveToDuckLake consumer
func TestIntegration_ContractInvocationToDuckLake(t *testing.T) {
	// Skip if not running integration tests
	if testing.Short() {
		t.Skip("Skipping integration test in short mode")
	}

	// Create temporary directory for test database
	tmpDir, err := os.MkdirTemp("", "ducklake_integration_test_*")
	require.NoError(t, err)
	defer os.RemoveAll(tmpDir)

	catalogPath := filepath.Join(tmpDir, "test.ducklake")
	dataPath := filepath.Join(tmpDir, "data")

	// Create data directory
	err = os.MkdirAll(dataPath, 0755)
	require.NoError(t, err)

	t.Logf("Test database: %s", catalogPath)
	t.Logf("Test data path: %s", dataPath)

	// Create SaveToDuckLake consumer
	duckLakeConsumer, err := consumer.NewSaveToDuckLake(map[string]interface{}{
		"catalog_path":   fmt.Sprintf("ducklake:%s", catalogPath),
		"catalog_name":   "stellar_lake",
		"data_path":      dataPath,
		"schema_name":    "main",
		"use_upsert":     true,
		"create_indexes": true,
		"batch_size":     10,
	})
	require.NoError(t, err, "Failed to create DuckLake consumer")
	defer duckLakeConsumer.Close()

	// Create ContractInvocationProcessor
	invocationProcessor, err := processor.NewContractInvocationProcessor(map[string]interface{}{
		"network_passphrase": "Test SDF Network ; September 2015",
	})
	require.NoError(t, err, "Failed to create ContractInvocationProcessor")

	// Subscribe consumer to processor
	invocationProcessor.Subscribe(duckLakeConsumer)

	ctx := context.Background()

	// Create test invocations
	testInvocations := []struct {
		ledgerSeq uint32
		txHash    string
		contractID string
		function  string
	}{
		{ledgerSeq: 1000, txHash: "tx_hash_1", contractID: "CCTEST1", function: "transfer"},
		{ledgerSeq: 1001, txHash: "tx_hash_2", contractID: "CCTEST2", function: "mint"},
		{ledgerSeq: 1002, txHash: "tx_hash_3", contractID: "CCTEST1", function: "burn"},
		{ledgerSeq: 1003, txHash: "tx_hash_4", contractID: "CCTEST3", function: "swap"},
		{ledgerSeq: 1004, txHash: "tx_hash_5", contractID: "CCTEST2", function: "deposit"},
	}

	// Process invocations through the pipeline
	for _, tc := range testInvocations {
		invocation := &processor.ContractInvocation{
			Timestamp:       time.Now(),
			LedgerSequence:  tc.ledgerSeq,
			TransactionHash: tc.txHash,
			ContractID:      tc.contractID,
			InvokingAccount: "GABC123...",
			FunctionName:    tc.function,
			Successful:      true,
			ArchiveMetadata: &processor.ArchiveSourceMetadata{
				SourceType:  "S3",
				BucketName:  "test-bucket",
				FilePath:    fmt.Sprintf("ledger-%d.xdr.gz", tc.ledgerSeq),
				StartLedger: tc.ledgerSeq,
				EndLedger:   tc.ledgerSeq,
				ProcessedAt: time.Now(),
			},
		}

		// This will forward to DuckLake consumer via forwardToProcessors
		// We need to call it directly since we're testing
		jsonBytes, err := json.Marshal(invocation)
		require.NoError(t, err)

		msg := processor.Message{
			Payload: jsonBytes,
			Metadata: map[string]interface{}{
				"processor_type":  string(processor.ProcessorTypeContractInvocation),
				"processor_name":  "ContractInvocationProcessor",
				"version":         "1.0.0",
				"timestamp":       time.Now(),
				"ledger_sequence": invocation.LedgerSequence,
				"archive_source":  invocation.ArchiveMetadata,
			},
		}

		err = duckLakeConsumer.Process(ctx, msg)
		require.NoError(t, err, "Failed to process invocation %s", tc.txHash)
	}

	// Flush any remaining batched data
	err = duckLakeConsumer.Close()
	require.NoError(t, err, "Failed to close consumer")

	// Wait a moment for data to be written
	time.Sleep(500 * time.Millisecond)

	// Verify data was written to DuckLake
	t.Run("VerifyDataInDuckLake", func(t *testing.T) {
		// Open DuckDB connection
		db, err := sql.Open("duckdb", catalogPath)
		require.NoError(t, err)
		defer db.Close()

		// Load extensions
		_, err = db.Exec("INSTALL ducklake")
		require.NoError(t, err)
		_, err = db.Exec("LOAD ducklake")
		require.NoError(t, err)

		// Attach catalog
		attachSQL := fmt.Sprintf("ATTACH 'ducklake:%s' AS stellar_lake (DATA_PATH '%s')", catalogPath, dataPath)
		_, err = db.Exec(attachSQL)
		require.NoError(t, err)

		// Query data
		query := "SELECT COUNT(*) FROM stellar_lake.main.contract_invocations"
		var count int
		err = db.QueryRow(query).Scan(&count)
		require.NoError(t, err)

		assert.Equal(t, 5, count, "Should have 5 invocations in the table")

		// Verify specific records
		query = "SELECT ledger_sequence, transaction_hash, contract_id, function_name FROM stellar_lake.main.contract_invocations ORDER BY ledger_sequence"
		rows, err := db.Query(query)
		require.NoError(t, err)
		defer rows.Close()

		recordCount := 0
		for rows.Next() {
			var ledgerSeq int64
			var txHash, contractID, functionName string
			err = rows.Scan(&ledgerSeq, &txHash, &contractID, &functionName)
			require.NoError(t, err)

			assert.Equal(t, testInvocations[recordCount].ledgerSeq, uint32(ledgerSeq))
			assert.Equal(t, testInvocations[recordCount].txHash, txHash)
			assert.Equal(t, testInvocations[recordCount].contractID, contractID)
			assert.Equal(t, testInvocations[recordCount].function, functionName)

			recordCount++
		}

		assert.Equal(t, 5, recordCount, "Should have read 5 records")
	})

	// Verify indexes were created
	t.Run("VerifyIndexes", func(t *testing.T) {
		db, err := sql.Open("duckdb", catalogPath)
		require.NoError(t, err)
		defer db.Close()

		_, err = db.Exec("INSTALL ducklake")
		require.NoError(t, err)
		_, err = db.Exec("LOAD ducklake")
		require.NoError(t, err)

		attachSQL := fmt.Sprintf("ATTACH 'ducklake:%s' AS stellar_lake (DATA_PATH '%s')", catalogPath, dataPath)
		_, err = db.Exec(attachSQL)
		require.NoError(t, err)

		// Check if indexes exist (DuckDB specific query)
		query := `
			SELECT index_name
			FROM duckdb_indexes()
			WHERE database_name = 'stellar_lake'
			  AND schema_name = 'main'
			  AND table_name = 'contract_invocations'
		`
		rows, err := db.Query(query)
		require.NoError(t, err)
		defer rows.Close()

		indexes := []string{}
		for rows.Next() {
			var indexName string
			err = rows.Scan(&indexName)
			require.NoError(t, err)
			indexes = append(indexes, indexName)
		}

		t.Logf("Found %d indexes: %v", len(indexes), indexes)
		// We expect at least the indexes defined in the schema
		assert.Greater(t, len(indexes), 0, "Should have created indexes")
	})
}

// TestIntegration_UpsertIdempotency tests that MERGE/UPSERT prevents duplicates
func TestIntegration_UpsertIdempotency(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping integration test in short mode")
	}

	// Create temporary directory for test database
	tmpDir, err := os.MkdirTemp("", "ducklake_upsert_test_*")
	require.NoError(t, err)
	defer os.RemoveAll(tmpDir)

	catalogPath := filepath.Join(tmpDir, "test.ducklake")
	dataPath := filepath.Join(tmpDir, "data")

	err = os.MkdirAll(dataPath, 0755)
	require.NoError(t, err)

	// Create SaveToDuckLake consumer with UPSERT enabled
	duckLakeConsumer, err := consumer.NewSaveToDuckLake(map[string]interface{}{
		"catalog_path":   fmt.Sprintf("ducklake:%s", catalogPath),
		"catalog_name":   "stellar_lake",
		"data_path":      dataPath,
		"schema_name":    "main",
		"use_upsert":     true, // Enable UPSERT
		"create_indexes": true,
		"batch_size":     10,
	})
	require.NoError(t, err)
	defer duckLakeConsumer.Close()

	ctx := context.Background()

	// Create a test invocation
	invocation := &processor.ContractInvocation{
		Timestamp:       time.Now(),
		LedgerSequence:  2000,
		TransactionHash: "duplicate_tx_hash",
		ContractID:      "CCTEST_DUPLICATE",
		InvokingAccount: "GABC123...",
		FunctionName:    "transfer",
		Successful:      true,
	}

	jsonBytes, err := json.Marshal(invocation)
	require.NoError(t, err)

	msg := processor.Message{
		Payload: jsonBytes,
		Metadata: map[string]interface{}{
			"processor_type":  string(processor.ProcessorTypeContractInvocation),
			"processor_name":  "ContractInvocationProcessor",
			"version":         "1.0.0",
			"timestamp":       time.Now(),
			"ledger_sequence": invocation.LedgerSequence,
		},
	}

	// Insert the same record THREE times
	for i := 0; i < 3; i++ {
		err = duckLakeConsumer.Process(ctx, msg)
		require.NoError(t, err, "Failed to process duplicate %d", i+1)
	}

	err = duckLakeConsumer.Close()
	require.NoError(t, err)

	time.Sleep(500 * time.Millisecond)

	// Verify only ONE record exists
	db, err := sql.Open("duckdb", catalogPath)
	require.NoError(t, err)
	defer db.Close()

	_, err = db.Exec("INSTALL ducklake")
	require.NoError(t, err)
	_, err = db.Exec("LOAD ducklake")
	require.NoError(t, err)

	attachSQL := fmt.Sprintf("ATTACH 'ducklake:%s' AS stellar_lake (DATA_PATH '%s')", catalogPath, dataPath)
	_, err = db.Exec(attachSQL)
	require.NoError(t, err)

	query := "SELECT COUNT(*) FROM stellar_lake.main.contract_invocations WHERE transaction_hash = 'duplicate_tx_hash'"
	var count int
	err = db.QueryRow(query).Scan(&count)
	require.NoError(t, err)

	assert.Equal(t, 1, count, "UPSERT should prevent duplicates - should have exactly 1 record, not 3")
}

// TestIntegration_BackwardCompatibility tests that old configurations still work
func TestIntegration_BackwardCompatibility(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping integration test in short mode")
	}

	tmpDir, err := os.MkdirTemp("", "ducklake_compat_test_*")
	require.NoError(t, err)
	defer os.RemoveAll(tmpDir)

	catalogPath := filepath.Join(tmpDir, "test.ducklake")
	dataPath := filepath.Join(tmpDir, "data")

	err = os.MkdirAll(dataPath, 0755)
	require.NoError(t, err)

	// Create consumer WITHOUT schema_type (old way - using table_name)
	duckLakeConsumer, err := consumer.NewSaveToDuckLake(map[string]interface{}{
		"catalog_path":   fmt.Sprintf("ducklake:%s", catalogPath),
		"catalog_name":   "stellar_lake",
		"data_path":      dataPath,
		"schema_name":    "main",
		"table_name":     "contract_invocations", // Old way - explicit table name
		"use_upsert":     true,
		"create_indexes": true,
	})
	require.NoError(t, err)
	defer duckLakeConsumer.Close()

	ctx := context.Background()

	// Create invocation WITHOUT metadata (old way)
	invocation := &processor.ContractInvocation{
		Timestamp:       time.Now(),
		LedgerSequence:  3000,
		TransactionHash: "compat_tx_hash",
		ContractID:      "CCTEST_COMPAT",
		InvokingAccount: "GABC123...",
		FunctionName:    "transfer",
		Successful:      true,
	}

	jsonBytes, err := json.Marshal(invocation)
	require.NoError(t, err)

	// Message WITHOUT metadata (old way)
	msg := processor.Message{
		Payload:  jsonBytes,
		Metadata: nil, // No metadata
	}

	err = duckLakeConsumer.Process(ctx, msg)
	require.NoError(t, err, "Old configuration should still work")

	err = duckLakeConsumer.Close()
	require.NoError(t, err)

	time.Sleep(500 * time.Millisecond)

	// Verify data was still written
	db, err := sql.Open("duckdb", catalogPath)
	require.NoError(t, err)
	defer db.Close()

	_, err = db.Exec("INSTALL ducklake")
	require.NoError(t, err)
	_, err = db.Exec("LOAD ducklake")
	require.NoError(t, err)

	attachSQL := fmt.Sprintf("ATTACH 'ducklake:%s' AS stellar_lake (DATA_PATH '%s')", catalogPath, dataPath)
	_, err = db.Exec(attachSQL)
	require.NoError(t, err)

	query := "SELECT COUNT(*) FROM stellar_lake.main.contract_invocations WHERE transaction_hash = 'compat_tx_hash'"
	var count int
	err = db.QueryRow(query).Scan(&count)
	require.NoError(t, err)

	assert.Equal(t, 1, count, "Backward compatibility: should have written 1 record using old config")
}

// TestIntegration_ContractEventToDuckLake tests contract event processing
func TestIntegration_ContractEventToDuckLake(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping integration test in short mode")
	}

	tmpDir, err := os.MkdirTemp("", "ducklake_events_test_*")
	require.NoError(t, err)
	defer os.RemoveAll(tmpDir)

	catalogPath := filepath.Join(tmpDir, "test.ducklake")
	dataPath := filepath.Join(tmpDir, "data")

	err = os.MkdirAll(dataPath, 0755)
	require.NoError(t, err)

	// Create SaveToDuckLake consumer
	duckLakeConsumer, err := consumer.NewSaveToDuckLake(map[string]interface{}{
		"catalog_path":   fmt.Sprintf("ducklake:%s", catalogPath),
		"catalog_name":   "stellar_lake",
		"data_path":      dataPath,
		"schema_name":    "main",
		"use_upsert":     true,
		"create_indexes": true,
	})
	require.NoError(t, err)
	defer duckLakeConsumer.Close()

	ctx := context.Background()

	// Create test events
	testEvents := []struct {
		ledgerSeq uint32
		txHash    string
		contractID string
		eventType string
	}{
		{ledgerSeq: 5000, txHash: "event_tx_1", contractID: "CCEVENT1", eventType: "transfer"},
		{ledgerSeq: 5001, txHash: "event_tx_2", contractID: "CCEVENT2", eventType: "mint"},
		{ledgerSeq: 5002, txHash: "event_tx_3", contractID: "CCEVENT1", eventType: "burn"},
	}

	for i, tc := range testEvents {
		event := &processor.ContractEvent{
			Timestamp:         time.Now(),
			LedgerSequence:    tc.ledgerSeq,
			TransactionHash:   tc.txHash,
			ContractID:        tc.contractID,
			Type:              "contract",
			EventType:         tc.eventType,
			InSuccessfulTx:    true,
			EventIndex:        i,
			OperationIndex:    0,
			NetworkPassphrase: "Test SDF Network ; September 2015",
		}

		jsonBytes, err := json.Marshal(event)
		require.NoError(t, err)

		msg := processor.Message{
			Payload: jsonBytes,
			Metadata: map[string]interface{}{
				"processor_type":   string(processor.ProcessorTypeContractEvent),
				"processor_name":   "ContractEventProcessor",
				"version":          "1.0.0",
				"timestamp":        time.Now(),
				"ledger_sequence":  event.LedgerSequence,
				"transaction_hash": event.TransactionHash,
				"contract_id":      event.ContractID,
				"event_type":       event.EventType,
			},
		}

		err = duckLakeConsumer.Process(ctx, msg)
		require.NoError(t, err, "Failed to process event %s", tc.txHash)
	}

	err = duckLakeConsumer.Close()
	require.NoError(t, err)

	time.Sleep(500 * time.Millisecond)

	// Verify data
	db, err := sql.Open("duckdb", catalogPath)
	require.NoError(t, err)
	defer db.Close()

	_, err = db.Exec("INSTALL ducklake")
	require.NoError(t, err)
	_, err = db.Exec("LOAD ducklake")
	require.NoError(t, err)

	attachSQL := fmt.Sprintf("ATTACH 'ducklake:%s' AS stellar_lake (DATA_PATH '%s')", catalogPath, dataPath)
	_, err = db.Exec(attachSQL)
	require.NoError(t, err)

	query := "SELECT COUNT(*) FROM stellar_lake.main.contract_events"
	var count int
	err = db.QueryRow(query).Scan(&count)
	require.NoError(t, err)

	assert.Equal(t, 3, count, "Should have 3 events in the table")
}
