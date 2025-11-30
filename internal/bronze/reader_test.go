package bronze

import (
	"context"
	"os"
	"testing"
)

func TestParquetReader_ReadLocalFile(t *testing.T) {
	// Skip if test file doesn't exist
	testFile := "/tmp/bronze-test/part-3-10002.parquet"

	reader, err := NewParquetReader()
	if err != nil {
		t.Fatalf("Failed to create reader: %v", err)
	}
	defer reader.Close()

	ctx := context.Background()

	// First, get info about the parquet file
	info, err := reader.GetParquetInfo(ctx, testFile)
	if err != nil {
		t.Fatalf("Failed to get parquet info: %v", err)
	}

	t.Logf("Parquet file info:")
	t.Logf("  Row count: %d", info.RowCount)
	t.Logf("  Ledger range: %d - %d", info.MinLedger, info.MaxLedger)
	t.Logf("  Bronze version: %s", info.BronzeVersion)
	t.Logf("  Era ID: %s", info.EraID)
	t.Logf("  Network: %s", info.Network)

	// Verify expected values
	if info.BronzeVersion != "v1" {
		t.Errorf("Expected bronze_version 'v1', got '%s'", info.BronzeVersion)
	}
	if info.EraID != "pre_p23" {
		t.Errorf("Expected era_id 'pre_p23', got '%s'", info.EraID)
	}
	if info.Network != "testnet" {
		t.Errorf("Expected network 'testnet', got '%s'", info.Network)
	}

	// Read a small range of ledgers
	startLedger := uint32(3)
	endLedger := uint32(12)

	ledgers, err := reader.ReadLedgersSync(ctx, testFile, startLedger, endLedger)
	if err != nil {
		t.Fatalf("Failed to read ledgers: %v", err)
	}

	expectedCount := int(endLedger - startLedger + 1)
	if len(ledgers) != expectedCount {
		t.Errorf("Expected %d ledgers, got %d", expectedCount, len(ledgers))
	}

	// Verify first ledger
	if len(ledgers) > 0 {
		first := ledgers[0]
		if first.LedgerSequence() != startLedger {
			t.Errorf("First ledger sequence: expected %d, got %d", startLedger, first.LedgerSequence())
		}
		t.Logf("First ledger: sequence=%d, hash=%x", first.LedgerSequence(), first.LedgerHash())
	}

	// Verify last ledger
	if len(ledgers) > 0 {
		last := ledgers[len(ledgers)-1]
		if last.LedgerSequence() != endLedger {
			t.Errorf("Last ledger sequence: expected %d, got %d", endLedger, last.LedgerSequence())
		}
		t.Logf("Last ledger: sequence=%d, hash=%x", last.LedgerSequence(), last.LedgerHash())
	}

	// Verify all ledgers are in sequence
	for i, lcm := range ledgers {
		expectedSeq := startLedger + uint32(i)
		if lcm.LedgerSequence() != expectedSeq {
			t.Errorf("Ledger %d: expected sequence %d, got %d", i, expectedSeq, lcm.LedgerSequence())
		}
	}

	t.Logf("Successfully read and unmarshaled %d ledgers", len(ledgers))
}

func TestParquetReader_ReadFromGCS(t *testing.T) {
	// Skip this test by default - requires GCS credentials (HMAC keys)
	// To run: go test -v -run TestParquetReader_ReadFromGCS ./internal/bronze/... -tags=gcs
	// Or set GCS_ACCESS_KEY_ID and GCS_SECRET_ACCESS_KEY environment variables
	accessKeyID := os.Getenv("GCS_ACCESS_KEY_ID")
	secretAccessKey := os.Getenv("GCS_SECRET_ACCESS_KEY")

	if accessKeyID == "" || secretAccessKey == "" {
		t.Skip("Skipping GCS test: GCS_ACCESS_KEY_ID and GCS_SECRET_ACCESS_KEY not set")
	}

	// Test reading from actual GCS bucket
	gcsPath := "gs://obsrvr-stellar-ledger-data-testnet-data/bronze/testnet/pre_p23/v1/ledgers_lcm_raw/range=3-10002/part-3-10002.parquet"

	reader, err := NewParquetReader()
	if err != nil {
		t.Fatalf("Failed to create reader: %v", err)
	}
	defer reader.Close()

	ctx := context.Background()

	// Configure for GCS with HMAC credentials
	if err := reader.ConfigureGCSWithHMAC(ctx, accessKeyID, secretAccessKey); err != nil {
		t.Fatalf("Failed to configure GCS: %v", err)
	}

	// Get info about the parquet file
	info, err := reader.GetParquetInfo(ctx, gcsPath)
	if err != nil {
		t.Fatalf("Failed to get parquet info from GCS: %v", err)
	}

	t.Logf("GCS Parquet file info:")
	t.Logf("  Row count: %d", info.RowCount)
	t.Logf("  Ledger range: %d - %d", info.MinLedger, info.MaxLedger)
	t.Logf("  Bronze version: %s", info.BronzeVersion)
	t.Logf("  Era ID: %s", info.EraID)
	t.Logf("  Network: %s", info.Network)

	// Read a small range
	ledgers, err := reader.ReadLedgersSync(ctx, gcsPath, 100, 104)
	if err != nil {
		t.Fatalf("Failed to read ledgers from GCS: %v", err)
	}

	if len(ledgers) != 5 {
		t.Errorf("Expected 5 ledgers, got %d", len(ledgers))
	}

	for _, lcm := range ledgers {
		t.Logf("Read ledger %d from GCS", lcm.LedgerSequence())
	}
}

func TestParquetReader_ReadWithChannels(t *testing.T) {
	testFile := "/tmp/bronze-test/part-3-10002.parquet"

	reader, err := NewParquetReader()
	if err != nil {
		t.Fatalf("Failed to create reader: %v", err)
	}
	defer reader.Close()

	ctx := context.Background()

	// Read using channels
	startLedger := uint32(100)
	endLedger := uint32(109)

	ledgerCh, errCh := reader.ReadLedgers(ctx, testFile, startLedger, endLedger)

	count := 0
	for lcm := range ledgerCh {
		count++
		t.Logf("Received ledger %d via channel", lcm.LedgerSequence())
	}

	// Check for errors
	select {
	case err := <-errCh:
		if err != nil {
			t.Fatalf("Error reading ledgers: %v", err)
		}
	default:
	}

	expectedCount := int(endLedger - startLedger + 1)
	if count != expectedCount {
		t.Errorf("Expected %d ledgers via channel, got %d", expectedCount, count)
	}
}
