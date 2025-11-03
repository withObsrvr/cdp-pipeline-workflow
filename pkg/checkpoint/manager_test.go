package checkpoint

import (
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"
)

func TestNewManager(t *testing.T) {
	tmpDir := t.TempDir()

	tests := []struct {
		name        string
		dir         string
		pipelineID  string
		teamSlug    string
		pipelineName string
		wantErr     bool
	}{
		{
			name:         "valid manager creation",
			dir:          filepath.Join(tmpDir, "test1"),
			pipelineID:   "123",
			teamSlug:     "acme-corp",
			pipelineName: "test-pipeline",
			wantErr:      false,
		},
		{
			name:         "empty directory",
			dir:          "",
			pipelineID:   "123",
			teamSlug:     "acme-corp",
			pipelineName: "test",
			wantErr:      true,
		},
		{
			name:         "empty pipeline ID",
			dir:          tmpDir,
			pipelineID:   "",
			teamSlug:     "acme-corp",
			pipelineName: "test",
			wantErr:      true,
		},
		{
			name:         "empty team slug",
			dir:          tmpDir,
			pipelineID:   "123",
			teamSlug:     "",
			pipelineName: "test",
			wantErr:      true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			mgr, err := NewManager(tt.dir, tt.pipelineID, tt.teamSlug, tt.pipelineName, nil)
			if (err != nil) != tt.wantErr {
				t.Errorf("NewManager() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !tt.wantErr && mgr == nil {
				t.Error("NewManager() returned nil manager without error")
			}
		})
	}
}

func TestSaveAndLoadCheckpoint(t *testing.T) {
	tmpDir := t.TempDir()
	checkpointDir := filepath.Join(tmpDir, "acme-corp", "42")

	mgr, err := NewManager(checkpointDir, "42", "acme-corp", "test-pipeline", map[string]string{"test": "config"})
	if err != nil {
		t.Fatalf("NewManager() failed: %v", err)
	}

	// Save a checkpoint
	testLedger := uint32(12345)
	if err := mgr.SaveCheckpoint(testLedger); err != nil {
		t.Fatalf("SaveCheckpoint() failed: %v", err)
	}

	// Verify checkpoint file exists
	filename := "checkpoint-acme-corp-42-latest.json"
	checkpointPath := filepath.Join(checkpointDir, filename)
	if _, err := os.Stat(checkpointPath); os.IsNotExist(err) {
		t.Fatalf("Checkpoint file not created: %s", checkpointPath)
	}

	// Load the checkpoint
	loaded, err := mgr.LoadCheckpoint()
	if err != nil {
		t.Fatalf("LoadCheckpoint() failed: %v", err)
	}

	// Verify checkpoint contents
	if loaded.LastProcessedLedger != testLedger {
		t.Errorf("LastProcessedLedger = %d, want %d", loaded.LastProcessedLedger, testLedger)
	}
	if loaded.PipelineID != "42" {
		t.Errorf("PipelineID = %s, want 42", loaded.PipelineID)
	}
	if loaded.TeamSlug != "acme-corp" {
		t.Errorf("TeamSlug = %s, want acme-corp", loaded.TeamSlug)
	}
	if loaded.Version != CheckpointVersion {
		t.Errorf("Version = %s, want %s", loaded.Version, CheckpointVersion)
	}
}

func TestLoadCheckpoint_NotFound(t *testing.T) {
	tmpDir := t.TempDir()
	checkpointDir := filepath.Join(tmpDir, "test-team", "999")

	mgr, err := NewManager(checkpointDir, "999", "test-team", "test-pipeline", nil)
	if err != nil {
		t.Fatalf("NewManager() failed: %v", err)
	}

	// Try to load non-existent checkpoint
	_, err = mgr.LoadCheckpoint()
	if err == nil {
		t.Error("LoadCheckpoint() should fail when checkpoint doesn't exist")
	}
}

func TestLoadCheckpoint_Corrupted(t *testing.T) {
	tmpDir := t.TempDir()
	checkpointDir := filepath.Join(tmpDir, "test-team", "888")

	mgr, err := NewManager(checkpointDir, "888", "test-team", "test-pipeline", nil)
	if err != nil {
		t.Fatalf("NewManager() failed: %v", err)
	}

	// Write corrupted checkpoint file
	filename := "checkpoint-test-team-888-latest.json"
	checkpointPath := filepath.Join(checkpointDir, filename)
	if err := os.WriteFile(checkpointPath, []byte("corrupted json{{{"), 0600); err != nil {
		t.Fatalf("Failed to write corrupted checkpoint: %v", err)
	}

	// Try to load corrupted checkpoint
	_, err = mgr.LoadCheckpoint()
	if err == nil {
		t.Error("LoadCheckpoint() should fail when checkpoint is corrupted")
	}
}

func TestAtomicWrite(t *testing.T) {
	tmpDir := t.TempDir()
	testFile := filepath.Join(tmpDir, "test.json")

	testData := []byte(`{"test": "data"}`)

	// Test successful write
	if err := WriteAtomic(testFile, testData); err != nil {
		t.Fatalf("WriteAtomic() failed: %v", err)
	}

	// Verify file exists and has correct content
	readData, err := os.ReadFile(testFile)
	if err != nil {
		t.Fatalf("Failed to read written file: %v", err)
	}
	if string(readData) != string(testData) {
		t.Errorf("File content = %s, want %s", readData, testData)
	}

	// Verify temp file was cleaned up
	tmpPath := testFile + ".tmp"
	if _, err := os.Stat(tmpPath); !os.IsNotExist(err) {
		t.Error("Temp file was not cleaned up")
	}
}

func TestAtomicWrite_DirectoryCreation(t *testing.T) {
	tmpDir := t.TempDir()
	nestedDir := filepath.Join(tmpDir, "a", "b", "c")
	testFile := filepath.Join(nestedDir, "test.json")

	testData := []byte(`{"test": "data"}`)

	// WriteAtomic should create the directory structure
	if err := WriteAtomic(testFile, testData); err != nil {
		t.Fatalf("WriteAtomic() failed: %v", err)
	}

	// Verify directory was created
	if _, err := os.Stat(nestedDir); os.IsNotExist(err) {
		t.Error("Directory was not created")
	}

	// Verify file exists
	if _, err := os.Stat(testFile); os.IsNotExist(err) {
		t.Error("File was not created")
	}
}

func TestStartAutoCheckpoint(t *testing.T) {
	tmpDir := t.TempDir()
	checkpointDir := filepath.Join(tmpDir, "test-team", "777")

	mgr, err := NewManager(checkpointDir, "777", "test-team", "auto-test", nil)
	if err != nil {
		t.Fatalf("NewManager() failed: %v", err)
	}

	// Set very short interval for testing
	mgr.interval = 100 * time.Millisecond

	// Track current ledger
	currentLedger := uint32(1000)
	getState := func() uint32 {
		return currentLedger
	}

	// Start auto-checkpoint
	ctx, cancel := context.WithCancel(context.Background())
	go mgr.StartAutoCheckpoint(ctx, getState)

	// Wait for at least one checkpoint
	time.Sleep(200 * time.Millisecond)

	// Increment ledger
	currentLedger = 1500

	// Wait for another checkpoint
	time.Sleep(200 * time.Millisecond)

	// Cancel context
	cancel()

	// Give goroutine time to finish
	time.Sleep(100 * time.Millisecond)

	// Load checkpoint and verify it has the latest ledger
	loaded, err := mgr.LoadCheckpoint()
	if err != nil {
		t.Fatalf("LoadCheckpoint() failed: %v", err)
	}

	if loaded.LastProcessedLedger != currentLedger {
		t.Errorf("LastProcessedLedger = %d, want %d", loaded.LastProcessedLedger, currentLedger)
	}
}

func TestUpdateStats(t *testing.T) {
	tmpDir := t.TempDir()
	checkpointDir := filepath.Join(tmpDir, "test-team", "666")

	mgr, err := NewManager(checkpointDir, "666", "test-team", "stats-test", nil)
	if err != nil {
		t.Fatalf("NewManager() failed: %v", err)
	}

	// Update stats
	mgr.UpdateStats(1000, 5)

	// Save checkpoint
	if err := mgr.SaveCheckpoint(5000); err != nil {
		t.Fatalf("SaveCheckpoint() failed: %v", err)
	}

	// Load and verify stats
	loaded, err := mgr.LoadCheckpoint()
	if err != nil {
		t.Fatalf("LoadCheckpoint() failed: %v", err)
	}

	if loaded.Statistics == nil {
		t.Fatal("Statistics is nil")
	}
	if loaded.Statistics.TotalProcessed != 1000 {
		t.Errorf("TotalProcessed = %d, want 1000", loaded.Statistics.TotalProcessed)
	}
	if loaded.Statistics.TotalErrors != 5 {
		t.Errorf("TotalErrors = %d, want 5", loaded.Statistics.TotalErrors)
	}
	if loaded.Statistics.UptimeSeconds < 0 {
		t.Error("UptimeSeconds should be >= 0")
	}
}

func TestConfigHashChange(t *testing.T) {
	tmpDir := t.TempDir()
	checkpointDir := filepath.Join(tmpDir, "test-team", "555")

	// Create manager with config1
	config1 := map[string]string{"key": "value1"}
	mgr1, err := NewManager(checkpointDir, "555", "test-team", "hash-test", config1)
	if err != nil {
		t.Fatalf("NewManager() failed: %v", err)
	}

	// Save checkpoint
	if err := mgr1.SaveCheckpoint(1000); err != nil {
		t.Fatalf("SaveCheckpoint() failed: %v", err)
	}

	// Create manager with config2 (different config)
	config2 := map[string]string{"key": "value2"}
	mgr2, err := NewManager(checkpointDir, "555", "test-team", "hash-test", config2)
	if err != nil {
		t.Fatalf("NewManager() failed: %v", err)
	}

	// Load checkpoint - should succeed but log warning about config change
	// (We can't test the warning log easily, but we verify it doesn't error)
	loaded, err := mgr2.LoadCheckpoint()
	if err != nil {
		t.Fatalf("LoadCheckpoint() failed: %v", err)
	}

	if loaded == nil {
		t.Error("LoadCheckpoint() returned nil")
	}
}
