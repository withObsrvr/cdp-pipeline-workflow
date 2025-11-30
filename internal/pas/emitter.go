package pas

import (
	"encoding/json"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"
)

// Emitter handles PAS event emission and persistence.
type Emitter struct {
	config       Config
	backupDir    string
	previousHash string
	eventCount   int64
}

// NewEmitter creates a new PAS emitter.
func NewEmitter(cfg Config) (*Emitter, error) {
	cfg.ApplyDefaults()

	if !cfg.Enabled {
		return nil, nil
	}

	// Ensure backup directory exists
	if err := os.MkdirAll(cfg.BackupDir, 0755); err != nil {
		return nil, fmt.Errorf("failed to create PAS backup directory: %w", err)
	}

	emitter := &Emitter{
		config:       cfg,
		backupDir:    cfg.BackupDir,
		previousHash: GenesisHash,
	}

	// Load previous hash from existing events
	if err := emitter.loadPreviousHash(); err != nil {
		log.Printf("[pas] Warning: failed to load previous hash: %v (starting fresh chain)", err)
		emitter.previousHash = GenesisHash
	}

	return emitter, nil
}

// loadPreviousHash finds the most recent event and loads its hash.
func (e *Emitter) loadPreviousHash() error {
	entries, err := os.ReadDir(e.backupDir)
	if err != nil {
		if os.IsNotExist(err) {
			return nil // No directory yet, start fresh
		}
		return err
	}

	// Find all PAS event files
	var eventFiles []string
	for _, entry := range entries {
		if !entry.IsDir() && strings.HasPrefix(entry.Name(), "pas_") && strings.HasSuffix(entry.Name(), ".json") {
			eventFiles = append(eventFiles, entry.Name())
		}
	}

	if len(eventFiles) == 0 {
		log.Printf("[pas] No existing events found, starting fresh chain")
		return nil
	}

	// Sort by filename (timestamp-based) to get most recent
	sort.Strings(eventFiles)
	latestFile := eventFiles[len(eventFiles)-1]

	// Load the latest event
	path := filepath.Join(e.backupDir, latestFile)
	data, err := os.ReadFile(path)
	if err != nil {
		return fmt.Errorf("failed to read latest event file: %w", err)
	}

	var event Event
	if err := json.Unmarshal(data, &event); err != nil {
		return fmt.Errorf("failed to parse latest event: %w", err)
	}

	e.previousHash = event.EventHash
	e.eventCount = int64(len(eventFiles))
	log.Printf("[pas] Loaded previous hash from %s: %s... (chain length: %d)",
		latestFile, event.EventHash[:16], e.eventCount)

	return nil
}

// GetPreviousHash returns the hash of the previous event.
func (e *Emitter) GetPreviousHash() string {
	return e.previousHash
}

// Emit writes a PAS event to the backup directory and updates the chain.
func (e *Emitter) Emit(event *Event) error {
	// Set previous hash if not already set
	if event.PreviousHash == "" {
		event.PreviousHash = e.previousHash
	}

	// Compute event hash
	event.EventHash = ComputeEventHash(event)

	// Generate filename based on ledger range and timestamp
	filename := fmt.Sprintf("pas_%d_%d_%s.json",
		event.Batch.LedgerStart,
		event.Batch.LedgerEnd,
		event.Timestamp.Format("20060102T150405Z"))

	path := filepath.Join(e.backupDir, filename)

	// Marshal with pretty printing for readability
	data, err := json.MarshalIndent(event, "", "  ")
	if err != nil {
		return fmt.Errorf("failed to marshal PAS event: %w", err)
	}

	// Atomic write: temp file + rename
	tmpPath := path + ".tmp"
	if err := os.WriteFile(tmpPath, data, 0644); err != nil {
		return fmt.Errorf("failed to write PAS event: %w", err)
	}

	if err := os.Rename(tmpPath, path); err != nil {
		os.Remove(tmpPath) // Clean up on failure
		return fmt.Errorf("failed to rename PAS event: %w", err)
	}

	// Update chain state
	e.previousHash = event.EventHash
	e.eventCount++

	log.Printf("[pas] Emitted event: %s (hash: %s..., chain length: %d)",
		filename, event.EventHash[:16], e.eventCount)

	return nil
}

// EmitBatch creates and emits a PAS event for a processed batch.
func (e *Emitter) EmitBatch(
	producer Producer,
	ledgerStart, ledgerEnd uint32,
	ledgerCount int,
	tables []TableSummary,
	manifestHash string,
	totalRows int64,
	processingDurationMs int64,
) error {
	batch := BatchInfo{
		LedgerStart:          ledgerStart,
		LedgerEnd:            ledgerEnd,
		LedgerCount:          ledgerCount,
		Tables:               tables,
		ManifestHash:         manifestHash,
		TotalRows:            totalRows,
		ProcessingDurationMs: processingDurationMs,
	}

	event := NewEvent(e.previousHash, producer, batch)
	return e.Emit(event)
}

// Close performs cleanup (no-op for file emitter, but satisfies interface pattern).
func (e *Emitter) Close() error {
	return nil
}

// GetChainLength returns the number of events in the chain.
func (e *Emitter) GetChainLength() int64 {
	return e.eventCount
}

// ListEvents returns all event files in the backup directory.
func (e *Emitter) ListEvents() ([]string, error) {
	entries, err := os.ReadDir(e.backupDir)
	if err != nil {
		return nil, err
	}

	var eventFiles []string
	for _, entry := range entries {
		if !entry.IsDir() && strings.HasPrefix(entry.Name(), "pas_") && strings.HasSuffix(entry.Name(), ".json") {
			eventFiles = append(eventFiles, entry.Name())
		}
	}

	sort.Strings(eventFiles)
	return eventFiles, nil
}

// LoadEvent loads a specific event from file.
func (e *Emitter) LoadEvent(filename string) (*Event, error) {
	path := filepath.Join(e.backupDir, filename)

	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("failed to read event file: %w", err)
	}

	var event Event
	if err := json.Unmarshal(data, &event); err != nil {
		return nil, fmt.Errorf("failed to parse event: %w", err)
	}

	return &event, nil
}

// VerifyChainIntegrity verifies the hash chain integrity of all events.
func (e *Emitter) VerifyChainIntegrity() error {
	files, err := e.ListEvents()
	if err != nil {
		return err
	}

	if len(files) == 0 {
		return nil // Empty chain is valid
	}

	var previousHash string = GenesisHash

	for i, filename := range files {
		event, err := e.LoadEvent(filename)
		if err != nil {
			return fmt.Errorf("failed to load event %s: %w", filename, err)
		}

		// Verify previous hash link
		if event.PreviousHash != previousHash {
			return fmt.Errorf("chain break at event %d (%s): expected previous_hash %s, got %s",
				i, filename, previousHash, event.PreviousHash)
		}

		// Verify event hash
		if !Verify(event) {
			return fmt.Errorf("invalid event hash at event %d (%s)", i, filename)
		}

		previousHash = event.EventHash
	}

	log.Printf("[pas] Chain integrity verified: %d events", len(files))
	return nil
}

// GetLatestEvent returns the most recent event.
func (e *Emitter) GetLatestEvent() (*Event, error) {
	files, err := e.ListEvents()
	if err != nil {
		return nil, err
	}

	if len(files) == 0 {
		return nil, nil
	}

	return e.LoadEvent(files[len(files)-1])
}

// Stats returns statistics about the PAS chain.
type Stats struct {
	ChainLength     int64     `json:"chain_length"`
	LatestEventHash string    `json:"latest_event_hash,omitempty"`
	LatestTimestamp time.Time `json:"latest_timestamp,omitempty"`
	IsGenesis       bool      `json:"is_genesis"`
}

// GetStats returns current chain statistics.
func (e *Emitter) GetStats() Stats {
	stats := Stats{
		ChainLength: e.eventCount,
		IsGenesis:   e.previousHash == GenesisHash,
	}

	if latest, err := e.GetLatestEvent(); err == nil && latest != nil {
		stats.LatestEventHash = latest.EventHash
		stats.LatestTimestamp = latest.Timestamp
	}

	return stats
}
