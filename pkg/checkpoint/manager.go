package checkpoint

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"sync"
	"time"
)

// Manager handles checkpoint operations for a pipeline
type Manager struct {
	directory    string
	interval     time.Duration
	pipelineID   string
	teamSlug     string
	pipelineName string
	configHash   string
	startTime    time.Time

	// Statistics tracking
	stats Stats
	mu    sync.RWMutex
}

// NewManager creates a new checkpoint manager
//
// The directory should be in the format: /checkpoints/{team_slug}/{pipeline_id}
// This function will create the directory if it doesn't exist
func NewManager(dir, pipelineID, teamSlug, pipelineName string, config interface{}) (*Manager, error) {
	if dir == "" {
		return nil, fmt.Errorf("checkpoint directory cannot be empty")
	}
	if pipelineID == "" {
		return nil, fmt.Errorf("pipeline ID cannot be empty")
	}
	if teamSlug == "" {
		return nil, fmt.Errorf("team slug cannot be empty")
	}

	// Ensure directory exists
	if err := os.MkdirAll(dir, 0700); err != nil {
		return nil, fmt.Errorf("failed to create checkpoint directory: %w", err)
	}

	// Calculate config hash
	configHash := calculateConfigHash(config)

	mgr := &Manager{
		directory:    dir,
		interval:     30 * time.Second, // Default 30 seconds
		pipelineID:   pipelineID,
		teamSlug:     teamSlug,
		pipelineName: pipelineName,
		configHash:   configHash,
		startTime:    time.Now(),
	}

	return mgr, nil
}

// SaveCheckpoint saves the current pipeline state to disk
//
// This operation is best-effort and will not fail the pipeline if it encounters errors.
// Errors are logged but do not propagate to the caller.
func (m *Manager) SaveCheckpoint(ledger uint32) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	checkpoint := Checkpoint{
		Version:              CheckpointVersion,
		PipelineID:           m.pipelineID,
		TeamSlug:             m.teamSlug,
		PipelineName:         m.pipelineName,
		ConfigHash:           m.configHash,
		LastProcessedLedger:  ledger,
		CheckpointTimestamp:  time.Now(),
		Statistics:           &m.stats,
	}

	// Marshal to JSON
	data, err := json.MarshalIndent(checkpoint, "", "  ")
	if err != nil {
		return fmt.Errorf("failed to marshal checkpoint: %w", err)
	}

	// Build checkpoint filename
	filename := fmt.Sprintf("checkpoint-%s-%s-latest.json", m.teamSlug, m.pipelineID)
	filePath := filepath.Join(m.directory, filename)

	// Write atomically
	if err := WriteAtomic(filePath, data); err != nil {
		return fmt.Errorf("failed to write checkpoint: %w", err)
	}

	return nil
}

// LoadCheckpoint loads the most recent checkpoint from disk
//
// Returns the checkpoint if found and valid, or an error if not found or corrupted.
// A missing checkpoint is not considered a fatal error - the pipeline should
// fall back to its configured start_ledger.
func (m *Manager) LoadCheckpoint() (*Checkpoint, error) {
	// Build checkpoint filename
	filename := fmt.Sprintf("checkpoint-%s-%s-latest.json", m.teamSlug, m.pipelineID)
	filePath := filepath.Join(m.directory, filename)

	// Check if checkpoint exists
	if _, err := os.Stat(filePath); os.IsNotExist(err) {
		return nil, fmt.Errorf("no checkpoint found at %s", filePath)
	}

	// Read checkpoint file
	data, err := os.ReadFile(filePath)
	if err != nil {
		return nil, fmt.Errorf("failed to read checkpoint: %w", err)
	}

	// Unmarshal JSON
	var checkpoint Checkpoint
	if err := json.Unmarshal(data, &checkpoint); err != nil {
		return nil, fmt.Errorf("failed to unmarshal checkpoint (possibly corrupted): %w", err)
	}

	// Basic validation
	if checkpoint.Version == "" || checkpoint.LastProcessedLedger == 0 {
		return nil, fmt.Errorf("invalid checkpoint: missing required fields")
	}

	// Check for config changes (warning only, not an error)
	if checkpoint.ConfigHash != m.configHash {
		log.Printf("[WARN] Configuration changed since checkpoint (checkpoint: %s, current: %s)",
			checkpoint.ConfigHash[:8], m.configHash[:8])
	}

	return &checkpoint, nil
}

// StartAutoCheckpoint starts a background goroutine that periodically saves checkpoints
//
// The getState function should return the current ledger being processed.
// The goroutine will run until the context is cancelled.
func (m *Manager) StartAutoCheckpoint(ctx context.Context, getState func() uint32) {
	ticker := time.NewTicker(m.interval)
	defer ticker.Stop()

	log.Printf("[INFO] Starting automatic checkpoint every %s", m.interval)

	for {
		select {
		case <-ctx.Done():
			// Context cancelled, save final checkpoint
			ledger := getState()
			if ledger > 0 {
				log.Printf("[INFO] Saving final checkpoint before shutdown")
				if err := m.SaveCheckpoint(ledger); err != nil {
					log.Printf("[ERROR] Failed to save final checkpoint: %v", err)
				} else {
					log.Printf("[INFO] Final checkpoint saved: ledger %d", ledger)
				}
			}
			return

		case <-ticker.C:
			// Periodic checkpoint
			ledger := getState()
			if ledger > 0 {
				if err := m.SaveCheckpoint(ledger); err != nil {
					log.Printf("[ERROR] Failed to save checkpoint: %v (continuing processing)", err)
				} else {
					log.Printf("[INFO] Checkpoint saved: ledger %d", ledger)
				}
			}
		}
	}
}

// UpdateStats updates the checkpoint statistics
//
// This should be called by the source adapter as it processes data
func (m *Manager) UpdateStats(totalProcessed, totalErrors uint64) {
	m.mu.Lock()
	defer m.mu.Unlock()

	m.stats.TotalProcessed = totalProcessed
	m.stats.TotalErrors = totalErrors
	m.stats.UptimeSeconds = int64(time.Since(m.startTime).Seconds())
}

// calculateConfigHash computes a hash of the configuration for change detection
func calculateConfigHash(config interface{}) string {
	if config == nil {
		return "no-config"
	}

	// Marshal config to JSON for consistent hashing
	data, err := json.Marshal(config)
	if err != nil {
		return "hash-error"
	}

	// Compute SHA256 hash
	hash := sha256.Sum256(data)

	// Return first 16 characters (8 bytes) as hex string
	return hex.EncodeToString(hash[:8])
}
