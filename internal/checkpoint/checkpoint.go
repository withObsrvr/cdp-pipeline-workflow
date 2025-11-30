// Package checkpoint provides resume capability for the Bronze Copier.
// It persists the last committed ledger sequence so processing can resume
// after crashes or restarts without data loss or duplication.
package checkpoint

import (
	"encoding/json"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"time"
)

// Checkpoint represents the processing state that can be persisted and restored.
type Checkpoint struct {
	// LastCommittedLedger is the sequence of the last successfully flushed ledger
	LastCommittedLedger uint32 `json:"last_committed_ledger"`

	// LastCommittedTime is when the last batch was committed
	LastCommittedTime time.Time `json:"last_committed_time"`

	// BatchNumber is an incrementing counter for audit purposes
	BatchNumber uint64 `json:"batch_number"`

	// SourceMode identifies how data was sourced (datastore, grpc)
	SourceMode string `json:"source_mode"`

	// NetworkPassphrase identifies the Stellar network
	NetworkPassphrase string `json:"network_passphrase"`

	// ProcessorVersion for tracking schema compatibility
	ProcessorVersion string `json:"processor_version"`

	// StartLedger is the original start ledger from config
	StartLedger uint32 `json:"start_ledger"`

	// EndLedger is the target end ledger (0 = tail mode)
	EndLedger uint32 `json:"end_ledger"`
}

// Checkpointer manages checkpoint persistence and recovery.
type Checkpointer struct {
	dir      string
	filename string
	current  *Checkpoint
}

// Config holds checkpoint configuration.
type Config struct {
	Enabled  bool   `yaml:"enabled"`
	Dir      string `yaml:"dir"`
	Filename string `yaml:"filename"`
}

// ApplyDefaults sets default values for checkpoint config.
func (c *Config) ApplyDefaults() {
	if c.Dir == "" {
		c.Dir = "./state"
	}
	if c.Filename == "" {
		c.Filename = "checkpoint.json"
	}
}

// NewCheckpointer creates a new checkpointer.
func NewCheckpointer(cfg Config) (*Checkpointer, error) {
	cfg.ApplyDefaults()

	// Ensure checkpoint directory exists
	if err := os.MkdirAll(cfg.Dir, 0755); err != nil {
		return nil, fmt.Errorf("failed to create checkpoint directory: %w", err)
	}

	return &Checkpointer{
		dir:      cfg.Dir,
		filename: cfg.Filename,
	}, nil
}

// Path returns the full path to the checkpoint file.
func (c *Checkpointer) Path() string {
	return filepath.Join(c.dir, c.filename)
}

// Load loads the checkpoint from disk if it exists.
// Returns nil, nil if no checkpoint file exists (fresh start).
func (c *Checkpointer) Load() (*Checkpoint, error) {
	path := c.Path()

	data, err := os.ReadFile(path)
	if os.IsNotExist(err) {
		log.Printf("[checkpoint] No existing checkpoint found at %s (fresh start)", path)
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("failed to read checkpoint file: %w", err)
	}

	var cp Checkpoint
	if err := json.Unmarshal(data, &cp); err != nil {
		return nil, fmt.Errorf("failed to parse checkpoint file: %w", err)
	}

	c.current = &cp
	log.Printf("[checkpoint] Loaded checkpoint: last_ledger=%d, batch=%d, time=%s",
		cp.LastCommittedLedger, cp.BatchNumber, cp.LastCommittedTime.Format(time.RFC3339))

	return &cp, nil
}

// Save persists the checkpoint to disk atomically.
// Uses temp file + rename to prevent corruption on crash.
func (c *Checkpointer) Save(cp *Checkpoint) error {
	data, err := json.MarshalIndent(cp, "", "  ")
	if err != nil {
		return fmt.Errorf("failed to marshal checkpoint: %w", err)
	}

	path := c.Path()
	tmpPath := path + ".tmp"

	// Write to temp file first
	if err := os.WriteFile(tmpPath, data, 0644); err != nil {
		return fmt.Errorf("failed to write temp checkpoint: %w", err)
	}

	// Atomic rename
	if err := os.Rename(tmpPath, path); err != nil {
		os.Remove(tmpPath) // Clean up temp file on failure
		return fmt.Errorf("failed to rename checkpoint: %w", err)
	}

	c.current = cp
	return nil
}

// Update creates a new checkpoint with updated ledger info and saves it.
func (c *Checkpointer) Update(lastLedger uint32, sourceMode, network, version string, startLedger, endLedger uint32) error {
	batchNum := uint64(1)
	if c.current != nil {
		batchNum = c.current.BatchNumber + 1
	}

	cp := &Checkpoint{
		LastCommittedLedger: lastLedger,
		LastCommittedTime:   time.Now().UTC(),
		BatchNumber:         batchNum,
		SourceMode:          sourceMode,
		NetworkPassphrase:   network,
		ProcessorVersion:    version,
		StartLedger:         startLedger,
		EndLedger:           endLedger,
	}

	if err := c.Save(cp); err != nil {
		return err
	}

	log.Printf("[checkpoint] Saved checkpoint: ledger=%d, batch=%d", lastLedger, batchNum)
	return nil
}

// Current returns the current checkpoint state.
func (c *Checkpointer) Current() *Checkpoint {
	return c.current
}

// GetResumePoint returns the ledger to resume from.
// Returns startLedger if no checkpoint exists, or lastCommitted+1 if resuming.
func (c *Checkpointer) GetResumePoint(configStartLedger uint32) uint32 {
	if c.current == nil {
		return configStartLedger
	}

	// Resume from next ledger after last committed
	resumePoint := c.current.LastCommittedLedger + 1

	// Don't go backwards if config start is higher
	if configStartLedger > resumePoint {
		log.Printf("[checkpoint] Config start ledger %d > checkpoint resume point %d, using config start",
			configStartLedger, resumePoint)
		return configStartLedger
	}

	log.Printf("[checkpoint] Resuming from ledger %d (checkpoint: %d)",
		resumePoint, c.current.LastCommittedLedger)
	return resumePoint
}

// Validate checks if the checkpoint is compatible with current config.
func (c *Checkpointer) Validate(network, sourceMode string) error {
	if c.current == nil {
		return nil // No checkpoint to validate
	}

	if c.current.NetworkPassphrase != network {
		return fmt.Errorf("checkpoint network mismatch: checkpoint=%s, config=%s",
			c.current.NetworkPassphrase, network)
	}

	// Source mode mismatch is a warning, not an error
	if c.current.SourceMode != sourceMode {
		log.Printf("[checkpoint] Warning: source mode changed from %s to %s",
			c.current.SourceMode, sourceMode)
	}

	return nil
}
