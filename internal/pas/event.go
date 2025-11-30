// Package pas provides the Public Audit Stream (PAS) event system.
// PAS events are hash-chained audit records that provide tamper-evident
// proof of data processing, enabling verification of data integrity
// across the entire processing pipeline.
package pas

import (
	"time"
)

// Event represents a PAS v1.1 event.
// Events form a hash chain where each event references the previous one,
// creating an immutable audit trail.
type Event struct {
	// Version of the PAS event schema
	Version string `json:"version"`

	// EventHash is SHA256 of the canonical event content
	EventHash string `json:"event_hash"`

	// PreviousHash links to the prior event in the chain
	PreviousHash string `json:"previous_hash"`

	// Timestamp when the event was generated
	Timestamp time.Time `json:"timestamp"`

	// Producer identifies who generated this event
	Producer Producer `json:"producer"`

	// Batch contains the processed data summary
	Batch BatchInfo `json:"batch"`
}

// Producer identifies the event generator.
type Producer struct {
	// ID is a unique identifier for this producer instance
	ID string `json:"id"`

	// Version of the producer software
	Version string `json:"version"`

	// Network is the Stellar network (mainnet, testnet)
	Network string `json:"network"`

	// EraID identifies the processing era (e.g., "pre_p23", "p23_plus")
	EraID string `json:"era_id"`
}

// BatchInfo summarizes the processed data.
type BatchInfo struct {
	// LedgerStart is the first ledger in the batch
	LedgerStart uint32 `json:"ledger_start"`

	// LedgerEnd is the last ledger in the batch
	LedgerEnd uint32 `json:"ledger_end"`

	// LedgerCount is the number of ledgers processed
	LedgerCount int `json:"ledger_count"`

	// Tables contains per-table summaries
	Tables []TableSummary `json:"tables"`

	// ManifestHash links to the manifest file checksum
	ManifestHash string `json:"manifest_hash"`

	// TotalRows across all tables
	TotalRows int64 `json:"total_rows"`

	// ProcessingDuration in milliseconds
	ProcessingDurationMs int64 `json:"processing_duration_ms"`
}

// TableSummary provides a checksum-verified record of table output.
type TableSummary struct {
	// Name is the table name
	Name string `json:"name"`

	// RowCount is the number of rows written
	RowCount int64 `json:"row_count"`

	// Checksum is SHA256 of the row data (if computed)
	Checksum string `json:"checksum,omitempty"`
}

// NewEvent creates a new PAS event with the given parameters.
// The event hash is computed separately after all fields are set.
func NewEvent(
	previousHash string,
	producer Producer,
	batch BatchInfo,
) *Event {
	return &Event{
		Version:      "1.1",
		PreviousHash: previousHash,
		Timestamp:    time.Now().UTC(),
		Producer:     producer,
		Batch:        batch,
	}
}

// GenesisHash is the previous_hash for the first event in a chain.
const GenesisHash = "0000000000000000000000000000000000000000000000000000000000000000"

// IsGenesis returns true if this is the first event in a chain.
func (e *Event) IsGenesis() bool {
	return e.PreviousHash == GenesisHash
}

// Config holds PAS configuration.
type Config struct {
	// Enabled determines if PAS events are generated
	Enabled bool `yaml:"enabled"`

	// BackupDir is where PAS event files are stored
	BackupDir string `yaml:"backup_dir"`

	// Endpoint is the HTTP endpoint for PAS event submission (optional)
	Endpoint string `yaml:"endpoint"`

	// Strict mode fails the batch if PAS emission fails
	Strict bool `yaml:"strict"`
}

// ApplyDefaults sets default values for PAS config.
func (c *Config) ApplyDefaults() {
	if c.BackupDir == "" {
		c.BackupDir = "./pas-backup"
	}
}
