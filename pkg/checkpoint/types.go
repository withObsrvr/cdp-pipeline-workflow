package checkpoint

import "time"

// Checkpoint represents the saved state of a pipeline at a specific point in time
type Checkpoint struct {
	// Version of the checkpoint format (for future compatibility)
	Version string `json:"version"`

	// Pipeline identification
	PipelineID   string `json:"pipeline_id"`
	TeamSlug     string `json:"team_slug"`
	PipelineName string `json:"pipeline_name"`

	// Configuration hash to detect config changes between runs
	ConfigHash string `json:"config_hash"`

	// Processing state
	LastProcessedLedger uint32 `json:"last_processed_ledger"`

	// Metadata
	CheckpointTimestamp time.Time `json:"checkpoint_timestamp"`

	// Optional statistics
	Statistics *Stats `json:"statistics,omitempty"`
}

// Stats contains optional processing statistics
type Stats struct {
	TotalProcessed uint64 `json:"total_processed"`
	TotalErrors    uint64 `json:"total_errors"`
	UptimeSeconds  int64  `json:"uptime_seconds"`
}

// CheckpointVersion is the current checkpoint format version
const CheckpointVersion = "1.0"
