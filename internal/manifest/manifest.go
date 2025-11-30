// Package manifest provides batch metadata generation for audit and provenance.
// Each flush generates a manifest file with checksums, row counts, and metadata
// that can be used for data verification and lineage tracking.
package manifest

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"time"
)

// Manifest represents the metadata for a batch of processed ledgers.
type Manifest struct {
	// Version of the manifest schema
	Version string `json:"version"`

	// GeneratedAt is when this manifest was created
	GeneratedAt time.Time `json:"generated_at"`

	// ProcessorVersion identifies the copier version
	ProcessorVersion string `json:"processor_version"`

	// LedgerRange describes the ledgers in this batch
	LedgerRange LedgerRange `json:"ledger_range"`

	// Tables contains metadata for each table written
	Tables []TableManifest `json:"tables"`

	// TotalRows is the sum of all table row counts
	TotalRows int64 `json:"total_rows"`

	// TotalBytes is the sum of all table byte sizes
	TotalBytes int64 `json:"total_bytes"`

	// ManifestChecksum is SHA256 of the manifest content (excluding this field)
	ManifestChecksum string `json:"manifest_checksum"`
}

// LedgerRange describes the ledger sequence range in a batch.
type LedgerRange struct {
	Start uint32 `json:"start"`
	End   uint32 `json:"end"`
	Count int    `json:"count"`
}

// TableManifest describes the output for a single table.
type TableManifest struct {
	// Name is the table name (e.g., "ledgers_row_v2")
	Name string `json:"name"`

	// RowCount is the number of rows written
	RowCount int64 `json:"row_count"`

	// ByteSize is the approximate size in bytes (0 if unknown)
	ByteSize int64 `json:"byte_size,omitempty"`

	// Checksum is SHA256 of row data (optional, for deterministic verification)
	Checksum string `json:"checksum,omitempty"`
}

// Config holds manifest configuration.
type Config struct {
	Enabled bool   `yaml:"enabled"`
	Dir     string `yaml:"dir"` // If empty, uses ducklake data_path/_manifests/
}

// Builder creates manifests for batches.
type Builder struct {
	processorVersion string
	outputDir        string
}

// NewBuilder creates a new manifest builder.
func NewBuilder(processorVersion, outputDir string) *Builder {
	return &Builder{
		processorVersion: processorVersion,
		outputDir:        outputDir,
	}
}

// Build creates a manifest from batch statistics.
func (b *Builder) Build(
	ledgerStart, ledgerEnd uint32,
	ledgerCount int,
	tableStats map[string]TableStats,
) *Manifest {
	tables := make([]TableManifest, 0, len(tableStats))
	var totalRows, totalBytes int64

	// Build table manifests in deterministic order
	tableOrder := []string{
		"ledgers_row_v2",
		"transactions_row_v2",
		"operations_row_v2",
		"native_balances_snapshot_v1",
		"effects_row_v1",
		"trades_row_v1",
		"accounts_snapshot_v1",
		"trustlines_snapshot_v1",
		"offers_snapshot_v1",
		"claimable_balances_snapshot_v1",
		"liquidity_pools_snapshot_v1",
		"contract_events_stream_v1",
		"contract_data_snapshot_v1",
		"contract_code_snapshot_v1",
		"config_settings_snapshot_v1",
		"ttl_snapshot_v1",
		"evicted_keys_state_v1",
		"restored_keys_state_v1",
		"account_signers_snapshot_v1",
	}

	for _, name := range tableOrder {
		if stats, ok := tableStats[name]; ok {
			tables = append(tables, TableManifest{
				Name:     name,
				RowCount: stats.RowCount,
				ByteSize: stats.ByteSize,
				Checksum: stats.Checksum,
			})
			totalRows += stats.RowCount
			totalBytes += stats.ByteSize
		}
	}

	manifest := &Manifest{
		Version:          "1.0",
		GeneratedAt:      time.Now().UTC(),
		ProcessorVersion: b.processorVersion,
		LedgerRange: LedgerRange{
			Start: ledgerStart,
			End:   ledgerEnd,
			Count: ledgerCount,
		},
		Tables:     tables,
		TotalRows:  totalRows,
		TotalBytes: totalBytes,
	}

	// Compute manifest checksum
	manifest.ManifestChecksum = b.computeChecksum(manifest)

	return manifest
}

// computeChecksum calculates SHA256 of manifest content.
func (b *Builder) computeChecksum(m *Manifest) string {
	// Create a copy without the checksum field for hashing
	hashContent := struct {
		Version          string          `json:"version"`
		GeneratedAt      time.Time       `json:"generated_at"`
		ProcessorVersion string          `json:"processor_version"`
		LedgerRange      LedgerRange     `json:"ledger_range"`
		Tables           []TableManifest `json:"tables"`
		TotalRows        int64           `json:"total_rows"`
		TotalBytes       int64           `json:"total_bytes"`
	}{
		Version:          m.Version,
		GeneratedAt:      m.GeneratedAt,
		ProcessorVersion: m.ProcessorVersion,
		LedgerRange:      m.LedgerRange,
		Tables:           m.Tables,
		TotalRows:        m.TotalRows,
		TotalBytes:       m.TotalBytes,
	}

	data, _ := json.Marshal(hashContent)
	hash := sha256.Sum256(data)
	return hex.EncodeToString(hash[:])
}

// Save writes the manifest to disk.
func (b *Builder) Save(m *Manifest) error {
	// Create manifests directory if needed
	manifestDir := filepath.Join(b.outputDir, "_manifests")
	if err := os.MkdirAll(manifestDir, 0755); err != nil {
		return fmt.Errorf("failed to create manifest directory: %w", err)
	}

	// Generate filename based on ledger range
	filename := fmt.Sprintf("manifest_%d_%d.json", m.LedgerRange.Start, m.LedgerRange.End)
	path := filepath.Join(manifestDir, filename)

	data, err := json.MarshalIndent(m, "", "  ")
	if err != nil {
		return fmt.Errorf("failed to marshal manifest: %w", err)
	}

	// Atomic write
	tmpPath := path + ".tmp"
	if err := os.WriteFile(tmpPath, data, 0644); err != nil {
		return fmt.Errorf("failed to write manifest: %w", err)
	}

	if err := os.Rename(tmpPath, path); err != nil {
		os.Remove(tmpPath)
		return fmt.Errorf("failed to rename manifest: %w", err)
	}

	log.Printf("[manifest] Saved manifest: %s (%d rows across %d tables)",
		filename, m.TotalRows, len(m.Tables))

	return nil
}

// TableStats holds statistics for a single table in a batch.
type TableStats struct {
	RowCount int64
	ByteSize int64
	Checksum string
}

// GetPath returns the path to a manifest file for given ledger range.
func (b *Builder) GetPath(ledgerStart, ledgerEnd uint32) string {
	filename := fmt.Sprintf("manifest_%d_%d.json", ledgerStart, ledgerEnd)
	return filepath.Join(b.outputDir, "_manifests", filename)
}

// Load reads an existing manifest from disk.
func (b *Builder) Load(ledgerStart, ledgerEnd uint32) (*Manifest, error) {
	path := b.GetPath(ledgerStart, ledgerEnd)

	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("failed to read manifest: %w", err)
	}

	var m Manifest
	if err := json.Unmarshal(data, &m); err != nil {
		return nil, fmt.Errorf("failed to parse manifest: %w", err)
	}

	return &m, nil
}
