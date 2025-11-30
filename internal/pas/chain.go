package pas

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"sort"
)

// ComputeEventHash calculates the SHA256 hash of the event's canonical content.
// The hash is computed over a deterministic JSON representation that excludes
// the EventHash field itself and uses sorted keys for reproducibility.
func ComputeEventHash(e *Event) string {
	// Build canonical representation for hashing
	// We exclude EventHash and use a fixed field order
	canonical := canonicalEvent{
		Version:      e.Version,
		PreviousHash: e.PreviousHash,
		// Note: Timestamp is included for uniqueness but makes events non-deterministic
		// For fully deterministic hashes, consider a sequence number instead
		Timestamp: e.Timestamp.UTC().Format("2006-01-02T15:04:05.000Z"),
		Producer:  canonicalProducerFrom(e.Producer),
		Batch:     canonicalBatchFrom(e.Batch),
	}

	data, err := json.Marshal(canonical)
	if err != nil {
		// Should never happen with our struct
		return ""
	}

	hash := sha256.Sum256(data)
	return hex.EncodeToString(hash[:])
}

// canonicalEvent is the hashable representation of an Event.
type canonicalEvent struct {
	Version      string            `json:"version"`
	PreviousHash string            `json:"previous_hash"`
	Timestamp    string            `json:"timestamp"`
	Producer     canonicalProducer `json:"producer"`
	Batch        canonicalBatch    `json:"batch"`
}

type canonicalProducer struct {
	ID      string `json:"id"`
	Version string `json:"version"`
	Network string `json:"network"`
	EraID   string `json:"era_id"`
}

type canonicalBatch struct {
	LedgerStart          uint32           `json:"ledger_start"`
	LedgerEnd            uint32           `json:"ledger_end"`
	LedgerCount          int              `json:"ledger_count"`
	Tables               []canonicalTable `json:"tables"`
	ManifestHash         string           `json:"manifest_hash"`
	TotalRows            int64            `json:"total_rows"`
	ProcessingDurationMs int64            `json:"processing_duration_ms"`
}

type canonicalTable struct {
	Name     string `json:"name"`
	RowCount int64  `json:"row_count"`
	Checksum string `json:"checksum,omitempty"`
}

func canonicalProducerFrom(p Producer) canonicalProducer {
	return canonicalProducer{
		ID:      p.ID,
		Version: p.Version,
		Network: p.Network,
		EraID:   p.EraID,
	}
}

func canonicalBatchFrom(b BatchInfo) canonicalBatch {
	tables := make([]canonicalTable, len(b.Tables))
	for i, t := range b.Tables {
		tables[i] = canonicalTable{
			Name:     t.Name,
			RowCount: t.RowCount,
			Checksum: t.Checksum,
		}
	}

	// Sort tables by name for determinism
	sort.Slice(tables, func(i, j int) bool {
		return tables[i].Name < tables[j].Name
	})

	return canonicalBatch{
		LedgerStart:          b.LedgerStart,
		LedgerEnd:            b.LedgerEnd,
		LedgerCount:          b.LedgerCount,
		Tables:               tables,
		ManifestHash:         b.ManifestHash,
		TotalRows:            b.TotalRows,
		ProcessingDurationMs: b.ProcessingDurationMs,
	}
}

// Verify checks if the event's hash is correct.
func Verify(e *Event) bool {
	computed := ComputeEventHash(e)
	return computed == e.EventHash
}

// VerifyChain checks if two consecutive events have correct linkage.
func VerifyChain(current, previous *Event) bool {
	return current.PreviousHash == previous.EventHash
}

// HashData computes SHA256 of arbitrary data.
func HashData(data []byte) string {
	hash := sha256.Sum256(data)
	return hex.EncodeToString(hash[:])
}
