package processor

import (
	"context"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"math/big"
	"strconv"
	"time"

	"github.com/stellar/go/hash"
	"github.com/stellar/go/ingest"
	"github.com/stellar/go/xdr"
)

// Processor defines the interface for processing messages.
type Processor interface {
	Process(context.Context, Message) error
	Subscribe(Processor)
}

type ProcessorConfig struct {
	Type   string                 `yaml:"type"`
	Config map[string]interface{} `yaml:"config"`
}

// Message encapsulates the payload to be processed with optional metadata.
type Message struct {
	Payload  interface{}            `json:"payload"`
	Metadata map[string]interface{} `json:"metadata,omitempty"`
}

// ArchiveSourceMetadata contains information about the source archive file
type ArchiveSourceMetadata struct {
	SourceType    string    `json:"source_type"`     // "S3", "GCS", "FS"
	BucketName    string    `json:"bucket_name"`     // for cloud storage
	FilePath      string    `json:"file_path"`       // full file path
	FileName      string    `json:"file_name"`       // just filename
	StartLedger   uint32    `json:"start_ledger"`    // first ledger in file
	EndLedger     uint32    `json:"end_ledger"`      // last ledger in file
	ProcessedAt   time.Time `json:"processed_at"`
	FileSize      int64     `json:"file_size,omitempty"`
	Partition     uint32    `json:"partition,omitempty"`
}

// GetArchiveMetadata extracts archive source metadata from the message
func (m *Message) GetArchiveMetadata() (*ArchiveSourceMetadata, bool) {
	if m.Metadata == nil {
		return nil, false
	}
	
	archiveData, exists := m.Metadata["archive_source"]
	if !exists {
		return nil, false
	}
	
	// Type assertion to ArchiveSourceMetadata
	if archiveMeta, ok := archiveData.(*ArchiveSourceMetadata); ok {
		return archiveMeta, true
	}
	
	return nil, false
}

type AssetDetails struct {
	Code      string    `json:"code"`
	Issuer    string    `json:"issuer,omitempty"` // omitempty since native assets have no issuer
	Type      string    `json:"type"`             // native, credit_alphanum4, credit_alphanum12
	Timestamp time.Time `json:"timestamp"`
}

type AssetInfo struct {
	AssetDetails
	Stats      AssetStats `json:"stats"`
	HomeDomain string     `json:"home_domain,omitempty"`
	Anchor     struct {
		Name string `json:"name,omitempty"`
		URL  string `json:"url,omitempty"`
	} `json:"anchor,omitempty"`
	Verified    bool   `json:"verified"`
	DisplayName string `json:"display_name,omitempty"`
	Description string `json:"description,omitempty"`
}

// Helper functions for parsing data
func parseTimestamp(ts string) (time.Time, error) {
	i, err := strconv.ParseInt(ts, 10, 64)
	if err != nil {
		return time.Time{}, err
	}
	return time.Unix(i, 0), nil
}

// ExtractLedgerCloseMeta extracts xdr.LedgerCloseMeta from a processor.Message
func ExtractLedgerCloseMeta(msg Message) (xdr.LedgerCloseMeta, error) {
	ledgerCloseMeta, ok := msg.Payload.(xdr.LedgerCloseMeta)
	if !ok {
		return xdr.LedgerCloseMeta{}, fmt.Errorf("expected LedgerCloseMeta, got %T", msg.Payload)
	}
	return ledgerCloseMeta, nil
}

// ExtractLedgerTransaction extracts ingest.LedgerTransaction from a processor.Message
func ExtractLedgerTransaction(msg Message) (ingest.LedgerTransaction, error) {
	ledgerTransaction, ok := msg.Payload.(ingest.LedgerTransaction)
	if !ok {
		return ingest.LedgerTransaction{}, fmt.Errorf("expected LedgerTransaction, got %T", msg.Payload)
	}
	return ledgerTransaction, nil
}

// CreateTransactionReader creates a new ingest.LedgerTransactionReader from a processor.Message
func CreateTransactionReader(lcm xdr.LedgerCloseMeta, networkPassphrase string) (*ingest.LedgerTransactionReader, error) {
	return ingest.NewLedgerTransactionReaderFromLedgerCloseMeta(networkPassphrase, lcm)
}

// ForwardToProcessors marshals the payload and forwards it to all downstream processors
func ForwardToProcessors(ctx context.Context, payload interface{}, processors []Processor) error {
	jsonBytes, err := json.Marshal(payload)
	if err != nil {
		return fmt.Errorf("error marshaling payload: %w", err)
	}

	for _, processor := range processors {
		if err := processor.Process(ctx, Message{Payload: jsonBytes}); err != nil {
			return fmt.Errorf("error in processor chain: %w", err)
		}
	}

	return nil
}

type Event struct {
	ID         string      `json:"id"`
	Type       string      `json:"type"`
	Ledger     uint64      `json:"ledger"`
	ContractID string      `json:"contractId"`
	TxHash     string      `json:"txHash"`
	Topic      interface{} `json:"topic"`
	Value      interface{} `json:"value"`
}

// New utility functions for processing Stellar data

// ExtractEntryFromIngestChange gets the most recent state of an entry from an ingestion change
func ExtractEntryFromIngestChange(change ingest.Change) (xdr.LedgerEntry, xdr.LedgerEntryChangeType, bool, error) {
	switch changeType := change.ChangeType; changeType {
	case xdr.LedgerEntryChangeTypeLedgerEntryCreated, xdr.LedgerEntryChangeTypeLedgerEntryUpdated:
		return *change.Post, changeType, false, nil
	case xdr.LedgerEntryChangeTypeLedgerEntryRemoved:
		return *change.Pre, changeType, true, nil
	default:
		return xdr.LedgerEntry{}, changeType, false, fmt.Errorf("unable to extract ledger entry type from change")
	}
}

// ExtractEntryFromXDRChange gets the most recent state of an entry from an ingestion change
func ExtractEntryFromXDRChange(change xdr.LedgerEntryChange) (xdr.LedgerEntry, xdr.LedgerEntryChangeType, bool, error) {
	switch change.Type {
	case xdr.LedgerEntryChangeTypeLedgerEntryCreated:
		return *change.Created, change.Type, false, nil
	case xdr.LedgerEntryChangeTypeLedgerEntryUpdated:
		return *change.Updated, change.Type, false, nil
	case xdr.LedgerEntryChangeTypeLedgerEntryRemoved:
		return *change.State, change.Type, true, nil
	default:
		return xdr.LedgerEntry{}, change.Type, false, fmt.Errorf("unable to extract ledger entry type from change")
	}
}

// TimePointToUTCTimeStamp converts an xdr TimePoint to UTC time.Time
func TimePointToUTCTimeStamp(providedTime xdr.TimePoint) (time.Time, error) {
	intTime := int64(providedTime)
	if intTime < 0 {
		return time.Time{}, fmt.Errorf("negative timepoint provided: %d", intTime)
	}
	return time.Unix(intTime, 0).UTC(), nil
}

// GetLedgerCloseTime extracts the close time from a ledger
func GetLedgerCloseTime(lcm xdr.LedgerCloseMeta) (time.Time, error) {
	return TimePointToUTCTimeStamp(lcm.LedgerHeaderHistoryEntry().Header.ScpValue.CloseTime)
}

// GetLedgerSequence extracts the sequence number from a ledger
func GetLedgerSequence(lcm xdr.LedgerCloseMeta) uint32 {
	return uint32(lcm.LedgerHeaderHistoryEntry().Header.LedgerSeq)
}

// LedgerEntryToLedgerKeyHash generates a unique hash for a ledger entry
func LedgerEntryToLedgerKeyHash(ledgerEntry xdr.LedgerEntry) (string, error) {
	ledgerKey, err := ledgerEntry.LedgerKey()
	if err != nil {
		return "", fmt.Errorf("failed to get ledger key: %w", err)
	}

	ledgerKeyByte, err := ledgerKey.MarshalBinary()
	if err != nil {
		return "", fmt.Errorf("failed to marshal ledger key: %w", err)
	}

	hashedLedgerKeyByte := hash.Hash(ledgerKeyByte)
	return hex.EncodeToString(hashedLedgerKeyByte[:]), nil
}

// ConvertStroopValueToReal converts stroop values to real XLM values
func ConvertStroopValueToReal(input xdr.Int64) (float64, error) {
	rat := big.NewRat(int64(input), int64(10000000))
	output, _ := rat.Float64()
	return output, nil
}

// HashToHexString converts an XDR Hash to a hex string
func HashToHexString(inputHash xdr.Hash) string {
	return hex.EncodeToString(inputHash[:])
}

// Price represents the price of an asset as a fraction
type Price struct {
	Numerator   int32 `json:"n"`
	Denominator int32 `json:"d"`
}

// Path is a representation of an asset without an ID that forms part of a path in a path payment
type Path struct {
	AssetCode   string `json:"asset_code"`
	AssetIssuer string `json:"asset_issuer"`
	AssetType   string `json:"asset_type"`
}

type SponsorshipOutput struct {
	Operation      xdr.Operation
	OperationIndex uint32
}

// TestTransaction transaction meta
type TestTransaction struct {
	Index         uint32
	EnvelopeXDR   string
	ResultXDR     string
	FeeChangesXDR string
	MetaXDR       string
	Hash          string
}

type Asset struct {
	Code   string `json:"code"`
	Issuer string `json:"issuer,omitempty"`
	Type   string `json:"type"` // native, credit_alphanum4, credit_alphanum12
}

// ExtractEntryFromChange gets the most recent state of an entry from an ingestio change, as well as if the entry was deleted
func ExtractEntryFromChange(change ingest.Change) (xdr.LedgerEntry, xdr.LedgerEntryChangeType, bool, error) {
	switch changeType := change.ChangeType; changeType {
	case xdr.LedgerEntryChangeTypeLedgerEntryCreated, xdr.LedgerEntryChangeTypeLedgerEntryUpdated:
		return *change.Post, changeType, false, nil
	case xdr.LedgerEntryChangeTypeLedgerEntryRemoved:
		return *change.Pre, changeType, true, nil
	default:
		return xdr.LedgerEntry{}, changeType, false, fmt.Errorf("unable to extract ledger entry type from change")
	}
}
