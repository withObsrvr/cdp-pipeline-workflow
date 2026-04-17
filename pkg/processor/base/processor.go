package base

import (
	"context"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"time"

	"github.com/stellar/go/hash"
	"github.com/stellar/go/ingest"
	"github.com/stellar/go/xdr"
	"github.com/withObsrvr/cdp-pipeline-workflow/pkg/common/types"
)

// Processor alias to types.Processor for backward compatibility
type Processor = types.Processor

// ProcessorConfig alias to types.ProcessorConfig for backward compatibility
type ProcessorConfig = types.ProcessorConfig

// Message alias to types.Message for backward compatibility
type Message = types.Message

// Helper functions for parsing data
func parseTimestamp(ts string) (time.Time, error) {
	return types.ParseTimestamp(ts)
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
	return types.ConvertStroopValueToReal(input)
}

// HashToHexString converts an XDR Hash to a hex string
func HashToHexString(inputHash xdr.Hash) string {
	return types.HashToHexString(inputHash)
}

// Use types from the common package
type Price = types.Price
type Path = types.Path
type Asset = types.Asset
type AssetStats = types.AssetStats
type SupplyData = types.SupplyData

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
