package processor

import (
	"context"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"sync"
	"time"

	"github.com/stellar/go/ingest"
	"github.com/stellar/go/xdr"
)

// LedgerChange represents a change in the ledger
type LedgerChange struct {
	Timestamp       time.Time              `json:"timestamp"`
	LedgerSequence  uint32                 `json:"ledger_sequence"`
	TransactionHash string                 `json:"transaction_hash"`
	OperationIndex  int                    `json:"operation_index"`
	ChangeType      string                 `json:"change_type"` // created, updated, removed, state
	EntryType       string                 `json:"entry_type"`  // account, trustline, offer, data, etc.
	EntryKey        string                 `json:"entry_key"`   // ledger key hash
	PreState        *json.RawMessage       `json:"pre_state"`   // state before change, if available
	PostState       *json.RawMessage       `json:"post_state"`  // state after change, if available
	Changes         map[string]interface{} `json:"changes"`     // map of changed fields
}

type LedgerChanges struct {
	Changes       []ingest.Change
	LedgerHeaders []xdr.LedgerHeaderHistoryEntry
}

type ChangeBatch struct {
	Changes    map[xdr.LedgerEntryType]LedgerChanges
	BatchStart uint32
	BatchEnd   uint32
}

type LedgerChangeProcessor struct {
	processors        []Processor
	networkPassphrase string
	batchSize         uint32
	mu                sync.RWMutex
	stats             struct {
		ProcessedLedgers uint32
		ChangesFound     uint64
		LastLedger       uint32
		LastUpdateTime   time.Time
	}
}

func NewLedgerChangeProcessor(config map[string]interface{}) (*LedgerChangeProcessor, error) {
	networkPassphrase, ok := config["network_passphrase"].(string)
	if !ok {
		return nil, fmt.Errorf("missing network_passphrase in configuration")
	}

	batchSize := uint32(100) // default batch size
	if size, ok := config["batch_size"].(uint32); ok {
		batchSize = size
	}

	return &LedgerChangeProcessor{
		networkPassphrase: networkPassphrase,
		batchSize:         batchSize,
	}, nil
}

func (p *LedgerChangeProcessor) Subscribe(processor Processor) {
	p.processors = append(p.processors, processor)
}

func (p *LedgerChangeProcessor) Process(ctx context.Context, msg Message) error {
	ledgerCloseMeta, ok := msg.Payload.(xdr.LedgerCloseMeta)
	if !ok {
		return fmt.Errorf("expected xdr.LedgerCloseMeta, got %T", msg.Payload)
	}

	sequence := ledgerCloseMeta.LedgerSequence()
	log.Printf("Processing ledger %d for changes", sequence)

	txReader, err := ingest.NewLedgerTransactionReaderFromLedgerCloseMeta(p.networkPassphrase, ledgerCloseMeta)
	if err != nil {
		return fmt.Errorf("error creating transaction reader: %w", err)
	}
	defer txReader.Close()

	closeTime := time.Unix(int64(ledgerCloseMeta.LedgerHeaderHistoryEntry().Header.ScpValue.CloseTime), 0).UTC()

	txCount := 0
	// Process each transaction
	for {
		tx, err := txReader.Read()
		if err == io.EOF {
			break
		}
		if err != nil {
			return fmt.Errorf("error reading transaction: %w", err)
		}
		txCount++
		log.Printf("Processing transaction %d in ledger %d", txCount, sequence)

		// Get changes directly from the transaction
		changes, err := tx.GetChanges()
		if err != nil {
			log.Printf("Error getting changes: %v", err)
			continue
		}
		log.Printf("Found %d changes in transaction %d", len(changes), txCount)

		// Process each change
		for _, change := range changes {
			// Convert ingest.Change to xdr.LedgerEntryChange
			var ledgerChange *LedgerChange
			var err error

			// Map the ingest.Change type to xdr.LedgerEntryChangeType
			var changeType xdr.LedgerEntryChangeType
			switch {
			case change.Pre == nil && change.Post != nil:
				changeType = xdr.LedgerEntryChangeTypeLedgerEntryCreated
			case change.Pre != nil && change.Post != nil:
				changeType = xdr.LedgerEntryChangeTypeLedgerEntryUpdated
			case change.Pre != nil && change.Post == nil:
				changeType = xdr.LedgerEntryChangeTypeLedgerEntryRemoved
			default:
				changeType = xdr.LedgerEntryChangeTypeLedgerEntryState
			}

			// For transaction-level changes, use -1 as the operation index
			opIndex := -1

			switch changeType {
			case xdr.LedgerEntryChangeTypeLedgerEntryCreated:
				ledgerChange, err = p.processChange(xdr.LedgerEntryChange{
					Type:    xdr.LedgerEntryChangeTypeLedgerEntryCreated,
					Created: change.Post,
				}, sequence, closeTime, tx.Result.TransactionHash.HexString(), opIndex)
			case xdr.LedgerEntryChangeTypeLedgerEntryUpdated:
				ledgerChange, err = p.processChange(xdr.LedgerEntryChange{
					Type:    xdr.LedgerEntryChangeTypeLedgerEntryUpdated,
					Updated: change.Post,
				}, sequence, closeTime, tx.Result.TransactionHash.HexString(), opIndex)
			case xdr.LedgerEntryChangeTypeLedgerEntryRemoved:
				// For removed entries, we need to get the key from Pre
				if change.Pre == nil {
					log.Printf("Warning: removed entry has nil Pre state")
					continue
				}
				key, err := change.Pre.LedgerKey()
				if err != nil {
					log.Printf("Error getting ledger key: %v", err)
					continue
				}
				ledgerChange, err = p.processChange(xdr.LedgerEntryChange{
					Type:    xdr.LedgerEntryChangeTypeLedgerEntryRemoved,
					Removed: &key,
				}, sequence, closeTime, tx.Result.TransactionHash.HexString(), opIndex)
			case xdr.LedgerEntryChangeTypeLedgerEntryState:
				ledgerChange, err = p.processChange(xdr.LedgerEntryChange{
					Type:  xdr.LedgerEntryChangeTypeLedgerEntryState,
					State: change.Pre,
				}, sequence, closeTime, tx.Result.TransactionHash.HexString(), opIndex)
			}

			if err != nil {
				log.Printf("Error processing change: %v", err)
				continue
			}

			// Forward the change
			if err := p.forwardToProcessors(ctx, ledgerChange); err != nil {
				log.Printf("Error forwarding change: %v", err)
				continue
			}

			p.mu.Lock()
			p.stats.ChangesFound++
			p.mu.Unlock()
		}

		// Also process operation level changes if available
		if tx.UnsafeMeta.V1 != nil {
			for opIndex, op := range tx.UnsafeMeta.V1.Operations {
				opChanges := op.Changes
				log.Printf("Found %d changes in operation %d", len(opChanges), opIndex)

				for _, change := range opChanges {
					ledgerChange, err := p.processChange(change, sequence, closeTime, tx.Result.TransactionHash.HexString(), opIndex)
					if err != nil {
						log.Printf("Error processing change: %v", err)
						continue
					}

					// Forward the change
					if err := p.forwardToProcessors(ctx, ledgerChange); err != nil {
						log.Printf("Error forwarding change: %v", err)
						continue
					}

					p.mu.Lock()
					p.stats.ChangesFound++
					p.mu.Unlock()
				}
			}
		}
	}

	log.Printf("Completed processing ledger %d with %d transactions", sequence, txCount)

	p.mu.Lock()
	p.stats.ProcessedLedgers++
	p.stats.LastLedger = sequence
	p.stats.LastUpdateTime = time.Now()
	p.mu.Unlock()

	return nil
}

func (p *LedgerChangeProcessor) processChange(
	change xdr.LedgerEntryChange,
	sequence uint32,
	closeTime time.Time,
	txHash string,
	opIndex int,
) (*LedgerChange, error) {
	var changeType string
	var preState, postState *json.RawMessage
	var entryType string
	var entryKey string
	var changes map[string]interface{}

	switch change.Type {
	case xdr.LedgerEntryChangeTypeLedgerEntryCreated:
		changeType = "created"
		entry := change.Created
		entryType = entry.Data.Type.String()
		key, err := entry.LedgerKey()
		if err != nil {
			return nil, fmt.Errorf("error getting ledger key: %w", err)
		}
		keyBytes, err := key.MarshalBinary()
		if err != nil {
			return nil, fmt.Errorf("error marshaling ledger key: %w", err)
		}
		entryKey = hex.EncodeToString(keyBytes)
		if postBytes, err := json.Marshal(entry); err == nil {
			raw := json.RawMessage(postBytes)
			postState = &raw
		}

	case xdr.LedgerEntryChangeTypeLedgerEntryUpdated:
		changeType = "updated"
		entry := change.Updated
		entryType = entry.Data.Type.String()
		key, err := entry.LedgerKey()
		if err != nil {
			return nil, fmt.Errorf("error getting ledger key: %w", err)
		}
		keyBytes, err := key.MarshalBinary()
		if err != nil {
			return nil, fmt.Errorf("error marshaling ledger key: %w", err)
		}
		entryKey = hex.EncodeToString(keyBytes)
		if postBytes, err := json.Marshal(entry); err == nil {
			raw := json.RawMessage(postBytes)
			postState = &raw
		}

	case xdr.LedgerEntryChangeTypeLedgerEntryRemoved:
		changeType = "removed"
		key := change.Removed
		if key == nil {
			return nil, fmt.Errorf("removed entry has nil key")
		}

		// For removed entries, we only have the key
		entryType = key.Type.String()
		keyBytes, err := key.MarshalBinary()
		if err != nil {
			return nil, fmt.Errorf("error marshaling ledger key: %w", err)
		}
		entryKey = hex.EncodeToString(keyBytes)
		// Note: For removed entries, we don't have pre/post state since we only have the key

	case xdr.LedgerEntryChangeTypeLedgerEntryState:
		changeType = "state"
		entry := change.State
		entryType = entry.Data.Type.String()
		key, err := entry.LedgerKey()
		if err != nil {
			return nil, fmt.Errorf("error getting ledger key: %w", err)
		}
		keyBytes, err := key.MarshalBinary()
		if err != nil {
			return nil, fmt.Errorf("error marshaling ledger key: %w", err)
		}
		entryKey = hex.EncodeToString(keyBytes)
		if stateBytes, err := json.Marshal(entry); err == nil {
			raw := json.RawMessage(stateBytes)
			preState = &raw
		}
	}

	return &LedgerChange{
		Timestamp:       closeTime,
		LedgerSequence:  sequence,
		TransactionHash: txHash,
		OperationIndex:  opIndex,
		ChangeType:      changeType,
		EntryType:       entryType,
		EntryKey:        entryKey,
		PreState:        preState,
		PostState:       postState,
		Changes:         changes,
	}, nil
}

func (p *LedgerChangeProcessor) forwardToProcessors(ctx context.Context, change *LedgerChange) error {
	jsonBytes, err := json.Marshal(change)
	if err != nil {
		return fmt.Errorf("error marshaling change: %w", err)
	}

	for _, processor := range p.processors {
		if err := processor.Process(ctx, Message{Payload: jsonBytes}); err != nil {
			return fmt.Errorf("error in processor chain: %w", err)
		}
	}
	return nil
}

func (p *LedgerChangeProcessor) GetStats() struct {
	ProcessedLedgers uint32
	ChangesFound     uint64
	LastLedger       uint32
	LastUpdateTime   time.Time
} {
	p.mu.RLock()
	defer p.mu.RUnlock()
	return p.stats
}
