package processor

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"sync"
	"time"

	"github.com/stellar/go/ingest"
	"github.com/stellar/go/processors/contract"
	"github.com/stellar/go/xdr"
)

// ContractDataProcessor processes contract data changes using stellar/go processor
type ContractDataProcessor struct {
	name              string
	subscribers       []Processor
	networkPassphrase string
	mu                sync.Mutex
}

// ContractDataMessage wraps the stellar/go ContractDataOutput for CDP pipeline
type ContractDataMessage struct {
	ContractData    contract.ContractDataOutput    `json:"contract_data"`
	ContractId      string                         `json:"contract_id"`      // Extracted for easy access
	Timestamp       time.Time                      `json:"timestamp"`
	LedgerSeq       uint32                         `json:"ledger_sequence"`
	ProcessorName   string                         `json:"processor_name"`
	MessageType     string                         `json:"message_type"`
	ArchiveMetadata *ArchiveSourceMetadata         `json:"archive_metadata,omitempty"` // Source file provenance
}

// NewContractDataProcessor creates a new contract data processor
func NewContractDataProcessor(config map[string]interface{}) (*ContractDataProcessor, error) {
	networkPassphrase, ok := config["network_passphrase"].(string)
	if !ok {
		return nil, fmt.Errorf("missing network_passphrase in configuration")
	}

	name, _ := config["name"].(string)
	if name == "" {
		name = "contract_data_processor"
	}

	return &ContractDataProcessor{
		name:              name,
		networkPassphrase: networkPassphrase,
		subscribers:       make([]Processor, 0),
	}, nil
}

// Process processes a ledger close meta message and extracts contract data changes
func (p *ContractDataProcessor) Process(ctx context.Context, msg Message) error {
	ledgerCloseMeta, ok := msg.Payload.(xdr.LedgerCloseMeta)
	if !ok {
		return fmt.Errorf("expected LedgerCloseMeta, got %T", msg.Payload)
	}

	// Extract source metadata from incoming message
	archiveMetadata, _ := msg.GetArchiveMetadata()

	// Create transaction reader
	txReader, err := ingest.NewLedgerTransactionReaderFromLedgerCloseMeta(
		p.networkPassphrase, ledgerCloseMeta)
	if err != nil {
		return fmt.Errorf("error creating transaction reader: %w", err)
	}
	defer txReader.Close()

	ledgerHeader := ledgerCloseMeta.LedgerHeaderHistoryEntry().Header

	// Process each transaction
	for {
		tx, err := txReader.Read()
		if err == io.EOF {
			break
		}
		if err != nil {
			return fmt.Errorf("error reading transaction: %w", err)
		}

		// Get changes from transaction
		changes, err := tx.GetChanges()
		if err != nil {
			log.Printf("Error getting changes: %v", err)
			continue
		}

		// Process each change
		for _, change := range changes {
			// Filter for contract data entries only
			if !p.isContractDataChange(change) {
				continue
			}

			// Use stellar/go processor directly
			transformer := contract.NewTransformContractDataStruct(
				contract.AssetFromContractData, 
				contract.ContractBalanceFromContractData,
			)
			contractData, err, shouldContinue := transformer.TransformContractData(
				change, p.networkPassphrase, ledgerCloseMeta.LedgerHeaderHistoryEntry())
			if err != nil {
				log.Printf("Error transforming contract data: %v", err)
				continue
			}
			if !shouldContinue {
				continue
			}

			// Create CDP message with preserved source metadata
			contractMsg := ContractDataMessage{
				ContractData:    contractData,
				ContractId:      contractData.ContractId, // Extract for easy access
				Timestamp:       time.Now(),
				LedgerSeq:       uint32(ledgerHeader.LedgerSeq),
				ProcessorName:   p.name,
				MessageType:     "contract_data",
				ArchiveMetadata: archiveMetadata,
			}

			// Send to subscribers
			if err := p.forwardToProcessors(ctx, &contractMsg); err != nil {
				log.Printf("Error forwarding contract data: %v", err)
			}
		}
	}

	return nil
}

// isContractDataChange checks if this change involves contract data
func (p *ContractDataProcessor) isContractDataChange(change ingest.Change) bool {
	// Check if this change involves contract data
	if change.Pre != nil && change.Pre.Data.Type == xdr.LedgerEntryTypeContractData {
		return true
	}
	if change.Post != nil && change.Post.Data.Type == xdr.LedgerEntryTypeContractData {
		return true
	}
	return false
}

// Subscribe adds a processor to receive contract data messages
func (p *ContractDataProcessor) Subscribe(processor Processor) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.subscribers = append(p.subscribers, processor)
}

// GetName returns the processor name
func (p *ContractDataProcessor) GetName() string {
	return p.name
}

// forwardToProcessors serializes and forwards contract data to subscribers
func (p *ContractDataProcessor) forwardToProcessors(ctx context.Context, contractMsg *ContractDataMessage) error {
	jsonBytes, err := json.Marshal(contractMsg)
	if err != nil {
		return fmt.Errorf("error marshaling contract data: %w", err)
	}

	p.mu.Lock()
	subscribers := make([]Processor, len(p.subscribers))
	copy(subscribers, p.subscribers)
	p.mu.Unlock()
	
	for _, subscriber := range subscribers {
		if err := subscriber.Process(ctx, Message{Payload: jsonBytes}); err != nil {
			return fmt.Errorf("error in processor chain: %w", err)
		}
	}
	
	return nil
}