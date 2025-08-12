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
	"github.com/stellar/go/strkey"
	"github.com/stellar/go/xdr"
	"github.com/withObsrvr/cdp-pipeline-workflow/pkg/control"
)

// ContractEvent represents an event emitted by a contract
type ContractEvent struct {
	Timestamp         time.Time        `json:"timestamp"`
	LedgerSequence    uint32           `json:"ledger_sequence"`
	TransactionHash   string           `json:"transaction_hash"`
	ContractID        string           `json:"contract_id"`
	Type              string           `json:"type"`
	EventType         string           `json:"event_type"`
	Topic             []xdr.ScVal      `json:"topic"`
	TopicDecoded      []interface{}    `json:"topic_decoded"`
	Data              json.RawMessage  `json:"data"`
	DataDecoded       interface{}      `json:"data_decoded"`
	InSuccessfulTx    bool             `json:"in_successful_tx"`
	EventIndex        int              `json:"event_index"`
	OperationIndex    int              `json:"operation_index"`
	DiagnosticEvents  []DiagnosticData `json:"diagnostic_events,omitempty"`
	NetworkPassphrase string           `json:"network_passphrase"`
}

// DiagnosticData captures additional event diagnostic information
type DiagnosticData struct {
	Event                    json.RawMessage `json:"event"`
	InSuccessfulContractCall bool            `json:"in_successful_contract_call"`
}

// ContractEventProcessor uses the SDK's helper methods for V3/V4 compatibility
type ContractEventProcessor struct {
	processors        []Processor
	networkPassphrase string
	mu                sync.RWMutex
	stats             struct {
		ProcessedLedgers  uint32
		EventsFound       uint64
		SuccessfulEvents  uint64
		FailedEvents      uint64
		LastLedger        uint32
		LastProcessedTime time.Time
	}
}

func NewContractEventProcessor(config map[string]interface{}) (*ContractEventProcessor, error) {
	networkPassphrase, ok := config["network_passphrase"].(string)
	if !ok {
		return nil, fmt.Errorf("missing network_passphrase in configuration")
	}

	return &ContractEventProcessor{
		networkPassphrase: networkPassphrase,
	}, nil
}

func (p *ContractEventProcessor) Subscribe(processor Processor) {
	p.processors = append(p.processors, processor)
}

func (p *ContractEventProcessor) Process(ctx context.Context, msg Message) error {
	ledgerCloseMeta, ok := msg.Payload.(xdr.LedgerCloseMeta)
	if !ok {
		return fmt.Errorf("expected xdr.LedgerCloseMeta, got %T", msg.Payload)
	}

	sequence := ledgerCloseMeta.LedgerSequence()
	log.Printf("Processing ledger %d for contract events", sequence)

	txReader, err := ingest.NewLedgerTransactionReaderFromLedgerCloseMeta(p.networkPassphrase, ledgerCloseMeta)
	if err != nil {
		return fmt.Errorf("error creating transaction reader: %w", err)
	}
	defer txReader.Close()

	// Process each transaction
	eventsInLedger := 0
	for {
		tx, err := txReader.Read()
		if err == io.EOF {
			break
		}
		if err != nil {
			return fmt.Errorf("error reading transaction: %w", err)
		}

		// Skip failed transactions
		if !tx.Result.Successful() {
			continue
		}

		// Use the SDK's helper method to get all transaction events
		// This abstracts away V3 vs V4 differences
		txEvents, err := tx.GetTransactionEvents()
		if err != nil {
			// Not a Soroban transaction or no events
			continue
		}

		// Process contract events from all operations
		for opIndex, opEvents := range txEvents.OperationEvents {
			for eventIdx, event := range opEvents {
				// Only process contract events (not system events)
				if event.Type != xdr.ContractEventTypeContract {
					continue
				}

				eventsInLedger++
				contractEvent, err := p.processContractEvent(tx, opIndex, eventIdx, event, ledgerCloseMeta)
				if err != nil {
					log.Printf("Error processing contract event: %v", err)
					continue
				}

				if contractEvent != nil {
					log.Printf("Successfully processed contract event from contract %s (op %d, event %d)", 
						contractEvent.ContractID, opIndex, eventIdx)
					if err := p.forwardToProcessors(ctx, contractEvent); err != nil {
						log.Printf("Error forwarding event: %v", err)
					}
				}
			}
		}

		// Also process transaction-level events if available (V4 only)
		for eventIdx, txEvent := range txEvents.TransactionEvents {
			if txEvent.Event.Type == xdr.ContractEventTypeContract {
				eventsInLedger++
				// Transaction-level events don't have a specific operation index
				contractEvent, err := p.processContractEvent(tx, -1, eventIdx, txEvent.Event, ledgerCloseMeta)
				if err != nil {
					log.Printf("Error processing transaction-level event: %v", err)
					continue
				}

				if contractEvent != nil {
					log.Printf("Successfully processed transaction-level event from contract %s", 
						contractEvent.ContractID)
					if err := p.forwardToProcessors(ctx, contractEvent); err != nil {
						log.Printf("Error forwarding event: %v", err)
					}
				}
			}
		}
	}

	if eventsInLedger > 0 {
		log.Printf("Found %d contract events in ledger %d", eventsInLedger, sequence)
	}

	p.mu.Lock()
	p.stats.ProcessedLedgers++
	p.stats.LastLedger = sequence
	p.stats.LastProcessedTime = time.Now()
	p.mu.Unlock()

	return nil
}

func (p *ContractEventProcessor) processContractEvent(
	tx ingest.LedgerTransaction,
	opIndex int,
	eventIndex int,
	event xdr.ContractEvent,
	meta xdr.LedgerCloseMeta,
) (*ContractEvent, error) {
	// Extract contract ID
	contractID, err := strkey.Encode(strkey.VersionByteContract, event.ContractId[:])
	if err != nil {
		return nil, fmt.Errorf("error encoding contract ID: %w", err)
	}

	// Convert event body to JSON
	data, err := json.Marshal(event.Body)
	if err != nil {
		return nil, fmt.Errorf("error marshaling event data: %w", err)
	}

	// Decode topics
	var topicDecoded []interface{}
	for _, topic := range event.Body.V0.Topics {
		decoded, err := ConvertScValToJSON(topic)
		if err != nil {
			log.Printf("Failed to decode topic: %v", err)
			decoded = nil
		}
		topicDecoded = append(topicDecoded, decoded)
	}

	// Decode event data if present
	var dataDecoded interface{}
	eventData := event.Body.V0.Data
	if eventData.Type != xdr.ScValTypeScvVoid {
		decoded, err := ConvertScValToJSON(eventData)
		if err != nil {
			log.Printf("Failed to decode event data: %v", err)
			dataDecoded = nil
		} else {
			dataDecoded = decoded
		}
	}

	// Determine if event was in successful transaction
	successful := tx.Result.Successful()

	p.mu.Lock()
	p.stats.EventsFound++
	if successful {
		p.stats.SuccessfulEvents++
	} else {
		p.stats.FailedEvents++
	}
	p.mu.Unlock()

	// Detect event type from topics
	eventType := DetectEventType(event.Body.V0.Topics)

	// Create contract event record
	contractEvent := &ContractEvent{
		Timestamp:         time.Unix(int64(meta.LedgerHeaderHistoryEntry().Header.ScpValue.CloseTime), 0),
		LedgerSequence:    meta.LedgerSequence(),
		TransactionHash:   tx.Result.TransactionHash.HexString(),
		ContractID:        contractID,
		Type:              string(event.Type),
		EventType:         eventType,
		Topic:             event.Body.V0.Topics,
		TopicDecoded:      topicDecoded,
		Data:              data,
		DataDecoded:       dataDecoded,
		InSuccessfulTx:    successful,
		EventIndex:        eventIndex,
		OperationIndex:    opIndex,
		NetworkPassphrase: p.networkPassphrase,
	}

	// Add diagnostic events if available
	diagnosticEvents, err := tx.GetDiagnosticEvents()
	if err == nil && len(diagnosticEvents) > 0 {
		var diagnosticData []DiagnosticData
		for _, diagEvent := range diagnosticEvents {
			if diagEvent.Event.Type == xdr.ContractEventTypeContract {
				eventData, err := json.Marshal(diagEvent.Event)
				if err != nil {
					continue
				}
				diagnosticData = append(diagnosticData, DiagnosticData{
					Event:                    eventData,
					InSuccessfulContractCall: diagEvent.InSuccessfulContractCall,
				})
			}
		}
		contractEvent.DiagnosticEvents = diagnosticData
	}

	return contractEvent, nil
}

// DetectEventType attempts to determine the event type from topics
func DetectEventType(topics []xdr.ScVal) string {
	// Check topics for common event type patterns
	for _, topic := range topics {
		if topic.Type == xdr.ScValTypeScvSymbol {
			sym := string(topic.MustSym())
			// Common event types
			switch sym {
			case "transfer", "Transfer":
				return "transfer"
			case "mint", "Mint":
				return "mint"
			case "burn", "Burn":
				return "burn"
			case "swap", "Swap":
				return "swap"
			case "sync", "Sync":
				return "sync"
			case "deposit", "Deposit":
				return "deposit"
			case "withdraw", "Withdraw":
				return "withdraw"
			case "new_pair", "NewPair":
				return "new_pair"
			}
		}
	}
	return "unknown"
}

func (p *ContractEventProcessor) forwardToProcessors(ctx context.Context, event *ContractEvent) error {
	jsonBytes, err := json.Marshal(event)
	if err != nil {
		return fmt.Errorf("error marshaling event: %w", err)
	}

	for _, processor := range p.processors {
		if err := processor.Process(ctx, Message{Payload: jsonBytes}); err != nil {
			return fmt.Errorf("error in processor chain: %w", err)
		}
	}
	return nil
}

// GetStats returns the current processing statistics
func (p *ContractEventProcessor) GetStats() struct {
	ProcessedLedgers  uint32
	EventsFound       uint64
	SuccessfulEvents  uint64
	FailedEvents      uint64
	LastLedger        uint32
	LastProcessedTime time.Time
} {
	p.mu.RLock()
	defer p.mu.RUnlock()
	return p.stats
}

// GetStats implements the control.StatsProvider interface
func (p *ContractEventProcessor) GetStatsForControl() control.ComponentStats {
	p.mu.RLock()
	defer p.mu.RUnlock()
	
	return control.ComponentStats{
		ComponentType: "processor",
		ComponentName: "ContractEventProcessor",
		Stats: map[string]interface{}{
			"processed_ledgers":   p.stats.ProcessedLedgers,
			"events_found":        p.stats.EventsFound,
			"successful_events":   p.stats.SuccessfulEvents,
			"failed_events":       p.stats.FailedEvents,
			"last_ledger":         p.stats.LastLedger,
			"last_processed_time": p.stats.LastProcessedTime.Unix(),
		},
		LastUpdated: time.Now(),
	}
}