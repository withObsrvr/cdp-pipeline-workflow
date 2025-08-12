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

	// Track metadata versions seen
	v3Count := 0
	v4Count := 0
	otherVersionCount := 0
	
	// Process each transaction
	for {
		tx, err := txReader.Read()
		if err == io.EOF {
			break
		}
		if err != nil {
			return fmt.Errorf("error reading transaction: %w", err)
		}

		// Check if transaction has Soroban meta with events
		// V3: Events are in sorobanMeta.Events
		// V4: Events are moved to tx.UnsafeMeta.V4.Events (Protocol 23)
		var sorobanEvents []xdr.ContractEvent
		
		switch tx.UnsafeMeta.V {
		case 3:
			v3Count++
			// V3 metadata - events are in SorobanTransactionMeta
			sorobanMeta := tx.UnsafeMeta.V3.SorobanMeta
			if sorobanMeta != nil && sorobanMeta.Events != nil {
				sorobanEvents = sorobanMeta.Events
			}
		case 4:
			v4Count++
			// V4 metadata (Protocol 23) - events moved to TransactionMetaV4.events
			// Note: In V4, SorobanTransactionMetaV2 no longer contains events
			
			// Debug: Check if V4 is accessible
			if tx.UnsafeMeta.V4 == nil {
				log.Printf("WARNING: V4 metadata is nil for transaction %s", tx.Result.TransactionHash.HexString())
				continue
			}
			
			// Check for events in V4.Events
			if tx.UnsafeMeta.V4.Events != nil && len(tx.UnsafeMeta.V4.Events) > 0 {
				// Extract ContractEvent from each TransactionEvent
				for _, txEvent := range tx.UnsafeMeta.V4.Events {
					// TransactionEvent wraps ContractEvent with a stage indicator
					if txEvent.Event.Type == xdr.ContractEventTypeContract {
						sorobanEvents = append(sorobanEvents, txEvent.Event)
					}
				}
				log.Printf("Found %d V4 transaction events, extracted %d contract events in tx %s", 
					len(tx.UnsafeMeta.V4.Events), len(sorobanEvents), tx.Result.TransactionHash.HexString())
			} else {
				// Log when V4 has no events (common case)
				if tx.Envelope.IsFeeBump() || !tx.Result.Successful() {
					// Skip logging for fee bumps and failed transactions
				} else if tx.Envelope.Operations() != nil && len(tx.Envelope.Operations()) > 0 {
					// Check if this is a Soroban operation
					for _, op := range tx.Envelope.Operations() {
						if op.Body.Type == xdr.OperationTypeInvokeHostFunction {
							log.Printf("V4 Soroban transaction %s has no events in V4.Events field", tx.Result.TransactionHash.HexString())
							break
						}
					}
				}
			}
			// Also check diagnostic events which may contain contract events
			if tx.UnsafeMeta.V4.DiagnosticEvents != nil {
				contractDiagnosticCount := 0
				for _, diagEvent := range tx.UnsafeMeta.V4.DiagnosticEvents {
					if diagEvent.Event.Type == xdr.ContractEventTypeContract {
						contractDiagnosticCount++
						// For now, we'll focus on the main events, not diagnostic ones
						// Diagnostic events are typically for debugging failed transactions
					}
				}
				if contractDiagnosticCount > 0 {
					log.Printf("Found %d contract events in V4 diagnostic events (not processed)", contractDiagnosticCount)
				}
			}
		default:
			otherVersionCount++
			// Pre-V3 or unknown future versions - skip
			continue
		}
		
		// For now, only process V3 events until we implement V4 TransactionEvent handling
		if len(sorobanEvents) == 0 {
			continue // No Soroban events, skip
		}

		// Log that we found events
		log.Printf("Found %d Soroban events in transaction %s", len(sorobanEvents), tx.Result.TransactionHash.HexString())
		
		// Process each event from SorobanMeta
		for eventIdx, event := range sorobanEvents {
			// Only process contract events (not system events)
			if event.Type != xdr.ContractEventTypeContract {
				log.Printf("Skipping non-contract event (type: %v)", event.Type)
				continue
			}

			// For now, we don't have operation index from the event itself,
			// so we'll use 0 as default (this could be improved in the future)
			opIdx := 0
			
			contractEvent, err := p.processContractEvent(tx, opIdx, eventIdx, event, ledgerCloseMeta)
			if err != nil {
				log.Printf("Error processing contract event: %v", err)
				continue
			}

			if contractEvent != nil {
				log.Printf("Successfully processed contract event from contract %s", contractEvent.ContractID)
				if err := p.forwardToProcessors(ctx, contractEvent); err != nil {
					log.Printf("Error forwarding event: %v", err)
				}
			}
		}
	}

	// Log metadata version statistics
	if v3Count > 0 || v4Count > 0 || otherVersionCount > 0 {
		log.Printf("Ledger %d metadata versions - V3: %d, V4: %d, Other: %d", 
			sequence, v3Count, v4Count, otherVersionCount)
	}
	
	p.mu.Lock()
	p.stats.ProcessedLedgers++
	p.stats.LastLedger = sequence
	p.stats.LastProcessedTime = time.Now()
	p.mu.Unlock()

	return nil
}

// filterContractEvents groups contract events by operation index
func filterContractEvents(diagnosticEvents []xdr.DiagnosticEvent) map[int][]xdr.ContractEvent {
	events := make(map[int][]xdr.ContractEvent)

	for _, diagEvent := range diagnosticEvents {
		if !diagEvent.InSuccessfulContractCall || diagEvent.Event.Type != xdr.ContractEventTypeContract {
			continue
		}

		// Use the operation index as the key
		opIndex := 0 // Default to 0 if no specific index available
		events[opIndex] = append(events[opIndex], diagEvent.Event)
	}
	return events
}

// DetectEventType detects the event type from the first topic if available
func DetectEventType(topics []xdr.ScVal) string {
	if len(topics) > 0 {
		eventName, err := ConvertScValToJSON(topics[0])
		if err == nil {
			if str, ok := eventName.(string); ok {
				return str
			}
		}
	}
	return "unknown"
}

func (p *ContractEventProcessor) processContractEvent(
	tx ingest.LedgerTransaction,
	opIndex, eventIndex int,
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
		Topic:             event.Body.V0.Topics, // Use V0 topics from the event body
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
	if err == nil {
		var diagnosticData []DiagnosticData
		for _, diagEvent := range diagnosticEvents {
			// Since we don't have ExtensionPoint, we'll use operation index directly
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
