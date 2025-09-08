package processor

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"sync"
	"time"

	"github.com/stellar/go/ingest"
	"github.com/stellar/go/processors/contract"
	"github.com/stellar/go/strkey"
	"github.com/stellar/go/xdr"
)

// StellarContractEventProcessor uses the Stellar SDK's contract event processor
type StellarContractEventProcessor struct {
	name              string
	subscribers       []Processor
	networkPassphrase string
	filterEventTypes  []int32  // Optional: filter specific event types (0=system, 1=contract, 2=diagnostic)
	mu                sync.Mutex
	stats             struct {
		ProcessedLedgers uint32
		EventsFound      uint64
		ContractEvents   uint64
		SystemEvents     uint64
		DiagnosticEvents uint64
		LastLedger       uint32
		LastProcessedTime time.Time
	}
}

// StellarContractEventMessage wraps the SDK's ContractEventOutput for CDP pipeline
type StellarContractEventMessage struct {
	Events          []contract.ContractEventOutput `json:"events"`
	TransactionHash string                         `json:"transaction_hash"`
	LedgerSequence  uint32                         `json:"ledger_sequence"`
	ProcessorName   string                         `json:"processor_name"`
	MessageType     string                         `json:"message_type"`
	Timestamp       time.Time                      `json:"timestamp"`
}

// NewStellarContractEventProcessor creates a new processor using Stellar SDK implementation
func NewStellarContractEventProcessor(config map[string]interface{}) (*StellarContractEventProcessor, error) {
	networkPassphrase, ok := config["network_passphrase"].(string)
	if !ok {
		return nil, fmt.Errorf("missing network_passphrase in configuration")
	}

	name, _ := config["name"].(string)
	if name == "" {
		name = "stellar_contract_event_processor"
	}

	processor := &StellarContractEventProcessor{
		name:              name,
		networkPassphrase: networkPassphrase,
		subscribers:       make([]Processor, 0),
	}

	// Optional: configure event type filtering
	// event_types: [0, 2] would include only contract and diagnostic events
	log.Printf("Config received: %+v", config)
	if eventTypes, ok := config["event_types"].([]interface{}); ok {
		processor.filterEventTypes = make([]int32, 0, len(eventTypes))
		for _, et := range eventTypes {
			if etNum, ok := et.(float64); ok {
				processor.filterEventTypes = append(processor.filterEventTypes, int32(etNum))
			} else if etNum, ok := et.(int); ok {
				processor.filterEventTypes = append(processor.filterEventTypes, int32(etNum))
			} else {
				log.Printf("Warning: unexpected type for event_type element: %T", et)
			}
		}
		log.Printf("Configured to filter event types: %v", processor.filterEventTypes)
	} else {
		log.Printf("No event_types filter configured (got type: %T)", config["event_types"])
	}

	return processor, nil
}

// Process processes a ledger close meta message and extracts contract events
func (p *StellarContractEventProcessor) Process(ctx context.Context, msg Message) error {
	ledgerCloseMeta, ok := msg.Payload.(xdr.LedgerCloseMeta)
	if !ok {
		return fmt.Errorf("expected LedgerCloseMeta, got %T", msg.Payload)
	}

	sequence := ledgerCloseMeta.LedgerSequence()
	log.Printf("Processing ledger %d for contract events using Stellar SDK", sequence)

	// Create transaction reader
	txReader, err := ingest.NewLedgerTransactionReaderFromLedgerCloseMeta(
		p.networkPassphrase, ledgerCloseMeta)
	if err != nil {
		return fmt.Errorf("error creating transaction reader: %w", err)
	}
	defer txReader.Close()

	ledgerHeader := ledgerCloseMeta.LedgerHeaderHistoryEntry()
	eventsInLedger := 0
	contractEventsInLedger := 0
	systemEventsInLedger := 0
	diagnosticEventsInLedger := 0

	// Process each transaction
	for {
		tx, err := txReader.Read()
		if err == io.EOF {
			break
		}
		if err != nil {
			return fmt.Errorf("error reading transaction: %w", err)
		}

		// First, let's see what GetTransactionEvents returns
		contractEventCount := 0
		txEvents, err1 := tx.GetTransactionEvents()
		if err1 == nil {
			for _, opEvents := range txEvents.OperationEvents {
				for _, event := range opEvents {
					if event.Type == xdr.ContractEventTypeContract {
						contractEventCount++
					}
				}
			}
			for _, txEvent := range txEvents.TransactionEvents {
				if txEvent.Event.Type == xdr.ContractEventTypeContract {
					contractEventCount++
				}
			}
			if contractEventCount > 0 {
				log.Printf("GetTransactionEvents found %d contract events in tx %s", 
					contractEventCount, tx.Result.TransactionHash.HexString())
			}
		}

		// TEMPORARY: Since TransformContractEvent isn't returning contract events,
		// let's use GetTransactionEvents directly and convert to ContractEventOutput format
		var events []contract.ContractEventOutput
		if err1 == nil && contractEventCount > 0 {
			events = p.convertFromTransactionEvents(tx, txEvents, ledgerHeader)
		} else {
			// Fall back to TransformContractEvent for other cases
			var err error
			events, err = contract.TransformContractEvent(tx, ledgerHeader)
			if err != nil {
				continue
			}
			if len(events) == 0 {
				continue
			}
		}
		
		// Debug: log transaction with events
		if len(events) > 0 {
			eventTypes := make(map[int32]int)
			for _, e := range events {
				eventTypes[e.Type]++
			}
			log.Printf("Transaction %s has %d events: %v", 
				tx.Result.TransactionHash.HexString(), len(events), eventTypes)
			
			// Log first few events to see their structure
			for i, e := range events {
				if i < 3 { // Only log first 3 events
					log.Printf("  Event %d: Type=%d (%s), ContractId=%s", 
						i, e.Type, e.TypeString, e.ContractId)
				}
			}
		}

		// Apply event type filtering if configured
		filteredEvents := events
		if len(p.filterEventTypes) > 0 {
			log.Printf("Applying filter for event types: %v", p.filterEventTypes)
			filteredEvents = make([]contract.ContractEventOutput, 0, len(events))
			for _, event := range events {
				for _, allowedType := range p.filterEventTypes {
					if event.Type == allowedType {
						filteredEvents = append(filteredEvents, event)
						break
					}
				}
			}
			log.Printf("Filtered %d events to %d events", len(events), len(filteredEvents))
			if len(filteredEvents) == 0 {
				continue // Skip if no events match filter
			}
		}

		// Count events by type for statistics
		for _, event := range filteredEvents {
			switch event.Type {
			case 0: // ContractEventTypeSystem
				p.stats.SystemEvents++
				systemEventsInLedger++
			case 1: // ContractEventTypeContract
				p.stats.ContractEvents++
				contractEventsInLedger++
			case 2: // ContractEventTypeDiagnostic
				p.stats.DiagnosticEvents++
				diagnosticEventsInLedger++
			}
		}

		eventsInLedger += len(filteredEvents)

		// Create message for each transaction's events
		eventMsg := StellarContractEventMessage{
			Events:          filteredEvents,
			TransactionHash: tx.Result.TransactionHash.HexString(),
			LedgerSequence:  sequence,
			ProcessorName:   p.name,
			MessageType:     "stellar_contract_events",
			Timestamp:       time.Now(),
		}

		// Forward to subscribers
		if err := p.forwardToProcessors(ctx, &eventMsg); err != nil {
			log.Printf("Error forwarding contract events: %v", err)
		}

		// Log successful processing with event type
		for _, event := range filteredEvents {
			var eventTypeStr string
			switch event.Type {
			case 0:
				eventTypeStr = "system"
			case 1:
				eventTypeStr = "contract"
			case 2:
				eventTypeStr = "diagnostic"
			default:
				eventTypeStr = fmt.Sprintf("unknown(%d)", event.Type)
			}
			log.Printf("Successfully processed %s event from contract %s in tx %s",
				eventTypeStr, event.ContractId, event.TransactionHash)
		}
	}

	if eventsInLedger > 0 {
		log.Printf("Found %d events in ledger %d (Contract: %d, System: %d, Diagnostic: %d)",
			eventsInLedger, sequence,
			contractEventsInLedger,
			systemEventsInLedger,
			diagnosticEventsInLedger)
	}

	// Update statistics
	p.mu.Lock()
	p.stats.ProcessedLedgers++
	p.stats.EventsFound += uint64(eventsInLedger)
	p.stats.LastLedger = sequence
	p.stats.LastProcessedTime = time.Now()
	p.mu.Unlock()

	return nil
}

// Subscribe adds a processor to receive contract event messages
func (p *StellarContractEventProcessor) Subscribe(processor Processor) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.subscribers = append(p.subscribers, processor)
}

// GetName returns the processor name
func (p *StellarContractEventProcessor) GetName() string {
	return p.name
}

// GetStats returns processing statistics
func (p *StellarContractEventProcessor) GetStats() struct {
	ProcessedLedgers uint32
	EventsFound      uint64
	ContractEvents   uint64
	SystemEvents     uint64
	DiagnosticEvents uint64
	LastLedger       uint32
	LastProcessedTime time.Time
} {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.stats
}

// forwardToProcessors serializes and forwards events to subscribers
func (p *StellarContractEventProcessor) forwardToProcessors(ctx context.Context, eventMsg *StellarContractEventMessage) error {
	jsonBytes, err := json.Marshal(eventMsg)
	if err != nil {
		return fmt.Errorf("error marshaling contract events: %w", err)
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

// convertFromTransactionEvents converts GetTransactionEvents output to ContractEventOutput format
func (p *StellarContractEventProcessor) convertFromTransactionEvents(tx ingest.LedgerTransaction, txEvents ingest.TransactionEvents, header xdr.LedgerHeaderHistoryEntry) []contract.ContractEventOutput {
	var outputs []contract.ContractEventOutput
	
	// Convert operation events
	for _, opEvents := range txEvents.OperationEvents {
		for _, event := range opEvents {
			if output := p.convertSingleEvent(event, tx, header); output != nil {
				outputs = append(outputs, *output)
			}
		}
	}
	
	// Convert transaction events
	for _, txEvent := range txEvents.TransactionEvents {
		if output := p.convertSingleEvent(txEvent.Event, tx, header); output != nil {
			outputs = append(outputs, *output)
		}
	}
	
	return outputs
}

// convertSingleEvent converts a single XDR event to ContractEventOutput
func (p *StellarContractEventProcessor) convertSingleEvent(event xdr.ContractEvent, tx ingest.LedgerTransaction, header xdr.LedgerHeaderHistoryEntry) *contract.ContractEventOutput {
	// Get contract ID
	contractID := ""
	if encoded, err := strkey.Encode(strkey.VersionByteContract, event.ContractId[:]); err == nil {
		contractID = encoded
	}
	
	// Determine type string
	var typeString string
	switch event.Type {
	case xdr.ContractEventTypeSystem:
		typeString = "ContractEventTypeSystem"
	case xdr.ContractEventTypeContract:
		typeString = "ContractEventTypeContract"
	case xdr.ContractEventTypeDiagnostic:
		typeString = "ContractEventTypeDiagnostic"
	default:
		typeString = fmt.Sprintf("Unknown(%d)", event.Type)
	}
	
	// Marshal event XDR
	eventXDR, _ := event.MarshalBinary()
	eventXDRBase64 := base64.StdEncoding.EncodeToString(eventXDR)
	
	// Create basic topics/data maps (simplified for now)
	topics := make(map[string][]map[string]string)
	topicsDecoded := make(map[string][]map[string]string)
	data := make(map[string]string)
	dataDecoded := make(map[string]string)
	
	// For now, just indicate the type - full decoding would require more work
	data["type"] = typeString
	dataDecoded["type"] = typeString
	
	return &contract.ContractEventOutput{
		TransactionHash:          tx.Result.TransactionHash.HexString(),
		TransactionID:            int64(tx.Index),
		Successful:               tx.Result.Successful(),
		LedgerSequence:           uint32(header.Header.LedgerSeq),
		ClosedAt:                 time.Unix(int64(header.Header.ScpValue.CloseTime), 0),
		InSuccessfulContractCall: tx.Result.Successful(),
		ContractId:               contractID,
		Type:                     int32(event.Type),
		TypeString:               typeString,
		Topics:                   topics,
		TopicsDecoded:            topicsDecoded,
		Data:                     data,
		DataDecoded:              dataDecoded,
		ContractEventXDR:         eventXDRBase64,
	}
}