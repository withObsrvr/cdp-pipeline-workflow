package processor

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"sync"
	"time"
)

// ContractFilterProcessor filters events by contract ID
type ContractFilterProcessor struct {
	processors  []Processor
	contractIDs map[string]bool // Using a map for O(1) lookups
	mu          sync.RWMutex
	stats       struct {
		ProcessedEvents uint64
		FilteredEvents  uint64
		LastEventTime   time.Time
	}
}

func NewContractFilterProcessor(config map[string]interface{}) (*ContractFilterProcessor, error) {
	contractIDs := make(map[string]bool)

	// Handle single contract_id (string)
	if contractID, ok := config["contract_id"].(string); ok && contractID != "" {
		contractIDs[contractID] = true
	}

	// Handle contract_ids (array)
	if contractIDsArray, ok := config["contract_ids"].([]interface{}); ok {
		for _, id := range contractIDsArray {
			if strID, ok := id.(string); ok && strID != "" {
				contractIDs[strID] = true
			}
		}
	}

	if len(contractIDs) == 0 {
		return nil, fmt.Errorf("at least one contract_id must be specified (via contract_id or contract_ids)")
	}

	// Log the contract IDs we're filtering for
	log.Printf("ContractFilterProcessor: Filtering for %d contract IDs", len(contractIDs))
	for id := range contractIDs {
		log.Printf("ContractFilterProcessor: Will filter for contract ID: %s", id)
	}

	return &ContractFilterProcessor{
		contractIDs: contractIDs,
	}, nil
}

func (p *ContractFilterProcessor) Subscribe(processor Processor) {
	p.processors = append(p.processors, processor)
}

func (p *ContractFilterProcessor) Process(ctx context.Context, msg Message) error {
	// Log the payload type for debugging.
	log.Printf("ContractFilterProcessor: received message payload type: %T", msg.Payload)

	var event ContractEvent
	switch payload := msg.Payload.(type) {
	case []byte:
		if err := json.Unmarshal(payload, &event); err != nil {
			log.Printf("ContractFilterProcessor: error decoding event: %v", err)
			return nil
		}
	case ContractEvent:
		event = payload
	default:
		log.Printf("ContractFilterProcessor: unexpected payload type: %T", msg.Payload)
		return nil
	}

	// Log every contract ID encountered
	log.Printf("ContractFilterProcessor: encountered event with contractID: %s", event.ContractID)

	p.mu.Lock()
	p.stats.ProcessedEvents++
	p.stats.LastEventTime = time.Now()
	p.mu.Unlock()

	// Filter events by contract ID; log if not matching
	if !p.contractIDs[event.ContractID] {
		log.Printf("ContractFilterProcessor: event contractID %s does not match any filter; skipping", event.ContractID)
		return nil
	}

	p.mu.Lock()
	p.stats.FilteredEvents++
	p.mu.Unlock()

	log.Printf("ContractFilterProcessor: processing filtered event for contract %s", event.ContractID)

	// Forward filtered event to downstream processors with proper ContractEvent payload
	eventMsg := Message{
		Payload: &event, // Pass pointer to ContractEvent
	}

	for _, processor := range p.processors {
		if err := processor.Process(ctx, eventMsg); err != nil {
			return fmt.Errorf("error in processor chain: %w", err)
		}
	}

	return nil
}

func (p *ContractFilterProcessor) GetStats() struct {
	ProcessedEvents uint64
	FilteredEvents  uint64
	LastEventTime   time.Time
} {
	p.mu.RLock()
	defer p.mu.RUnlock()
	return p.stats
}
