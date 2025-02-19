package processor

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"sync"
	"time"
)

// Assuming Event is your contract event type (alias to ContractEvent)

type ContractFilterProcessor struct {
	processors []Processor
	contractID string
	mu         sync.RWMutex
	stats      struct {
		ProcessedEvents uint64
		FilteredEvents  uint64
		LastEventTime   time.Time
	}
}

func NewContractFilterProcessor(config map[string]interface{}) (*ContractFilterProcessor, error) {
	contractID, ok := config["contract_id"].(string)
	if !ok {
		return nil, fmt.Errorf("contract_id must be specified")
	}

	return &ContractFilterProcessor{
		contractID: contractID,
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
	if event.ContractID != p.contractID {
		log.Printf("ContractFilterProcessor: event contractID %s does not match filter %s; skipping", event.ContractID, p.contractID)
		return nil
	}

	p.mu.Lock()
	p.stats.FilteredEvents++
	p.mu.Unlock()

	log.Printf("ContractFilterProcessor: processing filtered event for contract %s: %+v", p.contractID, event)

	// Forward filtered event to downstream processors
	for _, processor := range p.processors {
		if err := processor.Process(ctx, msg); err != nil {
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
