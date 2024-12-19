package processor

import (
	"context"
	"fmt"
	"log"
	"sync"
	"time"
)

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
	event, ok := msg.Payload.(Event)
	if !ok {
		return fmt.Errorf("expected Event, got %T", msg.Payload)
	}

	p.mu.Lock()
	p.stats.ProcessedEvents++
	p.stats.LastEventTime = time.Now()
	p.mu.Unlock()

	// Filter events by contract ID
	if event.ContractID != p.contractID {
		return nil
	}

	p.mu.Lock()
	p.stats.FilteredEvents++
	p.mu.Unlock()

	log.Printf("Processing filtered event for contract %s: %+v", p.contractID, event)

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
