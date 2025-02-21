package processor

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"sync"
	"time"

	"github.com/stellar/go/xdr"
)

// SyncEvent represents the structure of a Soroswap sync event
type SyncEvent struct {
	Type           string    `json:"type"`
	Timestamp      time.Time `json:"timestamp"`
	LedgerSequence uint32    `json:"ledger_sequence"`
	ContractID     string    `json:"contract_id"` // This should be the pair address
	NewReserve0    string    `json:"new_reserve_0"`
	NewReserve1    string    `json:"new_reserve_1"`
}

// PairInfo stores the token information for a pair
type PairInfo struct {
	TokenA string
	TokenB string
}

type SoroswapSyncProcessor struct {
	processors []Processor
	pairCache  map[string]*PairInfo // Cache to store pair information
	mu         sync.RWMutex
	stats      struct {
		ProcessedEvents uint64
		SyncEvents      uint64
		LastEventTime   time.Time
	}
}

func NewSoroswapSyncProcessor(config map[string]interface{}) (*SoroswapSyncProcessor, error) {
	return &SoroswapSyncProcessor{
		pairCache: make(map[string]*PairInfo),
	}, nil
}

func (p *SoroswapSyncProcessor) Subscribe(processor Processor) {
	p.processors = append(p.processors, processor)
}

// getPairInfo retrieves or fetches pair information
func (p *SoroswapSyncProcessor) getPairInfo(contractID string) (*PairInfo, error) {
	p.mu.RLock()
	if pair, exists := p.pairCache[contractID]; exists {
		p.mu.RUnlock()
		return pair, nil
	}
	p.mu.RUnlock()

	// TODO: Implement fetching pair info from the Soroban network
	// This would involve making a contract call to get token addresses
	// For now, we'll return a placeholder

	// Example implementation:
	pair := &PairInfo{
		TokenA: "placeholder_token_a",
		TokenB: "placeholder_token_b",
	}

	p.mu.Lock()
	p.pairCache[contractID] = pair
	p.mu.Unlock()

	return pair, nil
}

func validateContractID(id string) bool {
	log.Printf("Validating contract ID: %s (length: %d)", id, len(id))
	// Soroban contract IDs should be 56 characters long
	return len(id) == 56
}

func (p *SoroswapSyncProcessor) Process(ctx context.Context, msg Message) error {
	var contractEvent ContractEvent
	switch payload := msg.Payload.(type) {
	case []byte:
		if err := json.Unmarshal(payload, &contractEvent); err != nil {
			return fmt.Errorf("error decoding contract event: %w", err)
		}
	case ContractEvent:
		contractEvent = payload
	default:
		return fmt.Errorf("unexpected payload type: %T", msg.Payload)
	}

	// Check if this is a sync event
	if len(contractEvent.Topic) < 2 {
		return nil
	}

	var isSync bool
	for _, topic := range contractEvent.Topic {
		if topic.Type == xdr.ScValTypeScvSymbol {
			if sym := topic.MustSym(); sym == "sync" {
				isSync = true
				break
			}
		}
	}

	if !isSync {
		return nil
	}

	if !validateContractID(contractEvent.ContractID) {
		log.Printf("Warning: Invalid contract ID format: %s", contractEvent.ContractID)
		return nil
	}

	// Create sync event
	syncEvent := SyncEvent{
		Type:           "sync",
		Timestamp:      contractEvent.Timestamp,
		LedgerSequence: contractEvent.LedgerSequence,
		ContractID:     contractEvent.ContractID, // This is the pair address
	}

	// Parse the event data
	var eventData struct {
		V0 struct {
			Data struct {
				Map []struct {
					Key struct {
						Sym string `json:"Sym"`
					} `json:"Key"`
					Val struct {
						I128 struct {
							Lo uint64 `json:"Lo"`
						} `json:"I128"`
					} `json:"Val"`
				} `json:"Map"`
			} `json:"Data"`
		} `json:"V0"`
	}

	log.Printf("Processing sync event for pair: %s", contractEvent.ContractID)
	log.Printf("Raw contract event data: %+v", string(contractEvent.Data))

	if err := json.Unmarshal(contractEvent.Data, &eventData); err != nil {
		return fmt.Errorf("error parsing sync event data: %w", err)
	}

	// Extract reserve values
	for _, entry := range eventData.V0.Data.Map {
		switch entry.Key.Sym {
		case "new_reserve_0":
			syncEvent.NewReserve0 = fmt.Sprintf("%d", entry.Val.I128.Lo)
		case "new_reserve_1":
			syncEvent.NewReserve1 = fmt.Sprintf("%d", entry.Val.I128.Lo)
		}
	}

	log.Printf("Extracted reserves for pair %s: reserve0=%s, reserve1=%s",
		syncEvent.ContractID, syncEvent.NewReserve0, syncEvent.NewReserve1)

	// Update stats
	p.mu.Lock()
	p.stats.ProcessedEvents++
	p.stats.SyncEvents++
	p.stats.LastEventTime = time.Now()
	p.mu.Unlock()

	// Forward the sync event to downstream processors
	syncEventBytes, err := json.Marshal(syncEvent)
	if err != nil {
		return fmt.Errorf("error marshaling sync event: %w", err)
	}

	for _, processor := range p.processors {
		if err := processor.Process(ctx, Message{Payload: syncEventBytes}); err != nil {
			return fmt.Errorf("error in processor chain: %w", err)
		}
	}

	return nil
}

func (p *SoroswapSyncProcessor) GetStats() struct {
	ProcessedEvents uint64
	SyncEvents      uint64
	LastEventTime   time.Time
} {
	p.mu.RLock()
	defer p.mu.RUnlock()
	return p.stats
}
