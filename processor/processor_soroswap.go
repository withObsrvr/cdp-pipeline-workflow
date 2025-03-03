package processor

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"sync"
	"time"

	"github.com/stellar/go/strkey"
	"github.com/stellar/go/xdr"
)

// Event types
const (
	EventTypeNewPair = "new_pair"
	EventTypeSync    = "sync"
)

// NewPairEvent represents a new pair creation event from Soroswap
type NewPairEvent struct {
	Type           string    `json:"type"`
	Timestamp      time.Time `json:"timestamp"`
	LedgerSequence uint32    `json:"ledger_sequence"`
	ContractID     string    `json:"contract_id"`  // Factory contract ID
	PairAddress    string    `json:"pair_address"` // New pair contract address
	Token0         string    `json:"token_0"`      // First token contract ID
	Token1         string    `json:"token_1"`      // Second token contract ID
}

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
	Token0 string
	Token1 string
}

// SoroswapProcessor handles both new pair and sync events from Soroswap
type SoroswapProcessor struct {
	processors []Processor
	pairCache  map[string]*PairInfo // Cache to store pair information
	mu         sync.RWMutex
	stats      struct {
		ProcessedEvents uint64
		NewPairEvents   uint64
		SyncEvents      uint64
		LastEventTime   time.Time
	}
}

func NewSoroswapProcessor(config map[string]interface{}) (*SoroswapProcessor, error) {
	return &SoroswapProcessor{
		pairCache: make(map[string]*PairInfo),
	}, nil
}

func (p *SoroswapProcessor) Subscribe(processor Processor) {
	p.processors = append(p.processors, processor)
}

// Helper function to validate contract IDs
func validateContractID(id string) bool {
	// Soroban contract IDs should be 56 characters long
	return len(id) == 56
}

// Helper function to encode contract IDs
func encodeContractID(contractId []byte) (string, error) {
	return strkey.Encode(strkey.VersionByteContract, contractId)
}

// Process handles both new pair and sync events
func (p *SoroswapProcessor) Process(ctx context.Context, msg Message) error {
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

	// Check if we have enough topics
	if len(contractEvent.Topic) < 2 {
		return nil
	}

	// Determine event type from topics
	var eventType string
	for _, topic := range contractEvent.Topic {
		if topic.Type == xdr.ScValTypeScvSymbol {
			sym := topic.MustSym()
			if sym == "new_pair" {
				eventType = EventTypeNewPair
				break
			} else if sym == "sync" {
				eventType = EventTypeSync
				break
			}
		}
	}

	// Process based on event type
	switch eventType {
	case EventTypeNewPair:
		return p.processNewPairEvent(ctx, contractEvent)
	case EventTypeSync:
		return p.processSyncEvent(ctx, contractEvent)
	default:
		// Not an event we're interested in
		return nil
	}
}

// Process new pair events
func (p *SoroswapProcessor) processNewPairEvent(ctx context.Context, contractEvent ContractEvent) error {
	// Parse the new pair event data
	newPairEvent := NewPairEvent{
		Type:           EventTypeNewPair,
		Timestamp:      contractEvent.Timestamp,
		LedgerSequence: contractEvent.LedgerSequence,
		ContractID:     contractEvent.ContractID,
	}

	// Extract token and pair addresses from the event data
	var eventData struct {
		V0 struct {
			Data struct {
				Map []struct {
					Key struct {
						Sym string `json:"Sym"`
					} `json:"Key"`
					Val struct {
						Address struct {
							ContractId []byte `json:"ContractId"`
						} `json:"Address"`
					} `json:"Val"`
				} `json:"Map"`
			} `json:"Data"`
		} `json:"V0"`
	}

	if err := json.Unmarshal(contractEvent.Data, &eventData); err != nil {
		return fmt.Errorf("error parsing new pair event data: %w", err)
	}

	// Extract addresses from the event data
	for _, entry := range eventData.V0.Data.Map {
		switch entry.Key.Sym {
		case "token_0":
			if contractID, err := encodeContractID(entry.Val.Address.ContractId); err == nil {
				newPairEvent.Token0 = contractID
			}
		case "token_1":
			if contractID, err := encodeContractID(entry.Val.Address.ContractId); err == nil {
				newPairEvent.Token1 = contractID
			}
		case "pair":
			if contractID, err := encodeContractID(entry.Val.Address.ContractId); err == nil {
				newPairEvent.PairAddress = contractID
			}
		}
	}

	if !validateContractID(newPairEvent.PairAddress) {
		log.Printf("Warning: Invalid pair address format: %s", newPairEvent.PairAddress)
	}

	// Store pair info in cache for future sync events
	p.mu.Lock()
	p.pairCache[newPairEvent.PairAddress] = &PairInfo{
		Token0: newPairEvent.Token0,
		Token1: newPairEvent.Token1,
	}
	p.stats.ProcessedEvents++
	p.stats.NewPairEvents++
	p.stats.LastEventTime = time.Now()
	p.mu.Unlock()

	log.Printf("SoroswapProcessor: New pair created: %s (tokens: %s/%s)",
		newPairEvent.PairAddress, newPairEvent.Token0, newPairEvent.Token1)

	// Forward the new pair event to downstream processors
	newPairEventBytes, err := json.Marshal(newPairEvent)
	if err != nil {
		return fmt.Errorf("error marshaling new pair event: %w", err)
	}

	for _, processor := range p.processors {
		if err := processor.Process(ctx, Message{Payload: newPairEventBytes}); err != nil {
			return fmt.Errorf("error in processor chain: %w", err)
		}
	}

	return nil
}

// Process sync events
func (p *SoroswapProcessor) processSyncEvent(ctx context.Context, contractEvent ContractEvent) error {
	if !validateContractID(contractEvent.ContractID) {
		log.Printf("Warning: Invalid contract ID format: %s", contractEvent.ContractID)
		return nil
	}

	// Create sync event
	syncEvent := SyncEvent{
		Type:           EventTypeSync,
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

	log.Printf("SoroswapProcessor: Sync event for pair %s: reserve0=%s, reserve1=%s",
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

// GetPairInfo retrieves pair information from the cache
func (p *SoroswapProcessor) GetPairInfo(pairAddress string) (*PairInfo, bool) {
	p.mu.RLock()
	defer p.mu.RUnlock()
	pair, exists := p.pairCache[pairAddress]
	return pair, exists
}

// GetStats returns processor statistics
func (p *SoroswapProcessor) GetStats() struct {
	ProcessedEvents uint64
	NewPairEvents   uint64
	SyncEvents      uint64
	LastEventTime   time.Time
} {
	p.mu.RLock()
	defer p.mu.RUnlock()
	return p.stats
}
