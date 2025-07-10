package soroswap

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"sync"
	"time"

	"github.com/stellar/go/strkey"
	"github.com/stellar/go/xdr"
	"github.com/withObsrvr/cdp-pipeline-workflow/pkg/common/types"
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

// SoroswapPair represents a Soroswap pair
type SoroswapPair struct {
	PairAddress string    `json:"pair_address"`
	Token0      string    `json:"token_0"`
	Token1      string    `json:"token_1"`
	Reserve0    uint64    `json:"reserve_0"`
	Reserve1    uint64    `json:"reserve_1"`
	LastUpdate  time.Time `json:"last_update"`
}

// SoroswapProcessor processes Soroswap contract events
type SoroswapProcessor struct {
	factoryContractID string
	processors        []types.Processor
	mu                sync.RWMutex
	pairs             map[string]*SoroswapPair
	stats             struct {
		ProcessedEvents     uint64
		NewPairEvents       uint64
		SyncEvents          uint64
		LastProcessedTime   time.Time
		TotalValueLockedXLM float64
	}
}

// NewSoroswapProcessor creates a new SoroswapProcessor
func NewSoroswapProcessor(config map[string]interface{}) (*SoroswapProcessor, error) {
	contractID, ok := config["factory_contract_id"].(string)
	if !ok {
		return nil, fmt.Errorf("missing factory_contract_id in configuration")
	}

	log.Printf("Initializing SoroswapProcessor for factory contract: %s", contractID)

	return &SoroswapProcessor{
		factoryContractID: contractID,
		processors:        make([]types.Processor, 0),
		pairs:             make(map[string]*SoroswapPair),
	}, nil
}

// Subscribe adds a processor to the chain
func (p *SoroswapProcessor) Subscribe(processor types.Processor) {
	p.processors = append(p.processors, processor)
	log.Printf("Added processor to SoroswapProcessor, total processors: %d", len(p.processors))
}

// Process handles contract events
func (p *SoroswapProcessor) Process(ctx context.Context, msg types.Message) error {
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
	p.pairs[newPairEvent.PairAddress] = &SoroswapPair{
		PairAddress: newPairEvent.PairAddress,
		Token0:      newPairEvent.Token0,
		Token1:      newPairEvent.Token1,
	}
	p.stats.ProcessedEvents++
	p.stats.NewPairEvents++
	p.stats.LastProcessedTime = time.Now()
	p.mu.Unlock()

	log.Printf("SoroswapProcessor: New pair created: %s (tokens: %s/%s)",
		newPairEvent.PairAddress, newPairEvent.Token0, newPairEvent.Token1)

	// Forward the new pair event to downstream processors
	return p.forwardToProcessors(ctx, newPairEvent)
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
	p.stats.LastProcessedTime = time.Now()
	p.mu.Unlock()

	// Forward the sync event to downstream processors
	return p.forwardToProcessors(ctx, syncEvent)
}

// GetPairInfo retrieves pair information from the cache
func (p *SoroswapProcessor) GetPairInfo(pairAddress string) (*SoroswapPair, bool) {
	p.mu.RLock()
	defer p.mu.RUnlock()
	pair, exists := p.pairs[pairAddress]
	return pair, exists
}

// GetStats returns processor statistics
func (p *SoroswapProcessor) GetStats() struct {
	ProcessedEvents     uint64
	NewPairEvents       uint64
	SyncEvents          uint64
	LastProcessedTime   time.Time
	TotalValueLockedXLM float64
} {
	p.mu.RLock()
	defer p.mu.RUnlock()
	return p.stats
}

// ContractEvent is defined here for compatibility
type ContractEvent struct {
	Timestamp       time.Time       `json:"timestamp"`
	LedgerSequence  uint32          `json:"ledger_sequence"`
	TransactionHash string          `json:"transaction_hash"`
	ContractID      string          `json:"contract_id"`
	Type            string          `json:"type"`
	Topic           []xdr.ScVal     `json:"topic"`
	Data            json.RawMessage `json:"data"`
	InSuccessfulTx  bool            `json:"in_successful_tx"`
	EventIndex      int             `json:"event_index"`
	OperationIndex  int             `json:"operation_index"`
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

// forwardToProcessors forwards data to downstream processors
func (p *SoroswapProcessor) forwardToProcessors(ctx context.Context, data interface{}) error {
	jsonData, err := json.Marshal(data)
	if err != nil {
		return err
	}

	msg := types.Message{
		Payload: jsonData,
	}

	for _, processor := range p.processors {
		if err := processor.Process(ctx, msg); err != nil {
			return fmt.Errorf("error in processor chain: %w", err)
		}
	}

	return nil
}
