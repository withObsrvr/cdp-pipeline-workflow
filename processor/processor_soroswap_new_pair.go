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

// NewPairEvent represents a new pair creation event from Soroswap
type NewPairEvent struct {
	Type           string    `json:"type"` // Add this field
	Timestamp      time.Time `json:"timestamp"`
	LedgerSequence uint32    `json:"ledger_sequence"`
	ContractID     string    `json:"contract_id"`  // Factory contract ID
	PairAddress    string    `json:"pair_address"` // New pair contract address
	Token0         string    `json:"token_0"`      // First token contract ID
	Token1         string    `json:"token_1"`      // Second token contract ID
}

type SoroswapNewPairProcessor struct {
	processors []Processor
	mu         sync.RWMutex
	stats      struct {
		ProcessedEvents uint64
		NewPairEvents   uint64
		LastEventTime   time.Time
	}
}

func NewSoroswapNewPairProcessor(config map[string]interface{}) (*SoroswapNewPairProcessor, error) {
	return &SoroswapNewPairProcessor{}, nil
}

func (p *SoroswapNewPairProcessor) Subscribe(processor Processor) {
	p.processors = append(p.processors, processor)
}

func (p *SoroswapNewPairProcessor) Process(ctx context.Context, msg Message) error {
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

	// Check if this is a new_pair event by examining the topics
	if len(contractEvent.Topic) < 2 {
		return nil
	}

	// Look for "new_pair" in the topics
	var isNewPair bool
	for _, topic := range contractEvent.Topic {
		if topic.Type == xdr.ScValTypeScvSymbol {
			if sym := topic.MustSym(); sym == "new_pair" {
				isNewPair = true
				break
			}
		}
	}

	if !isNewPair {
		return nil
	}

	// Parse the new pair event data
	newPairEvent := NewPairEvent{
		Type:           "new_pair", // Set the type
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

	log.Printf("SoroswapNewPairProcessor: New pair created: %s (tokens: %s/%s)",
		newPairEvent.PairAddress, newPairEvent.Token0, newPairEvent.Token1)

	// Update stats
	p.mu.Lock()
	p.stats.ProcessedEvents++
	p.stats.NewPairEvents++
	p.stats.LastEventTime = time.Now()
	p.mu.Unlock()

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

func (p *SoroswapNewPairProcessor) GetStats() struct {
	ProcessedEvents uint64
	NewPairEvents   uint64
	LastEventTime   time.Time
} {
	p.mu.RLock()
	defer p.mu.RUnlock()
	return p.stats
}

// Helper function to encode contract IDs
func encodeContractID(contractId []byte) (string, error) {
	return strkey.Encode(strkey.VersionByteContract, contractId)
}
