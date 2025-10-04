package processor

import (
	"context"
	"fmt"
	"io"
	"log"
	"sync"
	"time"

	"github.com/stellar/go/ingest"
	"github.com/stellar/go/processors/effects"
	"github.com/stellar/go/xdr"
)

// StellarEffectsProcessor wraps the official Stellar effects processor
type StellarEffectsProcessor struct {
	name              string
	subscribers       []Processor
	networkPassphrase string
	mu                sync.Mutex
}

// StellarEffectsMessage wraps the stellar/go EffectOutput for CDP pipeline
type StellarEffectsMessage struct {
	Effects         []effects.EffectOutput `json:"effects"`
	LedgerSequence  uint32                `json:"ledger_sequence"`
	TransactionHash string                `json:"transaction_hash"`
	Timestamp       time.Time             `json:"timestamp"`
	ProcessorName   string                `json:"processor_name"`
	MessageType     string                `json:"message_type"`
}

// NewStellarEffectsProcessor creates a new effects processor using Stellar's implementation
func NewStellarEffectsProcessor(config map[string]interface{}) (*StellarEffectsProcessor, error) {
	networkPassphrase, ok := config["network_passphrase"].(string)
	if !ok {
		return nil, fmt.Errorf("missing network_passphrase in configuration")
	}

	name, _ := config["name"].(string)
	if name == "" {
		name = "stellar_effects_processor"
	}

	return &StellarEffectsProcessor{
		name:              name,
		networkPassphrase: networkPassphrase,
		subscribers:       []Processor{},
	}, nil
}

// Subscribe adds a processor to receive effects
func (p *StellarEffectsProcessor) Subscribe(processor Processor) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.subscribers = append(p.subscribers, processor)
}

// Process extracts effects using Stellar's official processor
func (p *StellarEffectsProcessor) Process(ctx context.Context, msg Message) error {
	// Extract LedgerCloseMeta  
	ledgerCloseMeta, ok := msg.Payload.(*xdr.LedgerCloseMeta)
	if !ok {
		// Check if it's already a pass-through message
		if lcm, ok := msg.Payload.(xdr.LedgerCloseMeta); ok {
			ledgerCloseMeta = &lcm
		} else {
			return fmt.Errorf("expected LedgerCloseMeta, got %T", msg.Payload)
		}
	}

	// Process all effects in the ledger
	effectsMessages, err := p.processLedgerEffects(ctx, ledgerCloseMeta)
	if err != nil {
		return fmt.Errorf("failed to process ledger effects: %w", err)
	}

	// Create new message with original payload and updated metadata
	outputMsg := Message{
		Payload:  ledgerCloseMeta,
		Metadata: msg.Metadata,
	}

	// Initialize metadata if nil
	if outputMsg.Metadata == nil {
		outputMsg.Metadata = make(map[string]interface{})
	}

	// Add effects to metadata
	outputMsg.Metadata["effects"] = effectsMessages
	outputMsg.Metadata["processor_stellar_effects"] = true

	// Forward to subscribers
	p.mu.Lock()
	defer p.mu.Unlock()
	
	for _, subscriber := range p.subscribers {
		if err := subscriber.Process(ctx, outputMsg); err != nil {
			log.Printf("Error in subscriber processing: %v", err)
		}
	}

	return nil
}

// processLedgerEffects processes all effects in a ledger
func (p *StellarEffectsProcessor) processLedgerEffects(ctx context.Context, ledgerCloseMeta *xdr.LedgerCloseMeta) ([]StellarEffectsMessage, error) {
	ledgerHeader := ledgerCloseMeta.LedgerHeaderHistoryEntry()
	closeTime := time.Unix(int64(ledgerHeader.Header.ScpValue.CloseTime), 0)
	sequence := ledgerCloseMeta.LedgerSequence()

	// Create transaction reader
	txReader, err := ingest.NewLedgerTransactionReaderFromLedgerCloseMeta(p.networkPassphrase, *ledgerCloseMeta)
	if err != nil {
		return nil, fmt.Errorf("error creating transaction reader: %w", err)
	}
	defer txReader.Close()

	var effectsMessages []StellarEffectsMessage

	// Process each transaction
	for {
		tx, err := txReader.Read()
		if err == io.EOF {
			break
		}
		if err != nil {
			return nil, fmt.Errorf("error reading transaction: %w", err)
		}

		// Skip failed transactions
		if !tx.Result.Successful() {
			continue
		}

		// Extract effects using Stellar's TransformEffect function
		effectOutputs, err := effects.TransformEffect(
			tx,
			uint32(ledgerHeader.Header.LedgerSeq),
			*ledgerCloseMeta,
			p.networkPassphrase,
		)
		if err != nil {
			log.Printf("Error processing transaction effects: %v", err)
			continue
		}

		if len(effectOutputs) == 0 {
			continue
		}

		// Create CDP message
		effectsMsg := StellarEffectsMessage{
			Effects:         effectOutputs,
			LedgerSequence:  sequence,
			TransactionHash: tx.Result.TransactionHash.HexString(),
			Timestamp:       closeTime,
			ProcessorName:   p.name,
			MessageType:     "stellar_effects",
		}

		effectsMessages = append(effectsMessages, effectsMsg)
	}

	return effectsMessages, nil
}

// forwardToProcessors sends the message to all subscribers
func (p *StellarEffectsProcessor) forwardToProcessors(ctx context.Context, data interface{}) error {
	p.mu.Lock()
	defer p.mu.Unlock()

	for _, subscriber := range p.subscribers {
		if err := subscriber.Process(ctx, Message{Payload: data}); err != nil {
			return fmt.Errorf("error in processor chain: %w", err)
		}
	}
	return nil
}

// ProcessorStellarEffects creates a processor using Stellar's official effects implementation
func ProcessorStellarEffects(config map[string]interface{}) Processor {
	processor, err := NewStellarEffectsProcessor(config)
	if err != nil {
		log.Printf("Error creating Stellar effects processor: %v", err)
		return nil
	}
	return processor
}