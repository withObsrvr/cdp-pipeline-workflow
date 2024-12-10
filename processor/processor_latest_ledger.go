package processor

import (
	"context"
	"encoding/json"
	"fmt"
	"log"

	"github.com/stellar/go/xdr"
)

type LatestLedger struct {
	Sequence uint32 `json:"sequence"`
	Hash     string `json:"hash"`
}

type LatestLedgerProcessor struct {
	processors []Processor
}

func NewLatestLedgerProcessor(config map[string]interface{}) (*LatestLedgerProcessor, error) {
	return &LatestLedgerProcessor{}, nil
}

func (p *LatestLedgerProcessor) Subscribe(processor Processor) {
	p.processors = append(p.processors, processor)
}

func (p *LatestLedgerProcessor) Process(ctx context.Context, msg Message) error {
	ledgerCloseMeta, ok := msg.Payload.(xdr.LedgerCloseMeta)
	if !ok {
		return fmt.Errorf("expected xdr.LedgerCloseMeta, got %T", msg.Payload)
	}

	latestLedger := LatestLedger{
		Sequence: ledgerCloseMeta.LedgerSequence(),
		Hash:     ledgerCloseMeta.LedgerHash().HexString(),
	}

	jsonBytes, err := json.Marshal(latestLedger)
	if err != nil {
		return fmt.Errorf("error marshaling latest ledger: %w", err)
	}

	log.Printf("Latest ledger: %d", latestLedger.Sequence)

	// Forward to next processors
	for _, processor := range p.processors {
		if err := processor.Process(ctx, Message{Payload: jsonBytes}); err != nil {
			return fmt.Errorf("error in processor chain: %w", err)
		}
	}

	return nil
}
