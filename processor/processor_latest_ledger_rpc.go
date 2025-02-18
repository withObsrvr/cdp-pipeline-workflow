package processor

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"time"
)

// LatestLedger holds ledger details. In a full implementation, you might enrich this
// data with additional fields (like hash, transaction counts, etc.) by fetching more details.
type LatestLedgerRPC struct {
	Sequence          uint32    `json:"sequence"`
	Hash              string    `json:"hash,omitempty"`
	TransactionCount  int       `json:"transaction_count,omitempty"`
	OperationCount    int       `json:"operation_count,omitempty"`
	SuccessfulTxCount int       `json:"successful_tx_count,omitempty"`
	FailedTxCount     int       `json:"failed_tx_count,omitempty"`
	TotalFeeCharged   int64     `json:"total_fee_charged,omitempty"`
	ClosedAt          time.Time `json:"closed_at,omitempty"`
	BaseFee           uint32    `json:"base_fee,omitempty"`

	// Soroban metrics
	SorobanTxCount            int    `json:"soroban_tx_count,omitempty"`
	TotalSorobanFees          int64  `json:"total_soroban_fees,omitempty"`
	TotalResourceInstructions uint64 `json:"total_resource_instructions,omitempty"`
}

// LatestLedgerProcessor transforms raw getLatestLedger data (a uint64 ledger sequence)
// into a LatestLedger message (marshaled as JSON bytes) and passes it to downstream processors.
type LatestLedgerRPCProcessor struct {
	networkPassphrase string
	processors        []Processor
}

// NewLatestLedgerRPCProcessor creates a new LatestLedgerRPCProcessor.
// Expects a "network_passphrase" entry in the configuration.
func NewLatestLedgerRPCProcessor(config map[string]interface{}) (*LatestLedgerRPCProcessor, error) {
	networkPassphrase, ok := config["network_passphrase"].(string)
	if !ok {
		return nil, fmt.Errorf("missing network_passphrase in config")
	}

	return &LatestLedgerRPCProcessor{
		networkPassphrase: networkPassphrase,
		processors:        make([]Processor, 0),
	}, nil
}

// Subscribe adds a downstream processor.
func (p *LatestLedgerRPCProcessor) Subscribe(proc Processor) {
	p.processors = append(p.processors, proc)
	log.Printf("LatestLedgerRPCProcessor: subscribed downstream processor: %T", proc)
}

// Process expects a Message whose Payload is a uint64 ledger sequence.
// It transforms this value into a LatestLedgerRPC struct, marshals it to JSON, and passes it downstream.
func (p *LatestLedgerRPCProcessor) Process(ctx context.Context, msg Message) error {
	// Expect the payload to be a uint64 ledger sequence.
	ledgerSeq, ok := msg.Payload.(uint64)
	if !ok {
		return fmt.Errorf("LatestLedgerRPCProcessor expected payload of type uint64, got %T", msg.Payload)
	}

	// Create LatestLedger. In a full implementation, additional details could be fetched.
	latestLedger := LatestLedgerRPC{
		Sequence: uint32(ledgerSeq),
		ClosedAt: time.Now(), // This is just a placeholder.
	}

	log.Printf("LatestLedgerRPCProcessor: processed ledger sequence %d", ledgerSeq)

	// Marshal the latestLedger struct to JSON so downstream processors receive []byte.
	data, err := json.Marshal(latestLedger)
	if err != nil {
		return fmt.Errorf("failed to marshal latest ledger: %w", err)
	}

	newMsg := Message{
		Payload: data,
	}

	// Forward the new message to all subscribed downstream processors.
	for _, proc := range p.processors {
		if err := proc.Process(ctx, newMsg); err != nil {
			log.Printf("LatestLedgerRPCProcessor: downstream processor error: %v", err)
		}
	}
	return nil
}
