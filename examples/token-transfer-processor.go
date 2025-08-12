package main

import (
	"context"
	"fmt"
	"io"

	"github.com/stellar/go/ingest"
	"github.com/stellar/go/xdr"
	"github.com/withObsrvr/cdp-pipeline-workflow/pkg/helpers/stellar"
	"github.com/withObsrvr/cdp-pipeline-workflow/processor"
)

// TokenTransferProcessor demonstrates best practices from Stellar's token processor
type TokenTransferProcessor struct {
	processors []processor.Processor
	errCtx     *stellar.ErrorContext
}

func NewTokenTransferProcessor() *TokenTransferProcessor {
	return &TokenTransferProcessor{
		errCtx: stellar.NewErrorContext("TokenTransferProcessor"),
	}
}

func (p *TokenTransferProcessor) Process(ctx context.Context, msg processor.Message) error {
	ledgerCloseMeta, ok := msg.Payload.(xdr.LedgerCloseMeta)
	if !ok {
		return fmt.Errorf("expected LedgerCloseMeta, got %T", msg.Payload)
	}

	ledgerSeq := ledgerCloseMeta.LedgerSequence()
	p.errCtx.WithLedger(ledgerSeq)

	reader, err := ingest.NewLedgerTransactionReaderFromLedgerCloseMeta(
		"Test SDF Network ; September 2015", ledgerCloseMeta)
	if err != nil {
		return p.errCtx.Wrap(err)
	}
	defer reader.Close()

	// Batch events for efficiency
	events := make([]TokenTransferEvent, 0, 100)

	for {
		tx, err := reader.Read()
		if err == io.EOF {
			break
		}
		if err != nil {
			return p.errCtx.Wrap(err)
		}

		// Early return pattern - skip failed transactions
		if !tx.Result.Successful() {
			continue
		}

		p.errCtx.WithTransaction(tx.Result.TransactionHash.HexString())

		// Process each operation
		for i, op := range tx.Envelope.Operations() {
			p.errCtx.WithOperation(i)

			// Only process payment operations
			if op.Body.Type != xdr.OperationTypePayment {
				continue
			}

			event, err := p.extractPaymentEvent(tx, i, op)
			if err != nil {
				// Log but continue processing
				fmt.Printf("Warning: %v\n", p.errCtx.Wrap(err))
				continue
			}

			if event != nil {
				events = append(events, *event)
			}
		}
	}

	// Process batch
	if len(events) > 0 {
		return p.processBatch(ctx, events)
	}

	return nil
}

type TokenTransferEvent struct {
	Type            stellar.TokenEventType `json:"type"`
	From            string                 `json:"from"`
	To              string                 `json:"to"`
	AssetCode       string                 `json:"asset_code"`
	AssetIssuer     string                 `json:"asset_issuer"`
	Amount          string                 `json:"amount"`
	LedgerSequence  uint32                 `json:"ledger_sequence"`
	TransactionHash string                 `json:"transaction_hash"`
	OperationIndex  int                    `json:"operation_index"`
}

func (p *TokenTransferProcessor) extractPaymentEvent(
	tx ingest.LedgerTransaction,
	opIndex int,
	op xdr.Operation,
) (*TokenTransferEvent, error) {
	payment := op.Body.MustPayment()

	// Get source account
	source, err := stellar.GetOperationSourceAccount(op, tx.Envelope.SourceAccount())
	if err != nil {
		return nil, fmt.Errorf("getting source account: %w", err)
	}

	// Get destination
	dest := payment.Destination.Address()

	// Validate addresses
	if err := stellar.ValidateAddress(source); err != nil {
		return nil, stellar.ValidationError{
			Field:   "source",
			Value:   source,
			Message: "invalid source address",
		}
	}

	if err := stellar.ValidateAddress(dest); err != nil {
		return nil, stellar.ValidationError{
			Field:   "destination",
			Value:   dest,
			Message: "invalid destination address",
		}
	}

	// Extract asset details
	assetCode := stellar.GetAssetCode(payment.Asset)
	assetIssuer := stellar.GetAssetIssuer(payment.Asset)

	// Determine event type (mint/burn/transfer)
	eventType := stellar.DetermineTokenEventType(source, dest, assetIssuer)

	// Format amount with proper decimals
	amount := stellar.FormatAmount(payment.Amount)

	return &TokenTransferEvent{
		Type:            eventType,
		From:            source,
		To:              dest,
		AssetCode:       assetCode,
		AssetIssuer:     assetIssuer,
		Amount:          amount,
		LedgerSequence:  tx.LedgerSequence(),
		TransactionHash: tx.Result.TransactionHash.HexString(),
		OperationIndex:  opIndex,
	}, nil
}

func (p *TokenTransferProcessor) processBatch(ctx context.Context, events []TokenTransferEvent) error {
	// In real implementation, would forward to downstream processors
	fmt.Printf("Processing batch of %d token transfer events\n", len(events))

	// Example: Group by event type for analytics
	stats := map[stellar.TokenEventType]int{}
	for _, event := range events {
		stats[event.Type]++
	}

	fmt.Printf("Event statistics: %+v\n", stats)
	return nil
}

func (p *TokenTransferProcessor) Subscribe(processor processor.Processor) {
	p.processors = append(p.processors, processor)
}

// Example verification pattern from Stellar's processor
func VerifyTokenTransfers(events []TokenTransferEvent, source xdr.LedgerCloseMeta) error {
	// Extract expected events from source
	expected := extractExpectedTransfers(source)

	if len(events) != len(expected) {
		return fmt.Errorf("event count mismatch: got %d, expected %d",
			len(events), len(expected))
	}

	// Verify each event matches
	for i := range events {
		if events[i].TransactionHash != expected[i].TransactionHash {
			return fmt.Errorf("transaction hash mismatch at index %d", i)
		}
		// Add more verification as needed
	}

	return nil
}

func extractExpectedTransfers(meta xdr.LedgerCloseMeta) []TokenTransferEvent {
	// Implementation would extract transfers directly from ledger meta
	// for verification purposes
	return []TokenTransferEvent{}
}