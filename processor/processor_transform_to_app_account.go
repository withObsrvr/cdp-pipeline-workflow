package processor

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log"

	"github.com/stellar/go/amount"
	"github.com/stellar/go/ingest"
	"github.com/stellar/go/xdr"
)

// TransformToAppAccount represents a processor that transforms account data
type TransformToAppAccount struct {
	networkPassphrase string
	processors        []Processor
}

// NewTransformToAppAccount creates a new TransformToAppAccount processor
func NewTransformToAppAccount(config map[string]interface{}) (*TransformToAppAccount, error) {
	networkPassphrase, ok := config["network_passphrase"].(string)
	if !ok {
		return nil, fmt.Errorf("invalid configuration for TransformToAppAccount: missing 'network_passphrase'")
	}

	return &TransformToAppAccount{networkPassphrase: networkPassphrase}, nil
}

// Subscribe registers a downstream processor
func (t *TransformToAppAccount) Subscribe(processor Processor) {
	t.processors = append(t.processors, processor)
}

// Process processes the incoming messages and transforms them into account data
func (t *TransformToAppAccount) Process(ctx context.Context, msg Message) error {
	log.Printf("Processing message in TransformToAppAccount")

	ledgerCloseMeta, ok := msg.Payload.(xdr.LedgerCloseMeta)
	if !ok {
		return fmt.Errorf("expected LedgerCloseMeta, got %T", msg.Payload)
	}

	ledgerTxReader, err := ingest.NewLedgerTransactionReaderFromLedgerCloseMeta(t.networkPassphrase, ledgerCloseMeta)
	if err != nil {
		return fmt.Errorf("failed to create reader for ledger %v: %w", ledgerCloseMeta.LedgerSequence(), err)
	}
	defer ledgerTxReader.Close()

	closeTime := uint(ledgerCloseMeta.LedgerHeaderHistoryEntry().Header.ScpValue.CloseTime)

	// Process all transactions in the ledger
	var tx ingest.LedgerTransaction
	for {
		tx, err = ledgerTxReader.Read()
		if err == io.EOF {
			break
		}
		if err != nil {
			return fmt.Errorf("error reading transaction: %w", err)
		}

		// Process each operation in the transaction
		for _, op := range tx.Envelope.Operations() {
			if err := t.processOperation(ctx, op, closeTime, tx.Result.TransactionHash.HexString()); err != nil {
				log.Printf("Error processing operation: %v", err)
			}
		}
	}

	return nil
}

// processOperation processes individual operations that affect accounts
func (t *TransformToAppAccount) processOperation(ctx context.Context, op xdr.Operation, closeTime uint, txHash string) error {
	switch op.Body.Type {
	case xdr.OperationTypeCreateAccount:
		createAccountOp := op.Body.MustCreateAccountOp()
		return t.processCreateAccount(ctx, createAccountOp, op, closeTime, txHash)
	case xdr.OperationTypePayment:
		paymentOp := op.Body.MustPaymentOp()
		return t.processPayment(ctx, paymentOp, op, closeTime, txHash)
	}
	return nil
}

// processCreateAccount handles create account operations
func (t *TransformToAppAccount) processCreateAccount(ctx context.Context, op xdr.CreateAccountOp, operation xdr.Operation, closeTime uint, txHash string) error {
	var sourceAccount string
	if operation.SourceAccount != nil {
		sourceAccount = operation.SourceAccount.Address()
	}

	account := CreateAccountOp{
		Timestamp:       fmt.Sprintf("%d", closeTime),
		Funder:          sourceAccount,
		Account:         op.Destination.Address(),
		StartingBalance: amount.String(op.StartingBalance),
		Type:            "create_account",
	}

	// Marshal to JSON bytes
	jsonBytes, err := json.Marshal(account)
	if err != nil {
		return fmt.Errorf("error marshaling create account data: %w", err)
	}

	// Send to downstream processors
	for _, processor := range t.processors {
		if err := processor.Process(ctx, Message{Payload: jsonBytes}); err != nil {
			return fmt.Errorf("error processing create account: %w", err)
		}
	}
	return nil
}

// processPayment handles payment operations
func (t *TransformToAppAccount) processPayment(ctx context.Context, op xdr.PaymentOp, operation xdr.Operation, closeTime uint, txHash string) error {
	var sourceAccount string
	if operation.SourceAccount != nil {
		sourceAccount = operation.SourceAccount.Address()
	}

	payment := AppPayment{
		Timestamp:            fmt.Sprintf("%d", closeTime),
		SourceAccountId:      sourceAccount,
		DestinationAccountId: op.Destination.Address(),
		AssetCode:            op.Asset.StringCanonical(),
		Amount:               amount.String(op.Amount),
		Type:                 "payment",
	}

	// Marshal to JSON bytes
	jsonBytes, err := json.Marshal(payment)
	if err != nil {
		return fmt.Errorf("error marshaling payment data: %w", err)
	}

	// Send to downstream processors
	for _, processor := range t.processors {
		if err := processor.Process(ctx, Message{Payload: jsonBytes}); err != nil {
			return fmt.Errorf("error processing payment: %w", err)
		}
	}
	return nil
}
