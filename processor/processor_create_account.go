package processor

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log"

	"github.com/pkg/errors"
	"github.com/stellar/go/amount"
	"github.com/stellar/go/ingest"
	"github.com/stellar/go/xdr"
)

type CreateAccount struct {
	networkPassphrase string
	processors        []Processor
}

func NewCreateAccount(config map[string]interface{}) (*CreateAccount, error) {
	networkPassphrase, ok := config["network_passphrase"].(string)
	if !ok {
		return nil, fmt.Errorf("invalid configuration for CreateAccount: missing 'network_passphrase'")
	}

	return &CreateAccount{networkPassphrase: networkPassphrase}, nil
}

func (t *CreateAccount) Subscribe(receiver Processor) {
	t.processors = append(t.processors, receiver)
}

func (t *CreateAccount) Process(ctx context.Context, msg Message) error {
	log.Printf("Processing message in CreateAccount")
	ledgerCloseMeta := msg.Payload.(xdr.LedgerCloseMeta)
	ledgerTxReader, err := ingest.NewLedgerTransactionReaderFromLedgerCloseMeta(t.networkPassphrase, ledgerCloseMeta)
	if err != nil {
		return errors.Wrapf(err, "failed to create reader for ledger %v", ledgerCloseMeta.LedgerSequence())
	}
	closeTime := uint(ledgerCloseMeta.LedgerHeaderHistoryEntry().Header.ScpValue.CloseTime)

	// scan all transactions in a ledger for create account operations
	transaction, err := ledgerTxReader.Read()

	for ; err == nil; transaction, err = ledgerTxReader.Read() {
		for _, op := range transaction.Envelope.Operations() {
			if op.Body.Type == xdr.OperationTypeCreateAccount {
				createAccountOp := op.Body.MustCreateAccountOp()
				createAccount := CreateAccountOp{
					Timestamp:       fmt.Sprintf("%d", closeTime),
					Funder:          op.SourceAccount.Address(),
					Account:         createAccountOp.Destination.Address(),
					StartingBalance: amount.String(createAccountOp.StartingBalance),
					Type:            "create_account",
				}

				jsonBytes, err := json.Marshal(createAccount)
				if err != nil {
					return fmt.Errorf("error marshaling AppCreateAccount: %w", err)
				}

				for _, processor := range t.processors {
					if err := processor.Process(ctx, Message{Payload: jsonBytes}); err != nil {
						return fmt.Errorf("error processing create account message: %w", err)
					}
				}
			}
		}
	}
	if err != io.EOF {
		return errors.Wrapf(err, "failed to read transaction from ledger %v", ledgerCloseMeta.LedgerSequence())
	}
	return nil
}

type CreateAccountOp struct {
	Timestamp       string `json:"timestamp"`
	Funder          string `json:"funder"`
	Account         string `json:"account"`
	StartingBalance string `json:"starting_balance"`
	Type            string `json:"type"`
}
