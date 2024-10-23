package main

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

type TransformToAppPayment struct {
	networkPassphrase string
	processors        []Processor
}

func NewTransformToAppPayment(config map[string]interface{}) (*TransformToAppPayment, error) {
	networkPassphrase, ok := config["network_passphrase"].(string)
	if !ok {
		return nil, fmt.Errorf("invalid configuration for TransformToAppPayment: missing 'network_passphrase'")
	}

	return &TransformToAppPayment{networkPassphrase: networkPassphrase}, nil
}

func (t *TransformToAppPayment) Subscribe(receiver Processor) {
	t.processors = append(t.processors, receiver)
}

func (t *TransformToAppPayment) Process(ctx context.Context, msg Message) error {
	log.Printf("Processing message in TransformToAppPayment")
	ledgerCloseMeta := msg.Payload.(xdr.LedgerCloseMeta)
	ledgerTxReader, err := ingest.NewLedgerTransactionReaderFromLedgerCloseMeta(t.networkPassphrase, ledgerCloseMeta)
	if err != nil {
		return errors.Wrapf(err, "failed to create reader for ledger %v", ledgerCloseMeta.LedgerSequence())
	}
	closeTime := uint(ledgerCloseMeta.LedgerHeaderHistoryEntry().Header.ScpValue.CloseTime)

	// scan all transactions in a ledger for payments to derive new model from
	transaction, err := ledgerTxReader.Read()

	for ; err == nil; transaction, err = ledgerTxReader.Read() {
		// Extract memo from transaction
		memo := ""
		switch transaction.Envelope.Memo().Type {
		case xdr.MemoTypeMemoText:
			memo = string(transaction.Envelope.Memo().MustText())
		case xdr.MemoTypeMemoId:
			memo = fmt.Sprintf("%d", transaction.Envelope.Memo().MustId())
		case xdr.MemoTypeMemoHash:
			hash := transaction.Envelope.Memo().MustHash()
			memo = fmt.Sprintf("%x", hash)
		case xdr.MemoTypeMemoReturn:
			retHash := transaction.Envelope.Memo().MustRetHash()
			memo = fmt.Sprintf("%x", retHash)
		}
		for _, op := range transaction.Envelope.Operations() {
			switch op.Body.Type {
			case xdr.OperationTypePayment:
				networkPayment := op.Body.MustPaymentOp()
				myPayment := AppPayment{
					Timestamp:       fmt.Sprintf("%d", closeTime),
					BuyerAccountId:  networkPayment.Destination.Address(),
					SellerAccountId: op.SourceAccount.Address(),
					AssetCode:       networkPayment.Asset.StringCanonical(),
					Amount:          amount.String(networkPayment.Amount),
					Type:            "payment",
					Memo:            memo,
				}
				jsonBytes, err := json.Marshal(myPayment)
				if err != nil {
					return err
				}

				for _, processor := range t.processors {
					if err := processor.Process(ctx, Message{Payload: jsonBytes}); err != nil {
						return fmt.Errorf("error processing message: %w", err)
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
