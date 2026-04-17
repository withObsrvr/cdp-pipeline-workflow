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

type BlankMessage struct {
	Timestamp       string `json:"timestamp"`
	BuyerAccountId  string `json:"buyer_account_id"`
	SellerAccountId string `json:"seller_account_id"`
	AssetCode       string `json:"asset_code"`
	Amount          string `json:"amount"`
	Type            string `json:"type"`
}

type BlankProcessor struct {
	networkPassphrase string
	processors        []Processor
}

func NewBlankProcessor(config map[string]interface{}) (*BlankProcessor, error) {
	networkPassphrase, ok := config["network_passphrase"].(string)
	if !ok {
		return nil, fmt.Errorf("invalid configuration for BlankProcessor: missing 'network_passphrase'")
	}

	return &BlankProcessor{networkPassphrase: networkPassphrase}, nil
}

func (t *BlankProcessor) Subscribe(receiver Processor) {
	t.processors = append(t.processors, receiver)
}

func (t *BlankProcessor) Process(ctx context.Context, msg Message) error {
	log.Printf("Processing message in BlankProcessor")
	ledgerCloseMeta := msg.Payload.(xdr.LedgerCloseMeta)
	ledgerTxReader, err := ingest.NewLedgerTransactionReaderFromLedgerCloseMeta(t.networkPassphrase, ledgerCloseMeta)
	if err != nil {
		return errors.Wrapf(err, "failed to create reader for ledger %v", ledgerCloseMeta.LedgerSequence())
	}
	closeTime := uint(ledgerCloseMeta.LedgerHeaderHistoryEntry().Header.ScpValue.CloseTime)

	// scan all transactions in a ledger for payments to derive new model from
	transaction, err := ledgerTxReader.Read()

	for ; err == nil; transaction, err = ledgerTxReader.Read() {

		for _, op := range transaction.Envelope.Operations() {
			switch op.Body.Type {
			case xdr.OperationTypePayment:
				networkPayment := op.Body.MustPaymentOp()
				myPayment := BlankMessage{
					Timestamp:       fmt.Sprintf("%d", closeTime),
					BuyerAccountId:  networkPayment.Destination.Address(),
					SellerAccountId: op.SourceAccount.Address(),
					AssetCode:       networkPayment.Asset.StringCanonical(),
					Amount:          amount.String(networkPayment.Amount),
					Type:            "payment",
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
