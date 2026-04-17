package processor

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"strconv"

	"github.com/stellar/go/amount"
	"github.com/stellar/go/xdr"
)

type FilterPayments struct {
	minAmount         float64
	assetCode         string
	processors        []Processor
	networkPassphrase string
}

type FilteredPayment struct {
	Timestamp       string `json:"timestamp"`
	BuyerAccountId  string `json:"buyer_account_id"`
	SellerAccountId string `json:"seller_account_id"`
	AssetCode       string `json:"asset_code"`
	Amount          string `json:"amount"`
	Type            string `json:"type"`
	Memo            string `json:"memo"`
	LedgerSeqNum    uint32 `json:"ledger_seq_num"`
}

func NewFilterPayments(config map[string]interface{}) (*FilterPayments, error) {
	minAmountStr, ok := config["min_amount"].(string)
	if !ok {
		return nil, fmt.Errorf("invalid configuration for FilterPayments: missing 'min_amount'")
	}
	minAmount, err := strconv.ParseFloat(minAmountStr, 64)
	if err != nil {
		return nil, fmt.Errorf("invalid 'min_amount' value: %v", err)
	}

	assetCode, ok := config["asset_code"].(string)
	if !ok {
		return nil, fmt.Errorf("invalid configuration for FilterPayments: missing 'asset_code'")
	}
	networkPassphrase, ok := config["network_passphrase"].(string)
	if !ok {
		return nil, fmt.Errorf("invalid configuration for FilterPayments: missing 'network_passphrase'")
	}

	return &FilterPayments{minAmount: minAmount, assetCode: assetCode, networkPassphrase: networkPassphrase}, nil
}

func (f *FilterPayments) Subscribe(receiver Processor) {
	f.processors = append(f.processors, receiver)
}

func (f *FilterPayments) Process(ctx context.Context, msg Message) error {
	log.Printf("Processing message in FilterPayments")

	// Unmarshal JSON payload into TransactionMessage
	var txMsg TransactionMessage
	if jsonBytes, ok := msg.Payload.([]uint8); ok {
		if err := json.Unmarshal(jsonBytes, &txMsg); err != nil {
			return fmt.Errorf("error unmarshaling transaction message: %w", err)
		}
	} else {
		return fmt.Errorf("invalid payload type: expected []uint8, got %T", msg.Payload)
	}

	closeTime := txMsg.CloseTime
	transaction := txMsg.Transaction

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
	log.Printf("Total operations in transaction: %d", len(transaction.Envelope.Operations()))
	for _, op := range transaction.Envelope.Operations() {
		switch op.Body.Type {
		case xdr.OperationTypePayment:
			networkPayment := op.Body.MustPaymentOp()
			myPayment := FilteredPayment{
				Timestamp:       fmt.Sprintf("%d", closeTime),
				BuyerAccountId:  networkPayment.Destination.Address(),
				SellerAccountId: op.SourceAccount.Address(),
				AssetCode:       networkPayment.Asset.StringCanonical(),
				Amount:          amount.String(networkPayment.Amount),
				Type:            "payment",
				Memo:            memo,
				LedgerSeqNum:    txMsg.LedgerSeqNum,
			}
			jsonBytes, err := json.Marshal(myPayment)
			if err != nil {
				return err
			}
			amountFloat, err := strconv.ParseFloat(myPayment.Amount, 64)
			if err != nil {
				return fmt.Errorf("invalid amount: %v", err)
			}
			if myPayment.AssetCode == f.assetCode && amountFloat >= f.minAmount {
				for _, processor := range f.processors {
					if err := processor.Process(ctx, Message{Payload: jsonBytes}); err != nil {
						return fmt.Errorf("error processing message: %w", err)
					}
				}
			}
		}
	}

	return nil

}
