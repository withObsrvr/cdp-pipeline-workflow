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
	for {
		tx, err := ledgerTxReader.Read()
		if err == io.EOF {
			break
		}
		if err != nil {
			return fmt.Errorf("error reading transaction: %w", err)
		}

		// Extract memo from transaction
		memo := extractMemo(tx.Envelope)

		// Process each operation in the transaction
		for i, op := range tx.Envelope.Operations() {
			opSourceAccount := op.SourceAccount
			if opSourceAccount == nil {
				sourceAcc := tx.Envelope.SourceAccount()
				opSourceAccount = &sourceAcc
			}
			ledgerSeq := ledgerTxReader.GetSequence()

			var payment *AppPayment
			var err error

			switch op.Body.Type {
			case xdr.OperationTypePayment:
				payment = t.createAppPaymentFromPaymentOp(op.Body.MustPaymentOp(), opSourceAccount, closeTime, ledgerSeq, memo)

			case xdr.OperationTypePathPaymentStrictReceive:
				payment = t.createAppPaymentFromPathPaymentStrictReceiveOp(op.Body.MustPathPaymentStrictReceiveOp(), opSourceAccount, closeTime, ledgerSeq, memo)

			case xdr.OperationTypePathPaymentStrictSend:
				payment = t.createAppPaymentFromPathPaymentStrictSendOp(op.Body.MustPathPaymentStrictSendOp(), opSourceAccount, closeTime, ledgerSeq, memo)

			case xdr.OperationTypeAccountMerge:
				payment, err = t.createAppPaymentFromAccountMerge(tx, i, opSourceAccount, closeTime, ledgerSeq, memo)
				if err != nil {
					return fmt.Errorf("error processing account merge: %w", err)
				}

			case xdr.OperationTypeClaimClaimableBalance:
				payment, err = t.createAppPaymentFromClaimableBalance(tx, i, opSourceAccount, closeTime, ledgerSeq, memo)
				if err != nil {
					return fmt.Errorf("error processing claimable balance: %w", err)
				}
			}

			if payment != nil {
				if err := t.forwardAppPayment(ctx, *payment); err != nil {
					return fmt.Errorf("error forwarding payment: %w", err)
				}
			}
		}
	}

	return nil
}

func extractMemo(envelope xdr.TransactionEnvelope) string {
	switch envelope.Memo().Type {
	case xdr.MemoTypeMemoText:
		return string(envelope.Memo().MustText())
	case xdr.MemoTypeMemoId:
		return fmt.Sprintf("%d", envelope.Memo().MustId())
	case xdr.MemoTypeMemoHash:
		hash := envelope.Memo().MustHash()
		return fmt.Sprintf("%x", hash)
	case xdr.MemoTypeMemoReturn:
		retHash := envelope.Memo().MustRetHash()
		return fmt.Sprintf("%x", retHash)
	default:
		return ""
	}
}

func (t *TransformToAppPayment) createAppPaymentFromPaymentOp(
	paymentOp xdr.PaymentOp,
	sourceAccount *xdr.MuxedAccount,
	closeTime uint,
	ledgerSeq uint32,
	memo string,
) *AppPayment {
	return &AppPayment{
		Timestamp:       fmt.Sprintf("%d", closeTime),
		BuyerAccountId:  paymentOp.Destination.Address(),
		SellerAccountId: sourceAccount.Address(),
		AssetCode:       paymentOp.Asset.StringCanonical(),
		Amount:          amount.String(paymentOp.Amount),
		Type:            "payment",
		LedgerSequence:  ledgerSeq,
		Memo:            memo,
	}
}

func (t *TransformToAppPayment) createAppPaymentFromPathPaymentStrictReceiveOp(
	pathPaymentOp xdr.PathPaymentStrictReceiveOp,
	sourceAccount *xdr.MuxedAccount,
	closeTime uint,
	ledgerSeq uint32,
	memo string,
) *AppPayment {
	return &AppPayment{
		Timestamp:       fmt.Sprintf("%d", closeTime),
		BuyerAccountId:  pathPaymentOp.Destination.Address(),
		SellerAccountId: sourceAccount.Address(),
		AssetCode:       pathPaymentOp.DestAsset.StringCanonical(),
		Amount:          amount.String(pathPaymentOp.DestAmount),
		Type:            "path_payment_strict_receive",
		Memo:            memo,
		LedgerSequence:  ledgerSeq,
	}
}

func (t *TransformToAppPayment) createAppPaymentFromPathPaymentStrictSendOp(
	pathPaymentOp xdr.PathPaymentStrictSendOp,
	sourceAccount *xdr.MuxedAccount,
	closeTime uint,
	ledgerSeq uint32,
	memo string,
) *AppPayment {
	return &AppPayment{
		Timestamp:       fmt.Sprintf("%d", closeTime),
		BuyerAccountId:  pathPaymentOp.Destination.Address(),
		SellerAccountId: sourceAccount.Address(),
		AssetCode:       pathPaymentOp.DestAsset.StringCanonical(),
		Amount:          amount.String(pathPaymentOp.SendAmount), // Note: actual received amount may differ
		Type:            "path_payment_strict_send",
		Memo:            memo,
		LedgerSequence:  ledgerSeq,
	}
}

func (t *TransformToAppPayment) createAppPaymentFromAccountMerge(
	tx ingest.LedgerTransaction,
	opIndex int,
	sourceAccount *xdr.MuxedAccount,
	closeTime uint,
	ledgerSeq uint32,
	memo string,
) (*AppPayment, error) {
	destination := tx.Envelope.Operations()[opIndex].Body.MustDestination()

	changes, err := tx.GetOperationChanges(uint32(opIndex))
	if err != nil {
		return nil, fmt.Errorf("error getting operation changes: %w", err)
	}

	var amountTransferred xdr.Int64
	for _, change := range changes {
		if change.Type != xdr.LedgerEntryType(xdr.LedgerEntryChangeTypeLedgerEntryRemoved) {
			continue
		}

		entry := change.Pre
		if entry == nil || entry.Data.Type != xdr.LedgerEntryTypeAccount {
			continue
		}

		account := entry.Data.Account
		if account.AccountId.Address() == sourceAccount.Address() {
			amountTransferred = account.Balance
			break
		}
	}

	return &AppPayment{
		Timestamp:       fmt.Sprintf("%d", closeTime),
		BuyerAccountId:  destination.Address(),
		SellerAccountId: sourceAccount.Address(),
		AssetCode:       "XLM", // Account merges only transfer native XLM
		Amount:          amount.String(amountTransferred),
		Type:            "account_merge",
		Memo:            memo,
		LedgerSequence:  ledgerSeq,
	}, nil
}

func (t *TransformToAppPayment) createAppPaymentFromClaimableBalance(
	tx ingest.LedgerTransaction,
	opIndex int,
	sourceAccount *xdr.MuxedAccount,
	closeTime uint,
	ledgerSeq uint32,
	memo string,
) (*AppPayment, error) {
	claimOp := tx.Envelope.Operations()[opIndex].Body.MustClaimClaimableBalanceOp()

	changes, err := tx.GetOperationChanges(uint32(opIndex))
	if err != nil {
		return nil, fmt.Errorf("error getting operation changes: %w", err)
	}

	var claimedAmount xdr.Int64
	var asset xdr.Asset
	var claimableBalance *xdr.ClaimableBalanceEntry
	for _, change := range changes {
		if change.Type != xdr.LedgerEntryType(xdr.LedgerEntryChangeTypeLedgerEntryRemoved) {
			continue
		}

		entry := change.Pre
		if entry == nil || entry.Data.Type != xdr.LedgerEntryTypeClaimableBalance {
			continue
		}

		claimableBalance := entry.Data.ClaimableBalance
		if claimableBalance.BalanceId == claimOp.BalanceId {
			claimedAmount = claimableBalance.Amount
			asset = claimableBalance.Asset
			break
		}
	}

	// Find the original creator of the claimable balance
	var creator string
	for _, claimant := range claimableBalance.Claimants {
		creator = claimant.MustV0().Destination.Address()
		break
	}

	return &AppPayment{
		Timestamp:       fmt.Sprintf("%d", closeTime),
		BuyerAccountId:  sourceAccount.Address(), // The claimer receives the funds
		SellerAccountId: creator,                 // The original creator of the balance
		AssetCode:       asset.StringCanonical(),
		Amount:          amount.String(claimedAmount),
		Type:            "claim_claimable_balance",
		Memo:            memo,
		LedgerSequence:  ledgerSeq,
	}, nil
}

func (t *TransformToAppPayment) forwardAppPayment(ctx context.Context, payment AppPayment) error {
	jsonBytes, err := json.Marshal(payment)
	if err != nil {
		return fmt.Errorf("error marshaling payment: %w", err)
	}

	for _, processor := range t.processors {
		if err := processor.Process(ctx, Message{Payload: jsonBytes}); err != nil {
			return fmt.Errorf("error in processor chain: %w", err)
		}
	}

	log.Printf("Successfully forwarded payment: %+v", payment)
	return nil
}

type AppPayment struct {
	Timestamp       string `json:"timestamp"`
	BuyerAccountId  string `json:"buyer_account_id"`
	SellerAccountId string `json:"seller_account_id"`
	AssetCode       string `json:"asset_code"`
	Amount          string `json:"amount"`
	Type            string `json:"type"`
	Memo            string `json:"memo"`
	LedgerSequence  uint32 `json:"ledger_sequence"`
}
