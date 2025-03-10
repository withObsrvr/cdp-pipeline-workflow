package processor

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"strconv"
	"time"

	"github.com/stellar/go/amount"
	"github.com/stellar/go/ingest"
	"github.com/stellar/go/ingest/ledger"
	"github.com/stellar/go/xdr"
)

type TransformToAppPayment struct {
	minAmount         *float64
	assetCode         *string
	addresses         []string
	memoText          *string
	networkPassphrase string
	processors        []Processor
}

func NewTransformToAppPayment(config map[string]interface{}) (*TransformToAppPayment, error) {
	networkPassphrase, ok := config["network_passphrase"].(string)
	if !ok {
		return nil, fmt.Errorf("invalid configuration for TransformToAppPayment: missing 'network_passphrase'")
	}

	t := &TransformToAppPayment{
		networkPassphrase: networkPassphrase,
	}

	// Optional min_amount
	if minAmountStr, ok := config["min_amount"].(string); ok && minAmountStr != "" {
		minAmount, err := strconv.ParseFloat(minAmountStr, 64)
		if err != nil {
			return nil, fmt.Errorf("invalid 'min_amount' value: %v", err)
		}
		t.minAmount = &minAmount
	}

	// Optional asset_code
	if assetCode, ok := config["asset_code"].(string); ok && assetCode != "" {
		t.assetCode = &assetCode
	}

	// Optional addresses
	if addresses, ok := config["addresses"].([]string); ok && len(addresses) > 0 {
		t.addresses = addresses
	}

	// Optional memo_text
	if memoText, ok := config["memo_text"].(string); ok && memoText != "" {
		t.memoText = &memoText
	}

	return t, nil
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
		return fmt.Errorf("failed to create reader for ledger %v: %w", ledger.Sequence(ledgerCloseMeta), err)
	}
	defer ledgerTxReader.Close()

	// Use the new ledger utility function to get a proper timestamp.
	closedAt := ledger.ClosedAt(ledgerCloseMeta)
	closedAtStr := closedAt.UTC().Format(time.RFC3339)

	// Optionally, you could also extract the ledger hash if needed.
	ledgerHash := ledger.Hash(ledgerCloseMeta)

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
				payment = t.createAppPaymentFromPaymentOp(op.Body.MustPaymentOp(), opSourceAccount, uint(closedAt.Unix()), ledgerSeq, memo)

			case xdr.OperationTypePathPaymentStrictReceive:
				payment = t.createAppPaymentFromPathPaymentStrictReceiveOp(op.Body.MustPathPaymentStrictReceiveOp(), opSourceAccount, uint(closedAt.Unix()), ledgerSeq, memo)

			case xdr.OperationTypePathPaymentStrictSend:
				payment = t.createAppPaymentFromPathPaymentStrictSendOp(op.Body.MustPathPaymentStrictSendOp(), opSourceAccount, uint(closedAt.Unix()), ledgerSeq, memo)

			case xdr.OperationTypeAccountMerge:
				payment, err = t.createAppPaymentFromAccountMerge(tx, i, opSourceAccount, uint(closedAt.Unix()), ledgerSeq, memo)
				if err != nil {
					return fmt.Errorf("error processing account merge: %w", err)
				}

			case xdr.OperationTypeClaimClaimableBalance:
				payment, err = t.createAppPaymentFromClaimableBalance(tx, i, opSourceAccount, uint(closedAt.Unix()), ledgerSeq, memo)
				if err != nil {
					return fmt.Errorf("error processing claimable balance: %w", err)
				}
			}

			if payment != nil {
				// You can attach additional ledger metadata to your payment if desired.
				payment.LedgerHash = ledgerHash
				payment.ClosedAt = closedAtStr

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

func getAccountAddress(account *xdr.MuxedAccount) string {
	if account == nil {
		return ""
	}
	addr, err := account.GetAddress()
	if err == nil && addr != "" {
		return addr
	}
	return account.Address()
}

func (t *TransformToAppPayment) createAppPaymentFromPaymentOp(
	paymentOp xdr.PaymentOp,
	sourceAccount *xdr.MuxedAccount,
	closeTime uint,
	ledgerSeq uint32,
	memo string,
) *AppPayment {
	return &AppPayment{
		Timestamp:            fmt.Sprintf("%d", closeTime),
		SourceAccountId:      getAccountAddress(&paymentOp.Destination),
		DestinationAccountId: getAccountAddress(sourceAccount),
		AssetCode:            paymentOp.Asset.StringCanonical(),
		Amount:               amount.String(paymentOp.Amount),
		Type:                 "payment",
		LedgerSequence:       ledgerSeq,
		Memo:                 memo,
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
		Timestamp:            fmt.Sprintf("%d", closeTime),
		SourceAccountId:      pathPaymentOp.Destination.Address(),
		DestinationAccountId: sourceAccount.Address(),
		AssetCode:            pathPaymentOp.DestAsset.StringCanonical(),
		Amount:               amount.String(pathPaymentOp.DestAmount),
		Type:                 "path_payment_strict_receive",
		Memo:                 memo,
		LedgerSequence:       ledgerSeq,
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
		Timestamp:            fmt.Sprintf("%d", closeTime),
		SourceAccountId:      pathPaymentOp.Destination.Address(),
		DestinationAccountId: sourceAccount.Address(),
		AssetCode:            pathPaymentOp.DestAsset.StringCanonical(),
		Amount:               amount.String(pathPaymentOp.SendAmount), // Note: actual received amount may differ
		Type:                 "path_payment_strict_send",
		Memo:                 memo,
		LedgerSequence:       ledgerSeq,
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

	log.Printf("Processing account merge from %s to %s", sourceAccount.Address(), destination.Address())
	log.Printf("Found %d changes for operation", len(changes))

	var sourceBalance xdr.Int64
	var destBalanceDiff xdr.Int64

	// Analyze all changes to find source and destination balances
	for _, change := range changes {
		log.Printf("Change type: %T = %v", change.Type, change.Type)

		if change.Pre != nil && change.Pre.Data.Type == xdr.LedgerEntryTypeAccount {
			preAccount := change.Pre.Data.MustAccount()

			// If this is the source account, record its balance
			if preAccount.AccountId.Address() == sourceAccount.Address() {
				sourceBalance = preAccount.Balance
				log.Printf("Found source account balance: %s", amount.String(sourceBalance))
			}

			// If this is the destination account, calculate balance difference
			if preAccount.AccountId.Address() == destination.Address() && change.Post != nil {
				postAccount := change.Post.Data.MustAccount()
				destBalanceDiff = postAccount.Balance - preAccount.Balance
				log.Printf("Found destination balance change: %s", amount.String(destBalanceDiff))
			}
		}
	}

	// Verify we found the necessary information
	if sourceBalance == 0 {
		log.Printf("Could not find source account balance")
		return nil, fmt.Errorf("could not find source account balance")
	}

	// Use the source balance as the transfer amount
	amountTransferred := sourceBalance

	log.Printf("Account merge details:")
	log.Printf("  Source balance: %s", amount.String(sourceBalance))
	log.Printf("  Destination balance change: %s", amount.String(destBalanceDiff))
	log.Printf("  Transfer amount: %s", amount.String(amountTransferred))

	return &AppPayment{
		Timestamp:            fmt.Sprintf("%d", closeTime),
		SourceAccountId:      destination.Address(),
		DestinationAccountId: sourceAccount.Address(),
		AssetCode:            "native",
		Amount:               amount.String(amountTransferred),
		Type:                 "account_merge",
		Memo:                 memo,
		LedgerSequence:       ledgerSeq,
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
	var creator string

	for _, change := range changes {
		if xdr.LedgerEntryChangeType(change.Type) != xdr.LedgerEntryChangeTypeLedgerEntryRemoved {
			continue
		}

		entry := change.Pre
		if entry == nil || entry.Data.Type != xdr.LedgerEntryTypeClaimableBalance {
			continue
		}

		balanceEntry := entry.Data.MustClaimableBalance()
		if balanceEntry.BalanceId == claimOp.BalanceId {
			claimedAmount = balanceEntry.Amount
			asset = balanceEntry.Asset
			// Get the first claimant as the creator
			if len(balanceEntry.Claimants) > 0 {
				creator = balanceEntry.Claimants[0].MustV0().Destination.Address()
			}
			break
		}
	}

	// If we couldn't find a creator, use a placeholder
	if creator == "" {
		creator = "unknown"
	}

	return &AppPayment{
		Timestamp:            fmt.Sprintf("%d", closeTime),
		SourceAccountId:      sourceAccount.Address(), // The claimer receives the funds
		DestinationAccountId: creator,                 // The original creator of the balance
		AssetCode:            asset.StringCanonical(),
		Amount:               amount.String(claimedAmount),
		Type:                 "claim_claimable_balance",
		Memo:                 memo,
		LedgerSequence:       ledgerSeq,
	}, nil
}

// Helper function to check if a payment should be processed
func (t *TransformToAppPayment) shouldProcessPayment(payment AppPayment) bool {
	// Track if we have any filters at all
	hasFilters := false

	// Check min amount if specified
	if t.minAmount != nil {
		hasFilters = true
		paymentAmount, err := strconv.ParseFloat(payment.Amount, 64)
		if err != nil {
			log.Printf("Warning: Could not parse payment amount %s: %v", payment.Amount, err)
			return false
		}
		if paymentAmount < *t.minAmount {
			log.Printf("Payment amount %f is below minimum %f", paymentAmount, *t.minAmount)
			return false
		}
	}

	// Check asset code if specified
	if t.assetCode != nil {
		hasFilters = true
		if payment.AssetCode != *t.assetCode {
			log.Printf("Payment asset code %s does not match filter %s", payment.AssetCode, *t.assetCode)
			return false
		}
	}

	// Check addresses if specified
	if len(t.addresses) > 0 {
		hasFilters = true
		addressMatch := false
		for _, addr := range t.addresses {
			if payment.SourceAccountId == addr || payment.DestinationAccountId == addr {
				addressMatch = true
				break
			}
		}
		if !addressMatch {
			log.Printf("Neither source %s nor destination %s match address filters",
				payment.SourceAccountId, payment.DestinationAccountId)
			return false
		}
	}

	// Check memo text if specified
	if t.memoText != nil {
		hasFilters = true
		if payment.Memo != *t.memoText {
			log.Printf("Payment memo %q does not match filter %q", payment.Memo, *t.memoText)
			return false
		}
	}

	// If no filters were specified, return true
	// If any filters were specified, we've already checked them all must match
	if hasFilters {
		log.Printf("Payment matched all specified filters")
	} else {
		log.Printf("No filters specified, accepting all payments")
	}

	return true
}

func (t *TransformToAppPayment) forwardAppPayment(ctx context.Context, payment AppPayment) error {
	// Start instrumentation timer.
	startTime := time.Now()

	// Check if payment meets filter criteria.
	if !t.shouldProcessPayment(payment) {
		log.Printf("Skipping payment that doesn't meet filter criteria: %+v", payment)
		return nil
	}

	// Mark the payment as successful since it passed all processing steps.
	payment.Successful = true

	jsonBytes, err := json.Marshal(payment)
	if err != nil {
		return fmt.Errorf("error marshaling payment: %w", err)
	}

	for _, processor := range t.processors {
		if err := processor.Process(ctx, Message{Payload: jsonBytes}); err != nil {
			return fmt.Errorf("error in processor chain: %w", err)
		}
	}

	elapsed := time.Since(startTime)
	// Log the processing time if it exceeds 10ms (adjust threshold as needed)
	if elapsed > 10*time.Millisecond {
		log.Printf("forwardAppPayment took %s for payment: %+v", elapsed, payment)
	} else {
		log.Printf("Processed payment in %s", elapsed)
	}

	log.Printf("Successfully forwarded payment: %+v", payment)
	return nil
}

type AppPayment struct {
	Timestamp            string `json:"timestamp"`
	SourceAccountId      string `json:"source_account_id"`
	DestinationAccountId string `json:"destination_account_id"`
	AssetCode            string `json:"asset_code"`
	Amount               string `json:"amount"`
	Type                 string `json:"type"`
	Memo                 string `json:"memo"`
	LedgerSequence       uint32 `json:"ledger_sequence"`
	// New fields
	FeeCharged      string `json:"fee_charged,omitempty"`
	FeePayer        string `json:"fee_payer,omitempty"`
	Successful      bool   `json:"successful"`
	ResultCode      string `json:"result_code,omitempty"`
	TransactionHash string `json:"transaction_hash,omitempty"`
	LedgerHash      string `json:"ledger_hash,omitempty"`
	ClosedAt        string `json:"closed_at,omitempty"`
}
