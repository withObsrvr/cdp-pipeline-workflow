package processor

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"strconv"
	"sync"
	"time"

	"github.com/stellar/go/ingest"
	"github.com/stellar/go/xdr"
)

type AppNetworkMetrics struct {
	Timestamp          string  `json:"timestamp"`
	LedgerSequence     uint32  `json:"ledger_sequence"`
	Type               string  `json:"type"` // Always "network_metrics"
	TransactionCount   int     `json:"transaction_count"`
	OperationCount     int     `json:"operation_count"`
	PaymentVolume      string  `json:"payment_volume"` // Total XLM volume
	FeeVolume          string  `json:"fee_volume"`
	ActiveAccounts     int     `json:"active_accounts"`  // Accounts involved in transactions
	NewAccounts        int     `json:"new_accounts"`     // New accounts created
	LedgerClosetime    uint32  `json:"ledger_closetime"` // Time taken to close ledger
	TransactionSuccess float64 `json:"tx_success_rate"`  // Success rate of transactions
	AverageFeePaid     string  `json:"average_fee_paid"`
	BaseFee            uint32  `json:"base_fee"`
	MaxFee             uint32  `json:"max_fee"`

	// Fee stats
	FeeCharged    string `json:"fee_charged"`
	MaxFeeCharged string `json:"max_fee_charged"`

	// Operation stats
	PaymentOps       int `json:"payment_ops"`
	CreateAccountOps int `json:"create_account_ops"`
	ChangeTrustOps   int `json:"change_trust_ops"`
	ManageOfferOps   int `json:"manage_offer_ops"`
	SetOptionsOps    int `json:"set_options_ops"`

	// Memory metrics
	TxSetSizeBytes int64 `json:"tx_set_size_bytes"`

	// Protocol version
	ProtocolVersion uint32 `json:"protocol_version"`
}

type TransformToAppMetrics struct {
	networkPassphrase string
	processors        []Processor
	activeAccounts    sync.Map // Track active accounts within time window
	accountTTL        time.Duration
}

func NewTransformToAppMetrics(config map[string]interface{}) (*TransformToAppMetrics, error) {
	networkPassphrase, ok := config["network_passphrase"].(string)
	if !ok {
		return nil, fmt.Errorf("invalid configuration for TransformToAppMetrics: missing 'network_passphrase'")
	}

	// Default TTL for active accounts tracking (e.g., 5 minutes)
	ttl := 5 * time.Minute
	if ttlMinutes, ok := config["active_accounts_ttl_minutes"].(float64); ok {
		ttl = time.Duration(ttlMinutes) * time.Minute
	}

	return &TransformToAppMetrics{
		networkPassphrase: networkPassphrase,
		accountTTL:        ttl,
	}, nil
}

func (t *TransformToAppMetrics) Subscribe(receiver Processor) {
	t.processors = append(t.processors, receiver)
}

func (t *TransformToAppMetrics) Process(ctx context.Context, msg Message) error {
	log.Printf("Processing message in TransformToAppMetrics")
	ledgerCloseMeta := msg.Payload.(xdr.LedgerCloseMeta)

	// Create ledger transaction reader
	ledgerTxReader, err := ingest.NewLedgerTransactionReaderFromLedgerCloseMeta(t.networkPassphrase, ledgerCloseMeta)
	if err != nil {
		return fmt.Errorf("failed to create reader for ledger %v: %w", ledgerCloseMeta.LedgerSequence(), err)
	}
	defer ledgerTxReader.Close()

	metrics := AppNetworkMetrics{
		Timestamp:       fmt.Sprintf("%d", uint(ledgerCloseMeta.LedgerHeaderHistoryEntry().Header.ScpValue.CloseTime)),
		LedgerSequence:  ledgerCloseMeta.LedgerSequence(),
		Type:            "network_metrics",
		ProtocolVersion: uint32(ledgerCloseMeta.LedgerHeaderHistoryEntry().Header.LedgerVersion),
	}

	// Process all transactions
	var totalFeePaid int64
	uniqueAccounts := make(map[string]struct{})

	var tx ingest.LedgerTransaction
	for tx, err = ledgerTxReader.Read(); err == nil; tx, err = ledgerTxReader.Read() {
		metrics.TransactionCount++

		// Track unique accounts
		sourceAccount := tx.Envelope.SourceAccount().ToAccountId().Address()
		uniqueAccounts[sourceAccount] = struct{}{}

		// Track active accounts
		t.activeAccounts.Store(sourceAccount, time.Now())

		// Track fees
		feeCharged := int64(tx.Result.Result.FeeCharged)
		totalFeePaid += feeCharged
		if feeCharged > mustParseInt64(metrics.MaxFeeCharged) {
			metrics.MaxFeeCharged = fmt.Sprintf("%d", feeCharged)
		}

		// Track operation counts
		for _, op := range tx.Envelope.Operations() {
			metrics.OperationCount++

			switch op.Body.Type {
			case xdr.OperationTypePayment:
				metrics.PaymentOps++
				payment := op.Body.MustPaymentOp()
				if payment.Asset.Type == xdr.AssetTypeAssetTypeNative {
					// Track XLM payment volume
					volume := mustParseInt64(metrics.PaymentVolume)
					volume += int64(payment.Amount)
					metrics.PaymentVolume = fmt.Sprintf("%d", volume)
				}
			case xdr.OperationTypeCreateAccount:
				metrics.CreateAccountOps++
				metrics.NewAccounts++
			case xdr.OperationTypeChangeTrust:
				metrics.ChangeTrustOps++
			case xdr.OperationTypeManageBuyOffer, xdr.OperationTypeManageSellOffer:
				metrics.ManageOfferOps++
			case xdr.OperationTypeSetOptions:
				metrics.SetOptionsOps++
			}

			// Track operation accounts
			if op.SourceAccount != nil {
				opAccount := op.SourceAccount.ToAccountId().Address()
				uniqueAccounts[opAccount] = struct{}{}
			}
		}

		// Track transaction success rate
		if tx.Result.Successful() {
			metrics.TransactionSuccess++
		}
	}

	// Calculate final metrics
	metrics.ActiveAccounts = len(uniqueAccounts)
	if metrics.TransactionCount > 0 {
		metrics.TransactionSuccess = (metrics.TransactionSuccess / float64(metrics.TransactionCount)) * 100
		metrics.AverageFeePaid = fmt.Sprintf("%d", totalFeePaid/int64(metrics.TransactionCount))
	}

	metrics.BaseFee = uint32(ledgerCloseMeta.LedgerHeaderHistoryEntry().Header.BaseFee)
	metrics.MaxFee = uint32(ledgerCloseMeta.LedgerHeaderHistoryEntry().Header.MaxTxSetSize)
	metrics.FeeVolume = fmt.Sprintf("%d", totalFeePaid)

	// Calculate tx set size
	if v0 := ledgerCloseMeta.V0; v0 != nil {
		metrics.TxSetSizeBytes = int64(len(v0.TxSet.Txs))
	} else if v1 := ledgerCloseMeta.V1; v1 != nil {
		metrics.TxSetSizeBytes = int64(len(v1.TxSet.V1TxSet.Phases))
	}

	// Send metrics through processors
	jsonBytes, err := json.Marshal(metrics)
	if err != nil {
		return fmt.Errorf("error marshaling metrics: %w", err)
	}

	message := Message{Payload: jsonBytes}
	for _, processor := range t.processors {
		if err := processor.Process(ctx, message); err != nil {
			return fmt.Errorf("error processing metrics message: %w", err)
		}
	}

	return nil
}

// Helper function to parse int64 with default value
func mustParseInt64(s string) int64 {
	if s == "" {
		return 0
	}
	v, err := strconv.ParseInt(s, 10, 64)
	if err != nil {
		return 0
	}
	return v
}
