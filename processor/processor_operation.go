package processor

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"sync"
	"time"

	"github.com/stellar/go/ingest"
	"github.com/stellar/go/xdr"
)

// OperationEvent represents a processed operation event
type OperationEvent struct {
	Type               string                 `json:"type"`
	SourceAccount      string                 `json:"source_account"`
	SourceAccountMuxed string                 `json:"source_account_muxed,omitempty"`
	OperationType      int32                  `json:"operation_type"`
	OperationTypeStr   string                 `json:"operation_type_string"`
	TransactionID      int64                  `json:"transaction_id"`
	OperationID        int64                  `json:"operation_id"`
	LedgerSequence     uint32                 `json:"ledger_sequence"`
	ApplicationOrder   uint32                 `json:"application_order"`
	ClosedAt           time.Time              `json:"closed_at"`
	Details            map[string]interface{} `json:"details"`
	ResultCode         string                 `json:"result_code"`
	TxHash             string                 `json:"tx_hash"`
}

type OperationProcessor struct {
	networkPassphrase string
	processors        []Processor
	mu                sync.RWMutex
	stats             struct {
		ProcessedOperations uint64
		SuccessOperations   uint64
		FailedOperations    uint64
		LastOperationTime   time.Time
	}
}

func NewOperationProcessor(config map[string]interface{}) (*OperationProcessor, error) {
	networkPassphrase, ok := config["network_passphrase"].(string)
	if !ok {
		return nil, fmt.Errorf("missing network_passphrase in configuration")
	}

	return &OperationProcessor{
		networkPassphrase: networkPassphrase,
	}, nil
}

func (p *OperationProcessor) Subscribe(processor Processor) {
	p.processors = append(p.processors, processor)
}

func (p *OperationProcessor) Process(ctx context.Context, msg Message) error {
	lcm, ok := msg.Payload.(xdr.LedgerCloseMeta)
	if !ok {
		return fmt.Errorf("expected LedgerCloseMeta, got %T", msg.Payload)
	}

	txReader, err := ingest.NewLedgerTransactionReaderFromLedgerCloseMeta(p.networkPassphrase, lcm)
	if err != nil {
		return fmt.Errorf("error creating transaction reader: %w", err)
	}
	defer txReader.Close()

	sequence := lcm.LedgerSequence()
	closeTime := lcm.LedgerHeaderHistoryEntry().Header.ScpValue.CloseTime

	var applicationOrder uint32

	for {
		tx, err := txReader.Read()
		if err == io.EOF {
			break
		}
		if err != nil {
			return fmt.Errorf("error reading transaction: %w", err)
		}

		// Process each operation in the transaction
		for opIndex, op := range tx.Envelope.Operations() {
			applicationOrder++

			event, err := p.transformOperation(
				op,
				tx,
				sequence,
				uint32(opIndex),
				applicationOrder,
				closeTime,
			)
			if err != nil {
				log.Printf("Error transforming operation: %v", err)
				continue
			}

			// Update stats
			p.updateStats(event.ResultCode)

			// Forward to downstream processors
			if err := p.forwardToProcessors(ctx, event); err != nil {
				return fmt.Errorf("error forwarding event: %w", err)
			}
		}
	}

	return nil
}

func (p *OperationProcessor) transformOperation(
	op xdr.Operation,
	tx ingest.LedgerTransaction,
	ledgerSequence uint32,
	opIndex uint32,
	applicationOrder uint32,
	closeTime xdr.TimePoint,
) (*OperationEvent, error) {
	sourceAccount := op.SourceAccount
	var sourceAccountAddress, sourceAccountMuxed string

	if sourceAccount != nil {
		sourceAccountAddress = sourceAccount.Address()
		if sourceAccount.Type == xdr.CryptoKeyTypeKeyTypeMuxedEd25519 {
			sourceAccountMuxed = sourceAccount.Address()
		}
	} else {
		txSourceAccount := tx.Envelope.SourceAccount()
		sourceAccountAddress = txSourceAccount.Address()
		if txSourceAccount.Type == xdr.CryptoKeyTypeKeyTypeMuxedEd25519 {
			sourceAccountMuxed = txSourceAccount.Address()
		}
	}

	details, err := extractOperationDetails(op)
	if err != nil {
		return nil, fmt.Errorf("error extracting operation details: %w", err)
	}

	resultCode := extractResultCode(tx, int(opIndex))
	closedAt := time.Unix(int64(closeTime), 0)

	return &OperationEvent{
		Type:               "operation",
		SourceAccount:      sourceAccountAddress,
		SourceAccountMuxed: sourceAccountMuxed,
		OperationType:      int32(op.Body.Type),
		OperationTypeStr:   op.Body.Type.String(),
		TransactionID:      int64(tx.Index),
		OperationID:        int64(opIndex),
		LedgerSequence:     ledgerSequence,
		ApplicationOrder:   applicationOrder,
		ClosedAt:           closedAt,
		Details:            details,
		ResultCode:         resultCode,
		TxHash:             tx.Result.TransactionHash.HexString(),
	}, nil
}

func extractOperationDetails(op xdr.Operation) (map[string]interface{}, error) {
	details := make(map[string]interface{})

	switch op.Body.Type {
	case xdr.OperationTypeCreateAccount:
		if createAccountOp, ok := op.Body.GetCreateAccountOp(); ok {
			details["starting_balance"] = float64(createAccountOp.StartingBalance) / 10000000.0
			details["destination"] = createAccountOp.Destination.Address()
		}
	case xdr.OperationTypePayment:
		if paymentOp, ok := op.Body.GetPaymentOp(); ok {
			details["amount"] = float64(paymentOp.Amount) / 10000000.0
			details["destination"] = paymentOp.Destination.Address()
			assetDetails := extractAssetDetails(paymentOp.Asset)
			details["asset"] = assetDetails
		}
	case xdr.OperationTypePathPaymentStrictReceive:
		if pathPaymentOp, ok := op.Body.GetPathPaymentStrictReceiveOp(); ok {
			details["destination"] = pathPaymentOp.Destination.Address()
			details["send_asset"] = extractAssetDetails(pathPaymentOp.SendAsset)
			details["send_max"] = float64(pathPaymentOp.SendMax) / 10000000.0
			details["dest_asset"] = extractAssetDetails(pathPaymentOp.DestAsset)
			details["dest_amount"] = float64(pathPaymentOp.DestAmount) / 10000000.0
			details["path"] = extractPath(pathPaymentOp.Path)
		}
	case xdr.OperationTypePathPaymentStrictSend:
		if pathPaymentOp, ok := op.Body.GetPathPaymentStrictSendOp(); ok {
			details["destination"] = pathPaymentOp.Destination.Address()
			details["send_asset"] = extractAssetDetails(pathPaymentOp.SendAsset)
			details["send_amount"] = float64(pathPaymentOp.SendAmount) / 10000000.0
			details["dest_asset"] = extractAssetDetails(pathPaymentOp.DestAsset)
			details["dest_min"] = float64(pathPaymentOp.DestMin) / 10000000.0
			details["path"] = extractPath(pathPaymentOp.Path)
		}
	case xdr.OperationTypeManageSellOffer:
		if sellOfferOp, ok := op.Body.GetManageSellOfferOp(); ok {
			details["selling"] = extractAssetDetails(sellOfferOp.Selling)
			details["buying"] = extractAssetDetails(sellOfferOp.Buying)
			details["amount"] = float64(sellOfferOp.Amount) / 10000000.0
			details["price"] = float64(sellOfferOp.Price.N) / float64(sellOfferOp.Price.D)
			details["offer_id"] = uint64(sellOfferOp.OfferId)
		}
	case xdr.OperationTypeCreateClaimableBalance:
		if claimBalanceOp, ok := op.Body.GetCreateClaimableBalanceOp(); ok {
			details["asset"] = extractAssetDetails(claimBalanceOp.Asset)
			details["amount"] = float64(claimBalanceOp.Amount) / 10000000.0
			claimants := make([]map[string]interface{}, len(claimBalanceOp.Claimants))
			for i, claimant := range claimBalanceOp.Claimants {
				claimants[i] = map[string]interface{}{
					"destination": claimant.MustV0().Destination.Address(),
					"predicate":   claimant.MustV0().Predicate,
				}
			}
			details["claimants"] = claimants
		}
	case xdr.OperationTypeClaimClaimableBalance:
		if claimOp, ok := op.Body.GetClaimClaimableBalanceOp(); ok {
			balanceID, err := xdr.MarshalHex(claimOp.BalanceId)
			if err != nil {
				return nil, fmt.Errorf("error marshaling balance ID: %w", err)
			}
			details["balance_id"] = balanceID
		}
	}

	return details, nil
}

func extractAssetDetails(asset xdr.Asset) map[string]interface{} {
	details := make(map[string]interface{})

	switch asset.Type {
	case xdr.AssetTypeAssetTypeNative:
		details["type"] = "native"
	case xdr.AssetTypeAssetTypeCreditAlphanum4:
		assetCode4 := asset.MustAlphaNum4()
		details["type"] = "credit_alphanum4"
		details["code"] = string(bytes.TrimRight(assetCode4.AssetCode[:], "\x00"))
		details["issuer"] = assetCode4.Issuer.Address()
	case xdr.AssetTypeAssetTypeCreditAlphanum12:
		assetCode12 := asset.MustAlphaNum12()
		details["type"] = "credit_alphanum12"
		details["code"] = string(bytes.TrimRight(assetCode12.AssetCode[:], "\x00"))
		details["issuer"] = assetCode12.Issuer.Address()
	}

	return details
}

func extractPath(path []xdr.Asset) []map[string]interface{} {
	result := make([]map[string]interface{}, len(path))
	for i, asset := range path {
		result[i] = extractAssetDetails(asset)
	}
	return result
}

func extractResultCode(tx ingest.LedgerTransaction, opIndex int) string {
	results, ok := tx.Result.Result.OperationResults()
	if !ok || opIndex >= len(results) {
		return "unknown"
	}
	return results[opIndex].Code.String()
}

func (p *OperationProcessor) updateStats(resultCode string) {
	p.mu.Lock()
	defer p.mu.Unlock()

	p.stats.ProcessedOperations++
	p.stats.LastOperationTime = time.Now()

	if resultCode == "txSUCCESS" {
		p.stats.SuccessOperations++
	} else {
		p.stats.FailedOperations++
	}
}

func (p *OperationProcessor) forwardToProcessors(ctx context.Context, event *OperationEvent) error {
	jsonBytes, err := json.Marshal(event)
	if err != nil {
		return fmt.Errorf("error marshaling event: %w", err)
	}

	for _, processor := range p.processors {
		if err := processor.Process(ctx, Message{Payload: jsonBytes}); err != nil {
			return fmt.Errorf("error in processor chain: %w", err)
		}
	}
	return nil
}

func (p *OperationProcessor) GetStats() struct {
	ProcessedOperations uint64
	SuccessOperations   uint64
	FailedOperations    uint64
	LastOperationTime   time.Time
} {
	p.mu.RLock()
	defer p.mu.RUnlock()
	return p.stats
}
