package processor

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"strconv"

	"github.com/pkg/errors"
	"github.com/stellar/go/ingest"
	"github.com/stellar/go/xdr"
)

type AppTrustline struct {
	Timestamp          string `json:"timestamp"`
	AccountID          string `json:"account_id"`
	AssetCode          string `json:"asset_code"`
	AssetIssuer        string `json:"asset_issuer"`
	Balance            string `json:"balance"` // Current balance of the trustline
	BuyingLiabilities  string `json:"buying_liabilities"`
	SellingLiabilities string `json:"selling_liabilities"`
	Limit              string `json:"limit"`
	Action             string `json:"action"` // created, removed, updated
	Type               string `json:"type"`   // always "trustline"
	LedgerSeq          uint32 `json:"ledger_seq"`
	TxHash             string `json:"tx_hash"`    // transaction hash
	AuthFlags          uint32 `json:"auth_flags"` // authorization flags
	AuthRequired       bool   `json:"auth_required"`
	AuthRevocable      bool   `json:"auth_revocable"`
	AuthImmutable      bool   `json:"auth_immutable"`
	IsPoolShare        bool   `json:"is_pool_share"`     // Whether this is a liquidity pool share asset
	PoolID             string `json:"pool_id,omitempty"` // If pool share, the associated pool ID
	PreviousFlags      uint32 `json:"previous_flags,omitempty"`
	PreviousLimit      string `json:"previous_limit,omitempty"`
	MetaData           struct {
		LastModified       string `json:"last_modified"`
		Version            int    `json:"version"`
		LastLedgerIncrease string `json:"last_ledger_increase,omitempty"`
		LastLedgerDecrease string `json:"last_ledger_decrease,omitempty"`
	} `json:"metadata"`
}

type TransformToAppTrustline struct {
	networkPassphrase string
	processors        []Processor
}

func NewTransformToAppTrustline(config map[string]interface{}) (*TransformToAppTrustline, error) {
	networkPassphrase, ok := config["network_passphrase"].(string)
	if !ok {
		return nil, fmt.Errorf("invalid configuration for TransformToAppTrustline: missing 'network_passphrase'")
	}

	return &TransformToAppTrustline{networkPassphrase: networkPassphrase}, nil
}

func (t *TransformToAppTrustline) Subscribe(receiver Processor) {
	t.processors = append(t.processors, receiver)
}

func (t *TransformToAppTrustline) Process(ctx context.Context, msg Message) error {
	log.Printf("Processing message in TransformToAppTrustline")
	ledgerCloseMeta := msg.Payload.(xdr.LedgerCloseMeta)
	ledgerTxReader, err := ingest.NewLedgerTransactionReaderFromLedgerCloseMeta(t.networkPassphrase, ledgerCloseMeta)
	if err != nil {
		return errors.Wrapf(err, "failed to create reader for ledger %v", ledgerCloseMeta.LedgerSequence())
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
			return errors.Wrap(err, "error reading transaction")
		}

		// Get transaction hash
		txHash := tx.Result.TransactionHash.HexString()

		for opIdx, op := range tx.Envelope.Operations() {
			switch op.Body.Type {
			case xdr.OperationTypeChangeTrust:
				if err := t.processChangeTrust(ctx, op, tx, closeTime, ledgerCloseMeta.LedgerSequence(), txHash); err != nil {
					log.Printf("Error processing change trust operation %d: %v", opIdx, err)
				}
			case xdr.OperationTypeSetTrustLineFlags:
				if err := t.processSetTrustlineFlags(ctx, op, tx, closeTime, ledgerCloseMeta.LedgerSequence(), txHash); err != nil {
					log.Printf("Error processing set trustline flags operation %d: %v", opIdx, err)
				}
			}
		}
	}

	return nil
}

func (t *TransformToAppTrustline) processChangeTrust(
	ctx context.Context,
	op xdr.Operation,
	tx ingest.LedgerTransaction,
	closeTime uint,
	ledgerSeq uint32,
	txHash string,
) error {
	log.Printf("Processing ChangeTrust operation in ledger %d with hash %s", ledgerSeq, txHash)

	// Safely get the change trust operation
	changeTrustOp, ok := op.Body.GetChangeTrustOp()
	if !ok {
		return fmt.Errorf("failed to get change trust operation")
	}

	// Debug log the ChangeTrustOp structure
	log.Printf("ChangeTrustOp details: %+v", changeTrustOp)
	log.Printf("Asset Type: %v", changeTrustOp.Line.Type)

	// Get source account with careful type checking
	var sourceAccount string
	if op.SourceAccount != nil {
		sourceAccount = op.SourceAccount.ToAccountId().Address()
		log.Printf("Source Account from operation: %s", sourceAccount)
	} else {
		source := tx.Envelope.SourceAccount()
		if source == (xdr.MuxedAccount{}) {
			return fmt.Errorf("no valid source account found")
		}
		sourceAccount = source.ToAccountId().Address()
		log.Printf("Source Account from envelope: %s", sourceAccount)
	}

	// Handle assets including pool shares with nil checks
	var assetCode, assetIssuer string
	isPoolShare := false
	var poolID string

	// Determine action type
	action := "updated"
	if changeTrustOp.Limit == 0 {
		action = "removed"
	} else {
		action = "created"
	}

	// Extract trustline changes from transaction metadata with proper type checks
	var currentBalance xdr.Int64
	var buyingLiabilities xdr.Int64
	var sellingLiabilities xdr.Int64
	var lastModifiedLedgerSeq uint32
	var previousFlags uint32

	// Safely handle different asset types
	switch changeTrustOp.Line.Type {
	case xdr.AssetTypeAssetTypePoolShare:
		isPoolShare = true
		assetCode = "POOL"
		// Check if LiquidityPool pointer is not nil
		if changeTrustOp.Line.LiquidityPool != nil {
			poolID = fmt.Sprintf("pool-%s", txHash[:8])
			log.Printf("Pool parameters found: %+v", *changeTrustOp.Line.LiquidityPool)
		} else {
			poolID = "unknown-pool"
			log.Printf("No pool parameters found")
		}
		assetIssuer = poolID

	default:
		asset := changeTrustOp.Line.ToAsset()
		if asset == (xdr.Asset{}) {
			return fmt.Errorf("invalid asset in change trust operation")
		}
		assetCode = asset.GetCode()
		assetIssuer = asset.GetIssuer()
	}

	// Safely process metadata changes
	if tx.UnsafeMeta.V1 != nil && len(tx.UnsafeMeta.V1.Operations) > 0 {
		for _, change := range tx.UnsafeMeta.V1.Operations[0].Changes {
			if change.Type == xdr.LedgerEntryChangeTypeLedgerEntryState {
				if change.State != nil && change.State.Data.Type == xdr.LedgerEntryTypeTrustline {
					if tl, ok := change.State.Data.GetTrustLine(); ok {
						previousFlags = uint32(tl.Flags)
					}
				}
			} else if change.Type == xdr.LedgerEntryChangeTypeLedgerEntryUpdated {
				if change.Updated != nil && change.Updated.Data.Type == xdr.LedgerEntryTypeTrustline {
					if tl, ok := change.Updated.Data.GetTrustLine(); ok {
						currentBalance = tl.Balance
						liabilities := tl.Liabilities()
						buyingLiabilities = liabilities.Buying
						sellingLiabilities = liabilities.Selling
						lastModifiedLedgerSeq = uint32(change.Updated.LastModifiedLedgerSeq)
					}
				}
			}
		}
	}

	// Create trustline object
	trustline := AppTrustline{
		Timestamp:          fmt.Sprintf("%d", closeTime),
		AccountID:          sourceAccount,
		AssetCode:          assetCode,
		AssetIssuer:        assetIssuer,
		Balance:            strconv.FormatInt(int64(currentBalance), 10),
		BuyingLiabilities:  strconv.FormatInt(int64(buyingLiabilities), 10),
		SellingLiabilities: strconv.FormatInt(int64(sellingLiabilities), 10),
		Limit:              strconv.FormatInt(int64(changeTrustOp.Limit), 10),
		Action:             action,
		Type:               "trustline",
		LedgerSeq:          ledgerSeq,
		TxHash:             txHash,
		IsPoolShare:        isPoolShare,
		PoolID:             poolID,
		PreviousFlags:      previousFlags,
		MetaData: struct {
			LastModified       string `json:"last_modified"`
			Version            int    `json:"version"`
			LastLedgerIncrease string `json:"last_ledger_increase,omitempty"`
			LastLedgerDecrease string `json:"last_ledger_decrease,omitempty"`
		}{
			LastModified: strconv.FormatUint(uint64(lastModifiedLedgerSeq), 10),
			Version:      1,
		},
	}

	// Log the final trustline object before sending
	trustlineJSON, _ := json.MarshalIndent(trustline, "", "  ")
	log.Printf("Final trustline object:\n%s", string(trustlineJSON))

	log.Printf("Created trustline object with type=%s, asset_code=%s", trustline.Type, trustline.AssetCode)

	return t.sendTrustlineToProcessors(ctx, trustline)
}

// Helper method to find account entry changes
func (t *TransformToAppTrustline) findAccountEntry(changes []xdr.LedgerEntryChange) (*xdr.LedgerEntryChange, bool) {
	for _, change := range changes {
		if change.Type == xdr.LedgerEntryChangeTypeLedgerEntryUpdated &&
			change.Updated.Data.Type == xdr.LedgerEntryTypeAccount {
			return &change, true
		}
	}
	return nil, false
}

func (t *TransformToAppTrustline) processSetTrustlineFlags(
	ctx context.Context,
	op xdr.Operation,
	tx ingest.LedgerTransaction,
	closeTime uint,
	ledgerSeq uint32,
	txHash string,
) error {
	if op.Body.SetTrustLineFlagsOp == nil {
		return fmt.Errorf("SetTrustLineFlagsOp is nil")
	}

	trustline := AppTrustline{
		Timestamp: fmt.Sprintf("%d", closeTime),
		Type:      "trustline_flags", // Add operation type
		LedgerSeq: ledgerSeq,
		TxHash:    txHash,
	}

	// Extract asset details
	asset := op.Body.SetTrustLineFlagsOp.Asset
	if asset.Type == xdr.AssetTypeAssetTypeNative {
		trustline.AssetCode = "XLM"
		trustline.AssetIssuer = ""
	} else {
		trustline.AssetCode = asset.GetCode()
		trustline.AssetIssuer = asset.GetIssuer()
	}

	// Set account ID
	if op.SourceAccount != nil {
		trustline.AccountID = op.SourceAccount.Address()
	}

	return t.sendTrustlineToProcessors(ctx, trustline)
}

func (t *TransformToAppTrustline) sendTrustlineToProcessors(ctx context.Context, trustline AppTrustline) error {
	jsonBytes, err := json.Marshal(trustline)
	if err != nil {
		return fmt.Errorf("error marshaling trustline: %w", err)
	}

	log.Printf("Forwarding trustline payload: %s", string(jsonBytes))

	message := Message{Payload: jsonBytes}
	for _, processor := range t.processors {
		if err := processor.Process(ctx, message); err != nil {
			return fmt.Errorf("error processing trustline message: %w", err)
		}
	}

	return nil
}
