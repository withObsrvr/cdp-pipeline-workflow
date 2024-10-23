package main

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
	Timestamp     string `json:"timestamp"`
	AccountID     string `json:"account_id"`
	AssetCode     string `json:"asset_code"`
	AssetIssuer   string `json:"asset_issuer"`
	Limit         string `json:"limit"`
	Action        string `json:"action"` // created, removed, updated
	Type          string `json:"type"`   // always "trustline"
	LedgerSeq     uint32 `json:"ledger_seq"`
	TxHash        string `json:"tx_hash"`    // transaction hash
	AuthFlags     uint32 `json:"auth_flags"` // authorization flags
	AuthRequired  bool   `json:"auth_required"`
	AuthRevocable bool   `json:"auth_revocable"`
	AuthImmutable bool   `json:"auth_immutable"`
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
	changeTrustOp := op.Body.MustChangeTrustOp()
	asset := changeTrustOp.Line.ToAsset()
	action := "updated"

	// Determine if this is a create or remove operation
	if changeTrustOp.Limit == 0 {
		action = "removed"
	} else {
		// Check if this is the first time this trustline is being created
		// Note: This is a simplified check. In a production environment,
		// you might want to maintain state to accurately determine this.
		action = "created"
	}

	trustline := AppTrustline{
		Timestamp:   fmt.Sprintf("%d", closeTime),
		AccountID:   op.SourceAccount.ToAccountId().Address(),
		AssetCode:   asset.GetCode(),
		AssetIssuer: asset.GetIssuer(),
		Limit:       strconv.FormatInt(int64(changeTrustOp.Limit), 10),
		Action:      action,
		Type:        "trustline",
		LedgerSeq:   ledgerSeq,
		TxHash:      txHash,
	}

	// Get the account flags if available
	if tx.UnsafeMeta.V1.TxChanges != nil {
		for _, change := range tx.UnsafeMeta.V1.TxChanges {
			if change.Type == xdr.LedgerEntryChangeType(xdr.LedgerEntryTypeAccount) {
				account := change.Updated.Data.Account
				trustline.AuthFlags = uint32(account.Flags)
				trustline.AuthRequired = account.Flags&xdr.Uint32(xdr.AccountFlagsAuthRequiredFlag) != 0
				trustline.AuthRevocable = account.Flags&xdr.Uint32(xdr.AccountFlagsAuthRevocableFlag) != 0
				trustline.AuthImmutable = account.Flags&xdr.Uint32(xdr.AccountFlagsAuthImmutableFlag) != 0
				break
			}
		}
	}

	return t.sendTrustlineToProcessors(ctx, trustline)
}

func (t *TransformToAppTrustline) processSetTrustlineFlags(
	ctx context.Context,
	op xdr.Operation,
	tx ingest.LedgerTransaction,
	closeTime uint,
	ledgerSeq uint32,
	txHash string,
) error {
	setFlagsOp := op.Body.MustSetTrustLineFlagsOp()
	asset := setFlagsOp.Asset

	trustline := AppTrustline{
		Timestamp:   fmt.Sprintf("%d", closeTime),
		AccountID:   op.SourceAccount.ToAccountId().Address(),
		AssetCode:   asset.GetCode(),
		AssetIssuer: asset.GetIssuer(),
		Action:      "updated",
		Type:        "trustline",
		LedgerSeq:   ledgerSeq,
		TxHash:      txHash,
	}

	// Process the changes in flags
	for _, change := range tx.UnsafeMeta.V1.Operations[0].Changes {
		if change.Type == xdr.LedgerEntryChangeType(xdr.LedgerEntryTypeTrustline) {
			if tl := change.Updated.Data.TrustLine; tl != nil {
				trustline.Limit = strconv.FormatInt(int64(tl.Limit), 10)
				trustline.AuthFlags = uint32(tl.Flags)
			}
		}
	}

	return t.sendTrustlineToProcessors(ctx, trustline)
}

func (t *TransformToAppTrustline) sendTrustlineToProcessors(ctx context.Context, trustline AppTrustline) error {
	jsonBytes, err := json.Marshal(trustline)
	if err != nil {
		return fmt.Errorf("error marshaling trustline: %w", err)
	}

	message := Message{Payload: jsonBytes}
	for _, processor := range t.processors {
		if err := processor.Process(ctx, message); err != nil {
			return fmt.Errorf("error processing trustline message: %w", err)
		}
	}

	return nil
}
