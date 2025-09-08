package processor

import (
	"context"
	"encoding/base64"
	"fmt"
	"time"

	"github.com/stellar/go/ingest"
	"github.com/stellar/go/support/log"
	"github.com/stellar/go/xdr"
)

// TransactionXDROutput represents the extracted XDR data from a transaction
type TransactionXDROutput struct {
	// Transaction identification
	LedgerSequence   uint32    `json:"ledger_sequence"`
	TransactionHash  string    `json:"transaction_hash"`
	TransactionIndex int32     `json:"transaction_index"`
	Timestamp        time.Time `json:"timestamp"`

	// Raw XDR data (base64 encoded)
	EnvelopeXDR   string `json:"envelope_xdr"`    // Full transaction envelope
	ResultXDR     string `json:"result_xdr"`      // Transaction result
	ResultMetaXDR string `json:"result_meta_xdr"` // Transaction metadata

	// Parsed basic info for quick access
	SourceAccount  string `json:"source_account"`
	Success        bool   `json:"success"`
	OperationCount int    `json:"operation_count"`
	FeeCharged     int64  `json:"fee_charged"`

	// Operations with individual XDR
	Operations []OperationXDR `json:"operations"`
}

// OperationXDR represents individual operation XDR data
type OperationXDR struct {
	Index         int    `json:"index"`
	Type          string `json:"type"`
	SourceAccount string `json:"source_account"`
	OperationXDR  string `json:"operation_xdr"` // Raw operation XDR
}

// TransactionXDRExtractor extracts and preserves raw XDR data from transactions
type TransactionXDRExtractor struct {
	subscribers       []Processor
	networkPassphrase string
}

// ProcessorTransactionXDRExtractor creates a new TransactionXDRExtractor processor
func ProcessorTransactionXDRExtractor(config map[string]interface{}) *TransactionXDRExtractor {
	networkPassphrase := "Test SDF Network ; September 2015" // Default to testnet
	if np, ok := config["network_passphrase"].(string); ok {
		networkPassphrase = np
	}
	
	return &TransactionXDRExtractor{
		subscribers:       []Processor{},
		networkPassphrase: networkPassphrase,
	}
}

// Subscribe adds a subscriber to receive processed messages
func (p *TransactionXDRExtractor) Subscribe(processor Processor) {
	p.subscribers = append(p.subscribers, processor)
}

// Process handles incoming messages and extracts XDR data
func (p *TransactionXDRExtractor) Process(ctx context.Context, msg Message) error {
	startTime := time.Now()
	defer func() {
		duration := time.Since(startTime)
		log.Debugf("TransactionXDRExtractor processing took %v", duration)
	}()

	ledgerCloseMeta, ok := msg.Payload.(*xdr.LedgerCloseMeta)
	if !ok {
		// Check if it's already a pass-through message
		if lcm, ok := msg.Payload.(xdr.LedgerCloseMeta); ok {
			ledgerCloseMeta = &lcm
		} else {
			log.Debugf("TransactionXDRExtractor received unsupported message type: %T", msg.Payload)
			return nil
		}
	}

	// Process and add to metadata
	xdrOutputs, err := p.extractAllTransactionXDR(ctx, ledgerCloseMeta)
	if err != nil {
		return fmt.Errorf("failed to extract XDR data: %w", err)
	}

	// Create new message with original payload and updated metadata
	outputMsg := Message{
		Payload: ledgerCloseMeta,
		Metadata: msg.Metadata,
	}

	// Initialize metadata if nil
	if outputMsg.Metadata == nil {
		outputMsg.Metadata = make(map[string]interface{})
	}

	// Add XDR outputs to metadata
	outputMsg.Metadata["xdr_outputs"] = xdrOutputs
	outputMsg.Metadata["processor_transaction_xdr"] = true

	// Forward to subscribers
	for _, subscriber := range p.subscribers {
		if err := subscriber.Process(ctx, outputMsg); err != nil {
			log.Errorf("Error in subscriber processing: %v", err)
		}
	}

	return nil
}

// extractAllTransactionXDR extracts XDR data from all transactions in a ledger
func (p *TransactionXDRExtractor) extractAllTransactionXDR(ctx context.Context, ledgerCloseMeta *xdr.LedgerCloseMeta) ([]TransactionXDROutput, error) {
	ledgerTxReader, err := ingest.NewLedgerTransactionReaderFromLedgerCloseMeta(p.networkPassphrase, *ledgerCloseMeta)
	if err != nil {
		return nil, fmt.Errorf("error creating ledger transaction reader: %w", err)
	}
	defer ledgerTxReader.Close()

	closeTime := uint64(ledgerCloseMeta.LedgerHeaderHistoryEntry().Header.ScpValue.CloseTime)
	timestamp := time.Unix(int64(closeTime), 0).UTC()
	ledgerSeq := uint32(ledgerCloseMeta.LedgerSequence())

	var outputs []TransactionXDROutput

	// Process each transaction in the ledger
	for {
		tx, err := ledgerTxReader.Read()
		if err != nil {
			if err.Error() == "EOF" {
				break
			}
			log.Errorf("Error reading transaction: %v", err)
			continue
		}

		output, err := p.extractTransactionXDR(&tx, ledgerSeq, timestamp)
		if err != nil {
			log.Errorf("Error extracting XDR for transaction %s: %v", tx.Result.TransactionHash.HexString(), err)
			continue
		}

		outputs = append(outputs, *output)
	}

	return outputs, nil
}

// extractTransactionXDR extracts all XDR data from a transaction
func (p *TransactionXDRExtractor) extractTransactionXDR(tx *ingest.LedgerTransaction, ledgerSeq uint32, timestamp time.Time) (*TransactionXDROutput, error) {
	output := &TransactionXDROutput{
		LedgerSequence:   ledgerSeq,
		TransactionHash:  tx.Result.TransactionHash.HexString(),
		TransactionIndex: int32(tx.Index),
		Timestamp:        timestamp,
		Success:          tx.Result.Successful(),
		OperationCount:   len(tx.Envelope.Operations()),
		Operations:       []OperationXDR{},
	}

	// Extract source account
	sourceAccount := tx.Envelope.SourceAccount().ToAccountId()
	output.SourceAccount = sourceAccount.Address()

	// Extract fee charged
	output.FeeCharged = int64(tx.Result.Result.FeeCharged)

	// Extract envelope XDR
	envelopeBytes, err := tx.Envelope.MarshalBinary()
	if err != nil {
		log.Warnf("Failed to marshal envelope XDR: %v", err)
	} else {
		output.EnvelopeXDR = base64.StdEncoding.EncodeToString(envelopeBytes)
	}

	// Extract result XDR
	resultBytes, err := tx.Result.MarshalBinary()
	if err != nil {
		log.Warnf("Failed to marshal result XDR: %v", err)
	} else {
		output.ResultXDR = base64.StdEncoding.EncodeToString(resultBytes)
	}

	// Extract metadata XDR
	metaBytes, err := tx.UnsafeMeta.MarshalBinary()
	if err != nil {
		log.Warnf("Failed to marshal metadata XDR: %v", err)
	} else {
		output.ResultMetaXDR = base64.StdEncoding.EncodeToString(metaBytes)
	}

	// Extract operations
	for i, op := range tx.Envelope.Operations() {
		opXDR := OperationXDR{
			Index: i,
			Type:  op.Body.Type.String(),
		}

		// Get operation source account
		if op.SourceAccount != nil {
			opXDR.SourceAccount = op.SourceAccount.ToAccountId().Address()
		} else {
			opXDR.SourceAccount = output.SourceAccount
		}

		// Extract operation XDR
		opBytes, err := op.MarshalBinary()
		if err != nil {
			log.Warnf("Failed to marshal operation %d XDR: %v", i, err)
		} else {
			opXDR.OperationXDR = base64.StdEncoding.EncodeToString(opBytes)
		}

		output.Operations = append(output.Operations, opXDR)
	}

	return output, nil
}