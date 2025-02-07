package processor

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"time"

	"github.com/stellar/go/ingest"
	"github.com/stellar/go/xdr"
)

// Enhanced LatestLedger struct with transaction metrics
type LatestLedger struct {
	Sequence          uint32    `json:"sequence"`
	Hash              string    `json:"hash"`
	TransactionCount  int       `json:"transaction_count"`
	OperationCount    int       `json:"operation_count"`
	SuccessfulTxCount int       `json:"successful_tx_count"`
	FailedTxCount     int       `json:"failed_tx_count"`
	TotalFeeCharged   int64     `json:"total_fee_charged"`
	ClosedAt          time.Time `json:"closed_at"`
	BaseFee           uint32    `json:"base_fee"`

	// Soroban metrics
	SorobanTxCount            int    `json:"soroban_tx_count"`
	TotalSorobanFees          int64  `json:"total_soroban_fees"`
	TotalResourceInstructions uint64 `json:"total_resource_instructions"`
}

type LatestLedgerProcessor struct {
	networkPassphrase string
	processors        []Processor
}

func NewLatestLedgerProcessor(config map[string]interface{}) (*LatestLedgerProcessor, error) {
	networkPassphrase, ok := config["network_passphrase"].(string)
	if !ok {
		return nil, fmt.Errorf("missing network_passphrase in config")
	}

	return &LatestLedgerProcessor{
		networkPassphrase: networkPassphrase,
	}, nil
}

func (p *LatestLedgerProcessor) Subscribe(processor Processor) {
	p.processors = append(p.processors, processor)
}

func (p *LatestLedgerProcessor) Process(ctx context.Context, msg Message) error {
	ledgerCloseMeta, ok := msg.Payload.(xdr.LedgerCloseMeta)
	if !ok {
		return fmt.Errorf("expected xdr.LedgerCloseMeta, got %T", msg.Payload)
	}

	// Create transaction reader
	txReader, err := ingest.NewLedgerTransactionReaderFromLedgerCloseMeta(
		p.networkPassphrase,
		ledgerCloseMeta,
	)
	if err != nil {
		return fmt.Errorf("error creating transaction reader: %v", err)
	}
	defer txReader.Close()

	// Initialize metrics
	metrics := LatestLedger{
		Sequence: ledgerCloseMeta.LedgerSequence(),
		Hash:     ledgerCloseMeta.LedgerHash().HexString(),
		BaseFee:  uint32(ledgerCloseMeta.LedgerHeaderHistoryEntry().Header.BaseFee),
		ClosedAt: time.Unix(int64(ledgerCloseMeta.LedgerHeaderHistoryEntry().Header.ScpValue.CloseTime), 0),
	}

	// Process each transaction
	for {
		tx, err := txReader.Read()
		if err == io.EOF {
			break
		}
		if err != nil {
			return fmt.Errorf("error reading transaction: %v", err)
		}

		// Update basic transaction metrics
		metrics.TransactionCount++
		metrics.OperationCount += len(tx.Envelope.Operations())
		metrics.TotalFeeCharged += int64(tx.Result.Result.FeeCharged)

		if tx.Result.Successful() {
			metrics.SuccessfulTxCount++
		} else {
			metrics.FailedTxCount++
		}

		// Track Soroban metrics
		if hasSorobanData := hasSorobanTransaction(tx); hasSorobanData {
			metrics.SorobanTxCount++
			sorobanMetrics := getSorobanMetrics(tx)
			metrics.TotalSorobanFees += sorobanMetrics.resourceFee
			metrics.TotalResourceInstructions += uint64(sorobanMetrics.instructions)
		}
	}

	jsonBytes, err := json.Marshal(metrics)
	if err != nil {
		return fmt.Errorf("error marshaling latest ledger: %w", err)
	}

	log.Printf("Latest ledger: %d (Transactions: %d, Operations: %d, Success Rate: %.2f%%)",
		metrics.Sequence,
		metrics.TransactionCount,
		metrics.OperationCount,
		float64(metrics.SuccessfulTxCount)/float64(metrics.TransactionCount)*100,
	)

	// Forward to next processors
	for _, processor := range p.processors {
		if err := processor.Process(ctx, Message{Payload: jsonBytes}); err != nil {
			return fmt.Errorf("error in processor chain: %w", err)
		}
	}

	return nil
}

type sorobanMetrics struct {
	resourceFee  int64
	instructions uint32
	readBytes    uint32
	writeBytes   uint32
}

func hasSorobanTransaction(tx ingest.LedgerTransaction) bool {
	switch tx.Envelope.Type {
	case xdr.EnvelopeTypeEnvelopeTypeTx:
		_, has := tx.Envelope.V1.Tx.Ext.GetSorobanData()
		return has
	case xdr.EnvelopeTypeEnvelopeTypeTxFeeBump:
		_, has := tx.Envelope.FeeBump.Tx.InnerTx.V1.Tx.Ext.GetSorobanData()
		return has
	}
	return false
}

func getSorobanMetrics(tx ingest.LedgerTransaction) sorobanMetrics {
	var sorobanData xdr.SorobanTransactionData
	var metrics sorobanMetrics

	switch tx.Envelope.Type {
	case xdr.EnvelopeTypeEnvelopeTypeTx:
		sorobanData, _ = tx.Envelope.V1.Tx.Ext.GetSorobanData()
	case xdr.EnvelopeTypeEnvelopeTypeTxFeeBump:
		sorobanData, _ = tx.Envelope.FeeBump.Tx.InnerTx.V1.Tx.Ext.GetSorobanData()
	}

	metrics.resourceFee = int64(sorobanData.ResourceFee)
	metrics.instructions = uint32(sorobanData.Resources.Instructions)
	metrics.readBytes = uint32(sorobanData.Resources.ReadBytes)
	metrics.writeBytes = uint32(sorobanData.Resources.WriteBytes)

	return metrics
}
