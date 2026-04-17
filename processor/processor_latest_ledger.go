package processor

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"strings"
	"time"

	"github.com/stellar/go/ingest"
	"github.com/stellar/go/ingest/ledger"
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

	// Create a transaction reader using the network passphrase.
	txReader, err := ingest.NewLedgerTransactionReaderFromLedgerCloseMeta(
		p.networkPassphrase,
		ledgerCloseMeta,
	)
	if err != nil {
		return fmt.Errorf("error creating transaction reader: %v", err)
	}
	defer txReader.Close()

	// Extract basic ledger fields using helper functions.
	metrics := LatestLedger{
		Sequence: ledger.Sequence(ledgerCloseMeta),
		Hash:     ledger.Hash(ledgerCloseMeta),
		BaseFee:  ledger.BaseFee(ledgerCloseMeta),
		ClosedAt: ledger.ClosedAt(ledgerCloseMeta),
	}

	// Process each transaction. If a transaction cannot be read due to an "unknown tx hash" error,
	// we log a warning and skip it.
	for {
		tx, err := txReader.Read()
		if err == io.EOF {
			break
		}
		if err != nil {
			// Check if the error is due to an unknown transaction hash.
			if strings.Contains(err.Error(), "unknown tx hash") {
				log.Printf("Warning: skipping transaction due to error: %v", err)
				continue
			}
			return fmt.Errorf("error reading transaction: %v", err)
		}

		// Update metrics based on transaction data.
		metrics.TransactionCount++
		metrics.OperationCount += len(tx.Envelope.Operations())
		metrics.TotalFeeCharged += int64(tx.Result.Result.FeeCharged)

		if tx.Result.Successful() {
			metrics.SuccessfulTxCount++
		} else {
			metrics.FailedTxCount++
		}

		// Process Soroban metrics, if present.
		if hasSorobanTransaction(tx) {
			metrics.SorobanTxCount++
			sMetrics := getSorobanMetrics(tx)
			metrics.TotalSorobanFees += sMetrics.resourceFee
			metrics.TotalResourceInstructions += uint64(sMetrics.instructions)
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

	// Forward to the next set of processors.
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
	var sMetrics sorobanMetrics

	switch tx.Envelope.Type {
	case xdr.EnvelopeTypeEnvelopeTypeTx:
		sorobanData, _ = tx.Envelope.V1.Tx.Ext.GetSorobanData()
	case xdr.EnvelopeTypeEnvelopeTypeTxFeeBump:
		sorobanData, _ = tx.Envelope.FeeBump.Tx.InnerTx.V1.Tx.Ext.GetSorobanData()
	}

	sMetrics.resourceFee = int64(sorobanData.ResourceFee)
	sMetrics.instructions = uint32(sorobanData.Resources.Instructions)
	sMetrics.readBytes = uint32(sorobanData.Resources.DiskReadBytes)
	sMetrics.writeBytes = uint32(sorobanData.Resources.WriteBytes)

	return sMetrics
}
