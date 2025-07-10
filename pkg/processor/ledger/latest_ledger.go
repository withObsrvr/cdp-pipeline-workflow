package ledger

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"strings"
	"time"

	"github.com/stellar/go/ingest"
	"github.com/stellar/go/xdr"
	"github.com/withObsrvr/cdp-pipeline-workflow/pkg/common/types"
	"github.com/withObsrvr/cdp-pipeline-workflow/pkg/processor/base"
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

// LatestLedgerProcessor processes ledger data and extracts metrics
type LatestLedgerProcessor struct {
	networkPassphrase string
	processors        []types.Processor
}

// NewLatestLedgerProcessor creates a new LatestLedgerProcessor
func NewLatestLedgerProcessor(config map[string]interface{}) (*LatestLedgerProcessor, error) {
	networkPassphrase, ok := config["network_passphrase"].(string)
	if !ok {
		return nil, fmt.Errorf("missing network_passphrase in config")
	}

	return &LatestLedgerProcessor{
		networkPassphrase: networkPassphrase,
	}, nil
}

// Subscribe adds a processor to the chain
func (p *LatestLedgerProcessor) Subscribe(processor types.Processor) {
	p.processors = append(p.processors, processor)
}

// Process processes a message containing a LedgerCloseMeta
func (p *LatestLedgerProcessor) Process(ctx context.Context, msg types.Message) error {
	var ledgerCloseMeta xdr.LedgerCloseMeta

	// Try different payload types
	switch payload := msg.Payload.(type) {
	case xdr.LedgerCloseMeta:
		ledgerCloseMeta = payload
	case []byte:
		// If the payload is JSON, try to extract ledger sequence and hash
		var jsonData map[string]interface{}
		if err := json.Unmarshal(payload, &jsonData); err == nil {
			// Create a simplified ledger object with basic info
			metrics := LatestLedger{}

			if seq, ok := jsonData["ledger_sequence"].(float64); ok {
				metrics.Sequence = uint32(seq)
			}

			if hash, ok := jsonData["ledger_hash"].(string); ok {
				metrics.Hash = hash
			}

			if closeTime, ok := jsonData["closed_at"].(string); ok {
				if t, err := time.Parse(time.RFC3339, closeTime); err == nil {
					metrics.ClosedAt = t
				}
			}

			// Marshal and forward to processors
			jsonBytes, err := json.Marshal(metrics)
			if err != nil {
				return fmt.Errorf("error marshaling latest ledger: %w", err)
			}

			log.Printf("Latest ledger from JSON: %d", metrics.Sequence)

			// Forward to the next set of processors
			for _, processor := range p.processors {
				if err := processor.Process(ctx, types.Message{Payload: jsonBytes}); err != nil {
					return fmt.Errorf("error in processor chain: %w", err)
				}
			}

			return nil
		}
		return fmt.Errorf("unable to process payload of type []byte")
	default:
		return fmt.Errorf("expected xdr.LedgerCloseMeta, got %T", msg.Payload)
	}

	// Create a transaction reader using the network passphrase
	txReader, err := ingest.NewLedgerTransactionReaderFromLedgerCloseMeta(
		p.networkPassphrase,
		ledgerCloseMeta,
	)
	if err != nil {
		return fmt.Errorf("error creating transaction reader: %v", err)
	}
	defer txReader.Close()

	// Extract basic ledger fields
	sequence := uint32(ledgerCloseMeta.LedgerHeaderHistoryEntry().Header.LedgerSeq)
	hash := base.HashToHexString(ledgerCloseMeta.LedgerHeaderHistoryEntry().Hash)
	closedAt, err := base.TimePointToUTCTimeStamp(ledgerCloseMeta.LedgerHeaderHistoryEntry().Header.ScpValue.CloseTime)
	if err != nil {
		return fmt.Errorf("error getting ledger close time: %v", err)
	}
	baseFee := ledgerCloseMeta.LedgerHeaderHistoryEntry().Header.BaseFee

	// Create the ledger metrics
	metrics := LatestLedger{
		Sequence: sequence,
		Hash:     hash,
		BaseFee:  uint32(baseFee),
		ClosedAt: closedAt,
	}

	// Process each transaction
	for {
		tx, err := txReader.Read()
		if err == io.EOF {
			break
		}
		if err != nil {
			// Check if the error is due to an unknown transaction hash
			if strings.Contains(err.Error(), "unknown tx hash") {
				log.Printf("Warning: skipping transaction due to error: %v", err)
				continue
			}
			return fmt.Errorf("error reading transaction: %v", err)
		}

		// Update metrics based on transaction data
		metrics.TransactionCount++
		metrics.OperationCount += len(tx.Envelope.Operations())
		metrics.TotalFeeCharged += int64(tx.Result.Result.FeeCharged)

		if tx.Result.Successful() {
			metrics.SuccessfulTxCount++
		} else {
			metrics.FailedTxCount++
		}

		// Process Soroban metrics, if present
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
		calculateSuccessRate(metrics.SuccessfulTxCount, metrics.TransactionCount),
	)

	// Forward to the next set of processors
	for _, processor := range p.processors {
		if err := processor.Process(ctx, types.Message{Payload: jsonBytes}); err != nil {
			return fmt.Errorf("error in processor chain: %w", err)
		}
	}

	return nil
}

// calculateSuccessRate calculates the success rate, handling division by zero
func calculateSuccessRate(successful, total int) float64 {
	if total == 0 {
		return 0.0
	}
	return float64(successful) / float64(total) * 100
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
	sMetrics.readBytes = uint32(sorobanData.Resources.ReadBytes)
	sMetrics.writeBytes = uint32(sorobanData.Resources.WriteBytes)

	return sMetrics
}
