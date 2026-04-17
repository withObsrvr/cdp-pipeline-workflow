package processor

import (
	"context"
	"encoding/json"
	"fmt"
	"log"

	"github.com/stellar/go/ingest"
	"github.com/stellar/go/xdr"
)

type LedgerReader struct {
	networkPassphrase string
	processors        []Processor
}

type TransactionMessage struct {
	Transaction  ingest.LedgerTransaction
	CloseTime    uint
	LedgerSeqNum uint32
}

func NewLedgerReader(config map[string]interface{}) (*LedgerReader, error) {
	networkPassphrase, ok := config["network_passphrase"].(string)
	if !ok {
		return nil, fmt.Errorf("invalid configuration for LedgerReader: missing 'network_passphrase'")
	}
	return &LedgerReader{networkPassphrase: networkPassphrase}, nil
}

func (l *LedgerReader) Subscribe(receiver Processor) {
	l.processors = append(l.processors, receiver)
}

func (l *LedgerReader) Process(ctx context.Context, msg Message) error {
	log.Printf("Processing message in LedgerReader")

	// Track stats
	var successfulTransactions, failedTransactions int
	var operationsInSuccessful, operationsInFailed int

	ledgerCloseMeta, ok := msg.Payload.(xdr.LedgerCloseMeta)
	if !ok {
		return fmt.Errorf("invalid message payload type: expected xdr.LedgerCloseMeta")
	}

	ledgerTxReader, err := ingest.NewLedgerTransactionReaderFromLedgerCloseMeta(l.networkPassphrase, ledgerCloseMeta)
	if err != nil {
		return fmt.Errorf("failed to create reader for ledger %d: %w", ledgerCloseMeta.LedgerSequence(), err)
	}
	defer ledgerTxReader.Close()

	ledgerSeqNum := ledgerCloseMeta.LedgerSequence()

	closeTime := uint(ledgerCloseMeta.LedgerHeaderHistoryEntry().Header.ScpValue.CloseTime)
	transaction, err := ledgerTxReader.Read()

	for ; err == nil; transaction, err = ledgerTxReader.Read() {
		operationCount := len(transaction.Envelope.Operations())
		if transaction.Result.Successful() {
			successfulTransactions++
			operationsInSuccessful += operationCount
		} else {
			failedTransactions++
			operationsInFailed += operationCount
		}

		log.Printf("Processing transaction in LedgerReader for ledger %d", ledgerCloseMeta.LedgerSequence())

		txMsg := TransactionMessage{
			Transaction:  transaction,
			CloseTime:    closeTime,
			LedgerSeqNum: ledgerSeqNum,
		}

		jsonBytes, err := json.Marshal(txMsg)
		if err != nil {
			return fmt.Errorf("error marshaling transaction: %w", err)
		}

		// Forward to next processors
		for _, processor := range l.processors {
			if err := processor.Process(ctx, Message{Payload: jsonBytes}); err != nil {
				return fmt.Errorf("error in processor chain: %w", err)
			}
		}
	}

	log.Printf("Total transactions in ledger: %d", successfulTransactions+failedTransactions)
	log.Printf("Total operations in successful transactions: %d", operationsInSuccessful)
	log.Printf("Total operations in failed transactions: %d", operationsInFailed)

	return nil
}
