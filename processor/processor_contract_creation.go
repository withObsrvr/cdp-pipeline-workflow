package processor

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"sync"
	"time"

	"github.com/stellar/go/ingest"
	"github.com/stellar/go/strkey"
	"github.com/stellar/go/xdr"
)

// ContractCreation represents a contract creation event
type ContractCreation struct {
	Timestamp         time.Time `json:"timestamp"`
	LedgerSequence    uint32    `json:"ledger_sequence"`
	TransactionHash   string    `json:"transaction_hash"`
	ContractID        string    `json:"contract_id"`
	CreatorAccount    string    `json:"creator_account"`
	WasmID            string    `json:"wasm_id,omitempty"`
	InitialSalt       string    `json:"initial_salt,omitempty"`
	Successful        bool      `json:"successful"`
	ExecutorAccount   string    `json:"executor_account,omitempty"`
	NetworkPassphrase string    `json:"network_passphrase,omitempty"`
}

type ContractCreationProcessor struct {
	processors        []Processor
	networkPassphrase string
	mu                sync.RWMutex
	stats             struct {
		ProcessedLedgers  uint32
		CreationsFound    uint64
		SuccessfulCreates uint64
		FailedCreates     uint64
		LastLedger        uint32
		LastProcessedTime time.Time
	}
}

func NewContractCreationProcessor(config map[string]interface{}) (*ContractCreationProcessor, error) {
	networkPassphrase, ok := config["network_passphrase"].(string)
	if !ok {
		return nil, fmt.Errorf("missing network_passphrase in configuration")
	}

	return &ContractCreationProcessor{
		networkPassphrase: networkPassphrase,
	}, nil
}

func (p *ContractCreationProcessor) Subscribe(processor Processor) {
	p.processors = append(p.processors, processor)
}

func (p *ContractCreationProcessor) Process(ctx context.Context, msg Message) error {
	ledgerCloseMeta, ok := msg.Payload.(xdr.LedgerCloseMeta)
	if !ok {
		return fmt.Errorf("expected xdr.LedgerCloseMeta, got %T", msg.Payload)
	}

	sequence := ledgerCloseMeta.LedgerSequence()
	log.Printf("Processing ledger %d for contract creations", sequence)

	txReader, err := ingest.NewLedgerTransactionReaderFromLedgerCloseMeta(p.networkPassphrase, ledgerCloseMeta)
	if err != nil {
		return fmt.Errorf("error creating transaction reader: %w", err)
	}
	defer txReader.Close()

	// Process each transaction
	for {
		tx, err := txReader.Read()
		if err == io.EOF {
			break
		}
		if err != nil {
			return fmt.Errorf("error reading transaction: %w", err)
		}

		// Check each operation for contract creations
		for opIndex, op := range tx.Envelope.Operations() {
			if op.Body.Type == xdr.OperationTypeInvokeHostFunction {
				creation, err := p.processContractCreation(tx, opIndex, op, ledgerCloseMeta)
				if err != nil {
					log.Printf("Error processing contract creation: %v", err)
					continue
				}

				if creation != nil {
					if err := p.forwardToProcessors(ctx, creation); err != nil {
						log.Printf("Error forwarding creation: %v", err)
					}
				}
			}
		}
	}

	p.mu.Lock()
	p.stats.ProcessedLedgers++
	p.stats.LastLedger = sequence
	p.stats.LastProcessedTime = time.Now()
	p.mu.Unlock()

	return nil
}

func (p *ContractCreationProcessor) processContractCreation(
	tx ingest.LedgerTransaction,
	opIndex int,
	op xdr.Operation,
	meta xdr.LedgerCloseMeta,
) (*ContractCreation, error) {
	invokeHostFunction := op.Body.MustInvokeHostFunctionOp()

	// Only process host functions that create contracts
	if invokeHostFunction.HostFunction.Type != xdr.HostFunctionTypeHostFunctionTypeCreateContract {
		return nil, nil
	}

	// Get the creator account
	var creatorAccount xdr.AccountId
	if op.SourceAccount != nil {
		creatorAccount = op.SourceAccount.ToAccountId()
	} else {
		creatorAccount = tx.Envelope.SourceAccount().ToAccountId()
	}

	// Extract contract creation details
	createContractArgs := invokeHostFunction.HostFunction.MustCreateContract()

	// Get contract ID from the changes
	var contractID string
	changes, err := tx.GetOperationChanges(uint32(opIndex))
	if err != nil {
		return nil, fmt.Errorf("error getting operation changes: %w", err)
	}

	// Look for the created contract in the changes
	for _, change := range changes {
		if change.LedgerEntryChangeType() == xdr.LedgerEntryChangeTypeLedgerEntryCreated {
			if entry := change.Post; entry != nil {
				if entry.Data.Type == xdr.LedgerEntryTypeContractData {
					contractData := entry.Data.MustContractData()
					if contractData.Contract.ContractId != nil {
						contractID, err = strkey.Encode(strkey.VersionByteContract, (*contractData.Contract.ContractId)[:])
						if err != nil {
							return nil, fmt.Errorf("error encoding contract ID: %w", err)
						}
						break
					}
				}
			}
		}
	}

	// Determine if creation was successful
	successful := false
	if tx.Result.Result.Result.Results != nil {
		if results := *tx.Result.Result.Result.Results; len(results) > opIndex {
			if result := results[opIndex]; result.Tr != nil {
				if invokeResult, ok := result.Tr.GetInvokeHostFunctionResult(); ok {
					successful = invokeResult.Code == xdr.InvokeHostFunctionResultCodeInvokeHostFunctionSuccess
				}
			}
		}
	}

	p.mu.Lock()
	p.stats.CreationsFound++
	if successful {
		p.stats.SuccessfulCreates++
	} else {
		p.stats.FailedCreates++
	}
	p.mu.Unlock()

	// Create contract creation record
	wasmID := ""
	if wasm := createContractArgs.Executable.WasmHash; wasm != nil {
		wasmID = fmt.Sprintf("%x", wasm)
	}

	creation := &ContractCreation{
		Timestamp:         time.Unix(int64(meta.LedgerHeaderHistoryEntry().Header.ScpValue.CloseTime), 0),
		LedgerSequence:    meta.LedgerSequence(),
		TransactionHash:   tx.Result.TransactionHash.HexString(),
		ContractID:        contractID,
		CreatorAccount:    creatorAccount.Address(),
		WasmID:            wasmID,
		Successful:        successful,
		ExecutorAccount:   tx.Envelope.SourceAccount().ToAccountId().Address(),
		NetworkPassphrase: p.networkPassphrase,
	}

	return creation, nil
}

func (p *ContractCreationProcessor) forwardToProcessors(ctx context.Context, creation *ContractCreation) error {
	jsonBytes, err := json.Marshal(creation)
	if err != nil {
		return fmt.Errorf("error marshaling creation: %w", err)
	}

	for _, processor := range p.processors {
		if err := processor.Process(ctx, Message{Payload: jsonBytes}); err != nil {
			return fmt.Errorf("error in processor chain: %w", err)
		}
	}
	return nil
}

// GetStats returns the current processing statistics
func (p *ContractCreationProcessor) GetStats() struct {
	ProcessedLedgers  uint32
	CreationsFound    uint64
	SuccessfulCreates uint64
	FailedCreates     uint64
	LastLedger        uint32
	LastProcessedTime time.Time
} {
	p.mu.RLock()
	defer p.mu.RUnlock()
	return p.stats
}
