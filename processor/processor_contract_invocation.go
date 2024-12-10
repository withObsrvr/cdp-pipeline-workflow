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

// ContractInvocation represents a contract invocation event
type ContractInvocation struct {
	Timestamp       time.Time `json:"timestamp"`
	LedgerSequence  uint32    `json:"ledger_sequence"`
	TransactionHash string    `json:"transaction_hash"`
	ContractID      string    `json:"contract_id"`
	InvokingAccount string    `json:"invoking_account"`
	FunctionName    string    `json:"function_name,omitempty"`
	Successful      bool      `json:"successful"`
}

type ContractInvocationProcessor struct {
	processors        []Processor
	networkPassphrase string
	mu                sync.RWMutex
	stats             struct {
		ProcessedLedgers  uint32
		InvocationsFound  uint64
		SuccessfulInvokes uint64
		FailedInvokes     uint64
		LastLedger        uint32
		LastProcessedTime time.Time
	}
}

func NewContractInvocationProcessor(config map[string]interface{}) (*ContractInvocationProcessor, error) {
	networkPassphrase, ok := config["network_passphrase"].(string)
	if !ok {
		return nil, fmt.Errorf("missing network_passphrase in configuration")
	}

	return &ContractInvocationProcessor{
		networkPassphrase: networkPassphrase,
	}, nil
}

func (p *ContractInvocationProcessor) Subscribe(processor Processor) {
	p.processors = append(p.processors, processor)
}

func (p *ContractInvocationProcessor) Process(ctx context.Context, msg Message) error {
	ledgerCloseMeta, ok := msg.Payload.(xdr.LedgerCloseMeta)
	if !ok {
		return fmt.Errorf("expected xdr.LedgerCloseMeta, got %T", msg.Payload)
	}

	sequence := ledgerCloseMeta.LedgerSequence()
	log.Printf("Processing ledger %d for contract invocations", sequence)

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

		// Check each operation for contract invocations
		for opIndex, op := range tx.Envelope.Operations() {
			if op.Body.Type == xdr.OperationTypeInvokeHostFunction {
				invocation, err := p.processContractInvocation(tx, opIndex, op, ledgerCloseMeta)
				if err != nil {
					log.Printf("Error processing contract invocation: %v", err)
					continue
				}

				if invocation != nil {
					if err := p.forwardToProcessors(ctx, invocation); err != nil {
						log.Printf("Error forwarding invocation: %v", err)
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

func (p *ContractInvocationProcessor) processContractInvocation(
	tx ingest.LedgerTransaction,
	opIndex int,
	op xdr.Operation,
	meta xdr.LedgerCloseMeta,
) (*ContractInvocation, error) {
	invokeHostFunction := op.Body.MustInvokeHostFunctionOp()

	// Get the invoking account
	var invokingAccount xdr.AccountId
	if op.SourceAccount != nil {
		invokingAccount = op.SourceAccount.ToAccountId()
	} else {
		invokingAccount = tx.Envelope.SourceAccount().ToAccountId()
	}

	// Get contract ID if available
	var contractID string
	if function := invokeHostFunction.HostFunction; function.Type == xdr.HostFunctionTypeHostFunctionTypeInvokeContract {
		contractIDBytes := function.MustInvokeContract().ContractAddress.ContractId
		var err error
		contractID, err = strkey.Encode(strkey.VersionByteContract, contractIDBytes[:])
		if err != nil {
			return nil, fmt.Errorf("error encoding contract ID: %w", err)
		}
	}

	// Determine if invocation was successful
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
	p.stats.InvocationsFound++
	if successful {
		p.stats.SuccessfulInvokes++
	} else {
		p.stats.FailedInvokes++
	}
	p.mu.Unlock()

	// Create invocation record
	invocation := &ContractInvocation{
		Timestamp:       time.Unix(int64(meta.LedgerHeaderHistoryEntry().Header.ScpValue.CloseTime), 0),
		LedgerSequence:  meta.LedgerSequence(),
		TransactionHash: tx.Result.TransactionHash.HexString(),
		ContractID:      contractID,
		InvokingAccount: invokingAccount.Address(),
		Successful:      successful,
	}

	// Try to get function name if available
	if function := invokeHostFunction.HostFunction; function.Type == xdr.HostFunctionTypeHostFunctionTypeInvokeContract {
		if args := function.MustInvokeContract().Args; len(args) > 0 {
			if sym, ok := args[0].GetSym(); ok {
				invocation.FunctionName = string(sym)
			}
		}
	}

	return invocation, nil
}

func (p *ContractInvocationProcessor) forwardToProcessors(ctx context.Context, invocation *ContractInvocation) error {
	jsonBytes, err := json.Marshal(invocation)
	if err != nil {
		return fmt.Errorf("error marshaling invocation: %w", err)
	}

	for _, processor := range p.processors {
		if err := processor.Process(ctx, Message{Payload: jsonBytes}); err != nil {
			return fmt.Errorf("error in processor chain: %w", err)
		}
	}
	return nil
}

func (p *ContractInvocationProcessor) GetStats() struct {
	ProcessedLedgers  uint32
	InvocationsFound  uint64
	SuccessfulInvokes uint64
	FailedInvokes     uint64
	LastLedger        uint32
	LastProcessedTime time.Time
} {
	p.mu.RLock()
	defer p.mu.RUnlock()
	return p.stats
}
