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

type FilteredContractInvocation struct {
	Timestamp       time.Time `json:"timestamp"`
	LedgerSequence  uint32    `json:"ledger_sequence"`
	TransactionHash string    `json:"transaction_hash"`
	ContractID      string    `json:"contract_id"`
	InvokingAccount string    `json:"invoking_account"`
	FunctionName    string    `json:"function_name,omitempty"`
	Successful      bool      `json:"successful"`
}

type FilteredContractInvocationProcessor struct {
	processors        []Processor
	networkPassphrase string
	contractIDs       map[string]bool // Set of contract IDs to filter by
	accountIDs        map[string]bool // Set of account IDs to filter by
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

func NewFilteredContractInvocationProcessor(config map[string]interface{}) (*FilteredContractInvocationProcessor, error) {
	networkPassphrase, ok := config["network_passphrase"].(string)
	if !ok {
		return nil, fmt.Errorf("missing network_passphrase in configuration")
	}

	p := &FilteredContractInvocationProcessor{
		networkPassphrase: networkPassphrase,
		contractIDs:       make(map[string]bool),
		accountIDs:        make(map[string]bool),
	}

	// Process contract IDs filter
	if contractIDs, ok := config["contract_ids"].([]interface{}); ok {
		for _, id := range contractIDs {
			if strID, ok := id.(string); ok {
				p.contractIDs[strID] = true
				log.Printf("Added contract filter: %s", strID)
			}
		}
	}

	// Process account IDs filter
	if accountIDs, ok := config["account_ids"].([]interface{}); ok {
		for _, id := range accountIDs {
			if strID, ok := id.(string); ok {
				p.accountIDs[strID] = true
				log.Printf("Added account filter: %s", strID)
			}
		}
	}

	return p, nil
}

func (p *FilteredContractInvocationProcessor) Subscribe(processor Processor) {
	p.processors = append(p.processors, processor)
}

func (p *FilteredContractInvocationProcessor) Process(ctx context.Context, msg Message) error {
	ledgerCloseMeta, ok := msg.Payload.(xdr.LedgerCloseMeta)
	if !ok {
		return fmt.Errorf("expected xdr.LedgerCloseMeta, got %T", msg.Payload)
	}

	sequence := ledgerCloseMeta.LedgerSequence()
	log.Printf("Processing ledger %d for filtered contract invocations", sequence)

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

func (p *FilteredContractInvocationProcessor) processContractInvocation(
	tx ingest.LedgerTransaction,
	opIndex int,
	op xdr.Operation,
	meta xdr.LedgerCloseMeta,
) (*FilteredContractInvocation, error) {
	// Get the invoking account first
	var invokingAccount string
	if op.SourceAccount != nil {
		invokingAccount = (*op.SourceAccount).Address()
	} else {
		invokingAccount = tx.Envelope.SourceAccount().ToAccountId().Address()
	}

	// Debug logging for account filtering
	log.Printf("Processing invocation from account: %s", invokingAccount)
	log.Printf("Account filter list: %v", p.accountIDs)

	// Early check for account filters
	if len(p.accountIDs) > 0 {
		if !p.accountIDs[invokingAccount] {
			log.Printf("Filtering out invocation from non-matching account: %s", invokingAccount)
			return nil, nil
		}
		log.Printf("Account matched filter: %s", invokingAccount)
	}

	invokeHostFunction := op.Body.MustInvokeHostFunctionOp()

	// Get contract ID if available
	var contractID string
	if function := invokeHostFunction.HostFunction; function.Type == xdr.HostFunctionTypeHostFunctionTypeInvokeContract {
		contractIDBytes := function.MustInvokeContract().ContractAddress.ContractId
		var err error
		contractID, err = strkey.Encode(strkey.VersionByteContract, contractIDBytes[:])
		if err != nil {
			return nil, fmt.Errorf("error encoding contract ID: %w", err)
		}

		// Debug logging for contract filtering
		log.Printf("Processing contract: %s", contractID)
		log.Printf("Contract filter list: %v", p.contractIDs)

		// Check contract filter if set
		if len(p.contractIDs) > 0 {
			if !p.contractIDs[contractID] {
				log.Printf("Filtering out non-matching contract: %s", contractID)
				return nil, nil
			}
			log.Printf("Contract matched filter: %s", contractID)
		}
	}

	// Determine if invocation was successful
	results := tx.Result.Result.Result.MustResults()
	successful := results[opIndex].Tr.MustInvokeHostFunctionResult().Code == xdr.InvokeHostFunctionResultCodeInvokeHostFunctionSuccess

	p.mu.Lock()
	p.stats.InvocationsFound++
	if successful {
		p.stats.SuccessfulInvokes++
	} else {
		p.stats.FailedInvokes++
	}
	p.mu.Unlock()

	// Create invocation record
	invocation := &FilteredContractInvocation{
		Timestamp:       time.Unix(int64(meta.LedgerHeaderHistoryEntry().Header.ScpValue.CloseTime), 0),
		LedgerSequence:  meta.LedgerSequence(),
		TransactionHash: tx.Result.TransactionHash.HexString(),
		ContractID:      contractID,
		InvokingAccount: invokingAccount,
		Successful:      successful,
	}

	// Extract function name
	if function := invokeHostFunction.HostFunction; function.Type == xdr.HostFunctionTypeHostFunctionTypeInvokeContract {
		contract := function.MustInvokeContract()

		invocation.FunctionName = getFunctionName(contract.Args)
		if invocation.FunctionName != "" {
			log.Printf("Found function name: %s", invocation.FunctionName)
		}
	}

	return invocation, nil
}

func (p *FilteredContractInvocationProcessor) forwardToProcessors(ctx context.Context, invocation *FilteredContractInvocation) error {
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

func (p *FilteredContractInvocationProcessor) GetStats() struct {
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

func getFunctionName(args []xdr.ScVal) string {
	if len(args) == 0 {
		return ""
	}

	// Try different ways to get the function name
	switch args[0].Type {
	case xdr.ScValTypeScvSymbol:
		return string(*args[0].Sym)
	case xdr.ScValTypeScvString:
		return string(*args[0].Str)
	case xdr.ScValTypeScvBytes:
		return string(*args[0].Bytes)
	default:
		return args[0].String()
	}
}
