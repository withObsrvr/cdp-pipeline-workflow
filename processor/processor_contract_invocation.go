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
	Timestamp        time.Time         `json:"timestamp"`
	LedgerSequence   uint32            `json:"ledger_sequence"`
	TransactionHash  string            `json:"transaction_hash"`
	ContractID       string            `json:"contract_id"`
	InvokingAccount  string            `json:"invoking_account"`
	FunctionName     string            `json:"function_name,omitempty"`
	Successful       bool              `json:"successful"`
	DiagnosticEvents []DiagnosticEvent `json:"diagnostic_events,omitempty"`
	ContractCalls    []ContractCall    `json:"contract_calls,omitempty"`
	StateChanges     []StateChange     `json:"state_changes,omitempty"`
	TtlExtensions    []TtlExtension    `json:"ttl_extensions,omitempty"`
}

// DiagnosticEvent represents a diagnostic event emitted during contract execution
type DiagnosticEvent struct {
	ContractID string          `json:"contract_id"`
	Topics     []string        `json:"topics"`
	Data       json.RawMessage `json:"data"`
}

// ContractCall represents a contract-to-contract call
type ContractCall struct {
	FromContract string `json:"from_contract"`
	ToContract   string `json:"to_contract"`
	Function     string `json:"function"`
	Successful   bool   `json:"successful"`
}

// StateChange represents a contract state change
type StateChange struct {
	ContractID string          `json:"contract_id"`
	Key        string          `json:"key"`
	OldValue   json.RawMessage `json:"old_value,omitempty"`
	NewValue   json.RawMessage `json:"new_value,omitempty"`
	Operation  string          `json:"operation"` // "create", "update", "delete"
}

// TtlExtension represents a TTL extension for a contract
type TtlExtension struct {
	ContractID string `json:"contract_id"`
	OldTtl     uint32 `json:"old_ttl"`
	NewTtl     uint32 `json:"new_ttl"`
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

	// Extract diagnostic events
	invocation.DiagnosticEvents = p.extractDiagnosticEvents(tx, opIndex)

	// Extract contract-to-contract calls
	invocation.ContractCalls = p.extractContractCalls(tx, opIndex)

	// Extract state changes
	invocation.StateChanges = p.extractStateChanges(tx, opIndex)

	// Extract TTL extensions
	invocation.TtlExtensions = p.extractTtlExtensions(tx, opIndex)

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

// extractDiagnosticEvents extracts diagnostic events from transaction meta
func (p *ContractInvocationProcessor) extractDiagnosticEvents(tx ingest.LedgerTransaction, opIndex int) []DiagnosticEvent {
	var events []DiagnosticEvent

	// Check if we have diagnostic events in the transaction meta
	if tx.UnsafeMeta.V == 3 {
		sorobanMeta := tx.UnsafeMeta.V3.SorobanMeta
		if sorobanMeta != nil && sorobanMeta.Events != nil {
			for _, event := range sorobanMeta.Events {
				// Convert contract ID
				contractIDBytes := event.ContractId
				contractID, err := strkey.Encode(strkey.VersionByteContract, contractIDBytes[:])
				if err != nil {
					log.Printf("Error encoding contract ID for diagnostic event: %v", err)
					continue
				}

				// Convert topics to strings
				var topics []string
				for _, topic := range event.Body.V0.Topics {
					// For simplicity, we'll just convert to JSON
					topicJSON, err := json.Marshal(topic)
					if err != nil {
						log.Printf("Error marshaling topic: %v", err)
						continue
					}
					topics = append(topics, string(topicJSON))
				}

				// Convert data to JSON
				dataJSON, err := json.Marshal(event.Body.V0.Data)
				if err != nil {
					log.Printf("Error marshaling event data: %v", err)
					continue
				}

				events = append(events, DiagnosticEvent{
					ContractID: contractID,
					Topics:     topics,
					Data:       dataJSON,
				})
			}
		}
	}

	return events
}

// extractContractCalls extracts contract calls from transaction meta
func (p *ContractInvocationProcessor) extractContractCalls(tx ingest.LedgerTransaction, opIndex int) []ContractCall {
	var calls []ContractCall

	// Check if we have Soroban meta in the transaction
	if tx.UnsafeMeta.V == 3 {
		sorobanMeta := tx.UnsafeMeta.V3.SorobanMeta
		if sorobanMeta != nil {
			// Process diagnostic events which may contain contract calls
			if len(sorobanMeta.DiagnosticEvents) > 0 {
				// Log diagnostic events for debugging
				log.Printf("Found %d diagnostic events", len(sorobanMeta.DiagnosticEvents))

				// Future implementation will process these events
				// when the XDR structure is better understood
			}
		}
	}

	return calls
}

// processInvocation processes a contract invocation and extracts contract calls
// This method is currently unused but will be used in future implementations
// when the XDR structure includes the necessary fields
//
//nolint:unused
func (p *ContractInvocationProcessor) processInvocation(
	invocation *xdr.SorobanAuthorizedInvocation,
	fromContract string,
	calls *[]ContractCall,
) {
	if invocation == nil {
		return
	}

	// Get the contract ID for this invocation
	var contractID string
	if invocation.Function.Type == xdr.SorobanAuthorizedFunctionTypeSorobanAuthorizedFunctionTypeContractFn {
		contractIDBytes := invocation.Function.ContractFn.ContractAddress.ContractId
		var err error
		contractID, err = strkey.Encode(strkey.VersionByteContract, contractIDBytes[:])
		if err != nil {
			log.Printf("Error encoding contract ID for invocation: %v", err)
			return
		}
	}

	// Get the function name
	var functionName string
	if invocation.Function.Type == xdr.SorobanAuthorizedFunctionTypeSorobanAuthorizedFunctionTypeContractFn {
		functionName = string(invocation.Function.ContractFn.FunctionName)
	}

	// If we have both a from and to contract, record the call
	if fromContract != "" && contractID != "" && fromContract != contractID {
		*calls = append(*calls, ContractCall{
			FromContract: fromContract,
			ToContract:   contractID,
			Function:     functionName,
			Successful:   true, // We don't have success info at this level
		})
	}

	// Process sub-invocations
	for _, subInvocation := range invocation.SubInvocations {
		p.processInvocation(&subInvocation, contractID, calls)
	}
}

// extractStateChanges extracts state changes from transaction meta
func (p *ContractInvocationProcessor) extractStateChanges(tx ingest.LedgerTransaction, opIndex int) []StateChange {
	var changes []StateChange

	// Check if we have Soroban meta in the transaction
	if tx.UnsafeMeta.V == 3 {
		sorobanMeta := tx.UnsafeMeta.V3.SorobanMeta
		if sorobanMeta != nil {
			// Based on the debug output, there's no direct state changes field
			// State changes would need to be extracted from other transaction data

			// Process events which may contain state change information
			if len(sorobanMeta.Events) > 0 {
				for _, _event := range sorobanMeta.Events {
					// Process events for state changes
					// Implementation depends on the structure of ContractEvent
					_ = _event // Placeholder until implementation is complete
				}
			}
		}
	}

	return changes
}

// extractTtlExtensions extracts TTL extensions from transaction meta
func (p *ContractInvocationProcessor) extractTtlExtensions(tx ingest.LedgerTransaction, opIndex int) []TtlExtension {
	var extensions []TtlExtension

	// Check if we have Soroban meta in the transaction
	if tx.UnsafeMeta.V == 3 {
		sorobanMeta := tx.UnsafeMeta.V3.SorobanMeta
		if sorobanMeta != nil {
			// Based on the debug output, SorobanTransactionMeta has:
			// - Events
			// - ReturnValue
			// - DiagnosticEvents
			// But no TTL extension information in this version

			// If TTL extensions are added in future versions, they would be processed here
		}
	}

	return extensions
}
