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
	Timestamp        time.Time              `json:"timestamp"`
	LedgerSequence   uint32                 `json:"ledger_sequence"`
	TransactionHash  string                 `json:"transaction_hash"`
	ContractID       string                 `json:"contract_id"`
	InvokingAccount  string                 `json:"invoking_account"`
	FunctionName     string                 `json:"function_name,omitempty"`
	ArgumentsRaw     []xdr.ScVal            `json:"arguments_raw,omitempty"` // Raw XDR arguments
	Arguments        []json.RawMessage      `json:"arguments,omitempty"`
	ArgumentsDecoded map[string]interface{} `json:"arguments_decoded,omitempty"`
	Successful       bool                   `json:"successful"`
	DiagnosticEvents []DiagnosticEvent      `json:"diagnostic_events,omitempty"`
	ContractCalls    []ContractCall         `json:"contract_calls,omitempty"`
	StateChanges     []StateChange          `json:"state_changes,omitempty"`
	TtlExtensions    []TtlExtension         `json:"ttl_extensions,omitempty"`
	ArchiveMetadata  *ArchiveSourceMetadata `json:"archive_metadata,omitempty"` // Source file provenance
}

// DiagnosticEvent represents a diagnostic event emitted during contract execution
type DiagnosticEvent struct {
	ContractID    string        `json:"contract_id"`
	Topics        []xdr.ScVal   `json:"topics"`         // Raw XDR topics
	TopicsDecoded []interface{} `json:"topics_decoded"` // Decoded human-readable topics
	Data          xdr.ScVal     `json:"data"`           // Raw XDR data
	DataDecoded   interface{}   `json:"data_decoded"`   // Decoded human-readable data
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
	ContractID  string      `json:"contract_id"`
	KeyRaw      xdr.ScVal   `json:"key_raw"`       // Raw XDR key
	Key         string      `json:"key"`           // Decoded human-readable key
	OldValueRaw xdr.ScVal   `json:"old_value_raw"` // Raw XDR old value
	OldValue    interface{} `json:"old_value"`     // Decoded human-readable old value
	NewValueRaw xdr.ScVal   `json:"new_value_raw"` // Raw XDR new value
	NewValue    interface{} `json:"new_value"`     // Decoded human-readable new value
	Operation   string      `json:"operation"`     // "create", "update", "delete"
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

	// Extract source metadata from incoming message
	archiveMetadata, _ := msg.GetArchiveMetadata()

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
				invocation, err := p.processContractInvocation(tx, opIndex, op, ledgerCloseMeta, archiveMetadata)
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
	archiveMetadata *ArchiveSourceMetadata,
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

	// Create invocation record with preserved source metadata
	invocation := &ContractInvocation{
		Timestamp:       time.Unix(int64(meta.LedgerHeaderHistoryEntry().Header.ScpValue.CloseTime), 0),
		LedgerSequence:  meta.LedgerSequence(),
		TransactionHash: tx.Result.TransactionHash.HexString(),
		ContractID:      contractID,
		InvokingAccount: invokingAccount.Address(),
		Successful:      successful,
		ArchiveMetadata: archiveMetadata,
	}

	// Extract function name and arguments
	if function := invokeHostFunction.HostFunction; function.Type == xdr.HostFunctionTypeHostFunctionTypeInvokeContract {
		invokeContract := function.MustInvokeContract()

		// Extract function name using robust method
		invocation.FunctionName = extractFunctionName(invokeContract)

		// Extract and convert arguments
		if len(invokeContract.Args) > 0 {
			argumentsRaw, rawArgs, decodedArgs, err := extractArguments(invokeContract.Args)
			if err != nil {
				log.Printf("Error extracting arguments: %v", err)
			}
			invocation.ArgumentsRaw = argumentsRaw
			invocation.Arguments = rawArgs
			invocation.ArgumentsDecoded = decodedArgs
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

				// Store raw topics
				topics := event.Body.V0.Topics

				// Decode topics
				var topicsDecoded []interface{}
				for _, topic := range topics {
					decoded, err := ConvertScValToJSON(topic)
					if err != nil {
						log.Printf("Error decoding topic: %v", err)
						decoded = nil
					}
					topicsDecoded = append(topicsDecoded, decoded)
				}

				// Store raw data
				data := event.Body.V0.Data

				// Decode data
				dataDecoded, err := ConvertScValToJSON(data)
				if err != nil {
					log.Printf("Error decoding event data: %v", err)
					dataDecoded = nil
				}

				events = append(events, DiagnosticEvent{
					ContractID:    contractID,
					Topics:        topics,
					TopicsDecoded: topicsDecoded,
					Data:          data,
					DataDecoded:   dataDecoded,
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

	// Extract state changes from ledger changes in the transaction meta
	txChanges, err := tx.GetChanges()
	if err != nil {
		log.Printf("Error getting transaction changes: %v", err)
		return changes
	}

	for _, change := range txChanges {
		// We're only interested in contract data changes
		if change.Type != xdr.LedgerEntryTypeContractData {
			continue
		}

		switch change.ChangeType {
		case xdr.LedgerEntryChangeTypeLedgerEntryCreated:
			if change.Post != nil && change.Post.Data.Type == xdr.LedgerEntryTypeContractData {
				contractData := change.Post.Data.ContractData
				if contractData != nil {
					if stateChange := p.extractStateChangeFromContractData(*contractData, xdr.ScVal{}, contractData.Val, "create"); stateChange != nil {
						changes = append(changes, *stateChange)
					}
				}
			}

		case xdr.LedgerEntryChangeTypeLedgerEntryUpdated:
			if change.Pre != nil && change.Post != nil &&
				change.Pre.Data.Type == xdr.LedgerEntryTypeContractData &&
				change.Post.Data.Type == xdr.LedgerEntryTypeContractData {

				preData := change.Pre.Data.ContractData
				postData := change.Post.Data.ContractData
				if preData != nil && postData != nil {
					if stateChange := p.extractStateChangeFromContractData(*postData, preData.Val, postData.Val, "update"); stateChange != nil {
						changes = append(changes, *stateChange)
					}
				}
			}

		case xdr.LedgerEntryChangeTypeLedgerEntryRemoved:
			if change.Pre != nil && change.Pre.Data.Type == xdr.LedgerEntryTypeContractData {
				contractData := change.Pre.Data.ContractData
				if contractData != nil {
					if stateChange := p.extractStateChangeFromContractData(*contractData, contractData.Val, xdr.ScVal{}, "delete"); stateChange != nil {
						changes = append(changes, *stateChange)
					}
				}
			}
		}
	}

	return changes
}

// extractStateChangeFromContractData extracts state change information from contract data
// Helper function to reduce code duplication in extractStateChanges
func (p *ContractInvocationProcessor) extractStateChangeFromContractData(
	contractData xdr.ContractDataEntry,
	oldValueRaw, newValueRaw xdr.ScVal,
	operation string,
) *StateChange {
	// Extract contract ID
	contractIDBytes := contractData.Contract.ContractId
	if contractIDBytes == nil {
		log.Printf("Contract ID is nil in state change data")
		return nil
	}

	contractID, err := strkey.Encode(strkey.VersionByteContract, contractIDBytes[:])
	if err != nil {
		log.Printf("Error encoding contract ID: %v", err)
		return nil
	}

	// Extract key
	keyRaw := contractData.Key
	var key string
	if keyDecoded, err := ConvertScValToJSON(keyRaw); err == nil {
		if keyStr, ok := keyDecoded.(string); ok {
			key = keyStr
		} else {
			// Convert complex keys to JSON string representation
			if keyBytes, err := json.Marshal(keyDecoded); err == nil {
				key = string(keyBytes)
			}
		}
	}

	// Decode values
	var oldValue, newValue interface{}
	if operation != "create" {
		oldValue, _ = ConvertScValToJSON(oldValueRaw)
	}
	if operation != "delete" {
		newValue, _ = ConvertScValToJSON(newValueRaw)
	}

	return &StateChange{
		ContractID:  contractID,
		KeyRaw:      keyRaw,
		Key:         key,
		OldValueRaw: oldValueRaw,
		OldValue:    oldValue,
		NewValueRaw: newValueRaw,
		NewValue:    newValue,
		Operation:   operation,
	}
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

// extractFunctionName extracts the function name from a contract invocation
func extractFunctionName(invokeContract xdr.InvokeContractArgs) string {
	// Primary method: Use FunctionName field directly
	if len(invokeContract.FunctionName) > 0 {
		return string(invokeContract.FunctionName)
	}

	// Fallback: Check first argument if it contains function name
	if len(invokeContract.Args) > 0 {
		functionName := GetFunctionNameFromScVal(invokeContract.Args[0])
		if functionName != "" {
			return functionName
		}
	}

	return "unknown"
}

// extractArguments extracts and converts all function arguments with dual representation
func extractArguments(args []xdr.ScVal) ([]xdr.ScVal, []json.RawMessage, map[string]interface{}, error) {
	// Create a copy of the input slice to avoid unintended side effects
	argsCopy := make([]xdr.ScVal, len(args))
	copy(argsCopy, args)

	jsonRawArgs := make([]json.RawMessage, 0, len(args))
	decodedArgs := make(map[string]interface{})

	for i, arg := range args {
		// Convert ScVal to JSON-serializable format
		converted, err := ConvertScValToJSON(arg)
		if err != nil {
			log.Printf("Error converting argument %d: %v", i, err)
			converted = map[string]interface{}{
				"error": err.Error(),
				"type":  arg.Type.String(),
			}
		}

		// Store raw JSON
		jsonBytes, err := json.Marshal(converted)
		if err != nil {
			log.Printf("Error marshaling argument %d: %v", i, err)
			continue
		}
		jsonRawArgs = append(jsonRawArgs, jsonBytes)

		// Store in decoded map with index
		decodedArgs[fmt.Sprintf("arg_%d", i)] = converted
	}

	return argsCopy, jsonRawArgs, decodedArgs, nil
}
