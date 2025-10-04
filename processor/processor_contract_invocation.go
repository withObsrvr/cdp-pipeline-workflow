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
	FromContract   string            `json:"from_contract"`
	ToContract     string            `json:"to_contract"`
	Function       string            `json:"function"`
	Arguments      []interface{}     `json:"arguments,omitempty"`
	ArgumentsRaw   []xdr.ScVal       `json:"arguments_raw,omitempty"`
	CallDepth      int               `json:"call_depth"`
	AuthType       string            `json:"auth_type"` // "source_account", "contract", or "inferred"
	Successful     bool              `json:"successful"`
	ExecutionOrder int               `json:"execution_order"`
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
		ProcessedLedgers    uint32
		InvocationsFound    uint64
		SuccessfulInvokes   uint64
		FailedInvokes       uint64
		CrossContractCalls  uint64
		MaxCallDepth        int
		LastLedger          uint32
		LastProcessedTime   time.Time
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

	// Extract contract-to-contract calls from authorization data
	invocation.ContractCalls = p.extractContractCallsFromAuth(invokeHostFunction, contractID)
	
	// Debug: Log authorization entries
	if len(invokeHostFunction.Auth) == 0 {
		log.Printf("No authorization entries found for transaction %s", tx.Result.TransactionHash.HexString())
	} else {
		log.Printf("Found %d authorization entries for transaction %s", 
			len(invokeHostFunction.Auth), tx.Result.TransactionHash.HexString())
	}
	
	// If no calls found from auth, try extracting from transaction metadata
	if len(invocation.ContractCalls) == 0 {
		invocation.ContractCalls = p.extractContractCalls(tx, opIndex)
		// Also try to extract from diagnostic events
		p.extractContractCallsFromDiagnosticEvents(tx, opIndex, invocation)
	}
	
	// Correlate authorization data with diagnostic events to determine actual execution
	p.correlateWithDiagnosticEvents(tx, opIndex, invocation)

	// Extract state changes
	invocation.StateChanges = p.extractStateChanges(tx, opIndex)

	// Extract TTL extensions
	invocation.TtlExtensions = p.extractTtlExtensions(tx, opIndex)

	// Update cross-contract call statistics
	if len(invocation.ContractCalls) > 0 {
		p.mu.Lock()
		p.stats.CrossContractCalls += uint64(len(invocation.ContractCalls))
		
		// Track maximum call depth
		for _, call := range invocation.ContractCalls {
			if call.CallDepth > p.stats.MaxCallDepth {
				p.stats.MaxCallDepth = call.CallDepth
			}
		}
		p.mu.Unlock()
		
		log.Printf("Contract invocation with %d cross-contract calls (max depth: %d)", 
			len(invocation.ContractCalls), p.stats.MaxCallDepth)
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
	ProcessedLedgers    uint32
	InvocationsFound    uint64
	SuccessfulInvokes   uint64
	FailedInvokes       uint64
	CrossContractCalls  uint64
	MaxCallDepth        int
	LastLedger          uint32
	LastProcessedTime   time.Time
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

// extractContractCallsFromAuth extracts contract-to-contract calls from authorization data
func (p *ContractInvocationProcessor) extractContractCallsFromAuth(
	invokeOp xdr.InvokeHostFunctionOp,
	mainContract string,
) []ContractCall {
	var calls []ContractCall
	executionOrder := 0
	
	// Process each authorization entry
	for _, authEntry := range invokeOp.Auth {
		// Determine the auth type based on credentials
		authType := "source_account"
		if authEntry.Credentials.Type == xdr.SorobanCredentialsTypeSorobanCredentialsAddress {
			authType = "contract"
		}
		
		// Process the authorization tree
		p.processAuthorizationTree(
			&authEntry.RootInvocation,
			mainContract, // Start from the main contract
			&calls,
			0, // depth
			authType,
			&executionOrder,
		)
	}
	
	if len(calls) > 0 {
		log.Printf("Extracted %d cross-contract calls from authorization data", len(calls))
	}
	
	return calls
}

// extractContractCalls extracts contract calls from transaction meta
func (p *ContractInvocationProcessor) extractContractCalls(tx ingest.LedgerTransaction, opIndex int) []ContractCall {
	var calls []ContractCall

	// Check if we have Soroban meta in the transaction
	if tx.UnsafeMeta.V == 3 {
		sorobanMeta := tx.UnsafeMeta.V3.SorobanMeta
		if sorobanMeta != nil {
			log.Printf("Checking SorobanMeta for transaction %s", tx.Result.TransactionHash.HexString())
			
			// Check regular events (not just diagnostic)
			if sorobanMeta.Events != nil && len(sorobanMeta.Events) > 0 {
				log.Printf("Found %d regular events in SorobanMeta", len(sorobanMeta.Events))
			}
			
			// Process diagnostic events which may contain contract calls
			if len(sorobanMeta.DiagnosticEvents) > 0 {
				// Log diagnostic events for debugging
				log.Printf("Found %d diagnostic events in SorobanMeta", len(sorobanMeta.DiagnosticEvents))
			}
			
			// Check the return value which might contain invocation information
			// Note: ReturnValue is a ScVal, not a pointer, so we check its type
			if sorobanMeta.ReturnValue.Type != xdr.ScValTypeScvVoid {
				log.Printf("Found non-void return value in SorobanMeta")
			}
		}
	}

	return calls
}

// processAuthorizationTree processes a contract invocation tree and extracts contract calls
func (p *ContractInvocationProcessor) processAuthorizationTree(
	invocation *xdr.SorobanAuthorizedInvocation,
	fromContract string,
	calls *[]ContractCall,
	depth int,
	authType string,
	executionOrder *int,
) {
	if invocation == nil {
		return
	}

	// Get the contract ID for this invocation
	var contractID string
	var functionName string
	var args []interface{}
	
	if invocation.Function.Type == xdr.SorobanAuthorizedFunctionTypeSorobanAuthorizedFunctionTypeContractFn {
		contractFn := invocation.Function.ContractFn
		
		// Get contract ID
		contractIDBytes := contractFn.ContractAddress.ContractId
		var err error
		contractID, err = strkey.Encode(strkey.VersionByteContract, contractIDBytes[:])
		if err != nil {
			log.Printf("Error encoding contract ID for invocation: %v", err)
			return
		}
		
		// Get function name
		functionName = string(contractFn.FunctionName)
		
		// Extract arguments
		if len(contractFn.Args) > 0 {
			args = make([]interface{}, 0, len(contractFn.Args))
			for _, arg := range contractFn.Args {
				if decoded, err := ConvertScValToJSON(arg); err == nil {
					args = append(args, decoded)
				}
			}
		}
		
		// Log the authorization for debugging
		log.Printf("Authorization tree: depth=%d, from=%s, to=%s, function=%s, args=%d", 
			depth, fromContract, contractID, functionName, len(args))
	}

	// If we have both a from and to contract, record the call (skip self-calls)
	if fromContract != "" && contractID != "" && fromContract != contractID {
		*calls = append(*calls, ContractCall{
			FromContract:   fromContract,
			ToContract:     contractID,
			Function:       functionName,
			Arguments:      args,
			CallDepth:      depth,
			AuthType:       authType,
			Successful:     true, // Default to true, will be updated by diagnostic events
			ExecutionOrder: *executionOrder,
		})
		*executionOrder++
	}

	// Process sub-invocations recursively
	for _, subInvocation := range invocation.SubInvocations {
		p.processAuthorizationTree(
			&subInvocation,
			contractID,
			calls,
			depth + 1,
			authType,
			executionOrder,
		)
	}
}

// extractContractCallsFromDiagnosticEvents extracts cross-contract calls from diagnostic events
func (p *ContractInvocationProcessor) extractContractCallsFromDiagnosticEvents(
	tx ingest.LedgerTransaction,
	opIndex int,
	invocation *ContractInvocation,
) {
	// Safety check
	if invocation == nil {
		log.Printf("ERROR: invocation is nil in extractContractCallsFromDiagnosticEvents")
		return
	}
	
	// Get diagnostic events
	diagnosticEvents, err := tx.GetDiagnosticEvents()
	if err != nil {
		log.Printf("Error getting diagnostic events: %v", err)
		return
	}
	
	if len(diagnosticEvents) == 0 {
		return
	}
	
	log.Printf("Checking %d diagnostic events for cross-contract calls", len(diagnosticEvents))
	
	// Look for contract invocation patterns in diagnostic events
	executionOrder := len(invocation.ContractCalls)
	currentDepth := 0
	
	// Initialize call stack - start with main contract if available
	callStack := []string{}
	if invocation.ContractID != "" {
		callStack = append(callStack, invocation.ContractID)
	}
	
	// Track function calls to match fn_call with fn_return
	// TODO: Future enhancement to match fn_call with fn_return events
	
	for i, diagEvent := range diagnosticEvents {
		// Safety check for the event structure
		if diagEvent.Event.ContractId == nil {
			log.Printf("Diagnostic event %d has nil ContractId, skipping", i)
			continue
		}
		
		// Get the contract that emitted this event
		eventContractID, err := strkey.Encode(strkey.VersionByteContract, diagEvent.Event.ContractId[:])
		if err != nil {
			log.Printf("Error encoding contract ID from diagnostic event %d: %v", i, err)
			continue
		}
		
		// Log the event details for debugging
		// eventType := "unknown"
		// switch diagEvent.Event.Type {
		// case 0:
		//	eventType = "system"
		// case 1:
		//	eventType = "contract"
		// case 2:
		//	eventType = "diagnostic"
		// }
		
		// Get first topic if available
		firstTopic := ""
		if diagEvent.Event.Body.V == 0 && diagEvent.Event.Body.V0 != nil && len(diagEvent.Event.Body.V0.Topics) > 0 {
			if decoded, err := ConvertScValToJSON(diagEvent.Event.Body.V0.Topics[0]); err == nil {
				firstTopic = fmt.Sprintf("%v", decoded)
			}
		}
		
		// Only log for debugging when needed
		// log.Printf("Diagnostic event %d: contract=%s, type=%s, topic=%s, successful=%v", 
		//	i, eventContractID, eventType, firstTopic, diagEvent.InSuccessfulContractCall)
		
		// For fn_call events from the main contract, check if they're calling another contract
		// by looking at the next event
		if firstTopic == "fn_call" && eventContractID == invocation.ContractID && i+1 < len(diagnosticEvents) {
			nextEvent := diagnosticEvents[i+1]
			if nextEvent.Event.ContractId != nil {
				nextContractID, err := strkey.Encode(strkey.VersionByteContract, nextEvent.Event.ContractId[:])
				if err == nil && nextContractID != eventContractID {
					// This fn_call is calling another contract
					// Extract function name and arguments from the fn_call data
					functionName := "unknown"
					var arguments []interface{}
					
					if diagEvent.Event.Body.V == 0 && diagEvent.Event.Body.V0 != nil {
						if decoded, err := ConvertScValToJSON(diagEvent.Event.Body.V0.Data); err == nil {
							// log.Printf("fn_call data for cross-contract call: %+v", decoded)
							// Try to extract function name and arguments from various formats
							switch data := decoded.(type) {
							case []interface{}:
								// For oracle calls, the pattern might be ["Stellar", contract_address]
								// which suggests lastprice(source, asset)
								if len(data) == 2 {
									if source, ok := data[0].(string); ok && source == "Stellar" {
										functionName = "lastprice"
										arguments = data // ["Stellar", contract_address]
									}
								} else if len(data) == 0 {
									// Empty args might be decimals()
									functionName = "decimals"
									arguments = []interface{}{}
								} else if len(data) >= 3 {
									// For token transfers, pattern is [from_contract, to_account, amount]
									functionName = "transfer"
									arguments = data // All arguments
								}
								// Otherwise try second element for function name
								if functionName == "unknown" && len(data) > 1 {
									if fnSymbol, ok := data[1].(string); ok {
										functionName = fnSymbol
										// Arguments would be everything after function name
										if len(data) > 2 {
											arguments = data[2:]
										}
									}
								}
								// If still unknown, just use all data as arguments
								if functionName == "unknown" && len(data) > 0 {
									arguments = data
								}
							case nil:
								// No arguments might indicate decimals() or similar
								functionName = "decimals"
								arguments = []interface{}{}
							default:
								// Single value argument
								arguments = []interface{}{decoded}
							}
							
							// log.Printf("Inferred function name: %s with args: %+v", functionName, arguments)
						}
					}
					
					// Add the cross-contract call
					call := ContractCall{
						FromContract:   eventContractID,
						ToContract:     nextContractID,
						Function:       functionName,
						Arguments:      arguments,
						CallDepth:      currentDepth,
						AuthType:       "inferred",
						Successful:     diagEvent.InSuccessfulContractCall,
						ExecutionOrder: executionOrder,
					}
					
					invocation.ContractCalls = append(invocation.ContractCalls, call)
					executionOrder++
					
					log.Printf("Found cross-contract call from fn_call: %s -> %s (%s)", 
						eventContractID, nextContractID, functionName)
				}
			}
		}
		
		// Check if this is a different contract than the main one (for contract events)
		if eventContractID != invocation.ContractID && len(callStack) > 0 && firstTopic != "fn_call" && firstTopic != "fn_return" {
			// This could be a cross-contract call
			fromContract := callStack[len(callStack)-1]
			
			// Check if we've already recorded this call
			alreadyRecorded := false
			for _, call := range invocation.ContractCalls {
				if call.FromContract == fromContract && call.ToContract == eventContractID {
					alreadyRecorded = true
					break
				}
			}
			
			if !alreadyRecorded {
				// Extract function name from event data if possible
				functionName := "unknown"
				
				// For fn_call events, we need to look at the next event to find what was called
				isFnCall := false
				if diagEvent.Event.Body.V == 0 && diagEvent.Event.Body.V0 != nil && len(diagEvent.Event.Body.V0.Topics) > 0 {
					if decoded, err := ConvertScValToJSON(diagEvent.Event.Body.V0.Topics[0]); err == nil {
						if fnName, ok := decoded.(string); ok {
							if fnName == "fn_return" {
								// Skip return events
								continue
							} else if fnName == "fn_call" {
								isFnCall = true
							}
						}
					}
				}
				
				// For fn_call events, look at the data to extract the actual function name
				if isFnCall && diagEvent.Event.Body.V == 0 && diagEvent.Event.Body.V0 != nil {
					if decoded, err := ConvertScValToJSON(diagEvent.Event.Body.V0.Data); err == nil {
						log.Printf("fn_call event %d data: %+v", i, decoded)
						// The data often contains [contract_id, function_name, ...args]
						if dataArray, ok := decoded.([]interface{}); ok && len(dataArray) > 1 {
							// Second element is often the function name
							if fnSymbol, ok := dataArray[1].(string); ok {
								functionName = fnSymbol
								log.Printf("Extracted function name from fn_call: %s", functionName)
							}
						}
					}
				}
				
				// For diagnostic events, we need to look at the event type and topics differently
				// Type 1 = Contract events (usually have meaningful topics)
				// Type 2 = Diagnostic events (often just fn_call/fn_return)
				if diagEvent.Event.Type == xdr.ContractEventTypeContract && diagEvent.Event.Body.V == 0 && diagEvent.Event.Body.V0 != nil {
					v0Body := diagEvent.Event.Body.V0
					if len(v0Body.Topics) > 0 {
						// For contract events, first topic is often the function/event name
						if decoded, err := ConvertScValToJSON(v0Body.Topics[0]); err == nil {
							if fnName, ok := decoded.(string); ok {
								functionName = fnName
							}
						}
					}
				} else if diagEvent.Event.Type == xdr.ContractEventTypeDiagnostic {
					// For diagnostic events, we might need to parse differently
					if diagEvent.Event.Body.V == 0 && diagEvent.Event.Body.V0 != nil {
						v0Body := diagEvent.Event.Body.V0
						// Try to extract function info from topics
						if len(v0Body.Topics) > 0 {
							if decoded, err := ConvertScValToJSON(v0Body.Topics[0]); err == nil {
								if fnName, ok := decoded.(string); ok {
									// Skip generic diagnostic events
									if fnName != "fn_return" && fnName != "fn_call" {
										functionName = fnName
									}
								}
							}
						}
						
						// If we only got fn_call/fn_return, try to extract from event data
						if (functionName == "unknown" || functionName == "fn_return" || functionName == "fn_call") && v0Body.Data.Type != xdr.ScValTypeScvVoid {
							// The data might contain the actual function information
							if decoded, err := ConvertScValToJSON(v0Body.Data); err == nil {
								// Log the structure to understand what's available
								if functionName == "fn_call" || functionName == "unknown" {
									log.Printf("Diagnostic event %d data for %s (type=%d): %+v", i, eventContractID, diagEvent.Event.Type, decoded)
								}
								
								// Try various patterns to extract function name
								switch data := decoded.(type) {
								case map[string]interface{}:
									// Look for common function name fields
									for _, key := range []string{"function", "fn", "method", "name"} {
										if fn, exists := data[key]; exists {
											if fnStr, ok := fn.(string); ok {
												functionName = fnStr
												break
											}
										}
									}
								case []interface{}:
									// Sometimes function name might be in an array
									if len(data) > 0 {
										if fnStr, ok := data[0].(string); ok {
											functionName = fnStr
										}
									}
								case string:
									// Direct string might be the function name
									if data != "" && data != "fn_return" && data != "fn_call" {
										functionName = data
									}
								}
							}
						}
					}
				}
				
				// Extract arguments if this is a contract event (type 1)
				var eventArgs []interface{}
				if diagEvent.Event.Type == xdr.ContractEventTypeContract && diagEvent.Event.Body.V == 0 && diagEvent.Event.Body.V0 != nil {
					// For contract events, topics after the first one are often arguments
					if len(diagEvent.Event.Body.V0.Topics) > 1 {
						eventArgs = make([]interface{}, 0, len(diagEvent.Event.Body.V0.Topics)-1)
						for i := 1; i < len(diagEvent.Event.Body.V0.Topics); i++ {
							if decoded, err := ConvertScValToJSON(diagEvent.Event.Body.V0.Topics[i]); err == nil {
								eventArgs = append(eventArgs, decoded)
							}
						}
					}
					// Also check if there's data that might be an argument
					if diagEvent.Event.Body.V0.Data.Type != xdr.ScValTypeScvVoid {
						if decoded, err := ConvertScValToJSON(diagEvent.Event.Body.V0.Data); err == nil {
							eventArgs = append(eventArgs, decoded)
						}
					}
				}
				
				// Add the cross-contract call
				call := ContractCall{
					FromContract:   fromContract,
					ToContract:     eventContractID,
					Function:       functionName,
					Arguments:      eventArgs,
					CallDepth:      currentDepth,
					AuthType:       "inferred", // Since it's from diagnostic events
					Successful:     diagEvent.InSuccessfulContractCall,
					ExecutionOrder: executionOrder,
				}
				
				invocation.ContractCalls = append(invocation.ContractCalls, call)
				executionOrder++
				
				log.Printf("Found cross-contract call from diagnostic event %d: %s -> %s (%s)", 
					i, fromContract, eventContractID, functionName)
			}
			
			// Update call stack if this is a new contract
			if !containsString(callStack, eventContractID) {
				callStack = append(callStack, eventContractID)
				currentDepth++
			}
		}
	}
	
	if len(invocation.ContractCalls) > 0 {
		log.Printf("Extracted %d cross-contract calls from diagnostic events", len(invocation.ContractCalls))
	}
}

// Helper function to check if a slice contains a string
func containsString(slice []string, item string) bool {
	for _, s := range slice {
		if s == item {
			return true
		}
	}
	return false
}

// correlateWithDiagnosticEvents correlates authorization data with diagnostic events
func (p *ContractInvocationProcessor) correlateWithDiagnosticEvents(
	tx ingest.LedgerTransaction,
	opIndex int,
	invocation *ContractInvocation,
) {
	// Get diagnostic events to determine actual execution
	diagnosticEvents, err := tx.GetDiagnosticEvents()
	if err != nil {
		log.Printf("Error getting diagnostic events: %v", err)
		return
	}
	
	if len(diagnosticEvents) == 0 {
		return
	}
	
	log.Printf("Correlating %d contract calls with %d diagnostic events", 
		len(invocation.ContractCalls), len(diagnosticEvents))
	
	// Create a map of authorized calls for quick lookup
	authCallMap := make(map[string]*ContractCall)
	for i := range invocation.ContractCalls {
		call := &invocation.ContractCalls[i]
		key := fmt.Sprintf("%s->%s:%s", call.FromContract, call.ToContract, call.Function)
		authCallMap[key] = call
	}
	
	// Process diagnostic events to determine actual execution
	successfulCalls := 0
	failedCalls := 0
	
	for _, event := range diagnosticEvents {
		// Only process contract events
		if event.Event.Type == xdr.ContractEventTypeContract {
			contractID, err := strkey.Encode(strkey.VersionByteContract, event.Event.ContractId[:])
			if err != nil {
				log.Printf("Error encoding contract ID from diagnostic event: %v", err)
				continue
			}
			
			// Update success status based on diagnostic events
			// Note: This is a simplified implementation. In reality, we'd need to parse
			// event topics/data to identify the specific invocation
			for key, call := range authCallMap {
				if call.ToContract == contractID {
					call.Successful = event.InSuccessfulContractCall
					if event.InSuccessfulContractCall {
						successfulCalls++
					} else {
						failedCalls++
					}
					log.Printf("Updated call %s success status to %v", key, event.InSuccessfulContractCall)
					break
				}
			}
		}
	}
	
	if successfulCalls > 0 || failedCalls > 0 {
		log.Printf("Correlation complete: %d successful, %d failed calls", successfulCalls, failedCalls)
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
