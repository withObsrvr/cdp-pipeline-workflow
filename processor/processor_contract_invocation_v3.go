package processor

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"sort"
	"sync"
	"time"

	"github.com/stellar/go/ingest"
	"github.com/stellar/go/strkey"
	"github.com/stellar/go/xdr"
)

// ============================================================================
// Contract Invocation Processor V3
// Combines V2's comprehensive processing with V1's single-message output
// ============================================================================

// ContractInvocationV3 represents the comprehensive output structure
type ContractInvocationV3 struct {
	// Core V1-compatible fields
	Timestamp        time.Time              `json:"timestamp"`
	LedgerSequence   uint32                 `json:"ledger_sequence"`
	TransactionHash  string                 `json:"transaction_hash"`
	ContractID       string                 `json:"contract_id"`
	InvokingAccount  string                 `json:"invoking_account"`
	FunctionName     string                 `json:"function_name,omitempty"`
	ArgumentsRaw     []xdr.ScVal            `json:"arguments_raw,omitempty"`
	Arguments        []json.RawMessage      `json:"arguments,omitempty"`
	ArgumentsDecoded map[string]interface{} `json:"arguments_decoded,omitempty"`
	Successful       bool                   `json:"successful"`
	
	// V1-compatible arrays (flattened from call graph)
	DiagnosticEvents []DiagnosticEvent      `json:"diagnostic_events,omitempty"`
	ContractCalls    []ContractCallEnhanced `json:"contract_calls,omitempty"`
	StateChanges     []StateChange          `json:"state_changes,omitempty"`
	TtlExtensions    []TtlExtension         `json:"ttl_extensions,omitempty"`
	
	// Enhanced V2 data
	AuthContext      *AuthContext           `json:"auth_context,omitempty"`
	CallGraph        *CallGraphNode         `json:"call_graph,omitempty"`
	AllEvents        []EnrichedEvent        `json:"all_events,omitempty"`
	EventSummary     *EventSummary          `json:"event_summary,omitempty"`
	
	// Metadata
	ProcessorVersion string                 `json:"processor_version"`
	ArchiveMetadata  *ArchiveSourceMetadata `json:"archive_metadata,omitempty"`
}

// ContractCallEnhanced is an enhanced version of V1's ContractCall with auth context
type ContractCallEnhanced struct {
	// V1 fields
	FromContract   string            `json:"from_contract"`
	ToContract     string            `json:"to_contract"`
	Function       string            `json:"function"`
	Arguments      []interface{}     `json:"arguments,omitempty"`
	ArgumentsRaw   []xdr.ScVal       `json:"arguments_raw,omitempty"`
	CallDepth      int               `json:"call_depth"`
	AuthType       string            `json:"auth_type"`
	Successful     bool              `json:"successful"`
	ExecutionOrder int               `json:"execution_order"`
	
	// V2 enhancements
	AuthContext    *AuthContext      `json:"auth_context,omitempty"`
	Events         []string          `json:"event_types,omitempty"` // Event types emitted by this call
	GasUsed        uint64            `json:"gas_used,omitempty"`
}

// EventSummary provides a high-level summary of all events
type EventSummary struct {
	TotalEvents      int               `json:"total_events"`
	ContractEvents   int               `json:"contract_events"`
	DiagnosticEvents int               `json:"diagnostic_events"`
	EventTypes       map[string]int    `json:"event_types"` // Count by event subtype
	ContractsInvolved []string         `json:"contracts_involved"`
}

// V3Config represents configuration for the V3 processor
type V3Config struct {
	NetworkPassphrase  string        `json:"network_passphrase"`
	IncludeRawXDR      bool          `json:"include_raw_xdr"`
	IncludeCallGraph   bool          `json:"include_call_graph"`
	IncludeAllEvents   bool          `json:"include_all_events"`
	IncludeAuthProof   bool          `json:"include_auth_proof"`
	MaxCallDepth       int           `json:"max_call_depth"`
	EventFilters       *EventFilters `json:"event_filters,omitempty"`
}

// ContractInvocationProcessorV3 implements the V3 processor
type ContractInvocationProcessorV3 struct {
	processors []Processor
	config     V3Config
	mu         sync.RWMutex
	stats      ProcessorStatsV3
	
	// Reuse V2's processing logic
	v2Processor *ContractInvocationProcessorV2
}

// ProcessorStatsV3 tracks V3 processor statistics
type ProcessorStatsV3 struct {
	ProcessedLedgers     uint32
	ProcessedTxs         uint64
	TotalInvocations     uint64
	TotalEvents          uint64
	TotalContractCalls   uint64
	MaxCallDepth         int
	AvgEventsPerTx       float64
	LastLedger           uint32
	LastProcessedTime    time.Time
}

// NewContractInvocationProcessorV3 creates a new V3 processor instance
func NewContractInvocationProcessorV3(config map[string]interface{}) (*ContractInvocationProcessorV3, error) {
	v3Config := V3Config{
		IncludeRawXDR:    true,
		IncludeCallGraph: false, // Default to false for smaller output
		IncludeAllEvents: false, // Default to false for V1 compatibility
		MaxCallDepth:     10,
	}

	// Parse network passphrase (required)
	networkPassphrase, ok := config["network_passphrase"].(string)
	if !ok {
		return nil, fmt.Errorf("missing network_passphrase in configuration")
	}
	v3Config.NetworkPassphrase = networkPassphrase

	// Parse optional configurations
	if includeRaw, ok := config["include_raw_xdr"].(bool); ok {
		v3Config.IncludeRawXDR = includeRaw
	}
	if includeGraph, ok := config["include_call_graph"].(bool); ok {
		v3Config.IncludeCallGraph = includeGraph
	}
	if includeEvents, ok := config["include_all_events"].(bool); ok {
		v3Config.IncludeAllEvents = includeEvents
	}
	if includeAuth, ok := config["include_auth_proof"].(bool); ok {
		v3Config.IncludeAuthProof = includeAuth
	}
	if maxDepth, ok := config["max_call_depth"].(int); ok {
		v3Config.MaxCallDepth = maxDepth
	}

	// Parse event filters
	if filters, ok := config["event_filters"].(map[string]interface{}); ok {
		v3Config.EventFilters = &EventFilters{}
		// Parse filter fields (reuse V2 logic)
		if contractIDs, ok := filters["contract_ids"].([]interface{}); ok {
			for _, id := range contractIDs {
				if idStr, ok := id.(string); ok {
					v3Config.EventFilters.ContractIDs = append(v3Config.EventFilters.ContractIDs, idStr)
				}
			}
		}
	}

	// Create an internal V2 processor for reusing processing logic
	v2Config := map[string]interface{}{
		"network_passphrase": networkPassphrase,
		"output_mode":        "event_per_message", // Not used in V3
		"include_raw_xdr":    v3Config.IncludeRawXDR,
		"include_auth_proof": v3Config.IncludeAuthProof,
		"max_call_depth":     v3Config.MaxCallDepth,
	}
	
	if v3Config.EventFilters != nil {
		v2Config["event_filters"] = config["event_filters"]
	}
	
	v2Processor, err := NewContractInvocationProcessorV2(v2Config)
	if err != nil {
		return nil, fmt.Errorf("error creating internal V2 processor: %w", err)
	}

	return &ContractInvocationProcessorV3{
		config:      v3Config,
		v2Processor: v2Processor,
	}, nil
}

// Subscribe adds a processor to receive output
func (p *ContractInvocationProcessorV3) Subscribe(processor Processor) {
	p.processors = append(p.processors, processor)
}

// Process handles incoming ledger data and outputs comprehensive invocation data
func (p *ContractInvocationProcessorV3) Process(ctx context.Context, msg Message) error {
	ledgerCloseMeta, ok := msg.Payload.(xdr.LedgerCloseMeta)
	if !ok {
		return fmt.Errorf("expected xdr.LedgerCloseMeta, got %T", msg.Payload)
	}

	// Extract source metadata
	archiveMetadata, _ := msg.GetArchiveMetadata()

	sequence := ledgerCloseMeta.LedgerSequence()
	log.Printf("ContractInvocationV3: Processing ledger %d", sequence)

	txReader, err := ingest.NewLedgerTransactionReaderFromLedgerCloseMeta(p.config.NetworkPassphrase, ledgerCloseMeta)
	if err != nil {
		return fmt.Errorf("error creating transaction reader: %w", err)
	}
	defer txReader.Close()

	// Process each transaction
	txCount := 0
	invocationCount := 0
	
	for {
		tx, err := txReader.Read()
		if err == io.EOF {
			break
		}
		if err != nil {
			return fmt.Errorf("error reading transaction: %w", err)
		}

		// Process transaction and get comprehensive invocation data
		invocations, err := p.processTransaction(tx, ledgerCloseMeta, archiveMetadata)
		if err != nil {
			log.Printf("Error processing transaction %s: %v", tx.Result.TransactionHash.HexString(), err)
			continue
		}

		// Forward each invocation as a single message
		for _, invocation := range invocations {
			if err := p.forwardInvocation(ctx, invocation); err != nil {
				log.Printf("Error forwarding invocation: %v", err)
			}
			invocationCount++
		}
		
		txCount++
	}

	// Update statistics
	p.mu.Lock()
	p.stats.ProcessedLedgers++
	p.stats.ProcessedTxs += uint64(txCount)
	p.stats.TotalInvocations += uint64(invocationCount)
	p.stats.LastLedger = sequence
	p.stats.LastProcessedTime = time.Now()
	p.mu.Unlock()

	log.Printf("ContractInvocationV3: Processed ledger %d - %d transactions, %d invocations", 
		sequence, txCount, invocationCount)

	return nil
}

// processTransaction processes a single transaction and returns all contract invocations
func (p *ContractInvocationProcessorV3) processTransaction(
	tx ingest.LedgerTransaction,
	meta xdr.LedgerCloseMeta,
	archiveMetadata *ArchiveSourceMetadata,
) ([]ContractInvocationV3, error) {
	var invocations []ContractInvocationV3

	// Get transaction submitter
	txSubmitter := tx.Envelope.SourceAccount().ToAccountId().Address()
	timestamp := time.Unix(int64(meta.LedgerHeaderHistoryEntry().Header.ScpValue.CloseTime), 0)

	// Process each operation
	for opIndex, op := range tx.Envelope.Operations() {
		if op.Body.Type != xdr.OperationTypeInvokeHostFunction {
			continue
		}

		invokeOp := op.Body.MustInvokeHostFunctionOp()
		
		// Extract main contract info
		var mainContractID string
		var functionName string
		var args []interface{}
		var argsRaw []xdr.ScVal

		if invokeOp.HostFunction.Type == xdr.HostFunctionTypeHostFunctionTypeInvokeContract {
			invokeContract := invokeOp.HostFunction.MustInvokeContract()
			
			// Get contract ID
			contractIDBytes := invokeContract.ContractAddress.ContractId
			mainContractID, _ = strkey.Encode(strkey.VersionByteContract, contractIDBytes[:])
			
			// Get function name
			functionName = string(invokeContract.FunctionName)
			
			// Get arguments
			argsRaw = invokeContract.Args
			for _, arg := range argsRaw {
				decoded, _ := ConvertScValToJSON(arg)
				args = append(args, decoded)
			}
		}

		// Get invoking account
		var invokingAccount xdr.AccountId
		if op.SourceAccount != nil {
			invokingAccount = op.SourceAccount.ToAccountId()
		} else {
			invokingAccount = tx.Envelope.SourceAccount().ToAccountId()
		}
		
		// Use V2 processor to build auth tree and call graph
		authTree := p.v2Processor.buildAuthorizationTree(invokeOp.Auth, txSubmitter)
		callGraph, err := p.v2Processor.buildCallGraph(tx, opIndex, invokeOp, authTree)
		if err != nil {
			log.Printf("Error building call graph: %v", err)
			continue
		}

		// Collect all events using V2's flattening
		allEvents := p.v2Processor.flattenToEnrichedEvents(
			callGraph,
			authTree,
			tx.Result.TransactionHash.HexString(),
			meta.LedgerSequence(),
			timestamp,
			archiveMetadata,
		)

		// Apply filters if configured
		if p.config.EventFilters != nil {
			allEvents = p.v2Processor.applyEventFilters(allEvents)
		}

		// Build the comprehensive V3 invocation
		invocation := ContractInvocationV3{
			// Core fields
			Timestamp:       timestamp,
			LedgerSequence:  meta.LedgerSequence(),
			TransactionHash: tx.Result.TransactionHash.HexString(),
			ContractID:      mainContractID,
			InvokingAccount: invokingAccount.Address(),
			FunctionName:    functionName,
			Arguments:       p.convertArgsToRawMessages(args),
			ArgumentsDecoded: p.createArgumentsDecodedMap(args),
			Successful:      p.v2Processor.isOperationSuccessful(tx, opIndex),
			ProcessorVersion: "v3.0.0",
			ArchiveMetadata: archiveMetadata,
			
			// V2 enhancements
			AuthContext: p.v2Processor.createAuthContext(authTree, []string{mainContractID}),
		}

		// Include raw XDR if configured
		if p.config.IncludeRawXDR {
			invocation.ArgumentsRaw = argsRaw
		}

		// Include full call graph if configured
		if p.config.IncludeCallGraph {
			invocation.CallGraph = callGraph
		}

		// Include all events if configured
		if p.config.IncludeAllEvents {
			invocation.AllEvents = allEvents
		}

		// Flatten call graph to V1-compatible arrays
		p.flattenCallGraph(callGraph, &invocation, tx, opIndex, mainContractID)

		// Extract diagnostic events for V1 compatibility
		p.extractDiagnosticEvents(allEvents, &invocation)

		// Extract state changes from transaction
		invocation.StateChanges = p.extractStateChanges(tx, opIndex)

		// Build event summary
		invocation.EventSummary = p.buildEventSummary(allEvents, callGraph, &invocation)

		// Update statistics
		p.updateStats(callGraph, allEvents)

		invocations = append(invocations, invocation)
	}

	return invocations, nil
}

// flattenCallGraph converts the call graph into V1-compatible arrays
func (p *ContractInvocationProcessorV3) flattenCallGraph(
	node *CallGraphNode, 
	invocation *ContractInvocationV3,
	tx ingest.LedgerTransaction,
	opIndex int,
	mainContractID string,
) {
	// Extract auth-based calls from call graph
	var authCalls []ContractCallEnhanced
	if node != nil {
		p.extractCallsFromNode(node, &authCalls, node.ContractID)
	}
	
	// Extract diagnostic-based cross-contract calls
	diagCalls := p.extractCrossContractCallsFromDiagnosticEvents(tx, opIndex, mainContractID)
	
	// Merge calls from both sources
	callMap := make(map[string]ContractCallEnhanced)
	
	// Add auth-based calls first
	for _, call := range authCalls {
		// Skip self-referential calls
		if call.FromContract == call.ToContract {
			log.Printf("V3: Skipping self-referential call: %s -> %s", call.FromContract, call.ToContract)
			continue
		}
		key := fmt.Sprintf("%s->%s:%s", call.FromContract, call.ToContract, call.Function)
		callMap[key] = call
	}
	
	// Add diagnostic-based calls (may override if duplicate)
	for _, call := range diagCalls {
		// Skip self-referential calls
		if call.FromContract == call.ToContract {
			continue
		}
		key := fmt.Sprintf("%s->%s:%s", call.FromContract, call.ToContract, call.Function)
		// Only add if not already present from auth, or if diagnostic has more info
		if existing, exists := callMap[key]; !exists || 
			(existing.Function == "unknown" && call.Function != "unknown") {
			callMap[key] = call
		}
	}
	
	// Convert map to slice and sort by execution order
	var finalCalls []ContractCallEnhanced
	for _, call := range callMap {
		finalCalls = append(finalCalls, call)
	}
	
	// Sort by execution order for consistent output
	sort.Slice(finalCalls, func(i, j int) bool {
		return finalCalls[i].ExecutionOrder < finalCalls[j].ExecutionOrder
	})
	
	invocation.ContractCalls = finalCalls
	
	if len(finalCalls) > 0 {
		log.Printf("V3: Total contract calls after merging: %d (auth: %d, diag: %d)", 
			len(finalCalls), len(authCalls), len(diagCalls))
	}
}

// extractCallsFromNode recursively extracts calls from call graph
func (p *ContractInvocationProcessorV3) extractCallsFromNode(node *CallGraphNode, calls *[]ContractCallEnhanced, fromContract string) {
	for _, child := range node.Children {
		// Extract event types from this call
		var eventTypes []string
		eventTypeMap := make(map[string]bool)
		for _, event := range child.Events {
			if !eventTypeMap[event.EventSubtype] {
				eventTypes = append(eventTypes, event.EventSubtype)
				eventTypeMap[event.EventSubtype] = true
			}
		}

		call := ContractCallEnhanced{
			// V1 fields
			FromContract:   fromContract,
			ToContract:     child.ContractID,
			Function:       child.FunctionName,
			Arguments:      child.Arguments,
			CallDepth:      child.CallDepth,
			AuthType:       p.getAuthType(child.AuthContext),
			Successful:     child.Success,
			ExecutionOrder: child.ExecutionOrder,
			
			// V2 enhancements
			AuthContext: child.AuthContext,
			Events:      eventTypes,
			GasUsed:     child.GasUsed,
		}

		// Include raw args if configured
		if p.config.IncludeRawXDR {
			call.ArgumentsRaw = child.ArgumentsRaw
		}

		*calls = append(*calls, call)
		
		// Recurse to children
		p.extractCallsFromNode(child, calls, child.ContractID)
	}
}

// extractDiagnosticEvents extracts diagnostic events in V1 format
func (p *ContractInvocationProcessorV3) extractDiagnosticEvents(events []EnrichedEvent, invocation *ContractInvocationV3) {
	for _, event := range events {
		if event.EventType == "diagnostic" {
			diagEvent := DiagnosticEvent{
				ContractID:    event.ContractID,
				TopicsDecoded: event.Topics,
				DataDecoded:   event.Data,
			}
			
			if p.config.IncludeRawXDR {
				diagEvent.Topics = event.TopicsRaw
				diagEvent.Data = event.DataRaw
			}
			
			invocation.DiagnosticEvents = append(invocation.DiagnosticEvents, diagEvent)
		}
	}
}

// extractStateChanges extracts state changes from transaction
func (p *ContractInvocationProcessorV3) extractStateChanges(tx ingest.LedgerTransaction, opIndex int) []StateChange {
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
func (p *ContractInvocationProcessorV3) extractStateChangeFromContractData(
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

// buildEventSummary creates a summary of all events
func (p *ContractInvocationProcessorV3) buildEventSummary(
	events []EnrichedEvent, 
	callGraph *CallGraphNode,
	invocation *ContractInvocationV3,
) *EventSummary {
	summary := &EventSummary{
		TotalEvents:      len(events),
		EventTypes:       make(map[string]int),
		ContractsInvolved: make([]string, 0),
	}

	contractMap := make(map[string]bool)
	
	// Add main contract
	if invocation.ContractID != "" {
		contractMap[invocation.ContractID] = true
	}
	
	// Track contracts from events
	for _, event := range events {
		// Count by type
		if event.EventType == "contract" {
			summary.ContractEvents++
		} else if event.EventType == "diagnostic" {
			summary.DiagnosticEvents++
		}
		
		// Count by subtype
		summary.EventTypes[event.EventSubtype]++
		
		// Track contracts
		if event.ContractID != "" && !contractMap[event.ContractID] {
			contractMap[event.ContractID] = true
		}
	}
	
	// Track contracts from contract calls (including cross-contract calls)
	for _, call := range invocation.ContractCalls {
		if call.FromContract != "" && !contractMap[call.FromContract] {
			contractMap[call.FromContract] = true
		}
		if call.ToContract != "" && !contractMap[call.ToContract] {
			contractMap[call.ToContract] = true
		}
	}
	
	// Convert map to slice
	for contract := range contractMap {
		summary.ContractsInvolved = append(summary.ContractsInvolved, contract)
	}
	
	// Sort for consistent output
	sort.Strings(summary.ContractsInvolved)

	return summary
}

// Helper methods

func (p *ContractInvocationProcessorV3) getAuthType(authContext *AuthContext) string {
	if authContext == nil || authContext.AuthTree == nil {
		return "unknown"
	}
	
	if authContext.AuthTree.AuthorizerType == "account" {
		return "source_account"
	}
	
	return authContext.AuthTree.AuthorizerType
}

func (p *ContractInvocationProcessorV3) convertArgsToRawMessages(args []interface{}) []json.RawMessage {
	var rawMessages []json.RawMessage
	for _, arg := range args {
		jsonBytes, err := json.Marshal(arg)
		if err != nil {
			continue
		}
		rawMessages = append(rawMessages, json.RawMessage(jsonBytes))
	}
	return rawMessages
}

func (p *ContractInvocationProcessorV3) createArgumentsDecodedMap(args []interface{}) map[string]interface{} {
	decoded := make(map[string]interface{})
	for i, arg := range args {
		decoded[fmt.Sprintf("arg_%d", i)] = arg
	}
	return decoded
}

func (p *ContractInvocationProcessorV3) updateStats(callGraph *CallGraphNode, events []EnrichedEvent) {
	p.mu.Lock()
	defer p.mu.Unlock()
	
	// Count calls
	callCount := p.countCallsInGraph(callGraph)
	p.stats.TotalContractCalls += uint64(callCount)
	
	// Count events
	p.stats.TotalEvents += uint64(len(events))
	
	// Update max depth
	maxDepth := p.getMaxDepth(callGraph)
	if maxDepth > p.stats.MaxCallDepth {
		p.stats.MaxCallDepth = maxDepth
	}
	
	// Update average
	if p.stats.ProcessedTxs > 0 {
		p.stats.AvgEventsPerTx = float64(p.stats.TotalEvents) / float64(p.stats.ProcessedTxs)
	}
}

func (p *ContractInvocationProcessorV3) countCallsInGraph(node *CallGraphNode) int {
	if node == nil {
		return 0
	}
	
	count := 1
	for _, child := range node.Children {
		count += p.countCallsInGraph(child)
	}
	return count
}

func (p *ContractInvocationProcessorV3) getMaxDepth(node *CallGraphNode) int {
	if node == nil {
		return 0
	}
	
	maxChildDepth := 0
	for _, child := range node.Children {
		childDepth := p.getMaxDepth(child)
		if childDepth > maxChildDepth {
			maxChildDepth = childDepth
		}
	}
	
	return node.CallDepth + maxChildDepth
}

func (p *ContractInvocationProcessorV3) forwardInvocation(ctx context.Context, invocation ContractInvocationV3) error {
	jsonBytes, err := json.Marshal(invocation)
	if err != nil {
		return fmt.Errorf("error marshaling invocation: %w", err)
	}

	// Create message with processor metadata for DuckLake schema detection
	msg := Message{
		Payload: jsonBytes,
		Metadata: map[string]interface{}{
			"processor_type":  string(ProcessorTypeContractInvocation),
			"processor_name":  "ContractInvocationProcessorV3",
			"version":         invocation.ProcessorVersion,
			"timestamp":       invocation.Timestamp,
			"ledger_sequence": invocation.LedgerSequence,
		},
	}

	// Preserve archive source metadata if present
	if invocation.ArchiveMetadata != nil {
		msg.Metadata["archive_source"] = invocation.ArchiveMetadata
	}

	for _, processor := range p.processors {
		if err := processor.Process(ctx, msg); err != nil {
			return fmt.Errorf("error in processor chain: %w", err)
		}
	}

	return nil
}

// extractCrossContractCallsFromDiagnosticEvents extracts cross-contract calls from diagnostic events
// This is adapted from V1's implementation to capture calls that may not be in the auth tree
func (p *ContractInvocationProcessorV3) extractCrossContractCallsFromDiagnosticEvents(
	tx ingest.LedgerTransaction,
	opIndex int,
	mainContractID string,
) []ContractCallEnhanced {
	var calls []ContractCallEnhanced
	
	// Get diagnostic events
	diagnosticEvents, err := tx.GetDiagnosticEvents()
	if err != nil || len(diagnosticEvents) == 0 {
		return calls
	}
	
	log.Printf("V3: Checking %d diagnostic events for cross-contract calls", len(diagnosticEvents))
	
	executionOrder := 0
	currentDepth := 0
	
	// Track call stack for determining "from" contract
	callStack := []string{mainContractID}
	
	for i, diagEvent := range diagnosticEvents {
		// Skip events with nil ContractId
		if diagEvent.Event.ContractId == nil {
			log.Printf("V3: Diagnostic event %d has nil ContractId, skipping", i)
			continue
		}
		
		// Get the contract that emitted this event
		eventContractID, err := strkey.Encode(strkey.VersionByteContract, diagEvent.Event.ContractId[:])
		if err != nil {
			log.Printf("V3: Error encoding contract ID from diagnostic event %d: %v", i, err)
			continue
		}
		
		// Get first topic if available
		firstTopic := ""
		if diagEvent.Event.Body.V == 0 && diagEvent.Event.Body.V0 != nil && len(diagEvent.Event.Body.V0.Topics) > 0 {
			if decoded, err := ConvertScValToJSON(diagEvent.Event.Body.V0.Topics[0]); err == nil {
				firstTopic = fmt.Sprintf("%v", decoded)
			}
		}
		
		// Pattern 1: fn_call from main contract followed by event from different contract
		if firstTopic == "fn_call" && eventContractID == mainContractID && i+1 < len(diagnosticEvents) {
			nextEvent := diagnosticEvents[i+1]
			if nextEvent.Event.ContractId != nil {
				nextContractID, err := strkey.Encode(strkey.VersionByteContract, nextEvent.Event.ContractId[:])
				if err == nil && nextContractID != eventContractID {
					// This fn_call is calling another contract
					functionName := "unknown"
					var arguments []interface{}
					
					// Extract function name and arguments from fn_call data
					if diagEvent.Event.Body.V == 0 && diagEvent.Event.Body.V0 != nil {
						if decoded, err := ConvertScValToJSON(diagEvent.Event.Body.V0.Data); err == nil {
							switch data := decoded.(type) {
							case []interface{}:
								// Pattern matching based on V1 logic
								if len(data) == 2 {
									if source, ok := data[0].(string); ok && source == "Stellar" {
										functionName = "lastprice"
										arguments = data
									}
								} else if len(data) == 0 {
									functionName = "decimals"
									arguments = []interface{}{}
								} else if len(data) >= 3 {
									functionName = "transfer"
									arguments = data
								}
							case nil:
								functionName = "decimals"
								arguments = []interface{}{}
							default:
								arguments = []interface{}{decoded}
							}
						}
					}
					
					call := ContractCallEnhanced{
						FromContract:   eventContractID,
						ToContract:     nextContractID,
						Function:       functionName,
						Arguments:      arguments,
						CallDepth:      currentDepth,
						AuthType:       "inferred",
						Successful:     diagEvent.InSuccessfulContractCall,
						ExecutionOrder: executionOrder,
					}
					
					calls = append(calls, call)
					executionOrder++
					
					log.Printf("V3: Found cross-contract call from fn_call: %s -> %s (%s)", 
						eventContractID, nextContractID, functionName)
				}
			}
		}
		
		// Pattern 2: Update call stack based on fn_call/fn_return
		if firstTopic == "fn_call" {
			// Push to call stack
			if eventContractID != "" && (len(callStack) == 0 || callStack[len(callStack)-1] != eventContractID) {
				callStack = append(callStack, eventContractID)
				currentDepth++
			}
		} else if firstTopic == "fn_return" && len(callStack) > 1 {
			// Pop from call stack
			callStack = callStack[:len(callStack)-1]
			if currentDepth > 0 {
				currentDepth--
			}
		}
	}
	
	if len(calls) > 0 {
		log.Printf("V3: Extracted %d cross-contract calls from diagnostic events", len(calls))
	}
	
	return calls
}

// GetStats returns current processor statistics
func (p *ContractInvocationProcessorV3) GetStats() ProcessorStatsV3 {
	p.mu.RLock()
	defer p.mu.RUnlock()
	return p.stats
}