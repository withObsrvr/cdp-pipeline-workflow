package processor

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
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

// ============================================================================
// Core Data Structures for Contract Invocation Processor V2
// ============================================================================

// AuthNode represents a node in the authorization tree
type AuthNode struct {
	NodeID         string                 `json:"node_id"`           // Unique ID for this auth node
	ParentID       string                 `json:"parent_id,omitempty"`
	AuthorizerType string                 `json:"authorizer_type"`   // "account", "contract", "stellar_asset"
	AuthorizerID   string                 `json:"authorizer_id"`     // Account/Contract address
	Credentials    AuthCredentials        `json:"credentials"`
	Children       []AuthNode             `json:"children,omitempty"`
	Metadata       map[string]interface{} `json:"metadata,omitempty"`
}

// AuthCredentials captures credential details
type AuthCredentials struct {
	Type            string `json:"type"` // "source_account", "contract", "stellar_asset"
	SignatureCount  int    `json:"signature_count,omitempty"`
	SignatureExpiry *int64 `json:"signature_expiry,omitempty"`
	Nonce           *int64 `json:"nonce,omitempty"`
}

// AuthContext represents the complete authorization context for an invocation
type AuthContext struct {
	RootAuthorizer string    `json:"root_authorizer"` // Original tx submitter
	AuthTree       *AuthNode `json:"auth_tree"`       // Full auth tree
	AuthPath       []string  `json:"auth_path"`       // Path from root to this node
	AuthProof      string    `json:"auth_proof"`      // Serialized proof
}

// CallGraphNode represents a node in the contract call graph
type CallGraphNode struct {
	NodeID         string            `json:"node_id"`
	ParentNodeID   string            `json:"parent_node_id,omitempty"`
	ContractID     string            `json:"contract_id"`
	FunctionName   string            `json:"function_name"`
	Arguments      []interface{}     `json:"arguments"`
	ArgumentsRaw   []xdr.ScVal       `json:"arguments_raw"`
	CallDepth      int               `json:"call_depth"`
	ExecutionOrder int               `json:"execution_order"`
	StartTime      int64             `json:"start_time"` // Relative execution time
	EndTime        int64             `json:"end_time"`
	GasUsed        uint64            `json:"gas_used"`
	Success        bool              `json:"success"`
	ErrorCode      string            `json:"error_code,omitempty"`
	AuthContext    *AuthContext      `json:"auth_context"`
	Events         []EnrichedEvent   `json:"events"`        // Events emitted by this call
	StateChanges   []StateChange     `json:"state_changes"` // State changes from this call
	Children       []*CallGraphNode  `json:"children,omitempty"`
}

// EnrichedEvent represents a flattened event with full context
type EnrichedEvent struct {
	// Event Identity
	EventID      string `json:"event_id"`      // Unique event ID
	EventType    string `json:"event_type"`    // "contract", "diagnostic", "system"
	EventSubtype string `json:"event_subtype"` // e.g., "transfer", "mint", "swap"

	// Event Source
	ContractID   string `json:"contract_id"`   // Emitting contract
	FunctionName string `json:"function_name"` // Function that emitted event

	// Event Data
	Topics    []interface{} `json:"topics"`     // Decoded topics
	TopicsRaw []xdr.ScVal   `json:"topics_raw"` // Raw XDR topics
	Data      interface{}   `json:"data"`       // Decoded data
	DataRaw   xdr.ScVal     `json:"data_raw"`   // Raw XDR data

	// Execution Context
	TransactionHash string    `json:"transaction_hash"`
	LedgerSequence  uint32    `json:"ledger_sequence"`
	Timestamp       time.Time `json:"timestamp"`
	EventIndex      int       `json:"event_index"` // Order within transaction

	// Call Graph Context
	CallNodeID string   `json:"call_node_id"` // Which call node emitted this
	CallPath   []string `json:"call_path"`    // Full path: [contract1.fn1, contract2.fn2]
	CallDepth  int      `json:"call_depth"`

	// Authorization Context
	AuthContext *AuthContext `json:"auth_context"` // Full auth context
	DirectAuth  string       `json:"direct_auth"`  // Who directly authorized this

	// Execution Status
	InSuccessfulCall bool `json:"in_successful_call"`

	// Metadata
	ProcessorVersion string                 `json:"processor_version"`
	ArchiveMetadata  *ArchiveSourceMetadata `json:"archive_metadata,omitempty"`
}

// V2Config represents configuration for the V2 processor
type V2Config struct {
	NetworkPassphrase string                 `json:"network_passphrase"`
	OutputMode        string                 `json:"output_mode"`        // "event_per_message" or "transaction_bundle"
	IncludeRawXDR     bool                   `json:"include_raw_xdr"`    // Include raw XDR in output
	IncludeAuthProof  bool                   `json:"include_auth_proof"` // Generate auth proofs
	EventFilters      *EventFilters          `json:"event_filters,omitempty"`
	MaxCallDepth      int                    `json:"max_call_depth"` // Safety limit, default 10
}

// EventFilters for optional filtering
type EventFilters struct {
	ContractIDs []string `json:"contract_ids,omitempty"`
	EventTypes  []string `json:"event_types,omitempty"`
	Functions   []string `json:"functions,omitempty"`
}

// ============================================================================
// Main Processor Implementation
// ============================================================================

// ContractInvocationProcessorV2 implements the enhanced processor with full auth tree and event flattening
type ContractInvocationProcessorV2 struct {
	processors        []Processor
	config            V2Config
	mu                sync.RWMutex
	stats             ProcessorStatsV2
}

// ProcessorStatsV2 tracks enhanced statistics
type ProcessorStatsV2 struct {
	ProcessedLedgers   uint32
	ProcessedTxs       uint64
	TotalEvents        uint64
	TotalContractCalls uint64
	MaxCallDepth       int
	MaxAuthDepth       int
	AvgEventsPerTx     float64
	LastLedger         uint32
	LastProcessedTime  time.Time
	ErrorCount         uint64
}

// NewContractInvocationProcessorV2 creates a new V2 processor instance
func NewContractInvocationProcessorV2(config map[string]interface{}) (*ContractInvocationProcessorV2, error) {
	v2Config := V2Config{
		OutputMode:   "event_per_message",
		MaxCallDepth: 10,
	}

	// Parse network passphrase (required)
	networkPassphrase, ok := config["network_passphrase"].(string)
	if !ok {
		return nil, fmt.Errorf("missing network_passphrase in configuration")
	}
	v2Config.NetworkPassphrase = networkPassphrase

	// Parse optional configurations
	if outputMode, ok := config["output_mode"].(string); ok {
		v2Config.OutputMode = outputMode
	}
	if includeRaw, ok := config["include_raw_xdr"].(bool); ok {
		v2Config.IncludeRawXDR = includeRaw
	}
	if includeAuth, ok := config["include_auth_proof"].(bool); ok {
		v2Config.IncludeAuthProof = includeAuth
	}
	if maxDepth, ok := config["max_call_depth"].(int); ok {
		v2Config.MaxCallDepth = maxDepth
	}

	// Parse event filters if provided
	if filters, ok := config["event_filters"].(map[string]interface{}); ok {
		v2Config.EventFilters = &EventFilters{}
		if contractIDs, ok := filters["contract_ids"].([]interface{}); ok {
			for _, id := range contractIDs {
				if idStr, ok := id.(string); ok {
					v2Config.EventFilters.ContractIDs = append(v2Config.EventFilters.ContractIDs, idStr)
				}
			}
		}
		if eventTypes, ok := filters["event_types"].([]interface{}); ok {
			for _, et := range eventTypes {
				if etStr, ok := et.(string); ok {
					v2Config.EventFilters.EventTypes = append(v2Config.EventFilters.EventTypes, etStr)
				}
			}
		}
		if functions, ok := filters["functions"].([]interface{}); ok {
			for _, fn := range functions {
				if fnStr, ok := fn.(string); ok {
					v2Config.EventFilters.Functions = append(v2Config.EventFilters.Functions, fnStr)
				}
			}
		}
	}

	return &ContractInvocationProcessorV2{
		config: v2Config,
	}, nil
}

// Subscribe adds a processor to receive output events
func (p *ContractInvocationProcessorV2) Subscribe(processor Processor) {
	p.processors = append(p.processors, processor)
}

// Process handles incoming ledger data and emits enriched events
func (p *ContractInvocationProcessorV2) Process(ctx context.Context, msg Message) error {
	ledgerCloseMeta, ok := msg.Payload.(xdr.LedgerCloseMeta)
	if !ok {
		return fmt.Errorf("expected xdr.LedgerCloseMeta, got %T", msg.Payload)
	}

	// Extract source metadata from incoming message
	archiveMetadata, _ := msg.GetArchiveMetadata()

	sequence := ledgerCloseMeta.LedgerSequence()
	log.Printf("ContractInvocationV2: Processing ledger %d", sequence)

	txReader, err := ingest.NewLedgerTransactionReaderFromLedgerCloseMeta(p.config.NetworkPassphrase, ledgerCloseMeta)
	if err != nil {
		return fmt.Errorf("error creating transaction reader: %w", err)
	}
	defer txReader.Close()

	// Process each transaction
	txCount := 0
	totalEvents := 0
	for {
		tx, err := txReader.Read()
		if err == io.EOF {
			break
		}
		if err != nil {
			return fmt.Errorf("error reading transaction: %w", err)
		}

		events, err := p.processTransaction(tx, ledgerCloseMeta, archiveMetadata)
		if err != nil {
			log.Printf("Error processing transaction %s: %v", tx.Result.TransactionHash.HexString(), err)
			p.mu.Lock()
			p.stats.ErrorCount++
			p.mu.Unlock()
			continue
		}

		if len(events) > 0 {
			totalEvents += len(events)
			if err := p.forwardEvents(ctx, events, tx.Result.TransactionHash.HexString()); err != nil {
				log.Printf("Error forwarding events: %v", err)
			}
		}
		txCount++
	}

	// Update statistics
	p.mu.Lock()
	p.stats.ProcessedLedgers++
	p.stats.ProcessedTxs += uint64(txCount)
	p.stats.TotalEvents += uint64(totalEvents)
	p.stats.LastLedger = sequence
	p.stats.LastProcessedTime = time.Now()
	if txCount > 0 {
		p.stats.AvgEventsPerTx = float64(p.stats.TotalEvents) / float64(p.stats.ProcessedTxs)
	}
	p.mu.Unlock()

	log.Printf("ContractInvocationV2: Processed ledger %d - %d transactions, %d events emitted", 
		sequence, txCount, totalEvents)

	return nil
}

// processTransaction processes a single transaction and extracts all enriched events
func (p *ContractInvocationProcessorV2) processTransaction(
	tx ingest.LedgerTransaction,
	meta xdr.LedgerCloseMeta,
	archiveMetadata *ArchiveSourceMetadata,
) ([]EnrichedEvent, error) {
	var allEvents []EnrichedEvent

	// Get the transaction submitter
	txSubmitter := tx.Envelope.SourceAccount().ToAccountId().Address()
	timestamp := time.Unix(int64(meta.LedgerHeaderHistoryEntry().Header.ScpValue.CloseTime), 0)

	// Process each operation
	for opIndex, op := range tx.Envelope.Operations() {
		if op.Body.Type != xdr.OperationTypeInvokeHostFunction {
			continue
		}

		invokeOp := op.Body.MustInvokeHostFunctionOp()
		
		// Build the authorization tree
		authTree := p.buildAuthorizationTree(invokeOp.Auth, txSubmitter)
		
		// Build the call graph
		callGraph, err := p.buildCallGraph(tx, opIndex, invokeOp, authTree)
		if err != nil {
			log.Printf("Error building call graph: %v", err)
			continue
		}

		// Update max depth statistics
		p.updateDepthStats(callGraph, authTree)

		// Flatten to enriched events
		events := p.flattenToEnrichedEvents(
			callGraph, 
			authTree, 
			tx.Result.TransactionHash.HexString(),
			meta.LedgerSequence(),
			timestamp,
			archiveMetadata,
		)

		// Apply filters if configured
		filteredEvents := p.applyEventFilters(events)
		allEvents = append(allEvents, filteredEvents...)
	}

	return allEvents, nil
}

// buildAuthorizationTree constructs the complete authorization tree from auth entries
func (p *ContractInvocationProcessorV2) buildAuthorizationTree(
	authEntries []xdr.SorobanAuthorizationEntry,
	txSubmitter string,
) *AuthNode {
	if len(authEntries) == 0 {
		// No explicit auth entries means tx submitter authorized everything
		return &AuthNode{
			NodeID:         generateNodeID("auth", txSubmitter, 0),
			AuthorizerType: "account",
			AuthorizerID:   txSubmitter,
			Credentials: AuthCredentials{
				Type: "source_account",
			},
		}
	}

	// Create root node for the transaction submitter
	root := &AuthNode{
		NodeID:         generateNodeID("auth", txSubmitter, 0),
		AuthorizerType: "account", 
		AuthorizerID:   txSubmitter,
		Credentials: AuthCredentials{
			Type: "source_account",
		},
		Children: make([]AuthNode, 0, len(authEntries)),
	}

	// Process each authorization entry
	for i, authEntry := range authEntries {
		authNode := p.processAuthEntry(&authEntry, root.NodeID, i)
		if authNode != nil {
			root.Children = append(root.Children, *authNode)
		}
	}

	return root
}

// processAuthEntry processes a single authorization entry and its sub-invocations
func (p *ContractInvocationProcessorV2) processAuthEntry(
	entry *xdr.SorobanAuthorizationEntry,
	parentID string,
	index int,
) *AuthNode {
	// Determine authorizer type and ID from credentials
	authType, authID := p.extractAuthorizerInfo(entry.Credentials)
	
	node := &AuthNode{
		NodeID:         generateNodeID("auth", authID, index),
		ParentID:       parentID,
		AuthorizerType: authType,
		AuthorizerID:   authID,
		Credentials:    p.extractCredentials(entry.Credentials),
		Children:       make([]AuthNode, 0),
	}

	// Process the root invocation recursively
	p.processAuthInvocation(&entry.RootInvocation, node)

	return node
}

// extractAuthorizerInfo extracts authorizer type and ID from credentials
func (p *ContractInvocationProcessorV2) extractAuthorizerInfo(creds xdr.SorobanCredentials) (string, string) {
	switch creds.Type {
	case xdr.SorobanCredentialsTypeSorobanCredentialsSourceAccount:
		return "account", "source_account" // Will be replaced with actual account later
	case xdr.SorobanCredentialsTypeSorobanCredentialsAddress:
		addr := creds.Address
		if addr.Address.Type == xdr.ScAddressTypeScAddressTypeAccount {
			accountID := addr.Address.AccountId
			address, _ := strkey.Encode(strkey.VersionByteAccountID, accountID.Ed25519[:])
			return "account", address
		} else if addr.Address.Type == xdr.ScAddressTypeScAddressTypeContract {
			contractID := addr.Address.ContractId
			address, _ := strkey.Encode(strkey.VersionByteContract, contractID[:])
			return "contract", address
		}
	}
	return "unknown", "unknown"
}

// extractCredentials converts XDR credentials to our credential structure
func (p *ContractInvocationProcessorV2) extractCredentials(creds xdr.SorobanCredentials) AuthCredentials {
	ac := AuthCredentials{}
	
	switch creds.Type {
	case xdr.SorobanCredentialsTypeSorobanCredentialsSourceAccount:
		ac.Type = "source_account"
	case xdr.SorobanCredentialsTypeSorobanCredentialsAddress:
		ac.Type = "address"
		addr := creds.Address
		// Nonce is always present in SorobanAddressCredentials
		nonce := int64(addr.Nonce)
		ac.Nonce = &nonce
		
		// SignatureExpirationLedger is always present
		expiry := int64(addr.SignatureExpirationLedger)
		ac.SignatureExpiry = &expiry
		// Note: In a real implementation, we'd also capture signature details
	}
	
	return ac
}

// processAuthInvocation recursively processes authorization invocations
func (p *ContractInvocationProcessorV2) processAuthInvocation(
	invocation *xdr.SorobanAuthorizedInvocation,
	parentNode *AuthNode,
) {
	if invocation == nil {
		return
	}

	// For contract function invocations, we might create child auth nodes
	// based on sub-invocations
	for i, subInvocation := range invocation.SubInvocations {
		// Extract contract/function info from the sub-invocation
		if subInvocation.Function.Type == xdr.SorobanAuthorizedFunctionTypeSorobanAuthorizedFunctionTypeContractFn {
			contractFn := subInvocation.Function.ContractFn
			contractIDBytes := contractFn.ContractAddress.ContractId
			contractID, _ := strkey.Encode(strkey.VersionByteContract, contractIDBytes[:])
			
			// Create a child node for this sub-invocation
			childNode := &AuthNode{
				NodeID:         generateNodeID("auth", contractID, i),
				ParentID:       parentNode.NodeID,
				AuthorizerType: "contract",
				AuthorizerID:   contractID,
				Credentials: AuthCredentials{
					Type: "contract",
				},
				Children: make([]AuthNode, 0),
				Metadata: map[string]interface{}{
					"function": string(contractFn.FunctionName),
				},
			}
			
			// Process its sub-invocations recursively
			p.processAuthInvocation(&subInvocation, childNode)
			
			parentNode.Children = append(parentNode.Children, *childNode)
		}
	}
}

// buildCallGraph constructs the complete call graph from various sources
func (p *ContractInvocationProcessorV2) buildCallGraph(
	tx ingest.LedgerTransaction,
	opIndex int,
	invokeOp xdr.InvokeHostFunctionOp,
	authTree *AuthNode,
) (*CallGraphNode, error) {
	// Extract the main contract being invoked
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

	// Create root node for the call graph
	root := &CallGraphNode{
		NodeID:         generateNodeID("call", mainContractID, 0),
		ContractID:     mainContractID,
		FunctionName:   functionName,
		Arguments:      args,
		ArgumentsRaw:   argsRaw,
		CallDepth:      0,
		ExecutionOrder: 0,
		Success:        p.isOperationSuccessful(tx, opIndex),
		AuthContext:    p.createAuthContext(authTree, []string{mainContractID}),
		Children:       make([]*CallGraphNode, 0),
	}

	// Build call graph from multiple sources:
	// 1. Authorization sub-invocations
	p.buildFromAuthInvocations(invokeOp.Auth, root, authTree)
	
	// 2. Transaction events (both persistent and diagnostic)
	p.enrichWithTransactionEvents(tx, root)
	
	// 3. State changes
	p.enrichWithStateChanges(tx, root)

	return root, nil
}

// buildFromAuthInvocations builds call graph nodes from authorization data
func (p *ContractInvocationProcessorV2) buildFromAuthInvocations(
	authEntries []xdr.SorobanAuthorizationEntry,
	rootNode *CallGraphNode,
	authTree *AuthNode,
) {
	executionOrder := 1
	
	for _, authEntry := range authEntries {
		p.processAuthInvocationForCallGraph(
			&authEntry.RootInvocation,
			rootNode,
			rootNode.ContractID,
			1,
			&executionOrder,
			authTree,
		)
	}
}

// processAuthInvocationForCallGraph processes auth invocations to build call graph
func (p *ContractInvocationProcessorV2) processAuthInvocationForCallGraph(
	invocation *xdr.SorobanAuthorizedInvocation,
	parentNode *CallGraphNode,
	fromContract string,
	depth int,
	executionOrder *int,
	authTree *AuthNode,
) {
	if invocation == nil || depth > p.config.MaxCallDepth {
		return
	}

	if invocation.Function.Type == xdr.SorobanAuthorizedFunctionTypeSorobanAuthorizedFunctionTypeContractFn {
		contractFn := invocation.Function.ContractFn
		
		// Get contract ID
		contractIDBytes := contractFn.ContractAddress.ContractId
		contractID, _ := strkey.Encode(strkey.VersionByteContract, contractIDBytes[:])
		
		// Get function name
		functionName := string(contractFn.FunctionName)
		
		// Get arguments
		var args []interface{}
		var argsRaw []xdr.ScVal
		argsRaw = contractFn.Args
		for _, arg := range argsRaw {
			decoded, _ := ConvertScValToJSON(arg)
			args = append(args, decoded)
		}
		
		// Create call node
		callNode := &CallGraphNode{
			NodeID:         generateNodeID("call", contractID, *executionOrder),
			ParentNodeID:   parentNode.NodeID,
			ContractID:     contractID,
			FunctionName:   functionName,
			Arguments:      args,
			ArgumentsRaw:   argsRaw,
			CallDepth:      depth,
			ExecutionOrder: *executionOrder,
			Success:        true, // Will be updated from events
			AuthContext:    p.createAuthContext(authTree, append(parentNode.AuthContext.AuthPath, contractID)),
			Children:       make([]*CallGraphNode, 0),
		}
		
		*executionOrder++
		
		// Add as child to parent
		parentNode.Children = append(parentNode.Children, callNode)
		
		// Process sub-invocations
		for _, subInvocation := range invocation.SubInvocations {
			p.processAuthInvocationForCallGraph(
				&subInvocation,
				callNode,
				contractID,
				depth+1,
				executionOrder,
				authTree,
			)
		}
	}
}

// enrichWithTransactionEvents adds event data to the call graph
func (p *ContractInvocationProcessorV2) enrichWithTransactionEvents(
	tx ingest.LedgerTransaction,
	rootNode *CallGraphNode,
) {
	// Get persistent events
	persistentEvents, err := tx.GetTransactionEvents()
	if err == nil {
		// Handle operation-level events
		for _, opEvents := range persistentEvents.OperationEvents {
			for _, event := range opEvents {
				if event.Type == xdr.ContractEventTypeContract && event.ContractId != nil {
					contractID, _ := strkey.Encode(strkey.VersionByteContract, event.ContractId[:])
					node := p.findNodeByContract(rootNode, contractID)
					if node != nil {
						node.Events = append(node.Events, p.convertToEnrichedEvent(event, "contract", node))
					}
				}
			}
		}
		
		// Handle transaction-level events
		for _, txEvent := range persistentEvents.TransactionEvents {
			if txEvent.Event.Type == xdr.ContractEventTypeContract && txEvent.Event.ContractId != nil {
				contractID, _ := strkey.Encode(strkey.VersionByteContract, txEvent.Event.ContractId[:])
				node := p.findNodeByContract(rootNode, contractID)
				if node != nil {
					node.Events = append(node.Events, p.convertToEnrichedEvent(txEvent.Event, "contract", node))
				}
			}
		}
	}
	
	// Get diagnostic events
	diagnosticEvents, _ := tx.GetDiagnosticEvents()
	for _, diagEvent := range diagnosticEvents {
		// Skip events with nil ContractId
		if diagEvent.Event.ContractId == nil {
			continue
		}
		contractID, _ := strkey.Encode(strkey.VersionByteContract, diagEvent.Event.ContractId[:])
		node := p.findNodeByContract(rootNode, contractID)
		if node != nil {
			node.Events = append(node.Events, p.convertDiagnosticToEnrichedEvent(diagEvent, node))
		}
	}
}

// enrichWithStateChanges adds state change data to the call graph
func (p *ContractInvocationProcessorV2) enrichWithStateChanges(
	tx ingest.LedgerTransaction,
	rootNode *CallGraphNode,
) {
	changes, _ := tx.GetChanges()
	for _, change := range changes {
		if change.Type == xdr.LedgerEntryTypeContractData {
			// Extract state change and associate with appropriate call node
			// This is simplified - in reality we'd correlate based on operation index
			if change.Post != nil && change.Post.Data.Type == xdr.LedgerEntryTypeContractData {
				contractData := change.Post.Data.ContractData
				if contractData != nil {
					contractIDBytes := contractData.Contract.ContractId
					contractID, _ := strkey.Encode(strkey.VersionByteContract, contractIDBytes[:])
					
					node := p.findNodeByContract(rootNode, contractID)
					if node != nil {
						// Add state change to node
						// (Using existing StateChange structure from V1)
					}
				}
			}
		}
	}
}

// flattenToEnrichedEvents converts the call graph into a flat list of enriched events
func (p *ContractInvocationProcessorV2) flattenToEnrichedEvents(
	callGraph *CallGraphNode,
	authTree *AuthNode,
	txHash string,
	ledgerSeq uint32,
	timestamp time.Time,
	archiveMetadata *ArchiveSourceMetadata,
) []EnrichedEvent {
	var events []EnrichedEvent
	eventIndex := 0
	
	// Depth-first traversal of the call graph
	p.traverseCallGraph(
		callGraph, 
		[]string{}, 
		&events, 
		&eventIndex,
		txHash,
		ledgerSeq,
		timestamp,
		archiveMetadata,
	)
	
	return events
}

// traverseCallGraph recursively traverses the call graph and collects events
func (p *ContractInvocationProcessorV2) traverseCallGraph(
	node *CallGraphNode,
	callPath []string,
	events *[]EnrichedEvent,
	eventIndex *int,
	txHash string,
	ledgerSeq uint32,
	timestamp time.Time,
	archiveMetadata *ArchiveSourceMetadata,
) {
	// Build current call path
	currentPath := append(callPath, fmt.Sprintf("%s.%s", node.ContractID, node.FunctionName))
	
	// Process all events from this node
	for _, event := range node.Events {
		// Update event with transaction context
		event.TransactionHash = txHash
		event.LedgerSequence = ledgerSeq
		event.Timestamp = timestamp
		event.EventIndex = *eventIndex
		event.CallPath = currentPath
		event.CallDepth = node.CallDepth
		event.ProcessorVersion = "v2.0.0"
		event.ArchiveMetadata = archiveMetadata
		
		*events = append(*events, event)
		*eventIndex++
	}
	
	// Process children
	for _, child := range node.Children {
		p.traverseCallGraph(
			child,
			currentPath,
			events,
			eventIndex,
			txHash,
			ledgerSeq,
			timestamp,
			archiveMetadata,
		)
	}
}

// Helper functions

// isOperationSuccessful checks if an operation was successful
func (p *ContractInvocationProcessorV2) isOperationSuccessful(tx ingest.LedgerTransaction, opIndex int) bool {
	if tx.Result.Result.Result.Results != nil {
		if results := *tx.Result.Result.Result.Results; len(results) > opIndex {
			if result := results[opIndex]; result.Tr != nil {
				if invokeResult, ok := result.Tr.GetInvokeHostFunctionResult(); ok {
					return invokeResult.Code == xdr.InvokeHostFunctionResultCodeInvokeHostFunctionSuccess
				}
			}
		}
	}
	return false
}

// createAuthContext creates an auth context for a given path
func (p *ContractInvocationProcessorV2) createAuthContext(authTree *AuthNode, authPath []string) *AuthContext {
	ctx := &AuthContext{
		RootAuthorizer: authTree.AuthorizerID,
		AuthTree:       authTree,
		AuthPath:       authPath,
	}
	
	// Generate auth proof if configured
	if p.config.IncludeAuthProof {
		ctx.AuthProof = p.generateAuthProof(authTree, authPath)
	}
	
	return ctx
}

// generateAuthProof creates a cryptographic proof of the authorization path
func (p *ContractInvocationProcessorV2) generateAuthProof(authTree *AuthNode, authPath []string) string {
	// Simplified proof generation - in production this would be more sophisticated
	h := sha256.New()
	h.Write([]byte(authTree.AuthorizerID))
	for _, path := range authPath {
		h.Write([]byte(path))
	}
	return hex.EncodeToString(h.Sum(nil))
}

// findNodeByContract finds a node in the call graph by contract ID
func (p *ContractInvocationProcessorV2) findNodeByContract(root *CallGraphNode, contractID string) *CallGraphNode {
	if root.ContractID == contractID {
		return root
	}
	for _, child := range root.Children {
		if found := p.findNodeByContract(child, contractID); found != nil {
			return found
		}
	}
	return nil
}

// getDirectAuth safely gets the direct authorizer from a call node
func (p *ContractInvocationProcessorV2) getDirectAuth(callNode *CallGraphNode) string {
	if callNode == nil || callNode.AuthContext == nil {
		return ""
	}
	if len(callNode.AuthContext.AuthPath) == 0 {
		return callNode.AuthContext.RootAuthorizer
	}
	return callNode.AuthContext.AuthPath[len(callNode.AuthContext.AuthPath)-1]
}

// convertToEnrichedEvent converts a persistent event to enriched event
func (p *ContractInvocationProcessorV2) convertToEnrichedEvent(
	event xdr.ContractEvent,
	eventType string,
	callNode *CallGraphNode,
) EnrichedEvent {
	// Decode topics
	var topics []interface{}
	var topicsRaw []xdr.ScVal
	if event.Body.V == 0 && event.Body.V0 != nil {
		topicsRaw = event.Body.V0.Topics
		for _, topic := range topicsRaw {
			decoded, _ := ConvertScValToJSON(topic)
			topics = append(topics, decoded)
		}
	}
	
	// Decode data
	var data interface{}
	var dataRaw xdr.ScVal
	if event.Body.V == 0 && event.Body.V0 != nil {
		dataRaw = event.Body.V0.Data
		data, _ = ConvertScValToJSON(dataRaw)
	}
	
	// Determine event subtype from first topic
	eventSubtype := "unknown"
	if len(topics) > 0 {
		if subtype, ok := topics[0].(string); ok {
			eventSubtype = subtype
		}
	}
	
	var contractID string
	if event.ContractId != nil {
		contractID, _ = strkey.Encode(strkey.VersionByteContract, event.ContractId[:])
	}
	
	return EnrichedEvent{
		EventID:          generateEventID(callNode.NodeID, eventSubtype),
		EventType:        eventType,
		EventSubtype:     eventSubtype,
		ContractID:       contractID,
		FunctionName:     callNode.FunctionName,
		Topics:           topics,
		TopicsRaw:        topicsRaw,
		Data:             data,
		DataRaw:          dataRaw,
		CallNodeID:       callNode.NodeID,
		AuthContext:      callNode.AuthContext,
		DirectAuth:       p.getDirectAuth(callNode),
		InSuccessfulCall: callNode.Success,
	}
}

// convertDiagnosticToEnrichedEvent converts a diagnostic event to enriched event
func (p *ContractInvocationProcessorV2) convertDiagnosticToEnrichedEvent(
	diagEvent xdr.DiagnosticEvent,
	callNode *CallGraphNode,
) EnrichedEvent {
	enriched := p.convertToEnrichedEvent(diagEvent.Event, "diagnostic", callNode)
	enriched.InSuccessfulCall = diagEvent.InSuccessfulContractCall
	return enriched
}

// applyEventFilters applies configured filters to events
func (p *ContractInvocationProcessorV2) applyEventFilters(events []EnrichedEvent) []EnrichedEvent {
	if p.config.EventFilters == nil {
		return events
	}
	
	var filtered []EnrichedEvent
	for _, event := range events {
		if p.passesFilters(event) {
			filtered = append(filtered, event)
		}
	}
	return filtered
}

// passesFilters checks if an event passes configured filters
func (p *ContractInvocationProcessorV2) passesFilters(event EnrichedEvent) bool {
	filters := p.config.EventFilters
	
	// Check contract ID filter
	if len(filters.ContractIDs) > 0 {
		found := false
		for _, id := range filters.ContractIDs {
			if event.ContractID == id {
				found = true
				break
			}
		}
		if !found {
			return false
		}
	}
	
	// Check event type filter
	if len(filters.EventTypes) > 0 {
		found := false
		for _, et := range filters.EventTypes {
			if event.EventSubtype == et {
				found = true
				break
			}
		}
		if !found {
			return false
		}
	}
	
	// Check function filter
	if len(filters.Functions) > 0 {
		found := false
		for _, fn := range filters.Functions {
			if event.FunctionName == fn {
				found = true
				break
			}
		}
		if !found {
			return false
		}
	}
	
	return true
}

// updateDepthStats updates maximum depth statistics
func (p *ContractInvocationProcessorV2) updateDepthStats(callGraph *CallGraphNode, authTree *AuthNode) {
	maxCallDepth := p.getMaxCallDepth(callGraph, 0)
	maxAuthDepth := p.getMaxAuthDepth(authTree, 0)
	
	p.mu.Lock()
	if maxCallDepth > p.stats.MaxCallDepth {
		p.stats.MaxCallDepth = maxCallDepth
	}
	if maxAuthDepth > p.stats.MaxAuthDepth {
		p.stats.MaxAuthDepth = maxAuthDepth
	}
	p.stats.TotalContractCalls += uint64(p.countCalls(callGraph))
	p.mu.Unlock()
}

// getMaxCallDepth recursively finds maximum call depth
func (p *ContractInvocationProcessorV2) getMaxCallDepth(node *CallGraphNode, currentMax int) int {
	if node.CallDepth > currentMax {
		currentMax = node.CallDepth
	}
	for _, child := range node.Children {
		currentMax = p.getMaxCallDepth(child, currentMax)
	}
	return currentMax
}

// getMaxAuthDepth recursively finds maximum auth depth
func (p *ContractInvocationProcessorV2) getMaxAuthDepth(node *AuthNode, depth int) int {
	maxDepth := depth
	for _, child := range node.Children {
		childDepth := p.getMaxAuthDepth(&child, depth+1)
		if childDepth > maxDepth {
			maxDepth = childDepth
		}
	}
	return maxDepth
}

// countCalls counts total number of calls in the graph
func (p *ContractInvocationProcessorV2) countCalls(node *CallGraphNode) int {
	count := 1
	for _, child := range node.Children {
		count += p.countCalls(child)
	}
	return count
}

// forwardEvents sends events to subscribed processors
func (p *ContractInvocationProcessorV2) forwardEvents(ctx context.Context, events []EnrichedEvent, txHash string) error {
	if len(p.processors) == 0 {
		return nil
	}

	switch p.config.OutputMode {
	case "event_per_message":
		// Send each event as a separate message
		for _, event := range events {
			jsonBytes, err := json.Marshal(event)
			if err != nil {
				return fmt.Errorf("error marshaling event: %w", err)
			}
			
			for _, processor := range p.processors {
				if err := processor.Process(ctx, Message{Payload: jsonBytes}); err != nil {
					return fmt.Errorf("error in processor chain: %w", err)
				}
			}
		}
		
	case "transaction_bundle":
		// Send all events from the transaction as a single message
		bundle := map[string]interface{}{
			"transaction_hash": txHash,
			"event_count":      len(events),
			"events":           events,
		}
		
		jsonBytes, err := json.Marshal(bundle)
		if err != nil {
			return fmt.Errorf("error marshaling event bundle: %w", err)
		}
		
		for _, processor := range p.processors {
			if err := processor.Process(ctx, Message{Payload: jsonBytes}); err != nil {
				return fmt.Errorf("error in processor chain: %w", err)
			}
		}
	}

	return nil
}

// GetStats returns current processor statistics
func (p *ContractInvocationProcessorV2) GetStats() ProcessorStatsV2 {
	p.mu.RLock()
	defer p.mu.RUnlock()
	return p.stats
}

// Utility functions

// generateNodeID creates a unique node identifier
func generateNodeID(prefix, identifier string, index int) string {
	h := sha256.New()
	h.Write([]byte(fmt.Sprintf("%s-%s-%d-%d", prefix, identifier, index, time.Now().UnixNano())))
	return fmt.Sprintf("%s_%s", prefix, hex.EncodeToString(h.Sum(nil))[:8])
}

// generateEventID creates a unique event identifier
func generateEventID(callNodeID, eventType string) string {
	h := sha256.New()
	h.Write([]byte(fmt.Sprintf("%s-%s-%d", callNodeID, eventType, time.Now().UnixNano())))
	return fmt.Sprintf("evt_%s", hex.EncodeToString(h.Sum(nil))[:12])
}