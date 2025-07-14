package processor

import (
	"encoding/json"
	"testing"
	"time"

	"github.com/stellar/go/xdr"
)

func TestDualRepresentationStructures(t *testing.T) {
	// Test that our dual representation structures compile and can be instantiated

	// Test DiagnosticEvent with dual representation
	diagnosticEvent := DiagnosticEvent{
		ContractID:    "CCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCC",
		Topics:        []xdr.ScVal{},
		TopicsDecoded: []interface{}{},
		Data:          xdr.ScVal{},
		DataDecoded:   nil,
	}

	if diagnosticEvent.ContractID == "" {
		t.Error("DiagnosticEvent ContractID should not be empty")
	}

	// Test StateChange with dual representation
	stateChange := StateChange{
		ContractID:  "CCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCC",
		KeyRaw:      xdr.ScVal{},
		Key:         "test_key",
		OldValueRaw: xdr.ScVal{},
		OldValue:    nil,
		NewValueRaw: xdr.ScVal{},
		NewValue:    "test_value",
		Operation:   "update",
	}

	if stateChange.Operation != "update" {
		t.Error("StateChange Operation should be 'update'")
	}

	// Test ContractInvocation with ArgumentsRaw
	invocation := ContractInvocation{
		Timestamp:        time.Now(),
		LedgerSequence:   12345,
		TransactionHash:  "abcd1234",
		ContractID:       "CCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCC",
		InvokingAccount:  "GAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA",
		FunctionName:     "test_function",
		ArgumentsRaw:     []xdr.ScVal{},
		Arguments:        []json.RawMessage{},
		ArgumentsDecoded: map[string]interface{}{},
		Successful:       true,
		DiagnosticEvents: []DiagnosticEvent{diagnosticEvent},
		ContractCalls:    []ContractCall{},
		StateChanges:     []StateChange{stateChange},
		TtlExtensions:    []TtlExtension{},
	}

	if invocation.FunctionName != "test_function" {
		t.Error("ContractInvocation FunctionName should be 'test_function'")
	}

	if len(invocation.DiagnosticEvents) != 1 {
		t.Error("ContractInvocation should have 1 diagnostic event")
	}

	if len(invocation.StateChanges) != 1 {
		t.Error("ContractInvocation should have 1 state change")
	}
}

func TestExtractArgumentsSignature(t *testing.T) {
	// Test that extractArguments function has the correct signature
	args := []xdr.ScVal{}

	argumentsRaw, jsonRawArgs, decodedArgs, err := extractArguments(args)

	if err != nil {
		t.Errorf("extractArguments should not return error for empty args: %v", err)
	}

	if argumentsRaw == nil {
		t.Error("argumentsRaw should not be nil")
	}

	if jsonRawArgs == nil {
		t.Error("jsonRawArgs should not be nil")
	}

	if decodedArgs == nil {
		t.Error("decodedArgs should not be nil")
	}

	// Should all be empty for empty input
	if len(argumentsRaw) != 0 {
		t.Error("argumentsRaw should be empty for empty input")
	}

	if len(jsonRawArgs) != 0 {
		t.Error("jsonRawArgs should be empty for empty input")
	}

	if len(decodedArgs) != 0 {
		t.Error("decodedArgs should be empty for empty input")
	}
}

func TestExtractArgumentsWithData(t *testing.T) {
	// Test extractArguments with actual ScVal data
	args := []xdr.ScVal{
		{
			Type: xdr.ScValTypeScvSymbol,
			Sym:  &[]xdr.ScSymbol{xdr.ScSymbol("test_function")}[0],
		},
		{
			Type: xdr.ScValTypeScvU32,
			U32:  func() *xdr.Uint32 { v := xdr.Uint32(123); return &v }(),
		},
	}

	argumentsRaw, jsonRawArgs, decodedArgs, err := extractArguments(args)

	if err != nil {
		t.Errorf("extractArguments should not return error: %v", err)
	}

	// Check that we got the right number of arguments
	if len(argumentsRaw) != 2 {
		t.Errorf("Expected 2 raw arguments, got %d", len(argumentsRaw))
	}

	if len(jsonRawArgs) != 2 {
		t.Errorf("Expected 2 JSON raw arguments, got %d", len(jsonRawArgs))
	}

	if len(decodedArgs) != 2 {
		t.Errorf("Expected 2 decoded arguments, got %d", len(decodedArgs))
	}

	// Check that original slice is not modified (copy protection)
	if &argumentsRaw[0] == &args[0] {
		t.Error("extractArguments should return a copy, not the original slice")
	}

	// Check that decoded arguments have correct keys
	if _, exists := decodedArgs["arg_0"]; !exists {
		t.Error("decodedArgs should contain 'arg_0'")
	}

	if _, exists := decodedArgs["arg_1"]; !exists {
		t.Error("decodedArgs should contain 'arg_1'")
	}
}

func TestExtractDiagnosticEvents(t *testing.T) {
	// Since creating a full mock LedgerTransaction is complex, we'll focus on testing
	// the dual representation aspect more directly by testing the helper functions
	// that are used within extractDiagnosticEvents

	// Test ConvertScValToJSON which is the core of the dual representation
	topicScVal := xdr.ScVal{
		Type: xdr.ScValTypeScvSymbol,
		Sym:  &[]xdr.ScSymbol{xdr.ScSymbol("transfer")}[0],
	}

	decoded, err := ConvertScValToJSON(topicScVal)
	if err != nil {
		t.Errorf("ConvertScValToJSON should not return error: %v", err)
	}

	if decoded != "transfer" {
		t.Errorf("Expected decoded value 'transfer', got %v", decoded)
	}

	// Test with numeric data
	dataScVal := xdr.ScVal{
		Type: xdr.ScValTypeScvU32,
		U32:  func() *xdr.Uint32 { v := xdr.Uint32(1000); return &v }(),
	}

	decoded, err = ConvertScValToJSON(dataScVal)
	if err != nil {
		t.Errorf("ConvertScValToJSON should not return error: %v", err)
	}

	if decodedUint32, ok := decoded.(xdr.Uint32); !ok || decodedUint32 != xdr.Uint32(1000) {
		t.Errorf("Expected decoded value 1000 (xdr.Uint32), got %v (type %T)", decoded, decoded)
	}
}

func TestExtractStateChangeFromContractData(t *testing.T) {
	// Create a mock ContractInvocationProcessor
	processor := &ContractInvocationProcessor{
		networkPassphrase: "Test SDF Network ; September 2015",
	}

	// Create mock contract data
	contractIDBytes := [32]byte{}
	copy(contractIDBytes[:], "test_contract_id")

	contractData := xdr.ContractDataEntry{
		Contract: xdr.ScAddress{
			Type:       xdr.ScAddressTypeScAddressTypeContract,
			ContractId: func() *xdr.Hash { h := xdr.Hash(contractIDBytes); return &h }(),
		},
		Key: xdr.ScVal{
			Type: xdr.ScValTypeScvSymbol,
			Sym:  &[]xdr.ScSymbol{xdr.ScSymbol("balance")}[0],
		},
		Val: xdr.ScVal{
			Type: xdr.ScValTypeScvU64,
			U64:  func() *xdr.Uint64 { v := xdr.Uint64(1000); return &v }(),
		},
	}

	oldValue := xdr.ScVal{
		Type: xdr.ScValTypeScvU64,
		U64:  func() *xdr.Uint64 { v := xdr.Uint64(500); return &v }(),
	}

	newValue := xdr.ScVal{
		Type: xdr.ScValTypeScvU64,
		U64:  func() *xdr.Uint64 { v := xdr.Uint64(1500); return &v }(),
	}

	// Test create operation
	stateChange := processor.extractStateChangeFromContractData(contractData, xdr.ScVal{}, newValue, "create")
	if stateChange == nil {
		t.Fatal("extractStateChangeFromContractData should not return nil for valid data")
	}

	if stateChange.Operation != "create" {
		t.Errorf("Expected operation 'create', got '%s'", stateChange.Operation)
	}

	if stateChange.Key != "balance" {
		t.Errorf("Expected key 'balance', got '%s'", stateChange.Key)
	}

	if newValueUint64, ok := stateChange.NewValue.(xdr.Uint64); !ok || newValueUint64 != xdr.Uint64(1500) {
		t.Errorf("Expected new value 1500 (xdr.Uint64), got %v (type %T)", stateChange.NewValue, stateChange.NewValue)
	}

	if stateChange.OldValue != nil {
		t.Errorf("Expected old value to be nil for create operation, got %v", stateChange.OldValue)
	}

	// Test update operation
	stateChange = processor.extractStateChangeFromContractData(contractData, oldValue, newValue, "update")
	if stateChange == nil {
		t.Fatal("extractStateChangeFromContractData should not return nil for valid data")
	}

	if stateChange.Operation != "update" {
		t.Errorf("Expected operation 'update', got '%s'", stateChange.Operation)
	}

	if oldValueUint64, ok := stateChange.OldValue.(xdr.Uint64); !ok || oldValueUint64 != xdr.Uint64(500) {
		t.Errorf("Expected old value 500 (xdr.Uint64), got %v (type %T)", stateChange.OldValue, stateChange.OldValue)
	}

	if newValueUint64, ok := stateChange.NewValue.(xdr.Uint64); !ok || newValueUint64 != xdr.Uint64(1500) {
		t.Errorf("Expected new value 1500 (xdr.Uint64), got %v (type %T)", stateChange.NewValue, stateChange.NewValue)
	}

	// Test delete operation
	stateChange = processor.extractStateChangeFromContractData(contractData, oldValue, xdr.ScVal{}, "delete")
	if stateChange == nil {
		t.Fatal("extractStateChangeFromContractData should not return nil for valid data")
	}

	if stateChange.Operation != "delete" {
		t.Errorf("Expected operation 'delete', got '%s'", stateChange.Operation)
	}

	if oldValueUint64, ok := stateChange.OldValue.(xdr.Uint64); !ok || oldValueUint64 != xdr.Uint64(500) {
		t.Errorf("Expected old value 500 (xdr.Uint64), got %v (type %T)", stateChange.OldValue, stateChange.OldValue)
	}

	if stateChange.NewValue != nil {
		t.Errorf("Expected new value to be nil for delete operation, got %v", stateChange.NewValue)
	}
}

func TestExtractStateChangeFromContractDataNilChecks(t *testing.T) {
	// Test nil handling to prevent segmentation faults
	processor := &ContractInvocationProcessor{
		networkPassphrase: "Test SDF Network ; September 2015",
	}

	// Test with nil ContractId
	contractDataWithNilID := xdr.ContractDataEntry{
		Contract: xdr.ScAddress{
			Type:       xdr.ScAddressTypeScAddressTypeContract,
			ContractId: nil, // This should be handled gracefully
		},
		Key: xdr.ScVal{
			Type: xdr.ScValTypeScvSymbol,
			Sym:  &[]xdr.ScSymbol{xdr.ScSymbol("balance")}[0],
		},
		Val: xdr.ScVal{
			Type: xdr.ScValTypeScvU64,
			U64:  func() *xdr.Uint64 { v := xdr.Uint64(1000); return &v }(),
		},
	}

	// This should return nil instead of panicking
	stateChange := processor.extractStateChangeFromContractData(contractDataWithNilID, xdr.ScVal{}, xdr.ScVal{}, "create")
	if stateChange != nil {
		t.Error("Expected nil state change when ContractId is nil")
	}
}

// Note: Complex mocking of ingest.LedgerTransaction interface was removed
// in favor of more focused unit testing of the dual representation logic
