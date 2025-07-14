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

	argumentsRaw, rawArgs, decodedArgs, err := extractArguments(args)

	if err != nil {
		t.Errorf("extractArguments should not return error for empty args: %v", err)
	}

	if argumentsRaw == nil {
		t.Error("argumentsRaw should not be nil")
	}

	if rawArgs == nil {
		t.Error("rawArgs should not be nil")
	}

	if decodedArgs == nil {
		t.Error("decodedArgs should not be nil")
	}

	// Should all be empty for empty input
	if len(argumentsRaw) != 0 {
		t.Error("argumentsRaw should be empty for empty input")
	}

	if len(rawArgs) != 0 {
		t.Error("rawArgs should be empty for empty input")
	}

	if len(decodedArgs) != 0 {
		t.Error("decodedArgs should be empty for empty input")
	}
}
