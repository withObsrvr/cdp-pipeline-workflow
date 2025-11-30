package processor

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"time"
)

// BronzeToContractInvocationProcessor converts Bronze SQL query results to ContractInvocation format
// This bridges Bronze data with the existing ContractInvocationExtractor
type BronzeToContractInvocationProcessor struct {
	processors []Processor
}

// NewBronzeToContractInvocationProcessor creates a new Bronze to ContractInvocation converter
func NewBronzeToContractInvocationProcessor(config map[string]interface{}) (*BronzeToContractInvocationProcessor, error) {
	return &BronzeToContractInvocationProcessor{
		processors: make([]Processor, 0),
	}, nil
}

// Subscribe adds a processor to the chain
func (p *BronzeToContractInvocationProcessor) Subscribe(processor Processor) {
	p.processors = append(p.processors, processor)
}

// Process converts Bronze operation data to ContractInvocation format
func (p *BronzeToContractInvocationProcessor) Process(ctx context.Context, msg Message) error {
	// Bronze data comes as map[string]interface{} from DuckLake query
	rowMap, ok := msg.Payload.(map[string]interface{})
	if !ok {
		return fmt.Errorf("expected map[string]interface{} from Bronze query, got %T", msg.Payload)
	}

	// Convert Bronze row to ContractInvocation format
	invocation, err := p.bronzeRowToContractInvocation(rowMap)
	if err != nil {
		log.Printf("BronzeToContractInvocation: Failed to convert row: %v", err)
		return nil // Don't fail pipeline, just skip this row
	}

	// Marshal to JSON (same format as ContractInvocation processor)
	jsonBytes, err := json.Marshal(invocation)
	if err != nil {
		return fmt.Errorf("failed to marshal invocation: %w", err)
	}

	// Forward to processors (typically ContractInvocationExtractor)
	for _, processor := range p.processors {
		if err := processor.Process(ctx, Message{Payload: jsonBytes}); err != nil {
			return fmt.Errorf("error in processor chain: %w", err)
		}
	}

	return nil
}

// bronzeRowToContractInvocation converts a Bronze SQL row to ContractInvocation format
func (p *BronzeToContractInvocationProcessor) bronzeRowToContractInvocation(row map[string]interface{}) (*ContractInvocation, error) {
	invocation := &ContractInvocation{}

	// Extract ledger_sequence
	if ledgerSeq, ok := getInt64Field(row, "ledger_sequence"); ok {
		invocation.LedgerSequence = uint32(ledgerSeq)
	} else {
		return nil, fmt.Errorf("missing ledger_sequence")
	}

	// Extract transaction_hash
	if txHash, ok := getStringField(row, "transaction_hash"); ok {
		invocation.TransactionHash = txHash
	} else {
		return nil, fmt.Errorf("missing transaction_hash")
	}

	// Extract operation_index
	if opIndex, ok := getStringField(row, "operation_index"); ok {
		// Bronze stores as string like "0", "1", etc.
		var opIdx int64
		fmt.Sscanf(opIndex, "%d", &opIdx)
		invocation.OperationIndex = uint32(opIdx)
	}

	// Extract source_account (invoking account)
	if sourceAccount, ok := getStringField(row, "source_account"); ok {
		invocation.InvokingAccount = sourceAccount
	}

	// Extract timestamp
	if createdAt, ok := getStringField(row, "created_at"); ok {
		if t, err := time.Parse(time.RFC3339, createdAt); err == nil {
			invocation.Timestamp = t
		}
	}

	// Extract successful flag
	if successful, ok := getBoolField(row, "transaction_successful"); ok {
		invocation.Successful = successful
	}

	// Extract contract_id (if available from JOIN with contract_events)
	if contractID, ok := getStringField(row, "contract_id"); ok {
		invocation.ContractID = contractID
	}

	// Extract function name from events (if available)
	// Bronze contract_events_stream_v1 has topics_decoded which contains function name
	if topicsJSON, ok := getStringField(row, "topics_decoded"); ok {
		var topics []interface{}
		if err := json.Unmarshal([]byte(topicsJSON), &topics); err == nil {
			// First topic is often "fn_call", second is contract address, third is function name
			if len(topics) >= 3 {
				if fnName, ok := topics[2].(string); ok {
					invocation.FunctionName = fnName
				}
			}
		}
	}

	// Extract arguments from data_decoded (if available)
	if dataJSON, ok := getStringField(row, "data_decoded"); ok {
		var dataDecoded interface{}
		if err := json.Unmarshal([]byte(dataJSON), &dataDecoded); err == nil {
			// Convert to Arguments format
			if args, ok := dataDecoded.([]interface{}); ok {
				invocation.ArgumentsDecoded = make(map[string]interface{})
				for i, arg := range args {
					invocation.ArgumentsDecoded[fmt.Sprintf("arg_%d", i)] = arg
				}
			}
		}
	}

	// Note: Bronze doesn't have DiagnosticEvents, ContractCalls, StateChanges, TtlExtensions
	// in the same nested format. Those would need to be queried separately if needed.

	return invocation, nil
}

// Helper functions to safely extract fields from Bronze row map

func getStringField(row map[string]interface{}, key string) (string, bool) {
	if val, ok := row[key]; ok && val != nil {
		if str, ok := val.(string); ok {
			return str, true
		}
	}
	return "", false
}

func getInt64Field(row map[string]interface{}, key string) (int64, bool) {
	if val, ok := row[key]; ok && val != nil {
		switch v := val.(type) {
		case int64:
			return v, true
		case int:
			return int64(v), true
		case float64:
			return int64(v), true
		case string:
			var i int64
			if _, err := fmt.Sscanf(v, "%d", &i); err == nil {
				return i, true
			}
		}
	}
	return 0, false
}

func getBoolField(row map[string]interface{}, key string) (bool, bool) {
	if val, ok := row[key]; ok && val != nil {
		if b, ok := val.(bool); ok {
			return b, true
		}
	}
	return false, false
}
