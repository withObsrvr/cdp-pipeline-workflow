package consumer

import (
	"fmt"

	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
)

// TransactionBuilder helps build transaction structs
type TransactionBuilder struct {
	structBuilder *array.StructBuilder
	pool          memory.Allocator
}

// NewTransactionBuilder creates a new transaction builder
func NewTransactionBuilder(pool memory.Allocator, listBuilder *array.ListBuilder) *TransactionBuilder {
	structBuilder := listBuilder.ValueBuilder().(*array.StructBuilder)
	return &TransactionBuilder{
		structBuilder: structBuilder,
		pool:          pool,
	}
}

// AppendTransaction appends a transaction to the builder
func (tb *TransactionBuilder) AppendTransaction(txData map[string]interface{}) error {
	tb.structBuilder.Append(true)

	// Transaction hash (field 0)
	if hash, ok := getLedgerStringValue(txData, "transaction_hash"); ok {
		tb.structBuilder.FieldBuilder(0).(*array.StringBuilder).Append(hash)
	} else {
		tb.structBuilder.FieldBuilder(0).(*array.StringBuilder).AppendNull()
	}

	// Account (field 1)
	if account, ok := getLedgerStringValue(txData, "account"); ok {
		tb.structBuilder.FieldBuilder(1).(*array.StringBuilder).Append(account)
	} else {
		tb.structBuilder.FieldBuilder(1).(*array.StringBuilder).AppendNull()
	}

	// Account muxed (field 2)
	if muxed, ok := getLedgerStringValue(txData, "account_muxed"); ok {
		tb.structBuilder.FieldBuilder(2).(*array.StringBuilder).Append(muxed)
	} else {
		tb.structBuilder.FieldBuilder(2).(*array.StringBuilder).AppendNull()
	}

	// Account sequence (field 3)
	if seq, ok := getLedgerInt64Value(txData, "account_sequence"); ok {
		tb.structBuilder.FieldBuilder(3).(*array.Int64Builder).Append(seq)
	} else {
		tb.structBuilder.FieldBuilder(3).(*array.Int64Builder).AppendNull()
	}

	// Max fee (field 4)
	if fee, ok := getLedgerInt64Value(txData, "max_fee"); ok {
		tb.structBuilder.FieldBuilder(4).(*array.Int64Builder).Append(fee)
	} else {
		tb.structBuilder.FieldBuilder(4).(*array.Int64Builder).AppendNull()
	}

	// Fee charged (field 5)
	if charged, ok := getLedgerInt64Value(txData, "fee_charged"); ok {
		tb.structBuilder.FieldBuilder(5).(*array.Int64Builder).Append(charged)
	} else {
		tb.structBuilder.FieldBuilder(5).(*array.Int64Builder).AppendNull()
	}

	// Fee account (field 6)
	if feeAcct, ok := getLedgerStringValue(txData, "fee_account"); ok {
		tb.structBuilder.FieldBuilder(6).(*array.StringBuilder).Append(feeAcct)
	} else {
		tb.structBuilder.FieldBuilder(6).(*array.StringBuilder).AppendNull()
	}

	// Fee account muxed (field 7)
	if feeMuxed, ok := getLedgerStringValue(txData, "fee_account_muxed"); ok {
		tb.structBuilder.FieldBuilder(7).(*array.StringBuilder).Append(feeMuxed)
	} else {
		tb.structBuilder.FieldBuilder(7).(*array.StringBuilder).AppendNull()
	}

	// Inner transaction hash (field 8)
	if innerHash, ok := getLedgerStringValue(txData, "inner_transaction_hash"); ok {
		tb.structBuilder.FieldBuilder(8).(*array.StringBuilder).Append(innerHash)
	} else {
		tb.structBuilder.FieldBuilder(8).(*array.StringBuilder).AppendNull()
	}

	// New max fee (field 9)
	if newFee, ok := getLedgerInt64Value(txData, "new_max_fee"); ok {
		tb.structBuilder.FieldBuilder(9).(*array.Int64Builder).Append(newFee)
	} else {
		tb.structBuilder.FieldBuilder(9).(*array.Int64Builder).AppendNull()
	}

	// Memo type (field 10)
	if memoType, ok := getLedgerStringValue(txData, "memo_type"); ok {
		tb.structBuilder.FieldBuilder(10).(*array.StringBuilder).Append(memoType)
	} else {
		tb.structBuilder.FieldBuilder(10).(*array.StringBuilder).AppendNull()
	}

	// Memo (field 11)
	if memo, ok := getLedgerStringValue(txData, "memo"); ok {
		tb.structBuilder.FieldBuilder(11).(*array.StringBuilder).Append(memo)
	} else {
		tb.structBuilder.FieldBuilder(11).(*array.StringBuilder).AppendNull()
	}

	// Time bounds lower (field 12)
	if lower, ok := getLedgerInt64Value(txData, "time_bounds_lower"); ok {
		tb.structBuilder.FieldBuilder(12).(*array.Int64Builder).Append(lower)
	} else {
		tb.structBuilder.FieldBuilder(12).(*array.Int64Builder).AppendNull()
	}

	// Time bounds upper (field 13)
	if upper, ok := getLedgerInt64Value(txData, "time_bounds_upper"); ok {
		tb.structBuilder.FieldBuilder(13).(*array.Int64Builder).Append(upper)
	} else {
		tb.structBuilder.FieldBuilder(13).(*array.Int64Builder).AppendNull()
	}

	// Successful (field 14)
	if success, ok := getLedgerBoolValue(txData, "successful"); ok {
		tb.structBuilder.FieldBuilder(14).(*array.BooleanBuilder).Append(success)
	} else {
		tb.structBuilder.FieldBuilder(14).(*array.BooleanBuilder).Append(false)
	}

	// Transaction result code (field 15)
	if code, ok := getLedgerStringValue(txData, "transaction_result_code"); ok {
		tb.structBuilder.FieldBuilder(15).(*array.StringBuilder).Append(code)
	} else {
		tb.structBuilder.FieldBuilder(15).(*array.StringBuilder).AppendNull()
	}

	// Operation count (field 16)
	if count, ok := getLedgerInt32Value(txData, "operation_count"); ok {
		tb.structBuilder.FieldBuilder(16).(*array.Int32Builder).Append(count)
	} else {
		tb.structBuilder.FieldBuilder(16).(*array.Int32Builder).Append(0)
	}

	// Soroban fee fields (fields 17-24)
	appendSorobanFeeFields(tb.structBuilder, txData)

	// TX signers (field 25)
	signersBuilder := tb.structBuilder.FieldBuilder(25).(*array.ListBuilder)
	if signers, ok := txData["tx_signers"].([]interface{}); ok {
		signersBuilder.Append(true)
		for _, signer := range signers {
			if signerStr, ok := signer.(string); ok {
				signersBuilder.ValueBuilder().(*array.StringBuilder).Append(signerStr)
			}
		}
	} else {
		signersBuilder.AppendNull()
	}

	// Operations (field 26)
	opsBuilder := tb.structBuilder.FieldBuilder(26).(*array.ListBuilder)
	if operations, ok := txData["operations"].([]interface{}); ok {
		opsBuilder.Append(true)
		opBuilder := NewOperationBuilder(tb.pool, opsBuilder)
		for _, op := range operations {
			if opData, ok := op.(map[string]interface{}); ok {
				if err := opBuilder.AppendOperation(opData); err != nil {
					return fmt.Errorf("failed to append operation: %w", err)
				}
			}
		}
	} else {
		opsBuilder.AppendNull()
	}

	// Events (field 27)
	eventsBuilder := tb.structBuilder.FieldBuilder(27).(*array.ListBuilder)
	if events, ok := txData["events"].([]interface{}); ok {
		eventsBuilder.Append(true)
		eventBuilder := NewEventBuilder(tb.pool, eventsBuilder)
		for _, event := range events {
			if eventData, ok := event.(map[string]interface{}); ok {
				if err := eventBuilder.AppendEvent(eventData); err != nil {
					return fmt.Errorf("failed to append event: %w", err)
				}
			}
		}
	} else {
		eventsBuilder.AppendNull()
	}

	// Diagnostic events (field 28)
	diagEventsBuilder := tb.structBuilder.FieldBuilder(28).(*array.ListBuilder)
	if diagEvents, ok := txData["diagnostic_events"].([]interface{}); ok {
		diagEventsBuilder.Append(true)
		diagEventBuilder := NewEventBuilder(tb.pool, diagEventsBuilder)
		for _, event := range diagEvents {
			if eventData, ok := event.(map[string]interface{}); ok {
				if err := diagEventBuilder.AppendEvent(eventData); err != nil {
					return fmt.Errorf("failed to append diagnostic event: %w", err)
				}
			}
		}
	} else {
		diagEventsBuilder.AppendNull()
	}

	return nil
}

// OperationBuilder helps build operation structs
type OperationBuilder struct {
	structBuilder *array.StructBuilder
	pool          memory.Allocator
}

// NewOperationBuilder creates a new operation builder
func NewOperationBuilder(pool memory.Allocator, listBuilder *array.ListBuilder) *OperationBuilder {
	structBuilder := listBuilder.ValueBuilder().(*array.StructBuilder)
	return &OperationBuilder{
		structBuilder: structBuilder,
		pool:          pool,
	}
}

// AppendOperation appends an operation to the builder
func (ob *OperationBuilder) AppendOperation(opData map[string]interface{}) error {
	ob.structBuilder.Append(true)

	// ID (field 0)
	if id, ok := getLedgerStringValue(opData, "id"); ok {
		ob.structBuilder.FieldBuilder(0).(*array.StringBuilder).Append(id)
	} else {
		ob.structBuilder.FieldBuilder(0).(*array.StringBuilder).AppendNull()
	}

	// Source account (field 1)
	if source, ok := getLedgerStringValue(opData, "source_account"); ok {
		ob.structBuilder.FieldBuilder(1).(*array.StringBuilder).Append(source)
	} else {
		ob.structBuilder.FieldBuilder(1).(*array.StringBuilder).AppendNull()
	}

	// Source account muxed (field 2)
	if muxed, ok := getLedgerStringValue(opData, "source_account_muxed"); ok {
		ob.structBuilder.FieldBuilder(2).(*array.StringBuilder).Append(muxed)
	} else {
		ob.structBuilder.FieldBuilder(2).(*array.StringBuilder).AppendNull()
	}

	// Type (field 3)
	if opType, ok := getLedgerStringValue(opData, "type"); ok {
		ob.structBuilder.FieldBuilder(3).(*array.StringBuilder).Append(opType)
	} else {
		ob.structBuilder.FieldBuilder(3).(*array.StringBuilder).AppendNull()
	}

	// Body (field 4)
	if body, ok := getLedgerStringValue(opData, "body"); ok {
		ob.structBuilder.FieldBuilder(4).(*array.StringBuilder).Append(body)
	} else {
		ob.structBuilder.FieldBuilder(4).(*array.StringBuilder).AppendNull()
	}

	// Result code (field 5)
	if code, ok := getLedgerStringValue(opData, "result_code"); ok {
		ob.structBuilder.FieldBuilder(5).(*array.StringBuilder).Append(code)
	} else {
		ob.structBuilder.FieldBuilder(5).(*array.StringBuilder).AppendNull()
	}

	// Ledger entry changes (field 6)
	changesBuilder := ob.structBuilder.FieldBuilder(6).(*array.ListBuilder)
	if changes, ok := opData["ledger_entry_changes"].([]interface{}); ok {
		changesBuilder.Append(true)
		changeBuilder := NewLedgerEntryChangeBuilder(ob.pool, changesBuilder)
		for _, change := range changes {
			if changeData, ok := change.(map[string]interface{}); ok {
				if err := changeBuilder.AppendChange(changeData); err != nil {
					return fmt.Errorf("failed to append ledger entry change: %w", err)
				}
			}
		}
	} else {
		changesBuilder.AppendNull()
	}

	// Events (field 7)
	eventsBuilder := ob.structBuilder.FieldBuilder(7).(*array.ListBuilder)
	if events, ok := opData["events"].([]interface{}); ok {
		eventsBuilder.Append(true)
		eventBuilder := NewEventBuilder(ob.pool, eventsBuilder)
		for _, event := range events {
			if eventData, ok := event.(map[string]interface{}); ok {
				if err := eventBuilder.AppendEvent(eventData); err != nil {
					return fmt.Errorf("failed to append operation event: %w", err)
				}
			}
		}
	} else {
		eventsBuilder.AppendNull()
	}

	return nil
}

// EventBuilder helps build event structs
type EventBuilder struct {
	structBuilder *array.StructBuilder
}

// NewEventBuilder creates a new event builder
func NewEventBuilder(pool memory.Allocator, listBuilder *array.ListBuilder) *EventBuilder {
	structBuilder := listBuilder.ValueBuilder().(*array.StructBuilder)
	return &EventBuilder{
		structBuilder: structBuilder,
	}
}

// AppendEvent appends an event to the builder
func (eb *EventBuilder) AppendEvent(eventData map[string]interface{}) error {
	eb.structBuilder.Append(true)

	// ID (field 0)
	if id, ok := getLedgerStringValue(eventData, "id"); ok {
		eb.structBuilder.FieldBuilder(0).(*array.StringBuilder).Append(id)
	} else {
		eb.structBuilder.FieldBuilder(0).(*array.StringBuilder).AppendNull()
	}

	// Type (field 1)
	if eventType, ok := getLedgerStringValue(eventData, "type"); ok {
		eb.structBuilder.FieldBuilder(1).(*array.StringBuilder).Append(eventType)
	} else {
		eb.structBuilder.FieldBuilder(1).(*array.StringBuilder).AppendNull()
	}

	// Contract ID (field 2)
	if contractId, ok := getLedgerStringValue(eventData, "contract_id"); ok {
		eb.structBuilder.FieldBuilder(2).(*array.StringBuilder).Append(contractId)
	} else {
		eb.structBuilder.FieldBuilder(2).(*array.StringBuilder).AppendNull()
	}

	// Topics (field 3)
	if topics, ok := getLedgerStringValue(eventData, "topics"); ok {
		eb.structBuilder.FieldBuilder(3).(*array.StringBuilder).Append(topics)
	} else {
		eb.structBuilder.FieldBuilder(3).(*array.StringBuilder).AppendNull()
	}

	// Data (field 4)
	if data, ok := getLedgerStringValue(eventData, "data"); ok {
		eb.structBuilder.FieldBuilder(4).(*array.StringBuilder).Append(data)
	} else {
		eb.structBuilder.FieldBuilder(4).(*array.StringBuilder).AppendNull()
	}

	// In successful contract call (field 5)
	if success, ok := getLedgerBoolValue(eventData, "in_successful_contract_call"); ok {
		eb.structBuilder.FieldBuilder(5).(*array.BooleanBuilder).Append(success)
	} else {
		eb.structBuilder.FieldBuilder(5).(*array.BooleanBuilder).Append(false)
	}

	return nil
}

// LedgerEntryChangeBuilder helps build ledger entry change structs
type LedgerEntryChangeBuilder struct {
	structBuilder *array.StructBuilder
}

// NewLedgerEntryChangeBuilder creates a new ledger entry change builder
func NewLedgerEntryChangeBuilder(pool memory.Allocator, listBuilder *array.ListBuilder) *LedgerEntryChangeBuilder {
	structBuilder := listBuilder.ValueBuilder().(*array.StructBuilder)
	return &LedgerEntryChangeBuilder{
		structBuilder: structBuilder,
	}
}

// AppendChange appends a ledger entry change to the builder
func (lb *LedgerEntryChangeBuilder) AppendChange(changeData map[string]interface{}) error {
	lb.structBuilder.Append(true)

	// ID (field 0)
	if id, ok := getLedgerStringValue(changeData, "id"); ok {
		lb.structBuilder.FieldBuilder(0).(*array.StringBuilder).Append(id)
	} else {
		lb.structBuilder.FieldBuilder(0).(*array.StringBuilder).AppendNull()
	}

	// Change type (field 1)
	if changeType, ok := getLedgerStringValue(changeData, "change_type"); ok {
		lb.structBuilder.FieldBuilder(1).(*array.StringBuilder).Append(changeType)
	} else {
		lb.structBuilder.FieldBuilder(1).(*array.StringBuilder).AppendNull()
	}

	// Ledger entry type (field 2)
	if entryType, ok := getLedgerStringValue(changeData, "ledger_entry_type"); ok {
		lb.structBuilder.FieldBuilder(2).(*array.StringBuilder).Append(entryType)
	} else {
		lb.structBuilder.FieldBuilder(2).(*array.StringBuilder).AppendNull()
	}

	// Entry data (field 3)
	if entryData, ok := getLedgerStringValue(changeData, "entry_data"); ok {
		lb.structBuilder.FieldBuilder(3).(*array.StringBuilder).Append(entryData)
	} else {
		lb.structBuilder.FieldBuilder(3).(*array.StringBuilder).AppendNull()
	}

	// Key data (field 4)
	if keyData, ok := getLedgerStringValue(changeData, "key_data"); ok {
		lb.structBuilder.FieldBuilder(4).(*array.StringBuilder).Append(keyData)
	} else {
		lb.structBuilder.FieldBuilder(4).(*array.StringBuilder).AppendNull()
	}

	// Last modified ledger sequence (field 5)
	if seq, ok := getLedgerInt64Value(changeData, "last_modified_ledger_sequence"); ok {
		lb.structBuilder.FieldBuilder(5).(*array.Int64Builder).Append(seq)
	} else {
		lb.structBuilder.FieldBuilder(5).(*array.Int64Builder).AppendNull()
	}

	return nil
}

// Helper functions to extract values

func getLedgerStringValue(data map[string]interface{}, key string) (string, bool) {
	if val, ok := data[key]; ok && val != nil {
		if str, ok := val.(string); ok {
			return str, true
		}
	}
	return "", false
}

func getLedgerInt64Value(data map[string]interface{}, key string) (int64, bool) {
	if val, ok := data[key]; ok && val != nil {
		switch v := val.(type) {
		case int64:
			return v, true
		case float64:
			return int64(v), true
		case int:
			return int64(v), true
		}
	}
	return 0, false
}

func getLedgerInt32Value(data map[string]interface{}, key string) (int32, bool) {
	if val, ok := data[key]; ok && val != nil {
		switch v := val.(type) {
		case int32:
			return v, true
		case float64:
			return int32(v), true
		case int:
			return int32(v), true
		case int64:
			return int32(v), true
		}
	}
	return 0, false
}

func getLedgerBoolValue(data map[string]interface{}, key string) (bool, bool) {
	if val, ok := data[key]; ok && val != nil {
		if b, ok := val.(bool); ok {
			return b, true
		}
	}
	return false, false
}

// appendSorobanFeeFields appends Soroban fee-related fields
func appendSorobanFeeFields(structBuilder *array.StructBuilder, txData map[string]interface{}) {
	// Inclusion fee bid (field 17)
	if fee, ok := getLedgerInt64Value(txData, "inclusion_fee_bid"); ok {
		structBuilder.FieldBuilder(17).(*array.Int64Builder).Append(fee)
	} else {
		structBuilder.FieldBuilder(17).(*array.Int64Builder).Append(0)
	}

	// Resource fee (field 18)
	if fee, ok := getLedgerInt64Value(txData, "resource_fee"); ok {
		structBuilder.FieldBuilder(18).(*array.Int64Builder).Append(fee)
	} else {
		structBuilder.FieldBuilder(18).(*array.Int64Builder).Append(0)
	}

	// Soroban resources instructions (field 19)
	if instr, ok := getLedgerInt64Value(txData, "soroban_resources_instructions"); ok {
		structBuilder.FieldBuilder(19).(*array.Int64Builder).Append(instr)
	} else {
		structBuilder.FieldBuilder(19).(*array.Int64Builder).Append(0)
	}

	// Soroban resources read bytes (field 20)
	if bytes, ok := getLedgerInt64Value(txData, "soroban_resources_read_bytes"); ok {
		structBuilder.FieldBuilder(20).(*array.Int64Builder).Append(bytes)
	} else {
		structBuilder.FieldBuilder(20).(*array.Int64Builder).Append(0)
	}

	// Soroban resources write bytes (field 21)
	if bytes, ok := getLedgerInt64Value(txData, "soroban_resources_write_bytes"); ok {
		structBuilder.FieldBuilder(21).(*array.Int64Builder).Append(bytes)
	} else {
		structBuilder.FieldBuilder(21).(*array.Int64Builder).Append(0)
	}

	// Non-refundable resource fee charged (field 22)
	if fee, ok := getLedgerInt64Value(txData, "non_refundable_resource_fee_charged"); ok {
		structBuilder.FieldBuilder(22).(*array.Int64Builder).Append(fee)
	} else {
		structBuilder.FieldBuilder(22).(*array.Int64Builder).Append(0)
	}

	// Refundable resource fee charged (field 23)
	if fee, ok := getLedgerInt64Value(txData, "refundable_resource_fee_charged"); ok {
		structBuilder.FieldBuilder(23).(*array.Int64Builder).Append(fee)
	} else {
		structBuilder.FieldBuilder(23).(*array.Int64Builder).Append(0)
	}

	// Rent fee charged (field 24)
	if fee, ok := getLedgerInt64Value(txData, "rent_fee_charged"); ok {
		structBuilder.FieldBuilder(24).(*array.Int64Builder).Append(fee)
	} else {
		structBuilder.FieldBuilder(24).(*array.Int64Builder).Append(0)
	}
}