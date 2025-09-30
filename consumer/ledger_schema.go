package consumer

import (
	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
)

// LedgerParquetSchema defines the Arrow schema for Stellar ledger parquet files
// that matches AWS's format with nested structures
type LedgerParquetSchema struct {
	Schema       *arrow.Schema
	MemoryPool   memory.Allocator
}

// NewLedgerParquetSchema creates a new ledger parquet schema matching AWS format
func NewLedgerParquetSchema() *LedgerParquetSchema {
	// Define event schema (used in multiple places)
	eventFields := []arrow.Field{
		{Name: "id", Type: arrow.BinaryTypes.String},
		{Name: "type", Type: arrow.BinaryTypes.String},
		{Name: "contract_id", Type: arrow.BinaryTypes.String, Nullable: true},
		{Name: "topics", Type: arrow.BinaryTypes.String, Nullable: true},
		{Name: "data", Type: arrow.BinaryTypes.String, Nullable: true},
		{Name: "in_successful_contract_call", Type: arrow.FixedWidthTypes.Boolean},
	}

	// Define ledger entry change schema
	ledgerEntryChangeFields := []arrow.Field{
		{Name: "id", Type: arrow.BinaryTypes.String},
		{Name: "change_type", Type: arrow.BinaryTypes.String},
		{Name: "ledger_entry_type", Type: arrow.BinaryTypes.String},
		{Name: "entry_data", Type: arrow.BinaryTypes.String, Nullable: true},
		{Name: "key_data", Type: arrow.BinaryTypes.String, Nullable: true},
		{Name: "last_modified_ledger_sequence", Type: arrow.PrimitiveTypes.Int64, Nullable: true},
	}

	// Define operation schema
	operationFields := []arrow.Field{
		{Name: "id", Type: arrow.BinaryTypes.String},
		{Name: "source_account", Type: arrow.BinaryTypes.String, Nullable: true},
		{Name: "source_account_muxed", Type: arrow.BinaryTypes.String, Nullable: true},
		{Name: "type", Type: arrow.BinaryTypes.String},
		{Name: "body", Type: arrow.BinaryTypes.String},
		{Name: "result_code", Type: arrow.BinaryTypes.String, Nullable: true},
		{Name: "ledger_entry_changes", Type: arrow.ListOf(arrow.StructOf(ledgerEntryChangeFields...))},
		{Name: "events", Type: arrow.ListOf(arrow.StructOf(eventFields...))},
	}

	// Define transaction schema
	transactionFields := []arrow.Field{
		{Name: "transaction_hash", Type: arrow.BinaryTypes.String},
		{Name: "account", Type: arrow.BinaryTypes.String},
		{Name: "account_muxed", Type: arrow.BinaryTypes.String, Nullable: true},
		{Name: "account_sequence", Type: arrow.PrimitiveTypes.Int64},
		{Name: "max_fee", Type: arrow.PrimitiveTypes.Int64},
		{Name: "fee_charged", Type: arrow.PrimitiveTypes.Int64},
		{Name: "fee_account", Type: arrow.BinaryTypes.String, Nullable: true},
		{Name: "fee_account_muxed", Type: arrow.BinaryTypes.String, Nullable: true},
		{Name: "inner_transaction_hash", Type: arrow.BinaryTypes.String, Nullable: true},
		{Name: "new_max_fee", Type: arrow.PrimitiveTypes.Int64, Nullable: true},
		{Name: "memo_type", Type: arrow.BinaryTypes.String, Nullable: true},
		{Name: "memo", Type: arrow.BinaryTypes.String, Nullable: true},
		{Name: "time_bounds_lower", Type: arrow.PrimitiveTypes.Int64, Nullable: true},
		{Name: "time_bounds_upper", Type: arrow.PrimitiveTypes.Int64, Nullable: true},
		{Name: "successful", Type: arrow.FixedWidthTypes.Boolean},
		{Name: "transaction_result_code", Type: arrow.BinaryTypes.String, Nullable: true},
		{Name: "operation_count", Type: arrow.PrimitiveTypes.Int32},
		{Name: "inclusion_fee_bid", Type: arrow.PrimitiveTypes.Int64},
		{Name: "resource_fee", Type: arrow.PrimitiveTypes.Int64},
		{Name: "soroban_resources_instructions", Type: arrow.PrimitiveTypes.Int64},
		{Name: "soroban_resources_read_bytes", Type: arrow.PrimitiveTypes.Int64},
		{Name: "soroban_resources_write_bytes", Type: arrow.PrimitiveTypes.Int64},
		{Name: "non_refundable_resource_fee_charged", Type: arrow.PrimitiveTypes.Int64},
		{Name: "refundable_resource_fee_charged", Type: arrow.PrimitiveTypes.Int64},
		{Name: "rent_fee_charged", Type: arrow.PrimitiveTypes.Int64},
		{Name: "tx_signers", Type: arrow.ListOf(arrow.BinaryTypes.String)},
		{Name: "operations", Type: arrow.ListOf(arrow.StructOf(operationFields...))},
		{Name: "events", Type: arrow.ListOf(arrow.StructOf(eventFields...))},
		{Name: "diagnostic_events", Type: arrow.ListOf(arrow.StructOf(eventFields...))},
	}

	// Define top-level ledger schema
	ledgerFields := []arrow.Field{
		{Name: "sequence", Type: arrow.PrimitiveTypes.Int64},
		{Name: "ledger_hash", Type: arrow.BinaryTypes.String},
		{Name: "previous_ledger_hash", Type: arrow.BinaryTypes.String},
		{Name: "closed_at", Type: arrow.FixedWidthTypes.Timestamp_ms},
		{Name: "protocol_version", Type: arrow.PrimitiveTypes.Int32},
		{Name: "total_coins", Type: arrow.PrimitiveTypes.Int64},
		{Name: "fee_pool", Type: arrow.PrimitiveTypes.Int64},
		{Name: "base_fee", Type: arrow.PrimitiveTypes.Int32},
		{Name: "base_reserve", Type: arrow.PrimitiveTypes.Int32},
		{Name: "max_tx_set_size", Type: arrow.PrimitiveTypes.Int32},
		{Name: "successful_transaction_count", Type: arrow.PrimitiveTypes.Int32},
		{Name: "failed_transaction_count", Type: arrow.PrimitiveTypes.Int32},
		{Name: "soroban_fee_write_1kb", Type: arrow.PrimitiveTypes.Int64, Nullable: true},
		{Name: "node_id", Type: arrow.BinaryTypes.String, Nullable: true},
		{Name: "signature", Type: arrow.BinaryTypes.String, Nullable: true},
		{Name: "transactions", Type: arrow.ListOf(arrow.StructOf(transactionFields...))},
	}

	schema := arrow.NewSchema(ledgerFields, nil)

	return &LedgerParquetSchema{
		Schema:     schema,
		MemoryPool: memory.NewGoAllocator(),
	}
}

// LedgerRecordBuilder helps build ledger records with proper nested structure
type LedgerRecordBuilder struct {
	builder *array.RecordBuilder
	schema  *LedgerParquetSchema
}

// NewLedgerRecordBuilder creates a new builder for ledger records
func NewLedgerRecordBuilder(schema *LedgerParquetSchema) *LedgerRecordBuilder {
	builder := array.NewRecordBuilder(schema.MemoryPool, schema.Schema)
	return &LedgerRecordBuilder{
		builder: builder,
		schema:  schema,
	}
}

// SequenceBuilder returns the builder for sequence field
func (b *LedgerRecordBuilder) SequenceBuilder() *array.Int64Builder {
	return b.builder.Field(0).(*array.Int64Builder)
}

// LedgerHashBuilder returns the builder for ledger_hash field
func (b *LedgerRecordBuilder) LedgerHashBuilder() *array.StringBuilder {
	return b.builder.Field(1).(*array.StringBuilder)
}

// PreviousLedgerHashBuilder returns the builder for previous_ledger_hash field
func (b *LedgerRecordBuilder) PreviousLedgerHashBuilder() *array.StringBuilder {
	return b.builder.Field(2).(*array.StringBuilder)
}

// ClosedAtBuilder returns the builder for closed_at field
func (b *LedgerRecordBuilder) ClosedAtBuilder() *array.TimestampBuilder {
	return b.builder.Field(3).(*array.TimestampBuilder)
}

// ProtocolVersionBuilder returns the builder for protocol_version field
func (b *LedgerRecordBuilder) ProtocolVersionBuilder() *array.Int32Builder {
	return b.builder.Field(4).(*array.Int32Builder)
}

// TotalCoinsBuilder returns the builder for total_coins field
func (b *LedgerRecordBuilder) TotalCoinsBuilder() *array.Int64Builder {
	return b.builder.Field(5).(*array.Int64Builder)
}

// FeePoolBuilder returns the builder for fee_pool field
func (b *LedgerRecordBuilder) FeePoolBuilder() *array.Int64Builder {
	return b.builder.Field(6).(*array.Int64Builder)
}

// BaseFeeBuilder returns the builder for base_fee field
func (b *LedgerRecordBuilder) BaseFeeBuilder() *array.Int32Builder {
	return b.builder.Field(7).(*array.Int32Builder)
}

// BaseReserveBuilder returns the builder for base_reserve field
func (b *LedgerRecordBuilder) BaseReserveBuilder() *array.Int32Builder {
	return b.builder.Field(8).(*array.Int32Builder)
}

// MaxTxSetSizeBuilder returns the builder for max_tx_set_size field
func (b *LedgerRecordBuilder) MaxTxSetSizeBuilder() *array.Int32Builder {
	return b.builder.Field(9).(*array.Int32Builder)
}

// SuccessfulTransactionCountBuilder returns the builder for successful_transaction_count field
func (b *LedgerRecordBuilder) SuccessfulTransactionCountBuilder() *array.Int32Builder {
	return b.builder.Field(10).(*array.Int32Builder)
}

// FailedTransactionCountBuilder returns the builder for failed_transaction_count field
func (b *LedgerRecordBuilder) FailedTransactionCountBuilder() *array.Int32Builder {
	return b.builder.Field(11).(*array.Int32Builder)
}

// SorobanFeeWrite1KbBuilder returns the builder for soroban_fee_write_1kb field
func (b *LedgerRecordBuilder) SorobanFeeWrite1KbBuilder() *array.Int64Builder {
	return b.builder.Field(12).(*array.Int64Builder)
}

// NodeIdBuilder returns the builder for node_id field
func (b *LedgerRecordBuilder) NodeIdBuilder() *array.StringBuilder {
	return b.builder.Field(13).(*array.StringBuilder)
}

// SignatureBuilder returns the builder for signature field
func (b *LedgerRecordBuilder) SignatureBuilder() *array.StringBuilder {
	return b.builder.Field(14).(*array.StringBuilder)
}

// TransactionsBuilder returns the builder for transactions field
func (b *LedgerRecordBuilder) TransactionsBuilder() *array.ListBuilder {
	return b.builder.Field(15).(*array.ListBuilder)
}

// NewRecord builds and returns the record
func (b *LedgerRecordBuilder) NewRecord() arrow.Record {
	return b.builder.NewRecord()
}

// Release releases the builder resources
func (b *LedgerRecordBuilder) Release() {
	b.builder.Release()
}