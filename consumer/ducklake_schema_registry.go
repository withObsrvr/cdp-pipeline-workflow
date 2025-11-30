package consumer

import (
	"encoding/json"
	"fmt"
	"sync"

	"github.com/withObsrvr/cdp-pipeline-workflow/processor"
)

// DuckLakeSchemaRegistry manages SQL schemas for different processor types.
// It provides a centralized location for schema definitions and enables
// automatic table creation based on processor type identification.
type DuckLakeSchemaRegistry struct {
	schemas map[processor.ProcessorType]SchemaDefinition
	mu      sync.RWMutex
}

// SchemaDefinition contains complete SQL schema definition with versioning.
// Each processor type should have a corresponding schema definition that
// describes how to create tables, insert data, and optimize queries.
type SchemaDefinition struct {
	ProcessorType processor.ProcessorType
	Version       string
	MinVersion    string // Minimum compatible version for migration
	Description   string

	// SQL statements
	CreateSQL string // CREATE TABLE statement with PRIMARY KEY
	InsertSQL string // INSERT statement with all columns
	UpsertSQL string // MERGE statement for idempotent writes

	// Index definitions for query optimization
	Indexes []IndexDefinition

	// Column mapping from JSON payload to SQL columns
	ColumnMapping map[string]ColumnInfo

	// Migration scripts from previous versions
	Migrations map[string]string // version -> migration SQL
}

// IndexDefinition defines an index for performance optimization.
// Indexes are created automatically if CreateIndexes is enabled
// in the SaveToDuckLake consumer configuration.
type IndexDefinition struct {
	Name       string
	Columns    []string
	Unique     bool
	Expression string // For computed indexes
}

// ColumnInfo describes a column's properties for automated value extraction.
// The JSONPath field uses dot notation to navigate nested JSON structures.
type ColumnInfo struct {
	SQLColumn    string // SQL column name
	JSONPath     string // JSON path in payload (e.g., "contract_id" or "arguments.0")
	DataType     string // SQL data type
	Required     bool
	DefaultValue string
}

// NewDuckLakeSchemaRegistry creates a new schema registry with all
// predefined schemas registered.
func NewDuckLakeSchemaRegistry() *DuckLakeSchemaRegistry {
	registry := &DuckLakeSchemaRegistry{
		schemas: make(map[processor.ProcessorType]SchemaDefinition),
	}
	registry.registerAllSchemas()
	return registry
}

// GetSchema retrieves the schema definition for a given processor type.
// Returns the schema and true if found, or an empty schema and false if not found.
func (r *DuckLakeSchemaRegistry) GetSchema(processorType processor.ProcessorType) (SchemaDefinition, bool) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	schema, ok := r.schemas[processorType]
	return schema, ok
}

// DetectProcessorType detects the processor type from a message.
// It checks metadata first (preferred method), then falls back to
// content-based detection for backward compatibility.
func (r *DuckLakeSchemaRegistry) DetectProcessorType(msg processor.Message) processor.ProcessorType {
	// Check metadata first (preferred method)
	if msg.Metadata != nil {
		// Check for processor_type (standard metadata)
		if pType, ok := msg.Metadata["processor_type"].(string); ok {
			return processor.ProcessorType(pType)
		}

		// Check for table_type (used by BronzeExtractorsProcessor)
		// Bronze tables use table names as processor types
		if tableType, ok := msg.Metadata["table_type"].(string); ok {
			return processor.ProcessorType(tableType)
		}
	}

	// Fall back to content detection (for backward compatibility)
	if msg.Payload == nil {
		return ""
	}

	// Try to unmarshal and detect from payload structure
	// This mirrors ParquetSchemaRegistry logic
	var data map[string]interface{}

	// Handle both []byte and json.RawMessage payloads
	switch v := msg.Payload.(type) {
	case []byte:
		if err := json.Unmarshal(v, &data); err != nil {
			return ""
		}
	case json.RawMessage:
		if err := json.Unmarshal(v, &data); err != nil {
			return ""
		}
	default:
		// Try direct type assertion
		var ok bool
		data, ok = msg.Payload.(map[string]interface{})
		if !ok {
			return ""
		}
	}

	// Detect based on field presence
	return r.detectFromPayload(data)
}

// detectFromPayload attempts to detect processor type from payload structure
func (r *DuckLakeSchemaRegistry) detectFromPayload(data map[string]interface{}) processor.ProcessorType {
	// Contract invocation detection
	if _, hasContractID := data["contract_id"]; hasContractID {
		if _, hasFunctionName := data["function_name"]; hasFunctionName {
			if _, hasInvokingAccount := data["invoking_account"]; hasInvokingAccount {
				return processor.ProcessorTypeContractInvocation
			}
		}
	}

	// Contract event detection
	if _, hasEventID := data["event_id"]; hasEventID {
		if _, hasEventType := data["event_type"]; hasEventType {
			if _, hasTopics := data["topics"]; hasTopics {
				return processor.ProcessorTypeContractEvent
			}
		}
	}

	// Payment detection
	if _, hasFrom := data["from"]; hasFrom {
		if _, hasTo := data["to"]; hasTo {
			if _, hasAmount := data["amount"]; hasAmount {
				if _, hasAsset := data["asset"]; hasAsset {
					return processor.ProcessorTypePayment
				}
			}
		}
	}

	// Trade detection
	if _, hasSellAsset := data["selling_asset"]; hasSellAsset {
		if _, hasBuyAsset := data["buying_asset"]; hasBuyAsset {
			if _, hasPrice := data["price"]; hasPrice {
				return processor.ProcessorTypeTrade
			}
		}
	}

	// Unable to detect
	return ""
}

// GetAllProcessorTypes returns a list of all registered processor types
func (r *DuckLakeSchemaRegistry) GetAllProcessorTypes() []processor.ProcessorType {
	r.mu.RLock()
	defer r.mu.RUnlock()

	types := make([]processor.ProcessorType, 0, len(r.schemas))
	for pType := range r.schemas {
		types = append(types, pType)
	}
	return types
}

// HasSchema checks if a schema is registered for the given processor type
func (r *DuckLakeSchemaRegistry) HasSchema(processorType processor.ProcessorType) bool {
	r.mu.RLock()
	defer r.mu.RUnlock()
	_, ok := r.schemas[processorType]
	return ok
}

// registerAllSchemas registers all known processor schemas.
// This is called during initialization to populate the registry.
func (r *DuckLakeSchemaRegistry) registerAllSchemas() {
	// Silver layer schemas
	r.registerContractInvocationSchema()
	r.registerContractEventSchema()

	// Bronze layer schemas (19 Hubble-compatible tables)
	r.registerBronzeLedgerSchema()
	r.registerBronzeTransactionSchema()
	r.registerBronzeOperationSchema()
	r.registerBronzeEffectSchema()
	r.registerBronzeTradeSchema()
	r.registerBronzeNativeBalanceSchema()
	r.registerBronzeAccountSchema()
	r.registerBronzeTrustlineSchema()
	r.registerBronzeOfferSchema()
	r.registerBronzeClaimableBalanceSchema()
	r.registerBronzeLiquidityPoolSchema()
	r.registerBronzeContractEventSchema()
	r.registerBronzeContractDataSchema()
	r.registerBronzeContractCodeSchema()
	r.registerBronzeConfigSettingSchema()
	r.registerBronzeTTLSchema()
	r.registerBronzeEvictedKeySchema()
	r.registerBronzeRestoredKeySchema()
	r.registerBronzeAccountSignerSchema()

	// Additional Silver layer schemas will be registered in future phases:
	// r.registerPaymentSchema()
	// r.registerAccountDataSchema()
	// r.registerTradeSchema()
}

// registerSchema is a helper to register a schema definition
func (r *DuckLakeSchemaRegistry) registerSchema(def SchemaDefinition) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.schemas[def.ProcessorType] = def
}

// ValidateSchema performs basic validation on a schema definition
func (r *DuckLakeSchemaRegistry) ValidateSchema(def SchemaDefinition) error {
	if def.ProcessorType == "" {
		return fmt.Errorf("processor type cannot be empty")
	}
	if !def.ProcessorType.IsValid() {
		return fmt.Errorf("processor type %s is not valid", def.ProcessorType)
	}
	if def.CreateSQL == "" {
		return fmt.Errorf("CreateSQL cannot be empty for processor type %s", def.ProcessorType)
	}
	if def.Version == "" {
		return fmt.Errorf("Version cannot be empty for processor type %s", def.ProcessorType)
	}
	return nil
}

// registerContractInvocationSchema registers the schema for contract invocations.
// This schema stores decoded smart contract invocation data from the bronze layer (Galexie).
func (r *DuckLakeSchemaRegistry) registerContractInvocationSchema() {
	r.registerSchema(SchemaDefinition{
		ProcessorType: processor.ProcessorTypeContractInvocation,
		Version:       "1.0.0",
		MinVersion:    "1.0.0",
		Description:   "Stellar smart contract invocations (silver layer - processed from Galexie bronze)",

		CreateSQL: `
CREATE TABLE IF NOT EXISTS %s.%s.%s (
    -- Primary Key Components
    ledger_sequence BIGINT NOT NULL,
    transaction_hash VARCHAR NOT NULL,
    operation_index INT NOT NULL,

    -- Timing
    ledger_close_time TIMESTAMP NOT NULL,

    -- Identity
    contract_id VARCHAR NOT NULL,
    invoking_account VARCHAR NOT NULL,
    function_name VARCHAR NOT NULL,

    -- Arguments (decoded from bronze XDR)
    function_args JSON,

    -- Authorization (decoded from bronze XDR)
    auth_entries JSON,

    -- Soroban metadata
    soroban_resources JSON,
    soroban_data JSON,

    -- Execution results
    transaction_successful BOOLEAN NOT NULL,
    operation_successful BOOLEAN NOT NULL,
    result_value JSON,

    -- Resource metrics
    resource_fee BIGINT,
    instructions_used BIGINT,
    read_bytes BIGINT,
    write_bytes BIGINT,

    -- Cross-contract calls (extracted from diagnostic events)
    contract_calls JSON,

    -- State changes (extracted from ledger changes)
    state_changes JSON,

    -- Source metadata (references back to bronze layer)
    archive_source JSON,

    -- Metadata
    metadata JSON,
    ingestion_timestamp TIMESTAMP,

    -- Note: DuckLake does not support PRIMARY KEY constraints
    -- Deduplication is handled via MERGE/UPSERT logic at runtime
)`,

		InsertSQL: `
INSERT INTO %s.%s.%s (
    ledger_sequence, transaction_hash, operation_index,
    ledger_close_time, contract_id, invoking_account, function_name,
    function_args, auth_entries, soroban_resources, soroban_data,
    transaction_successful, operation_successful, result_value,
    resource_fee, instructions_used, read_bytes, write_bytes,
    contract_calls, state_changes, archive_source, metadata
) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,

		UpsertSQL: `
MERGE INTO %s.%s.%s AS target
USING (SELECT
    ? as ledger_sequence,
    ? as transaction_hash,
    ? as operation_index,
    ? as ledger_close_time,
    ? as contract_id,
    ? as invoking_account,
    ? as function_name,
    ? as function_args,
    ? as auth_entries,
    ? as soroban_resources,
    ? as soroban_data,
    ? as transaction_successful,
    ? as operation_successful,
    ? as result_value,
    ? as resource_fee,
    ? as instructions_used,
    ? as read_bytes,
    ? as write_bytes,
    ? as contract_calls,
    ? as state_changes,
    ? as archive_source,
    ? as metadata
) AS source
ON target.ledger_sequence = source.ledger_sequence
   AND target.transaction_hash = source.transaction_hash
   AND target.operation_index = source.operation_index
WHEN NOT MATCHED THEN
    INSERT (ledger_sequence, transaction_hash, operation_index,
            ledger_close_time, contract_id, invoking_account, function_name,
            function_args, auth_entries, soroban_resources, soroban_data,
            transaction_successful, operation_successful, result_value,
            resource_fee, instructions_used, read_bytes, write_bytes,
            contract_calls, state_changes, archive_source, metadata)
    VALUES (source.ledger_sequence, source.transaction_hash, source.operation_index,
            source.ledger_close_time, source.contract_id, source.invoking_account,
            source.function_name, source.function_args, source.auth_entries,
            source.soroban_resources, source.soroban_data,
            source.transaction_successful, source.operation_successful,
            source.result_value, source.resource_fee, source.instructions_used,
            source.read_bytes, source.write_bytes, source.contract_calls,
            source.state_changes, source.archive_source, source.metadata)
WHEN MATCHED THEN
    UPDATE SET
        function_args = source.function_args,
        contract_calls = source.contract_calls,
        state_changes = source.state_changes,
        metadata = source.metadata`,

		Indexes: []IndexDefinition{
			{
				Name:    "idx_contract_invocations_contract_id",
				Columns: []string{"contract_id", "ledger_sequence"},
			},
			{
				Name:    "idx_contract_invocations_function",
				Columns: []string{"function_name", "ledger_sequence"},
			},
			{
				Name:    "idx_contract_invocations_time",
				Columns: []string{"ledger_close_time"},
			},
			{
				Name:    "idx_contract_invocations_account",
				Columns: []string{"invoking_account", "ledger_sequence"},
			},
		},

		ColumnMapping: map[string]ColumnInfo{
			"ledger_sequence":        {SQLColumn: "ledger_sequence", JSONPath: "ledger_sequence", DataType: "BIGINT", Required: true},
			"transaction_hash":       {SQLColumn: "transaction_hash", JSONPath: "transaction_hash", DataType: "VARCHAR", Required: true},
			"operation_index":        {SQLColumn: "operation_index", JSONPath: "operation_index", DataType: "INT", Required: false, DefaultValue: "0"},
			"ledger_close_time":      {SQLColumn: "ledger_close_time", JSONPath: "timestamp", DataType: "TIMESTAMP", Required: true},
			"contract_id":            {SQLColumn: "contract_id", JSONPath: "contract_id", DataType: "VARCHAR", Required: true},
			"invoking_account":       {SQLColumn: "invoking_account", JSONPath: "invoking_account", DataType: "VARCHAR", Required: true},
			"function_name":          {SQLColumn: "function_name", JSONPath: "function_name", DataType: "VARCHAR", Required: true},
			"function_args":          {SQLColumn: "function_args", JSONPath: "arguments_decoded", DataType: "JSON", Required: false},
			"auth_entries":           {SQLColumn: "auth_entries", JSONPath: "auth_entries", DataType: "JSON", Required: false},
			"soroban_resources":      {SQLColumn: "soroban_resources", JSONPath: "soroban_resources", DataType: "JSON", Required: false},
			"soroban_data":           {SQLColumn: "soroban_data", JSONPath: "soroban_data", DataType: "JSON", Required: false},
			"transaction_successful": {SQLColumn: "transaction_successful", JSONPath: "successful", DataType: "BOOLEAN", Required: true},
			"operation_successful":   {SQLColumn: "operation_successful", JSONPath: "successful", DataType: "BOOLEAN", Required: true},
			"result_value":           {SQLColumn: "result_value", JSONPath: "result_value", DataType: "JSON", Required: false},
			"resource_fee":           {SQLColumn: "resource_fee", JSONPath: "resource_fee", DataType: "BIGINT", Required: false},
			"instructions_used":      {SQLColumn: "instructions_used", JSONPath: "instructions_used", DataType: "BIGINT", Required: false},
			"read_bytes":             {SQLColumn: "read_bytes", JSONPath: "read_bytes", DataType: "BIGINT", Required: false},
			"write_bytes":            {SQLColumn: "write_bytes", JSONPath: "write_bytes", DataType: "BIGINT", Required: false},
			"contract_calls":         {SQLColumn: "contract_calls", JSONPath: "contract_calls", DataType: "JSON", Required: false},
			"state_changes":          {SQLColumn: "state_changes", JSONPath: "state_changes", DataType: "JSON", Required: false},
			"archive_source":         {SQLColumn: "archive_source", JSONPath: "archive_metadata", DataType: "JSON", Required: false},
			"metadata":               {SQLColumn: "metadata", JSONPath: "metadata", DataType: "JSON", Required: false},
		},
	})
}

// registerContractEventSchema registers the schema for contract events.
// This schema stores decoded smart contract event data from the bronze layer (Galexie).
func (r *DuckLakeSchemaRegistry) registerContractEventSchema() {
	r.registerSchema(SchemaDefinition{
		ProcessorType: processor.ProcessorTypeContractEvent,
		Version:       "1.0.0",
		MinVersion:    "1.0.0",
		Description:   "Stellar smart contract events (silver layer - decoded from bronze)",

		CreateSQL: `
CREATE TABLE IF NOT EXISTS %s.%s.%s (
    -- Primary Key Components
    ledger_sequence BIGINT NOT NULL,
    transaction_hash VARCHAR NOT NULL,
    event_index INT NOT NULL,

    -- Timing
    ledger_close_time TIMESTAMP NOT NULL,

    -- Event Identity
    event_id VARCHAR NOT NULL,
    event_type VARCHAR NOT NULL,
    contract_id VARCHAR NOT NULL,

    -- Event Data (decoded)
    topics JSON NOT NULL,
    data JSON NOT NULL,

    -- Execution context
    in_successful_contract_call BOOLEAN NOT NULL,

    -- Source metadata
    archive_source JSON,

    -- Metadata
    metadata JSON,
    ingestion_timestamp TIMESTAMP,

    -- Note: DuckLake does not support PRIMARY KEY constraints
    -- Deduplication is handled via MERGE/UPSERT logic at runtime
)`,

		InsertSQL: `
INSERT INTO %s.%s.%s (
    ledger_sequence, transaction_hash, event_index,
    ledger_close_time, event_id, event_type, contract_id,
    topics, data, in_successful_contract_call,
    archive_source, metadata
) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,

		UpsertSQL: `
MERGE INTO %s.%s.%s AS target
USING (SELECT
    ? as ledger_sequence,
    ? as transaction_hash,
    ? as event_index,
    ? as ledger_close_time,
    ? as event_id,
    ? as event_type,
    ? as contract_id,
    ? as topics,
    ? as data,
    ? as in_successful_contract_call,
    ? as archive_source,
    ? as metadata
) AS source
ON target.ledger_sequence = source.ledger_sequence
   AND target.transaction_hash = source.transaction_hash
   AND target.event_index = source.event_index
WHEN NOT MATCHED THEN
    INSERT (ledger_sequence, transaction_hash, event_index,
            ledger_close_time, event_id, event_type, contract_id,
            topics, data, in_successful_contract_call,
            archive_source, metadata)
    VALUES (source.ledger_sequence, source.transaction_hash, source.event_index,
            source.ledger_close_time, source.event_id, source.event_type,
            source.contract_id, source.topics, source.data,
            source.in_successful_contract_call, source.archive_source,
            source.metadata)
WHEN MATCHED THEN
    UPDATE SET metadata = source.metadata`,

		Indexes: []IndexDefinition{
			{
				Name:    "idx_contract_events_contract_id",
				Columns: []string{"contract_id", "ledger_sequence"},
			},
			{
				Name:    "idx_contract_events_type",
				Columns: []string{"event_type", "ledger_sequence"},
			},
			{
				Name:    "idx_contract_events_time",
				Columns: []string{"ledger_close_time"},
			},
		},

		ColumnMapping: map[string]ColumnInfo{
			"ledger_sequence":               {SQLColumn: "ledger_sequence", JSONPath: "ledger", DataType: "BIGINT", Required: true},
			"transaction_hash":              {SQLColumn: "transaction_hash", JSONPath: "txHash", DataType: "VARCHAR", Required: true},
			"event_index":                   {SQLColumn: "event_index", JSONPath: "event_index", DataType: "INT", Required: false, DefaultValue: "0"},
			"ledger_close_time":             {SQLColumn: "ledger_close_time", JSONPath: "timestamp", DataType: "TIMESTAMP", Required: false},
			"event_id":                      {SQLColumn: "event_id", JSONPath: "id", DataType: "VARCHAR", Required: true},
			"event_type":                    {SQLColumn: "event_type", JSONPath: "type", DataType: "VARCHAR", Required: true},
			"contract_id":                   {SQLColumn: "contract_id", JSONPath: "contractId", DataType: "VARCHAR", Required: true},
			"topics":                        {SQLColumn: "topics", JSONPath: "topic", DataType: "JSON", Required: true},
			"data":                          {SQLColumn: "data", JSONPath: "value", DataType: "JSON", Required: true},
			"in_successful_contract_call":  {SQLColumn: "in_successful_contract_call", JSONPath: "in_successful_contract_call", DataType: "BOOLEAN", Required: false, DefaultValue: "true"},
			"archive_source":                {SQLColumn: "archive_source", JSONPath: "archive_metadata", DataType: "JSON", Required: false},
			"metadata":                      {SQLColumn: "metadata", JSONPath: "metadata", DataType: "JSON", Required: false},
		},
	})
}
