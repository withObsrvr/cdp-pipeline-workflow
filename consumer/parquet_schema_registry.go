package consumer

import (
	"fmt"
	"log"
	"reflect"
	"sort"
	"sync"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	processor "github.com/withObsrvr/cdp-pipeline-workflow/processor"
)

// ParquetSchemaRegistry manages Arrow schemas for different message types
type ParquetSchemaRegistry struct {
	allocator memory.Allocator
	schemas   map[string]*arrow.Schema
	mu        sync.RWMutex
}

// NewParquetSchemaRegistry creates a new schema registry
func NewParquetSchemaRegistry() *ParquetSchemaRegistry {
	return &ParquetSchemaRegistry{
		allocator: memory.NewGoAllocator(),
		schemas:   make(map[string]*arrow.Schema),
	}
}

// GetSchema retrieves a cached schema by key
func (r *ParquetSchemaRegistry) GetSchema(key string) (*arrow.Schema, bool) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	schema, ok := r.schemas[key]
	return schema, ok
}

// SetSchema caches a schema by key
func (r *ParquetSchemaRegistry) SetSchema(key string, schema *arrow.Schema) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.schemas[key] = schema
}

// InferSchema infers an Arrow schema from a batch of messages
func (r *ParquetSchemaRegistry) InferSchema(messages []processor.Message) (*arrow.Schema, error) {
	if len(messages) == 0 {
		return nil, fmt.Errorf("cannot infer schema from empty message batch")
	}
	
	// Analyze first message to determine type
	firstPayload := messages[0].Payload
	
	// Try to identify known types
	switch payload := firstPayload.(type) {
	case map[string]interface{}:
		// Check for ContractDataMessage format (from ContractDataProcessor)
		if msgType, hasType := payload["message_type"].(string); hasType && msgType == "contract_data" {
			// This is a ContractDataMessage from the processor
			if _, ok := payload["contract_data"].(map[string]interface{}); ok {
				// Extract the nested contract data and use its schema
				return r.getContractDataMessageSchema(), nil
			}
		}
		
		// Check for direct contract data fields
		if _, hasContractID := payload["contract_id"]; hasContractID {
			if _, hasKeyType := payload["contract_key_type"]; hasKeyType {
				return r.getContractDataSchema(), nil
			}
		}
		if _, hasType := payload["type"]; hasType {
			if _, hasLedger := payload["ledger"]; hasLedger {
				return r.getEventSchema(), nil
			}
		}
		if _, hasCode := payload["code"]; hasCode {
			if _, hasTimestamp := payload["timestamp"]; hasTimestamp {
				return r.getAssetDetailsSchema(), nil
			}
		}
		// Generic map inference
		return r.inferSchemaFromMap(payload)
	case []byte:
		// Try to unmarshal and re-analyze
		return nil, fmt.Errorf("bytes payload not yet supported for schema inference")
	default:
		// Check for known struct types using reflection
		typeName := reflect.TypeOf(payload).String()
		if schema, ok := r.GetSchema(typeName); ok {
			return schema, nil
		}
		return nil, fmt.Errorf("unsupported payload type for schema inference: %T", payload)
	}
}

// inferSchemaFromMap dynamically infers schema from map structure
func (r *ParquetSchemaRegistry) inferSchemaFromMap(data map[string]interface{}) (*arrow.Schema, error) {
	fields := make([]arrow.Field, 0, len(data))
	
	// Sort keys for consistent field ordering
	keys := make([]string, 0, len(data))
	for k := range data {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	
	for _, key := range keys {
		value := data[key]
		field := arrow.Field{
			Name:     key,
			Nullable: true,
		}
		
		// Infer Arrow type from Go type
		switch v := value.(type) {
		case string:
			field.Type = arrow.BinaryTypes.String
		case float64:
			// Check if it's actually an integer
			if v == float64(int64(v)) {
				field.Type = arrow.PrimitiveTypes.Int64
			} else {
				field.Type = arrow.PrimitiveTypes.Float64
			}
		case bool:
			field.Type = arrow.FixedWidthTypes.Boolean
		case int, int32, int64:
			field.Type = arrow.PrimitiveTypes.Int64
		case uint32:
			field.Type = arrow.PrimitiveTypes.Uint32
		case uint64:
			field.Type = arrow.PrimitiveTypes.Uint64
		case nil:
			field.Type = arrow.BinaryTypes.String // Default to string for null values
		default:
			// For complex types, serialize as JSON string
			field.Type = arrow.BinaryTypes.String
		}
		
		fields = append(fields, field)
	}
	
	metadata := arrow.NewMetadata([]string{"inferred", "version"}, []string{"true", "1.0.0"})
	return arrow.NewSchema(fields, &metadata), nil
}

// getContractDataSchema returns the predefined schema for contract data
func (r *ParquetSchemaRegistry) getContractDataSchema() *arrow.Schema {
	if schema, ok := r.GetSchema("contract_data"); ok {
		return schema
	}
	
	fields := []arrow.Field{
		{Name: "contract_id", Type: arrow.BinaryTypes.String, Nullable: false},
		{Name: "contract_key_type", Type: arrow.BinaryTypes.String, Nullable: false},
		{Name: "contract_durability", Type: arrow.BinaryTypes.String, Nullable: false},
		{Name: "asset_code", Type: arrow.BinaryTypes.String, Nullable: true},
		{Name: "asset_issuer", Type: arrow.BinaryTypes.String, Nullable: true},
		{Name: "asset_type", Type: arrow.BinaryTypes.String, Nullable: true},
		{Name: "balance_holder", Type: arrow.BinaryTypes.String, Nullable: true},
		{Name: "balance", Type: arrow.BinaryTypes.String, Nullable: true}, // String for big numbers
		{Name: "last_modified_ledger", Type: arrow.PrimitiveTypes.Uint32, Nullable: false},
		{Name: "ledger_entry_change", Type: arrow.PrimitiveTypes.Uint32, Nullable: false},
		{Name: "deleted", Type: arrow.FixedWidthTypes.Boolean, Nullable: false},
		{Name: "closed_at", Type: &arrow.TimestampType{Unit: arrow.Microsecond, TimeZone: "UTC"}, Nullable: false},
		{Name: "ledger_sequence", Type: arrow.PrimitiveTypes.Uint32, Nullable: false},
		{Name: "ledger_key_hash", Type: arrow.BinaryTypes.String, Nullable: true},
	}
	
	metadata := arrow.NewMetadata(
		[]string{"schema_type", "version", "description"},
		[]string{"contract_data", "1.0.0", "Stellar contract data entries"},
	)
	
	schema := arrow.NewSchema(fields, &metadata)
	r.SetSchema("contract_data", schema)
	return schema
}

// getEventSchema returns the predefined schema for events
func (r *ParquetSchemaRegistry) getEventSchema() *arrow.Schema {
	if schema, ok := r.GetSchema("event"); ok {
		return schema
	}
	
	fields := []arrow.Field{
		{Name: "type", Type: arrow.BinaryTypes.String, Nullable: false},
		{Name: "ledger", Type: arrow.PrimitiveTypes.Uint64, Nullable: false},
		{Name: "ledger_closed_at", Type: arrow.BinaryTypes.String, Nullable: false},
		{Name: "contract_id", Type: arrow.BinaryTypes.String, Nullable: false},
		{Name: "id", Type: arrow.BinaryTypes.String, Nullable: false},
		{Name: "paging_token", Type: arrow.BinaryTypes.String, Nullable: true},
		{Name: "topic", Type: arrow.BinaryTypes.String, Nullable: true}, // JSON string
		{Name: "value", Type: arrow.BinaryTypes.String, Nullable: true}, // JSON string
		{Name: "in_successful_contract_call", Type: arrow.FixedWidthTypes.Boolean, Nullable: false},
		{Name: "tx_hash", Type: arrow.BinaryTypes.String, Nullable: false},
	}
	
	metadata := arrow.NewMetadata(
		[]string{"schema_type", "version", "description"},
		[]string{"event", "1.0.0", "Stellar contract events"},
	)
	
	schema := arrow.NewSchema(fields, &metadata)
	r.SetSchema("event", schema)
	return schema
}

// getAssetDetailsSchema returns the predefined schema for asset details
func (r *ParquetSchemaRegistry) getAssetDetailsSchema() *arrow.Schema {
	if schema, ok := r.GetSchema("asset_details"); ok {
		return schema
	}
	
	fields := []arrow.Field{
		{Name: "code", Type: arrow.BinaryTypes.String, Nullable: false},
		{Name: "issuer", Type: arrow.BinaryTypes.String, Nullable: true},
		{Name: "type", Type: arrow.BinaryTypes.String, Nullable: false},
		{Name: "timestamp", Type: &arrow.TimestampType{Unit: arrow.Microsecond, TimeZone: "UTC"}, Nullable: false},
	}
	
	metadata := arrow.NewMetadata(
		[]string{"schema_type", "version", "description"},
		[]string{"asset_details", "1.0.0", "Stellar asset information"},
	)
	
	schema := arrow.NewSchema(fields, &metadata)
	r.SetSchema("asset_details", schema)
	return schema
}

// getContractDataMessageSchema returns the schema for ContractDataMessage from processor
func (r *ParquetSchemaRegistry) getContractDataMessageSchema() *arrow.Schema {
	if schema, ok := r.GetSchema("contract_data_message"); ok {
		return schema
	}
	
	fields := []arrow.Field{
		// ContractData fields (flattened)
		{Name: "contract_id", Type: arrow.BinaryTypes.String, Nullable: false},
		{Name: "contract_key_type", Type: arrow.BinaryTypes.String, Nullable: true},
		{Name: "contract_durability", Type: arrow.BinaryTypes.String, Nullable: true},
		{Name: "asset_code", Type: arrow.BinaryTypes.String, Nullable: true},
		{Name: "asset_issuer", Type: arrow.BinaryTypes.String, Nullable: true},
		{Name: "asset_type", Type: arrow.BinaryTypes.String, Nullable: true},
		{Name: "balance_holder", Type: arrow.BinaryTypes.String, Nullable: true},
		{Name: "balance", Type: arrow.BinaryTypes.String, Nullable: true},
		{Name: "last_modified_ledger", Type: arrow.PrimitiveTypes.Uint32, Nullable: true},
		{Name: "ledger_entry_change", Type: arrow.PrimitiveTypes.Uint32, Nullable: true},
		{Name: "deleted", Type: arrow.FixedWidthTypes.Boolean, Nullable: true},
		{Name: "closed_at", Type: &arrow.TimestampType{Unit: arrow.Microsecond, TimeZone: "UTC"}, Nullable: true},
		{Name: "ledger_sequence", Type: arrow.PrimitiveTypes.Uint32, Nullable: false},
		{Name: "ledger_key_hash", Type: arrow.BinaryTypes.String, Nullable: true},
		
		// Message metadata fields
		{Name: "processor_name", Type: arrow.BinaryTypes.String, Nullable: true},
		{Name: "message_type", Type: arrow.BinaryTypes.String, Nullable: true},
		{Name: "timestamp", Type: &arrow.TimestampType{Unit: arrow.Microsecond, TimeZone: "UTC"}, Nullable: true},
		
		// Archive metadata (flattened)
		{Name: "archive_file_name", Type: arrow.BinaryTypes.String, Nullable: true},
		{Name: "archive_bucket_name", Type: arrow.BinaryTypes.String, Nullable: true},
		{Name: "archive_file_size", Type: arrow.PrimitiveTypes.Int64, Nullable: true},
	}
	
	metadata := arrow.NewMetadata(
		[]string{"schema_type", "version", "description"},
		[]string{"contract_data_message", "1.0.0", "Stellar contract data from processor"},
	)
	
	schema := arrow.NewSchema(fields, &metadata)
	r.SetSchema("contract_data_message", schema)
	return schema
}

// BuildRecordBatch creates an Arrow record batch from messages using the provided schema
func (r *ParquetSchemaRegistry) BuildRecordBatch(schema *arrow.Schema, messages []processor.Message) (arrow.Record, error) {
	if len(messages) == 0 {
		return nil, fmt.Errorf("no messages to build record batch")
	}
	
	// Create builders for each field
	builders := make([]array.Builder, len(schema.Fields()))
	for i, field := range schema.Fields() {
		builders[i] = array.NewBuilder(r.allocator, field.Type)
	}
	defer func() {
		for _, b := range builders {
			b.Release()
		}
	}()
	
	// Process each message
	for _, msg := range messages {
		data, ok := msg.Payload.(map[string]interface{})
		if !ok {
			// Try to handle other types
			continue
		}
		
		// Check if this is a ContractDataMessage with nested structure
		isContractDataMessage := false
		if msgType, ok := data["message_type"].(string); ok && msgType == "contract_data" {
			isContractDataMessage = true
		}
		
		// Append values for each field
		for i, field := range schema.Fields() {
			var value interface{}
			var exists bool
			
			if isContractDataMessage {
				// Try to get value from nested contract_data first
				if contractData, ok := data["contract_data"].(map[string]interface{}); ok {
					value, exists = contractData[field.Name]
				}
				
				// If not found in contract_data, try top-level fields
				if !exists {
					switch field.Name {
					case "contract_id":
						value, exists = data["contract_id"], true
					case "processor_name":
						value, exists = data["processor_name"], true
					case "message_type":
						value, exists = data["message_type"], true
					case "timestamp":
						value, exists = data["timestamp"], true
					case "ledger_sequence":
						value, exists = data["ledger_sequence"], true
					case "archive_file_name", "archive_bucket_name", "archive_file_size":
						// Extract from archive_metadata if available
						if archiveMeta, ok := data["archive_metadata"].(map[string]interface{}); ok {
							switch field.Name {
							case "archive_file_name":
								value, exists = archiveMeta["file_name"], true
							case "archive_bucket_name":
								value, exists = archiveMeta["bucket_name"], true
							case "archive_file_size":
								value, exists = archiveMeta["file_size"], true
							}
						}
					default:
						value, exists = data[field.Name]
					}
				}
			} else {
				// Direct field access for non-ContractDataMessage
				value, exists = data[field.Name]
			}
			
			if !exists || value == nil {
				builders[i].AppendNull()
				continue
			}
			
			// Append value based on field type
			if err := appendValueToBuilder(builders[i], field.Type, value); err != nil {
				return nil, fmt.Errorf("failed to append value for field %s: %w", field.Name, err)
			}
		}
	}
	
	// Build arrays from builders
	arrays := make([]arrow.Array, len(builders))
	for i, builder := range builders {
		arrays[i] = builder.NewArray()
	}
	
	// Create record batch
	record := array.NewRecord(schema, arrays, int64(len(messages)))
	
	// Release arrays (record holds references)
	for _, arr := range arrays {
		arr.Release()
	}
	
	return record, nil
}

// appendValueToBuilder appends a value to the appropriate builder based on type
func appendValueToBuilder(builder array.Builder, dataType arrow.DataType, value interface{}) error {
	switch dt := dataType.(type) {
	case *arrow.StringType:
		switch v := value.(type) {
		case string:
			builder.(*array.StringBuilder).Append(v)
		default:
			builder.(*array.StringBuilder).Append(fmt.Sprintf("%v", v))
		}
		
	case *arrow.Int64Type:
		switch v := value.(type) {
		case float64:
			builder.(*array.Int64Builder).Append(int64(v))
		case int64:
			builder.(*array.Int64Builder).Append(v)
		case int:
			builder.(*array.Int64Builder).Append(int64(v))
		default:
			return fmt.Errorf("cannot convert %T to int64", v)
		}
		
	case *arrow.Uint32Type:
		switch v := value.(type) {
		case float64:
			builder.(*array.Uint32Builder).Append(uint32(v))
		case uint32:
			builder.(*array.Uint32Builder).Append(v)
		default:
			return fmt.Errorf("cannot convert %T to uint32", v)
		}
		
	case *arrow.Uint64Type:
		switch v := value.(type) {
		case float64:
			builder.(*array.Uint64Builder).Append(uint64(v))
		case uint64:
			builder.(*array.Uint64Builder).Append(v)
		default:
			return fmt.Errorf("cannot convert %T to uint64", v)
		}
		
	case *arrow.Float64Type:
		switch v := value.(type) {
		case float64:
			builder.(*array.Float64Builder).Append(v)
		case int:
			builder.(*array.Float64Builder).Append(float64(v))
		default:
			return fmt.Errorf("cannot convert %T to float64", v)
		}
		
	case *arrow.BooleanType:
		switch v := value.(type) {
		case bool:
			builder.(*array.BooleanBuilder).Append(v)
		default:
			return fmt.Errorf("cannot convert %T to bool", v)
		}
		
	case *arrow.TimestampType:
		// Handle timestamp conversion
		switch v := value.(type) {
		case string:
			// Parse timestamp string - try multiple formats
			var parsedTime time.Time
			var err error
			
			// Try common timestamp formats
			formats := []string{
				time.RFC3339,
				time.RFC3339Nano,
				"2006-01-02T15:04:05Z",
				"2006-01-02 15:04:05",
				"2006-01-02T15:04:05.999999Z",
			}
			
			for _, format := range formats {
				parsedTime, err = time.Parse(format, v)
				if err == nil {
					break
				}
			}
			
			if err != nil {
				// If all formats fail, append null and log warning
				builder.(*array.TimestampBuilder).AppendNull()
				log.Printf("Warning: Failed to parse timestamp string '%s': %v", v, err)
			} else {
				// Convert to microseconds since epoch
				microseconds := parsedTime.UnixNano() / 1000
				builder.(*array.TimestampBuilder).Append(arrow.Timestamp(microseconds))
			}
		case float64:
			// Unix timestamp
			builder.(*array.TimestampBuilder).Append(arrow.Timestamp(int64(v * 1e6))) // Convert to microseconds
		default:
			return fmt.Errorf("cannot convert %T to timestamp", v)
		}
		
	default:
		return fmt.Errorf("unsupported Arrow type: %s", dt)
	}
	
	return nil
}