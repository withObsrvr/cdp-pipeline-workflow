package consumer

import (
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
	processor "github.com/withObsrvr/cdp-pipeline-workflow/processor"
)

func TestParquetSchemaRegistry(t *testing.T) {
	t.Run("create registry", func(t *testing.T) {
		registry := NewParquetSchemaRegistry()
		if registry == nil {
			t.Fatal("NewParquetSchemaRegistry() returned nil")
		}
		if registry.allocator == nil {
			t.Error("Registry allocator is nil")
		}
		if registry.schemas == nil {
			t.Error("Registry schemas map is nil")
		}
	})
	
	t.Run("get and set schema", func(t *testing.T) {
		registry := NewParquetSchemaRegistry()
		
		// Create a test schema
		fields := []arrow.Field{
			{Name: "id", Type: arrow.BinaryTypes.String, Nullable: false},
			{Name: "value", Type: arrow.PrimitiveTypes.Int64, Nullable: true},
		}
		schema := arrow.NewSchema(fields, nil)
		
		// Set schema
		registry.SetSchema("test", schema)
		
		// Get schema
		retrieved, ok := registry.GetSchema("test")
		if !ok {
			t.Error("Failed to retrieve schema")
		}
		if retrieved == nil {
			t.Error("Retrieved schema is nil")
		}
		if len(retrieved.Fields()) != 2 {
			t.Errorf("Schema has %d fields, expected 2", len(retrieved.Fields()))
		}
	})
}

func TestSchemaInference(t *testing.T) {
	registry := NewParquetSchemaRegistry()
	
	t.Run("contract data inference", func(t *testing.T) {
		messages := []processor.Message{
			{
				Payload: map[string]interface{}{
					"contract_id":        "CDLZFC3SYJYDZT7K67VZ75HPJVIEUVNIXF47ZG2FB2RMQQVU2HHGCYSC",
					"contract_key_type":  "ContractData",
					"contract_durability": "persistent",
					"asset_code":        "USDC",
					"asset_issuer":      "GA5ZSEJYB37JRC5AVCIA5MOP4RHTM335X2KGX3IHOJAPP5RE34K4KZVN",
					"deleted":           false,
					"ledger_sequence":   uint32(123456),
				},
			},
		}
		
		schema, err := registry.InferSchema(messages)
		if err != nil {
			t.Fatalf("InferSchema() error = %v", err)
		}
		
		if schema == nil {
			t.Fatal("InferSchema() returned nil schema")
		}
		
		// Check for key fields
		hasContractID := false
		hasLedgerSeq := false
		for _, field := range schema.Fields() {
			if field.Name == "contract_id" {
				hasContractID = true
			}
			if field.Name == "ledger_sequence" {
				hasLedgerSeq = true
			}
		}
		
		if !hasContractID {
			t.Error("Schema missing contract_id field")
		}
		if !hasLedgerSeq {
			t.Error("Schema missing ledger_sequence field")
		}
	})
	
	t.Run("event inference", func(t *testing.T) {
		messages := []processor.Message{
			{
				Payload: map[string]interface{}{
					"type":                        "contract",
					"ledger":                      uint64(123456),
					"ledger_closed_at":            "2024-01-01T00:00:00Z",
					"contract_id":                 "CDLZFC3SYJYDZT7K67VZ75HPJVIEUVNIXF47ZG2FB2RMQQVU2HHGCYSC",
					"id":                          "event-123",
					"in_successful_contract_call": true,
					"tx_hash":                     "abc123",
				},
			},
		}
		
		schema, err := registry.InferSchema(messages)
		if err != nil {
			t.Fatalf("InferSchema() error = %v", err)
		}
		
		if schema == nil {
			t.Fatal("InferSchema() returned nil schema")
		}
		
		// Check schema metadata
		metadata := schema.Metadata()
		if metadata.FindKey("schema_type") != -1 {
			schemaType := metadata.Values()[metadata.FindKey("schema_type")]
			if schemaType != "event" {
				t.Errorf("Schema type = %s, expected event", schemaType)
			}
		}
	})
	
	t.Run("generic map inference", func(t *testing.T) {
		messages := []processor.Message{
			{
				Payload: map[string]interface{}{
					"string_field": "hello",
					"int_field":    float64(42),
					"bool_field":   true,
					"null_field":   nil,
				},
			},
		}
		
		schema, err := registry.InferSchema(messages)
		if err != nil {
			t.Fatalf("InferSchema() error = %v", err)
		}
		
		if schema == nil {
			t.Fatal("InferSchema() returned nil schema")
		}
		
		if len(schema.Fields()) != 4 {
			t.Errorf("Schema has %d fields, expected 4", len(schema.Fields()))
		}
		
		// Check field types
		for _, field := range schema.Fields() {
			switch field.Name {
			case "string_field":
				if field.Type != arrow.BinaryTypes.String {
					t.Errorf("string_field type = %v, expected String", field.Type)
				}
			case "int_field":
				if field.Type != arrow.PrimitiveTypes.Int64 {
					t.Errorf("int_field type = %v, expected Int64", field.Type)
				}
			case "bool_field":
				if field.Type != arrow.FixedWidthTypes.Boolean {
					t.Errorf("bool_field type = %v, expected Boolean", field.Type)
				}
			}
		}
	})
	
	t.Run("empty messages", func(t *testing.T) {
		messages := []processor.Message{}
		
		_, err := registry.InferSchema(messages)
		if err == nil {
			t.Error("InferSchema() with empty messages should return error")
		}
	})
}

func TestPredefinedSchemas(t *testing.T) {
	registry := NewParquetSchemaRegistry()
	
	t.Run("contract data schema", func(t *testing.T) {
		schema := registry.getContractDataSchema()
		if schema == nil {
			t.Fatal("getContractDataSchema() returned nil")
		}
		
		expectedFields := []string{
			"contract_id", "contract_key_type", "contract_durability",
			"asset_code", "asset_issuer", "asset_type",
			"balance_holder", "balance", "last_modified_ledger",
			"ledger_entry_change", "deleted", "closed_at",
			"ledger_sequence", "ledger_key_hash",
		}
		
		if len(schema.Fields()) != len(expectedFields) {
			t.Errorf("Schema has %d fields, expected %d", len(schema.Fields()), len(expectedFields))
		}
		
		// Check field names
		fieldMap := make(map[string]bool)
		for _, field := range schema.Fields() {
			fieldMap[field.Name] = true
		}
		
		for _, expected := range expectedFields {
			if !fieldMap[expected] {
				t.Errorf("Schema missing field: %s", expected)
			}
		}
		
		// Check caching
		schema2 := registry.getContractDataSchema()
		if schema != schema2 {
			t.Error("Schema not properly cached")
		}
	})
	
	t.Run("event schema", func(t *testing.T) {
		schema := registry.getEventSchema()
		if schema == nil {
			t.Fatal("getEventSchema() returned nil")
		}
		
		// Check for timestamp field
		var hasLedger bool
		for _, field := range schema.Fields() {
			if field.Name == "ledger" {
				hasLedger = true
				if field.Type != arrow.PrimitiveTypes.Uint64 {
					t.Errorf("ledger field type = %v, expected Uint64", field.Type)
				}
			}
		}
		
		if !hasLedger {
			t.Error("Event schema missing ledger field")
		}
	})
	
	t.Run("asset details schema", func(t *testing.T) {
		schema := registry.getAssetDetailsSchema()
		if schema == nil {
			t.Fatal("getAssetDetailsSchema() returned nil")
		}
		
		// Check timestamp field type
		var timestampField arrow.Field
		found := false
		for _, field := range schema.Fields() {
			if field.Name == "timestamp" {
				timestampField = field
				found = true
				break
			}
		}
		
		if !found {
			t.Fatal("Asset details schema missing timestamp field")
		}
		
		if _, ok := timestampField.Type.(*arrow.TimestampType); !ok {
			t.Errorf("timestamp field type = %v, expected TimestampType", timestampField.Type)
		}
	})
}

func TestBuildRecordBatch(t *testing.T) {
	registry := NewParquetSchemaRegistry()
	
	t.Run("simple record batch", func(t *testing.T) {
		// Create schema
		fields := []arrow.Field{
			{Name: "id", Type: arrow.BinaryTypes.String, Nullable: false},
			{Name: "value", Type: arrow.PrimitiveTypes.Int64, Nullable: true},
			{Name: "active", Type: arrow.FixedWidthTypes.Boolean, Nullable: false},
		}
		schema := arrow.NewSchema(fields, nil)
		
		// Create messages
		messages := []processor.Message{
			{
				Payload: map[string]interface{}{
					"id":     "test1",
					"value":  float64(100),
					"active": true,
				},
			},
			{
				Payload: map[string]interface{}{
					"id":     "test2",
					"value":  float64(200),
					"active": false,
				},
			},
			{
				Payload: map[string]interface{}{
					"id":     "test3",
					"value":  nil,
					"active": true,
				},
			},
		}
		
		// Build record batch
		record, err := registry.BuildRecordBatch(schema, messages)
		if err != nil {
			t.Fatalf("BuildRecordBatch() error = %v", err)
		}
		defer record.Release()
		
		if record == nil {
			t.Fatal("BuildRecordBatch() returned nil")
		}
		
		if record.NumRows() != 3 {
			t.Errorf("Record has %d rows, expected 3", record.NumRows())
		}
		
		if record.NumCols() != 3 {
			t.Errorf("Record has %d columns, expected 3", record.NumCols())
		}
	})
	
	t.Run("empty messages", func(t *testing.T) {
		fields := []arrow.Field{
			{Name: "id", Type: arrow.BinaryTypes.String, Nullable: false},
		}
		schema := arrow.NewSchema(fields, nil)
		
		messages := []processor.Message{}
		
		_, err := registry.BuildRecordBatch(schema, messages)
		if err == nil {
			t.Error("BuildRecordBatch() with empty messages should return error")
		}
	})
}