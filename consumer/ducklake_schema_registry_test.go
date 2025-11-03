package consumer

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/withObsrvr/cdp-pipeline-workflow/processor"
)

// TestNewDuckLakeSchemaRegistry tests registry initialization
func TestNewDuckLakeSchemaRegistry(t *testing.T) {
	registry := NewDuckLakeSchemaRegistry()

	require.NotNil(t, registry, "Registry should not be nil")
	require.NotNil(t, registry.schemas, "Schemas map should not be nil")

	// Verify that schemas were registered
	assert.Greater(t, len(registry.schemas), 0, "Should have at least one schema registered")
}

// TestGetSchema_ContractInvocation tests schema retrieval for contract invocations
func TestGetSchema_ContractInvocation(t *testing.T) {
	registry := NewDuckLakeSchemaRegistry()

	schema, ok := registry.GetSchema(processor.ProcessorTypeContractInvocation)

	require.True(t, ok, "Should find contract invocation schema")
	assert.Equal(t, processor.ProcessorTypeContractInvocation, schema.ProcessorType)
	assert.NotEmpty(t, schema.CreateSQL, "CreateSQL should not be empty")
	assert.NotEmpty(t, schema.InsertSQL, "InsertSQL should not be empty")
	assert.NotEmpty(t, schema.UpsertSQL, "UpsertSQL should not be empty")
	assert.Greater(t, len(schema.Indexes), 0, "Should have indexes defined")
	assert.Greater(t, len(schema.ColumnMapping), 0, "Should have column mapping defined")
}

// TestGetSchema_ContractEvent tests schema retrieval for contract events
func TestGetSchema_ContractEvent(t *testing.T) {
	registry := NewDuckLakeSchemaRegistry()

	schema, ok := registry.GetSchema(processor.ProcessorTypeContractEvent)

	require.True(t, ok, "Should find contract event schema")
	assert.Equal(t, processor.ProcessorTypeContractEvent, schema.ProcessorType)
	assert.NotEmpty(t, schema.CreateSQL, "CreateSQL should not be empty")
	assert.NotEmpty(t, schema.InsertSQL, "InsertSQL should not be empty")
	assert.NotEmpty(t, schema.UpsertSQL, "UpsertSQL should not be empty")
	assert.Greater(t, len(schema.Indexes), 0, "Should have indexes defined")
	assert.Greater(t, len(schema.ColumnMapping), 0, "Should have column mapping defined")
}

// TestGetSchema_UnknownType tests schema retrieval for unknown processor type
func TestGetSchema_UnknownType(t *testing.T) {
	registry := NewDuckLakeSchemaRegistry()

	_, ok := registry.GetSchema(processor.ProcessorType("unknown_type"))

	assert.False(t, ok, "Unknown processor type should not be found")
}

// TestGetAllProcessorTypes tests getting all registered processor types
func TestGetAllProcessorTypes(t *testing.T) {
	registry := NewDuckLakeSchemaRegistry()

	types := registry.GetAllProcessorTypes()

	require.NotNil(t, types)
	assert.Greater(t, len(types), 0, "Should have at least one processor type")

	// Verify the types we know are registered
	foundInvocation := false
	foundEvent := false
	for _, pType := range types {
		if pType == processor.ProcessorTypeContractInvocation {
			foundInvocation = true
		}
		if pType == processor.ProcessorTypeContractEvent {
			foundEvent = true
		}
	}
	assert.True(t, foundInvocation, "Should include contract invocation type")
	assert.True(t, foundEvent, "Should include contract event type")
}

// TestHasSchema tests checking schema existence
func TestHasSchema(t *testing.T) {
	registry := NewDuckLakeSchemaRegistry()

	// Should have contract invocation schema
	assert.True(t, registry.HasSchema(processor.ProcessorTypeContractInvocation))

	// Should have contract event schema
	assert.True(t, registry.HasSchema(processor.ProcessorTypeContractEvent))

	// Should not have unknown schema
	assert.False(t, registry.HasSchema(processor.ProcessorType("unknown")))
}

// TestProcessorType_IsValid tests processor type validation
func TestProcessorType_IsValid(t *testing.T) {
	testCases := []struct {
		name     string
		pType    processor.ProcessorType
		expected bool
	}{
		{
			name:     "contract_invocation is valid",
			pType:    processor.ProcessorTypeContractInvocation,
			expected: true,
		},
		{
			name:     "contract_event is valid",
			pType:    processor.ProcessorTypeContractEvent,
			expected: true,
		},
		{
			name:     "ledger is valid",
			pType:    processor.ProcessorTypeLedger,
			expected: true,
		},
		{
			name:     "payment is valid",
			pType:    processor.ProcessorTypePayment,
			expected: true,
		},
		{
			name:     "unknown type is invalid",
			pType:    processor.ProcessorType("unknown"),
			expected: false,
		},
		{
			name:     "empty string is invalid",
			pType:    processor.ProcessorType(""),
			expected: false,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			assert.Equal(t, tc.expected, tc.pType.IsValid(), "IsValid() should return %v for %s", tc.expected, tc.pType)
		})
	}
}

// TestSchemaDefinition_CreateSQL tests that CreateSQL has PRIMARY KEY
func TestSchemaDefinition_CreateSQL(t *testing.T) {
	registry := NewDuckLakeSchemaRegistry()

	testCases := []struct {
		pType           processor.ProcessorType
		expectedPKCols  []string
	}{
		{
			pType:          processor.ProcessorTypeContractInvocation,
			expectedPKCols: []string{"ledger_sequence", "transaction_hash", "operation_index"},
		},
		{
			pType:          processor.ProcessorTypeContractEvent,
			expectedPKCols: []string{"ledger_sequence", "transaction_hash", "event_index"},
		},
	}

	for _, tc := range testCases {
		t.Run(string(tc.pType), func(t *testing.T) {
			schema, ok := registry.GetSchema(tc.pType)
			require.True(t, ok)

			// Verify PRIMARY KEY clause exists
			assert.Contains(t, schema.CreateSQL, "PRIMARY KEY", "CreateSQL should contain PRIMARY KEY clause")

			// Verify expected columns are mentioned in PRIMARY KEY
			for _, col := range tc.expectedPKCols {
				assert.Contains(t, schema.CreateSQL, col, "PRIMARY KEY should reference column %s", col)
			}

			// Verify PARTITION BY RANGE clause exists
			assert.Contains(t, schema.CreateSQL, "PARTITION BY RANGE", "CreateSQL should contain PARTITION BY RANGE clause")
		})
	}
}

// TestSchemaDefinition_UpsertSQL tests that UpsertSQL has MERGE logic
func TestSchemaDefinition_UpsertSQL(t *testing.T) {
	registry := NewDuckLakeSchemaRegistry()

	schemas := []processor.ProcessorType{
		processor.ProcessorTypeContractInvocation,
		processor.ProcessorTypeContractEvent,
	}

	for _, pType := range schemas {
		t.Run(string(pType), func(t *testing.T) {
			schema, ok := registry.GetSchema(pType)
			require.True(t, ok)

			// Verify MERGE/UPSERT structure
			assert.Contains(t, schema.UpsertSQL, "MERGE INTO", "UpsertSQL should use MERGE INTO")
			assert.Contains(t, schema.UpsertSQL, "WHEN NOT MATCHED THEN", "UpsertSQL should have WHEN NOT MATCHED clause")
			assert.Contains(t, schema.UpsertSQL, "WHEN MATCHED THEN", "UpsertSQL should have WHEN MATCHED clause")
			assert.Contains(t, schema.UpsertSQL, "INSERT", "UpsertSQL should have INSERT clause")
			assert.Contains(t, schema.UpsertSQL, "UPDATE SET", "UpsertSQL should have UPDATE SET clause")
		})
	}
}

// TestSchemaDefinition_Indexes tests index definitions
func TestSchemaDefinition_Indexes(t *testing.T) {
	registry := NewDuckLakeSchemaRegistry()

	testCases := []struct {
		pType                 processor.ProcessorType
		expectedIndexPrefixes []string
	}{
		{
			pType: processor.ProcessorTypeContractInvocation,
			expectedIndexPrefixes: []string{
				"idx_contract_invocations_contract_id",
				"idx_contract_invocations_function",
				"idx_contract_invocations_time",
				"idx_contract_invocations_account",
			},
		},
		{
			pType: processor.ProcessorTypeContractEvent,
			expectedIndexPrefixes: []string{
				"idx_contract_events_contract_id",
				"idx_contract_events_type",
				"idx_contract_events_time",
			},
		},
	}

	for _, tc := range testCases {
		t.Run(string(tc.pType), func(t *testing.T) {
			schema, ok := registry.GetSchema(tc.pType)
			require.True(t, ok)

			// Collect all index names
			indexNames := make(map[string]bool)
			for _, idx := range schema.Indexes {
				indexNames[idx.Name] = true

				// Verify each index has required fields
				assert.NotEmpty(t, idx.Name, "Index name should not be empty")
				assert.Greater(t, len(idx.Columns), 0, "Index should have at least one column")
			}

			// Verify expected indexes exist
			for _, expectedPrefix := range tc.expectedIndexPrefixes {
				found := false
				for idxName := range indexNames {
					if strings.HasPrefix(idxName, expectedPrefix) || idxName == expectedPrefix {
						found = true
						break
					}
				}
				assert.True(t, found, "Should have index matching %s", expectedPrefix)
			}
		})
	}
}

// TestSchemaDefinition_ColumnMapping tests column mapping
func TestSchemaDefinition_ColumnMapping(t *testing.T) {
	registry := NewDuckLakeSchemaRegistry()

	testCases := []struct {
		pType            processor.ProcessorType
		expectedColumns  []string
	}{
		{
			pType: processor.ProcessorTypeContractInvocation,
			expectedColumns: []string{
				"ledger_sequence",
				"transaction_hash",
				"contract_id",
				"function_name",
			},
		},
		{
			pType: processor.ProcessorTypeContractEvent,
			expectedColumns: []string{
				"ledger_sequence",
				"transaction_hash",
				"event_type",
				"contract_id",
			},
		},
	}

	for _, tc := range testCases {
		t.Run(string(tc.pType), func(t *testing.T) {
			schema, ok := registry.GetSchema(tc.pType)
			require.True(t, ok)

			// Verify expected columns are in mapping
			for _, col := range tc.expectedColumns {
				colInfo, exists := schema.ColumnMapping[col]
				assert.True(t, exists, "Column %s should exist in mapping", col)
				if exists {
					assert.NotEmpty(t, colInfo.SQLColumn, "SQLColumn should not be empty")
					assert.NotEmpty(t, colInfo.JSONPath, "JSONPath should not be empty")
					assert.NotEmpty(t, colInfo.DataType, "DataType should not be empty")
				}
			}
		})
	}
}

// TestSchemaRegistry_ThreadSafety tests concurrent access to registry
func TestSchemaRegistry_ThreadSafety(t *testing.T) {
	registry := NewDuckLakeSchemaRegistry()

	// Launch multiple goroutines accessing the registry concurrently
	done := make(chan bool)

	for i := 0; i < 10; i++ {
		go func() {
			for j := 0; j < 100; j++ {
				// Alternate between contract invocations and events
				if j%2 == 0 {
					_, ok := registry.GetSchema(processor.ProcessorTypeContractInvocation)
					assert.True(t, ok)
				} else {
					_, ok := registry.GetSchema(processor.ProcessorTypeContractEvent)
					assert.True(t, ok)
				}

				// Also test HasSchema
				registry.HasSchema(processor.ProcessorTypeContractInvocation)

				// And GetAllProcessorTypes
				registry.GetAllProcessorTypes()
			}
			done <- true
		}()
	}

	// Wait for all goroutines to complete
	for i := 0; i < 10; i++ {
		<-done
	}
}

// TestSchemaDefinition_Versioning tests version fields
func TestSchemaDefinition_Versioning(t *testing.T) {
	registry := NewDuckLakeSchemaRegistry()

	schemas := []processor.ProcessorType{
		processor.ProcessorTypeContractInvocation,
		processor.ProcessorTypeContractEvent,
	}

	for _, pType := range schemas {
		t.Run(string(pType), func(t *testing.T) {
			schema, ok := registry.GetSchema(pType)
			require.True(t, ok)

			assert.NotEmpty(t, schema.Version, "Version should not be empty")
			assert.NotEmpty(t, schema.MinVersion, "MinVersion should not be empty")
			assert.NotEmpty(t, schema.Description, "Description should not be empty")
		})
	}
}

// TestValidateSchema tests schema validation
func TestValidateSchema(t *testing.T) {
	registry := NewDuckLakeSchemaRegistry()

	testCases := []struct {
		name        string
		schema      SchemaDefinition
		expectError bool
	}{
		{
			name: "valid schema",
			schema: SchemaDefinition{
				ProcessorType: processor.ProcessorTypeContractInvocation,
				Version:       "1.0.0",
				CreateSQL:     "CREATE TABLE...",
			},
			expectError: false,
		},
		{
			name: "empty processor type",
			schema: SchemaDefinition{
				ProcessorType: "",
				Version:       "1.0.0",
				CreateSQL:     "CREATE TABLE...",
			},
			expectError: true,
		},
		{
			name: "invalid processor type",
			schema: SchemaDefinition{
				ProcessorType: processor.ProcessorType("invalid"),
				Version:       "1.0.0",
				CreateSQL:     "CREATE TABLE...",
			},
			expectError: true,
		},
		{
			name: "empty CreateSQL",
			schema: SchemaDefinition{
				ProcessorType: processor.ProcessorTypeContractInvocation,
				Version:       "1.0.0",
				CreateSQL:     "",
			},
			expectError: true,
		},
		{
			name: "empty version",
			schema: SchemaDefinition{
				ProcessorType: processor.ProcessorTypeContractInvocation,
				Version:       "",
				CreateSQL:     "CREATE TABLE...",
			},
			expectError: true,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			err := registry.ValidateSchema(tc.schema)
			if tc.expectError {
				assert.Error(t, err, "Expected validation error")
			} else {
				assert.NoError(t, err, "Expected no validation error")
			}
		})
	}
}

// TestIndexDefinition_NoDuplicates tests that there are no duplicate index names
func TestIndexDefinition_NoDuplicates(t *testing.T) {
	registry := NewDuckLakeSchemaRegistry()

	schemas := []processor.ProcessorType{
		processor.ProcessorTypeContractInvocation,
		processor.ProcessorTypeContractEvent,
	}

	for _, pType := range schemas {
		t.Run(string(pType), func(t *testing.T) {
			schema, ok := registry.GetSchema(pType)
			require.True(t, ok)

			// Track index names
			indexNames := make(map[string]bool)

			for _, idx := range schema.Indexes {
				assert.False(t, indexNames[idx.Name], "Duplicate index name found: %s", idx.Name)
				indexNames[idx.Name] = true
			}
		})
	}
}

// TestColumnDefinition_NoDuplicates tests that there are no duplicate column names
func TestColumnDefinition_NoDuplicates(t *testing.T) {
	registry := NewDuckLakeSchemaRegistry()

	schemas := []processor.ProcessorType{
		processor.ProcessorTypeContractInvocation,
		processor.ProcessorTypeContractEvent,
	}

	for _, pType := range schemas {
		t.Run(string(pType), func(t *testing.T) {
			schema, ok := registry.GetSchema(pType)
			require.True(t, ok)

			// Track SQL column names (keys in ColumnMapping are already unique by map definition)
			sqlColumns := make(map[string]bool)

			for _, colInfo := range schema.ColumnMapping {
				assert.False(t, sqlColumns[colInfo.SQLColumn], "Duplicate SQL column name found: %s", colInfo.SQLColumn)
				sqlColumns[colInfo.SQLColumn] = true
			}
		})
	}
}
