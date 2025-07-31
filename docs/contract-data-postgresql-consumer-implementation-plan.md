# PostgreSQL Consumer for Contract Data - Implementation Plan

## Executive Summary

This document outlines the implementation plan for creating a configurable PostgreSQL consumer that works with contract data processors, providing selective field storage capabilities and flexible database schema configuration.

## Current State Analysis

### Existing Architecture

**Contract Data Processors:**
- `ContractInvocationProcessor`: Processes Soroban smart contract function invocations
- `ContractEventProcessor`: Extracts events emitted by smart contracts  
- `ContractDataProcessor`: Tracks contract storage state changes
- `SoroswapProcessor`: Specialized DEX processor for Soroswap events

**PostgreSQL Consumer Patterns:**
- Fixed schema consumers (e.g., `SaveToPostgreSQL`, `SaveContractEventsToPostgreSQL`)
- Configurable extraction consumers (e.g., `SaveExtractedContractInvocationsToPostgreSQL`)
- JSONB-based flexible storage consumers

**Data Flow:**
```
Source → Contract Processor → [New PostgreSQL Consumer] → PostgreSQL Database
```

### Key Requirements

1. **Configurable Field Selection**: Allow users to specify which fields to store
2. **Support Multiple Contract Data Types**: Handle contract invocations, events, and data changes
3. **Flexible Schema Management**: Dynamic table creation based on configuration
4. **Performance Optimization**: Efficient storage and querying patterns
5. **Backward Compatibility**: Work within existing pipeline architecture

## Proposed Architecture

### Core Components

1. **ConfigurableContractDataPostgreSQLConsumer**: Main consumer implementation
2. **FieldSelector**: Handles field extraction and transformation logic
3. **SchemaManager**: Manages dynamic table creation and updates
4. **ConfigurationParser**: Parses YAML-based field selection configuration

### Configuration-Driven Design

The consumer will use a YAML-based configuration system similar to the existing contract extraction schemas:

```yaml
consumer:
  type: SaveContractDataToPostgreSQL
  config:
    # Database connection
    database_url: "postgres://<>:<>@localhost:5432/stellar"
    
    # Table configuration
    table_name: "contract_data_archive"
    
    # Contract data type filter (optional)
    data_types: ["contract_invocation", "contract_event"]
    
    # Field selection schema
    fields:
      # Core tracking fields (always included)
      - name: "id"
        type: "SERIAL PRIMARY KEY"
        generated: true
      - name: "ledger_sequence"
        source_path: "ledger_sequence"
        type: "BIGINT"
        required: true
        index: true
      - name: "transaction_hash"
        source_path: "transaction_hash"
        type: "TEXT"
        required: true
        index: true
      - name: "created_at"
        type: "TIMESTAMP DEFAULT NOW()"
        generated: true
        
      # Contract-specific fields
      - name: "contract_id"
        source_path: "contract_id"
        type: "TEXT"
        required: true
        index: true
      - name: "function_name"
        source_path: "function_name"
        type: "TEXT"
        required: false
      - name: "success"
        source_path: "success"
        type: "BOOLEAN"
        required: false
        
      # Flexible data storage
      - name: "arguments_raw"
        source_path: "args_raw"
        type: "JSONB"
        required: false
        index: "gin"
      - name: "arguments_decoded"
        source_path: "args_decoded"
        type: "JSONB" 
        required: false
        index: "gin"
      - name: "diagnostic_events"
        source_path: "diagnostic_events"
        type: "JSONB"
        required: false
        
      # Archive metadata
      - name: "source_file"
        source_path: "archive_metadata.source_file"
        type: "TEXT"
        required: false
        
    # Index configuration
    indexes:
      - name: "idx_contract_ledger"
        columns: ["contract_id", "ledger_sequence"]
      - name: "idx_function_success"
        columns: ["function_name", "success"]
        condition: "function_name IS NOT NULL"
```

## Implementation Plan

### Phase 1: Core Infrastructure (Week 1)

**1.1 Create Base Consumer Structure**
- File: `consumer/save_contract_data_to_postgresql.go`
- Implement basic `Processor` interface
- Database connection management
- Configuration parsing

**1.2 Configuration System**
- File: `pkg/config/contract_data_schema.go`
- YAML schema definition structures
- Configuration validation
- Field type mapping

**1.3 Schema Manager**
- File: `pkg/database/schema_manager.go`
- Dynamic table creation
- Index management
- Schema migration support

### Phase 2: Field Selection Engine (Week 2)

**2.1 Field Selector Implementation**
- File: `pkg/processor/field_selector.go`
- JSON path extraction (using `gjson` library)
- Type conversion and validation
- Error handling for missing/invalid fields

**2.2 Data Transformation Pipeline**
- Transform contract data messages to configured schema
- Handle different contract data types (invocations, events, data changes)
- Support for computed/generated fields

**2.3 Testing Framework**
- Unit tests for field extraction
- Schema validation tests
- Configuration parsing tests

### Phase 3: Integration and Optimization (Week 3)

**3.1 Consumer Registration**
- Update `main.go` to register the new consumer
- Integration with existing processor chain
- Configuration file examples

**3.2 Performance Optimization**
- Batch insertion support
- Connection pooling optimization
- Query performance analysis

**3.3 Error Handling and Monitoring**
- Comprehensive error handling
- Logging and metrics
- Recovery mechanisms

### Phase 4: Documentation and Testing (Week 4)

**4.1 Documentation**
- Configuration reference guide
- Schema design best practices
- Performance tuning guide

**4.2 Integration Testing**
- End-to-end pipeline testing
- Performance benchmarking  
- Migration testing

**4.3 Example Configurations**
- Contract invocation archival
- Event-focused storage
- Minimal storage configuration

## Technical Design Details

### Field Selection Engine

**JSON Path Extraction:**
```go
type FieldConfig struct {
    Name       string `yaml:"name"`
    SourcePath string `yaml:"source_path"`
    Type       string `yaml:"type"`
    Required   bool   `yaml:"required"`
    Index      string `yaml:"index,omitempty"`
    Generated  bool   `yaml:"generated,omitempty"`
}

func (fs *FieldSelector) ExtractField(data []byte, config FieldConfig) (interface{}, error) {
    if config.Generated {
        return nil, nil // Skip generated fields
    }
    
    result := gjson.GetBytes(data, config.SourcePath)
    if !result.Exists() && config.Required {
        return nil, fmt.Errorf("required field %s not found", config.Name)
    }
    
    return fs.convertType(result.Value(), config.Type)
}
```

**Schema Generation:**
```go
func (sm *SchemaManager) GenerateCreateTableSQL(tableName string, fields []FieldConfig) string {
    var columns []string
    
    for _, field := range fields {
        column := fmt.Sprintf("%s %s", field.Name, field.Type)
        if field.Required && !field.Generated {
            column += " NOT NULL"
        }
        columns = append(columns, column)
    }
    
    return fmt.Sprintf("CREATE TABLE IF NOT EXISTS %s (%s)", 
        tableName, strings.Join(columns, ", "))
}
```

### Database Schema Design

**Core Tables Structure:**
```sql
-- Example generated table
CREATE TABLE contract_data_archive (
    id SERIAL PRIMARY KEY,
    ledger_sequence BIGINT NOT NULL,
    transaction_hash TEXT NOT NULL,
    contract_id TEXT NOT NULL,
    function_name TEXT,
    success BOOLEAN,
    arguments_raw JSONB,
    arguments_decoded JSONB,
    diagnostic_events JSONB,
    source_file TEXT,
    created_at TIMESTAMP DEFAULT NOW()
);

-- Indexes
CREATE INDEX idx_contract_ledger ON contract_data_archive (contract_id, ledger_sequence);
CREATE INDEX idx_function_success ON contract_data_archive (function_name, success) 
    WHERE function_name IS NOT NULL;
CREATE INDEX idx_args_gin ON contract_data_archive USING GIN (arguments_decoded);
```

### Configuration Validation

**Validation Rules:**
1. Required fields must have valid `source_path`
2. Database types must be supported PostgreSQL types
3. Index types must be valid (btree, gin, gist, etc.)
4. Table name must be valid identifier
5. Field names must be unique within table

**Supported Data Types:**
- `TEXT`, `VARCHAR(n)`: String data
- `INTEGER`, `BIGINT`: Numeric data
- `BOOLEAN`: Boolean values
- `TIMESTAMP`, `DATE`: Time-based data
- `JSONB`: Flexible JSON storage
- `SERIAL`, `BIGSERIAL`: Auto-incrementing IDs

## Testing Strategy

### Unit Tests
- Configuration parsing and validation
- Field extraction from various contract data types
- Schema generation SQL correctness
- Type conversion accuracy

### Integration Tests
- End-to-end pipeline with real contract data
- Database schema creation and modification
- Performance under load
- Error handling scenarios

### Performance Tests
- Bulk insertion benchmarks
- Query performance with various indexes
- Memory usage analysis
- Connection pool efficiency

## Rollout Plan

### Development Environment
1. Implement core consumer functionality
2. Create comprehensive test suite
3. Performance optimization and tuning

### Staging Environment
1. Deploy with real testnet contract data
2. Monitor performance and stability
3. Validate configuration flexibility

### Production Environment
1. Gradual rollout with monitoring
2. Performance metrics collection
3. Documentation and training

## Risk Mitigation

**Technical Risks:**
- **Complex Configuration**: Mitigate with comprehensive validation and clear error messages
- **Performance Impact**: Address with batch processing and optimized indexing
- **Schema Evolution**: Handle with migration support and versioning

**Operational Risks:**
- **Data Loss**: Implement transaction-based processing and rollback capabilities
- **Configuration Errors**: Provide validation tools and comprehensive examples
- **Scaling Issues**: Design with horizontal scaling and partitioning considerations

## Success Metrics

1. **Functionality**: Successfully process and store contract data with configurable fields
2. **Performance**: < 100ms average processing time per message
3. **Flexibility**: Support for 10+ different field configurations
4. **Reliability**: 99.9% uptime with proper error handling
5. **Usability**: Clear documentation and configuration examples

## Future Enhancements

1. **Schema Versioning**: Support for schema evolution and migrations
2. **Multiple Table Support**: Allow single consumer to populate multiple tables
3. **Data Transformation**: Built-in functions for common data transformations
4. **Real-time Analytics**: Integration with time-series databases
5. **GraphQL API**: Auto-generated API based on configured schema

---

**Document Version**: 1.0  
**Created**: 2025-07-23  
**Last Updated**: 2025-07-23  
**Status**: Draft - Ready for Implementation