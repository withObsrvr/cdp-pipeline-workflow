package consumer

import (
	"fmt"
	"strings"
)

// SchemaManager handles database schema creation and management
type SchemaManager struct {
	// Add any state needed for schema management
}

// NewSchemaManager creates a new schema manager
func NewSchemaManager() *SchemaManager {
	return &SchemaManager{}
}

// GenerateCreateTableSQL generates a CREATE TABLE statement from field configuration
func (sm *SchemaManager) GenerateCreateTableSQL(tableName string, fields []FieldConfig) string {
	var columns []string
	
	for _, field := range fields {
		column := sm.generateColumnDefinition(field)
		columns = append(columns, column)
	}
	
	return fmt.Sprintf("CREATE TABLE IF NOT EXISTS %s (%s)", 
		tableName, strings.Join(columns, ",\n    "))
}

// generateColumnDefinition creates a column definition from field configuration
func (sm *SchemaManager) generateColumnDefinition(field FieldConfig) string {
	var parts []string
	
	// Column name and type
	parts = append(parts, fmt.Sprintf("%s %s", field.Name, field.Type))
	
	// Add constraints
	if field.Required && !field.Generated {
		parts = append(parts, "NOT NULL")
	}
	
	// Add default value if specified
	if field.Default != "" && !field.Generated {
		parts = append(parts, fmt.Sprintf("DEFAULT %s", sm.formatDefaultValue(field.Default, field.Type)))
	}
	
	return strings.Join(parts, " ")
}

// formatDefaultValue properly formats a default value for SQL
func (sm *SchemaManager) formatDefaultValue(value, fieldType string) string {
	// Normalize type
	normalizedType := strings.ToUpper(strings.Split(fieldType, "(")[0])
	
	switch normalizedType {
	case "TEXT", "VARCHAR", "CHAR", "JSON", "JSONB":
		// String types need quotes
		return fmt.Sprintf("'%s'", strings.ReplaceAll(value, "'", "''"))
	case "TIMESTAMP", "TIMESTAMPTZ", "DATE":
		// Time types - check for special values
		if strings.ToUpper(value) == "NOW()" || strings.ToUpper(value) == "CURRENT_TIMESTAMP" {
			return strings.ToUpper(value)
		}
		// Otherwise quote as string
		return fmt.Sprintf("'%s'", value)
	case "BOOLEAN", "BOOL":
		// Boolean values
		if strings.ToLower(value) == "true" || value == "1" {
			return "TRUE"
		}
		return "FALSE"
	default:
		// Numeric types and others don't need quotes
		return value
	}
}

// GenerateCreateIndexSQL generates CREATE INDEX statements from index configuration
func (sm *SchemaManager) GenerateCreateIndexSQL(tableName string, indexConfig IndexConfig) string {
	var parts []string
	
	// Base CREATE INDEX statement
	indexType := "BTREE" // Default index type
	if indexConfig.Type != "" {
		indexType = strings.ToUpper(indexConfig.Type)
	}
	
	parts = append(parts, "CREATE INDEX IF NOT EXISTS")
	parts = append(parts, indexConfig.Name)
	parts = append(parts, "ON")
	parts = append(parts, tableName)
	
	// Add USING clause for non-BTREE indexes
	if indexType != "BTREE" {
		parts = append(parts, fmt.Sprintf("USING %s", indexType))
	}
	
	// Add column list
	parts = append(parts, fmt.Sprintf("(%s)", strings.Join(indexConfig.Columns, ", ")))
	
	// Add WHERE condition if specified
	if indexConfig.Condition != "" {
		parts = append(parts, fmt.Sprintf("WHERE %s", indexConfig.Condition))
	}
	
	return strings.Join(parts, " ")
}

// ValidateTableName validates that a table name is safe to use
func (sm *SchemaManager) ValidateTableName(tableName string) error {
	if tableName == "" {
		return fmt.Errorf("table name cannot be empty")
	}
	
	// Check for basic SQL injection patterns
	dangerous := []string{";", "--", "/*", "*/", "DROP", "DELETE", "UPDATE", "INSERT"}
	upperTableName := strings.ToUpper(tableName)
	
	for _, pattern := range dangerous {
		if strings.Contains(upperTableName, pattern) {
			return fmt.Errorf("table name contains potentially dangerous pattern: %s", pattern)
		}
	}
	
	// Check that it starts with a letter or underscore
	if len(tableName) > 0 {
		first := tableName[0]
		if !((first >= 'a' && first <= 'z') || (first >= 'A' && first <= 'Z') || first == '_') {
			return fmt.Errorf("table name must start with a letter or underscore")
		}
	}
	
	// Check for valid characters (letters, numbers, underscores)
	for _, char := range tableName {
		if !((char >= 'a' && char <= 'z') || (char >= 'A' && char <= 'Z') || (char >= '0' && char <= '9') || char == '_') {
			return fmt.Errorf("table name contains invalid character: %c", char)
		}
	}
	
	return nil
}

// ValidateFieldName validates that a field name is safe to use
func (sm *SchemaManager) ValidateFieldName(fieldName string) error {
	if fieldName == "" {
		return fmt.Errorf("field name cannot be empty")
	}
	
	// Check against PostgreSQL reserved words (common ones)
	reservedWords := map[string]bool{
		"SELECT": true, "FROM": true, "WHERE": true, "INSERT": true, "UPDATE": true,
		"DELETE": true, "CREATE": true, "DROP": true, "ALTER": true, "TABLE": true,
		"INDEX": true, "PRIMARY": true, "KEY": true, "FOREIGN": true, "UNIQUE": true,
		"NOT": true, "NULL": true, "DEFAULT": true, "CHECK": true, "CONSTRAINT": true,
		"USER": true, "ORDER": true, "GROUP": true, "BY": true, "HAVING": true,
		"LIMIT": true, "OFFSET": true, "UNION": true, "JOIN": true, "ON": true,
		"LEFT": true, "RIGHT": true, "INNER": true, "OUTER": true, "FULL": true,
	}
	
	upperFieldName := strings.ToUpper(fieldName)
	if reservedWords[upperFieldName] {
		return fmt.Errorf("field name '%s' is a reserved word", fieldName)
	}
	
	// Check that it starts with a letter or underscore  
	if len(fieldName) > 0 {
		first := fieldName[0]
		if !((first >= 'a' && first <= 'z') || (first >= 'A' && first <= 'Z') || first == '_') {
			return fmt.Errorf("field name must start with a letter or underscore")
		}
	}
	
	// Check for valid characters
	for _, char := range fieldName {
		if !((char >= 'a' && char <= 'z') || (char >= 'A' && char <= 'Z') || (char >= '0' && char <= '9') || char == '_') {
			return fmt.Errorf("field name contains invalid character: %c", char)
		}
	}
	
	return nil
}

// ValidateIndexName validates that an index name is safe to use
func (sm *SchemaManager) ValidateIndexName(indexName string) error {
	// Index names follow similar rules to table names
	return sm.ValidateTableName(indexName)
}

// GetSupportedIndexTypes returns a list of supported index types
func (sm *SchemaManager) GetSupportedIndexTypes() []string {
	return []string{
		"BTREE",   // Default, good for equality and range queries
		"HASH",    // Good for equality queries only
		"GIN",     // Good for JSONB, arrays, full-text search
		"GIST",    // Good for geometric data, full-text search
		"SPGIST",  // Good for non-balanced data structures
		"BRIN",    // Good for very large tables with natural ordering
	}
}

// ValidateIndexType validates that an index type is supported
func (sm *SchemaManager) ValidateIndexType(indexType string) error {
	if indexType == "" {
		return nil // Default is BTREE
	}
	
	supported := sm.GetSupportedIndexTypes()
	upperType := strings.ToUpper(indexType)
	
	for _, supportedType := range supported {
		if upperType == supportedType {
			return nil
		}
	}
	
	return fmt.Errorf("unsupported index type: %s. Supported types: %s", 
		indexType, strings.Join(supported, ", "))
}

// GenerateDropTableSQL generates a DROP TABLE statement
func (sm *SchemaManager) GenerateDropTableSQL(tableName string) string {
	return fmt.Sprintf("DROP TABLE IF EXISTS %s", tableName)
}

// GenerateDropIndexSQL generates a DROP INDEX statement
func (sm *SchemaManager) GenerateDropIndexSQL(indexName string) string {
	return fmt.Sprintf("DROP INDEX IF EXISTS %s", indexName)
}

// ValidateSchema validates the entire schema configuration
func (sm *SchemaManager) ValidateSchema(tableName string, fields []FieldConfig, indexes []IndexConfig) error {
	// Validate table name
	if err := sm.ValidateTableName(tableName); err != nil {
		return fmt.Errorf("invalid table name: %w", err)
	}
	
	// Validate field names and collect them for index validation
	fieldNames := make(map[string]bool)
	for _, field := range fields {
		if err := sm.ValidateFieldName(field.Name); err != nil {
			return fmt.Errorf("invalid field name '%s': %w", field.Name, err)
		}
		fieldNames[field.Name] = true
	}
	
	// Validate indexes
	for _, index := range indexes {
		if err := sm.ValidateIndexName(index.Name); err != nil {
			return fmt.Errorf("invalid index name '%s': %w", index.Name, err)
		}
		
		if err := sm.ValidateIndexType(index.Type); err != nil {
			return fmt.Errorf("invalid index type for '%s': %w", index.Name, err)
		}
		
		// Validate that index columns exist in the table
		for _, column := range index.Columns {
			if !fieldNames[column] {
				return fmt.Errorf("index '%s' references non-existent column '%s'", index.Name, column)
			}
		}
		
		// Validate that index has at least one column
		if len(index.Columns) == 0 {
			return fmt.Errorf("index '%s' must have at least one column", index.Name)
		}
	}
	
	return nil
}