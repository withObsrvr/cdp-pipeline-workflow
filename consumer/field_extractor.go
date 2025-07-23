package consumer

import (
	"encoding/json"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/tidwall/gjson"
)

// FieldExtractor handles extracting and converting fields from JSON data
type FieldExtractor struct {
	// Add any state needed for field extraction
}

// NewFieldExtractor creates a new field extractor
func NewFieldExtractor() *FieldExtractor {
	return &FieldExtractor{}
}

// ExtractFields extracts all configured fields from the JSON data
func (fe *FieldExtractor) ExtractFields(data []byte, fields []FieldConfig) (map[string]interface{}, error) {
	result := make(map[string]interface{})
	
	for _, field := range fields {
		// Skip generated fields (they're handled by the database)
		if field.Generated {
			continue
		}
		
		value, err := fe.ExtractField(data, field)
		if err != nil {
			return nil, fmt.Errorf("failed to extract field %s: %w", field.Name, err)
		}
		
		result[field.Name] = value
	}
	
	return result, nil
}

// ExtractField extracts a single field from JSON data
func (fe *FieldExtractor) ExtractField(data []byte, config FieldConfig) (interface{}, error) {
	// Skip generated fields
	if config.Generated {
		return nil, nil
	}
	
	// Get value from JSON using gjson
	result := gjson.GetBytes(data, config.SourcePath)
	
	// Handle missing values
	if !result.Exists() {
		if config.Required {
			return nil, fmt.Errorf("required field %s not found at path %s", config.Name, config.SourcePath)
		}
		
		// Return default value if configured
		if config.Default != "" {
			return fe.convertStringToType(config.Default, config.Type)
		}
		
		// Return nil for optional missing fields
		return nil, nil
	}
	
	// Convert to appropriate type
	return fe.convertToType(result, config.Type)
}

// convertToType converts a gjson.Result to the specified PostgreSQL type
func (fe *FieldExtractor) convertToType(result gjson.Result, pgType string) (interface{}, error) {
	// Normalize type (remove size specifications, etc.)
	normalizedType := strings.ToUpper(strings.Split(pgType, "(")[0])
	
	switch normalizedType {
	case "TEXT", "VARCHAR", "CHAR":
		return result.String(), nil
		
	case "INTEGER", "INT", "INT4":
		if result.Type == gjson.Number {
			return int(result.Int()), nil
		}
		// Try to parse string as int
		if result.Type == gjson.String {
			return strconv.Atoi(result.String())
		}
		return nil, fmt.Errorf("cannot convert %s to integer", result.Type)
		
	case "BIGINT", "INT8":
		if result.Type == gjson.Number {
			return result.Int(), nil
		}
		// Try to parse string as int64
		if result.Type == gjson.String {
			return strconv.ParseInt(result.String(), 10, 64)
		}
		return nil, fmt.Errorf("cannot convert %s to bigint", result.Type)
		
	case "BOOLEAN", "BOOL":
		if result.Type == gjson.True {
			return true, nil
		}
		if result.Type == gjson.False {
			return false, nil
		}
		// Try to parse string as bool
		if result.Type == gjson.String {
			return strconv.ParseBool(result.String())
		}
		return nil, fmt.Errorf("cannot convert %s to boolean", result.Type)
		
	case "REAL", "FLOAT4":
		if result.Type == gjson.Number {
			return float32(result.Float()), nil
		}
		// Try to parse string as float32
		if result.Type == gjson.String {
			val, err := strconv.ParseFloat(result.String(), 32)
			return float32(val), err
		}
		return nil, fmt.Errorf("cannot convert %s to real", result.Type)
		
	case "DOUBLE", "FLOAT8":
		if result.Type == gjson.Number {
			return result.Float(), nil
		}
		// Try to parse string as float64
		if result.Type == gjson.String {
			return strconv.ParseFloat(result.String(), 64)
		}
		return nil, fmt.Errorf("cannot convert %s to double", result.Type)
		
	case "TIMESTAMP", "TIMESTAMPTZ":
		return fe.parseTimestamp(result)
		
	case "DATE":
		return fe.parseDate(result)
		
	case "JSON", "JSONB":
		// For JSON types, we can store the raw JSON
		if result.Type == gjson.JSON {
			// Return the raw JSON string for JSONB storage
			return result.Raw, nil
		}
		// For other types, convert to JSON
		var jsonData interface{}
		switch result.Type {
		case gjson.String:
			jsonData = result.String()
		case gjson.Number:
			jsonData = result.Float()
		case gjson.True, gjson.False:
			jsonData = result.Bool()
		default:
			jsonData = result.Value()
		}
		
		jsonBytes, err := json.Marshal(jsonData)
		if err != nil {
			return nil, fmt.Errorf("failed to marshal to JSON: %w", err)
		}
		return string(jsonBytes), nil
		
	case "BYTEA":
		// For bytea, expect base64 encoded string
		return result.String(), nil
		
	default:
		// For unknown types, return as string
		return result.String(), nil
	}
}

// convertStringToType converts a string default value to the specified type
func (fe *FieldExtractor) convertStringToType(value, pgType string) (interface{}, error) {
	// Create a fake gjson result to reuse conversion logic
	fakeResult := gjson.Result{
		Type: gjson.String,
		Raw:  value,
		Str:  value,
	}
	
	return fe.convertToType(fakeResult, pgType)
}

// parseTimestamp attempts to parse various timestamp formats
func (fe *FieldExtractor) parseTimestamp(result gjson.Result) (interface{}, error) {
	if result.Type != gjson.String {
		return nil, fmt.Errorf("timestamp must be a string")
	}
	
	timeStr := result.String()
	
	// Try common timestamp formats
	formats := []string{
		time.RFC3339,
		time.RFC3339Nano,
		"2006-01-02T15:04:05Z",
		"2006-01-02T15:04:05.000Z",
		"2006-01-02T15:04:05.000000Z",
		"2006-01-02 15:04:05",
		"2006-01-02 15:04:05.000",
		"2006-01-02 15:04:05.000000",
	}
	
	for _, format := range formats {
		if t, err := time.Parse(format, timeStr); err == nil {
			return t, nil
		}
	}
	
	// Try parsing as Unix timestamp (seconds)
	if unixTime, err := strconv.ParseInt(timeStr, 10, 64); err == nil {
		return time.Unix(unixTime, 0), nil
	}
	
	// Try parsing as Unix timestamp (milliseconds)
	if unixTimeMs, err := strconv.ParseInt(timeStr, 10, 64); err == nil && unixTimeMs > 1000000000000 {
		return time.Unix(unixTimeMs/1000, (unixTimeMs%1000)*1000000), nil
	}
	
	return nil, fmt.Errorf("unable to parse timestamp: %s", timeStr)
}

// parseDate attempts to parse various date formats
func (fe *FieldExtractor) parseDate(result gjson.Result) (interface{}, error) {
	if result.Type != gjson.String {
		return nil, fmt.Errorf("date must be a string")
	}
	
	dateStr := result.String()
	
	// Try common date formats
	formats := []string{
		"2006-01-02",
		"01/02/2006",
		"02/01/2006", // European format
		"2006/01/02",
	}
	
	for _, format := range formats {
		if t, err := time.Parse(format, dateStr); err == nil {
			return t, nil
		}
	}
	
	return nil, fmt.Errorf("unable to parse date: %s", dateStr)
}

// ValidateFieldTypes validates that the configured field types are supported
func (fe *FieldExtractor) ValidateFieldTypes(fields []FieldConfig) error {
	supportedTypes := map[string]bool{
		"TEXT": true, "VARCHAR": true, "CHAR": true,
		"INTEGER": true, "INT": true, "INT4": true,
		"BIGINT": true, "INT8": true,
		"BOOLEAN": true, "BOOL": true,
		"REAL": true, "FLOAT4": true,
		"DOUBLE": true, "FLOAT8": true,
		"TIMESTAMP": true, "TIMESTAMPTZ": true,
		"DATE": true,
		"JSON": true, "JSONB": true,
		"BYTEA": true,
		"SERIAL": true, "BIGSERIAL": true,
	}
	
	for _, field := range fields {
		// Normalize type (remove size and constraints)
		normalizedType := strings.ToUpper(strings.Split(field.Type, "(")[0])
		normalizedType = strings.Split(normalizedType, " ")[0] // Remove constraints like NOT NULL
		
		if !supportedTypes[normalizedType] {
			return fmt.Errorf("unsupported field type: %s for field %s", field.Type, field.Name)
		}
	}
	
	return nil
}