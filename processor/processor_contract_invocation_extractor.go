package processor

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"regexp"
	"sync"
	"time"
)

// ExtractedContractInvocation represents a contract invocation with extracted business data
type ExtractedContractInvocation struct {
	// Core identifiers
	Toid            uint64 `json:"toid"`
	Ledger          uint32 `json:"ledger"`
	Timestamp       string `json:"timestamp"`
	ContractID      string `json:"contract_id"`
	FunctionName    string `json:"function_name"`
	InvokingAccount string `json:"invoking_account"`
	TxHash          string `json:"transaction_hash"`

	// Extracted business fields
	Funder    string `json:"funder,omitempty"`
	Recipient string `json:"recipient,omitempty"`
	Amount    uint64 `json:"amount,omitempty"`
	ProjectID string `json:"project_id,omitempty"`
	MemoText  string `json:"memo_text,omitempty"`
	Email     string `json:"email,omitempty"`

	// Metadata
	ProcessedAt time.Time `json:"processed_at"`
	SchemaName  string    `json:"schema_name"`
	Successful  bool      `json:"successful"`
}

// ExtractionSchema defines how to extract fields from contract arguments
type ExtractionSchema struct {
	SchemaName   string                    `json:"schema_name"`
	FunctionName string                    `json:"function_name"`
	ContractIDs  []string                  `json:"contract_ids"`
	Extractors   map[string]FieldExtractor `json:"extractors"`
	Validation   map[string]ValidationRule `json:"validation"`
}

// FieldExtractor defines how to extract a specific field from arguments
type FieldExtractor struct {
	ArgumentIndex int    `json:"argument_index"`
	FieldPath     string `json:"field_path"`
	FieldType     string `json:"field_type"`
	Required      bool   `json:"required"`
	DefaultValue  string `json:"default_value,omitempty"`
}

// ValidationRule defines validation constraints for extracted fields
type ValidationRule struct {
	MinLength int      `json:"min_length,omitempty"`
	MaxLength int      `json:"max_length,omitempty"`
	Pattern   string   `json:"pattern,omitempty"`
	MinValue  *float64 `json:"min_value,omitempty"`
	MaxValue  *float64 `json:"max_value,omitempty"`
}

// ContractInvocationExtractor processes contract invocations into structured format
type ContractInvocationExtractor struct {
	processors        []Processor
	extractionSchemas map[string]ExtractionSchema
	mu                sync.RWMutex
	stats             struct {
		ProcessedInvocations  uint64
		SuccessfulExtractions uint64
		FailedExtractions     uint64
		SchemaNotFound        uint64
		ValidationErrors      uint64
		LastProcessedTime     time.Time
	}
}

// NewContractInvocationExtractor creates a new contract invocation extractor
func NewContractInvocationExtractor(config map[string]interface{}) (*ContractInvocationExtractor, error) {
	processor := &ContractInvocationExtractor{
		extractionSchemas: make(map[string]ExtractionSchema),
	}

	if err := processor.loadExtractionSchemas(config); err != nil {
		return nil, fmt.Errorf("failed to load extraction schemas: %w", err)
	}

	log.Printf("ContractInvocationExtractor initialized with %d schemas", len(processor.extractionSchemas))
	return processor, nil
}

// Subscribe adds a processor to the chain
func (p *ContractInvocationExtractor) Subscribe(processor Processor) {
	p.processors = append(p.processors, processor)
}

// Process handles incoming contract invocation messages
func (p *ContractInvocationExtractor) Process(ctx context.Context, msg Message) error {
	p.mu.Lock()
	p.stats.ProcessedInvocations++
	p.stats.LastProcessedTime = time.Now()
	p.mu.Unlock()

	// Parse contract invocation
	jsonBytes, ok := msg.Payload.([]byte)
	if !ok {
		return fmt.Errorf("expected JSON payload, got %T", msg.Payload)
	}

	var invocation ContractInvocation
	if err := json.Unmarshal(jsonBytes, &invocation); err != nil {
		return fmt.Errorf("failed to unmarshal contract invocation: %w", err)
	}

	// Extract structured data
	extracted, err := p.extractContractData(&invocation)
	if err != nil {
		p.mu.Lock()
		p.stats.FailedExtractions++
		p.mu.Unlock()
		log.Printf("Failed to extract contract data: %v", err)
		return nil // Don't fail the pipeline, just log the error
	}

	p.mu.Lock()
	p.stats.SuccessfulExtractions++
	p.mu.Unlock()

	// Forward to processors
	extractedBytes, err := json.Marshal(extracted)
	if err != nil {
		return fmt.Errorf("failed to marshal extracted data: %w", err)
	}

	for _, processor := range p.processors {
		if err := processor.Process(ctx, Message{Payload: extractedBytes}); err != nil {
			return fmt.Errorf("error in processor chain: %w", err)
		}
	}

	return nil
}

// extractContractData extracts structured data from contract invocation
func (p *ContractInvocationExtractor) extractContractData(invocation *ContractInvocation) (*ExtractedContractInvocation, error) {
	// Find matching schema
	schema, exists := p.findMatchingSchema(invocation.ContractID, invocation.FunctionName)
	if !exists {
		p.mu.Lock()
		p.stats.SchemaNotFound++
		p.mu.Unlock()
		return nil, fmt.Errorf("no extraction schema found for contract %s function %s",
			invocation.ContractID, invocation.FunctionName)
	}

	// Generate TOID (Transaction Operation ID)
	toid := p.generateTOID(invocation.LedgerSequence, invocation.TransactionIndex, invocation.OperationIndex)

	// Create base extracted structure
	extracted := &ExtractedContractInvocation{
		Toid:            toid,
		Ledger:          invocation.LedgerSequence,
		Timestamp:       mustToUTC(invocation.Timestamp),
		ContractID:      invocation.ContractID,
		FunctionName:    invocation.FunctionName,
		InvokingAccount: invocation.InvokingAccount,
		TxHash:          invocation.TransactionHash,
		ProcessedAt:     time.Now(),
		SchemaName:      schema.SchemaName,
		Successful:      invocation.Successful,
	}

	// Extract fields according to schema
	if err := p.extractFieldsFromArguments(extracted, invocation.ArgumentsDecoded, schema); err != nil {
		return nil, fmt.Errorf("failed to extract fields: %w", err)
	}

	// Validate extracted data
	if err := p.validateExtractedData(extracted, schema); err != nil {
		p.mu.Lock()
		p.stats.ValidationErrors++
		p.mu.Unlock()
		return nil, fmt.Errorf("validation failed: %w", err)
	}

	return extracted, nil
}

// generateTOID creates a unique Transaction Operation ID
func (p *ContractInvocationExtractor) generateTOID(ledger uint32, txIdx, opIdx uint32) uint64 {
	return (uint64(ledger) << 32) | (uint64(txIdx) << 20) | uint64(opIdx)
}

// findMatchingSchema finds the extraction schema for a contract and function
func (p *ContractInvocationExtractor) findMatchingSchema(contractID, functionName string) (ExtractionSchema, bool) {
	p.mu.RLock()
	defer p.mu.RUnlock()

	key := fmt.Sprintf("%s:%s", contractID, functionName)
	schema, exists := p.extractionSchemas[key]
	return schema, exists
}

// extractFieldsFromArguments extracts fields from arguments according to schema
func (p *ContractInvocationExtractor) extractFieldsFromArguments(
	extracted *ExtractedContractInvocation,
	args map[string]interface{},
	schema ExtractionSchema,
) error {
	for fieldName, extractor := range schema.Extractors {
		value, err := p.extractFieldValue(args, extractor)
		if err != nil {
			if extractor.Required {
				return fmt.Errorf("required field %s: %w", fieldName, err)
			}
			// Use default value if provided
			if extractor.DefaultValue != "" {
				if err := p.setFieldValue(extracted, fieldName, extractor.DefaultValue); err != nil {
					return fmt.Errorf("failed to set default value for field %s: %w", fieldName, err)
				}
			}
			continue
		}

		// Set field value based on field name
		if err := p.setFieldValue(extracted, fieldName, value); err != nil {
			return fmt.Errorf("failed to set field %s: %w", fieldName, err)
		}
	}

	return nil
}

// extractFieldValue extracts a single field value from arguments
func (p *ContractInvocationExtractor) extractFieldValue(args map[string]interface{}, extractor FieldExtractor) (interface{}, error) {
	argKey := fmt.Sprintf("arg_%d", extractor.ArgumentIndex)
	arg, exists := args[argKey]
	if !exists {
		return nil, fmt.Errorf("argument %s not found", argKey)
	}

	// Navigate nested field path if specified
	if extractor.FieldPath != "" {
		if argMap, ok := arg.(map[string]interface{}); ok {
			if value, exists := argMap[extractor.FieldPath]; exists {
				return p.convertFieldType(value, extractor.FieldType)
			}
		}
		return nil, fmt.Errorf("field %s not found in argument %s", extractor.FieldPath, argKey)
	}

	return p.convertFieldType(arg, extractor.FieldType)
}

// convertFieldType converts a value to the specified type
func (p *ContractInvocationExtractor) convertFieldType(value interface{}, fieldType string) (interface{}, error) {
	switch fieldType {
	case "string":
		if str, ok := value.(string); ok {
			return str, nil
		}
		return fmt.Sprintf("%v", value), nil

	case "uint64":
		if num, ok := value.(float64); ok {
			return uint64(num), nil
		}
		if num, ok := value.(int64); ok {
			return uint64(num), nil
		}
		if num, ok := value.(uint64); ok {
			return num, nil
		}
		return nil, fmt.Errorf("cannot convert %v (%T) to uint64", value, value)

	case "address":
		if str, ok := value.(string); ok {
			return str, nil
		}
		return nil, fmt.Errorf("cannot convert %v to address", value)

	default:
		return value, nil
	}
}

// setFieldValue sets a field value on the extracted structure
func (p *ContractInvocationExtractor) setFieldValue(extracted *ExtractedContractInvocation, fieldName string, value interface{}) error {
	switch fieldName {
	case "funder":
		if str, ok := value.(string); ok {
			extracted.Funder = str
		}
	case "recipient":
		if str, ok := value.(string); ok {
			extracted.Recipient = str
		}
	case "amount":
		if num, ok := value.(uint64); ok {
			extracted.Amount = num
		}
	case "project_id":
		if str, ok := value.(string); ok {
			extracted.ProjectID = str
		}
	case "memo_text":
		if str, ok := value.(string); ok {
			extracted.MemoText = str
		}
	case "email":
		if str, ok := value.(string); ok {
			extracted.Email = str
		}
	default:
		return fmt.Errorf("unknown field name: %s", fieldName)
	}

	return nil
}

// validateExtractedData validates the extracted data against schema rules
func (p *ContractInvocationExtractor) validateExtractedData(extracted *ExtractedContractInvocation, schema ExtractionSchema) error {
	for fieldName, rule := range schema.Validation {
		if err := p.validateField(extracted, fieldName, rule); err != nil {
			return fmt.Errorf("validation failed for field %s: %w", fieldName, err)
		}
	}

	return nil
}

// validateField validates a single field against its validation rule
func (p *ContractInvocationExtractor) validateField(extracted *ExtractedContractInvocation, fieldName string, rule ValidationRule) error {
	var value interface{}

	switch fieldName {
	case "funder":
		value = extracted.Funder
	case "recipient":
		value = extracted.Recipient
	case "amount":
		value = extracted.Amount
	case "project_id":
		value = extracted.ProjectID
	case "memo_text":
		value = extracted.MemoText
	case "email":
		value = extracted.Email
	default:
		return fmt.Errorf("unknown field for validation: %s", fieldName)
	}

	// Skip validation for empty optional fields
	if value == nil || (value == "" && rule.MinLength == 0) {
		return nil
	}

	// Validate string fields
	if str, ok := value.(string); ok {
		if rule.MinLength > 0 && len(str) < rule.MinLength {
			return fmt.Errorf("string too short: %d < %d", len(str), rule.MinLength)
		}
		if rule.MaxLength > 0 && len(str) > rule.MaxLength {
			return fmt.Errorf("string too long: %d > %d", len(str), rule.MaxLength)
		}
		if rule.Pattern != "" {
			if matched, err := regexp.MatchString(rule.Pattern, str); err != nil {
				return fmt.Errorf("pattern validation error: %w", err)
			} else if !matched {
				return fmt.Errorf("string does not match pattern: %s", rule.Pattern)
			}
		}
	}

	// Validate numeric fields
	if num, ok := value.(uint64); ok {
		if rule.MinValue != nil && float64(num) < *rule.MinValue {
			return fmt.Errorf("value too small: %d < %f", num, *rule.MinValue)
		}
		if rule.MaxValue != nil && float64(num) > *rule.MaxValue {
			return fmt.Errorf("value too large: %d > %f", num, *rule.MaxValue)
		}
	}

	return nil
}

// loadExtractionSchemas loads extraction schemas from configuration
func (p *ContractInvocationExtractor) loadExtractionSchemas(config map[string]interface{}) error {
	// Handle both map[string]interface{} and map[interface{}]interface{} from YAML parsing
	var schemas map[string]interface{}
	if s, ok := config["extraction_schemas"].(map[string]interface{}); ok {
		schemas = s
	} else if s, ok := config["extraction_schemas"].(map[interface{}]interface{}); ok {
		schemas = convertInterfaceMap(s)
	} else {
		return fmt.Errorf("missing extraction_schemas in configuration")
	}

	for schemaName, schemaConfig := range schemas {
		schema, err := p.parseExtractionSchema(schemaName, schemaConfig)
		if err != nil {
			return fmt.Errorf("failed to parse schema %s: %w", schemaName, err)
		}

		// Register schema for each contract ID
		for _, contractID := range schema.ContractIDs {
			key := fmt.Sprintf("%s:%s", contractID, schema.FunctionName)
			p.extractionSchemas[key] = schema
		}
	}

	return nil
}

// parseExtractionSchema parses a schema configuration
func (p *ContractInvocationExtractor) parseExtractionSchema(name string, config interface{}) (ExtractionSchema, error) {
	var configMap map[string]interface{}
	
	if cm, ok := config.(map[string]interface{}); ok {
		configMap = cm
	} else if cm, ok := config.(map[interface{}]interface{}); ok {
		configMap = convertInterfaceMap(cm)
	} else {
		return ExtractionSchema{}, fmt.Errorf("schema config must be a map, got %T", config)
	}

	schema := ExtractionSchema{
		SchemaName:  name,
		Extractors:  make(map[string]FieldExtractor),
		Validation:  make(map[string]ValidationRule),
	}

	// Parse function name
	if functionName, ok := configMap["function_name"].(string); ok {
		schema.FunctionName = functionName
	} else {
		return ExtractionSchema{}, fmt.Errorf("missing function_name in schema")
	}

	// Parse contract IDs
	if contractIDs, ok := configMap["contract_ids"].([]interface{}); ok {
		for _, id := range contractIDs {
			if idStr, ok := id.(string); ok {
				schema.ContractIDs = append(schema.ContractIDs, idStr)
			}
		}
	} else {
		return ExtractionSchema{}, fmt.Errorf("missing contract_ids in schema")
	}

	// Parse extractors
	if extractors, ok := configMap["extractors"].(map[string]interface{}); ok {
		for fieldName, extractorConfig := range extractors {
			extractor, err := p.parseFieldExtractor(extractorConfig)
			if err != nil {
				return ExtractionSchema{}, fmt.Errorf("failed to parse extractor %s: %w", fieldName, err)
			}
			schema.Extractors[fieldName] = extractor
		}
	}

	// Parse validation rules
	if validation, ok := configMap["validation"].(map[string]interface{}); ok {
		for fieldName, validationConfig := range validation {
			rule, err := p.parseValidationRule(validationConfig)
			if err != nil {
				return ExtractionSchema{}, fmt.Errorf("failed to parse validation rule %s: %w", fieldName, err)
			}
			schema.Validation[fieldName] = rule
		}
	}

	return schema, nil
}

// parseFieldExtractor parses a field extractor configuration
func (p *ContractInvocationExtractor) parseFieldExtractor(config interface{}) (FieldExtractor, error) {
	var configMap map[string]interface{}
	
	if cm, ok := config.(map[string]interface{}); ok {
		configMap = cm
	} else if cm, ok := config.(map[interface{}]interface{}); ok {
		configMap = convertInterfaceMap(cm)
	} else {
		return FieldExtractor{}, fmt.Errorf("extractor config must be a map, got %T", config)
	}

	extractor := FieldExtractor{}

	if argIndex, ok := configMap["argument_index"].(int); ok {
		extractor.ArgumentIndex = argIndex
	} else if argIndexFloat, ok := configMap["argument_index"].(float64); ok {
		extractor.ArgumentIndex = int(argIndexFloat)
	} else {
		return FieldExtractor{}, fmt.Errorf("missing argument_index in extractor")
	}

	if fieldPath, ok := configMap["field_path"].(string); ok {
		extractor.FieldPath = fieldPath
	}

	if fieldType, ok := configMap["field_type"].(string); ok {
		extractor.FieldType = fieldType
	} else {
		return FieldExtractor{}, fmt.Errorf("missing field_type in extractor")
	}

	if required, ok := configMap["required"].(bool); ok {
		extractor.Required = required
	}

	if defaultValue, ok := configMap["default_value"].(string); ok {
		extractor.DefaultValue = defaultValue
	}

	return extractor, nil
}

// parseValidationRule parses a validation rule configuration
func (p *ContractInvocationExtractor) parseValidationRule(config interface{}) (ValidationRule, error) {
	var configMap map[string]interface{}
	
	if cm, ok := config.(map[string]interface{}); ok {
		configMap = cm
	} else if cm, ok := config.(map[interface{}]interface{}); ok {
		configMap = convertInterfaceMap(cm)
	} else {
		return ValidationRule{}, fmt.Errorf("validation rule config must be a map, got %T", config)
	}

	rule := ValidationRule{}

	if minLength, ok := configMap["min_length"].(int); ok {
		rule.MinLength = minLength
	} else if minLengthFloat, ok := configMap["min_length"].(float64); ok {
		rule.MinLength = int(minLengthFloat)
	}

	if maxLength, ok := configMap["max_length"].(int); ok {
		rule.MaxLength = maxLength
	} else if maxLengthFloat, ok := configMap["max_length"].(float64); ok {
		rule.MaxLength = int(maxLengthFloat)
	}

	if pattern, ok := configMap["pattern"].(string); ok {
		rule.Pattern = pattern
	}

	if minValue, ok := configMap["min_value"].(float64); ok {
		rule.MinValue = &minValue
	}

	if maxValue, ok := configMap["max_value"].(float64); ok {
		rule.MaxValue = &maxValue
	}

	return rule, nil
}

// convertInterfaceMap converts map[interface{}]interface{} to map[string]interface{}
func convertInterfaceMap(m map[interface{}]interface{}) map[string]interface{} {
	result := make(map[string]interface{})
	for k, v := range m {
		strKey, ok := k.(string)
		if !ok {
			continue
		}
		
		// Recursively convert nested maps
		if nestedMap, ok := v.(map[interface{}]interface{}); ok {
			result[strKey] = convertInterfaceMap(nestedMap)
		} else {
			result[strKey] = v
		}
	}
	return result
}

// getKeys returns the keys of a map for debugging
func getKeys(m map[string]interface{}) []string {
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	return keys
}

// mustToUTC converts timestamp to UTC string format
func mustToUTC(timestamp interface{}) string {
	switch t := timestamp.(type) {
	case time.Time:
		return t.UTC().Format(time.RFC3339)
	case string:
		parsed, err := time.Parse(time.RFC3339, t)
		if err != nil {
			log.Printf("Error parsing timestamp %s: %v", t, err)
			return t
		}
		return parsed.UTC().Format(time.RFC3339)
	default:
		log.Printf("Unknown timestamp type: %T", t)
		return time.Now().UTC().Format(time.RFC3339)
	}
}

// GetStats returns processor statistics
func (p *ContractInvocationExtractor) GetStats() struct {
	ProcessedInvocations  uint64
	SuccessfulExtractions uint64
	FailedExtractions     uint64
	SchemaNotFound        uint64
	ValidationErrors      uint64
	LastProcessedTime     time.Time
} {
	p.mu.RLock()
	defer p.mu.RUnlock()
	return p.stats
}