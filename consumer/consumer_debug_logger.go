package consumer

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"github.com/withObsrvr/cdp-pipeline-workflow/processor"
)

// DebugLoggerConfig holds configuration for the debug logger consumer
type DebugLoggerConfig struct {
	Name      string `json:"name"`
	LogPrefix string `json:"log_prefix"`
	MaxFields int    `json:"max_fields"`
}

// DebugLogger is a simple consumer that logs message details for debugging
type DebugLogger struct {
	config     DebugLoggerConfig
	processors []processor.Processor
	msgCount   int64
}

// NewDebugLogger creates a new debug logger consumer
func NewDebugLogger(config map[string]interface{}) (*DebugLogger, error) {
	var cfg DebugLoggerConfig
	
	// Extract configuration
	if name, ok := config["name"].(string); ok {
		cfg.Name = name
	} else {
		cfg.Name = "debug_logger"
	}
	
	if prefix, ok := config["log_prefix"].(string); ok {
		cfg.LogPrefix = prefix
	} else {
		cfg.LogPrefix = "DEBUG"
	}
	
	if maxFields, ok := config["max_fields"].(float64); ok {
		cfg.MaxFields = int(maxFields)
	} else {
		cfg.MaxFields = 10
	}
	
	return &DebugLogger{
		config:     cfg,
		processors: make([]processor.Processor, 0),
	}, nil
}

// Subscribe adds a processor to receive data
func (d *DebugLogger) Subscribe(processor processor.Processor) {
	d.processors = append(d.processors, processor)
}

// Process logs the incoming message details
func (d *DebugLogger) Process(ctx context.Context, msg processor.Message) error {
	d.msgCount++
	
	log.Printf("[%s] Message #%d received", d.config.LogPrefix, d.msgCount)
	log.Printf("[%s] Payload type: %T", d.config.LogPrefix, msg.Payload)
	
	switch payload := msg.Payload.(type) {
	case []byte:
		log.Printf("[%s] Payload is []byte, length: %d", d.config.LogPrefix, len(payload))
		
		// Try to parse as JSON
		var data map[string]interface{}
		if err := json.Unmarshal(payload, &data); err != nil {
			log.Printf("[%s] Failed to parse as JSON: %v", d.config.LogPrefix, err)
			log.Printf("[%s] First 200 chars: %s", d.config.LogPrefix, string(payload[:min(200, len(payload))]))
		} else {
			log.Printf("[%s] Parsed JSON with %d fields", d.config.LogPrefix, len(data))
			d.logFields(data)
		}
		
	case map[string]interface{}:
		log.Printf("[%s] Payload is map with %d fields", d.config.LogPrefix, len(payload))
		d.logFields(payload)
		
	default:
		log.Printf("[%s] Payload is other type, converting to string", d.config.LogPrefix)
		log.Printf("[%s] Content: %v", d.config.LogPrefix, payload)
	}
	
	// Log metadata if present
	if msg.Metadata != nil && len(msg.Metadata) > 0 {
		log.Printf("[%s] Metadata present with %d fields", d.config.LogPrefix, len(msg.Metadata))
		for key, value := range msg.Metadata {
			log.Printf("[%s]   %s: %v", d.config.LogPrefix, key, value)
		}
	}
	
	// Forward to downstream processors
	for _, proc := range d.processors {
		if err := proc.Process(ctx, msg); err != nil {
			log.Printf("[%s] Error in downstream processor: %v", d.config.LogPrefix, err)
		}
	}
	
	return nil
}

// logFields logs the fields of a map up to max_fields limit
func (d *DebugLogger) logFields(data map[string]interface{}) {
	count := 0
	for key, value := range data {
		if count >= d.config.MaxFields {
			log.Printf("[%s]   ... and %d more fields", d.config.LogPrefix, len(data)-count)
			break
		}
		
		// Format value based on type
		var valueStr string
		switch v := value.(type) {
		case string:
			if len(v) > 50 {
				valueStr = fmt.Sprintf("\"%s...\" (truncated)", v[:50])
			} else {
				valueStr = fmt.Sprintf("\"%s\"", v)
			}
		case float64:
			valueStr = fmt.Sprintf("%v", v)
		case bool:
			valueStr = fmt.Sprintf("%v", v)
		case nil:
			valueStr = "null"
		case map[string]interface{}:
			valueStr = fmt.Sprintf("map[%d fields]", len(v))
		case []interface{}:
			valueStr = fmt.Sprintf("array[%d items]", len(v))
		default:
			valueStr = fmt.Sprintf("%T", v)
		}
		
		log.Printf("[%s]   %s: %s", d.config.LogPrefix, key, valueStr)
		count++
	}
}

func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}