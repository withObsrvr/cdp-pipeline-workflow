package kale

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/withObsrvr/cdp-pipeline-workflow/pkg/common/types"
)

// Event types
const (
	EventTypePlant   = "plant"
	EventTypeWork    = "work"
	EventTypeHarvest = "harvest"
	EventTypeMint    = "mint"
)

// KaleBlockMetrics represents the metrics for a Kale block
type KaleBlockMetrics struct {
	BlockIndex       uint32    `json:"block_index"`
	Timestamp        time.Time `json:"timestamp"`
	TotalStaked      int64     `json:"total_staked"`
	TotalReward      int64     `json:"total_reward"`
	Participants     int       `json:"participants"`
	HighestZeroCount int       `json:"highest_zero_count"`
	CloseTimeMs      int64     `json:"close_time_ms"`
	Farmers          []string  `json:"farmers"`
	MaxZeros         uint32    `json:"max_zeros"`
	MinZeros         uint32    `json:"min_zeros"`
	OpenTimeMs       int64     `json:"open_time_ms"`
	Duration         int64     `json:"duration"`
}

// KaleProcessor processes Kale contract events and invocations
type KaleProcessor struct {
	contractID   string
	processors   []types.Processor
	blockMetrics map[uint32]*KaleBlockMetrics
	mu           sync.RWMutex
	stats        struct {
		ProcessedEvents   uint64
		PlantEvents       uint64
		WorkEvents        uint64
		HarvestEvents     uint64
		MintEvents        uint64
		LastProcessedTime time.Time
	}
}

// NewKaleProcessor creates a new KaleProcessor
func NewKaleProcessor(config map[string]interface{}) (*KaleProcessor, error) {
	contractID, ok := config["contract_id"].(string)
	if !ok {
		return nil, fmt.Errorf("missing contract_id in configuration")
	}

	log.Printf("Initializing KaleProcessor for contract: %s", contractID)

	return &KaleProcessor{
		contractID:   contractID,
		blockMetrics: make(map[uint32]*KaleBlockMetrics),
		processors:   make([]types.Processor, 0),
	}, nil
}

// Subscribe adds a processor to the chain
func (p *KaleProcessor) Subscribe(processor types.Processor) {
	p.processors = append(p.processors, processor)
	log.Printf("Added processor to KaleProcessor, total processors: %d", len(p.processors))
}

// Process handles both contract events and invocations
func (p *KaleProcessor) Process(ctx context.Context, msg types.Message) error {
	// Parse the message
	var rawMessage map[string]interface{}

	switch payload := msg.Payload.(type) {
	case []byte:
		if err := json.Unmarshal(payload, &rawMessage); err != nil {
			return fmt.Errorf("error decoding message: %w", err)
		}
	case map[string]interface{}:
		rawMessage = payload
	default:
		return fmt.Errorf("unexpected payload type: %T", msg.Payload)
	}

	// Check if this is from our target contract
	contractID, ok := rawMessage["contract_id"].(string)
	if !ok {
		// Try contract_id in camelCase
		contractID, ok = rawMessage["contractId"].(string)
		if !ok {
			return nil // Not a contract message, skip
		}
	}

	if contractID != p.contractID {
		return nil // Not our contract, skip
	}

	// Check if this is an event or an invocation
	if _, hasTopics := rawMessage["topic"]; hasTopics {
		// This is an event message
		return p.processEventMessage(ctx, rawMessage)
	} else if _, hasInvokingAccount := rawMessage["invoking_account"]; hasInvokingAccount {
		// This is a contract invocation
		return p.processInvocationMessage(ctx, rawMessage)
	} else if _, hasFunctionName := rawMessage["function_name"]; hasFunctionName {
		// This is also a contract invocation
		return p.processInvocationMessage(ctx, rawMessage)
	}

	// Unknown message type
	return nil
}

// processEventMessage processes contract events
func (p *KaleProcessor) processEventMessage(ctx context.Context, contractEvent map[string]interface{}) error {
	// Extract topic to determine event type
	topicsRaw, ok := contractEvent["topic"]
	if !ok {
		return nil // No topics, skip
	}

	// Parse event type from topics
	var eventType string
	var blockIndex uint32

	// Extract event type from topics
	topics, ok := topicsRaw.([]interface{})
	if !ok || len(topics) == 0 {
		return nil
	}

	// Try to extract event type from the first topic
	firstTopic := topics[0]
	switch t := firstTopic.(type) {
	case string:
		eventType = t
	case map[string]interface{}:
		if str, ok := t["string"].(string); ok {
			eventType = str
		} else if sym, ok := t["sym"].(string); ok {
			eventType = sym
		} else if sym, ok := t["Sym"].(string); ok {
			eventType = sym
		}
	}

	if eventType == "" {
		return nil
	}

	// Try to extract block index from topics or data
	if len(topics) >= 3 {
		if indexVal, ok := topics[2].(float64); ok {
			blockIndex = uint32(indexVal)
		}
	}

	// Extract timestamp
	var timestamp time.Time
	if timestampStr, ok := contractEvent["timestamp"].(string); ok {
		if ts, err := time.Parse(time.RFC3339, timestampStr); err == nil {
			timestamp = ts
		}
	}

	// Extract ledger sequence
	var ledgerSequence uint32
	if seq, ok := contractEvent["ledger_sequence"].(float64); ok {
		ledgerSequence = uint32(seq)
	}

	// If block index is still 0, use ledger sequence as fallback
	if blockIndex == 0 {
		blockIndex = ledgerSequence
	}

	// Get or create block metrics
	metrics := p.getOrCreateBlockMetrics(blockIndex)
	metrics.Timestamp = timestamp

	// Extract farmer address
	var farmerAddr string
	if len(topics) >= 4 {
		if addr, ok := topics[3].(string); ok {
			farmerAddr = addr
		}
	}

	// Parse event data
	dataRaw, ok := contractEvent["data"]
	if !ok {
		return nil
	}

	var eventData map[string]interface{}
	dataBytes, err := json.Marshal(dataRaw)
	if err != nil {
		return fmt.Errorf("error marshaling event data: %w", err)
	}

	if err := json.Unmarshal(dataBytes, &eventData); err != nil {
		// Try to handle it as a simple value
		eventData = map[string]interface{}{
			"value": dataRaw,
		}
	}

	// Update metrics based on event type
	switch eventType {
	case EventTypePlant:
		p.mu.Lock()
		p.stats.PlantEvents++
		p.stats.ProcessedEvents++
		p.stats.LastProcessedTime = time.Now()
		p.mu.Unlock()
		p.updatePlantMetrics(metrics, eventData, farmerAddr)
	case EventTypeWork:
		p.mu.Lock()
		p.stats.WorkEvents++
		p.stats.ProcessedEvents++
		p.stats.LastProcessedTime = time.Now()
		p.mu.Unlock()
		p.updateWorkMetrics(metrics, eventData, farmerAddr)
	case EventTypeHarvest, EventTypeMint:
		p.mu.Lock()
		if eventType == EventTypeHarvest {
			p.stats.HarvestEvents++
		} else {
			p.stats.MintEvents++
		}
		p.stats.ProcessedEvents++
		p.stats.LastProcessedTime = time.Now()
		p.mu.Unlock()
		p.updateHarvestMetrics(metrics, eventData, farmerAddr)
	}

	// Forward metrics to consumers
	return p.forwardToProcessors(ctx, metrics)
}

// processInvocationMessage processes contract invocations
func (p *KaleProcessor) processInvocationMessage(ctx context.Context, rawMessage map[string]interface{}) error {
	// Extract function name
	functionName, hasFunctionName := rawMessage["function_name"].(string)
	if !hasFunctionName {
		return nil // No function name, skip
	}

	// Extract ledger sequence
	var ledgerSeq uint32
	if seq, ok := rawMessage["ledger_sequence"].(float64); ok {
		ledgerSeq = uint32(seq)
	}

	// Extract block index based on function name and arguments
	blockIndex := ledgerSeq // Default to ledger sequence

	// Try to extract block index from arguments
	if argsRaw, ok := rawMessage["arguments"].([]interface{}); ok && len(argsRaw) > 0 {
		// For plant function, the first argument might be the block index
		if functionName == "plant" && len(argsRaw) >= 1 {
			if indexArg, ok := argsRaw[0].(map[string]interface{}); ok {
				if indexVal, ok := indexArg["u32"].(float64); ok {
					blockIndex = uint32(indexVal)
				}
			}
		}
	}

	// Get or create block metrics
	metrics := p.getOrCreateBlockMetrics(blockIndex)

	// Extract timestamp
	if timestampStr, ok := rawMessage["timestamp"].(string); ok {
		if timestamp, err := time.Parse(time.RFC3339, timestampStr); err == nil {
			metrics.Timestamp = timestamp
		}
	}

	// Check for diagnostic events
	if diagnosticEventsRaw, ok := rawMessage["diagnostic_events"].([]interface{}); ok && len(diagnosticEventsRaw) > 0 {
		// Process diagnostic events to extract additional data
		for _, eventRaw := range diagnosticEventsRaw {
			event, ok := eventRaw.(map[string]interface{})
			if !ok {
				continue
			}

			// Extract topics
			topicsRaw, ok := event["topics"]
			if !ok {
				continue
			}

			topics, ok := topicsRaw.([]interface{})
			if !ok || len(topics) == 0 {
				continue
			}

			// Extract event type from topics
			var eventType string
			for _, topicRaw := range topics {
				topicStr, ok := topicRaw.(string)
				if !ok {
					continue
				}

				var topic map[string]interface{}
				if err := json.Unmarshal([]byte(topicStr), &topic); err != nil {
					continue
				}

				if sym, ok := topic["Sym"].(string); ok {
					eventType = sym
					break
				}
			}

			// Extract data
			dataRaw, ok := event["data"].(map[string]interface{})
			if !ok {
				continue
			}

			// Process based on event type
			switch eventType {
			case "mint":
				// Extract amount from I128 value
				if i128Raw, ok := dataRaw["I128"].(map[string]interface{}); ok {
					if loVal, ok := i128Raw["Lo"].(float64); ok {
						metrics.TotalReward += int64(loVal)
					}
				}
			case "burn":
				// Extract amount from I128 value
				if i128Raw, ok := dataRaw["I128"].(map[string]interface{}); ok {
					if loVal, ok := i128Raw["Lo"].(float64); ok {
						metrics.TotalStaked += int64(loVal)
					}
				}
			}
		}
	}

	// Update metrics based on function name
	switch functionName {
	case "plant":
		// When processing a plant invocation for a new block
		_, blockExists := p.blockMetrics[blockIndex]
		if !blockExists {
			currentTimeMs := time.Now().UnixMilli()
			metrics.OpenTimeMs = currentTimeMs
		}

		// Add farmer to participants if not already included
		if invokingAccount, ok := rawMessage["invoking_account"].(string); ok && invokingAccount != "" {
			if !contains(metrics.Farmers, invokingAccount) {
				metrics.Farmers = append(metrics.Farmers, invokingAccount)
				metrics.Participants = len(metrics.Farmers)
			}
		}

	case "work":
		// Extract zero count from the hash
		if argsRaw, ok := rawMessage["arguments"].([]interface{}); ok && len(argsRaw) >= 2 {
			// The second argument should contain the hash
			if hashArg, ok := argsRaw[1].(map[string]interface{}); ok {
				if hashVal, ok := hashArg["hash"].(string); ok {
					zeroCount := countLeadingZeros(hashVal)
					metrics.MaxZeros = max(metrics.MaxZeros, zeroCount)
					if metrics.MinZeros == 0 || zeroCount < metrics.MinZeros {
						metrics.MinZeros = zeroCount
					}
				}
			}
		}

		// Add farmer to participants if not already included
		if invokingAccount, ok := rawMessage["invoking_account"].(string); ok && invokingAccount != "" {
			if !contains(metrics.Farmers, invokingAccount) {
				metrics.Farmers = append(metrics.Farmers, invokingAccount)
				metrics.Participants = len(metrics.Farmers)
			}
		}

	case "harvest":
		// Set close time if not already set
		if metrics.CloseTimeMs == 0 {
			if timestampStr, ok := rawMessage["timestamp"].(string); ok {
				if timestamp, err := time.Parse(time.RFC3339, timestampStr); err == nil {
					metrics.CloseTimeMs = timestamp.UnixMilli()
				}
			}
		}

		// Calculate duration if both open and close times are set
		if metrics.OpenTimeMs > 0 && metrics.CloseTimeMs > 0 {
			metrics.Duration = metrics.CloseTimeMs - metrics.OpenTimeMs
		}

		// Add farmer to participants if not already included
		if invokingAccount, ok := rawMessage["invoking_account"].(string); ok && invokingAccount != "" {
			if !contains(metrics.Farmers, invokingAccount) {
				metrics.Farmers = append(metrics.Farmers, invokingAccount)
				metrics.Participants = len(metrics.Farmers)
			}
		}
	}

	// Forward metrics to processors
	return p.forwardToProcessors(ctx, metrics)
}

// getOrCreateBlockMetrics gets or creates block metrics for a given block index
func (p *KaleProcessor) getOrCreateBlockMetrics(blockIndex uint32) *KaleBlockMetrics {
	p.mu.Lock()
	defer p.mu.Unlock()

	metrics, exists := p.blockMetrics[blockIndex]
	if !exists {
		metrics = &KaleBlockMetrics{
			BlockIndex:   blockIndex,
			Timestamp:    time.Now(),
			Participants: 0,
			Farmers:      []string{},
		}
		p.blockMetrics[blockIndex] = metrics
	}
	return metrics
}

// forwardToProcessors forwards metrics to downstream processors
func (p *KaleProcessor) forwardToProcessors(ctx context.Context, metrics *KaleBlockMetrics) error {
	data, err := json.Marshal(metrics)
	if err != nil {
		return err
	}

	msg := types.Message{
		Payload: data,
	}

	for _, processor := range p.processors {
		if err := processor.Process(ctx, msg); err != nil {
			return fmt.Errorf("error in processor chain: %w", err)
		}
	}

	return nil
}

// updatePlantMetrics updates metrics for plant events
func (p *KaleProcessor) updatePlantMetrics(metrics *KaleBlockMetrics, data map[string]interface{}, farmerAddr string) {
	// Add farmer to participants if not already present
	if farmerAddr != "" && !contains(metrics.Farmers, farmerAddr) {
		metrics.Farmers = append(metrics.Farmers, farmerAddr)
		metrics.Participants = len(metrics.Farmers)
	}

	// Update total staked if available
	if stakeVal, ok := data["amount"]; ok {
		stake := p.parseAmount(stakeVal)
		metrics.TotalStaked += stake
	}
}

// updateWorkMetrics updates metrics for work events
func (p *KaleProcessor) updateWorkMetrics(metrics *KaleBlockMetrics, data map[string]interface{}, farmerAddr string) {
	// Add farmer to participants if not already present
	if farmerAddr != "" && !contains(metrics.Farmers, farmerAddr) {
		metrics.Farmers = append(metrics.Farmers, farmerAddr)
		metrics.Participants = len(metrics.Farmers)
	}

	// Update highest zero count if available
	if zerosVal, ok := data["zeros"]; ok {
		zeros := 0
		switch v := zerosVal.(type) {
		case float64:
			zeros = int(v)
		case string:
			z, err := strconv.Atoi(v)
			if err == nil {
				zeros = z
			}
		}

		if zeros > metrics.HighestZeroCount {
			metrics.HighestZeroCount = zeros
		}
	}
}

// updateHarvestMetrics updates metrics for harvest events
func (p *KaleProcessor) updateHarvestMetrics(metrics *KaleBlockMetrics, data map[string]interface{}, farmerAddr string) {
	// Add farmer to participants if not already present
	if farmerAddr != "" && !contains(metrics.Farmers, farmerAddr) {
		metrics.Farmers = append(metrics.Farmers, farmerAddr)
		metrics.Participants = len(metrics.Farmers)
	}

	// Update total reward if available
	if rewardVal, ok := data["reward"]; ok {
		reward := p.parseAmount(rewardVal)
		metrics.TotalReward += reward
	}

	// Update close time if available
	if closeTimeVal, ok := data["close_time"]; ok {
		switch v := closeTimeVal.(type) {
		case float64:
			metrics.CloseTimeMs = int64(v)
		case string:
			ct, err := strconv.ParseInt(v, 10, 64)
			if err == nil {
				metrics.CloseTimeMs = ct
			}
		}
	}

	// Calculate duration if both open and close times are set
	if metrics.OpenTimeMs > 0 && metrics.CloseTimeMs > 0 {
		metrics.Duration = metrics.CloseTimeMs - metrics.OpenTimeMs
	}
}

// parseAmount parses an amount value from various types
func (p *KaleProcessor) parseAmount(val interface{}) int64 {
	switch v := val.(type) {
	case float64:
		return int64(v)
	case string:
		// Remove any non-numeric characters except decimal point
		numStr := strings.Map(func(r rune) rune {
			if (r >= '0' && r <= '9') || r == '.' {
				return r
			}
			return -1
		}, v)

		// Parse as float first to handle decimal values
		f, err := strconv.ParseFloat(numStr, 64)
		if err == nil {
			return int64(f)
		}
	}
	return 0
}

// GetStats returns processor statistics
func (p *KaleProcessor) GetStats() struct {
	ProcessedEvents   uint64
	PlantEvents       uint64
	WorkEvents        uint64
	HarvestEvents     uint64
	MintEvents        uint64
	LastProcessedTime time.Time
} {
	p.mu.RLock()
	defer p.mu.RUnlock()
	return p.stats
}

// Helper function to check if a slice contains a string
func contains(slice []string, item string) bool {
	for _, s := range slice {
		if s == item {
			return true
		}
	}
	return false
}

// countLeadingZeros counts the number of leading zeros in a string
func countLeadingZeros(s string) uint32 {
	count := uint32(0)
	for _, char := range s {
		if char == '0' {
			count++
		} else {
			break
		}
	}
	return count
}

// max returns the maximum of two uint32 values
func max(a, b uint32) uint32 {
	if a > b {
		return a
	}
	return b
}
