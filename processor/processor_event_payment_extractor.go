package processor

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"sync"
	"time"
)

// EventPayment represents a structured payment event from Kwickbit's payment processor contract
type EventPayment struct {
	ID             string    `json:"id"`
	PaymentID      string    `json:"payment_id"`        // Hex string with 0x prefix
	TokenID        string    `json:"token_id"`          // Token contract address
	Amount         uint64    `json:"amount"`            // Payment amount
	FromID         string    `json:"from_id"`           // Sender account address
	MerchantID     string    `json:"merchant_id"`       // Merchant account address
	RoyaltyAmount  uint64    `json:"royalty_amount"`    // Royalty amount
	TxHash         string    `json:"tx_hash"`           // Transaction hash
	BlockHeight    uint32    `json:"block_height"`      // Ledger sequence
	BlockTimestamp time.Time `json:"block_timestamp"`   // Ledger close time
}

// EventPaymentExtractor extracts structured EventPayment data from ContractEvent messages
// Specifically designed for Kwickbit's payment processor contract events
type EventPaymentExtractor struct {
	processors []Processor
	stats      EventPaymentStats
	mu         sync.RWMutex
}

// EventPaymentStats tracks extraction statistics
type EventPaymentStats struct {
	ProcessedLedgers uint32
	TotalEvents      uint64
	ExtractedPayments uint64
	SkippedEvents    uint64
	Errors           uint64
}

// NewEventPaymentExtractor creates a new EventPaymentExtractor processor
func NewEventPaymentExtractor(config map[string]interface{}) (*EventPaymentExtractor, error) {
	log.Printf("EventPaymentExtractor: Initializing processor")

	return &EventPaymentExtractor{
		processors: make([]Processor, 0),
		stats:      EventPaymentStats{},
	}, nil
}

// Subscribe adds a processor to receive extracted EventPayment messages
func (p *EventPaymentExtractor) Subscribe(processor Processor) {
	p.processors = append(p.processors, processor)
}

// Process extracts EventPayment from ContractEvent messages
func (p *EventPaymentExtractor) Process(ctx context.Context, msg Message) error {
	// Expect ContractEvent payload
	contractEvent, ok := msg.Payload.(*ContractEvent)
	if !ok {
		return fmt.Errorf("expected *ContractEvent payload, got %T", msg.Payload)
	}

	p.mu.Lock()
	p.stats.TotalEvents++
	p.mu.Unlock()

	// Only process "payment" event types
	if contractEvent.EventType != "payment" {
		p.mu.Lock()
		p.stats.SkippedEvents++
		p.mu.Unlock()
		return nil
	}

	// Extract EventPayment from the contract event
	eventPayment, err := p.extractEventPayment(contractEvent)
	if err != nil {
		p.mu.Lock()
		p.stats.Errors++
		p.mu.Unlock()
		log.Printf("EventPaymentExtractor: Error extracting payment from ledger %d: %v",
			contractEvent.LedgerSequence, err)
		return nil // Don't stop processing, just skip this event
	}

	p.mu.Lock()
	p.stats.ExtractedPayments++
	p.mu.Unlock()

	log.Printf("EventPaymentExtractor: Extracted payment_id=%s, amount=%d, merchant=%s, block=%d",
		eventPayment.PaymentID, eventPayment.Amount, eventPayment.MerchantID, eventPayment.BlockHeight)

	// Forward to subscribed processors/consumers
	eventPaymentMsg := Message{
		Payload: eventPayment,
	}

	for _, processor := range p.processors {
		if err := processor.Process(ctx, eventPaymentMsg); err != nil {
			log.Printf("EventPaymentExtractor: Error forwarding to processor: %v", err)
		}
	}

	return nil
}

// extractEventPayment extracts EventPayment structure from ContractEvent
func (p *EventPaymentExtractor) extractEventPayment(event *ContractEvent) (*EventPayment, error) {
	// The decoded data should be a map
	dataDecoded, ok := event.DataDecoded.(map[string]interface{})
	if !ok {
		return nil, fmt.Errorf("data_decoded is not a map, got %T", event.DataDecoded)
	}

	entries, ok := dataDecoded["entries"].(map[string]interface{})
	if !ok {
		return nil, fmt.Errorf("data_decoded.entries is not a map, got %T", dataDecoded["entries"])
	}

	// Extract payment_id (bytes -> hex string)
	paymentID, err := extractPaymentID(entries["payment_id"])
	if err != nil {
		return nil, fmt.Errorf("failed to extract payment_id: %w", err)
	}

	// Extract token address
	tokenID, err := extractAddress(entries["token"])
	if err != nil {
		return nil, fmt.Errorf("failed to extract token: %w", err)
	}

	// Extract amount
	amount, err := extractUint64(entries["amount"])
	if err != nil {
		return nil, fmt.Errorf("failed to extract amount: %w", err)
	}

	// Extract from address
	fromID, err := extractAddress(entries["from"])
	if err != nil {
		return nil, fmt.Errorf("failed to extract from: %w", err)
	}

	// Extract merchant address
	merchantID, err := extractAddress(entries["merchant"])
	if err != nil {
		return nil, fmt.Errorf("failed to extract merchant: %w", err)
	}

	// Extract royalty_amount
	royaltyAmount, err := extractUint64(entries["royalty_amount"])
	if err != nil {
		return nil, fmt.Errorf("failed to extract royalty_amount: %w", err)
	}

	// Generate unique ID: ledger-txhash[:8]-eventindex
	id := fmt.Sprintf("%d-%s-%d",
		event.LedgerSequence,
		event.TransactionHash[:min(8, len(event.TransactionHash))],
		event.EventIndex,
	)

	return &EventPayment{
		ID:             id,
		PaymentID:      paymentID,
		TokenID:        tokenID,
		Amount:         amount,
		FromID:         fromID,
		MerchantID:     merchantID,
		RoyaltyAmount:  royaltyAmount,
		TxHash:         event.TransactionHash,
		BlockHeight:    event.LedgerSequence,
		BlockTimestamp: event.Timestamp,
	}, nil
}

// extractPaymentID converts payment_id bytes to hex string with 0x prefix
func extractPaymentID(value interface{}) (string, error) {
	if value == nil {
		return "", fmt.Errorf("payment_id is nil")
	}

	// Value should be a map with "hex" field
	valMap, ok := value.(map[string]interface{})
	if !ok {
		return "", fmt.Errorf("payment_id is not a map, got %T", value)
	}

	hexStr, ok := valMap["hex"].(string)
	if !ok {
		return "", fmt.Errorf("payment_id.hex is not a string, got %T", valMap["hex"])
	}

	// Add 0x prefix if not present
	if len(hexStr) >= 2 && hexStr[:2] != "0x" {
		hexStr = "0x" + hexStr
	} else if len(hexStr) > 0 && len(hexStr) < 2 {
		// If hexStr is too short but not empty, add 0x prefix anyway
		hexStr = "0x" + hexStr
	}

	return hexStr, nil
}

// extractAddress extracts an address string from various formats
func extractAddress(value interface{}) (string, error) {
	if value == nil {
		return "", fmt.Errorf("address is nil")
	}

	// Value should be a map with "address" field
	valMap, ok := value.(map[string]interface{})
	if !ok {
		return "", fmt.Errorf("address value is not a map, got %T", value)
	}

	address, ok := valMap["address"].(string)
	if !ok {
		return "", fmt.Errorf("address.address is not a string, got %T", valMap["address"])
	}

	if address == "" {
		return "", fmt.Errorf("address is empty")
	}

	return address, nil
}

// extractUint64 extracts a uint64 value from interface{}
func extractUint64(value interface{}) (uint64, error) {
	if value == nil {
		return 0, fmt.Errorf("value is nil")
	}

	switch v := value.(type) {
	case uint64:
		return v, nil
	case int64:
		if v < 0 {
			return 0, fmt.Errorf("negative value not allowed: %d", v)
		}
		return uint64(v), nil
	case float64:
		if v < 0 {
			return 0, fmt.Errorf("negative value not allowed: %f", v)
		}
		return uint64(v), nil
	case json.Number:
		val, err := v.Int64()
		if err != nil {
			return 0, fmt.Errorf("failed to convert json.Number to int64: %w", err)
		}
		if val < 0 {
			return 0, fmt.Errorf("negative value not allowed: %d", val)
		}
		return uint64(val), nil
	default:
		return 0, fmt.Errorf("unsupported type for uint64: %T", value)
	}
}

// GetStats returns current processing statistics
func (p *EventPaymentExtractor) GetStats() EventPaymentStats {
	p.mu.RLock()
	defer p.mu.RUnlock()
	return p.stats
}

// Name returns the processor name
func (p *EventPaymentExtractor) Name() string {
	return "EventPaymentExtractor"
}

// Helper function for min
func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}

// Close implements io.Closer interface
func (p *EventPaymentExtractor) Close() error {
	p.mu.Lock()
	defer p.mu.Unlock()

	log.Printf("EventPaymentExtractor Stats: Processed %d events, Extracted %d payments, Skipped %d, Errors %d",
		p.stats.TotalEvents, p.stats.ExtractedPayments, p.stats.SkippedEvents, p.stats.Errors)

	return nil
}

// Ensure EventPaymentExtractor implements Processor and io.Closer interfaces
var _ Processor = (*EventPaymentExtractor)(nil)
var _ io.Closer = (*EventPaymentExtractor)(nil)
