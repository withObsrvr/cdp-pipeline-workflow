package processor

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"strconv"
	"sync"
	"time"
)

// AccountDataFilter filters AccountRecord messages based on various criteria
type AccountDataFilter struct {
	processors    []Processor
	accountIDs    map[string]bool // Using a map for O(1) lookups
	startDate     *time.Time
	endDate       *time.Time
	dateField     string // "closed_at", "sequence_time", or "timestamp"
	changeTypes   map[string]bool
	minBalance    *int64
	deletedOnly   bool
	mu            sync.RWMutex
	stats         struct {
		ProcessedRecords int64
		FilteredRecords  int64
		LastProcessTime  time.Time
	}
}

// NewAccountDataFilter creates a new AccountDataFilter processor
func NewAccountDataFilter(config map[string]interface{}) (*AccountDataFilter, error) {
	filter := &AccountDataFilter{
		accountIDs:  make(map[string]bool),
		changeTypes: make(map[string]bool),
		dateField:   "closed_at", // Default date field
	}

	// Handle single account_id (string)
	if accountID, ok := config["account_id"].(string); ok && accountID != "" {
		filter.accountIDs[accountID] = true
	}

	// Handle account_ids (array)
	if accountIDsArray, ok := config["account_ids"].([]interface{}); ok {
		for _, id := range accountIDsArray {
			if strID, ok := id.(string); ok && strID != "" {
				filter.accountIDs[strID] = true
			}
		}
	}

	// Handle date range filtering
	if startDateStr, ok := config["start_date"].(string); ok && startDateStr != "" {
		startDate, err := time.Parse(time.RFC3339, startDateStr)
		if err != nil {
			return nil, fmt.Errorf("invalid start_date format (expected RFC3339): %w", err)
		}
		filter.startDate = &startDate
	}

	if endDateStr, ok := config["end_date"].(string); ok && endDateStr != "" {
		endDate, err := time.Parse(time.RFC3339, endDateStr)
		if err != nil {
			return nil, fmt.Errorf("invalid end_date format (expected RFC3339): %w", err)
		}
		filter.endDate = &endDate
	}

	// Handle date field selection
	if dateField, ok := config["date_field"].(string); ok && dateField != "" {
		switch dateField {
		case "closed_at", "sequence_time", "timestamp":
			filter.dateField = dateField
		default:
			return nil, fmt.Errorf("invalid date_field: %s (must be 'closed_at', 'sequence_time', or 'timestamp')", dateField)
		}
	}

	// Handle change type filtering
	if changeTypesArray, ok := config["change_types"].([]interface{}); ok {
		for _, ct := range changeTypesArray {
			if strType, ok := ct.(string); ok && strType != "" {
				switch strType {
				case "created", "updated", "removed", "state":
					filter.changeTypes[strType] = true
				default:
					return nil, fmt.Errorf("invalid change_type: %s (must be 'created', 'updated', 'removed', or 'state')", strType)
				}
			}
		}
	}

	// Handle minimum balance filtering
	if minBalanceStr, ok := config["min_balance"].(string); ok && minBalanceStr != "" {
		minBalance, err := strconv.ParseInt(minBalanceStr, 10, 64)
		if err != nil {
			return nil, fmt.Errorf("invalid min_balance format: %w", err)
		}
		filter.minBalance = &minBalance
	}

	// Handle deleted_only flag
	if deletedOnly, ok := config["deleted_only"].(bool); ok {
		filter.deletedOnly = deletedOnly
	}

	// Log the filter configuration
	log.Printf("AccountDataFilter: Configured with the following filters:")
	if len(filter.accountIDs) > 0 {
		log.Printf("  - Account IDs: %d accounts", len(filter.accountIDs))
		for id := range filter.accountIDs {
			log.Printf("    - %s", id)
		}
	}
	if filter.startDate != nil {
		log.Printf("  - Start date: %s", filter.startDate.Format(time.RFC3339))
	}
	if filter.endDate != nil {
		log.Printf("  - End date: %s", filter.endDate.Format(time.RFC3339))
	}
	log.Printf("  - Date field: %s", filter.dateField)
	if len(filter.changeTypes) > 0 {
		log.Printf("  - Change types: %v", getMapKeys(filter.changeTypes))
	}
	if filter.minBalance != nil {
		log.Printf("  - Minimum balance: %d stroops", *filter.minBalance)
	}
	if filter.deletedOnly {
		log.Printf("  - Deleted accounts only: true")
	}

	return filter, nil
}

// Subscribe adds a processor to the chain
func (f *AccountDataFilter) Subscribe(processor Processor) {
	f.processors = append(f.processors, processor)
}

// Process processes an AccountRecord message and applies filtering
func (f *AccountDataFilter) Process(ctx context.Context, msg Message) error {
	f.mu.Lock()
	f.stats.ProcessedRecords++
	f.mu.Unlock()

	// Parse the AccountRecord from JSON
	var accountRecord AccountRecord
	if jsonBytes, ok := msg.Payload.([]byte); ok {
		if err := json.Unmarshal(jsonBytes, &accountRecord); err != nil {
			return fmt.Errorf("error unmarshaling AccountRecord: %w", err)
		}
	} else {
		return fmt.Errorf("invalid payload type: expected []byte, got %T", msg.Payload)
	}

	// Apply filters
	if !f.passesFilters(&accountRecord) {
		f.mu.Lock()
		f.stats.FilteredRecords++
		f.mu.Unlock()
		return nil // Filtered out, don't forward
	}

	// Forward to downstream processors
	for i, processor := range f.processors {
		if err := processor.Process(ctx, msg); err != nil {
			return fmt.Errorf("error in downstream processor %d: %w", i, err)
		}
	}

	f.mu.Lock()
	f.stats.LastProcessTime = time.Now()
	f.mu.Unlock()

	return nil
}

// passesFilters checks if an AccountRecord passes all configured filters
func (f *AccountDataFilter) passesFilters(record *AccountRecord) bool {
	// Account ID filter
	if len(f.accountIDs) > 0 {
		if _, exists := f.accountIDs[record.AccountID]; !exists {
			return false
		}
	}

	// Date range filter
	if f.startDate != nil || f.endDate != nil {
		var recordTime time.Time
		switch f.dateField {
		case "closed_at":
			recordTime = record.ClosedAt
		case "sequence_time":
			recordTime = record.SequenceTime
		case "timestamp":
			recordTime = record.Timestamp
		}

		if f.startDate != nil && recordTime.Before(*f.startDate) {
			return false
		}
		if f.endDate != nil && recordTime.After(*f.endDate) {
			return false
		}
	}

	// Change type filter
	if len(f.changeTypes) > 0 {
		changeType := getChangeTypeString(record.LedgerEntryChange)
		if _, exists := f.changeTypes[changeType]; !exists {
			return false
		}
	}

	// Minimum balance filter
	if f.minBalance != nil && !record.Deleted {
		balance, err := strconv.ParseInt(record.Balance, 10, 64)
		if err != nil {
			log.Printf("Warning: Could not parse balance '%s' for account %s", record.Balance, record.AccountID)
			return false
		}
		if balance < *f.minBalance {
			return false
		}
	}

	// Deleted only filter
	if f.deletedOnly && !record.Deleted {
		return false
	}

	return true
}

// GetStats returns the current filter statistics
func (f *AccountDataFilter) GetStats() struct {
	ProcessedRecords int64
	FilteredRecords  int64
	LastProcessTime  time.Time
} {
	f.mu.RLock()
	defer f.mu.RUnlock()
	return f.stats
}

// Helper function to get change type string from uint32
func getChangeTypeString(changeType uint32) string {
	switch changeType {
	case 1:
		return "created"
	case 2:
		return "updated"
	case 3:
		return "state"
	default:
		return "unknown"
	}
}

// Helper function to get keys from a map
func getMapKeys(m map[string]bool) []string {
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	return keys
}