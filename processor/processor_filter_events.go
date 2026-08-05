package processor

import (
	"context"
	"encoding/hex"
	"fmt"
	"log"
	"strings"

	"github.com/stellar/go-stellar-sdk/ingest"
	"github.com/stellar/go-stellar-sdk/xdr"
)

type FilterEventsProcessor struct {
	rules             []FilterRule
	processors        []Processor
	networkPassphrase string
}

type FilterRule struct {
	Field    string      `json:"field"`
	Operator string      `json:"operator"`
	Value    interface{} `json:"value"`
}

func NewFilterEventsProcessor(config map[string]interface{}) (*FilterEventsProcessor, error) {
	rulesData, ok := config["rules"].([]interface{})
	if !ok {
		return nil, fmt.Errorf("missing or invalid rules configuration")
	}

	networkPassphrase, ok := config["network_passphrase"].(string)
	if !ok {
		return nil, fmt.Errorf("invalid configuration for FilterEventsProcessor: missing 'network_passphrase'")
	}

	var rules []FilterRule
	for _, r := range rulesData {
		ruleMap, ok := r.(map[string]interface{})
		if !ok {
			continue
		}

		rule := FilterRule{
			Field:    ruleMap["field"].(string),
			Operator: ruleMap["operator"].(string),
			Value:    ruleMap["value"],
		}
		rules = append(rules, rule)
	}

	return &FilterEventsProcessor{
		rules:             rules,
		networkPassphrase: networkPassphrase,
	}, nil
}

func (f *FilterEventsProcessor) Subscribe(processor Processor) {
	f.processors = append(f.processors, processor)
}

func (f *FilterEventsProcessor) Process(ctx context.Context, msg Message) error {
	log.Printf("Processing message in FilterEventsProcessor")

	if msg.Payload == nil {
		return fmt.Errorf("nil payload received")
	}

	switch meta := msg.Payload.(type) {
	case xdr.LedgerCloseMeta:
		return f.processLedgerCloseMeta(ctx, meta)
	default:
		return fmt.Errorf("unsupported event type: %T", msg.Payload)
	}
}

func (f *FilterEventsProcessor) processLedgerCloseMeta(ctx context.Context, ledger xdr.LedgerCloseMeta) error {
	// Create transaction reader
	txReader, err := ingest.NewLedgerTransactionReaderFromLedgerCloseMeta(
		f.networkPassphrase,
		ledger,
	)
	if err != nil {
		return fmt.Errorf("error creating transaction reader: %w", err)
	}
	defer txReader.Close()

	// Process each transaction
	var tx ingest.LedgerTransaction
	for tx, err = txReader.Read(); err == nil; tx, err = txReader.Read() {
		// GetChanges is meta-version aware (V1 through V4) and covers both
		// transaction-level and operation-level changes. Contract data is written
		// by operations, so reading UnsafeMeta.V1.TxChanges alone missed it.
		changes, changesErr := tx.GetChanges()
		if changesErr != nil {
			log.Printf("Error getting changes: %v", changesErr)
			continue
		}

		// Process each change
		for _, change := range changes {
			// Check if it's contract data
			if change.Type != xdr.LedgerEntryTypeContractData {
				continue
			}

			// Inspect the resulting state, falling back to the prior state for
			// removals.
			entry := change.Post
			if entry == nil {
				entry = change.Pre
			}
			if entry == nil {
				continue
			}

			// Extract contract data
			contractData, ok := entry.Data.GetContractData()
			if !ok || contractData.Contract.ContractId == nil {
				continue
			}

			// Convert contract ID to string
			contractIDStr := hex.EncodeToString((*contractData.Contract.ContractId)[:])

			log.Printf("Found contract ID: %s", contractIDStr)

			// Check against rules
			matches := true
			for _, rule := range f.rules {
				switch rule.Field {
				case "contractId":
					if !f.compareValues(contractIDStr, rule.Operator, rule.Value) {
						matches = false
						log.Printf("Contract ID %s did not match rule value %v", contractIDStr, rule.Value)
					}
				}
				if !matches {
					break
				}
			}

			if !matches {
				continue
			}

			log.Printf("Found matching contract: %s", contractIDStr)

			// Forward the change as-is: it already carries the correct entry type
			// along with the pre/post states.
			changeMsg := Message{Payload: change}

			// Forward to next processors
			for _, processor := range f.processors {
				if err := processor.Process(ctx, changeMsg); err != nil {
					log.Printf("Error in processor: %v", err)
					// Continue processing even if one processor fails
					continue
				}
			}
		}
	}

	// Check if we ended because of an error
	if err != nil && err.Error() != "EOF" {
		return fmt.Errorf("error reading transactions: %w", err)
	}

	return nil
}

func (f *FilterEventsProcessor) compareValues(fieldValue interface{}, operator string, ruleValue interface{}) bool {
	// Convert values to strings for comparison
	fieldStr := fmt.Sprintf("%v", fieldValue)
	ruleStr := fmt.Sprintf("%v", ruleValue)

	switch operator {
	case "eq":
		return strings.EqualFold(fieldStr, ruleStr) // Case-insensitive comparison
	case "neq":
		return !strings.EqualFold(fieldStr, ruleStr)
	case "contains":
		return strings.Contains(strings.ToLower(fieldStr), strings.ToLower(ruleStr))
	case "startsWith":
		return strings.HasPrefix(strings.ToLower(fieldStr), strings.ToLower(ruleStr))
	case "endsWith":
		return strings.HasSuffix(strings.ToLower(fieldStr), strings.ToLower(ruleStr))
	default:
		return false
	}
}
