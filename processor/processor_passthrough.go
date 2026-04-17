package processor

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"

	"github.com/stellar/go/xdr"
)

// PassthroughProcessor passes ledger data through with minimal transformation
// It maintains the original XDR LedgerCloseMeta structure while optionally
// adding lightweight metadata for downstream processing
type PassthroughProcessor struct {
	subscribers       []Processor
	addMetadata      bool
	includeFullXDR   bool
	includeXDRJSON   bool
	networkPassphrase string
}

// NewPassthroughProcessor creates a new passthrough processor
func NewPassthroughProcessor(config map[string]interface{}) (*PassthroughProcessor, error) {
	networkPassphrase := "Public Global Stellar Network ; September 2015" // Default mainnet
	addMetadata := false
	includeFullXDR := true   // Default to true for data preservation
	includeXDRJSON := false  // Default to false as it can be large

	// Parse configuration
	if val, ok := config["network_passphrase"].(string); ok {
		networkPassphrase = val
	}
	if val, ok := config["add_metadata"].(bool); ok {
		addMetadata = val
	}
	if val, ok := config["include_full_xdr"].(bool); ok {
		includeFullXDR = val
	}
	if val, ok := config["include_xdr_json"].(bool); ok {
		includeXDRJSON = val
	}

	return &PassthroughProcessor{
		subscribers:       []Processor{},
		addMetadata:      addMetadata,
		includeFullXDR:   includeFullXDR,
		includeXDRJSON:   includeXDRJSON,
		networkPassphrase: networkPassphrase,
	}, nil
}

// ProcessorPassthrough creates a new passthrough processor from config
func ProcessorPassthrough(config map[string]interface{}) Processor {
	proc, _ := NewPassthroughProcessor(config)
	return proc
}

// Subscribe adds a subscriber to receive processed messages
func (p *PassthroughProcessor) Subscribe(subscriber Processor) {
	p.subscribers = append(p.subscribers, subscriber)
}

// Process forwards the ledger data as JSON with key fields extracted
func (p *PassthroughProcessor) Process(ctx context.Context, msg Message) error {
	// Extract LedgerCloseMeta
	ledgerCloseMeta, ok := msg.Payload.(*xdr.LedgerCloseMeta)
	if !ok {
		// Check if it's already a value (not pointer)
		if lcm, ok := msg.Payload.(xdr.LedgerCloseMeta); ok {
			ledgerCloseMeta = &lcm
		} else {
			return fmt.Errorf("expected LedgerCloseMeta, got %T", msg.Payload)
		}
	}

	// Extract key information
	header := ledgerCloseMeta.LedgerHeaderHistoryEntry()
	
	// Build output structure with key fields
	output := map[string]interface{}{
		// Core ledger information
		"ledger_sequence":  float64(ledgerCloseMeta.LedgerSequence()),
		"ledger_hash":      ledgerCloseMeta.LedgerHash().HexString(),
		"protocol_version": float64(ledgerCloseMeta.ProtocolVersion()),
		
		// Header information
		"close_time":       float64(header.Header.ScpValue.CloseTime),
		"base_fee":         float64(header.Header.BaseFee),
		"base_reserve":     float64(header.Header.BaseReserve),
		"max_tx_set_size":  float64(header.Header.MaxTxSetSize),
		
		// Transaction count
		"transaction_count": float64(p.getTransactionCount(ledgerCloseMeta)),
		
		// Ledger version (V0 or V1)
		"ledger_version": float64(p.getLedgerVersion(ledgerCloseMeta)),
	}
	
	// Add V1-specific fields if applicable
	if ledgerCloseMeta.V1 != nil {
		output["total_byte_size_of_soroban_state"] = float64(ledgerCloseMeta.V1.TotalByteSizeOfLiveSorobanState)
		output["evicted_keys_count"] = float64(len(ledgerCloseMeta.V1.EvictedKeys))
	}
	
	// Include full XDR as base64
	if p.includeFullXDR {
		xdrBytes, err := ledgerCloseMeta.MarshalBinary()
		if err != nil {
			return fmt.Errorf("failed to marshal XDR: %w", err)
		}
		output["ledger_xdr_base64"] = base64.StdEncoding.EncodeToString(xdrBytes)
		output["ledger_xdr_size_bytes"] = float64(len(xdrBytes))
	}
	
	// Note: includeXDRJSON is reserved for future use when xdr2json becomes publicly available
	// For now, we only support base64 encoding of the full XDR
	
	// Convert to JSON bytes
	jsonBytes, err := json.Marshal(output)
	if err != nil {
		return fmt.Errorf("failed to marshal output: %w", err)
	}
	
	// Create output message with JSON payload
	outputMsg := Message{
		Payload:  jsonBytes,
		Metadata: msg.Metadata,
	}
	
	// Initialize metadata if nil
	if outputMsg.Metadata == nil {
		outputMsg.Metadata = make(map[string]interface{})
	}
	
	// Add processing metadata
	if p.addMetadata {
		outputMsg.Metadata["processor_passthrough"] = true
		outputMsg.Metadata["ledger_sequence"] = ledgerCloseMeta.LedgerSequence()
		outputMsg.Metadata["output_format"] = "json"
	}
	
	// Forward to all subscribers
	for _, subscriber := range p.subscribers {
		if err := subscriber.Process(ctx, outputMsg); err != nil {
			return fmt.Errorf("error in subscriber processing: %w", err)
		}
	}

	return nil
}

// getTransactionCount returns the number of transactions in the ledger
func (p *PassthroughProcessor) getTransactionCount(lcm *xdr.LedgerCloseMeta) int {
	if lcm.V0 != nil {
		return len(lcm.V0.TxSet.Txs)
	} else if lcm.V1 != nil {
		return len(lcm.V1.TxProcessing)
	}
	return 0
}

// getLedgerVersion returns the ledger format version (0 for V0, 1 for V1)
func (p *PassthroughProcessor) getLedgerVersion(lcm *xdr.LedgerCloseMeta) int {
	if lcm.V0 != nil {
		return 0
	} else if lcm.V1 != nil {
		return 1
	}
	return -1
}