package processor

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"
	"time"

	"github.com/stellar/go/xdr"
)

// AssetEvent represents a processed asset event
type AssetEvent struct {
	AssetCode      string    `json:"asset_code"`
	AssetIssuer    string    `json:"asset_issuer"`
	AssetType      string    `json:"asset_type"`
	AssetID        int64     `json:"asset_id"`
	Timestamp      time.Time `json:"timestamp"`
	LedgerSequence uint32    `json:"ledger_sequence"`
	OperationType  string    `json:"operation_type"`
	TxHash         string    `json:"tx_hash"`
}

type AssetProcessor struct {
	processors []Processor
	mu         sync.RWMutex
	stats      struct {
		ProcessedEvents  uint64
		PaymentEvents    uint64
		ManageSellEvents uint64
		LastEventTime    time.Time
	}
}

func NewAssetProcessor() *AssetProcessor {
	return &AssetProcessor{
		processors: make([]Processor, 0),
	}
}

func (p *AssetProcessor) Subscribe(processor Processor) {
	p.processors = append(p.processors, processor)
}

func (p *AssetProcessor) Process(ctx context.Context, msg Message) error {
	var contractEvent ContractEvent
	switch payload := msg.Payload.(type) {
	case []byte:
		if err := json.Unmarshal(payload, &contractEvent); err != nil {
			return fmt.Errorf("error decoding contract event: %w", err)
		}
	default:
		return fmt.Errorf("unexpected payload type: %T", msg.Payload)
	}

	// Parse the operation
	var operation xdr.Operation
	if err := json.Unmarshal(contractEvent.Data, &operation); err != nil {
		return fmt.Errorf("error parsing operation: %w", err)
	}

	opType := operation.Body.Type
	if opType != xdr.OperationTypePayment && opType != xdr.OperationTypeManageSellOffer {
		return nil // Skip non-asset operations
	}

	// Extract asset based on operation type
	var asset xdr.Asset
	switch opType {
	case xdr.OperationTypeManageSellOffer:
		if op, ok := operation.Body.GetManageSellOfferOp(); ok {
			asset = op.Selling
		}
	case xdr.OperationTypePayment:
		if op, ok := operation.Body.GetPaymentOp(); ok {
			asset = op.Asset
		}
	}

	// Extract asset details
	var assetType, assetCode, assetIssuer string
	if err := asset.Extract(&assetType, &assetCode, &assetIssuer); err != nil {
		return fmt.Errorf("error extracting asset details: %w", err)
	}

	// Create asset event
	event := AssetEvent{
		AssetCode:      assetCode,
		AssetIssuer:    assetIssuer,
		AssetType:      assetType,
		AssetID:        computeAssetID(assetCode, assetIssuer, assetType),
		Timestamp:      contractEvent.Timestamp,
		LedgerSequence: contractEvent.LedgerSequence,
		OperationType:  opType.String(),
		TxHash:         contractEvent.TransactionHash,
	}

	// Update stats
	p.mu.Lock()
	p.stats.ProcessedEvents++
	switch opType {
	case xdr.OperationTypePayment:
		p.stats.PaymentEvents++
	case xdr.OperationTypeManageSellOffer:
		p.stats.ManageSellEvents++
	}
	p.stats.LastEventTime = time.Now()
	p.mu.Unlock()

	// Forward to downstream processors
	eventBytes, err := json.Marshal(event)
	if err != nil {
		return fmt.Errorf("error marshaling asset event: %w", err)
	}

	for _, processor := range p.processors {
		if err := processor.Process(ctx, Message{Payload: eventBytes}); err != nil {
			return fmt.Errorf("error in processor chain: %w", err)
		}
	}

	return nil
}

func (p *AssetProcessor) GetStats() struct {
	ProcessedEvents  uint64
	PaymentEvents    uint64
	ManageSellEvents uint64
	LastEventTime    time.Time
} {
	p.mu.RLock()
	defer p.mu.RUnlock()
	return p.stats
}

// Helper function to compute asset ID
func computeAssetID(code, issuer, assetType string) int64 {
	// Implement your asset ID computation logic here
	// You might want to use the same logic as utils.FarmHashAsset
	return 0 // Placeholder
}
