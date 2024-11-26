package processor

import (
	"context"
	"encoding/json"
	"fmt"
	"strconv"
	"time"

	"github.com/stellar/go/ingest"
	"github.com/stellar/go/xdr"
)

// Processor defines the interface for processing messages.
type Processor interface {
	Process(context.Context, Message) error
	Subscribe(Processor)
}

type ProcessorConfig struct {
	Type   string                 `yaml:"type"`
	Config map[string]interface{} `yaml:"config"`
}

// Message encapsulates the payload to be processed.
type Message struct {
	Payload interface{}
}

type AssetDetails struct {
	Code      string    `json:"code"`
	Issuer    string    `json:"issuer,omitempty"` // omitempty since native assets have no issuer
	Type      string    `json:"type"`             // native, credit_alphanum4, credit_alphanum12
	Timestamp time.Time `json:"timestamp"`
}

type AssetInfo struct {
	AssetDetails
	Stats      AssetStats `json:"stats"`
	HomeDomain string     `json:"home_domain,omitempty"`
	Anchor     struct {
		Name string `json:"name,omitempty"`
		URL  string `json:"url,omitempty"`
	} `json:"anchor,omitempty"`
	Verified    bool   `json:"verified"`
	DisplayName string `json:"display_name,omitempty"`
	Description string `json:"description,omitempty"`
}

// Helper functions for parsing data
func parseTimestamp(ts string) (time.Time, error) {
	i, err := strconv.ParseInt(ts, 10, 64)
	if err != nil {
		return time.Time{}, err
	}
	return time.Unix(i, 0), nil
}

// ExtractLedgerCloseMeta extracts xdr.LedgerCloseMeta from a processor.Message
func ExtractLedgerCloseMeta(msg Message) (xdr.LedgerCloseMeta, error) {
	ledgerCloseMeta, ok := msg.Payload.(xdr.LedgerCloseMeta)
	if !ok {
		return xdr.LedgerCloseMeta{}, fmt.Errorf("expected LedgerCloseMeta, got %T", msg.Payload)
	}
	return ledgerCloseMeta, nil
}

// ExtractLedgerTransaction extracts ingest.LedgerTransaction from a processor.Message
func ExtractLedgerTransaction(msg Message) (ingest.LedgerTransaction, error) {
	ledgerTransaction, ok := msg.Payload.(ingest.LedgerTransaction)
	if !ok {
		return ingest.LedgerTransaction{}, fmt.Errorf("expected LedgerTransaction, got %T", msg.Payload)
	}
	return ledgerTransaction, nil
}

// CreateTransactionReader creates a new ingest.LedgerTransactionReader from a processor.Message
func CreateTransactionReader(lcm xdr.LedgerCloseMeta, networkPassphrase string) (*ingest.LedgerTransactionReader, error) {
	return ingest.NewLedgerTransactionReaderFromLedgerCloseMeta(networkPassphrase, lcm)
}

// ForwardToProcessors marshals the payload and forwards it to all downstream processors
func ForwardToProcessors(ctx context.Context, payload interface{}, processors []Processor) error {
	jsonBytes, err := json.Marshal(payload)
	if err != nil {
		return fmt.Errorf("error marshaling payload: %w", err)
	}

	for _, processor := range processors {
		if err := processor.Process(ctx, Message{Payload: jsonBytes}); err != nil {
			return fmt.Errorf("error in processor chain: %w", err)
		}
	}

	return nil
}
