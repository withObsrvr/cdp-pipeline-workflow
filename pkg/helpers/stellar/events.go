package stellar

import (
	"fmt"
	"math"

	"github.com/stellar/go/xdr"
)

// TokenEventType represents the type of token event
type TokenEventType string

const (
	EventTypeMint     TokenEventType = "mint"
	EventTypeBurn     TokenEventType = "burn"
	EventTypeTransfer TokenEventType = "transfer"
	EventTypeUnknown  TokenEventType = "unknown"
)

// DetermineTokenEventType determines if a token operation is mint, burn, or transfer
func DetermineTokenEventType(from, to, assetIssuer string) TokenEventType {
	if assetIssuer == "" {
		// Native asset, always transfer
		return EventTypeTransfer
	}
	
	// Mint: issuer sends to non-issuer
	if from == assetIssuer && to != assetIssuer {
		return EventTypeMint
	}
	
	// Burn: non-issuer sends to issuer
	if from != assetIssuer && to == assetIssuer {
		return EventTypeBurn
	}
	
	// Transfer: between non-issuers (or issuer to issuer)
	return EventTypeTransfer
}

// ExtractEventTopic extracts a specific topic value from contract event
func ExtractEventTopic(event xdr.ContractEvent, index int) (*xdr.ScVal, error) {
	if event.Body.V != 0 {
		return nil, fmt.Errorf("unsupported event body version: %d", event.Body.V)
	}
	
	topics := event.Body.V0.Topics
	if index >= len(topics) {
		return nil, fmt.Errorf("topic index %d out of range (have %d topics)", index, len(topics))
	}
	
	return &topics[index], nil
}

// ExtractEventSymbol extracts a symbol from event topics
func ExtractEventSymbol(event xdr.ContractEvent, topicIndex int) (string, error) {
	topic, err := ExtractEventTopic(event, topicIndex)
	if err != nil {
		return "", err
	}
	
	if topic.Type != xdr.ScValTypeScvSymbol {
		return "", fmt.Errorf("topic at index %d is not a symbol", topicIndex)
	}
	
	return string(topic.MustSym()), nil
}

// IsContractEvent checks if an event is a contract event
func IsContractEvent(event xdr.ContractEvent) bool {
	return event.Type == xdr.ContractEventTypeContract
}

// EventContractID returns the contract ID as a string
func EventContractID(event xdr.ContractEvent) (string, error) {
	return EncodeContractID(event.ContractId)
}

// ExtractI128Amount extracts an i128 amount from ScVal
func ExtractI128Amount(val xdr.ScVal) (int64, error) {
	if val.Type != xdr.ScValTypeScvI128 {
		return 0, fmt.Errorf("value is not i128 type")
	}
	
	i128 := val.MustI128()
	
	// Check if high part is 0 (fits in int64)
	if i128.Hi != 0 {
		return 0, fmt.Errorf("i128 value too large for int64")
	}
	
	// Check if low part fits in int64
	if i128.Lo > uint64(math.MaxInt64) {
		return 0, fmt.Errorf("i128 value too large for int64")
	}
	
	return int64(i128.Lo), nil
}

// ExtractU64Amount extracts a u64 amount from ScVal
func ExtractU64Amount(val xdr.ScVal) (int64, error) {
	if val.Type != xdr.ScValTypeScvU64 {
		return 0, fmt.Errorf("value is not u64 type")
	}
	
	u64Val := val.MustU64()
	
	// Check if fits in int64
	if u64Val > uint64(math.MaxInt64) {
		return 0, fmt.Errorf("u64 value too large for int64")
	}
	
	return int64(u64Val), nil
}

// ExtractAddressFromEvent extracts address from event data
func ExtractAddressFromEvent(val xdr.ScVal) (string, error) {
	return ExtractAddressFromScVal(val)
}

// GetEventData returns the data field of a contract event
func GetEventData(event xdr.ContractEvent) *xdr.ScVal {
	if event.Body.V != 0 {
		return nil
	}
	return &event.Body.V0.Data
}