package processor

import "time"

// ProcessorType represents the type of processor that emitted a message.
// This is used by the DuckLake Schema Registry to determine which schema
// to use when creating tables and inserting data.
type ProcessorType string

// Standard processor type constants
const (
	// Core ledger processors
	ProcessorTypeLedger      ProcessorType = "ledger"
	ProcessorTypeTransaction ProcessorType = "transaction"
	ProcessorTypeLedgerChange ProcessorType = "ledger_change"

	// Contract processors
	ProcessorTypeContractInvocation ProcessorType = "contract_invocation"
	ProcessorTypeContractEvent      ProcessorType = "contract_event"
	ProcessorTypeContractData       ProcessorType = "contract_data"
	ProcessorTypeContractCreation   ProcessorType = "contract_creation"

	// Operation processors
	ProcessorTypePayment            ProcessorType = "payment"
	ProcessorTypeCreateAccount      ProcessorType = "create_account"
	ProcessorTypeAccountData        ProcessorType = "account_data"
	ProcessorTypeAccountEffects     ProcessorType = "account_effects"
	ProcessorTypeAccountTransaction ProcessorType = "account_transaction"

	// Market processors
	ProcessorTypeTrade         ProcessorType = "trade"
	ProcessorTypeOrderbook     ProcessorType = "orderbook"
	ProcessorTypeMarketMetrics ProcessorType = "market_metrics"
	ProcessorTypeTickerAsset   ProcessorType = "ticker_asset"

	// Asset processors
	ProcessorTypeAsset      ProcessorType = "asset"
	ProcessorTypeAssetStats ProcessorType = "asset_stats"
	ProcessorTypeTokenPrice ProcessorType = "token_price"

	// Other processors
	ProcessorTypeClaimableBalance ProcessorType = "claimable_balance"
	ProcessorTypeTrustline        ProcessorType = "trustline"
	ProcessorTypeEffects          ProcessorType = "effects"
	ProcessorTypeOperation        ProcessorType = "operation"
)

// ProcessorMetadata is a standard metadata structure that should be
// included in Message.Metadata to enable schema registry detection.
// This is the recommended way to add processor type identification
// to messages.
//
// Example usage in a processor:
//
//	outputMsg := Message{
//	    Payload: jsonBytes,
//	    Metadata: map[string]interface{}{
//	        "processor_type": string(processor.ProcessorTypeContractInvocation),
//	        "processor_name": "ContractInvocationProcessor",
//	        "version":        "1.0.0",
//	        "timestamp":      time.Now(),
//	    },
//	}
type ProcessorMetadata struct {
	ProcessorType ProcessorType `json:"processor_type"`
	ProcessorName string        `json:"processor_name"`
	Version       string        `json:"version"`
	Timestamp     time.Time     `json:"timestamp"`
}

// String returns the string representation of the ProcessorType
func (pt ProcessorType) String() string {
	return string(pt)
}

// IsValid checks if the processor type is a known/valid type
func (pt ProcessorType) IsValid() bool {
	switch pt {
	case ProcessorTypeLedger,
		ProcessorTypeTransaction,
		ProcessorTypeLedgerChange,
		ProcessorTypeContractInvocation,
		ProcessorTypeContractEvent,
		ProcessorTypeContractData,
		ProcessorTypeContractCreation,
		ProcessorTypePayment,
		ProcessorTypeCreateAccount,
		ProcessorTypeAccountData,
		ProcessorTypeAccountEffects,
		ProcessorTypeAccountTransaction,
		ProcessorTypeTrade,
		ProcessorTypeOrderbook,
		ProcessorTypeMarketMetrics,
		ProcessorTypeTickerAsset,
		ProcessorTypeAsset,
		ProcessorTypeAssetStats,
		ProcessorTypeTokenPrice,
		ProcessorTypeClaimableBalance,
		ProcessorTypeTrustline,
		ProcessorTypeEffects,
		ProcessorTypeOperation:
		return true
	default:
		return false
	}
}
