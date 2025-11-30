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

	// Bronze Layer Tables (Hubble-compatible schema)
	// These correspond to the 19 Bronze tables from BronzeExtractorsProcessor
	ProcessorTypeBronzeLedger              ProcessorType = "ledgers_row_v2"
	ProcessorTypeBronzeTransaction         ProcessorType = "transactions_row_v2"
	ProcessorTypeBronzeOperation           ProcessorType = "operations_row_v2"
	ProcessorTypeBronzeEffect              ProcessorType = "effects_row_v1"
	ProcessorTypeBronzeTrade               ProcessorType = "trades_row_v1"
	ProcessorTypeBronzeNativeBalance       ProcessorType = "native_balances_snapshot_v1"
	ProcessorTypeBronzeAccount             ProcessorType = "accounts_snapshot_v1"
	ProcessorTypeBronzeTrustline           ProcessorType = "trustlines_snapshot_v1"
	ProcessorTypeBronzeOffer               ProcessorType = "offers_snapshot_v1"
	ProcessorTypeBronzeClaimableBalance    ProcessorType = "claimable_balances_snapshot_v1"
	ProcessorTypeBronzeLiquidityPool       ProcessorType = "liquidity_pools_snapshot_v1"
	ProcessorTypeBronzeContractEvent       ProcessorType = "contract_events_stream_v1"
	ProcessorTypeBronzeContractData        ProcessorType = "contract_data_snapshot_v1"
	ProcessorTypeBronzeContractCode        ProcessorType = "contract_code_snapshot_v1"
	ProcessorTypeBronzeConfigSetting       ProcessorType = "config_settings_snapshot_v1"
	ProcessorTypeBronzeTTL                 ProcessorType = "ttl_snapshot_v1"
	ProcessorTypeBronzeEvictedKey          ProcessorType = "evicted_keys_state_v1"
	ProcessorTypeBronzeRestoredKey         ProcessorType = "restored_keys_state_v1"
	ProcessorTypeBronzeAccountSigner       ProcessorType = "account_signers_snapshot_v1"
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
		ProcessorTypeOperation,
		// Bronze layer tables
		ProcessorTypeBronzeLedger,
		ProcessorTypeBronzeTransaction,
		ProcessorTypeBronzeOperation,
		ProcessorTypeBronzeEffect,
		ProcessorTypeBronzeTrade,
		ProcessorTypeBronzeNativeBalance,
		ProcessorTypeBronzeAccount,
		ProcessorTypeBronzeTrustline,
		ProcessorTypeBronzeOffer,
		ProcessorTypeBronzeClaimableBalance,
		ProcessorTypeBronzeLiquidityPool,
		ProcessorTypeBronzeContractEvent,
		ProcessorTypeBronzeContractData,
		ProcessorTypeBronzeContractCode,
		ProcessorTypeBronzeConfigSetting,
		ProcessorTypeBronzeTTL,
		ProcessorTypeBronzeEvictedKey,
		ProcessorTypeBronzeRestoredKey,
		ProcessorTypeBronzeAccountSigner:
		return true
	default:
		return false
	}
}
