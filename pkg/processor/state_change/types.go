package state_change

import (
	"time"

	"github.com/stellar/go/xdr"
)

// StateChangeType represents the type of state change
type StateChangeType string

const (
	// Token movements
	StateChangeTypeCredit    StateChangeType = "CREDIT"
	StateChangeTypeDebit     StateChangeType = "DEBIT"
	StateChangeTypeMint      StateChangeType = "MINT"
	StateChangeTypeBurn      StateChangeType = "BURN"
	StateChangeTypeClawback  StateChangeType = "CLAWBACK"
	
	// Account configuration
	StateChangeTypeSigner    StateChangeType = "SIGNER"
	StateChangeTypeThreshold StateChangeType = "THRESHOLD"
	StateChangeTypeFlags     StateChangeType = "FLAGS"
	StateChangeTypeInflation StateChangeType = "INFLATION"
	
	// Metadata
	StateChangeTypeHomeDomain StateChangeType = "HOME_DOMAIN"
	StateChangeTypeDataEntry  StateChangeType = "DATA_ENTRY"
	
	// Sponsorship
	StateChangeTypeSponsorship StateChangeType = "SPONSORSHIP"
	
	// Contract
	StateChangeTypeContractData  StateChangeType = "CONTRACT_DATA"
	StateChangeTypeContractEvent StateChangeType = "CONTRACT_EVENT"
	
	// Account lifecycle
	StateChangeTypeAccountCreated StateChangeType = "ACCOUNT_CREATED"
	StateChangeTypeAccountRemoved StateChangeType = "ACCOUNT_REMOVED"
	
	// Trustlines
	StateChangeTypeTrustlineCreated StateChangeType = "TRUSTLINE_CREATED"
	StateChangeTypeTrustlineUpdated StateChangeType = "TRUSTLINE_UPDATED"
	StateChangeTypeTrustlineRemoved StateChangeType = "TRUSTLINE_REMOVED"
	
	// Offers
	StateChangeTypeOfferCreated StateChangeType = "OFFER_CREATED"
	StateChangeTypeOfferUpdated StateChangeType = "OFFER_UPDATED"
	StateChangeTypeOfferRemoved StateChangeType = "OFFER_REMOVED"
	
	// Liquidity pools
	StateChangeTypeLiquidityPoolDeposit  StateChangeType = "LIQUIDITY_POOL_DEPOSIT"
	StateChangeTypeLiquidityPoolWithdraw StateChangeType = "LIQUIDITY_POOL_WITHDRAW"
)

// StateChange represents a single state change in the blockchain
type StateChange struct {
	// Core identifiers
	LedgerSequence    uint32          `json:"ledger_sequence"`
	TransactionHash   string          `json:"transaction_hash"`
	TransactionIndex  int32           `json:"transaction_index"`
	OperationIndex    int32           `json:"operation_index"`
	OperationType     xdr.OperationType `json:"operation_type"`
	ApplicationOrder  int32           `json:"application_order"`
	
	// State change details
	Type              StateChangeType `json:"type"`
	Account           string          `json:"account"`
	Timestamp         time.Time       `json:"timestamp"`
	
	// Optional fields based on type
	Asset             *Asset          `json:"asset,omitempty"`
	Amount            *string         `json:"amount,omitempty"`
	BalanceBefore     *string         `json:"balance_before,omitempty"`
	BalanceAfter      *string         `json:"balance_after,omitempty"`
	
	// For account configuration changes
	SignerKey         *string         `json:"signer_key,omitempty"`
	SignerWeight      *uint32         `json:"signer_weight,omitempty"`
	ThresholdType     *string         `json:"threshold_type,omitempty"`
	ThresholdValue    *uint32         `json:"threshold_value,omitempty"`
	FlagsSet          *uint32         `json:"flags_set,omitempty"`
	FlagsClear        *uint32         `json:"flags_clear,omitempty"`
	
	// For data entries
	DataName          *string         `json:"data_name,omitempty"`
	DataValue         *string         `json:"data_value,omitempty"`
	
	// For sponsorship
	SponsorshipAccount *string        `json:"sponsorship_account,omitempty"`
	
	// For contract events
	ContractID        *string         `json:"contract_id,omitempty"`
	ContractEventType *string         `json:"contract_event_type,omitempty"`
	ContractEventData interface{}     `json:"contract_event_data,omitempty"`
	
	// Metadata
	Metadata          map[string]interface{} `json:"metadata,omitempty"`
}

// Asset represents a Stellar asset
type Asset struct {
	Code   string `json:"code"`
	Issuer string `json:"issuer,omitempty"`
	Type   string `json:"type"` // native, credit_alphanum4, credit_alphanum12
}

// StateChangeOutput is the message format emitted by the processor
type StateChangeOutput struct {
	LedgerSequence uint32         `json:"ledger_sequence"`
	Changes        []StateChange  `json:"changes"`
	Participants   []string       `json:"participants"` // All accounts involved
}