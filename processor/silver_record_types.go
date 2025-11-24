package processor

import (
	"database/sql/driver"
	"time"
)

// Silver record types that implement SilverRowGetter for DuckDB Appender API
// Each type corresponds to one of the 19 Silver tables
// IMPORTANT: Field order and count MUST match the table schemas in consumer/bronze_tables.go

// SilverLedgerRecord represents a row in ledgers_row_v2 (24 columns)
type SilverLedgerRecord struct {
	Sequence             uint32
	LedgerHash           string
	PreviousLedgerHash   string
	ClosedAt             time.Time
	ProtocolVersion      uint32
	TotalCoins           int64
	FeePool              int64
	BaseFee              uint32
	BaseReserve          uint32
	MaxTxSetSize         uint32
	SuccessfulTxCount    uint32
	FailedTxCount        uint32
	IngestionTimestamp   *time.Time
	LedgerRange          *uint32
	TransactionCount     *uint32
	OperationCount       *uint32
	TxSetOperationCount  *uint32
	SorobanFeeWrite1KB   *int64
	NodeID               *string
	Signature            *string
	LedgerHeader         *string
	BucketListSize       *uint64
	LiveSorobanStateSize *uint64
	EvictedKeysCount     *uint32
}

func (r *SilverLedgerRecord) GetSilverRow() []driver.Value {
	return []driver.Value{
		r.Sequence,
		r.LedgerHash,
		r.PreviousLedgerHash,
		r.ClosedAt,
		r.ProtocolVersion,
		r.TotalCoins,
		r.FeePool,
		r.BaseFee,
		r.BaseReserve,
		r.MaxTxSetSize,
		r.SuccessfulTxCount,
		r.FailedTxCount,
		ptrToValue(r.IngestionTimestamp),
		ptrToValue(r.LedgerRange),
		ptrToValue(r.TransactionCount),
		ptrToValue(r.OperationCount),
		ptrToValue(r.TxSetOperationCount),
		ptrToValue(r.SorobanFeeWrite1KB),
		ptrToValue(r.NodeID),
		ptrToValue(r.Signature),
		ptrToValue(r.LedgerHeader),
		ptrToValue(r.BucketListSize),
		ptrToValue(r.LiveSorobanStateSize),
		ptrToValue(r.EvictedKeysCount),
	}
}

// SilverTransactionRecord represents a row in transactions_row_v2 (46 columns)
type SilverTransactionRecord struct {
	LedgerSequence               uint32
	TransactionHash              string
	SourceAccount                string
	FeeCharged                   int64
	MaxFee                       int64
	Successful                   bool
	TransactionResultCode        string
	OperationCount               int32
	MemoType                     *string
	Memo                         *string
	CreatedAt                    time.Time
	AccountSequence              *int64
	LedgerRange                  *uint32
	SourceAccountMuxed           *string
	FeeAccountMuxed              *string
	InnerTransactionHash         *string
	FeeBumpFee                   *int64
	MaxFeeBid                    *int64
	InnerSourceAccount           *string
	TimeboundsMinTime            *int64
	TimeboundsMaxTime            *int64
	LedgerboundsMin              *uint32
	LedgerboundsMax              *uint32
	MinSequenceNumber            *int64
	MinSequenceAge               *int64
	SorobanResourcesInstructions *int64
	SorobanResourcesReadBytes    *int64
	SorobanResourcesWriteBytes   *int64
	SorobanDataSizeBytes         *int32
	SorobanDataResources         *string
	SorobanFeeBase               *int64
	SorobanFeeResources          *int64
	SorobanFeeRefund             *int64
	SorobanFeeCharged            *int64
	SorobanFeeWasted             *int64
	SorobanHostFunctionType      *string
	SorobanContractID            *string
	SorobanContractEventsCount   *int32
	SignaturesCount              int32
	NewAccount                   bool
	TxEnvelope                   *string
	TxResult                     *string
	TxMeta                       *string
	TxFeeMeta                    *string
	TxSigners                    *string
	ExtraSigners                 *string
}

func (r *SilverTransactionRecord) GetSilverRow() []driver.Value {
	return []driver.Value{
		r.LedgerSequence,
		r.TransactionHash,
		r.SourceAccount,
		r.FeeCharged,
		r.MaxFee,
		r.Successful,
		r.TransactionResultCode,
		r.OperationCount,
		ptrToValue(r.MemoType),
		ptrToValue(r.Memo),
		r.CreatedAt,
		ptrToValue(r.AccountSequence),
		ptrToValue(r.LedgerRange),
		ptrToValue(r.SourceAccountMuxed),
		ptrToValue(r.FeeAccountMuxed),
		ptrToValue(r.InnerTransactionHash),
		ptrToValue(r.FeeBumpFee),
		ptrToValue(r.MaxFeeBid),
		ptrToValue(r.InnerSourceAccount),
		ptrToValue(r.TimeboundsMinTime),
		ptrToValue(r.TimeboundsMaxTime),
		ptrToValue(r.LedgerboundsMin),
		ptrToValue(r.LedgerboundsMax),
		ptrToValue(r.MinSequenceNumber),
		ptrToValue(r.MinSequenceAge),
		ptrToValue(r.SorobanResourcesInstructions),
		ptrToValue(r.SorobanResourcesReadBytes),
		ptrToValue(r.SorobanResourcesWriteBytes),
		ptrToValue(r.SorobanDataSizeBytes),
		ptrToValue(r.SorobanDataResources),
		ptrToValue(r.SorobanFeeBase),
		ptrToValue(r.SorobanFeeResources),
		ptrToValue(r.SorobanFeeRefund),
		ptrToValue(r.SorobanFeeCharged),
		ptrToValue(r.SorobanFeeWasted),
		ptrToValue(r.SorobanHostFunctionType),
		ptrToValue(r.SorobanContractID),
		ptrToValue(r.SorobanContractEventsCount),
		r.SignaturesCount,
		r.NewAccount,
		ptrToValue(r.TxEnvelope),
		ptrToValue(r.TxResult),
		ptrToValue(r.TxMeta),
		ptrToValue(r.TxFeeMeta),
		ptrToValue(r.TxSigners),
		ptrToValue(r.ExtraSigners),
	}
}

// SilverOperationRecord represents a row in operations_row_v2 (58 columns)
type SilverOperationRecord struct {
	TransactionHash                string
	OperationIndex                 int32
	LedgerSequence                 uint32
	SourceAccount                  string
	Type                           int32
	TypeString                     string
	CreatedAt                      time.Time
	TransactionSuccessful          bool
	OperationResultCode            *string
	OperationTraceCode             *string
	LedgerRange                    *uint32
	SourceAccountMuxed             *string
	Asset                          *string
	AssetType                      *string
	AssetCode                      *string
	AssetIssuer                    *string
	SourceAsset                    *string
	SourceAssetType                *string
	SourceAssetCode                *string
	SourceAssetIssuer              *string
	Amount                         *int64
	SourceAmount                   *int64
	DestinationMin                 *int64
	StartingBalance                *int64
	Destination                    *string
	TrustlineLimit                 *int64
	Trustor                        *string
	Authorize                      *bool
	AuthorizeToMaintainLiabilities *bool
	TrustLineFlags                 *int32
	BalanceID                      *string
	ClaimantsCount                 *int32
	SponsoredID                    *string
	OfferID                        *int64
	Price                          *string
	PriceR                         *string
	BuyingAsset                    *string
	BuyingAssetType                *string
	BuyingAssetCode                *string
	BuyingAssetIssuer              *string
	SellingAsset                   *string
	SellingAssetType               *string
	SellingAssetCode               *string
	SellingAssetIssuer             *string
	SorobanOperation               *string
	SorobanFunction                *string
	SorobanContractID              *string
	SorobanAuthRequired            *bool
	BumpTo                         *int64
	SetFlags                       *int32
	ClearFlags                     *int32
	HomeDomain                     *string
	MasterWeight                   *int32
	LowThreshold                   *int32
	MediumThreshold                *int32
	HighThreshold                  *int32
	DataName                       *string
	DataValue                      *string
}

func (r *SilverOperationRecord) GetSilverRow() []driver.Value {
	return []driver.Value{
		r.TransactionHash,
		r.OperationIndex,
		r.LedgerSequence,
		r.SourceAccount,
		r.Type,
		r.TypeString,
		r.CreatedAt,
		r.TransactionSuccessful,
		ptrToValue(r.OperationResultCode),
		ptrToValue(r.OperationTraceCode),
		ptrToValue(r.LedgerRange),
		ptrToValue(r.SourceAccountMuxed),
		ptrToValue(r.Asset),
		ptrToValue(r.AssetType),
		ptrToValue(r.AssetCode),
		ptrToValue(r.AssetIssuer),
		ptrToValue(r.SourceAsset),
		ptrToValue(r.SourceAssetType),
		ptrToValue(r.SourceAssetCode),
		ptrToValue(r.SourceAssetIssuer),
		ptrToValue(r.Amount),
		ptrToValue(r.SourceAmount),
		ptrToValue(r.DestinationMin),
		ptrToValue(r.StartingBalance),
		ptrToValue(r.Destination),
		ptrToValue(r.TrustlineLimit),
		ptrToValue(r.Trustor),
		ptrToValue(r.Authorize),
		ptrToValue(r.AuthorizeToMaintainLiabilities),
		ptrToValue(r.TrustLineFlags),
		ptrToValue(r.BalanceID),
		ptrToValue(r.ClaimantsCount),
		ptrToValue(r.SponsoredID),
		ptrToValue(r.OfferID),
		ptrToValue(r.Price),
		ptrToValue(r.PriceR),
		ptrToValue(r.BuyingAsset),
		ptrToValue(r.BuyingAssetType),
		ptrToValue(r.BuyingAssetCode),
		ptrToValue(r.BuyingAssetIssuer),
		ptrToValue(r.SellingAsset),
		ptrToValue(r.SellingAssetType),
		ptrToValue(r.SellingAssetCode),
		ptrToValue(r.SellingAssetIssuer),
		ptrToValue(r.SorobanOperation),
		ptrToValue(r.SorobanFunction),
		ptrToValue(r.SorobanContractID),
		ptrToValue(r.SorobanAuthRequired),
		ptrToValue(r.BumpTo),
		ptrToValue(r.SetFlags),
		ptrToValue(r.ClearFlags),
		ptrToValue(r.HomeDomain),
		ptrToValue(r.MasterWeight),
		ptrToValue(r.LowThreshold),
		ptrToValue(r.MediumThreshold),
		ptrToValue(r.HighThreshold),
		ptrToValue(r.DataName),
		ptrToValue(r.DataValue),
	}
}

// SilverEffectRecord represents a row in effects_row_v1 (20 columns)
type SilverEffectRecord struct {
	LedgerSequence   uint32
	TransactionHash  string
	OperationIndex   int32
	EffectIndex      int32
	EffectType       int32
	EffectTypeString string
	AccountID        *string
	Amount           *string
	AssetCode        *string
	AssetIssuer      *string
	AssetType        *string
	TrustlineLimit   *string
	AuthorizeFlag    *bool
	ClawbackFlag     *bool
	SignerAccount    *string
	SignerWeight     *int32
	OfferID          *int64
	SellerAccount    *string
	CreatedAt        time.Time
	LedgerRange      *uint32
}

func (r *SilverEffectRecord) GetSilverRow() []driver.Value {
	return []driver.Value{
		r.LedgerSequence,
		r.TransactionHash,
		r.OperationIndex,
		r.EffectIndex,
		r.EffectType,
		r.EffectTypeString,
		ptrToValue(r.AccountID),
		ptrToValue(r.Amount),
		ptrToValue(r.AssetCode),
		ptrToValue(r.AssetIssuer),
		ptrToValue(r.AssetType),
		ptrToValue(r.TrustlineLimit),
		ptrToValue(r.AuthorizeFlag),
		ptrToValue(r.ClawbackFlag),
		ptrToValue(r.SignerAccount),
		ptrToValue(r.SignerWeight),
		ptrToValue(r.OfferID),
		ptrToValue(r.SellerAccount),
		r.CreatedAt,
		ptrToValue(r.LedgerRange),
	}
}

// SilverTradeRecord represents a row in trades_row_v1 (17 columns)
type SilverTradeRecord struct {
	LedgerSequence     uint32
	TransactionHash    string
	OperationIndex     int32
	TradeIndex         int32
	TradeType          string
	TradeTimestamp     time.Time
	SellerAccount      string
	SellingAssetCode   *string
	SellingAssetIssuer *string
	SellingAmount      string
	BuyerAccount       string
	BuyingAssetCode    *string
	BuyingAssetIssuer  *string
	BuyingAmount       string
	Price              string
	CreatedAt          time.Time
	LedgerRange        *uint32
}

func (r *SilverTradeRecord) GetSilverRow() []driver.Value {
	return []driver.Value{
		r.LedgerSequence,
		r.TransactionHash,
		r.OperationIndex,
		r.TradeIndex,
		r.TradeType,
		r.TradeTimestamp,
		r.SellerAccount,
		ptrToValue(r.SellingAssetCode),
		ptrToValue(r.SellingAssetIssuer),
		r.SellingAmount,
		r.BuyerAccount,
		ptrToValue(r.BuyingAssetCode),
		ptrToValue(r.BuyingAssetIssuer),
		r.BuyingAmount,
		r.Price,
		r.CreatedAt,
		ptrToValue(r.LedgerRange),
	}
}

// SilverNativeBalanceRecord represents a row in native_balances_snapshot_v1 (11 columns)
type SilverNativeBalanceRecord struct {
	AccountID          string
	Balance            int64
	BuyingLiabilities  int64
	SellingLiabilities int64
	NumSubentries      int32
	NumSponsoring      int32
	NumSponsored       int32
	SequenceNumber     *int64
	LastModifiedLedger uint32
	LedgerSequence     uint32
	LedgerRange        *uint32
}

func (r *SilverNativeBalanceRecord) GetSilverRow() []driver.Value {
	return []driver.Value{
		r.AccountID,
		r.Balance,
		r.BuyingLiabilities,
		r.SellingLiabilities,
		r.NumSubentries,
		r.NumSponsoring,
		r.NumSponsored,
		ptrToValue(r.SequenceNumber),
		r.LastModifiedLedger,
		r.LedgerSequence,
		ptrToValue(r.LedgerRange),
	}
}

// SilverAccountRecord represents a row in accounts_snapshot_v1 (24 columns)
type SilverAccountRecord struct {
	AccountID          string    // account_id
	LedgerSequence     uint32    // ledger_sequence
	ClosedAt           time.Time // closed_at
	Balance            string    // balance (VARCHAR)
	SequenceNumber     int64     // sequence_number
	NumSubentries      int32     // num_subentries
	NumSponsoring      int32     // num_sponsoring
	NumSponsored       int32     // num_sponsored
	HomeDomain         *string   // home_domain
	MasterWeight       int32     // master_weight
	LowThreshold       int32     // low_threshold
	MedThreshold       int32     // med_threshold
	HighThreshold      int32     // high_threshold
	Flags              int32     // flags
	AuthRequired       bool      // auth_required
	AuthRevocable      bool      // auth_revocable
	AuthImmutable      bool      // auth_immutable
	AuthClawbackEnabled bool     // auth_clawback_enabled
	Signers            *string   // signers (JSON array)
	SponsorAccount     *string   // sponsor_account
	CreatedAt          time.Time // created_at
	UpdatedAt          time.Time // updated_at
	LedgerRange        uint32    // ledger_range
}

func (r *SilverAccountRecord) GetSilverRow() []driver.Value {
	return []driver.Value{
		r.AccountID,
		r.LedgerSequence,
		r.ClosedAt,
		r.Balance,
		r.SequenceNumber,
		r.NumSubentries,
		r.NumSponsoring,
		r.NumSponsored,
		ptrToValue(r.HomeDomain),
		r.MasterWeight,
		r.LowThreshold,
		r.MedThreshold,
		r.HighThreshold,
		r.Flags,
		r.AuthRequired,
		r.AuthRevocable,
		r.AuthImmutable,
		r.AuthClawbackEnabled,
		ptrToValue(r.Signers),
		ptrToValue(r.SponsorAccount),
		r.CreatedAt,
		r.UpdatedAt,
		r.LedgerRange,
	}
}

// SilverTrustlineRecord represents a row in trustlines_snapshot_v1 (14 columns)
type SilverTrustlineRecord struct {
	AccountID                       string    // account_id
	AssetCode                       string    // asset_code
	AssetIssuer                     string    // asset_issuer
	AssetType                       string    // asset_type
	Balance                         string    // balance (VARCHAR)
	TrustLimit                      string    // trust_limit (VARCHAR)
	BuyingLiabilities               string    // buying_liabilities (VARCHAR)
	SellingLiabilities              string    // selling_liabilities (VARCHAR)
	Authorized                      bool      // authorized
	AuthorizedToMaintainLiabilities bool      // authorized_to_maintain_liabilities
	ClawbackEnabled                 bool      // clawback_enabled
	LedgerSequence                  uint32    // ledger_sequence
	CreatedAt                       time.Time // created_at
	LedgerRange                     uint32    // ledger_range
}

func (r *SilverTrustlineRecord) GetSilverRow() []driver.Value {
	return []driver.Value{
		r.AccountID,
		r.AssetCode,
		r.AssetIssuer,
		r.AssetType,
		r.Balance,
		r.TrustLimit,
		r.BuyingLiabilities,
		r.SellingLiabilities,
		r.Authorized,
		r.AuthorizedToMaintainLiabilities,
		r.ClawbackEnabled,
		r.LedgerSequence,
		r.CreatedAt,
		r.LedgerRange,
	}
}

// SilverOfferRecord represents a row in offers_snapshot_v1 (16 columns)
type SilverOfferRecord struct {
	OfferID            int64     // offer_id
	SellerAccount      string    // seller_account
	LedgerSequence     uint32    // ledger_sequence
	ClosedAt           time.Time // closed_at
	SellingAssetType   string    // selling_asset_type
	SellingAssetCode   *string   // selling_asset_code
	SellingAssetIssuer *string   // selling_asset_issuer
	BuyingAssetType    string    // buying_asset_type
	BuyingAssetCode    *string   // buying_asset_code
	BuyingAssetIssuer  *string   // buying_asset_issuer
	Amount             string    // amount (VARCHAR)
	Price              string    // price (VARCHAR)
	Flags              int32     // flags
	CreatedAt          time.Time // created_at
	LedgerRange        uint32    // ledger_range
}

func (r *SilverOfferRecord) GetSilverRow() []driver.Value {
	return []driver.Value{
		r.OfferID,
		r.SellerAccount,
		r.LedgerSequence,
		r.ClosedAt,
		r.SellingAssetType,
		ptrToValue(r.SellingAssetCode),
		ptrToValue(r.SellingAssetIssuer),
		r.BuyingAssetType,
		ptrToValue(r.BuyingAssetCode),
		ptrToValue(r.BuyingAssetIssuer),
		r.Amount,
		r.Price,
		r.Flags,
		r.CreatedAt,
		r.LedgerRange,
	}
}

// SilverClaimableBalanceRecord represents a row in claimable_balances_snapshot_v1 (12 columns)
type SilverClaimableBalanceRecord struct {
	BalanceID      string    // balance_id
	Sponsor        string    // sponsor
	LedgerSequence uint32    // ledger_sequence
	ClosedAt       time.Time // closed_at
	AssetType      string    // asset_type
	AssetCode      *string   // asset_code
	AssetIssuer    *string   // asset_issuer
	Amount         int64     // amount (BIGINT)
	ClaimantsCount int32     // claimants_count
	Flags          int32     // flags
	CreatedAt      time.Time // created_at
	LedgerRange    uint32    // ledger_range
}

func (r *SilverClaimableBalanceRecord) GetSilverRow() []driver.Value {
	return []driver.Value{
		r.BalanceID,
		r.Sponsor,
		r.LedgerSequence,
		r.ClosedAt,
		r.AssetType,
		ptrToValue(r.AssetCode),
		ptrToValue(r.AssetIssuer),
		r.Amount,
		r.ClaimantsCount,
		r.Flags,
		r.CreatedAt,
		r.LedgerRange,
	}
}

// SilverLiquidityPoolRecord represents a row in liquidity_pools_snapshot_v1 (18 columns)
type SilverLiquidityPoolRecord struct {
	LiquidityPoolID string    // liquidity_pool_id
	LedgerSequence  uint32    // ledger_sequence
	ClosedAt        time.Time // closed_at
	PoolType        string    // pool_type
	Fee             int32     // fee
	TrustlineCount  int32     // trustline_count
	TotalPoolShares int64     // total_pool_shares
	AssetAType      string    // asset_a_type
	AssetACode      *string   // asset_a_code
	AssetAIssuer    *string   // asset_a_issuer
	AssetAAmount    int64     // asset_a_amount
	AssetBType      string    // asset_b_type
	AssetBCode      *string   // asset_b_code
	AssetBIssuer    *string   // asset_b_issuer
	AssetBAmount    int64     // asset_b_amount
	CreatedAt       time.Time // created_at
	LedgerRange     uint32    // ledger_range
}

func (r *SilverLiquidityPoolRecord) GetSilverRow() []driver.Value {
	return []driver.Value{
		r.LiquidityPoolID,
		r.LedgerSequence,
		r.ClosedAt,
		r.PoolType,
		r.Fee,
		r.TrustlineCount,
		r.TotalPoolShares,
		r.AssetAType,
		ptrToValue(r.AssetACode),
		ptrToValue(r.AssetAIssuer),
		r.AssetAAmount,
		r.AssetBType,
		ptrToValue(r.AssetBCode),
		ptrToValue(r.AssetBIssuer),
		r.AssetBAmount,
		r.CreatedAt,
		r.LedgerRange,
	}
}

// SilverContractEventRecord represents a row in contract_events_stream_v1 (16 columns)
type SilverContractEventRecord struct {
	EventID                    string    // event_id
	ContractID                 *string   // contract_id
	LedgerSequence             uint32    // ledger_sequence
	TransactionHash            string    // transaction_hash
	ClosedAt                   time.Time // closed_at
	EventType                  string    // event_type
	InSuccessfulContractCall   bool      // in_successful_contract_call
	TopicsJSON                 string    // topics_json
	TopicsDecoded              string    // topics_decoded
	DataXDR                    string    // data_xdr
	DataDecoded                string    // data_decoded
	TopicCount                 int32     // topic_count
	OperationIndex             int32     // operation_index
	EventIndex                 int32     // event_index
	CreatedAt                  time.Time // created_at
	LedgerRange                uint32    // ledger_range
}

func (r *SilverContractEventRecord) GetSilverRow() []driver.Value {
	return []driver.Value{
		r.EventID,
		ptrToValue(r.ContractID),
		r.LedgerSequence,
		r.TransactionHash,
		r.ClosedAt,
		r.EventType,
		r.InSuccessfulContractCall,
		r.TopicsJSON,
		r.TopicsDecoded,
		r.DataXDR,
		r.DataDecoded,
		r.TopicCount,
		r.OperationIndex,
		r.EventIndex,
		r.CreatedAt,
		r.LedgerRange,
	}
}

// SilverContractDataRecord represents a row in contract_data_snapshot_v1 (17 columns)
type SilverContractDataRecord struct {
	ContractID         string    // contract_id
	LedgerSequence     uint32    // ledger_sequence
	LedgerKeyHash      string    // ledger_key_hash
	ContractKeyType    string    // contract_key_type
	ContractDurability string    // contract_durability
	AssetCode          *string   // asset_code
	AssetIssuer        *string   // asset_issuer
	AssetType          *string   // asset_type
	BalanceHolder      *string   // balance_holder
	Balance            *string   // balance
	LastModifiedLedger int32     // last_modified_ledger
	LedgerEntryChange  int32     // ledger_entry_change
	Deleted            bool      // deleted
	ClosedAt           time.Time // closed_at
	ContractDataXDR    string    // contract_data_xdr
	CreatedAt          time.Time // created_at
	LedgerRange        uint32    // ledger_range
}

func (r *SilverContractDataRecord) GetSilverRow() []driver.Value {
	return []driver.Value{
		r.ContractID,
		r.LedgerSequence,
		r.LedgerKeyHash,
		r.ContractKeyType,
		r.ContractDurability,
		ptrToValue(r.AssetCode),
		ptrToValue(r.AssetIssuer),
		ptrToValue(r.AssetType),
		ptrToValue(r.BalanceHolder),
		ptrToValue(r.Balance),
		r.LastModifiedLedger,
		r.LedgerEntryChange,
		r.Deleted,
		r.ClosedAt,
		r.ContractDataXDR,
		r.CreatedAt,
		r.LedgerRange,
	}
}

// SilverContractCodeRecord represents a row in contract_code_snapshot_v1 (22 columns)
type SilverContractCodeRecord struct {
	ContractCodeHash   string    // contract_code_hash
	LedgerKeyHash      string    // ledger_key_hash
	ContractCodeExtV   int32     // contract_code_ext_v
	LastModifiedLedger int32     // last_modified_ledger
	LedgerEntryChange  int32     // ledger_entry_change
	Deleted            bool      // deleted
	ClosedAt           time.Time // closed_at
	LedgerSequence     uint32    // ledger_sequence
	NInstructions      *int64    // n_instructions
	NFunctions         *int64    // n_functions
	NGlobals           *int64    // n_globals
	NTableEntries      *int64    // n_table_entries
	NTypes             *int64    // n_types
	NDataSegments      *int64    // n_data_segments
	NElemSegments      *int64    // n_elem_segments
	NImports           *int64    // n_imports
	NExports           *int64    // n_exports
	NDataSegmentBytes  *int64    // n_data_segment_bytes
	CreatedAt          time.Time // created_at
	LedgerRange        uint32    // ledger_range
}

func (r *SilverContractCodeRecord) GetSilverRow() []driver.Value {
	return []driver.Value{
		r.ContractCodeHash,
		r.LedgerKeyHash,
		r.ContractCodeExtV,
		r.LastModifiedLedger,
		r.LedgerEntryChange,
		r.Deleted,
		r.ClosedAt,
		r.LedgerSequence,
		ptrToValue(r.NInstructions),
		ptrToValue(r.NFunctions),
		ptrToValue(r.NGlobals),
		ptrToValue(r.NTableEntries),
		ptrToValue(r.NTypes),
		ptrToValue(r.NDataSegments),
		ptrToValue(r.NElemSegments),
		ptrToValue(r.NImports),
		ptrToValue(r.NExports),
		ptrToValue(r.NDataSegmentBytes),
		r.CreatedAt,
		r.LedgerRange,
	}
}

// SilverConfigSettingRecord represents a row in config_settings_snapshot_v1 (22 columns)
type SilverConfigSettingRecord struct {
	ConfigSettingID                   int32     // config_setting_id
	LedgerSequence                    uint32    // ledger_sequence
	LastModifiedLedger                int32     // last_modified_ledger
	Deleted                           bool      // deleted
	ClosedAt                          time.Time // closed_at
	LedgerMaxInstructions             *int64    // ledger_max_instructions
	TxMaxInstructions                 *int64    // tx_max_instructions
	FeeRatePerInstructionsIncrement   *int64    // fee_rate_per_instructions_increment
	TxMemoryLimit                     *uint32   // tx_memory_limit
	LedgerMaxReadLedgerEntries        *uint32   // ledger_max_read_ledger_entries
	LedgerMaxReadBytes                *uint32   // ledger_max_read_bytes
	LedgerMaxWriteLedgerEntries       *uint32   // ledger_max_write_ledger_entries
	LedgerMaxWriteBytes               *uint32   // ledger_max_write_bytes
	TxMaxReadLedgerEntries            *uint32   // tx_max_read_ledger_entries
	TxMaxReadBytes                    *uint32   // tx_max_read_bytes
	TxMaxWriteLedgerEntries           *uint32   // tx_max_write_ledger_entries
	TxMaxWriteBytes                   *uint32   // tx_max_write_bytes
	ContractMaxSizeBytes              *uint32   // contract_max_size_bytes
	ConfigSettingXDR                  string    // config_setting_xdr
	CreatedAt                         time.Time // created_at
	LedgerRange                       uint32    // ledger_range
}

func (r *SilverConfigSettingRecord) GetSilverRow() []driver.Value {
	return []driver.Value{
		r.ConfigSettingID,
		r.LedgerSequence,
		r.LastModifiedLedger,
		r.Deleted,
		r.ClosedAt,
		ptrToValue(r.LedgerMaxInstructions),
		ptrToValue(r.TxMaxInstructions),
		ptrToValue(r.FeeRatePerInstructionsIncrement),
		ptrToValue(r.TxMemoryLimit),
		ptrToValue(r.LedgerMaxReadLedgerEntries),
		ptrToValue(r.LedgerMaxReadBytes),
		ptrToValue(r.LedgerMaxWriteLedgerEntries),
		ptrToValue(r.LedgerMaxWriteBytes),
		ptrToValue(r.TxMaxReadLedgerEntries),
		ptrToValue(r.TxMaxReadBytes),
		ptrToValue(r.TxMaxWriteLedgerEntries),
		ptrToValue(r.TxMaxWriteBytes),
		ptrToValue(r.ContractMaxSizeBytes),
		r.ConfigSettingXDR,
		r.CreatedAt,
		r.LedgerRange,
	}
}

// SilverTTLRecord represents a row in ttl_snapshot_v1 (10 columns)
type SilverTTLRecord struct {
	KeyHash            string    // key_hash
	LedgerSequence     uint32    // ledger_sequence
	LiveUntilLedgerSeq int64     // live_until_ledger_seq (BIGINT)
	TTLRemaining       int64     // ttl_remaining (BIGINT)
	Expired            bool      // expired
	LastModifiedLedger int32     // last_modified_ledger
	Deleted            bool      // deleted
	ClosedAt           time.Time // closed_at
	CreatedAt          time.Time // created_at
	LedgerRange        uint32    // ledger_range
}

func (r *SilverTTLRecord) GetSilverRow() []driver.Value {
	return []driver.Value{
		r.KeyHash,
		r.LedgerSequence,
		r.LiveUntilLedgerSeq,
		r.TTLRemaining,
		r.Expired,
		r.LastModifiedLedger,
		r.Deleted,
		r.ClosedAt,
		r.CreatedAt,
		r.LedgerRange,
	}
}

// SilverEvictedKeyRecord represents a row in evicted_keys_state_v1 (8 columns)
type SilverEvictedKeyRecord struct {
	KeyHash        string    // key_hash
	LedgerSequence uint32    // ledger_sequence
	ContractID     string    // contract_id
	KeyType        string    // key_type
	Durability     string    // durability
	ClosedAt       time.Time // closed_at
	LedgerRange    uint32    // ledger_range
	CreatedAt      time.Time // created_at
}

func (r *SilverEvictedKeyRecord) GetSilverRow() []driver.Value {
	return []driver.Value{
		r.KeyHash,
		r.LedgerSequence,
		r.ContractID,
		r.KeyType,
		r.Durability,
		r.ClosedAt,
		r.LedgerRange,
		r.CreatedAt,
	}
}

// SilverRestoredKeyRecord represents a row in restored_keys_state_v1 (9 columns)
type SilverRestoredKeyRecord struct {
	KeyHash            string    // key_hash
	LedgerSequence     uint32    // ledger_sequence
	ContractID         string    // contract_id
	KeyType            string    // key_type
	Durability         string    // durability
	RestoredFromLedger int64     // restored_from_ledger
	ClosedAt           time.Time // closed_at
	LedgerRange        uint32    // ledger_range
	CreatedAt          time.Time // created_at
}

func (r *SilverRestoredKeyRecord) GetSilverRow() []driver.Value {
	return []driver.Value{
		r.KeyHash,
		r.LedgerSequence,
		r.ContractID,
		r.KeyType,
		r.Durability,
		r.RestoredFromLedger,
		r.ClosedAt,
		r.LedgerRange,
		r.CreatedAt,
	}
}

// SilverAccountSignerRecord represents a row in account_signers_snapshot_v1 (9 columns)
type SilverAccountSignerRecord struct {
	AccountID      string    // account_id
	Signer         string    // signer
	LedgerSequence uint32    // ledger_sequence
	Weight         int32     // weight
	Sponsor        *string   // sponsor
	Deleted        bool      // deleted
	ClosedAt       time.Time // closed_at
	LedgerRange    uint32    // ledger_range
	CreatedAt      time.Time // created_at
}

func (r *SilverAccountSignerRecord) GetSilverRow() []driver.Value {
	return []driver.Value{
		r.AccountID,
		r.Signer,
		r.LedgerSequence,
		r.Weight,
		ptrToValue(r.Sponsor),
		r.Deleted,
		r.ClosedAt,
		r.LedgerRange,
		r.CreatedAt,
	}
}

// ptrToValue converts a pointer to driver.Value (nil or dereferenced value)
func ptrToValue[T any](p *T) driver.Value {
	if p == nil {
		return nil
	}
	return *p
}
