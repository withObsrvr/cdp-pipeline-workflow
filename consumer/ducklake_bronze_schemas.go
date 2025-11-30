package consumer

import "github.com/withObsrvr/cdp-pipeline-workflow/processor"

// Bronze Layer Schema Registration Functions
// These schemas correspond to the 19 Bronze tables produced by BronzeExtractorsProcessor
// and are Hubble-compatible for direct comparison with the existing Stellar data pipeline.

// registerBronzeLedgerSchema registers the schema for ledgers_row_v2 (24 columns)
func (r *DuckLakeSchemaRegistry) registerBronzeLedgerSchema() {
	r.registerSchema(SchemaDefinition{
		ProcessorType: processor.ProcessorTypeBronzeLedger,
		Version:       "2.0.0",
		MinVersion:    "2.0.0",
		Description:   "Bronze layer ledger records (Hubble-compatible schema v2)",

		CreateSQL: `
CREATE TABLE IF NOT EXISTS %s.%s.%s (
    sequence UBIGINT NOT NULL,
    ledger_hash VARCHAR NOT NULL,
    previous_ledger_hash VARCHAR NOT NULL,
    closed_at TIMESTAMP NOT NULL,
    protocol_version UINTEGER NOT NULL,
    total_coins BIGINT NOT NULL,
    fee_pool BIGINT NOT NULL,
    base_fee UINTEGER NOT NULL,
    base_reserve UINTEGER NOT NULL,
    max_tx_set_size UINTEGER NOT NULL,
    successful_tx_count UINTEGER NOT NULL,
    failed_tx_count UINTEGER NOT NULL,
    ingestion_timestamp TIMESTAMP,
    ledger_range UINTEGER,
    transaction_count UINTEGER,
    operation_count UINTEGER,
    tx_set_operation_count UINTEGER,
    soroban_fee_write_1kb BIGINT,
    node_id VARCHAR,
    signature VARCHAR,
    ledger_header VARCHAR,
    bucket_list_size UBIGINT,
    live_soroban_state_size UBIGINT,
    evicted_keys_count UINTEGER
)`,

		InsertSQL: `
INSERT INTO %s.%s.%s (
    sequence, ledger_hash, previous_ledger_hash, closed_at, protocol_version,
    total_coins, fee_pool, base_fee, base_reserve, max_tx_set_size,
    successful_tx_count, failed_tx_count, ingestion_timestamp, ledger_range,
    transaction_count, operation_count, tx_set_operation_count, soroban_fee_write_1kb,
    node_id, signature, ledger_header, bucket_list_size, live_soroban_state_size,
    evicted_keys_count
) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,

		UpsertSQL: `
MERGE INTO %s.%s.%s AS target
USING (SELECT ? as sequence) AS source
ON target.sequence = source.sequence
WHEN NOT MATCHED THEN
    INSERT VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
	})
}

// registerBronzeTransactionSchema registers the schema for transactions_row_v2 (46 columns)
func (r *DuckLakeSchemaRegistry) registerBronzeTransactionSchema() {
	r.registerSchema(SchemaDefinition{
		ProcessorType: processor.ProcessorTypeBronzeTransaction,
		Version:       "2.0.0",
		MinVersion:    "2.0.0",
		Description:   "Bronze layer transaction records (Hubble-compatible schema v2)",

		CreateSQL: `
CREATE TABLE IF NOT EXISTS %s.%s.%s (
    ledger_sequence UINTEGER NOT NULL,
    transaction_hash VARCHAR NOT NULL,
    source_account VARCHAR NOT NULL,
    fee_charged BIGINT NOT NULL,
    max_fee BIGINT NOT NULL,
    successful BOOLEAN NOT NULL,
    transaction_result_code VARCHAR NOT NULL,
    operation_count INTEGER NOT NULL,
    memo_type VARCHAR,
    memo VARCHAR,
    created_at TIMESTAMP NOT NULL,
    account_sequence BIGINT,
    ledger_range UINTEGER,
    source_account_muxed VARCHAR,
    fee_account_muxed VARCHAR,
    inner_transaction_hash VARCHAR,
    fee_bump_fee BIGINT,
    max_fee_bid BIGINT,
    inner_source_account VARCHAR,
    timebounds_min_time BIGINT,
    timebounds_max_time BIGINT,
    ledgerbounds_min UINTEGER,
    ledgerbounds_max UINTEGER,
    min_sequence_number BIGINT,
    min_sequence_age BIGINT,
    soroban_resources_instructions BIGINT,
    soroban_resources_read_bytes BIGINT,
    soroban_resources_write_bytes BIGINT,
    soroban_data_size_bytes INTEGER,
    soroban_data_resources VARCHAR,
    soroban_fee_base BIGINT,
    soroban_fee_resources BIGINT,
    soroban_fee_refund BIGINT,
    soroban_fee_charged BIGINT,
    soroban_fee_wasted BIGINT,
    soroban_host_function_type VARCHAR,
    soroban_contract_id VARCHAR,
    soroban_contract_events_count INTEGER,
    signatures_count INTEGER NOT NULL,
    new_account BOOLEAN NOT NULL,
    tx_envelope VARCHAR,
    tx_result VARCHAR,
    tx_meta VARCHAR,
    tx_fee_meta VARCHAR,
    tx_signers VARCHAR,
    extra_signers VARCHAR
)`,

		InsertSQL: `
INSERT INTO %s.%s.%s (
    ledger_sequence, transaction_hash, source_account, fee_charged, max_fee,
    successful, transaction_result_code, operation_count, memo_type, memo,
    created_at, account_sequence, ledger_range, source_account_muxed, fee_account_muxed,
    inner_transaction_hash, fee_bump_fee, max_fee_bid, inner_source_account,
    timebounds_min_time, timebounds_max_time, ledgerbounds_min, ledgerbounds_max,
    min_sequence_number, min_sequence_age, soroban_resources_instructions,
    soroban_resources_read_bytes, soroban_resources_write_bytes, soroban_data_size_bytes,
    soroban_data_resources, soroban_fee_base, soroban_fee_resources, soroban_fee_refund,
    soroban_fee_charged, soroban_fee_wasted, soroban_host_function_type, soroban_contract_id,
    soroban_contract_events_count, signatures_count, new_account, tx_envelope, tx_result,
    tx_meta, tx_fee_meta, tx_signers, extra_signers
) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,

		UpsertSQL: `
MERGE INTO %s.%s.%s AS target
USING (SELECT ? as transaction_hash) AS source
ON target.transaction_hash = source.transaction_hash
WHEN NOT MATCHED THEN
    INSERT VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
	})
}

// registerBronzeOperationSchema registers the schema for operations_row_v2 (58 columns)
func (r *DuckLakeSchemaRegistry) registerBronzeOperationSchema() {
	r.registerSchema(SchemaDefinition{
		ProcessorType: processor.ProcessorTypeBronzeOperation,
		Version:       "2.0.0",
		MinVersion:    "2.0.0",
		Description:   "Bronze layer operation records (Hubble-compatible schema v2)",

		CreateSQL: `
CREATE TABLE IF NOT EXISTS %s.%s.%s (
    transaction_hash VARCHAR NOT NULL,
    operation_index INTEGER NOT NULL,
    ledger_sequence UINTEGER NOT NULL,
    source_account VARCHAR NOT NULL,
    type INTEGER NOT NULL,
    type_string VARCHAR NOT NULL,
    created_at TIMESTAMP NOT NULL,
    transaction_successful BOOLEAN NOT NULL,
    operation_result_code VARCHAR,
    operation_trace_code VARCHAR,
    ledger_range UINTEGER,
    source_account_muxed VARCHAR,
    asset VARCHAR,
    asset_type VARCHAR,
    asset_code VARCHAR,
    asset_issuer VARCHAR,
    source_asset VARCHAR,
    source_asset_type VARCHAR,
    source_asset_code VARCHAR,
    source_asset_issuer VARCHAR,
    amount BIGINT,
    source_amount BIGINT,
    destination_min BIGINT,
    starting_balance BIGINT,
    destination VARCHAR,
    trustline_limit BIGINT,
    trustor VARCHAR,
    authorize BOOLEAN,
    authorize_to_maintain_liabilities BOOLEAN,
    trust_line_flags INTEGER,
    balance_id VARCHAR,
    claimants_count INTEGER,
    sponsored_id VARCHAR,
    offer_id BIGINT,
    price VARCHAR,
    price_r VARCHAR,
    buying_asset VARCHAR,
    buying_asset_type VARCHAR,
    buying_asset_code VARCHAR,
    buying_asset_issuer VARCHAR,
    selling_asset VARCHAR,
    selling_asset_type VARCHAR,
    selling_asset_code VARCHAR,
    selling_asset_issuer VARCHAR,
    soroban_operation VARCHAR,
    soroban_function VARCHAR,
    soroban_contract_id VARCHAR,
    soroban_auth_required BOOLEAN,
    bump_to BIGINT,
    set_flags INTEGER,
    clear_flags INTEGER,
    home_domain VARCHAR,
    master_weight INTEGER,
    low_threshold INTEGER,
    medium_threshold INTEGER,
    high_threshold INTEGER,
    data_name VARCHAR,
    data_value VARCHAR
)`,

		InsertSQL: `
INSERT INTO %s.%s.%s VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,

		UpsertSQL: `
MERGE INTO %s.%s.%s AS target
USING (SELECT ? as transaction_hash, ? as operation_index) AS source
ON target.transaction_hash = source.transaction_hash AND target.operation_index = source.operation_index
WHEN NOT MATCHED THEN
    INSERT VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
	})
}

// registerBronzeEffectSchema registers the schema for effects_row_v1 (20 columns)
func (r *DuckLakeSchemaRegistry) registerBronzeEffectSchema() {
	r.registerSchema(SchemaDefinition{
		ProcessorType: processor.ProcessorTypeBronzeEffect,
		Version:       "1.0.0",
		MinVersion:    "1.0.0",
		Description:   "Bronze layer effect records (Hubble-compatible schema v1)",

		CreateSQL: `
CREATE TABLE IF NOT EXISTS %s.%s.%s (
    ledger_sequence UINTEGER NOT NULL,
    transaction_hash VARCHAR NOT NULL,
    operation_index INTEGER NOT NULL,
    effect_index INTEGER NOT NULL,
    effect_type INTEGER NOT NULL,
    effect_type_string VARCHAR NOT NULL,
    account_id VARCHAR,
    amount VARCHAR,
    asset_code VARCHAR,
    asset_issuer VARCHAR,
    asset_type VARCHAR,
    trustline_limit VARCHAR,
    authorize_flag BOOLEAN,
    clawback_flag BOOLEAN,
    signer_account VARCHAR,
    signer_weight INTEGER,
    offer_id BIGINT,
    seller_account VARCHAR,
    created_at TIMESTAMP NOT NULL,
    ledger_range UINTEGER
)`,

		InsertSQL: `
INSERT INTO %s.%s.%s VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,

		UpsertSQL: `
MERGE INTO %s.%s.%s AS target
USING (SELECT ? as transaction_hash, ? as operation_index, ? as effect_index) AS source
ON target.transaction_hash = source.transaction_hash
   AND target.operation_index = source.operation_index
   AND target.effect_index = source.effect_index
WHEN NOT MATCHED THEN
    INSERT VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
	})
}

// registerBronzeTradeSchema registers the schema for trades_row_v1 (17 columns)
func (r *DuckLakeSchemaRegistry) registerBronzeTradeSchema() {
	r.registerSchema(SchemaDefinition{
		ProcessorType: processor.ProcessorTypeBronzeTrade,
		Version:       "1.0.0",
		MinVersion:    "1.0.0",
		Description:   "Bronze layer trade records (Hubble-compatible schema v1)",

		CreateSQL: `
CREATE TABLE IF NOT EXISTS %s.%s.%s (
    ledger_sequence UINTEGER NOT NULL,
    transaction_hash VARCHAR NOT NULL,
    operation_index INTEGER NOT NULL,
    trade_index INTEGER NOT NULL,
    trade_type VARCHAR NOT NULL,
    trade_timestamp TIMESTAMP NOT NULL,
    seller_account VARCHAR NOT NULL,
    selling_asset_code VARCHAR,
    selling_asset_issuer VARCHAR,
    selling_amount VARCHAR NOT NULL,
    buyer_account VARCHAR NOT NULL,
    buying_asset_code VARCHAR,
    buying_asset_issuer VARCHAR,
    buying_amount VARCHAR NOT NULL,
    price VARCHAR NOT NULL,
    created_at TIMESTAMP NOT NULL,
    ledger_range UINTEGER
)`,

		InsertSQL: `
INSERT INTO %s.%s.%s VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,

		UpsertSQL: `
MERGE INTO %s.%s.%s AS target
USING (SELECT ? as transaction_hash, ? as operation_index, ? as trade_index) AS source
ON target.transaction_hash = source.transaction_hash
   AND target.operation_index = source.operation_index
   AND target.trade_index = source.trade_index
WHEN NOT MATCHED THEN
    INSERT VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
	})
}

// registerBronzeNativeBalanceSchema registers the schema for native_balances_snapshot_v1 (11 columns)
func (r *DuckLakeSchemaRegistry) registerBronzeNativeBalanceSchema() {
	r.registerSchema(SchemaDefinition{
		ProcessorType: processor.ProcessorTypeBronzeNativeBalance,
		Version:       "1.0.0",
		MinVersion:    "1.0.0",
		Description:   "Bronze layer native balance snapshot records (Hubble-compatible schema v1)",

		CreateSQL: `
CREATE TABLE IF NOT EXISTS %s.%s.%s (
    account_id VARCHAR NOT NULL,
    balance BIGINT NOT NULL,
    buying_liabilities BIGINT NOT NULL,
    selling_liabilities BIGINT NOT NULL,
    num_subentries INTEGER NOT NULL,
    num_sponsoring INTEGER NOT NULL,
    num_sponsored INTEGER NOT NULL,
    sequence_number BIGINT,
    last_modified_ledger UINTEGER NOT NULL,
    ledger_sequence UINTEGER NOT NULL,
    ledger_range UINTEGER
)`,

		InsertSQL: `
INSERT INTO %s.%s.%s VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,

		UpsertSQL: `
MERGE INTO %s.%s.%s AS target
USING (SELECT ? as account_id, ? as ledger_sequence) AS source
ON target.account_id = source.account_id AND target.ledger_sequence = source.ledger_sequence
WHEN NOT MATCHED THEN
    INSERT VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
	})
}

// registerBronzeAccountSchema registers the schema for accounts_snapshot_v1 (23 columns)
func (r *DuckLakeSchemaRegistry) registerBronzeAccountSchema() {
	r.registerSchema(SchemaDefinition{
		ProcessorType: processor.ProcessorTypeBronzeAccount,
		Version:       "1.0.0",
		MinVersion:    "1.0.0",
		Description:   "Bronze layer account snapshot records (Hubble-compatible schema v1)",

		CreateSQL: `
CREATE TABLE IF NOT EXISTS %s.%s.%s (
    account_id VARCHAR NOT NULL,
    ledger_sequence UINTEGER NOT NULL,
    closed_at TIMESTAMP NOT NULL,
    balance VARCHAR NOT NULL,
    sequence_number BIGINT NOT NULL,
    num_subentries INTEGER NOT NULL,
    num_sponsoring INTEGER NOT NULL,
    num_sponsored INTEGER NOT NULL,
    home_domain VARCHAR,
    master_weight INTEGER NOT NULL,
    low_threshold INTEGER NOT NULL,
    med_threshold INTEGER NOT NULL,
    high_threshold INTEGER NOT NULL,
    flags INTEGER NOT NULL,
    auth_required BOOLEAN NOT NULL,
    auth_revocable BOOLEAN NOT NULL,
    auth_immutable BOOLEAN NOT NULL,
    auth_clawback_enabled BOOLEAN NOT NULL,
    signers VARCHAR,
    sponsor_account VARCHAR,
    created_at TIMESTAMP NOT NULL,
    updated_at TIMESTAMP NOT NULL,
    ledger_range UINTEGER NOT NULL
)`,

		InsertSQL: `
INSERT INTO %s.%s.%s VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,

		UpsertSQL: `
MERGE INTO %s.%s.%s AS target
USING (SELECT ? as account_id, ? as ledger_sequence) AS source
ON target.account_id = source.account_id AND target.ledger_sequence = source.ledger_sequence
WHEN NOT MATCHED THEN
    INSERT VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
	})
}

// registerBronzeTrustlineSchema registers the schema for trustlines_snapshot_v1 (14 columns)
func (r *DuckLakeSchemaRegistry) registerBronzeTrustlineSchema() {
	r.registerSchema(SchemaDefinition{
		ProcessorType: processor.ProcessorTypeBronzeTrustline,
		Version:       "1.0.0",
		MinVersion:    "1.0.0",
		Description:   "Bronze layer trustline snapshot records (Hubble-compatible schema v1)",

		CreateSQL: `
CREATE TABLE IF NOT EXISTS %s.%s.%s (
    account_id VARCHAR NOT NULL,
    asset_code VARCHAR NOT NULL,
    asset_issuer VARCHAR NOT NULL,
    asset_type VARCHAR NOT NULL,
    balance VARCHAR NOT NULL,
    trust_limit VARCHAR NOT NULL,
    buying_liabilities VARCHAR NOT NULL,
    selling_liabilities VARCHAR NOT NULL,
    authorized BOOLEAN NOT NULL,
    authorized_to_maintain_liabilities BOOLEAN NOT NULL,
    clawback_enabled BOOLEAN NOT NULL,
    ledger_sequence UINTEGER NOT NULL,
    created_at TIMESTAMP NOT NULL,
    ledger_range UINTEGER NOT NULL
)`,

		InsertSQL: `
INSERT INTO %s.%s.%s VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,

		UpsertSQL: `
MERGE INTO %s.%s.%s AS target
USING (SELECT ? as account_id, ? as asset_code, ? as asset_issuer, ? as ledger_sequence) AS source
ON target.account_id = source.account_id
   AND target.asset_code = source.asset_code
   AND target.asset_issuer = source.asset_issuer
   AND target.ledger_sequence = source.ledger_sequence
WHEN NOT MATCHED THEN
    INSERT VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
	})
}

// registerBronzeOfferSchema registers the schema for offers_snapshot_v1 (15 columns - adjusted from 16)
func (r *DuckLakeSchemaRegistry) registerBronzeOfferSchema() {
	r.registerSchema(SchemaDefinition{
		ProcessorType: processor.ProcessorTypeBronzeOffer,
		Version:       "1.0.0",
		MinVersion:    "1.0.0",
		Description:   "Bronze layer offer snapshot records (Hubble-compatible schema v1)",

		CreateSQL: `
CREATE TABLE IF NOT EXISTS %s.%s.%s (
    offer_id BIGINT NOT NULL,
    seller_account VARCHAR NOT NULL,
    ledger_sequence UINTEGER NOT NULL,
    closed_at TIMESTAMP NOT NULL,
    selling_asset_type VARCHAR NOT NULL,
    selling_asset_code VARCHAR,
    selling_asset_issuer VARCHAR,
    buying_asset_type VARCHAR NOT NULL,
    buying_asset_code VARCHAR,
    buying_asset_issuer VARCHAR,
    amount VARCHAR NOT NULL,
    price VARCHAR NOT NULL,
    flags INTEGER NOT NULL,
    created_at TIMESTAMP NOT NULL,
    ledger_range UINTEGER NOT NULL
)`,

		InsertSQL: `
INSERT INTO %s.%s.%s VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,

		UpsertSQL: `
MERGE INTO %s.%s.%s AS target
USING (SELECT ? as offer_id, ? as ledger_sequence) AS source
ON target.offer_id = source.offer_id AND target.ledger_sequence = source.ledger_sequence
WHEN NOT MATCHED THEN
    INSERT VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
	})
}

// registerBronzeClaimableBalanceSchema registers the schema for claimable_balances_snapshot_v1 (12 columns)
func (r *DuckLakeSchemaRegistry) registerBronzeClaimableBalanceSchema() {
	r.registerSchema(SchemaDefinition{
		ProcessorType: processor.ProcessorTypeBronzeClaimableBalance,
		Version:       "1.0.0",
		MinVersion:    "1.0.0",
		Description:   "Bronze layer claimable balance snapshot records (Hubble-compatible schema v1)",

		CreateSQL: `
CREATE TABLE IF NOT EXISTS %s.%s.%s (
    balance_id VARCHAR NOT NULL,
    sponsor VARCHAR NOT NULL,
    ledger_sequence UINTEGER NOT NULL,
    closed_at TIMESTAMP NOT NULL,
    asset_type VARCHAR NOT NULL,
    asset_code VARCHAR,
    asset_issuer VARCHAR,
    amount BIGINT NOT NULL,
    claimants_count INTEGER NOT NULL,
    flags INTEGER NOT NULL,
    created_at TIMESTAMP NOT NULL,
    ledger_range UINTEGER NOT NULL
)`,

		InsertSQL: `
INSERT INTO %s.%s.%s VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,

		UpsertSQL: `
MERGE INTO %s.%s.%s AS target
USING (SELECT ? as balance_id, ? as ledger_sequence) AS source
ON target.balance_id = source.balance_id AND target.ledger_sequence = source.ledger_sequence
WHEN NOT MATCHED THEN
    INSERT VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
	})
}

// registerBronzeLiquidityPoolSchema registers the schema for liquidity_pools_snapshot_v1 (17 columns)
func (r *DuckLakeSchemaRegistry) registerBronzeLiquidityPoolSchema() {
	r.registerSchema(SchemaDefinition{
		ProcessorType: processor.ProcessorTypeBronzeLiquidityPool,
		Version:       "1.0.0",
		MinVersion:    "1.0.0",
		Description:   "Bronze layer liquidity pool snapshot records (Hubble-compatible schema v1)",

		CreateSQL: `
CREATE TABLE IF NOT EXISTS %s.%s.%s (
    liquidity_pool_id VARCHAR NOT NULL,
    ledger_sequence UINTEGER NOT NULL,
    closed_at TIMESTAMP NOT NULL,
    pool_type VARCHAR NOT NULL,
    fee INTEGER NOT NULL,
    trustline_count INTEGER NOT NULL,
    total_pool_shares BIGINT NOT NULL,
    asset_a_type VARCHAR NOT NULL,
    asset_a_code VARCHAR,
    asset_a_issuer VARCHAR,
    asset_a_amount BIGINT NOT NULL,
    asset_b_type VARCHAR NOT NULL,
    asset_b_code VARCHAR,
    asset_b_issuer VARCHAR,
    asset_b_amount BIGINT NOT NULL,
    created_at TIMESTAMP NOT NULL,
    ledger_range UINTEGER NOT NULL
)`,

		InsertSQL: `
INSERT INTO %s.%s.%s VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,

		UpsertSQL: `
MERGE INTO %s.%s.%s AS target
USING (SELECT ? as liquidity_pool_id, ? as ledger_sequence) AS source
ON target.liquidity_pool_id = source.liquidity_pool_id AND target.ledger_sequence = source.ledger_sequence
WHEN NOT MATCHED THEN
    INSERT VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
	})
}

// registerBronzeContractEventSchema registers the schema for contract_events_stream_v1 (16 columns)
func (r *DuckLakeSchemaRegistry) registerBronzeContractEventSchema() {
	r.registerSchema(SchemaDefinition{
		ProcessorType: processor.ProcessorTypeBronzeContractEvent,
		Version:       "1.0.0",
		MinVersion:    "1.0.0",
		Description:   "Bronze layer contract event stream records (Hubble-compatible schema v1)",

		CreateSQL: `
CREATE TABLE IF NOT EXISTS %s.%s.%s (
    event_id VARCHAR NOT NULL,
    contract_id VARCHAR,
    ledger_sequence UINTEGER NOT NULL,
    transaction_hash VARCHAR NOT NULL,
    closed_at TIMESTAMP NOT NULL,
    event_type VARCHAR NOT NULL,
    in_successful_contract_call BOOLEAN NOT NULL,
    topics_json VARCHAR NOT NULL,
    topics_decoded VARCHAR NOT NULL,
    data_xdr VARCHAR NOT NULL,
    data_decoded VARCHAR NOT NULL,
    topic_count INTEGER NOT NULL,
    operation_index INTEGER NOT NULL,
    event_index INTEGER NOT NULL,
    created_at TIMESTAMP NOT NULL,
    ledger_range UINTEGER NOT NULL
)`,

		InsertSQL: `
INSERT INTO %s.%s.%s VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,

		UpsertSQL: `
MERGE INTO %s.%s.%s AS target
USING (SELECT ? as event_id) AS source
ON target.event_id = source.event_id
WHEN NOT MATCHED THEN
    INSERT VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
	})
}

// registerBronzeContractDataSchema registers the schema for contract_data_snapshot_v1 (17 columns)
func (r *DuckLakeSchemaRegistry) registerBronzeContractDataSchema() {
	r.registerSchema(SchemaDefinition{
		ProcessorType: processor.ProcessorTypeBronzeContractData,
		Version:       "1.0.0",
		MinVersion:    "1.0.0",
		Description:   "Bronze layer contract data snapshot records (Hubble-compatible schema v1)",

		CreateSQL: `
CREATE TABLE IF NOT EXISTS %s.%s.%s (
    contract_id VARCHAR NOT NULL,
    ledger_sequence UINTEGER NOT NULL,
    ledger_key_hash VARCHAR NOT NULL,
    contract_key_type VARCHAR NOT NULL,
    contract_durability VARCHAR NOT NULL,
    asset_code VARCHAR,
    asset_issuer VARCHAR,
    asset_type VARCHAR,
    balance_holder VARCHAR,
    balance VARCHAR,
    last_modified_ledger INTEGER NOT NULL,
    ledger_entry_change INTEGER NOT NULL,
    deleted BOOLEAN NOT NULL,
    closed_at TIMESTAMP NOT NULL,
    contract_data_xdr VARCHAR NOT NULL,
    created_at TIMESTAMP NOT NULL,
    ledger_range UINTEGER NOT NULL
)`,

		InsertSQL: `
INSERT INTO %s.%s.%s VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,

		UpsertSQL: `
MERGE INTO %s.%s.%s AS target
USING (SELECT ? as ledger_key_hash, ? as ledger_sequence) AS source
ON target.ledger_key_hash = source.ledger_key_hash AND target.ledger_sequence = source.ledger_sequence
WHEN NOT MATCHED THEN
    INSERT VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
	})
}

// registerBronzeContractCodeSchema registers the schema for contract_code_snapshot_v1 (20 columns)
func (r *DuckLakeSchemaRegistry) registerBronzeContractCodeSchema() {
	r.registerSchema(SchemaDefinition{
		ProcessorType: processor.ProcessorTypeBronzeContractCode,
		Version:       "1.0.0",
		MinVersion:    "1.0.0",
		Description:   "Bronze layer contract code snapshot records (Hubble-compatible schema v1)",

		CreateSQL: `
CREATE TABLE IF NOT EXISTS %s.%s.%s (
    contract_code_hash VARCHAR NOT NULL,
    ledger_key_hash VARCHAR NOT NULL,
    contract_code_ext_v INTEGER NOT NULL,
    last_modified_ledger INTEGER NOT NULL,
    ledger_entry_change INTEGER NOT NULL,
    deleted BOOLEAN NOT NULL,
    closed_at TIMESTAMP NOT NULL,
    ledger_sequence UINTEGER NOT NULL,
    n_instructions BIGINT,
    n_functions BIGINT,
    n_globals BIGINT,
    n_table_entries BIGINT,
    n_types BIGINT,
    n_data_segments BIGINT,
    n_elem_segments BIGINT,
    n_imports BIGINT,
    n_exports BIGINT,
    n_data_segment_bytes BIGINT,
    created_at TIMESTAMP NOT NULL,
    ledger_range UINTEGER NOT NULL
)`,

		InsertSQL: `
INSERT INTO %s.%s.%s VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,

		UpsertSQL: `
MERGE INTO %s.%s.%s AS target
USING (SELECT ? as contract_code_hash, ? as ledger_sequence) AS source
ON target.contract_code_hash = source.contract_code_hash AND target.ledger_sequence = source.ledger_sequence
WHEN NOT MATCHED THEN
    INSERT VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
	})
}

// registerBronzeConfigSettingSchema registers the schema for config_settings_snapshot_v1 (21 columns)
func (r *DuckLakeSchemaRegistry) registerBronzeConfigSettingSchema() {
	r.registerSchema(SchemaDefinition{
		ProcessorType: processor.ProcessorTypeBronzeConfigSetting,
		Version:       "1.0.0",
		MinVersion:    "1.0.0",
		Description:   "Bronze layer config setting snapshot records (Hubble-compatible schema v1)",

		CreateSQL: `
CREATE TABLE IF NOT EXISTS %s.%s.%s (
    config_setting_id INTEGER NOT NULL,
    ledger_sequence UINTEGER NOT NULL,
    last_modified_ledger INTEGER NOT NULL,
    deleted BOOLEAN NOT NULL,
    closed_at TIMESTAMP NOT NULL,
    ledger_max_instructions BIGINT,
    tx_max_instructions BIGINT,
    fee_rate_per_instructions_increment BIGINT,
    tx_memory_limit UINTEGER,
    ledger_max_read_ledger_entries UINTEGER,
    ledger_max_read_bytes UINTEGER,
    ledger_max_write_ledger_entries UINTEGER,
    ledger_max_write_bytes UINTEGER,
    tx_max_read_ledger_entries UINTEGER,
    tx_max_read_bytes UINTEGER,
    tx_max_write_ledger_entries UINTEGER,
    tx_max_write_bytes UINTEGER,
    contract_max_size_bytes UINTEGER,
    config_setting_xdr VARCHAR NOT NULL,
    created_at TIMESTAMP NOT NULL,
    ledger_range UINTEGER NOT NULL
)`,

		InsertSQL: `
INSERT INTO %s.%s.%s VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,

		UpsertSQL: `
MERGE INTO %s.%s.%s AS target
USING (SELECT ? as config_setting_id, ? as ledger_sequence) AS source
ON target.config_setting_id = source.config_setting_id AND target.ledger_sequence = source.ledger_sequence
WHEN NOT MATCHED THEN
    INSERT VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
	})
}

// registerBronzeTTLSchema registers the schema for ttl_snapshot_v1 (10 columns)
func (r *DuckLakeSchemaRegistry) registerBronzeTTLSchema() {
	r.registerSchema(SchemaDefinition{
		ProcessorType: processor.ProcessorTypeBronzeTTL,
		Version:       "1.0.0",
		MinVersion:    "1.0.0",
		Description:   "Bronze layer TTL snapshot records (Hubble-compatible schema v1)",

		CreateSQL: `
CREATE TABLE IF NOT EXISTS %s.%s.%s (
    key_hash VARCHAR NOT NULL,
    ledger_sequence UINTEGER NOT NULL,
    live_until_ledger_seq BIGINT NOT NULL,
    ttl_remaining BIGINT NOT NULL,
    expired BOOLEAN NOT NULL,
    last_modified_ledger INTEGER NOT NULL,
    deleted BOOLEAN NOT NULL,
    closed_at TIMESTAMP NOT NULL,
    created_at TIMESTAMP NOT NULL,
    ledger_range UINTEGER NOT NULL
)`,

		InsertSQL: `
INSERT INTO %s.%s.%s VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,

		UpsertSQL: `
MERGE INTO %s.%s.%s AS target
USING (SELECT ? as key_hash, ? as ledger_sequence) AS source
ON target.key_hash = source.key_hash AND target.ledger_sequence = source.ledger_sequence
WHEN NOT MATCHED THEN
    INSERT VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
	})
}

// registerBronzeEvictedKeySchema registers the schema for evicted_keys_state_v1 (8 columns)
func (r *DuckLakeSchemaRegistry) registerBronzeEvictedKeySchema() {
	r.registerSchema(SchemaDefinition{
		ProcessorType: processor.ProcessorTypeBronzeEvictedKey,
		Version:       "1.0.0",
		MinVersion:    "1.0.0",
		Description:   "Bronze layer evicted key state records (Hubble-compatible schema v1)",

		CreateSQL: `
CREATE TABLE IF NOT EXISTS %s.%s.%s (
    key_hash VARCHAR NOT NULL,
    ledger_sequence UINTEGER NOT NULL,
    contract_id VARCHAR NOT NULL,
    key_type VARCHAR NOT NULL,
    durability VARCHAR NOT NULL,
    closed_at TIMESTAMP NOT NULL,
    ledger_range UINTEGER NOT NULL,
    created_at TIMESTAMP NOT NULL
)`,

		InsertSQL: `
INSERT INTO %s.%s.%s VALUES (?, ?, ?, ?, ?, ?, ?, ?)`,

		UpsertSQL: `
MERGE INTO %s.%s.%s AS target
USING (SELECT ? as key_hash, ? as ledger_sequence) AS source
ON target.key_hash = source.key_hash AND target.ledger_sequence = source.ledger_sequence
WHEN NOT MATCHED THEN
    INSERT VALUES (?, ?, ?, ?, ?, ?, ?, ?)`,
	})
}

// registerBronzeRestoredKeySchema registers the schema for restored_keys_state_v1 (9 columns)
func (r *DuckLakeSchemaRegistry) registerBronzeRestoredKeySchema() {
	r.registerSchema(SchemaDefinition{
		ProcessorType: processor.ProcessorTypeBronzeRestoredKey,
		Version:       "1.0.0",
		MinVersion:    "1.0.0",
		Description:   "Bronze layer restored key state records (Hubble-compatible schema v1)",

		CreateSQL: `
CREATE TABLE IF NOT EXISTS %s.%s.%s (
    key_hash VARCHAR NOT NULL,
    ledger_sequence UINTEGER NOT NULL,
    contract_id VARCHAR NOT NULL,
    key_type VARCHAR NOT NULL,
    durability VARCHAR NOT NULL,
    restored_from_ledger BIGINT NOT NULL,
    closed_at TIMESTAMP NOT NULL,
    ledger_range UINTEGER NOT NULL,
    created_at TIMESTAMP NOT NULL
)`,

		InsertSQL: `
INSERT INTO %s.%s.%s VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)`,

		UpsertSQL: `
MERGE INTO %s.%s.%s AS target
USING (SELECT ? as key_hash, ? as ledger_sequence) AS source
ON target.key_hash = source.key_hash AND target.ledger_sequence = source.ledger_sequence
WHEN NOT MATCHED THEN
    INSERT VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)`,
	})
}

// registerBronzeAccountSignerSchema registers the schema for account_signers_snapshot_v1 (9 columns)
func (r *DuckLakeSchemaRegistry) registerBronzeAccountSignerSchema() {
	r.registerSchema(SchemaDefinition{
		ProcessorType: processor.ProcessorTypeBronzeAccountSigner,
		Version:       "1.0.0",
		MinVersion:    "1.0.0",
		Description:   "Bronze layer account signer snapshot records (Hubble-compatible schema v1)",

		CreateSQL: `
CREATE TABLE IF NOT EXISTS %s.%s.%s (
    account_id VARCHAR NOT NULL,
    signer VARCHAR NOT NULL,
    ledger_sequence UINTEGER NOT NULL,
    weight INTEGER NOT NULL,
    sponsor VARCHAR,
    deleted BOOLEAN NOT NULL,
    closed_at TIMESTAMP NOT NULL,
    ledger_range UINTEGER NOT NULL,
    created_at TIMESTAMP NOT NULL
)`,

		InsertSQL: `
INSERT INTO %s.%s.%s VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)`,

		UpsertSQL: `
MERGE INTO %s.%s.%s AS target
USING (SELECT ? as account_id, ? as signer, ? as ledger_sequence) AS source
ON target.account_id = source.account_id
   AND target.signer = source.signer
   AND target.ledger_sequence = source.ledger_sequence
WHEN NOT MATCHED THEN
    INSERT VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)`,
	})
}
