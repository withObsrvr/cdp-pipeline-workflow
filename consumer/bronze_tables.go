package consumer

import (
	"database/sql"
	"fmt"
	"log"
)

// Bronze table names - all 19 Hubble-compatible tables
var BronzeTableNames = []string{
	"ledgers_row_v2",
	"transactions_row_v2",
	"operations_row_v2",
	"effects_row_v1",
	"trades_row_v1",
	"native_balances_snapshot_v1",
	"accounts_snapshot_v1",
	"trustlines_snapshot_v1",
	"offers_snapshot_v1",
	"claimable_balances_snapshot_v1",
	"liquidity_pools_snapshot_v1",
	"contract_events_stream_v1",
	"contract_data_snapshot_v1",
	"contract_code_snapshot_v1",
	"config_settings_snapshot_v1",
	"ttl_snapshot_v1",
	"evicted_keys_state_v1",
	"restored_keys_state_v1",
	"account_signers_snapshot_v1",
}

// Metadata table names
var MetadataTableNames = []string{
	"_meta_datasets",
	"_meta_lineage",
	"_meta_quality",
	"_meta_changes",
}

// CreateBronzeTables creates all 19 Bronze tables in the specified catalog/schema
func CreateBronzeTables(db *sql.DB, catalogName, schemaName string) error {
	// Create each table
	tableFuncs := []func(*sql.DB, string, string) error{
		createLedgersTable,
		createTransactionsTable,
		createOperationsTable,
		createEffectsTable,
		createTradesTable,
		createNativeBalancesTable,
		createAccountsTable,
		createTrustlinesTable,
		createOffersTable,
		createClaimableBalancesTable,
		createLiquidityPoolsTable,
		createContractEventsTable,
		createContractDataTable,
		createContractCodeTable,
		createConfigSettingsTable,
		createTTLTable,
		createEvictedKeysTable,
		createRestoredKeysTable,
		createAccountSignersTable,
	}

	for _, createFunc := range tableFuncs {
		if err := createFunc(db, catalogName, schemaName); err != nil {
			return err
		}
	}

	log.Printf("BronzeCopier: All 19 Bronze tables created in %s.%s", catalogName, schemaName)
	return nil
}

// CreateMetadataTables creates the 4 Obsrvr metadata tables
func CreateMetadataTables(db *sql.DB, catalogName, schemaName string) error {
	// 1. _meta_datasets (Dataset Registry)
	datasetsSQL := fmt.Sprintf(`
		CREATE TABLE IF NOT EXISTS %s.%s._meta_datasets (
			dataset TEXT NOT NULL,
			tier TEXT NOT NULL,
			domain TEXT NOT NULL,
			major_version INT NOT NULL,
			current_minor_version INT NOT NULL,
			owner TEXT,
			purpose TEXT,
			grain TEXT,
			created_at TIMESTAMP NOT NULL,
			updated_at TIMESTAMP NOT NULL
		)`,
		catalogName, schemaName,
	)
	if _, err := db.Exec(datasetsSQL); err != nil {
		return fmt.Errorf("failed to create _meta_datasets table: %w", err)
	}
	log.Printf("BronzeCopier: Metadata table ready: %s.%s._meta_datasets", catalogName, schemaName)

	// 2. _meta_lineage (Processing Provenance)
	lineageSQL := fmt.Sprintf(`
		CREATE TABLE IF NOT EXISTS %s.%s._meta_lineage (
			id INTEGER NOT NULL,
			dataset TEXT NOT NULL,
			partition TEXT,
			source_ledger_start INT NOT NULL,
			source_ledger_end INT NOT NULL,
			pipeline_version TEXT NOT NULL,
			processor_name TEXT NOT NULL,
			checksum TEXT,
			previous_pointer TEXT,
			row_count INT,
			created_at TIMESTAMP NOT NULL
		)`,
		catalogName, schemaName,
	)
	if _, err := db.Exec(lineageSQL); err != nil {
		return fmt.Errorf("failed to create _meta_lineage table: %w", err)
	}
	log.Printf("BronzeCopier: Metadata table ready: %s.%s._meta_lineage", catalogName, schemaName)

	// 3. _meta_quality (Data Quality Tracking)
	qualitySQL := fmt.Sprintf(`
		CREATE TABLE IF NOT EXISTS %s.%s._meta_quality (
			id BIGINT NOT NULL,
			dataset TEXT NOT NULL,
			partition TEXT,
			check_name TEXT NOT NULL,
			check_type TEXT NOT NULL,
			passed BOOLEAN NOT NULL,
			details TEXT,
			row_count INT,
			null_anomalies INT,
			created_at TIMESTAMP NOT NULL
		)`,
		catalogName, schemaName,
	)
	if _, err := db.Exec(qualitySQL); err != nil {
		return fmt.Errorf("failed to create _meta_quality table: %w", err)
	}
	log.Printf("BronzeCopier: Metadata table ready: %s.%s._meta_quality", catalogName, schemaName)

	// 4. _meta_changes (Schema Evolution Log)
	changesSQL := fmt.Sprintf(`
		CREATE TABLE IF NOT EXISTS %s.%s._meta_changes (
			id INTEGER NOT NULL,
			dataset TEXT NOT NULL,
			from_version TEXT,
			to_version TEXT NOT NULL,
			change_type TEXT NOT NULL,
			summary TEXT,
			migration_sql TEXT,
			applied_at TIMESTAMP NOT NULL
		)`,
		catalogName, schemaName,
	)
	if _, err := db.Exec(changesSQL); err != nil {
		return fmt.Errorf("failed to create _meta_changes table: %w", err)
	}
	log.Printf("BronzeCopier: Metadata table ready: %s.%s._meta_changes", catalogName, schemaName)

	log.Printf("BronzeCopier: All 4 metadata tables created in %s.%s", catalogName, schemaName)
	return nil
}

// createLedgersTable creates the ledgers_row_v2 table
func createLedgersTable(db *sql.DB, catalogName, schemaName string) error {
	createSQL := fmt.Sprintf(`
		CREATE TABLE IF NOT EXISTS %s.%s.ledgers_row_v2 (
			sequence BIGINT NOT NULL,
			ledger_hash VARCHAR NOT NULL,
			previous_ledger_hash VARCHAR NOT NULL,
			closed_at TIMESTAMP NOT NULL,
			protocol_version INT NOT NULL,
			total_coins BIGINT NOT NULL,
			fee_pool BIGINT NOT NULL,
			base_fee INT NOT NULL,
			base_reserve INT NOT NULL,
			max_tx_set_size INT NOT NULL,
			successful_tx_count INT NOT NULL,
			failed_tx_count INT NOT NULL,
			ingestion_timestamp TIMESTAMP,
			ledger_range BIGINT,

			-- Operation counts (stellar-etl alignment)
			transaction_count INT,
			operation_count INT,
			tx_set_operation_count INT,

			-- Soroban (Protocol 20+)
			soroban_fee_write1kb BIGINT,

			-- Consensus metadata
			node_id VARCHAR,
			signature VARCHAR,
			ledger_header TEXT,

			-- State tracking
			bucket_list_size BIGINT,
			live_soroban_state_size BIGINT,

			-- Protocol 23 (CAP-62) - Hot Archive
			evicted_keys_count INT
		)`,
		catalogName, schemaName,
	)

	if _, err := db.Exec(createSQL); err != nil {
		return fmt.Errorf("failed to create ledgers_row_v2 table: %w", err)
	}
	log.Printf("BronzeCopier: Table ready: %s.%s.ledgers_row_v2", catalogName, schemaName)
	return nil
}

// createTransactionsTable creates the transactions_row_v2 table
func createTransactionsTable(db *sql.DB, catalogName, schemaName string) error {
	createSQL := fmt.Sprintf(`
		CREATE TABLE IF NOT EXISTS %s.%s.transactions_row_v2 (
			-- Core fields (13)
			ledger_sequence BIGINT NOT NULL,
			transaction_hash VARCHAR NOT NULL,
			source_account VARCHAR NOT NULL,
			fee_charged BIGINT NOT NULL,
			max_fee BIGINT NOT NULL,
			successful BOOLEAN NOT NULL,
			transaction_result_code VARCHAR NOT NULL,
			operation_count INT NOT NULL,
			memo_type VARCHAR,
			memo VARCHAR,
			created_at TIMESTAMP NOT NULL,
			account_sequence BIGINT,
			ledger_range BIGINT,

			-- Muxed accounts (2) - CAP-27
			source_account_muxed VARCHAR,
			fee_account_muxed VARCHAR,

			-- Fee bump transactions (4) - CAP-15
			inner_transaction_hash VARCHAR,
			fee_bump_fee BIGINT,
			max_fee_bid BIGINT,
			inner_source_account VARCHAR,

			-- Preconditions (6) - CAP-21
			timebounds_min_time BIGINT,
			timebounds_max_time BIGINT,
			ledgerbounds_min BIGINT,
			ledgerbounds_max BIGINT,
			min_sequence_number BIGINT,
			min_sequence_age BIGINT,

			-- Soroban fields (13) - CAP-46/CAP-47
			soroban_resources_instructions BIGINT,
			soroban_resources_read_bytes BIGINT,
			soroban_resources_write_bytes BIGINT,
			soroban_data_size_bytes INT,
			soroban_data_resources TEXT,
			soroban_fee_base BIGINT,
			soroban_fee_resources BIGINT,
			soroban_fee_refund BIGINT,
			soroban_fee_charged BIGINT,
			soroban_fee_wasted BIGINT,
			soroban_host_function_type VARCHAR,
			soroban_contract_id VARCHAR,
			soroban_contract_events_count INT,

			-- Metadata (2)
			signatures_count INT NOT NULL,
			new_account BOOLEAN NOT NULL,

			-- XDR fields (4)
			tx_envelope TEXT,
			tx_result TEXT,
			tx_meta TEXT,
			tx_fee_meta TEXT,

			-- Signer fields (2)
			tx_signers TEXT,
			extra_signers TEXT
		)`,
		catalogName, schemaName,
	)

	if _, err := db.Exec(createSQL); err != nil {
		return fmt.Errorf("failed to create transactions_row_v2 table: %w", err)
	}
	log.Printf("BronzeCopier: Table ready: %s.%s.transactions_row_v2", catalogName, schemaName)
	return nil
}

// createOperationsTable creates the operations_row_v2 table
func createOperationsTable(db *sql.DB, catalogName, schemaName string) error {
	createSQL := fmt.Sprintf(`
		CREATE TABLE IF NOT EXISTS %s.%s.operations_row_v2 (
			-- Core fields (11)
			transaction_hash VARCHAR NOT NULL,
			operation_index INT NOT NULL,
			ledger_sequence BIGINT NOT NULL,
			source_account VARCHAR NOT NULL,
			type INT NOT NULL,
			type_string VARCHAR NOT NULL,
			created_at TIMESTAMP NOT NULL,
			transaction_successful BOOLEAN NOT NULL,
			operation_result_code VARCHAR,
			operation_trace_code VARCHAR,
			ledger_range BIGINT,

			-- Muxed accounts (1)
			source_account_muxed VARCHAR,

			-- Asset fields (8)
			asset VARCHAR,
			asset_type VARCHAR,
			asset_code VARCHAR,
			asset_issuer VARCHAR,
			source_asset VARCHAR,
			source_asset_type VARCHAR,
			source_asset_code VARCHAR,
			source_asset_issuer VARCHAR,

			-- Amount/price fields (4)
			amount BIGINT,
			source_amount BIGINT,
			destination_min BIGINT,
			starting_balance BIGINT,

			-- Destination (1)
			destination VARCHAR,

			-- Trustline (5)
			trustline_limit BIGINT,
			trustor VARCHAR,
			authorize BOOLEAN,
			authorize_to_maintain_liabilities BOOLEAN,
			trust_line_flags INT,

			-- Claimable balance (2)
			balance_id VARCHAR,
			claimants_count INT,

			-- Sponsorship (1)
			sponsored_id VARCHAR,

			-- DEX fields (11)
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

			-- Soroban (4)
			soroban_operation VARCHAR,
			soroban_function VARCHAR,
			soroban_contract_id VARCHAR,
			soroban_auth_required BOOLEAN,

			-- Account operations (8)
			bump_to BIGINT,
			set_flags INT,
			clear_flags INT,
			home_domain VARCHAR,
			master_weight INT,
			low_threshold INT,
			medium_threshold INT,
			high_threshold INT,

			-- Other (2)
			data_name VARCHAR,
			data_value VARCHAR
		)`,
		catalogName, schemaName,
	)

	if _, err := db.Exec(createSQL); err != nil {
		return fmt.Errorf("failed to create operations_row_v2 table: %w", err)
	}
	log.Printf("BronzeCopier: Table ready: %s.%s.operations_row_v2", catalogName, schemaName)
	return nil
}

// createEffectsTable creates the effects_row_v1 table
func createEffectsTable(db *sql.DB, catalogName, schemaName string) error {
	createSQL := fmt.Sprintf(`
		CREATE TABLE IF NOT EXISTS %s.%s.effects_row_v1 (
			-- Identity (4 fields)
			ledger_sequence BIGINT NOT NULL,
			transaction_hash VARCHAR NOT NULL,
			operation_index INT NOT NULL,
			effect_index INT NOT NULL,

			-- Effect type (2 fields)
			effect_type INT NOT NULL,
			effect_type_string VARCHAR NOT NULL,

			-- Account affected (1 field)
			account_id VARCHAR,

			-- Amount changes (4 fields) - Nullable
			amount VARCHAR,
			asset_code VARCHAR,
			asset_issuer VARCHAR,
			asset_type VARCHAR,

			-- Trustline effects (3 fields) - Nullable
			trustline_limit VARCHAR,
			authorize_flag BOOLEAN,
			clawback_flag BOOLEAN,

			-- Signer effects (2 fields) - Nullable
			signer_account VARCHAR,
			signer_weight INT,

			-- Offer effects (2 fields) - Nullable
			offer_id BIGINT,
			seller_account VARCHAR,

			-- Metadata (2 fields)
			created_at TIMESTAMP NOT NULL,
			ledger_range BIGINT
		)`,
		catalogName, schemaName,
	)

	if _, err := db.Exec(createSQL); err != nil {
		return fmt.Errorf("failed to create effects_row_v1 table: %w", err)
	}
	log.Printf("BronzeCopier: Table ready: %s.%s.effects_row_v1", catalogName, schemaName)
	return nil
}

// createTradesTable creates the trades_row_v1 table
func createTradesTable(db *sql.DB, catalogName, schemaName string) error {
	createSQL := fmt.Sprintf(`
		CREATE TABLE IF NOT EXISTS %s.%s.trades_row_v1 (
			-- Identity (4 fields)
			ledger_sequence BIGINT NOT NULL,
			transaction_hash VARCHAR NOT NULL,
			operation_index INT NOT NULL,
			trade_index INT NOT NULL,

			-- Trade details (2 fields)
			trade_type VARCHAR NOT NULL,
			trade_timestamp TIMESTAMP NOT NULL,

			-- Seller side (4 fields)
			seller_account VARCHAR NOT NULL,
			selling_asset_code VARCHAR,
			selling_asset_issuer VARCHAR,
			selling_amount VARCHAR NOT NULL,

			-- Buyer side (4 fields)
			buyer_account VARCHAR NOT NULL,
			buying_asset_code VARCHAR,
			buying_asset_issuer VARCHAR,
			buying_amount VARCHAR NOT NULL,

			-- Price (1 field)
			price VARCHAR NOT NULL,

			-- Metadata (2 fields)
			created_at TIMESTAMP NOT NULL,
			ledger_range BIGINT
		)`,
		catalogName, schemaName,
	)

	if _, err := db.Exec(createSQL); err != nil {
		return fmt.Errorf("failed to create trades_row_v1 table: %w", err)
	}
	log.Printf("BronzeCopier: Table ready: %s.%s.trades_row_v1", catalogName, schemaName)
	return nil
}

// createNativeBalancesTable creates the native_balances_snapshot_v1 table
func createNativeBalancesTable(db *sql.DB, catalogName, schemaName string) error {
	createSQL := fmt.Sprintf(`
		CREATE TABLE IF NOT EXISTS %s.%s.native_balances_snapshot_v1 (
			account_id VARCHAR NOT NULL,
			balance BIGINT NOT NULL,
			buying_liabilities BIGINT NOT NULL,
			selling_liabilities BIGINT NOT NULL,
			num_subentries INT NOT NULL,
			num_sponsoring INT NOT NULL,
			num_sponsored INT NOT NULL,
			sequence_number BIGINT,
			last_modified_ledger BIGINT NOT NULL,
			ledger_sequence BIGINT NOT NULL,
			ledger_range BIGINT
		)`,
		catalogName, schemaName,
	)

	if _, err := db.Exec(createSQL); err != nil {
		return fmt.Errorf("failed to create native_balances_snapshot_v1 table: %w", err)
	}
	log.Printf("BronzeCopier: Table ready: %s.%s.native_balances_snapshot_v1", catalogName, schemaName)
	return nil
}

// createAccountsTable creates the accounts_snapshot_v1 table
func createAccountsTable(db *sql.DB, catalogName, schemaName string) error {
	createSQL := fmt.Sprintf(`
		CREATE TABLE IF NOT EXISTS %s.%s.accounts_snapshot_v1 (
			-- Identity (3 fields)
			account_id VARCHAR NOT NULL,
			ledger_sequence BIGINT NOT NULL,
			closed_at TIMESTAMP NOT NULL,

			-- Balance (1 field)
			balance VARCHAR NOT NULL,

			-- Account Settings (5 fields)
			sequence_number BIGINT NOT NULL,
			num_subentries INT NOT NULL,
			num_sponsoring INT NOT NULL,
			num_sponsored INT NOT NULL,
			home_domain VARCHAR,

			-- Thresholds (4 fields)
			master_weight INT NOT NULL,
			low_threshold INT NOT NULL,
			med_threshold INT NOT NULL,
			high_threshold INT NOT NULL,

			-- Flags (5 fields)
			flags INT NOT NULL,
			auth_required BOOLEAN NOT NULL,
			auth_revocable BOOLEAN NOT NULL,
			auth_immutable BOOLEAN NOT NULL,
			auth_clawback_enabled BOOLEAN NOT NULL,

			-- Signers (1 field) - JSON array
			signers VARCHAR,

			-- Sponsorship (1 field)
			sponsor_account VARCHAR,

			-- Metadata (3 fields)
			created_at TIMESTAMP NOT NULL,
			updated_at TIMESTAMP NOT NULL,
			ledger_range BIGINT NOT NULL
		)`,
		catalogName, schemaName,
	)

	if _, err := db.Exec(createSQL); err != nil {
		return fmt.Errorf("failed to create accounts_snapshot_v1 table: %w", err)
	}
	log.Printf("BronzeCopier: Table ready: %s.%s.accounts_snapshot_v1", catalogName, schemaName)
	return nil
}

// createTrustlinesTable creates the trustlines_snapshot_v1 table
func createTrustlinesTable(db *sql.DB, catalogName, schemaName string) error {
	createSQL := fmt.Sprintf(`
		CREATE TABLE IF NOT EXISTS %s.%s.trustlines_snapshot_v1 (
			-- Identity (4 fields)
			account_id VARCHAR NOT NULL,
			asset_code VARCHAR NOT NULL,
			asset_issuer VARCHAR NOT NULL,
			asset_type VARCHAR NOT NULL,

			-- Trust & Balance (4 fields)
			balance VARCHAR NOT NULL,
			trust_limit VARCHAR NOT NULL,
			buying_liabilities VARCHAR NOT NULL,
			selling_liabilities VARCHAR NOT NULL,

			-- Authorization (3 fields)
			authorized BOOLEAN NOT NULL,
			authorized_to_maintain_liabilities BOOLEAN NOT NULL,
			clawback_enabled BOOLEAN NOT NULL,

			-- Metadata (3 fields)
			ledger_sequence BIGINT NOT NULL,
			created_at TIMESTAMP NOT NULL,
			ledger_range BIGINT NOT NULL
		)`,
		catalogName, schemaName,
	)

	if _, err := db.Exec(createSQL); err != nil {
		return fmt.Errorf("failed to create trustlines_snapshot_v1 table: %w", err)
	}
	log.Printf("BronzeCopier: Table ready: %s.%s.trustlines_snapshot_v1", catalogName, schemaName)
	return nil
}

// createOffersTable creates the offers_snapshot_v1 table
func createOffersTable(db *sql.DB, catalogName, schemaName string) error {
	createSQL := fmt.Sprintf(`
		CREATE TABLE IF NOT EXISTS %s.%s.offers_snapshot_v1 (
			-- Identity (4 fields)
			offer_id BIGINT NOT NULL,
			seller_account VARCHAR NOT NULL,
			ledger_sequence BIGINT NOT NULL,
			closed_at TIMESTAMP NOT NULL,

			-- Selling Asset (3 fields)
			selling_asset_type VARCHAR NOT NULL,
			selling_asset_code VARCHAR,
			selling_asset_issuer VARCHAR,

			-- Buying Asset (3 fields)
			buying_asset_type VARCHAR NOT NULL,
			buying_asset_code VARCHAR,
			buying_asset_issuer VARCHAR,

			-- Offer Details (2 fields)
			amount VARCHAR NOT NULL,
			price VARCHAR NOT NULL,

			-- Flags (1 field)
			flags INT NOT NULL,

			-- Metadata (2 fields)
			created_at TIMESTAMP NOT NULL,
			ledger_range BIGINT NOT NULL
		)`,
		catalogName, schemaName,
	)

	if _, err := db.Exec(createSQL); err != nil {
		return fmt.Errorf("failed to create offers_snapshot_v1 table: %w", err)
	}
	log.Printf("BronzeCopier: Table ready: %s.%s.offers_snapshot_v1", catalogName, schemaName)
	return nil
}

// createClaimableBalancesTable creates the claimable_balances_snapshot_v1 table
func createClaimableBalancesTable(db *sql.DB, catalogName, schemaName string) error {
	createSQL := fmt.Sprintf(`
		CREATE TABLE IF NOT EXISTS %s.%s.claimable_balances_snapshot_v1 (
			-- Identity (4 fields)
			balance_id VARCHAR NOT NULL,
			sponsor VARCHAR NOT NULL,
			ledger_sequence BIGINT NOT NULL,
			closed_at TIMESTAMP NOT NULL,

			-- Asset & Amount (4 fields)
			asset_type VARCHAR NOT NULL,
			asset_code VARCHAR,
			asset_issuer VARCHAR,
			amount BIGINT NOT NULL,

			-- Claimants (1 field)
			claimants_count INT NOT NULL,

			-- Flags (1 field)
			flags INT NOT NULL,

			-- Metadata (2 fields)
			created_at TIMESTAMP NOT NULL,
			ledger_range BIGINT NOT NULL
		)`,
		catalogName, schemaName,
	)

	if _, err := db.Exec(createSQL); err != nil {
		return fmt.Errorf("failed to create claimable_balances_snapshot_v1 table: %w", err)
	}
	log.Printf("BronzeCopier: Table ready: %s.%s.claimable_balances_snapshot_v1", catalogName, schemaName)
	return nil
}

// createLiquidityPoolsTable creates the liquidity_pools_snapshot_v1 table
func createLiquidityPoolsTable(db *sql.DB, catalogName, schemaName string) error {
	createSQL := fmt.Sprintf(`
		CREATE TABLE IF NOT EXISTS %s.%s.liquidity_pools_snapshot_v1 (
			-- Identity (3 fields)
			liquidity_pool_id VARCHAR NOT NULL,
			ledger_sequence BIGINT NOT NULL,
			closed_at TIMESTAMP NOT NULL,

			-- Pool Type (1 field)
			pool_type VARCHAR NOT NULL,

			-- Fee (1 field)
			fee INT NOT NULL,

			-- Pool Shares (2 fields)
			trustline_count INT NOT NULL,
			total_pool_shares BIGINT NOT NULL,

			-- Asset A (4 fields)
			asset_a_type VARCHAR NOT NULL,
			asset_a_code VARCHAR,
			asset_a_issuer VARCHAR,
			asset_a_amount BIGINT NOT NULL,

			-- Asset B (4 fields)
			asset_b_type VARCHAR NOT NULL,
			asset_b_code VARCHAR,
			asset_b_issuer VARCHAR,
			asset_b_amount BIGINT NOT NULL,

			-- Metadata (2 fields)
			created_at TIMESTAMP NOT NULL,
			ledger_range BIGINT NOT NULL
		)`,
		catalogName, schemaName,
	)

	if _, err := db.Exec(createSQL); err != nil {
		return fmt.Errorf("failed to create liquidity_pools_snapshot_v1 table: %w", err)
	}
	log.Printf("BronzeCopier: Table ready: %s.%s.liquidity_pools_snapshot_v1", catalogName, schemaName)
	return nil
}

// createContractEventsTable creates the contract_events_stream_v1 table
func createContractEventsTable(db *sql.DB, catalogName, schemaName string) error {
	createSQL := fmt.Sprintf(`
		CREATE TABLE IF NOT EXISTS %s.%s.contract_events_stream_v1 (
			-- Identity (5 fields)
			event_id VARCHAR NOT NULL,
			contract_id VARCHAR,
			ledger_sequence BIGINT NOT NULL,
			transaction_hash VARCHAR NOT NULL,
			closed_at TIMESTAMP NOT NULL,

			-- Event Type (2 fields)
			event_type VARCHAR NOT NULL,
			in_successful_contract_call BOOLEAN NOT NULL,

			-- Event Data (5 fields - Hubble compatible with decoded versions)
			topics_json VARCHAR NOT NULL,
			topics_decoded VARCHAR NOT NULL,
			data_xdr VARCHAR NOT NULL,
			data_decoded VARCHAR NOT NULL,
			topic_count INT NOT NULL,

			-- Context (2 fields)
			operation_index INT NOT NULL,
			event_index INT NOT NULL,

			-- Metadata (2 fields)
			created_at TIMESTAMP NOT NULL,
			ledger_range BIGINT NOT NULL
		)`,
		catalogName, schemaName,
	)

	if _, err := db.Exec(createSQL); err != nil {
		return fmt.Errorf("failed to create contract_events_stream_v1 table: %w", err)
	}
	log.Printf("BronzeCopier: Table ready: %s.%s.contract_events_stream_v1", catalogName, schemaName)
	return nil
}

// createContractDataTable creates the contract_data_snapshot_v1 table
func createContractDataTable(db *sql.DB, catalogName, schemaName string) error {
	createSQL := fmt.Sprintf(`
		CREATE TABLE IF NOT EXISTS %s.%s.contract_data_snapshot_v1 (
			-- Identity (3 fields)
			contract_id VARCHAR NOT NULL,
			ledger_sequence BIGINT NOT NULL,
			ledger_key_hash VARCHAR NOT NULL,

			-- Contract metadata (2 fields)
			contract_key_type VARCHAR NOT NULL,
			contract_durability VARCHAR NOT NULL,

			-- Asset information (3 fields, nullable)
			asset_code VARCHAR,
			asset_issuer VARCHAR,
			asset_type VARCHAR,

			-- Balance information (2 fields, nullable)
			balance_holder VARCHAR,
			balance VARCHAR,

			-- Ledger metadata (4 fields)
			last_modified_ledger INT NOT NULL,
			ledger_entry_change INT NOT NULL,
			deleted BOOLEAN NOT NULL,
			closed_at TIMESTAMP NOT NULL,

			-- XDR data (1 field)
			contract_data_xdr TEXT NOT NULL,

			-- Metadata (2 fields)
			created_at TIMESTAMP NOT NULL,
			ledger_range BIGINT NOT NULL
		)`,
		catalogName, schemaName,
	)

	if _, err := db.Exec(createSQL); err != nil {
		return fmt.Errorf("failed to create contract_data_snapshot_v1 table: %w", err)
	}
	log.Printf("BronzeCopier: Table ready: %s.%s.contract_data_snapshot_v1", catalogName, schemaName)
	return nil
}

// createContractCodeTable creates the contract_code_snapshot_v1 table
func createContractCodeTable(db *sql.DB, catalogName, schemaName string) error {
	createSQL := fmt.Sprintf(`
		CREATE TABLE IF NOT EXISTS %s.%s.contract_code_snapshot_v1 (
			-- Identity (2 fields)
			contract_code_hash VARCHAR NOT NULL,
			ledger_key_hash VARCHAR NOT NULL,

			-- Extension (1 field)
			contract_code_ext_v INT NOT NULL,

			-- Ledger metadata (4 fields)
			last_modified_ledger INT NOT NULL,
			ledger_entry_change INT NOT NULL,
			deleted BOOLEAN NOT NULL,
			closed_at TIMESTAMP NOT NULL,

			-- Ledger tracking (1 field)
			ledger_sequence BIGINT NOT NULL,

			-- WASM metadata (10 fields, nullable)
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

			-- Metadata (2 fields)
			created_at TIMESTAMP NOT NULL,
			ledger_range BIGINT NOT NULL
		)`,
		catalogName, schemaName,
	)

	if _, err := db.Exec(createSQL); err != nil {
		return fmt.Errorf("failed to create contract_code_snapshot_v1 table: %w", err)
	}
	log.Printf("BronzeCopier: Table ready: %s.%s.contract_code_snapshot_v1", catalogName, schemaName)
	return nil
}

// createConfigSettingsTable creates the config_settings_snapshot_v1 table
func createConfigSettingsTable(db *sql.DB, catalogName, schemaName string) error {
	createSQL := fmt.Sprintf(`
		CREATE TABLE IF NOT EXISTS %s.%s.config_settings_snapshot_v1 (
			-- Identity (2 fields)
			config_setting_id INT NOT NULL,
			ledger_sequence BIGINT NOT NULL,

			-- Ledger metadata (3 fields)
			last_modified_ledger INT NOT NULL,
			deleted BOOLEAN NOT NULL,
			closed_at TIMESTAMP NOT NULL,

			-- Soroban compute settings (4 fields, nullable)
			ledger_max_instructions BIGINT,
			tx_max_instructions BIGINT,
			fee_rate_per_instructions_increment BIGINT,
			tx_memory_limit UINTEGER,

			-- Soroban ledger cost settings (8 fields, nullable)
			ledger_max_read_ledger_entries UINTEGER,
			ledger_max_read_bytes UINTEGER,
			ledger_max_write_ledger_entries UINTEGER,
			ledger_max_write_bytes UINTEGER,
			tx_max_read_ledger_entries UINTEGER,
			tx_max_read_bytes UINTEGER,
			tx_max_write_ledger_entries UINTEGER,
			tx_max_write_bytes UINTEGER,

			-- Contract size limit (1 field, nullable)
			contract_max_size_bytes UINTEGER,

			-- Raw XDR (1 field)
			config_setting_xdr TEXT NOT NULL,

			-- Metadata (2 fields)
			created_at TIMESTAMP NOT NULL,
			ledger_range BIGINT NOT NULL
		)`,
		catalogName, schemaName,
	)

	if _, err := db.Exec(createSQL); err != nil {
		return fmt.Errorf("failed to create config_settings_snapshot_v1 table: %w", err)
	}
	log.Printf("BronzeCopier: Table ready: %s.%s.config_settings_snapshot_v1", catalogName, schemaName)
	return nil
}

// createTTLTable creates the ttl_snapshot_v1 table
func createTTLTable(db *sql.DB, catalogName, schemaName string) error {
	createSQL := fmt.Sprintf(`
		CREATE TABLE IF NOT EXISTS %s.%s.ttl_snapshot_v1 (
			-- Identity (2 fields)
			key_hash VARCHAR NOT NULL,
			ledger_sequence BIGINT NOT NULL,

			-- TTL tracking (3 fields)
			live_until_ledger_seq BIGINT NOT NULL,
			ttl_remaining BIGINT NOT NULL,
			expired BOOLEAN NOT NULL,

			-- Ledger metadata (3 fields)
			last_modified_ledger INT NOT NULL,
			deleted BOOLEAN NOT NULL,
			closed_at TIMESTAMP NOT NULL,

			-- Metadata (2 fields)
			created_at TIMESTAMP NOT NULL,
			ledger_range BIGINT NOT NULL
		)`,
		catalogName, schemaName,
	)

	if _, err := db.Exec(createSQL); err != nil {
		return fmt.Errorf("failed to create ttl_snapshot_v1 table: %w", err)
	}
	log.Printf("BronzeCopier: Table ready: %s.%s.ttl_snapshot_v1", catalogName, schemaName)
	return nil
}

// createEvictedKeysTable creates the evicted_keys_state_v1 table
func createEvictedKeysTable(db *sql.DB, catalogName, schemaName string) error {
	createSQL := fmt.Sprintf(`
		CREATE TABLE IF NOT EXISTS %s.%s.evicted_keys_state_v1 (
			-- Identity (2 fields)
			key_hash VARCHAR NOT NULL,
			ledger_sequence BIGINT NOT NULL,

			-- Eviction details (3 fields)
			contract_id VARCHAR NOT NULL,
			key_type VARCHAR NOT NULL,
			durability VARCHAR NOT NULL,

			-- Metadata (3 fields)
			closed_at TIMESTAMP NOT NULL,
			ledger_range BIGINT NOT NULL,
			created_at TIMESTAMP NOT NULL
		)`,
		catalogName, schemaName,
	)

	if _, err := db.Exec(createSQL); err != nil {
		return fmt.Errorf("failed to create evicted_keys_state_v1 table: %w", err)
	}
	log.Printf("BronzeCopier: Table ready: %s.%s.evicted_keys_state_v1", catalogName, schemaName)
	return nil
}

// createRestoredKeysTable creates the restored_keys_state_v1 table
func createRestoredKeysTable(db *sql.DB, catalogName, schemaName string) error {
	createSQL := fmt.Sprintf(`
		CREATE TABLE IF NOT EXISTS %s.%s.restored_keys_state_v1 (
			-- Identity (2 fields)
			key_hash VARCHAR NOT NULL,
			ledger_sequence BIGINT NOT NULL,

			-- Restoration details (4 fields)
			contract_id VARCHAR NOT NULL,
			key_type VARCHAR NOT NULL,
			durability VARCHAR NOT NULL,
			restored_from_ledger BIGINT NOT NULL,

			-- Metadata (3 fields)
			closed_at TIMESTAMP NOT NULL,
			ledger_range BIGINT NOT NULL,
			created_at TIMESTAMP NOT NULL
		)`,
		catalogName, schemaName,
	)

	if _, err := db.Exec(createSQL); err != nil {
		return fmt.Errorf("failed to create restored_keys_state_v1 table: %w", err)
	}
	log.Printf("BronzeCopier: Table ready: %s.%s.restored_keys_state_v1", catalogName, schemaName)
	return nil
}

// createAccountSignersTable creates the account_signers_snapshot_v1 table
func createAccountSignersTable(db *sql.DB, catalogName, schemaName string) error {
	createSQL := fmt.Sprintf(`
		CREATE TABLE IF NOT EXISTS %s.%s.account_signers_snapshot_v1 (
			-- Identity (3 fields)
			account_id VARCHAR NOT NULL,
			signer VARCHAR NOT NULL,
			ledger_sequence BIGINT NOT NULL,

			-- Signer details (2 fields)
			weight INTEGER NOT NULL,
			sponsor VARCHAR,

			-- Status (1 field)
			deleted BOOLEAN NOT NULL,

			-- Metadata (3 fields)
			closed_at TIMESTAMP NOT NULL,
			ledger_range BIGINT NOT NULL,
			created_at TIMESTAMP NOT NULL
		)`,
		catalogName, schemaName,
	)

	if _, err := db.Exec(createSQL); err != nil {
		return fmt.Errorf("failed to create account_signers_snapshot_v1 table: %w", err)
	}
	log.Printf("BronzeCopier: Table ready: %s.%s.account_signers_snapshot_v1", catalogName, schemaName)
	return nil
}
