package consumer

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"log"
	"strings"
	"sync"
	"time"

	_ "github.com/lib/pq"
	"github.com/withObsrvr/cdp-pipeline-workflow/processor"
)

// SaveToPostgreSQLBronze implements a high-performance Bronze layer consumer
// for PostgreSQL with COPY protocol, time-based partitioning, and retention policies.
type SaveToPostgreSQLBronze struct {
	db                  *sql.DB
	processors          []processor.Processor
	buffers             map[string][]map[string]interface{} // Table name -> buffered records
	bufferMutex         sync.Mutex
	batchSize           int
	retentionDays       int
	enablePartitioning  bool
	enableRetention     bool
	retentionCheckTimer *time.Ticker
	stopChan            chan struct{}
	logger              *log.Logger
}

// PostgresBronzeConfig holds configuration for Bronze PostgreSQL consumer
type PostgresBronzeConfig struct {
	Host               string
	Port               int
	Database           string
	Username           string
	Password           string
	SSLMode            string
	MaxOpenConns       int
	MaxIdleConns       int
	BatchSize          int  // Number of records to buffer before COPY (default: 200)
	RetentionDays      int  // Days to keep data (default: 30)
	EnablePartitioning bool // Enable time-based partitioning (default: true)
	EnableRetention    bool // Enable automatic retention policy (default: true)
	RetentionInterval  int  // Hours between retention checks (default: 24)
}

// Bronze table schemas with time-based partitioning support
const bronzeInitSchema = `
-- Enable TimescaleDB extension if available (optional, falls back to native partitioning)
CREATE EXTENSION IF NOT EXISTS timescaledb CASCADE;

-- =====================================================================
-- LEDGERS TABLE (24 columns)
-- =====================================================================
CREATE TABLE IF NOT EXISTS bronze_ledgers_row_v2 (
    sequence                BIGINT NOT NULL,
    ledger_hash             VARCHAR(64) NOT NULL,
    previous_ledger_hash    VARCHAR(64) NOT NULL,
    closed_at               TIMESTAMPTZ NOT NULL,
    protocol_version        INTEGER NOT NULL,
    total_coins             BIGINT NOT NULL,
    fee_pool                BIGINT NOT NULL,
    base_fee                INTEGER NOT NULL,
    base_reserve            INTEGER NOT NULL,
    max_tx_set_size         INTEGER NOT NULL,
    successful_tx_count     INTEGER NOT NULL,
    failed_tx_count         INTEGER NOT NULL,
    ingestion_timestamp     TIMESTAMPTZ,
    ledger_range            INTEGER,
    transaction_count       INTEGER,
    operation_count         INTEGER,
    tx_set_operation_count  INTEGER,
    soroban_fee_write_1kb   BIGINT,
    node_id                 TEXT,
    signature               TEXT,
    ledger_header           TEXT,
    bucket_list_size        BIGINT,
    live_soroban_state_size BIGINT,
    evicted_keys_count      INTEGER,
    PRIMARY KEY (sequence, closed_at)
) PARTITION BY RANGE (closed_at);

CREATE INDEX IF NOT EXISTS idx_bronze_ledgers_closed_at ON bronze_ledgers_row_v2(closed_at);
CREATE INDEX IF NOT EXISTS idx_bronze_ledgers_sequence ON bronze_ledgers_row_v2(sequence);

-- =====================================================================
-- TRANSACTIONS TABLE (46 columns)
-- =====================================================================
CREATE TABLE IF NOT EXISTS bronze_transactions_row_v2 (
    ledger_sequence                 BIGINT NOT NULL,
    transaction_hash                VARCHAR(64) NOT NULL,
    source_account                  VARCHAR(56) NOT NULL,
    fee_charged                     BIGINT NOT NULL,
    max_fee                         BIGINT NOT NULL,
    successful                      BOOLEAN NOT NULL,
    transaction_result_code         VARCHAR(50) NOT NULL,
    operation_count                 INTEGER NOT NULL,
    memo_type                       VARCHAR(20),
    memo                            TEXT,
    created_at                      TIMESTAMPTZ NOT NULL,
    account_sequence                BIGINT,
    ledger_range                    INTEGER,
    source_account_muxed            VARCHAR(100),
    fee_account_muxed               VARCHAR(100),
    inner_transaction_hash          VARCHAR(64),
    fee_bump_fee                    BIGINT,
    max_fee_bid                     BIGINT,
    inner_source_account            VARCHAR(56),
    timebounds_min_time             BIGINT,
    timebounds_max_time             BIGINT,
    ledgerbounds_min                INTEGER,
    ledgerbounds_max                INTEGER,
    min_sequence_number             BIGINT,
    min_sequence_age                BIGINT,
    soroban_resources_instructions  BIGINT,
    soroban_resources_read_bytes    BIGINT,
    soroban_resources_write_bytes   BIGINT,
    soroban_data_size_bytes         INTEGER,
    soroban_data_resources          TEXT,
    soroban_fee_base                BIGINT,
    soroban_fee_resources           BIGINT,
    soroban_fee_refund              BIGINT,
    soroban_fee_charged             BIGINT,
    soroban_fee_wasted              BIGINT,
    soroban_host_function_type      VARCHAR(50),
    soroban_contract_id             VARCHAR(56),
    soroban_contract_events_count   INTEGER,
    signatures_count                INTEGER NOT NULL,
    new_account                     BOOLEAN NOT NULL,
    tx_envelope                     TEXT,
    tx_result                       TEXT,
    tx_meta                         TEXT,
    tx_fee_meta                     TEXT,
    tx_signers                      TEXT,
    extra_signers                   TEXT,
    PRIMARY KEY (transaction_hash, created_at)
) PARTITION BY RANGE (created_at);

CREATE INDEX IF NOT EXISTS idx_bronze_transactions_created_at ON bronze_transactions_row_v2(created_at);
CREATE INDEX IF NOT EXISTS idx_bronze_transactions_ledger ON bronze_transactions_row_v2(ledger_sequence);
CREATE INDEX IF NOT EXISTS idx_bronze_transactions_source ON bronze_transactions_row_v2(source_account);

-- =====================================================================
-- OPERATIONS TABLE (58 columns)
-- =====================================================================
CREATE TABLE IF NOT EXISTS bronze_operations_row_v2 (
    transaction_hash                        VARCHAR(64) NOT NULL,
    operation_index                         INTEGER NOT NULL,
    ledger_sequence                         BIGINT NOT NULL,
    source_account                          VARCHAR(56) NOT NULL,
    type                                    INTEGER NOT NULL,
    type_string                             VARCHAR(50) NOT NULL,
    created_at                              TIMESTAMPTZ NOT NULL,
    transaction_successful                  BOOLEAN NOT NULL,
    operation_result_code                   VARCHAR(50),
    operation_trace_code                    VARCHAR(50),
    ledger_range                            INTEGER,
    source_account_muxed                    VARCHAR(100),
    asset                                   VARCHAR(100),
    asset_type                              VARCHAR(20),
    asset_code                              VARCHAR(12),
    asset_issuer                            VARCHAR(56),
    source_asset                            VARCHAR(100),
    source_asset_type                       VARCHAR(20),
    source_asset_code                       VARCHAR(12),
    source_asset_issuer                     VARCHAR(56),
    amount                                  BIGINT,
    source_amount                           BIGINT,
    destination_min                         BIGINT,
    starting_balance                        BIGINT,
    destination                             VARCHAR(56),
    trustline_limit                         BIGINT,
    trustor                                 VARCHAR(56),
    authorize                               BOOLEAN,
    authorize_to_maintain_liabilities       BOOLEAN,
    clawback_enabled                        BOOLEAN,
    set_flags                               INTEGER,
    clear_flags                             INTEGER,
    high_threshold                          INTEGER,
    medium_threshold                        INTEGER,
    low_threshold                           INTEGER,
    home_domain                             VARCHAR(255),
    signer_key                              VARCHAR(56),
    signer_weight                           INTEGER,
    master_weight                           INTEGER,
    inflation_destination                   VARCHAR(56),
    name                                    VARCHAR(100),
    value                                   TEXT,
    sponsor                                 VARCHAR(56),
    sponsored_id                            VARCHAR(56),
    begin_sponsor                           VARCHAR(56),
    liquidity_pool_id                       VARCHAR(64),
    liquidity_pool_type                     VARCHAR(20),
    liquidity_pool_fee                      INTEGER,
    reserve_a_asset                         VARCHAR(100),
    reserve_a_amount                        BIGINT,
    reserve_a_max_amount                    BIGINT,
    reserve_a_deposit                       BIGINT,
    reserve_b_asset                         VARCHAR(100),
    reserve_b_amount                        BIGINT,
    reserve_b_max_amount                    BIGINT,
    reserve_b_deposit                       BIGINT,
    shares_received                         BIGINT,
    PRIMARY KEY (transaction_hash, operation_index, created_at)
) PARTITION BY RANGE (created_at);

CREATE INDEX IF NOT EXISTS idx_bronze_operations_created_at ON bronze_operations_row_v2(created_at);
CREATE INDEX IF NOT EXISTS idx_bronze_operations_ledger ON bronze_operations_row_v2(ledger_sequence);
CREATE INDEX IF NOT EXISTS idx_bronze_operations_type ON bronze_operations_row_v2(type_string);

-- Add remaining 16 Bronze tables with similar structure...
-- (Abbreviated for now, will add full schemas for all 19 tables)
`

func NewSaveToPostgreSQLBronze(config map[string]interface{}) (*SaveToPostgreSQLBronze, error) {
	pgConfig, err := parsePostgresBronzeConfig(config)
	if err != nil {
		return nil, err
	}

	connStr := fmt.Sprintf(
		"host=%s port=%d user=%s password=%s dbname=%s sslmode=%s",
		pgConfig.Host, pgConfig.Port, pgConfig.Username, pgConfig.Password,
		pgConfig.Database, pgConfig.SSLMode,
	)

	db, err := sql.Open("postgres", connStr)
	if err != nil {
		return nil, fmt.Errorf("error connecting to PostgreSQL: %w", err)
	}

	// Set connection pool settings
	db.SetMaxOpenConns(pgConfig.MaxOpenConns)
	db.SetMaxIdleConns(pgConfig.MaxIdleConns)
	db.SetConnMaxLifetime(time.Hour)

	// Test connection
	if err := db.Ping(); err != nil {
		return nil, fmt.Errorf("error pinging PostgreSQL: %w", err)
	}

	logger := log.New(log.Writer(), "[postgresql-bronze] ", log.LstdFlags)

	// Initialize schema
	logger.Println("Initializing Bronze schema with partitioning...")
	if err := initializeBronzeSchema(db); err != nil {
		return nil, fmt.Errorf("error initializing Bronze schema: %w", err)
	}

	consumer := &SaveToPostgreSQLBronze{
		db:                 db,
		buffers:            make(map[string][]map[string]interface{}),
		batchSize:          pgConfig.BatchSize,
		retentionDays:      pgConfig.RetentionDays,
		enablePartitioning: pgConfig.EnablePartitioning,
		enableRetention:    pgConfig.EnableRetention,
		stopChan:           make(chan struct{}),
		logger:             logger,
	}

	// Create initial partitions for the next 7 days
	if pgConfig.EnablePartitioning {
		logger.Println("Creating initial partitions...")
		if err := consumer.createPartitions(db, 7); err != nil {
			logger.Printf("Warning: Failed to create initial partitions: %v", err)
		}
	}

	// Start retention policy enforcement
	if pgConfig.EnableRetention {
		consumer.startRetentionEnforcement(time.Duration(pgConfig.RetentionInterval) * time.Hour)
	}

	logger.Printf("Bronze PostgreSQL consumer initialized (batch_size=%d, retention=%dd)",
		pgConfig.BatchSize, pgConfig.RetentionDays)

	return consumer, nil
}

func parsePostgresBronzeConfig(config map[string]interface{}) (PostgresBronzeConfig, error) {
	var pgConfig PostgresBronzeConfig

	log.Printf("[postgresql-bronze] Config: %+v", config)

	// Parse required fields
	host, ok := config["host"].(string)
	if !ok {
		return pgConfig, fmt.Errorf("missing host in config")
	}
	pgConfig.Host = host

	port, ok := config["port"].(float64)
	if !ok {
		portInt, ok := config["port"].(int)
		if ok {
			pgConfig.Port = portInt
		} else {
			pgConfig.Port = 5432
		}
	} else {
		pgConfig.Port = int(port)
	}

	database, ok := config["database"].(string)
	if !ok {
		return pgConfig, fmt.Errorf("missing database in config")
	}
	pgConfig.Database = database

	username, ok := config["username"].(string)
	if !ok {
		return pgConfig, fmt.Errorf("missing username in config")
	}
	pgConfig.Username = username

	password, ok := config["password"].(string)
	if !ok {
		return pgConfig, fmt.Errorf("missing password in config")
	}
	pgConfig.Password = password

	sslMode, ok := config["sslmode"].(string)
	if !ok {
		pgConfig.SSLMode = "disable"
	} else {
		pgConfig.SSLMode = sslMode
	}

	// Set defaults
	pgConfig.MaxOpenConns = 25
	pgConfig.MaxIdleConns = 5
	pgConfig.BatchSize = 200
	pgConfig.RetentionDays = 30
	pgConfig.EnablePartitioning = true
	pgConfig.EnableRetention = true
	pgConfig.RetentionInterval = 24

	// Parse optional fields
	if maxOpen, ok := config["max_open_conns"].(float64); ok {
		pgConfig.MaxOpenConns = int(maxOpen)
	}
	if maxIdle, ok := config["max_idle_conns"].(float64); ok {
		pgConfig.MaxIdleConns = int(maxIdle)
	}
	if batchSize, ok := config["batch_size"].(float64); ok {
		pgConfig.BatchSize = int(batchSize)
	}
	if retentionDays, ok := config["retention_days"].(float64); ok {
		pgConfig.RetentionDays = int(retentionDays)
	}
	if enablePart, ok := config["enable_partitioning"].(bool); ok {
		pgConfig.EnablePartitioning = enablePart
	}
	if enableRet, ok := config["enable_retention"].(bool); ok {
		pgConfig.EnableRetention = enableRet
	}
	if retInterval, ok := config["retention_interval"].(float64); ok {
		pgConfig.RetentionInterval = int(retInterval)
	}

	return pgConfig, nil
}

func initializeBronzeSchema(db *sql.DB) error {
	_, err := db.Exec(bronzeInitSchema)
	return err
}

func (p *SaveToPostgreSQLBronze) Subscribe(processor processor.Processor) {
	p.processors = append(p.processors, processor)
}

func (p *SaveToPostgreSQLBronze) Process(ctx context.Context, msg processor.Message) error {
	payload, ok := msg.Payload.([]byte)
	if !ok {
		return fmt.Errorf("expected []byte payload, got %T", msg.Payload)
	}

	// Parse the Bronze record
	var data map[string]interface{}
	if err := json.Unmarshal(payload, &data); err != nil {
		return fmt.Errorf("error unmarshaling payload: %w", err)
	}

	// Determine table name from record type
	tableName, ok := data["_table"].(string)
	if !ok {
		return fmt.Errorf("missing _table field in Bronze record")
	}

	// Add to buffer
	p.bufferMutex.Lock()
	p.buffers[tableName] = append(p.buffers[tableName], data)
	bufferSize := len(p.buffers[tableName])
	p.bufferMutex.Unlock()

	// Flush if batch size reached
	if bufferSize >= p.batchSize {
		return p.flushTable(ctx, tableName)
	}

	return nil
}

// flushTable uses COPY protocol for high-performance bulk insert
func (p *SaveToPostgreSQLBronze) flushTable(ctx context.Context, tableName string) error {
	p.bufferMutex.Lock()
	records := p.buffers[tableName]
	p.buffers[tableName] = nil
	p.bufferMutex.Unlock()

	if len(records) == 0 {
		return nil
	}

	start := time.Now()

	tx, err := p.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("error starting transaction: %w", err)
	}
	defer tx.Rollback()

	// Use COPY for bulk insert
	fullTableName := "bronze_" + tableName
	columns := p.getTableColumns(tableName)
	stmt, err := tx.PrepareContext(ctx, fmt.Sprintf("COPY %s (%s) FROM STDIN", fullTableName, strings.Join(columns, ",")))
	if err != nil {
		return fmt.Errorf("error preparing COPY statement: %w", err)
	}

	for _, record := range records {
		values := p.extractRecordValues(tableName, record, columns)
		if _, err := stmt.ExecContext(ctx, values...); err != nil {
			return fmt.Errorf("error executing COPY: %w", err)
		}
	}

	if _, err := stmt.ExecContext(ctx); err != nil {
		return fmt.Errorf("error finalizing COPY: %w", err)
	}

	if err := tx.Commit(); err != nil {
		return fmt.Errorf("error committing transaction: %w", err)
	}

	duration := time.Since(start)
	p.logger.Printf("Flushed %d records to %s in %v", len(records), fullTableName, duration)

	return nil
}

// getTableColumns returns the ordered column list for a Bronze table
func (p *SaveToPostgreSQLBronze) getTableColumns(tableName string) []string {
	switch tableName {
	case "ledgers_row_v2":
		return []string{
			"sequence", "ledger_hash", "previous_ledger_hash", "closed_at",
			"protocol_version", "total_coins", "fee_pool", "base_fee",
			"base_reserve", "max_tx_set_size", "successful_tx_count", "failed_tx_count",
			"ingestion_timestamp", "ledger_range", "transaction_count", "operation_count",
			"tx_set_operation_count", "soroban_fee_write_1kb", "node_id", "signature",
			"ledger_header", "bucket_list_size", "live_soroban_state_size", "evicted_keys_count",
		}
	case "transactions_row_v2":
		return []string{
			"ledger_sequence", "transaction_hash", "source_account", "fee_charged",
			"max_fee", "successful", "transaction_result_code", "operation_count",
			"memo_type", "memo", "created_at", "account_sequence", "ledger_range",
			"source_account_muxed", "fee_account_muxed", "inner_transaction_hash",
			"fee_bump_fee", "max_fee_bid", "inner_source_account", "timebounds_min_time",
			"timebounds_max_time", "ledgerbounds_min", "ledgerbounds_max", "min_sequence_number",
			"min_sequence_age", "soroban_resources_instructions", "soroban_resources_read_bytes",
			"soroban_resources_write_bytes", "soroban_data_size_bytes", "soroban_data_resources",
			"soroban_fee_base", "soroban_fee_resources", "soroban_fee_refund", "soroban_fee_charged",
			"soroban_fee_wasted", "soroban_host_function_type", "soroban_contract_id",
			"soroban_contract_events_count", "signatures_count", "new_account", "tx_envelope",
			"tx_result", "tx_meta", "tx_fee_meta", "tx_signers", "extra_signers",
		}
	case "operations_row_v2":
		return []string{
			"transaction_hash", "operation_index", "ledger_sequence", "source_account",
			"type", "type_string", "created_at", "transaction_successful",
			"operation_result_code", "operation_trace_code", "ledger_range", "source_account_muxed",
			"asset", "asset_type", "asset_code", "asset_issuer", "source_asset",
			"source_asset_type", "source_asset_code", "source_asset_issuer", "amount",
			"source_amount", "destination_min", "starting_balance", "destination",
			"trustline_limit", "trustor", "authorize", "authorize_to_maintain_liabilities",
			"clawback_enabled", "set_flags", "clear_flags", "high_threshold",
			"medium_threshold", "low_threshold", "home_domain", "signer_key",
			"signer_weight", "master_weight", "inflation_destination", "name",
			"value", "sponsor", "sponsored_id", "begin_sponsor", "liquidity_pool_id",
			"liquidity_pool_type", "liquidity_pool_fee", "reserve_a_asset", "reserve_a_amount",
			"reserve_a_max_amount", "reserve_a_deposit", "reserve_b_asset", "reserve_b_amount",
			"reserve_b_max_amount", "reserve_b_deposit", "shares_received",
		}
	default:
		p.logger.Printf("Warning: Unknown table %s, using empty column list", tableName)
		return []string{}
	}
}

// extractRecordValues extracts values from a Bronze record in column order
func (p *SaveToPostgreSQLBronze) extractRecordValues(tableName string, record map[string]interface{}, columns []string) []interface{} {
	values := make([]interface{}, len(columns))
	for i, col := range columns {
		values[i] = record[col]
	}
	return values
}

// createPartitions creates daily partitions for the next N days
func (p *SaveToPostgreSQLBronze) createPartitions(db *sql.DB, daysAhead int) error {
	now := time.Now().UTC()

	tables := []string{"bronze_ledgers_row_v2", "bronze_transactions_row_v2", "bronze_operations_row_v2"}

	for _, table := range tables {
		for i := 0; i < daysAhead; i++ {
			partDate := now.AddDate(0, 0, i)
			partName := fmt.Sprintf("%s_%s", table, partDate.Format("20060102"))
			rangeStart := partDate.Format("2006-01-02")
			rangeEnd := partDate.AddDate(0, 0, 1).Format("2006-01-02")

			sql := fmt.Sprintf(`
				CREATE TABLE IF NOT EXISTS %s
				PARTITION OF %s
				FOR VALUES FROM ('%s') TO ('%s')
			`, partName, table, rangeStart, rangeEnd)

			if _, err := db.Exec(sql); err != nil {
				// Partition might already exist, log warning but continue
				p.logger.Printf("Warning: Failed to create partition %s: %v", partName, err)
			} else {
				p.logger.Printf("Created partition %s", partName)
			}
		}
	}

	return nil
}

// startRetentionEnforcement runs retention policy enforcement on a schedule
func (p *SaveToPostgreSQLBronze) startRetentionEnforcement(interval time.Duration) {
	p.retentionCheckTimer = time.NewTicker(interval)

	go func() {
		for {
			select {
			case <-p.retentionCheckTimer.C:
				p.logger.Println("Running retention policy enforcement...")
				if err := p.enforceRetention(); err != nil {
					p.logger.Printf("Error enforcing retention: %v", err)
				}
			case <-p.stopChan:
				return
			}
		}
	}()
}

// enforceRetention drops partitions older than retention period
func (p *SaveToPostgreSQLBronze) enforceRetention() error {
	cutoffDate := time.Now().UTC().AddDate(0, 0, -p.retentionDays)
	cutoffStr := cutoffDate.Format("20060102")

	tables := []string{"bronze_ledgers_row_v2", "bronze_transactions_row_v2", "bronze_operations_row_v2"}

	for _, table := range tables {
		// Find partitions older than retention period
		query := `
			SELECT tablename
			FROM pg_tables
			WHERE schemaname = 'public'
			  AND tablename LIKE $1
			  AND tablename < $2
		`

		rows, err := p.db.Query(query, table+"_%", table+"_"+cutoffStr)
		if err != nil {
			return fmt.Errorf("error querying partitions: %w", err)
		}
		defer rows.Close()

		var droppedCount int
		for rows.Next() {
			var partName string
			if err := rows.Scan(&partName); err != nil {
				return fmt.Errorf("error scanning partition name: %w", err)
			}

			dropSQL := fmt.Sprintf("DROP TABLE IF EXISTS %s", partName)
			if _, err := p.db.Exec(dropSQL); err != nil {
				p.logger.Printf("Error dropping partition %s: %v", partName, err)
			} else {
				p.logger.Printf("Dropped expired partition %s", partName)
				droppedCount++
			}
		}

		if droppedCount > 0 {
			p.logger.Printf("Dropped %d expired partitions from %s", droppedCount, table)
		}
	}

	return nil
}

func (p *SaveToPostgreSQLBronze) Close() error {
	// Flush all remaining buffers
	p.logger.Println("Flushing remaining buffers before close...")
	ctx := context.Background()
	for tableName := range p.buffers {
		if err := p.flushTable(ctx, tableName); err != nil {
			p.logger.Printf("Error flushing table %s: %v", tableName, err)
		}
	}

	// Stop retention enforcement
	if p.retentionCheckTimer != nil {
		p.retentionCheckTimer.Stop()
	}
	close(p.stopChan)

	// Close database connection
	if p.db != nil {
		return p.db.Close()
	}
	return nil
}
