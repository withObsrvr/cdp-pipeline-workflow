package consumer

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"strings"
	"sync"
	"time"

	_ "github.com/lib/pq"
	"github.com/stellar/go/support/log"
	"github.com/withObsrvr/cdp-pipeline-workflow/processor"
)

// WalletBackendPostgreSQL implements a consumer that matches Stellar wallet-backend's exact schema
type WalletBackendPostgreSQL struct {
	db                  *sql.DB
	bufferMutex         sync.Mutex
	transactionBuffer   []processor.TransactionXDROutput
	stateChangesBuffer  []processor.WalletBackendOutput
	participantsBuffer  []processor.ParticipantOutput
	effectsBuffer       []processor.StellarEffectsMessage
	accountsBuffer      []processor.AccountRecord
	bufferSize          int
	flushInterval       time.Duration
	maxRetries          int
	retryDelay          time.Duration
	createSchema        bool
	dropExisting        bool
	flushTimer          *time.Timer
	ctx                 context.Context
	cancel              context.CancelFunc
	wg                  sync.WaitGroup
	subscribers         []processor.Processor
}

// WalletBackendPostgreSQLConfig holds configuration for the consumer
type WalletBackendPostgreSQLConfig struct {
	Host              string `mapstructure:"host"`
	Port              int    `mapstructure:"port"`
	Database          string `mapstructure:"database"`
	Username          string `mapstructure:"username"`
	Password          string `mapstructure:"password"`
	SSLMode           string `mapstructure:"sslmode"`
	BufferSize        int    `mapstructure:"buffer_size"`
	FlushIntervalMs   int    `mapstructure:"flush_interval_ms"`
	MaxRetries        int    `mapstructure:"max_retries"`
	RetryDelayMs      int    `mapstructure:"retry_delay_ms"`
	MaxOpenConns      int    `mapstructure:"max_open_conns"`
	MaxIdleConns      int    `mapstructure:"max_idle_conns"`
	CreateSchema      bool   `mapstructure:"create_schema"`
	DropExisting      bool   `mapstructure:"drop_existing"`
}

// ConsumerWalletBackendPostgreSQL creates a new WalletBackendPostgreSQL consumer
func ConsumerWalletBackendPostgreSQL(config map[string]interface{}) (processor.Processor, error) {
	log.Infof("ConsumerWalletBackendPostgreSQL: Starting consumer creation")
	cfg := &WalletBackendPostgreSQLConfig{
		Host:            "localhost",
		Port:            5432,
		SSLMode:         "require",
		BufferSize:      1000,
		FlushIntervalMs: 5000,
		MaxRetries:      3,
		RetryDelayMs:    1000,
		MaxOpenConns:    20,
		MaxIdleConns:    10,
		CreateSchema:    true,
		DropExisting:    false,
	}

	// Parse configuration
	for key, value := range config {
		switch key {
		case "host":
			cfg.Host = value.(string)
		case "port":
			switch v := value.(type) {
			case int:
				cfg.Port = v
			case float64:
				cfg.Port = int(v)
			}
		case "database":
			cfg.Database = value.(string)
		case "username":
			cfg.Username = value.(string)
		case "password":
			cfg.Password = value.(string)
		case "sslmode":
			cfg.SSLMode = value.(string)
		case "buffer_size":
			switch v := value.(type) {
			case int:
				cfg.BufferSize = v
			case float64:
				cfg.BufferSize = int(v)
			}
		case "flush_interval_ms":
			switch v := value.(type) {
			case int:
				cfg.FlushIntervalMs = v
			case float64:
				cfg.FlushIntervalMs = int(v)
			}
		case "max_retries":
			switch v := value.(type) {
			case int:
				cfg.MaxRetries = v
			case float64:
				cfg.MaxRetries = int(v)
			}
		case "retry_delay_ms":
			switch v := value.(type) {
			case int:
				cfg.RetryDelayMs = v
			case float64:
				cfg.RetryDelayMs = int(v)
			}
		case "max_open_conns":
			switch v := value.(type) {
			case int:
				cfg.MaxOpenConns = v
			case float64:
				cfg.MaxOpenConns = int(v)
			}
		case "max_idle_conns":
			switch v := value.(type) {
			case int:
				cfg.MaxIdleConns = v
			case float64:
				cfg.MaxIdleConns = int(v)
			}
		case "create_schema":
			cfg.CreateSchema = value.(bool)
		case "drop_existing":
			cfg.DropExisting = value.(bool)
		}
	}

	// Connect to database
	connStr := fmt.Sprintf("host=%s port=%d user=%s password=%s dbname=%s sslmode=%s",
		cfg.Host, cfg.Port, cfg.Username, cfg.Password, cfg.Database, cfg.SSLMode)

	log.Infof("WalletBackendPostgreSQL: Connecting to database at %s:%d, database=%s", cfg.Host, cfg.Port, cfg.Database)

	db, err := sql.Open("postgres", connStr)
	if err != nil {
		return nil, fmt.Errorf("failed to connect to database: %w", err)
	}

	// Configure connection pool
	db.SetMaxOpenConns(cfg.MaxOpenConns)
	db.SetMaxIdleConns(cfg.MaxIdleConns)

	// Test connection
	if err := db.Ping(); err != nil {
		return nil, fmt.Errorf("failed to ping database: %w", err)
	}

	log.Infof("WalletBackendPostgreSQL: Successfully connected to database")

	ctx, cancel := context.WithCancel(context.Background())

	consumer := &WalletBackendPostgreSQL{
		db:                  db,
		transactionBuffer:   make([]processor.TransactionXDROutput, 0, cfg.BufferSize),
		stateChangesBuffer:  make([]processor.WalletBackendOutput, 0, cfg.BufferSize),
		participantsBuffer:  make([]processor.ParticipantOutput, 0, cfg.BufferSize),
		effectsBuffer:       make([]processor.StellarEffectsMessage, 0, cfg.BufferSize),
		accountsBuffer:      make([]processor.AccountRecord, 0, cfg.BufferSize),
		bufferSize:          cfg.BufferSize,
		flushInterval:       time.Duration(cfg.FlushIntervalMs) * time.Millisecond,
		maxRetries:          cfg.MaxRetries,
		retryDelay:          time.Duration(cfg.RetryDelayMs) * time.Millisecond,
		createSchema:        cfg.CreateSchema,
		dropExisting:        cfg.DropExisting,
		ctx:                 ctx,
		cancel:              cancel,
		subscribers:         []processor.Processor{},
	}

	// Create schema if configured
	if cfg.CreateSchema {
		log.Infof("WalletBackendPostgreSQL: Creating database schema")
		if err := consumer.createTables(); err != nil {
			return nil, fmt.Errorf("failed to create schema: %w", err)
		}
		log.Infof("WalletBackendPostgreSQL: Database schema created successfully")
	}

	// Start flush timer
	consumer.startFlushTimer()

	log.Infof("WalletBackendPostgreSQL consumer created successfully with buffer_size=%d, flush_interval=%v", 
		cfg.BufferSize, consumer.flushInterval)

	return consumer, nil
}

// Subscribe adds a subscriber to receive processed messages
func (c *WalletBackendPostgreSQL) Subscribe(p processor.Processor) {
	c.subscribers = append(c.subscribers, p)
}

// Process handles incoming messages from various processors
func (c *WalletBackendPostgreSQL) Process(ctx context.Context, msg processor.Message) error {
	log.Infof("WalletBackendPostgreSQL: Process called")
	
	// Add timeout to prevent hanging
	processCtx, cancel := context.WithTimeout(ctx, 60*time.Second)
	defer cancel()
	
	// First, add data to buffers while holding the lock
	c.bufferMutex.Lock()
	log.Infof("WalletBackendPostgreSQL: Processing message with metadata keys: %d", len(msg.Metadata))

	// Check if this is a pass-through message with metadata
	if msg.Metadata != nil {
		// Extract XDR outputs
		if xdrOutputs, ok := msg.Metadata["xdr_outputs"]; ok {
			if outputs, ok := xdrOutputs.([]processor.TransactionXDROutput); ok {
				for _, output := range outputs {
					c.transactionBuffer = append(c.transactionBuffer, output)
				}
			}
		}

		// Extract state changes
		if stateChanges, ok := msg.Metadata["state_changes"]; ok {
			if outputs, ok := stateChanges.([]processor.WalletBackendOutput); ok {
				for _, output := range outputs {
					c.stateChangesBuffer = append(c.stateChangesBuffer, output)
				}
			}
		}

		// Extract participants
		if participants, ok := msg.Metadata["participants"]; ok {
			if outputs, ok := participants.([]processor.ParticipantOutput); ok {
				for _, output := range outputs {
					c.participantsBuffer = append(c.participantsBuffer, output)
				}
			}
		}

		// Extract effects
		if effects, ok := msg.Metadata["effects"]; ok {
			if outputs, ok := effects.([]processor.StellarEffectsMessage); ok {
				for _, output := range outputs {
					c.effectsBuffer = append(c.effectsBuffer, output)
				}
			}
		}

		// Extract accounts
		if accounts, ok := msg.Metadata["accounts"]; ok {
			if outputs, ok := accounts.([]processor.AccountRecord); ok {
				for _, output := range outputs {
					c.accountsBuffer = append(c.accountsBuffer, output)
				}
			}
		}
	} else {
		// Fallback to direct payload processing for backward compatibility
		switch payload := msg.Payload.(type) {
		case *processor.TransactionXDROutput:
			c.transactionBuffer = append(c.transactionBuffer, *payload)
		case *processor.WalletBackendOutput:
			c.stateChangesBuffer = append(c.stateChangesBuffer, *payload)
		case *processor.ParticipantOutput:
			c.participantsBuffer = append(c.participantsBuffer, *payload)
		case *processor.StellarEffectsMessage:
			c.effectsBuffer = append(c.effectsBuffer, *payload)
		case *processor.AccountRecord:
			c.accountsBuffer = append(c.accountsBuffer, *payload)
		default:
			log.Debugf("WalletBackendPostgreSQL received unsupported message type: %T", payload)
			c.bufferMutex.Unlock()
			return nil
		}
	}

	// Log buffer sizes
	totalBuffered := len(c.transactionBuffer) + len(c.stateChangesBuffer) + 
		len(c.participantsBuffer) + len(c.effectsBuffer) + len(c.accountsBuffer)
	log.Infof("WalletBackendPostgreSQL: Buffer sizes - tx: %d, changes: %d, participants: %d, effects: %d, accounts: %d (total: %d, buffer_size: %d)",
		len(c.transactionBuffer), len(c.stateChangesBuffer), len(c.participantsBuffer), 
		len(c.effectsBuffer), len(c.accountsBuffer), totalBuffered, c.bufferSize)

	// Release the lock before calling flush to avoid deadlock
	c.bufferMutex.Unlock()

	// Always flush after processing a ledger to avoid buffer overflow
	// This ensures we don't accumulate too much data and cause deadlocks
	if totalBuffered > 0 {
		log.Infof("WalletBackendPostgreSQL: Flushing after ledger (items: %d)", totalBuffered)
		return c.flush(processCtx)
	}

	return nil
}

// shouldFlush determines if the buffer should be flushed
func (c *WalletBackendPostgreSQL) shouldFlush() bool {
	totalSize := len(c.transactionBuffer) + len(c.stateChangesBuffer) + 
		len(c.participantsBuffer) + len(c.effectsBuffer) + len(c.accountsBuffer)
	return totalSize >= c.bufferSize
}

// flush writes all buffered data to the database atomically
func (c *WalletBackendPostgreSQL) flush(ctx context.Context) error {
	log.Infof("WalletBackendPostgreSQL: Starting flush")
	
	// Copy buffers to avoid holding lock during database operations
	var txBuffer []processor.TransactionXDROutput
	var scBuffer []processor.WalletBackendOutput
	var pBuffer []processor.ParticipantOutput
	var eBuffer []processor.StellarEffectsMessage
	var aBuffer []processor.AccountRecord

	c.bufferMutex.Lock()
	log.Infof("WalletBackendPostgreSQL: Acquired buffer mutex for flush")
	if len(c.transactionBuffer) > 0 {
		txBuffer = make([]processor.TransactionXDROutput, len(c.transactionBuffer))
		copy(txBuffer, c.transactionBuffer)
		c.transactionBuffer = c.transactionBuffer[:0]
	}
	if len(c.stateChangesBuffer) > 0 {
		scBuffer = make([]processor.WalletBackendOutput, len(c.stateChangesBuffer))
		copy(scBuffer, c.stateChangesBuffer)
		c.stateChangesBuffer = c.stateChangesBuffer[:0]
	}
	if len(c.participantsBuffer) > 0 {
		pBuffer = make([]processor.ParticipantOutput, len(c.participantsBuffer))
		copy(pBuffer, c.participantsBuffer)
		c.participantsBuffer = c.participantsBuffer[:0]
	}
	if len(c.effectsBuffer) > 0 {
		eBuffer = make([]processor.StellarEffectsMessage, len(c.effectsBuffer))
		copy(eBuffer, c.effectsBuffer)
		c.effectsBuffer = c.effectsBuffer[:0]
	}
	if len(c.accountsBuffer) > 0 {
		aBuffer = make([]processor.AccountRecord, len(c.accountsBuffer))
		copy(aBuffer, c.accountsBuffer)
		c.accountsBuffer = c.accountsBuffer[:0]
	}
	c.bufferMutex.Unlock()

	// Nothing to flush
	if len(txBuffer) == 0 && len(scBuffer) == 0 && len(pBuffer) == 0 && 
		len(eBuffer) == 0 && len(aBuffer) == 0 {
		return nil
	}

	log.Infof("WalletBackendPostgreSQL: Starting database write - tx:%d sc:%d p:%d e:%d a:%d",
		len(txBuffer), len(scBuffer), len(pBuffer), len(eBuffer), len(aBuffer))
	
	// Perform database operations with retry
	var err error
	for attempt := 0; attempt <= c.maxRetries; attempt++ {
		log.Infof("WalletBackendPostgreSQL: Write attempt %d", attempt+1)
		err = c.writeBatch(ctx, txBuffer, scBuffer, pBuffer, eBuffer, aBuffer)
		if err == nil {
			log.Infof("WalletBackendPostgreSQL: Write successful")
			break
		}
		if attempt < c.maxRetries {
			log.Warnf("Flush attempt %d failed, retrying: %v", attempt+1, err)
			time.Sleep(c.retryDelay)
		}
	}

	if err != nil {
		return fmt.Errorf("failed to flush after %d attempts: %w", c.maxRetries+1, err)
	}

	log.Infof("WalletBackendPostgreSQL successfully flushed %d transactions, %d state changes, %d participants, %d effects, %d accounts",
		len(txBuffer), len(scBuffer), len(pBuffer), len(eBuffer), len(aBuffer))

	return nil
}

// writeBatch writes all data in a single database transaction
func (c *WalletBackendPostgreSQL) writeBatch(ctx context.Context, 
	txBuffer []processor.TransactionXDROutput,
	scBuffer []processor.WalletBackendOutput,
	pBuffer []processor.ParticipantOutput,
	eBuffer []processor.StellarEffectsMessage,
	aBuffer []processor.AccountRecord) error {

	tx, err := c.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("failed to begin transaction: %w", err)
	}
	defer tx.Rollback()

	// Transaction ID mapping for foreign keys
	txIDMap := make(map[string]int64)

	// 1. Insert transactions and operations
	for _, txData := range txBuffer {
		var txID int64
		err := tx.QueryRow(`
			INSERT INTO transactions (
				hash, ledger_sequence, transaction_index, source_account,
				created_at, fee_charged, successful, operation_count,
				envelope_xdr, result_xdr, result_meta_xdr
			) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11)
			ON CONFLICT (hash) DO UPDATE SET ledger_sequence = EXCLUDED.ledger_sequence
			RETURNING id`,
			txData.TransactionHash, txData.LedgerSequence, txData.TransactionIndex,
			txData.SourceAccount, txData.Timestamp, txData.FeeCharged,
			txData.Success, txData.OperationCount,
			txData.EnvelopeXDR, txData.ResultXDR, txData.ResultMetaXDR,
		).Scan(&txID)
		if err != nil {
			return fmt.Errorf("failed to insert transaction: %w", err)
		}

		txIDMap[txData.TransactionHash] = txID

		// Insert operations
		for _, op := range txData.Operations {
			_, err = tx.Exec(`
				INSERT INTO operations (
					transaction_id, transaction_hash, operation_index,
					type, source_account, operation_xdr
				) VALUES ($1, $2, $3, $4, $5, $6)`,
				txID, txData.TransactionHash, op.Index,
				op.Type, op.SourceAccount, op.OperationXDR,
			)
			if err != nil {
				return fmt.Errorf("failed to insert operation: %w", err)
			}
		}
	}

	// 2. Insert state changes
	for _, output := range scBuffer {
		for _, sc := range output.Changes {
			txID, ok := txIDMap[sc.TransactionHash]
			if !ok {
				// Try to fetch from database
				err := tx.QueryRow("SELECT id FROM transactions WHERE hash = $1", sc.TransactionHash).Scan(&txID)
				if err != nil {
					log.Warnf("Transaction not found for state change: %s", sc.TransactionHash)
					continue
				}
			}

			// Extract asset details
			var assetCode, assetIssuer, assetType string
			if sc.Asset != nil {
				assetCode = sc.Asset.Code
				assetIssuer = sc.Asset.Issuer
				assetType = sc.Asset.Type
			}

			// Convert pointer strings to values
			var amount, balanceBefore, balanceAfter string
			if sc.Amount != nil {
				amount = *sc.Amount
			}
			if sc.BalanceBefore != nil {
				balanceBefore = *sc.BalanceBefore
			}
			if sc.BalanceAfter != nil {
				balanceAfter = *sc.BalanceAfter
			}

			metadata, _ := json.Marshal(sc.Metadata)
			timestamp := time.Unix(sc.Timestamp, 0).UTC()
			
			_, err = tx.Exec(`
				INSERT INTO state_changes (
					transaction_id, operation_index, application_order,
					change_type, account, timestamp,
					asset_code, asset_issuer, asset_type,
					amount, balance_before, balance_after, metadata
				) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13)`,
				txID, sc.OperationIndex, sc.ApplicationOrder,
				cleanString(string(sc.Type)), cleanString(sc.Account), timestamp,
				cleanString(assetCode), cleanString(assetIssuer), cleanString(assetType),
				cleanString(amount), cleanString(balanceBefore), cleanString(balanceAfter), cleanString(string(metadata)),
			)
			if err != nil {
				return fmt.Errorf("failed to insert state change: %w", err)
			}
		}
	}

	// 3. Insert participants
	for _, p := range pBuffer {
		txID, ok := txIDMap[p.TransactionHash]
		if !ok {
			err := tx.QueryRow("SELECT id FROM transactions WHERE hash = $1", p.TransactionHash).Scan(&txID)
			if err != nil {
				log.Warnf("Transaction not found for participant: %s", p.TransactionHash)
				continue
			}
		}

		// Insert each participant with their role
		for _, account := range p.SourceAccounts {
			_, err = tx.Exec(`
				INSERT INTO transaction_participants (transaction_id, transaction_hash, account, role)
				VALUES ($1, $2, $3, $4)
				ON CONFLICT (transaction_hash, account, role) DO NOTHING`,
				txID, p.TransactionHash, account, "source",
			)
			if err != nil {
				return fmt.Errorf("failed to insert participant: %w", err)
			}
		}

		for _, account := range p.DestinationAccounts {
			_, err = tx.Exec(`
				INSERT INTO transaction_participants (transaction_id, transaction_hash, account, role)
				VALUES ($1, $2, $3, $4)
				ON CONFLICT (transaction_hash, account, role) DO NOTHING`,
				txID, p.TransactionHash, account, "destination",
			)
			if err != nil {
				return fmt.Errorf("failed to insert participant: %w", err)
			}
		}

		for _, account := range p.SignerAccounts {
			_, err = tx.Exec(`
				INSERT INTO transaction_participants (transaction_id, transaction_hash, account, role)
				VALUES ($1, $2, $3, $4)
				ON CONFLICT (transaction_hash, account, role) DO NOTHING`,
				txID, p.TransactionHash, account, "signer",
			)
			if err != nil {
				return fmt.Errorf("failed to insert participant: %w", err)
			}
		}
	}

	// 4. Insert effects
	for _, effectMsg := range eBuffer {
		var txID int64
		err := tx.QueryRow("SELECT id FROM transactions WHERE hash = $1", effectMsg.TransactionHash).Scan(&txID)
		if err != nil {
			log.Warnf("Transaction not found for effects: %s", effectMsg.TransactionHash)
			continue
		}

		for effectIndex, effect := range effectMsg.Effects {
			// Convert effect to JSON for details
			details, _ := json.Marshal(effect)
			
			// Extract basic effect info - we'll store the full effect as JSON
			_, err = tx.Exec(`
				INSERT INTO effects (
					transaction_id, operation_index, effect_index,
					type, type_i, account, created_at, details
				) VALUES ($1, $2, $3, $4, $5, $6, $7, $8)`,
				txID, 0, effectIndex, // We don't have operation_index in the effect output
				"stellar_effect", 0, "", // Type and account would need extraction from effect
				effectMsg.Timestamp, details,
			)
			if err != nil {
				return fmt.Errorf("failed to insert effect: %w", err)
			}
		}
	}

	// 5. Update accounts (current state)
	for _, account := range aBuffer {
		// Create a data object with additional fields
		data := map[string]interface{}{
			"buying_liabilities": account.BuyingLiabilities,
			"selling_liabilities": account.SellingLiabilities,
			"inflation_dest": account.InflationDest,
			"sponsor": account.Sponsor,
			"num_sponsored": account.NumSponsored,
			"num_sponsoring": account.NumSponsoring,
			"deleted": account.Deleted,
		}
		dataJSON, _ := json.Marshal(data)
		
		// Convert balance from string to int64 (stroops)
		var balance int64
		fmt.Sscanf(account.Balance, "%d", &balance)
		
		_, err = tx.Exec(`
			INSERT INTO accounts (
				id, last_modified_ledger, last_modified_time,
				balance, sequence_number, num_subentries, flags,
				home_domain, master_weight,
				threshold_low, threshold_medium, threshold_high, data
			) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13)
			ON CONFLICT (id) DO UPDATE SET
				last_modified_ledger = EXCLUDED.last_modified_ledger,
				last_modified_time = EXCLUDED.last_modified_time,
				balance = EXCLUDED.balance,
				sequence_number = EXCLUDED.sequence_number,
				num_subentries = EXCLUDED.num_subentries,
				flags = EXCLUDED.flags,
				home_domain = EXCLUDED.home_domain,
				master_weight = EXCLUDED.master_weight,
				threshold_low = EXCLUDED.threshold_low,
				threshold_medium = EXCLUDED.threshold_medium,
				threshold_high = EXCLUDED.threshold_high,
				data = EXCLUDED.data`,
			cleanString(account.AccountID), account.LastModifiedLedger, account.ClosedAt,
			balance, account.Sequence, account.NumSubentries,
			account.Flags, cleanString(account.HomeDomain), account.MasterWeight,
			account.LowThreshold, account.MediumThreshold, account.HighThreshold,
			dataJSON,
		)
		if err != nil {
			return fmt.Errorf("failed to update account: %w", err)
		}
	}

	// Commit transaction
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("failed to commit transaction: %w", err)
	}

	return nil
}

// startFlushTimer starts the periodic flush timer
func (c *WalletBackendPostgreSQL) startFlushTimer() {
	c.wg.Add(1)
	go func() {
		defer c.wg.Done()
		ticker := time.NewTicker(c.flushInterval)
		defer ticker.Stop()

		for {
			select {
			case <-ticker.C:
				if err := c.flush(c.ctx); err != nil {
					log.Errorf("Error during periodic flush: %v", err)
				}
			case <-c.ctx.Done():
				return
			}
		}
	}()
}

// Close gracefully shuts down the consumer
func (c *WalletBackendPostgreSQL) Close() error {
	c.cancel()
	
	// Final flush
	if err := c.flush(context.Background()); err != nil {
		log.Errorf("Error during final flush: %v", err)
	}

	c.wg.Wait()
	return c.db.Close()
}

// cleanString removes null bytes and other invalid UTF-8 sequences
func cleanString(s string) string {
	// Remove null bytes
	cleaned := strings.ReplaceAll(s, "\x00", "")
	// Ensure valid UTF-8
	return strings.ToValidUTF8(cleaned, "")
}

// createTables creates the database schema matching wallet-backend
func (c *WalletBackendPostgreSQL) createTables() error {
	if c.dropExisting {
		dropQueries := []string{
			"DROP TABLE IF EXISTS effects CASCADE",
			"DROP TABLE IF EXISTS transaction_participants CASCADE",
			"DROP TABLE IF EXISTS state_changes CASCADE",
			"DROP TABLE IF EXISTS operations CASCADE",
			"DROP TABLE IF EXISTS transactions CASCADE",
			"DROP TABLE IF EXISTS accounts CASCADE",
		}
		for _, query := range dropQueries {
			if _, err := c.db.Exec(query); err != nil {
				return fmt.Errorf("failed to drop table: %w", err)
			}
		}
	}

	createQueries := []string{
		// 1. Transactions table
		`CREATE TABLE IF NOT EXISTS transactions (
			id BIGSERIAL PRIMARY KEY,
			hash VARCHAR(64) UNIQUE NOT NULL,
			ledger_sequence BIGINT NOT NULL,
			transaction_index INT NOT NULL,
			source_account VARCHAR(56) NOT NULL,
			created_at TIMESTAMPTZ NOT NULL,
			fee_charged BIGINT NOT NULL,
			successful BOOLEAN NOT NULL,
			operation_count INT NOT NULL,
			envelope_xdr TEXT NOT NULL,
			result_xdr TEXT NOT NULL,
			result_meta_xdr TEXT NOT NULL,
			CONSTRAINT idx_unique_ledger_tx UNIQUE (ledger_sequence, transaction_index)
		)`,
		`CREATE INDEX IF NOT EXISTS idx_ledger_seq ON transactions(ledger_sequence)`,
		`CREATE INDEX IF NOT EXISTS idx_source_account ON transactions(source_account)`,
		`CREATE INDEX IF NOT EXISTS idx_created_at ON transactions(created_at)`,

		// 2. Operations table
		`CREATE TABLE IF NOT EXISTS operations (
			id BIGSERIAL PRIMARY KEY,
			transaction_id BIGINT REFERENCES transactions(id),
			transaction_hash VARCHAR(64) NOT NULL,
			operation_index INT NOT NULL,
			type VARCHAR(50) NOT NULL,
			source_account VARCHAR(56) NOT NULL,
			operation_xdr TEXT NOT NULL,
			details JSONB,
			CONSTRAINT idx_unique_tx_op UNIQUE (transaction_hash, operation_index)
		)`,
		`CREATE INDEX IF NOT EXISTS idx_op_type ON operations(type)`,
		`CREATE INDEX IF NOT EXISTS idx_op_source ON operations(source_account)`,

		// 3. State changes table
		`CREATE TABLE IF NOT EXISTS state_changes (
			id BIGSERIAL PRIMARY KEY,
			transaction_id BIGINT REFERENCES transactions(id),
			operation_index INT NOT NULL,
			application_order INT NOT NULL,
			change_type VARCHAR(50) NOT NULL,
			account VARCHAR(56) NOT NULL,
			timestamp TIMESTAMPTZ NOT NULL,
			asset_code VARCHAR(12),
			asset_issuer VARCHAR(56),
			asset_type VARCHAR(20),
			amount VARCHAR(40),
			balance_before VARCHAR(40),
			balance_after VARCHAR(40),
			metadata JSONB,
			created_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP
		)`,
		`CREATE INDEX IF NOT EXISTS idx_sc_account ON state_changes(account)`,
		`CREATE INDEX IF NOT EXISTS idx_sc_change_type ON state_changes(change_type)`,
		`CREATE INDEX IF NOT EXISTS idx_sc_timestamp ON state_changes(timestamp)`,

		// 4. Transaction participants
		`CREATE TABLE IF NOT EXISTS transaction_participants (
			id BIGSERIAL PRIMARY KEY,
			transaction_id BIGINT REFERENCES transactions(id),
			transaction_hash VARCHAR(64) NOT NULL,
			account VARCHAR(56) NOT NULL,
			role VARCHAR(20) NOT NULL,
			CONSTRAINT idx_unique_tx_participant_role UNIQUE (transaction_hash, account, role)
		)`,
		`CREATE INDEX IF NOT EXISTS idx_participant ON transaction_participants(account)`,

		// 5. Effects table
		`CREATE TABLE IF NOT EXISTS effects (
			id BIGSERIAL PRIMARY KEY,
			transaction_id BIGINT REFERENCES transactions(id),
			operation_index INT NOT NULL,
			effect_index INT NOT NULL,
			type VARCHAR(50) NOT NULL,
			type_i INT NOT NULL,
			account VARCHAR(56) NOT NULL,
			created_at TIMESTAMPTZ NOT NULL,
			details JSONB NOT NULL
		)`,
		`CREATE INDEX IF NOT EXISTS idx_effect_account ON effects(account)`,
		`CREATE INDEX IF NOT EXISTS idx_effect_type ON effects(type)`,
		`CREATE INDEX IF NOT EXISTS idx_effect_created ON effects(created_at)`,

		// 6. Accounts table
		`CREATE TABLE IF NOT EXISTS accounts (
			id VARCHAR(56) PRIMARY KEY,
			last_modified_ledger BIGINT NOT NULL,
			last_modified_time TIMESTAMPTZ NOT NULL,
			balance BIGINT NOT NULL,
			sequence_number BIGINT NOT NULL,
			num_subentries INT NOT NULL,
			flags INT NOT NULL,
			home_domain VARCHAR(255),
			master_weight INT NOT NULL,
			threshold_low INT NOT NULL,
			threshold_medium INT NOT NULL,
			threshold_high INT NOT NULL,
			data JSONB
		)`,
		`CREATE INDEX IF NOT EXISTS idx_last_modified ON accounts(last_modified_ledger)`,
	}

	for _, query := range createQueries {
		if _, err := c.db.Exec(query); err != nil {
			return fmt.Errorf("failed to create table/index: %w", err)
		}
	}

	log.Info("WalletBackendPostgreSQL: Schema created successfully")
	return nil
}