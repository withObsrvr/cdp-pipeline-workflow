package main

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	cdpProcessor "github.com/withObsrvr/cdp-pipeline-workflow/processor"
	"github.com/withobsrvr/flowctl/pkg/console/heartbeat"
	_ "github.com/marcboeker/go-duckdb/v2"
)

// DuckLakeSourceConfig holds configuration for DuckLakeSourceAdapter
type DuckLakeSourceConfig struct {
	// Required: DuckLake catalog connection
	CatalogPath string `yaml:"catalog_path"` // e.g., "ducklake:postgres:postgresql:///catalog_name?host=/path/to/socket"
	DataPath    string `yaml:"data_path"`    // e.g., "/absolute/path/to/parquet/files"
	Schema      string `yaml:"schema"`       // e.g., "testnet" or "mainnet"
	Query       string `yaml:"query"`        // SQL query to execute

	// Optional: Batch and streaming control
	BatchSize           int    `yaml:"batch_size"`             // Number of rows to fetch at once (0 = no batching)
	LogProgressInterval int    `yaml:"log_progress_interval"`  // Log every N rows (default 1000)
	TimeoutSeconds      int    `yaml:"timeout_seconds"`        // Query timeout (0 = no timeout)
	PollInterval        int    `yaml:"poll_interval"`          // Seconds to wait between polls in streaming mode (default 5)
	EndLedger           uint32 `yaml:"end_ledger"`             // If 0, run in streaming mode (continuous)

	// Optional: Checkpoint for streaming mode
	CheckpointEnabled  bool   `yaml:"checkpoint_enabled"`  // Enable checkpoint persistence
	CheckpointDir      string `yaml:"checkpoint_dir"`      // Directory for checkpoint file
	CheckpointFilename string `yaml:"checkpoint_filename"` // Checkpoint file name
}

// DuckLakeCheckpoint tracks processing progress
type DuckLakeCheckpoint struct {
	LastProcessedLedger uint32    `json:"last_processed_ledger"`
	LastProcessedTime   time.Time `json:"last_processed_time"`
	TotalRowsProcessed  uint64    `json:"total_rows_processed"`
}

// DuckLakeSourceAdapter queries Bronze DuckLake tables via SQL
type DuckLakeSourceAdapter struct {
	config          DuckLakeSourceConfig
	subscribers     []chan cdpProcessor.Message
	mu              sync.RWMutex
	db              *sql.DB
	checkpoint      DuckLakeCheckpoint
	heartbeatClient *heartbeat.Client // Console heartbeat for billing tracking
}

// NewDuckLakeSourceAdapter creates a new DuckLake source adapter from a map
func NewDuckLakeSourceAdapter(configMap map[string]interface{}) (SourceAdapter, error) {
	config := DuckLakeSourceConfig{
		CatalogPath:         getStringConfigDuckLake(configMap, "catalog_path", ""),
		DataPath:            getStringConfigDuckLake(configMap, "data_path", ""),
		Schema:              getStringConfigDuckLake(configMap, "schema", ""),
		Query:               getStringConfigDuckLake(configMap, "query", ""),
		BatchSize:           getIntConfigDuckLake(configMap, "batch_size", 0),
		LogProgressInterval: getIntConfigDuckLake(configMap, "log_progress_interval", 1000),
		TimeoutSeconds:      getIntConfigDuckLake(configMap, "timeout_seconds", 0),
		PollInterval:        getIntConfigDuckLake(configMap, "poll_interval", 5),
		EndLedger:           uint32(getIntConfigDuckLake(configMap, "end_ledger", 0)),
		CheckpointEnabled:   getBoolConfigDuckLake(configMap, "checkpoint_enabled", false),
		CheckpointDir:       getStringConfigDuckLake(configMap, "checkpoint_dir", "./checkpoints"),
		CheckpointFilename:  getStringConfigDuckLake(configMap, "checkpoint_filename", "ducklake_checkpoint.json"),
	}

	return newDuckLakeSourceAdapterWithConfig(config)
}

// newDuckLakeSourceAdapterWithConfig creates a new DuckLake source adapter with validated config
func newDuckLakeSourceAdapterWithConfig(config DuckLakeSourceConfig) (*DuckLakeSourceAdapter, error) {
	// Validate required fields
	if config.CatalogPath == "" {
		return nil, fmt.Errorf("catalog_path is required")
	}
	if config.DataPath == "" {
		return nil, fmt.Errorf("data_path is required")
	}
	if config.Schema == "" {
		return nil, fmt.Errorf("schema is required")
	}
	if config.Query == "" {
		return nil, fmt.Errorf("query is required")
	}

	// Set defaults
	if config.LogProgressInterval == 0 {
		config.LogProgressInterval = 1000
	}
	if config.PollInterval == 0 {
		config.PollInterval = 5
	}
	if config.CheckpointDir == "" {
		config.CheckpointDir = "./checkpoints"
	}
	if config.CheckpointFilename == "" {
		config.CheckpointFilename = "ducklake_checkpoint.json"
	}

	adapter := &DuckLakeSourceAdapter{
		config:      config,
		subscribers: make([]chan cdpProcessor.Message, 0),
	}

	// Initialize console heartbeat client
	adapter.heartbeatClient = initializeHeartbeatClient()

	return adapter, nil
}

// Subscribe allows processors to receive messages from this adapter
func (a *DuckLakeSourceAdapter) Subscribe(p cdpProcessor.Processor) {
	ch := make(chan cdpProcessor.Message, 1000)
	a.mu.Lock()
	a.subscribers = append(a.subscribers, ch)
	a.mu.Unlock()

	go func() {
		for msg := range ch {
			if err := p.Process(context.Background(), msg); err != nil {
				log.Printf("DuckLakeSourceAdapter: Error processing message: %v", err)
			}
		}
	}()
}

// emit sends a message to all subscribers
func (a *DuckLakeSourceAdapter) emit(msg cdpProcessor.Message) {
	a.mu.RLock()
	defer a.mu.RUnlock()
	for _, ch := range a.subscribers {
		select {
		case ch <- msg:
		default:
			log.Printf("DuckLakeSourceAdapter: Warning: subscriber channel full, dropping message")
		}
	}
}

// Run executes the SQL query and streams results to subscribers
func (a *DuckLakeSourceAdapter) Run(ctx context.Context) error {
	// Load checkpoint if enabled
	if a.config.CheckpointEnabled {
		if err := a.loadCheckpoint(); err != nil {
			log.Printf("DuckLakeSourceAdapter: Could not load checkpoint (starting fresh): %v", err)
		} else {
			log.Printf("DuckLakeSourceAdapter: Loaded checkpoint, last_processed_ledger=%d",
				a.checkpoint.LastProcessedLedger)
		}
	}

	// Determine if streaming mode (no end_ledger)
	streamingMode := a.config.EndLedger == 0

	if streamingMode {
		log.Printf("DuckLakeSourceAdapter: Starting in STREAMING mode (no end_ledger)")
		return a.runStreaming(ctx)
	} else {
		log.Printf("DuckLakeSourceAdapter: Starting in BATCH mode, end_ledger=%d", a.config.EndLedger)
		return a.runBatch(ctx)
	}
}

// runBatch executes a one-time query for a fixed ledger range
func (a *DuckLakeSourceAdapter) runBatch(ctx context.Context) error {
	// Connect to DuckDB
	if err := a.connect(ctx); err != nil {
		return fmt.Errorf("connect to DuckDB: %w", err)
	}
	defer a.close()

	// Execute query and stream results
	startTime := time.Now()
	rowsProcessed, err := a.executeQuery(ctx)
	if err != nil {
		return fmt.Errorf("execute query: %w", err)
	}

	elapsed := time.Since(startTime).Seconds()
	rowsPerSec := float64(rowsProcessed) / elapsed
	log.Printf("DuckLakeSourceAdapter: Complete, processed %d rows in %.2fs (%.0f rows/sec)",
		rowsProcessed, elapsed, rowsPerSec)

	return nil
}

// runStreaming continuously polls for new ledgers and processes them
func (a *DuckLakeSourceAdapter) runStreaming(ctx context.Context) error {
	// Connect to DuckDB (keep connection open)
	if err := a.connect(ctx); err != nil {
		return fmt.Errorf("connect to DuckDB: %w", err)
	}
	defer a.close()

	log.Printf("DuckLakeSourceAdapter: Streaming mode started, poll_interval=%ds", a.config.PollInterval)

	pollTicker := time.NewTicker(time.Duration(a.config.PollInterval) * time.Second)
	defer pollTicker.Stop()

	for {
		select {
		case <-ctx.Done():
			log.Printf("DuckLakeSourceAdapter: Context cancelled, stopping streaming")
			return ctx.Err()

		case <-pollTicker.C:
			// Query for new ledgers since checkpoint
			maxLedger, err := a.getMaxLedger(ctx)
			if err != nil {
				log.Printf("DuckLakeSourceAdapter: Error getting max ledger: %v", err)
				continue
			}

			if maxLedger <= a.checkpoint.LastProcessedLedger {
				// No new data, keep polling
				continue
			}

			log.Printf("DuckLakeSourceAdapter: New data available, max_ledger=%d, checkpoint=%d",
				maxLedger, a.checkpoint.LastProcessedLedger)

			// Process new ledgers in batches
			batchSize := a.config.BatchSize
			if batchSize == 0 {
				batchSize = 100 // Default streaming batch
			}

			startLedger := a.checkpoint.LastProcessedLedger + 1
			endLedger := startLedger + uint32(batchSize) - 1
			if endLedger > maxLedger {
				endLedger = maxLedger
			}

			rowsProcessed, err := a.executeQueryWithLedgerRange(ctx, startLedger, endLedger)
			if err != nil {
				log.Printf("DuckLakeSourceAdapter: Error processing ledgers %d-%d: %v",
					startLedger, endLedger, err)
				continue
			}

			// Update checkpoint
			a.checkpoint.LastProcessedLedger = endLedger
			a.checkpoint.LastProcessedTime = time.Now()
			a.checkpoint.TotalRowsProcessed += rowsProcessed

			// Update console heartbeat
			if a.heartbeatClient != nil {
				a.heartbeatClient.SetLedgerCount(int64(a.checkpoint.TotalRowsProcessed))
			}

			if a.config.CheckpointEnabled {
				if err := a.saveCheckpoint(); err != nil {
					log.Printf("DuckLakeSourceAdapter: Error saving checkpoint: %v", err)
				}
			}

			log.Printf("DuckLakeSourceAdapter: Processed %d rows, checkpoint=%d",
				rowsProcessed, a.checkpoint.LastProcessedLedger)
		}
	}
}

// connect opens a DuckDB connection and attaches the DuckLake catalog
func (a *DuckLakeSourceAdapter) connect(ctx context.Context) error {
	log.Printf("DuckLakeSourceAdapter: Connecting to DuckDB...")

	db, err := sql.Open("duckdb", "")
	if err != nil {
		return fmt.Errorf("open DuckDB: %w", err)
	}
	a.db = db

	// Load DuckLake extension
	log.Printf("DuckLakeSourceAdapter: Loading DuckLake extension...")
	if _, err := a.db.ExecContext(ctx, "LOAD ducklake;"); err != nil {
		return fmt.Errorf("load ducklake extension: %w", err)
	}

	// Attach catalog with the schema name as the alias for easier querying
	log.Printf("DuckLakeSourceAdapter: Attaching catalog: %s", a.config.CatalogPath)
	attachSQL := fmt.Sprintf(`
		ATTACH IF NOT EXISTS '%s' AS %s (
			DATA_PATH '%s',
			METADATA_SCHEMA '%s'
		);`,
		a.config.CatalogPath,
		a.config.Schema, // Use schema name as alias (e.g., "testnet")
		a.config.DataPath,
		a.config.Schema,
	)

	if _, err := a.db.ExecContext(ctx, attachSQL); err != nil {
		return fmt.Errorf("attach catalog: %w", err)
	}

	log.Printf("DuckLakeSourceAdapter: Connected and attached schema: %s", a.config.Schema)
	return nil
}

// close closes the DuckDB connection
func (a *DuckLakeSourceAdapter) close() {
	if a.db != nil {
		a.db.Close()
		log.Printf("DuckLakeSourceAdapter: Connection closed")
	}
}

// executeQuery runs the user's SQL query and emits rows
func (a *DuckLakeSourceAdapter) executeQuery(ctx context.Context) (uint64, error) {
	log.Printf("DuckLakeSourceAdapter: Executing query...")

	// Apply timeout if configured
	queryCtx := ctx
	if a.config.TimeoutSeconds > 0 {
		var cancel context.CancelFunc
		queryCtx, cancel = context.WithTimeout(ctx, time.Duration(a.config.TimeoutSeconds)*time.Second)
		defer cancel()
	}

	rows, err := a.db.QueryContext(queryCtx, a.config.Query)
	if err != nil {
		return 0, fmt.Errorf("query failed: %w", err)
	}
	defer rows.Close()

	return a.streamRows(ctx, rows)
}

// executeQueryWithLedgerRange augments the user query with ledger range filtering
func (a *DuckLakeSourceAdapter) executeQueryWithLedgerRange(ctx context.Context, startLedger, endLedger uint32) (uint64, error) {
	// Inject WHERE clause for ledger_sequence range
	// This assumes the query has ledger_sequence column available
	augmentedQuery := a.augmentQueryWithLedgerRange(a.config.Query, startLedger, endLedger)

	log.Printf("DuckLakeSourceAdapter: Executing query for ledgers %d-%d", startLedger, endLedger)

	queryCtx := ctx
	if a.config.TimeoutSeconds > 0 {
		var cancel context.CancelFunc
		queryCtx, cancel = context.WithTimeout(ctx, time.Duration(a.config.TimeoutSeconds)*time.Second)
		defer cancel()
	}

	rows, err := a.db.QueryContext(queryCtx, augmentedQuery)
	if err != nil {
		return 0, fmt.Errorf("query failed: %w", err)
	}
	defer rows.Close()

	return a.streamRows(ctx, rows)
}

// augmentQueryWithLedgerRange injects ledger_sequence filtering into user query
func (a *DuckLakeSourceAdapter) augmentQueryWithLedgerRange(query string, startLedger, endLedger uint32) string {
	// Simple approach: Add WHERE clause or AND clause
	// This is a basic implementation - could be improved with SQL parsing
	query = strings.TrimSpace(query)

	ledgerCondition := fmt.Sprintf("ledger_sequence >= %d AND ledger_sequence <= %d", startLedger, endLedger)

	// Check if query has WHERE clause
	queryLower := strings.ToLower(query)
	if strings.Contains(queryLower, " where ") {
		// Append to existing WHERE
		query = strings.TrimSuffix(query, ";")
		return fmt.Sprintf("%s AND %s", query, ledgerCondition)
	} else {
		// Add new WHERE clause before ORDER BY if present
		if strings.Contains(queryLower, " order by ") {
			parts := strings.Split(query, " ORDER BY ")
			return fmt.Sprintf("%s WHERE %s ORDER BY %s", parts[0], ledgerCondition, parts[1])
		}
		// No WHERE or ORDER BY, just append
		query = strings.TrimSuffix(query, ";")
		return fmt.Sprintf("%s WHERE %s", query, ledgerCondition)
	}
}

// streamRows processes query result rows and emits messages
func (a *DuckLakeSourceAdapter) streamRows(ctx context.Context, rows *sql.Rows) (uint64, error) {
	// Get column names
	columns, err := rows.Columns()
	if err != nil {
		return 0, fmt.Errorf("get columns: %w", err)
	}

	var rowsProcessed uint64
	startTime := time.Now()

	for rows.Next() {
		select {
		case <-ctx.Done():
			return rowsProcessed, ctx.Err()
		default:
		}

		// Parse row to map
		rowMap, err := a.scanRowToMap(rows, columns)
		if err != nil {
			log.Printf("DuckLakeSourceAdapter: Error scanning row %d: %v", rowsProcessed+1, err)
			continue
		}

		// Emit message
		msg := cdpProcessor.Message{
			Payload: rowMap,
		}
		a.emit(msg)

		rowsProcessed++

		// Progress logging
		if rowsProcessed%uint64(a.config.LogProgressInterval) == 0 {
			elapsed := time.Since(startTime).Seconds()
			rowsPerSec := float64(rowsProcessed) / elapsed
			log.Printf("DuckLakeSourceAdapter: Processed %d rows (%.0f rows/sec)", rowsProcessed, rowsPerSec)
		}
	}

	if err := rows.Err(); err != nil {
		return rowsProcessed, fmt.Errorf("rows iteration error: %w", err)
	}

	return rowsProcessed, nil
}

// scanRowToMap scans a SQL row into a map[string]interface{}
func (a *DuckLakeSourceAdapter) scanRowToMap(rows *sql.Rows, columns []string) (map[string]interface{}, error) {
	// Create slice of interface{} for Scan
	values := make([]interface{}, len(columns))
	valuePtrs := make([]interface{}, len(columns))
	for i := range values {
		valuePtrs[i] = &values[i]
	}

	if err := rows.Scan(valuePtrs...); err != nil {
		return nil, err
	}

	// Build map
	rowMap := make(map[string]interface{})
	for i, col := range columns {
		val := values[i]

		// Handle NULL values
		if val == nil {
			rowMap[col] = nil
			continue
		}

		// Type conversion
		switch v := val.(type) {
		case []byte:
			// Keep as byte array (for XDR columns)
			rowMap[col] = v
		case int64:
			rowMap[col] = v
		case float64:
			rowMap[col] = v
		case bool:
			rowMap[col] = v
		case string:
			rowMap[col] = v
		case time.Time:
			rowMap[col] = v
		default:
			// Fallback: convert to string
			rowMap[col] = fmt.Sprintf("%v", v)
		}
	}

	return rowMap, nil
}

// getMaxLedger queries the maximum ledger_sequence from Bronze
func (a *DuckLakeSourceAdapter) getMaxLedger(ctx context.Context) (uint32, error) {
	// Query max ledger from ledgers_row_v2 table
	query := fmt.Sprintf("SELECT MAX(ledger_sequence) FROM %s.ledgers_row_v2", a.config.Schema)

	var maxLedger sql.NullInt64
	err := a.db.QueryRowContext(ctx, query).Scan(&maxLedger)
	if err != nil {
		return 0, fmt.Errorf("query max ledger: %w", err)
	}

	if !maxLedger.Valid {
		return 0, nil
	}

	return uint32(maxLedger.Int64), nil
}

// loadCheckpoint loads the checkpoint from disk
func (a *DuckLakeSourceAdapter) loadCheckpoint() error {
	checkpointPath := filepath.Join(a.config.CheckpointDir, a.config.CheckpointFilename)

	data, err := os.ReadFile(checkpointPath)
	if err != nil {
		if os.IsNotExist(err) {
			// No checkpoint exists, start fresh
			return nil
		}
		return fmt.Errorf("read checkpoint: %w", err)
	}

	if err := json.Unmarshal(data, &a.checkpoint); err != nil {
		return fmt.Errorf("unmarshal checkpoint: %w", err)
	}

	return nil
}

// saveCheckpoint saves the checkpoint to disk atomically
func (a *DuckLakeSourceAdapter) saveCheckpoint() error {
	checkpointPath := filepath.Join(a.config.CheckpointDir, a.config.CheckpointFilename)

	// Ensure directory exists
	if err := os.MkdirAll(a.config.CheckpointDir, 0755); err != nil {
		return fmt.Errorf("create checkpoint dir: %w", err)
	}

	data, err := json.MarshalIndent(a.checkpoint, "", "  ")
	if err != nil {
		return fmt.Errorf("marshal checkpoint: %w", err)
	}

	// Atomic write: write to temp file, then rename
	tempPath := checkpointPath + ".tmp"
	if err := os.WriteFile(tempPath, data, 0644); err != nil {
		return fmt.Errorf("write temp checkpoint: %w", err)
	}

	if err := os.Rename(tempPath, checkpointPath); err != nil {
		return fmt.Errorf("rename checkpoint: %w", err)
	}

	return nil
}

// Close closes any open resources
func (a *DuckLakeSourceAdapter) Close() error {
	a.close()

	a.mu.Lock()
	defer a.mu.Unlock()
	for _, ch := range a.subscribers {
		close(ch)
	}

	return nil
}

// Config helper functions

func getStringConfigDuckLake(config map[string]interface{}, key string, defaultValue string) string {
	if val, ok := config[key]; ok {
		if str, ok := val.(string); ok {
			return str
		}
	}
	return defaultValue
}

func getIntConfigDuckLake(config map[string]interface{}, key string, defaultValue int) int {
	if val, ok := config[key]; ok {
		switch v := val.(type) {
		case int:
			return v
		case int64:
			return int(v)
		case float64:
			return int(v)
		}
	}
	return defaultValue
}

func getBoolConfigDuckLake(config map[string]interface{}, key string, defaultValue bool) bool {
	if val, ok := config[key]; ok {
		if b, ok := val.(bool); ok {
			return b
		}
	}
	return defaultValue
}
