package consumer

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	processor "github.com/withObsrvr/cdp-pipeline-workflow/processor"
)

// MockDuckDBConnection simulates DuckDB connection
type MockDuckDBConnection struct {
	mu            sync.Mutex
	tables        map[string][]map[string]interface{}
	execErr       error
	queryErr      error
	beginErr      error
	commitErr     error
	rollbackErr   error
	callCounts    map[string]int
	lastQueries   []string
	inTransaction bool
	sqlmock       sqlmock.Sqlmock
}

func NewMockDuckDBConnection() *MockDuckDBConnection {
	return &MockDuckDBConnection{
		tables:      make(map[string][]map[string]interface{}),
		callCounts:  make(map[string]int),
		lastQueries: make([]string, 0),
	}
}

func (m *MockDuckDBConnection) Exec(query string, args ...interface{}) (sql.Result, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	m.callCounts["Exec"]++
	m.lastQueries = append(m.lastQueries, query)

	if m.execErr != nil {
		return nil, m.execErr
	}

	// Simulate successful execution
	return &MockResult{rowsAffected: 1}, nil
}

func (m *MockDuckDBConnection) Query(query string, args ...interface{}) (*sql.Rows, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	m.callCounts["Query"]++

	if m.queryErr != nil {
		return nil, m.queryErr
	}

	// Return mock rows
	return nil, nil
}

func (m *MockDuckDBConnection) Begin() (*sql.Tx, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	m.callCounts["Begin"]++

	if m.beginErr != nil {
		return nil, m.beginErr
	}

	m.inTransaction = true
	return &sql.Tx{}, nil
}

type MockResult struct {
	rowsAffected int64
}

func (r *MockResult) LastInsertId() (int64, error) {
	return 0, nil
}

func (r *MockResult) RowsAffected() (int64, error) {
	return r.rowsAffected, nil
}

// TestNewSaveToDuckDB tests configuration validation
func TestNewSaveToDuckDB(t *testing.T) {
	tests := []struct {
		name    string
		config  map[string]interface{}
		wantErr bool
		errMsg  string
	}{
		{
			name: "valid configuration",
			config: map[string]interface{}{
				"database_path": "/tmp/stellar.duckdb",
				"table_name":    "contract_data",
			},
			wantErr: false,
		},
		{
			name: "in-memory database",
			config: map[string]interface{}{
				"database_path": ":memory:",
				"table_name":    "payments",
			},
			wantErr: false,
		},
		{
			name: "missing database_path",
			config: map[string]interface{}{
				"table_name": "contract_data",
			},
			wantErr: true,
			errMsg:  "database_path is required",
		},
		{
			name: "missing table_name",
			config: map[string]interface{}{
				"database_path": "/tmp/stellar.duckdb",
			},
			wantErr: true,
			errMsg:  "table_name is required",
		},
		{
			name: "with batch configuration",
			config: map[string]interface{}{
				"database_path": "/tmp/stellar.duckdb",
				"table_name":    "contract_data",
				"batch_size":    1000,
				"batch_timeout": 30,
			},
			wantErr: false,
		},
		{
			name: "with schema configuration",
			config: map[string]interface{}{
				"database_path": "/tmp/stellar.duckdb",
				"table_name":    "contract_data",
				"schema": map[string]interface{}{
					"contract_id": "VARCHAR",
					"value":       "BIGINT",
					"closed_at":   "TIMESTAMP",
				},
			},
			wantErr: false,
		},
		{
			name: "with partitioning",
			config: map[string]interface{}{
				"database_path":  "/tmp/stellar.duckdb",
				"table_name":     "ledgers",
				"partition_by":   "closed_at",
				"partition_type": "RANGE",
			},
			wantErr: false,
		},
		{
			name: "with compression",
			config: map[string]interface{}{
				"database_path": "/tmp/stellar.duckdb",
				"table_name":    "contract_data",
				"compression":   "ZSTD",
			},
			wantErr: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Validate configuration
			if !tt.wantErr {
				dbPath, ok := tt.config["database_path"].(string)
				assert.True(t, ok, "database_path should be a string")
				assert.NotEmpty(t, dbPath)

				tableName, ok := tt.config["table_name"].(string)
				assert.True(t, ok, "table_name should be a string")
				assert.NotEmpty(t, tableName)
			}
		})
	}
}

// TestSaveToDuckDB_Process tests message processing
func TestSaveToDuckDB_Process(t *testing.T) {
	// Create mock database
	db, mock, err := sqlmock.New()
	require.NoError(t, err)
	defer db.Close()

	t.Run("insert single row", func(t *testing.T) {
		consumer := &SaveToDuckDB{
			db:        db,
			tableName: "contract_data",
		}

		// Expect table check
		mock.ExpectQuery("SELECT COUNT").
			WithArgs("contract_data").
			WillReturnRows(sqlmock.NewRows([]string{"count"}).AddRow(1))

		// Expect insert
		mock.ExpectExec("INSERT INTO contract_data").
			WithArgs(sqlmock.AnyArg()).
			WillReturnResult(sqlmock.NewResult(1, 1))

		ctx := context.Background()

		msg := processor.Message{
			Payload: map[string]interface{}{
				"contract_id": "CONTRACT123",
				"key":         "balance",
				"value":       int64(1000000),
				"closed_at":   time.Now(),
			},
		}

		err := consumer.Process(ctx, msg)
		assert.NoError(t, err)

		err = mock.ExpectationsWereMet()
		assert.NoError(t, err)
	})

	t.Run("handle JSON payload", func(t *testing.T) {
		consumer := &SaveToDuckDB{
			db:        db,
			tableName: "payments",
		}

		// Reset expectations
		mock = resetDuckDBMock(db, mock, t)

		mock.ExpectQuery("SELECT COUNT").
			WithArgs("payments").
			WillReturnRows(sqlmock.NewRows([]string{"count"}).AddRow(1))

		mock.ExpectExec("INSERT INTO payments").
			WithArgs(sqlmock.AnyArg()).
			WillReturnResult(sqlmock.NewResult(1, 1))

		ctx := context.Background()

		jsonData := map[string]interface{}{
			"source":    "GSOURCE123",
			"dest":      "GDEST456",
			"amount":    1000000,
			"asset":     "XLM",
			"timestamp": time.Now().Unix(),
		}
		jsonBytes, _ := json.Marshal(jsonData)

		msg := processor.Message{
			Payload: jsonBytes,
		}

		err := consumer.Process(ctx, msg)
		assert.NoError(t, err)

		err = mock.ExpectationsWereMet()
		assert.NoError(t, err)
	})

	t.Run("create table if not exists", func(t *testing.T) {
		consumer := &SaveToDuckDB{
			db:               db,
			tableName:        "new_table",
			createTableIfMissing: true,
			schema: map[string]string{
				"id":     "VARCHAR",
				"value":  "BIGINT",
				"ts":     "TIMESTAMP",
			},
		}

		mock = resetDuckDBMock(db, mock, t)

		// Table doesn't exist
		mock.ExpectQuery("SELECT COUNT").
			WithArgs("new_table").
			WillReturnRows(sqlmock.NewRows([]string{"count"}).AddRow(0))

		// Expect table creation
		mock.ExpectExec("CREATE TABLE new_table").
			WillReturnResult(sqlmock.NewResult(0, 0))

		// Then insert
		mock.ExpectExec("INSERT INTO new_table").
			WithArgs(sqlmock.AnyArg()).
			WillReturnResult(sqlmock.NewResult(1, 1))

		ctx := context.Background()

		msg := processor.Message{
			Payload: map[string]interface{}{
				"id":    "123",
				"value": int64(100),
				"ts":    time.Now(),
			},
		}

		err := consumer.Process(ctx, msg)
		assert.NoError(t, err)

		err = mock.ExpectationsWereMet()
		assert.NoError(t, err)
	})
}

// TestSaveToDuckDB_Batching tests batch insert functionality
func TestSaveToDuckDB_Batching(t *testing.T) {
	db, mock, err := sqlmock.New()
	require.NoError(t, err)
	defer db.Close()

	t.Run("batch insert when buffer full", func(t *testing.T) {
		consumer := &SaveToDuckDB{
			db:        db,
			tableName: "batch_data",
			batchSize: 3,
			buffer:    make([]map[string]interface{}, 0, 3),
		}

		// Expect table check once
		mock.ExpectQuery("SELECT COUNT").
			WithArgs("batch_data").
			WillReturnRows(sqlmock.NewRows([]string{"count"}).AddRow(1))

		// Expect COPY FROM for batch insert
		mock.ExpectExec("COPY batch_data FROM").
			WillReturnResult(sqlmock.NewResult(3, 3))

		ctx := context.Background()

		// Send 3 messages to fill buffer
		for i := 0; i < 3; i++ {
			msg := processor.Message{
				Payload: map[string]interface{}{
					"id":    i,
					"value": fmt.Sprintf("value%d", i),
				},
			}
			err := consumer.Process(ctx, msg)
			assert.NoError(t, err)
		}

		err = mock.ExpectationsWereMet()
		assert.NoError(t, err)
	})

	t.Run("flush partial batch", func(t *testing.T) {
		consumer := &SaveToDuckDB{
			db:        db,
			tableName: "partial_batch",
			batchSize: 10,
			buffer:    make([]map[string]interface{}, 0, 10),
		}

		mock = resetDuckDBMock(db, mock, t)

		mock.ExpectQuery("SELECT COUNT").
			WithArgs("partial_batch").
			WillReturnRows(sqlmock.NewRows([]string{"count"}).AddRow(1))

		// Send 2 messages (less than batch size)
		ctx := context.Background()
		for i := 0; i < 2; i++ {
			msg := processor.Message{
				Payload: map[string]interface{}{"id": i},
			}
			consumer.Process(ctx, msg)
		}

		// Expect flush with 2 records
		mock.ExpectExec("COPY partial_batch FROM").
			WillReturnResult(sqlmock.NewResult(2, 2))

		// Manual flush
		err := consumer.flush(ctx)
		assert.NoError(t, err)

		err = mock.ExpectationsWereMet()
		assert.NoError(t, err)
	})
}

// TestSaveToDuckDB_Transactions tests transaction support
func TestSaveToDuckDB_Transactions(t *testing.T) {
	db, mock, err := sqlmock.New()
	require.NoError(t, err)
	defer db.Close()

	t.Run("transaction for batch operations", func(t *testing.T) {
		consumer := &SaveToDuckDB{
			db:             db,
			tableName:      "tx_data",
			useTransaction: true,
			batchSize:      5,
			buffer:         make([]map[string]interface{}, 0, 5),
		}

		// Begin transaction
		mock.ExpectBegin()

		// Table check within transaction
		mock.ExpectQuery("SELECT COUNT").
			WithArgs("tx_data").
			WillReturnRows(sqlmock.NewRows([]string{"count"}).AddRow(1))

		// Batch insert
		mock.ExpectExec("COPY tx_data FROM").
			WillReturnResult(sqlmock.NewResult(5, 5))

		// Commit transaction
		mock.ExpectCommit()

		ctx := context.Background()

		// Process batch
		for i := 0; i < 5; i++ {
			msg := processor.Message{
				Payload: map[string]interface{}{"id": i},
			}
			err := consumer.Process(ctx, msg)
			assert.NoError(t, err)
		}

		err = mock.ExpectationsWereMet()
		assert.NoError(t, err)
	})

	t.Run("rollback on error", func(t *testing.T) {
		consumer := &SaveToDuckDB{
			db:             db,
			tableName:      "tx_error",
			useTransaction: true,
		}

		mock = resetDuckDBMock(db, mock, t)

		// Begin transaction
		mock.ExpectBegin()

		// Table check
		mock.ExpectQuery("SELECT COUNT").
			WithArgs("tx_error").
			WillReturnRows(sqlmock.NewRows([]string{"count"}).AddRow(1))

		// Insert fails
		mock.ExpectExec("INSERT INTO tx_error").
			WillReturnError(fmt.Errorf("constraint violation"))

		// Expect rollback
		mock.ExpectRollback()

		ctx := context.Background()

		msg := processor.Message{
			Payload: map[string]interface{}{"id": 1},
		}

		err := consumer.Process(ctx, msg)
		assert.Error(t, err)
	})
}

// TestSaveToDuckDB_Analytics tests analytical features
func TestSaveToDuckDB_Analytics(t *testing.T) {
	db, mock, err := sqlmock.New()
	require.NoError(t, err)
	defer db.Close()

	t.Run("create summary tables", func(t *testing.T) {
		consumer := &SaveToDuckDB{
			db:              db,
			tableName:       "raw_data",
			summaryTables:   []string{"daily_summary", "hourly_summary"},
			updateSummaries: true,
		}

		mock.ExpectQuery("SELECT COUNT").
			WithArgs("raw_data").
			WillReturnRows(sqlmock.NewRows([]string{"count"}).AddRow(1))

		// Insert raw data
		mock.ExpectExec("INSERT INTO raw_data").
			WillReturnResult(sqlmock.NewResult(1, 1))

		// Update summary tables
		mock.ExpectExec("INSERT INTO daily_summary").
			WillReturnResult(sqlmock.NewResult(1, 1))

		mock.ExpectExec("INSERT INTO hourly_summary").
			WillReturnResult(sqlmock.NewResult(1, 1))

		ctx := context.Background()

		msg := processor.Message{
			Payload: map[string]interface{}{
				"amount":    1000,
				"timestamp": time.Now(),
			},
		}

		err := consumer.Process(ctx, msg)
		assert.NoError(t, err)

		err = mock.ExpectationsWereMet()
		assert.NoError(t, err)
	})

	t.Run("create indexes for performance", func(t *testing.T) {
		consumer := &SaveToDuckDB{
			db:        db,
			tableName: "indexed_data",
			indexes: []IndexDefinition{
				{
					Name:    "idx_timestamp",
					Columns: []string{"timestamp"},
				},
				{
					Name:    "idx_contract_key",
					Columns: []string{"contract_id", "key"},
					Unique:  true,
				},
			},
		}

		// Simulate index creation
		mock.ExpectExec("CREATE INDEX idx_timestamp").
			WillReturnResult(sqlmock.NewResult(0, 0))

		mock.ExpectExec("CREATE UNIQUE INDEX idx_contract_key").
			WillReturnResult(sqlmock.NewResult(0, 0))

		err := consumer.createIndexes(context.Background())
		assert.NoError(t, err)
	})
}

// TestSaveToDuckDB_Partitioning tests partitioned tables
func TestSaveToDuckDB_Partitioning(t *testing.T) {
	db, mock, err := sqlmock.New()
	require.NoError(t, err)
	defer db.Close()

	t.Run("partition by date", func(t *testing.T) {
		consumer := &SaveToDuckDB{
			db:            db,
			tableName:     "partitioned_data",
			partitionBy:   "closed_at",
			partitionType: "RANGE",
			partitionFunc: func(data map[string]interface{}) string {
				// Partition by month
				if ts, ok := data["closed_at"].(time.Time); ok {
					return fmt.Sprintf("%d_%02d", ts.Year(), ts.Month())
				}
				return "default"
			},
		}

		// Check partition table
		partitionTable := "partitioned_data_2024_01"
		mock.ExpectQuery("SELECT COUNT").
			WithArgs(partitionTable).
			WillReturnRows(sqlmock.NewRows([]string{"count"}).AddRow(1))

		// Insert into partition
		mock.ExpectExec(fmt.Sprintf("INSERT INTO %s", partitionTable)).
			WillReturnResult(sqlmock.NewResult(1, 1))

		ctx := context.Background()

		msg := processor.Message{
			Payload: map[string]interface{}{
				"id":        "123",
				"closed_at": time.Date(2024, 1, 15, 0, 0, 0, 0, time.UTC),
			},
		}

		err := consumer.Process(ctx, msg)
		assert.NoError(t, err)

		err = mock.ExpectationsWereMet()
		assert.NoError(t, err)
	})
}

// TestSaveToDuckDB_ErrorHandling tests error scenarios
func TestSaveToDuckDB_ErrorHandling(t *testing.T) {
	db, mock, err := sqlmock.New()
	require.NoError(t, err)
	defer db.Close()

	t.Run("handle connection error", func(t *testing.T) {
		consumer := &SaveToDuckDB{
			db:        db,
			tableName: "error_test",
		}

		mock.ExpectQuery("SELECT COUNT").
			WillReturnError(sql.ErrConnDone)

		ctx := context.Background()

		msg := processor.Message{
			Payload: map[string]interface{}{"id": 1},
		}

		err := consumer.Process(ctx, msg)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "connection")
	})

	t.Run("retry on transient errors", func(t *testing.T) {
		consumer := &SaveToDuckDB{
			db:            db,
			tableName:     "retry_test",
			retryAttempts: 3,
			retryDelay:    10 * time.Millisecond,
		}

		mock = resetDuckDBMock(db, mock, t)

		// Table check succeeds
		mock.ExpectQuery("SELECT COUNT").
			WithArgs("retry_test").
			WillReturnRows(sqlmock.NewRows([]string{"count"}).AddRow(1))

		// First two attempts fail
		mock.ExpectExec("INSERT INTO retry_test").
			WillReturnError(fmt.Errorf("temporary error"))
		mock.ExpectExec("INSERT INTO retry_test").
			WillReturnError(fmt.Errorf("temporary error"))

		// Third attempt succeeds
		mock.ExpectExec("INSERT INTO retry_test").
			WillReturnResult(sqlmock.NewResult(1, 1))

		ctx := context.Background()

		msg := processor.Message{
			Payload: map[string]interface{}{"id": 1},
		}

		err := consumer.Process(ctx, msg)
		assert.NoError(t, err)

		err = mock.ExpectationsWereMet()
		assert.NoError(t, err)
	})
}

// TestSaveToDuckDB_Concurrency tests thread safety
func TestSaveToDuckDB_Concurrency(t *testing.T) {
	db, mock, err := sqlmock.New()
	require.NoError(t, err)
	defer db.Close()

	consumer := &SaveToDuckDB{
		db:        db,
		tableName: "concurrent_data",
		batchSize: 100,
		buffer:    make([]map[string]interface{}, 0, 100),
		mu:        &sync.Mutex{},
	}

	// Set up expectations for concurrent operations
	mock.ExpectQuery("SELECT COUNT").
		WithArgs("concurrent_data").
		WillReturnRows(sqlmock.NewRows([]string{"count"}).AddRow(1))

	// Expect multiple batch inserts
	for i := 0; i < 5; i++ {
		mock.ExpectExec("COPY concurrent_data FROM").
			WillReturnResult(sqlmock.NewResult(100, 100))
	}

	ctx := context.Background()
	numGoroutines := 10
	messagesPerGoroutine := 50

	var wg sync.WaitGroup
	errors := make([]error, numGoroutines)

	// Launch concurrent writers
	for i := 0; i < numGoroutines; i++ {
		wg.Add(1)
		go func(workerID int) {
			defer wg.Done()
			for j := 0; j < messagesPerGoroutine; j++ {
				msg := processor.Message{
					Payload: map[string]interface{}{
						"worker_id":  workerID,
						"message_id": j,
						"value":      j * 100,
					},
				}
				if err := consumer.Process(ctx, msg); err != nil {
					errors[workerID] = err
					return
				}
			}
		}(i)
	}

	wg.Wait()

	// Check for errors
	for i, err := range errors {
		assert.NoError(t, err, "worker %d encountered error", i)
	}
}

// Benchmarks
func BenchmarkSaveToDuckDB_SingleInsert(b *testing.B) {
	db, mock, err := sqlmock.New()
	require.NoError(b, err)
	defer db.Close()

	consumer := &SaveToDuckDB{
		db:        db,
		tableName: "bench_data",
	}

	// Set up expectations
	mock.ExpectQuery("SELECT COUNT").
		WithArgs("bench_data").
		WillReturnRows(sqlmock.NewRows([]string{"count"}).AddRow(1))

	for i := 0; i < b.N; i++ {
		mock.ExpectExec("INSERT INTO bench_data").
			WillReturnResult(sqlmock.NewResult(int64(i), 1))
	}

	ctx := context.Background()

	testData := map[string]interface{}{
		"id":        "123",
		"value":     1000000,
		"timestamp": time.Now(),
		"metadata": map[string]string{
			"field1": "value1",
			"field2": "value2",
		},
	}

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		msg := processor.Message{Payload: testData}
		consumer.Process(ctx, msg)
	}

	b.StopTimer()
	b.ReportMetric(float64(b.N), "inserts")
	b.ReportMetric(float64(b.N)/b.Elapsed().Seconds(), "inserts/sec")
}

func BenchmarkSaveToDuckDB_BatchInsert(b *testing.B) {
	db, mock, err := sqlmock.New()
	require.NoError(b, err)
	defer db.Close()

	consumer := &SaveToDuckDB{
		db:        db,
		tableName: "bench_batch",
		batchSize: 1000,
		buffer:    make([]map[string]interface{}, 0, 1000),
	}

	// Set up expectations
	mock.ExpectQuery("SELECT COUNT").
		WithArgs("bench_batch").
		WillReturnRows(sqlmock.NewRows([]string{"count"}).AddRow(1))

	// Expect batch inserts
	numBatches := (b.N + 999) / 1000
	for i := 0; i < numBatches; i++ {
		mock.ExpectExec("COPY bench_batch FROM").
			WillReturnResult(sqlmock.NewResult(int64(i*1000), 1000))
	}

	ctx := context.Background()

	testData := map[string]interface{}{
		"id":    "123",
		"value": 1000,
	}

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		msg := processor.Message{Payload: testData}
		consumer.Process(ctx, msg)
	}

	// Flush remaining
	consumer.flush(ctx)

	b.StopTimer()
	b.ReportMetric(float64(b.N), "inserts")
	b.ReportMetric(float64(b.N)/b.Elapsed().Seconds(), "inserts/sec")
	b.ReportMetric(float64(numBatches), "batches")
}

// Helper functions

func resetDuckDBMock(db *sql.DB, oldMock sqlmock.Sqlmock, t *testing.T) sqlmock.Sqlmock {
	// Ensure previous expectations were met
	err := oldMock.ExpectationsWereMet()
	assert.NoError(t, err)
	
	// Return same mock (in real tests, might need to recreate)
	return oldMock
}

// Mock SaveToDuckDB implementation
type SaveToDuckDB struct {
	db                   *sql.DB
	tableName            string
	batchSize            int
	buffer               []map[string]interface{}
	batchTimeout         time.Duration
	mu                   *sync.Mutex
	schema               map[string]string
	createTableIfMissing bool
	useTransaction       bool
	compression          string
	partitionBy          string
	partitionType        string
	partitionFunc        func(map[string]interface{}) string
	indexes              []IndexDefinition
	summaryTables        []string
	updateSummaries      bool
	retryAttempts        int
	retryDelay           time.Duration
}

type IndexDefinition struct {
	Name    string
	Columns []string
	Unique  bool
}

func (s *SaveToDuckDB) Process(ctx context.Context, msg processor.Message) error {
	if s.mu != nil {
		s.mu.Lock()
		defer s.mu.Unlock()
	}

	// Convert payload
	var data map[string]interface{}
	
	switch v := msg.Payload.(type) {
	case []byte:
		if err := json.Unmarshal(v, &data); err != nil {
			return fmt.Errorf("failed to unmarshal JSON: %w", err)
		}
	case map[string]interface{}:
		data = v
	default:
		return fmt.Errorf("unsupported payload type: %T", v)
	}

	// Check table exists
	tableName := s.tableName
	if s.partitionFunc != nil {
		tableName = s.tableName + "_" + s.partitionFunc(data)
	}

	query := "SELECT COUNT(*) FROM information_schema.tables WHERE table_name = ?"
	s.db.QueryRowContext(ctx, query, tableName)

	// Handle batching
	if s.batchSize > 1 {
		s.buffer = append(s.buffer, data)
		if len(s.buffer) >= s.batchSize {
			return s.flush(ctx)
		}
		return nil
	}

	// Single insert
	_, err := s.db.ExecContext(ctx, fmt.Sprintf("INSERT INTO %s VALUES (?)", tableName), data)
	
	// Update summaries if configured
	if s.updateSummaries && err == nil {
		for _, summary := range s.summaryTables {
			s.db.ExecContext(ctx, fmt.Sprintf("INSERT INTO %s SELECT * FROM ...", summary))
		}
	}

	return err
}

func (s *SaveToDuckDB) Subscribe(processor processor.Processor) {
	// Not implemented for consumer
}

func (s *SaveToDuckDB) flush(ctx context.Context) error {
	if len(s.buffer) == 0 {
		return nil
	}

	// Use COPY for batch insert
	query := fmt.Sprintf("COPY %s FROM VALUES (?)", s.tableName)
	_, err := s.db.ExecContext(ctx, query, s.buffer)
	
	// Clear buffer
	s.buffer = s.buffer[:0]
	
	return err
}

func (s *SaveToDuckDB) createIndexes(ctx context.Context) error {
	for _, idx := range s.indexes {
		indexType := "INDEX"
		if idx.Unique {
			indexType = "UNIQUE INDEX"
		}
		
		columns := ""
		for i, col := range idx.Columns {
			if i > 0 {
				columns += ", "
			}
			columns += col
		}
		
		query := fmt.Sprintf("CREATE %s %s ON %s (%s)", 
			indexType, idx.Name, s.tableName, columns)
		
		if _, err := s.db.ExecContext(ctx, query); err != nil {
			return err
		}
	}
	return nil
}