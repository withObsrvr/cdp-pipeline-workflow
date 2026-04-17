package quality

import "time"

// LedgerData represents a Stellar ledger record for quality checking
type LedgerData struct {
	Sequence            uint32
	LedgerHash          string
	PreviousLedgerHash  string
	TransactionCount    uint32
	ClosedAt            time.Time
}

// TransactionData represents a Stellar transaction record for quality checking
type TransactionData struct {
	LedgerSequence  uint32
	TransactionHash string
	SourceAccount   string
	FeeCharged      int64
}

// OperationData represents a Stellar operation record for quality checking
type OperationData struct {
	LedgerSequence  uint32
	TransactionHash string
	OperationIndex  int32
}

// BalanceData represents a Stellar account balance record for quality checking
type BalanceData struct {
	AccountID          string
	LedgerSequence     uint32
	Balance            int64
	BuyingLiabilities  int64
	SellingLiabilities int64
	SequenceNumber     int64
}

// DataBuffers holds buffered data for quality checking
type DataBuffers struct {
	ledgers      []LedgerData
	transactions []TransactionData
	operations   []OperationData
	balances     []BalanceData
}

// DuckLakeConfig represents DuckLake configuration for quality checks
type DuckLakeConfig struct {
	CatalogName string
	SchemaName  string
}

// Config represents the configuration for the Ingester
type Config struct {
	DuckLake DuckLakeConfig
}

// Result represents a database execution result
type Result interface{}

// DB represents a database connection interface for quality checks
type DB interface {
	Exec(query string, args ...interface{}) (Result, error)
}

// Ingester represents a data ingester with quality checking capabilities
type Ingester struct {
	buffers *DataBuffers
	config  *Config
	db      DB
}
