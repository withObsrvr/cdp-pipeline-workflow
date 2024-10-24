package main

import (
	"context"
	"log"
	"time"

	"github.com/pkg/errors"
	"github.com/stellar/go/ingest/cdp"
	"github.com/stellar/go/ingest/ledgerbackend"
	"github.com/stellar/go/support/datastore"
	"github.com/stellar/go/xdr"
)

type BufferedStorageSourceAdapter struct {
	config     BufferedStorageConfig
	processors []Processor
}

type BufferedStorageConfig struct {
	BucketName  string
	BufferSize  uint32
	NumWorkers  uint32
	RetryLimit  uint32
	RetryWait   uint32
	Network     string
	StartLedger uint32
}

func NewBufferedStorageSourceAdapter(config map[string]interface{}) (SourceAdapter, error) {
	// Helper function to safely convert interface{} to int
	getIntValue := func(v interface{}) (int, bool) {
		switch i := v.(type) {
		case int:
			return i, true
		case float64:
			return int(i), true
		case int64:
			return int(i), true
		}
		return 0, false
	}

	// Get start ledger with more flexible type handling
	startLedgerRaw, ok := config["start_ledger"]
	if !ok {
		return nil, errors.New("start_ledger must be specified")
	}
	startLedgerInt, ok := getIntValue(startLedgerRaw)
	if !ok {
		return nil, errors.New("invalid start_ledger value")
	}
	startLedger := uint32(startLedgerInt)

	bucketName, ok := config["bucket_name"].(string)
	if !ok {
		return nil, errors.New("bucket_name is missing")
	}

	network, ok := config["network"].(string)
	if !ok {
		return nil, errors.New("network must be specified")
	}

	// Get other config values with defaults
	bufferSizeInt, _ := getIntValue(config["buffer_size"])
	if bufferSizeInt == 0 {
		bufferSizeInt = 1024
	}

	numWorkersInt, _ := getIntValue(config["num_workers"])
	if numWorkersInt == 0 {
		numWorkersInt = 10
	}

	retryLimitInt, _ := getIntValue(config["retry_limit"])
	if retryLimitInt == 0 {
		retryLimitInt = 3
	}

	retryWaitInt, _ := getIntValue(config["retry_wait"])
	if retryWaitInt == 0 {
		retryWaitInt = 5
	}

	bufferConfig := BufferedStorageConfig{
		BucketName:  bucketName,
		Network:     network,
		BufferSize:  uint32(bufferSizeInt),
		NumWorkers:  uint32(numWorkersInt),
		RetryLimit:  uint32(retryLimitInt),
		RetryWait:   uint32(retryWaitInt),
		StartLedger: startLedger,
	}

	log.Printf("Parsed configuration: start_ledger=%d, bucket=%s, network=%s",
		startLedger, bucketName, network)

	return &BufferedStorageSourceAdapter{
		config: bufferConfig,
	}, nil
}

func (adapter *BufferedStorageSourceAdapter) Subscribe(receiver Processor) {
	adapter.processors = append(adapter.processors, receiver)
}

func (adapter *BufferedStorageSourceAdapter) Run(ctx context.Context) error {
	log.Printf("Initializing with start ledger: %d", adapter.config.StartLedger)

	// Create DataStore configuration
	schema := datastore.DataStoreSchema{
		LedgersPerFile:    uint32(64),
		FilesPerPartition: uint32(10),
	}

	dataStoreConfig := datastore.DataStoreConfig{
		Type:   "GCS",
		Schema: schema,
		Params: map[string]string{
			"destination_bucket_path": adapter.config.BucketName,
		},
	}

	// Create buffered storage configuration using CDP defaults
	bufferedConfig := cdp.DefaultBufferedStorageBackendConfig(schema.LedgersPerFile)
	// Override with any custom settings
	bufferedConfig.BufferSize = adapter.config.BufferSize
	bufferedConfig.NumWorkers = adapter.config.NumWorkers
	bufferedConfig.RetryLimit = adapter.config.RetryLimit
	bufferedConfig.RetryWait = time.Duration(adapter.config.RetryWait) * time.Second

	// Create publisher configuration
	publisherConfig := cdp.PublisherConfig{
		DataStoreConfig:       dataStoreConfig,
		BufferedStorageConfig: bufferedConfig,
	}

	// Create ledger range starting from configured ledger
	ledgerRange := ledgerbackend.UnboundedRange(adapter.config.StartLedger)

	// Process ledgers using CDP's ApplyLedgerMetadata
	return cdp.ApplyLedgerMetadata(
		ledgerRange,
		publisherConfig,
		ctx,
		func(lcm xdr.LedgerCloseMeta) error {
			return adapter.processLedger(ctx, lcm)
		},
	)
}

func (adapter *BufferedStorageSourceAdapter) processLedger(ctx context.Context, ledger xdr.LedgerCloseMeta) error {
	sequence := ledger.LedgerSequence()
	log.Printf("Processing ledger %d", sequence)

	for _, processor := range adapter.processors {
		if err := processor.Process(ctx, Message{Payload: ledger}); err != nil {
			return errors.Wrap(err, "error in processor")
		}
	}

	return nil
}

func (adapter *BufferedStorageSourceAdapter) Close() error {
	return nil
}
