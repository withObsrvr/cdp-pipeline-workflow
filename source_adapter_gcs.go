package main

import (
	"context"
	"log"
	"time"

	"github.com/pkg/errors"
	"github.com/stellar/go/ingest/ledgerbackend"
	"github.com/stellar/go/support/datastore"
	"github.com/stellar/go/xdr"
)

type BufferedStorageSourceAdapter struct {
	config     BufferedStorageConfig
	processors []Processor
	backend    *ledgerbackend.BufferedStorageBackend
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

	return newBufferedStorageAdapter(bufferConfig)
}
func newBufferedStorageAdapter(config BufferedStorageConfig) (*BufferedStorageSourceAdapter, error) {
	ctx := context.Background()

	log.Printf("Creating GCS data store for bucket: %s", config.BucketName)
	schema := datastore.DataStoreSchema{
		LedgersPerFile:    uint32(64),
		FilesPerPartition: uint32(10),
	}

	dataStore, err := datastore.NewGCSDataStore(ctx, config.BucketName, schema)
	if err != nil {
		return nil, errors.Wrap(err, "error creating GCS data store")
	}
	log.Printf("Successfully created GCS data store")

	backendConfig := ledgerbackend.BufferedStorageBackendConfig{
		BufferSize: config.BufferSize,
		NumWorkers: config.NumWorkers,
	}

	log.Printf("Creating buffered storage backend with buffer size: %d, workers: %d",
		config.BufferSize, config.NumWorkers)
	backend, err := ledgerbackend.NewBufferedStorageBackend(backendConfig, dataStore)
	if err != nil {
		return nil, errors.Wrap(err, "error creating BufferedStorageBackend")
	}
	log.Printf("Successfully created buffered storage backend")

	return &BufferedStorageSourceAdapter{
		config:  config,
		backend: backend,
	}, nil
}

func (adapter *BufferedStorageSourceAdapter) Subscribe(receiver Processor) {
	adapter.processors = append(adapter.processors, receiver)
}

func (adapter *BufferedStorageSourceAdapter) Run(ctx context.Context) error {
	log.Printf("Initializing with start ledger: %d", adapter.config.StartLedger)

	ledgerRange := ledgerbackend.UnboundedRange(adapter.config.StartLedger)
	log.Printf("Preparing range starting at ledger %d", adapter.config.StartLedger)

	if err := adapter.backend.PrepareRange(ctx, ledgerRange); err != nil {
		return errors.Wrap(err, "error preparing range")
	}

	time.Sleep(time.Second * 2)

	// Verify we're prepared
	prepared, err := adapter.backend.IsPrepared(ctx, ledgerRange)
	if err != nil {
		return errors.Wrap(err, "error checking if range is prepared")
	}
	if !prepared {
		return errors.New("range preparation failed")
	}
	log.Printf("Successfully prepared range starting at ledger %d", adapter.config.StartLedger)

	// Try to get the latest ledger sequence to verify connectivity
	latestLedger, err := adapter.backend.GetLatestLedgerSequence(ctx)
	if err != nil {
		return errors.Wrap(err, "error getting latest ledger sequence")
	}
	log.Printf("Latest available ledger in data store: %d", latestLedger)

	// Initialize current ledger
	currentLedger := adapter.config.StartLedger
	log.Printf("Beginning processing at ledger %d", currentLedger)

	// Main processing loop
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
			log.Printf("Attempting to retrieve ledger %d", currentLedger)
			ledger, err := adapter.backend.GetLedger(ctx, currentLedger)
			if err != nil {
				log.Printf("Error getting ledger %d: %v", currentLedger, err)

				if currentLedger > adapter.config.StartLedger {
					time.Sleep(time.Second * 5)
					continue
				}
				return errors.Wrapf(err, "error getting ledger %d", currentLedger)
			}

			log.Printf("Successfully retrieved ledger %d", currentLedger)

			if err := adapter.processLedger(ctx, ledger); err != nil {
				return errors.Wrapf(err, "error processing ledger %d", currentLedger)
			}

			log.Printf("Successfully processed ledger %d", currentLedger)
			currentLedger++
		}
	}
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
	if adapter.backend != nil {
		return adapter.backend.Close()
	}
	return nil
}
