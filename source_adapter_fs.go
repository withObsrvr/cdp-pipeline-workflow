package main

import (
	"context"
	"log"
	"time"

	"github.com/pkg/errors"
	"github.com/stellar/go/xdr"
	cdpProcessor "github.com/withObsrvr/cdp-pipeline-workflow/processor"
	cdp "github.com/withObsrvr/stellar-cdp"
	datastore "github.com/withObsrvr/stellar-datastore"
	ledgerbackend "github.com/withObsrvr/stellar-ledgerbackend"
)

type FSBufferedStorageSourceAdapter struct {
	config     FSBufferedStorageConfig
	processors []cdpProcessor.Processor
}

type FSBufferedStorageConfig struct {
	BasePath          string
	BufferSize        uint32
	NumWorkers        uint32
	RetryLimit        uint32
	RetryWait         uint32
	Network           string
	StartLedger       uint32
	EndLedger         uint32
	LedgersPerFile    uint32
	FilesPerPartition uint32
}

func NewFSBufferedStorageSourceAdapter(config map[string]interface{}) (SourceAdapter, error) {
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

	var fsConfig FSBufferedStorageConfig

	// Get base path
	if basePath, ok := config["base_path"].(string); ok {
		fsConfig.BasePath = basePath
	} else {
		return nil, errors.New("base_path must be specified")
	}

	// Get buffer size
	if bufferSize, ok := getIntValue(config["buffer_size"]); ok {
		fsConfig.BufferSize = uint32(bufferSize)
	} else {
		fsConfig.BufferSize = 64 // default buffer size
	}

	// Get number of workers
	if numWorkers, ok := getIntValue(config["num_workers"]); ok {
		fsConfig.NumWorkers = uint32(numWorkers)
	} else {
		fsConfig.NumWorkers = 10 // default number of workers
	}

	// Get retry limit
	if retryLimit, ok := getIntValue(config["retry_limit"]); ok {
		fsConfig.RetryLimit = uint32(retryLimit)
	} else {
		fsConfig.RetryLimit = 5 // default retry limit
	}

	// Get retry wait
	if retryWait, ok := getIntValue(config["retry_wait"]); ok {
		fsConfig.RetryWait = uint32(retryWait)
	} else {
		fsConfig.RetryWait = 30 // default retry wait in seconds
	}

	// Get network
	if network, ok := config["network"].(string); ok {
		fsConfig.Network = network
	} else {
		return nil, errors.New("network must be specified")
	}

	// Get start ledger
	if startLedger, ok := getIntValue(config["start_ledger"]); ok {
		fsConfig.StartLedger = uint32(startLedger)
	} else {
		return nil, errors.New("start_ledger must be specified")
	}

	// Get end ledger (optional)
	if endLedger, ok := getIntValue(config["end_ledger"]); ok {
		fsConfig.EndLedger = uint32(endLedger)
	}

	if fsConfig.EndLedger > 0 && fsConfig.EndLedger < fsConfig.StartLedger {
		return nil, errors.New("end_ledger must be greater than start_ledger")
	}

	// Get LedgersPerFile with default
	ledgersPerFileInt, _ := getIntValue(config["ledgers_per_file"])
	if ledgersPerFileInt == 0 {
		ledgersPerFileInt = 64 // default value
	}

	// Get FilesPerPartition with default
	filesPerPartitionInt, _ := getIntValue(config["files_per_partition"])
	if filesPerPartitionInt == 0 {
		filesPerPartitionInt = 10 // default value
	}

	return &FSBufferedStorageSourceAdapter{
		config: fsConfig,
	}, nil
}

func (adapter *FSBufferedStorageSourceAdapter) Run(ctx context.Context) error {
	log.Printf("Starting FSBufferedStorageSourceAdapter from ledger %d", adapter.config.StartLedger)
	if adapter.config.EndLedger > 0 {
		log.Printf("Will process until ledger %d", adapter.config.EndLedger)
	} else {
		log.Printf("Will process indefinitely from start ledger")
	}

	// Create DataStore configuration
	schema := datastore.DataStoreSchema{
		LedgersPerFile:    adapter.config.LedgersPerFile,
		FilesPerPartition: adapter.config.FilesPerPartition,
	}

	log.Printf("Created schema with LedgersPerFile=%d, FilesPerPartition=%d",
		schema.LedgersPerFile, schema.FilesPerPartition)

	dataStoreConfig := datastore.DataStoreConfig{
		Type:   "FS",
		Schema: schema,
		Params: map[string]string{
			"base_path": adapter.config.BasePath,
		},
	}

	// Create buffered storage configuration
	bufferedConfig := cdp.DefaultBufferedStorageBackendConfig(schema.LedgersPerFile)
	bufferedConfig.BufferSize = adapter.config.BufferSize
	bufferedConfig.NumWorkers = adapter.config.NumWorkers
	bufferedConfig.RetryLimit = adapter.config.RetryLimit
	bufferedConfig.RetryWait = time.Duration(adapter.config.RetryWait) * time.Second

	log.Printf("Created buffered config with BufferSize=%d, NumWorkers=%d",
		bufferedConfig.BufferSize, bufferedConfig.NumWorkers)

	publisherConfig := cdp.PublisherConfig{
		DataStoreConfig:       dataStoreConfig,
		BufferedStorageConfig: bufferedConfig,
	}

	log.Printf("Starting ledger processing with range: %v to %v",
		adapter.config.StartLedger, adapter.config.EndLedger)
	log.Printf("Using filesystem path: %s", adapter.config.BasePath)

	var ledgerRange ledgerbackend.Range
	if adapter.config.EndLedger > 0 {
		ledgerRange = ledgerbackend.BoundedRange(
			adapter.config.StartLedger,
			adapter.config.EndLedger,
		)
	} else {
		ledgerRange = ledgerbackend.UnboundedRange(adapter.config.StartLedger)
	}

	log.Printf("Created ledger range: %+v", ledgerRange)
	log.Printf("About to call processLedgerRange...")

	return adapter.processLedgerRange(ctx, ledgerRange, publisherConfig)
}

func (adapter *FSBufferedStorageSourceAdapter) Subscribe(processor cdpProcessor.Processor) {
	adapter.processors = append(adapter.processors, processor)
}

func (adapter *FSBufferedStorageSourceAdapter) processLedgerRange(ctx context.Context, ledgerRange ledgerbackend.Range, publisherConfig cdp.PublisherConfig) error {
	log.Printf("Entered processLedgerRange")
	processedLedgers := 0
	startTime := time.Now()
	lastLogTime := startTime
	lastLedgerTime := startTime

	log.Printf("About to call ApplyLedgerMetadata...")
	err := cdp.ApplyLedgerMetadata(
		ledgerRange,
		publisherConfig,
		ctx,
		func(lcm xdr.LedgerCloseMeta) error {
			log.Printf("ApplyLedgerMetadata callback received ledger %d", lcm.LedgerSequence())
			currentTime := time.Now()
			ledgerProcessingTime := currentTime.Sub(lastLedgerTime)
			lastLedgerTime = currentTime

			log.Printf("Starting to process ledger %d (took %v since last ledger)",
				lcm.LedgerSequence(), ledgerProcessingTime)

			if err := adapter.processLedger(ctx, lcm); err != nil {
				log.Printf("Error processing ledger %d: %v", lcm.LedgerSequence(), err)
				return err
			}

			processedLedgers++
			if time.Since(lastLogTime) > time.Minute {
				rate := float64(processedLedgers) / time.Since(lastLogTime).Seconds()
				log.Printf("Processed %d ledgers at %.2f ledgers/sec", processedLedgers, rate)
				lastLogTime = time.Now()
			}

			return nil
		},
	)

	if err != nil {
		log.Printf("Pipeline error: %v", err)
		return err
	}

	duration := time.Since(startTime)
	rate := float64(processedLedgers) / duration.Seconds()
	log.Printf("Pipeline completed successfully. Processed %d ledgers in %v (%.2f ledgers/sec)",
		processedLedgers, duration, rate)
	return nil
}

func (adapter *FSBufferedStorageSourceAdapter) processLedger(ctx context.Context, ledger xdr.LedgerCloseMeta) error {
	sequence := ledger.LedgerSequence()
	log.Printf("Processing ledger %d", sequence)

	for _, processor := range adapter.processors {
		if err := processor.Process(ctx, cdpProcessor.Message{Payload: ledger}); err != nil {
			log.Printf("Error processing ledger %d in processor %T: %v",
				sequence, processor, err)
			return errors.Wrap(err, "error in processor")
		}
		log.Printf("Successfully processed ledger %d in processor %T",
			sequence, processor)
	}

	return nil
}

func (adapter *FSBufferedStorageSourceAdapter) Close() error {
	return nil
}
