package main

import (
	"context"
	"log"
	"time"

	"github.com/pkg/errors"
	cdp "github.com/withObsrvr/stellar-cdp"
	ledgerbackend "github.com/withObsrvr/stellar-ledgerbackend"

	"github.com/stellar/go/xdr"
	cdpProcessor "github.com/withObsrvr/cdp-pipeline-workflow/processor"

	datastore "github.com/withObsrvr/stellar-datastore"
)

type BufferedStorageSourceAdapter struct {
	config     BufferedStorageConfig
	processors []cdpProcessor.Processor
}

type BufferedStorageConfig struct {
	BucketName        string
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

	// Get end ledger with same type handling as start ledger
	endLedgerRaw, ok := config["end_ledger"]
	var endLedger uint32
	if ok {
		endLedgerInt, ok := getIntValue(endLedgerRaw)
		if !ok {
			return nil, errors.New("invalid end_ledger value")
		}
		endLedger = uint32(endLedgerInt)

		// Validate end ledger is greater than start ledger
		if endLedger > 0 && endLedger < startLedger {
			return nil, errors.New("end_ledger must be greater than start_ledger")
		}
	}

	ledgersPerFileInt, _ := getIntValue(config["ledgers_per_file"])
	if ledgersPerFileInt == 0 {
		ledgersPerFileInt = 64 // default value
	}

	filesPerPartitionInt, _ := getIntValue(config["files_per_partition"])
	if filesPerPartitionInt == 0 {
		filesPerPartitionInt = 10 // default value
	}

	bufferConfig := BufferedStorageConfig{
		BucketName:        bucketName,
		Network:           network,
		BufferSize:        uint32(bufferSizeInt),
		NumWorkers:        uint32(numWorkersInt),
		RetryLimit:        uint32(retryLimitInt),
		RetryWait:         uint32(retryWaitInt),
		StartLedger:       startLedger,
		EndLedger:         endLedger,
		LedgersPerFile:    uint32(ledgersPerFileInt),
		FilesPerPartition: uint32(filesPerPartitionInt),
	}

	log.Printf("Parsed configuration: start_ledger=%d, end_ledger=%d, bucket=%s, network=%s",
		startLedger, endLedger, bucketName, network)

	return &BufferedStorageSourceAdapter{
		config: bufferConfig,
	}, nil
}

func (adapter *BufferedStorageSourceAdapter) Subscribe(receiver cdpProcessor.Processor) {
	adapter.processors = append(adapter.processors, receiver)
}

func (adapter *BufferedStorageSourceAdapter) Run(ctx context.Context) error {
	log.Printf("Starting BufferedStorageSourceAdapter from ledger %d", adapter.config.StartLedger)
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

	dataStoreConfig := datastore.DataStoreConfig{
		Type:   "GCS",
		Schema: schema,
		Params: map[string]string{
			"destination_bucket_path": adapter.config.BucketName,
		},
	}

	// Create buffered storage configuration
	bufferedConfig := cdp.DefaultBufferedStorageBackendConfig(schema.LedgersPerFile)
	bufferedConfig.BufferSize = adapter.config.BufferSize
	bufferedConfig.NumWorkers = adapter.config.NumWorkers
	bufferedConfig.RetryLimit = adapter.config.RetryLimit
	bufferedConfig.RetryWait = time.Duration(adapter.config.RetryWait) * time.Second

	publisherConfig := cdp.PublisherConfig{
		DataStoreConfig:       dataStoreConfig,
		BufferedStorageConfig: bufferedConfig,
	}

	// Create ledger range based on configuration
	var ledgerRange ledgerbackend.Range
	if adapter.config.EndLedger > 0 {
		ledgerRange = ledgerbackend.BoundedRange(
			adapter.config.StartLedger,
			adapter.config.EndLedger,
		)
	} else {
		ledgerRange = ledgerbackend.UnboundedRange(adapter.config.StartLedger)
	}

	log.Printf("Starting ledger processing with range: %v", ledgerRange)

	processedLedgers := 0
	lastLogTime := time.Now()
	lastLedgerTime := time.Now()

	err := cdp.ApplyLedgerMetadata(
		ledgerRange,
		publisherConfig,
		ctx,
		func(lcm xdr.LedgerCloseMeta) error {
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
			if time.Since(lastLogTime) > time.Second*10 {
				rate := float64(processedLedgers) / time.Since(lastLogTime).Seconds()
				log.Printf("Processed %d ledgers so far (%.2f ledgers/sec)",
					processedLedgers, rate)
				lastLogTime = time.Now()
			}

			return nil
		},
	)

	if err != nil {
		log.Printf("Pipeline error: %v", err)
		return err
	}

	duration := time.Since(lastLogTime)
	rate := float64(processedLedgers) / duration.Seconds()
	log.Printf("Pipeline completed successfully. Processed %d ledgers in %v (%.2f ledgers/sec)",
		processedLedgers, duration, rate)
	return nil
}

func (adapter *BufferedStorageSourceAdapter) processLedger(ctx context.Context, ledger xdr.LedgerCloseMeta) error {
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

func (adapter *BufferedStorageSourceAdapter) Close() error {
	return nil
}
