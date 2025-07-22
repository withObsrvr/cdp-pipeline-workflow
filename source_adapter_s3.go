package main

import (
	"context"
	"fmt"
	"log"
	"time"

	"github.com/pkg/errors"
	"github.com/stellar/go/ingest/cdp"
	"github.com/stellar/go/ingest/ledgerbackend"
	"github.com/stellar/go/support/datastore"
	"github.com/stellar/go/xdr"
	cdpProcessor "github.com/withObsrvr/cdp-pipeline-workflow/processor"
)

type S3BufferedStorageSourceAdapter struct {
	config     S3BufferedStorageConfig
	processors []cdpProcessor.Processor
}

type S3BufferedStorageConfig struct {
	BucketName        string
	Region            string
	Endpoint          string
	ForcePathStyle    bool
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

func NewS3BufferedStorageSourceAdapter(config map[string]interface{}) (SourceAdapter, error) {
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

	// Get required configuration values
	startLedgerRaw, ok := config["start_ledger"]
	if !ok {
		return nil, errors.New("start_ledger must be specified")
	}
	startLedgerInt, ok := getIntValue(startLedgerRaw)
	if !ok {
		return nil, errors.New("invalid start_ledger value")
	}
	startLedger := uint32(startLedgerInt)

	// Add after startLedger handling
	endLedger := uint32(0)
	endLedgerRaw, ok := config["end_ledger"]
	if ok {
		endLedgerInt, ok := getIntValue(endLedgerRaw)
		if !ok {
			return nil, errors.New("invalid end_ledger value")
		}
		endLedger = uint32(endLedgerInt)
	}

	bucketName, ok := config["bucket_name"].(string)
	if !ok {
		return nil, errors.New("bucket_name is missing")
	}

	region, ok := config["region"].(string)
	if !ok {
		return nil, errors.New("region must be specified")
	}

	network, ok := config["network"].(string)
	if !ok {
		return nil, errors.New("network must be specified")
	}

	// Optional configuration values with defaults
	endpoint, _ := config["endpoint"].(string)
	forcePathStyle, _ := config["force_path_style"].(bool)

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

	return &S3BufferedStorageSourceAdapter{
		config: S3BufferedStorageConfig{
			BucketName:        bucketName,
			Region:            region,
			Endpoint:          endpoint,
			ForcePathStyle:    forcePathStyle,
			BufferSize:        uint32(bufferSizeInt),
			NumWorkers:        uint32(numWorkersInt),
			RetryLimit:        uint32(retryLimitInt),
			RetryWait:         uint32(retryWaitInt),
			Network:           network,
			StartLedger:       startLedger,
			EndLedger:         endLedger,
			LedgersPerFile:    uint32(ledgersPerFileInt),
			FilesPerPartition: uint32(filesPerPartitionInt),
		},
	}, nil
}

func (adapter *S3BufferedStorageSourceAdapter) Subscribe(processor cdpProcessor.Processor) {
	adapter.processors = append(adapter.processors, processor)
}

func (adapter *S3BufferedStorageSourceAdapter) Run(ctx context.Context) error {
	startTime := time.Now()

	log.Printf("Starting S3BufferedStorageSourceAdapter from ledger %d", adapter.config.StartLedger)
	if adapter.config.EndLedger > 0 {
		log.Printf("Will process until ledger %d", adapter.config.EndLedger)
	} else {
		log.Printf("Will process indefinitely from start ledger")
	}

	schema := datastore.DataStoreSchema{
		LedgersPerFile:    adapter.config.LedgersPerFile,
		FilesPerPartition: adapter.config.FilesPerPartition,
	}

	dataStoreConfig := datastore.DataStoreConfig{
		Type:   "S3",
		Schema: schema,
		Params: map[string]string{
			"bucket_name":      adapter.config.BucketName,
			"region":           adapter.config.Region,
			"endpoint":         adapter.config.Endpoint,
			"force_path_style": fmt.Sprintf("%v", adapter.config.ForcePathStyle),
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

	// Create datastore config
	datastoreConfig := datastore.DataStoreConfig{
		Type: "S3",
		Params: map[string]string{
			"bucket_name":      adapter.config.BucketName,
			"region":           adapter.config.Region,
			"endpoint":         adapter.config.Endpoint,
			"force_path_style": fmt.Sprintf("%v", adapter.config.ForcePathStyle),
		},
		Schema: schema,
	}

	// Create datastore
	store, err := datastore.NewDataStore(ctx, datastoreConfig)
	if err != nil {
		return errors.Wrap(err, "failed to create S3 datastore")
	}
	defer store.Close()

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

	var processedLedgers uint32
	lastLogTime := time.Now()
	lastLedgerTime := time.Now()

	err = cdp.ApplyLedgerMetadata(
		ledgerRange,
		publisherConfig,
		ctx,
		func(lcm xdr.LedgerCloseMeta) error {
			currentTime := time.Now()
			ledgerProcessingTime := currentTime.Sub(lastLedgerTime)
			lastLedgerTime = currentTime

			log.Printf("Processing ledger %d (took %v since last ledger)",
				lcm.LedgerSequence(), ledgerProcessingTime)

			if err := adapter.processLedger(ctx, lcm); err != nil {
				log.Printf("Error processing ledger %d: %v", lcm.LedgerSequence(), err)
				return err
			}

			processedLedgers++
			if time.Since(lastLogTime) > time.Second*10 {
				rate := float64(processedLedgers) / time.Since(lastLogTime).Seconds()
				log.Printf("Processed %d ledgers (%.2f ledgers/sec)",
					processedLedgers, rate)
				lastLogTime = time.Now()
			}

			return nil
		},
	)

	if err != nil {
		return errors.Wrap(err, "pipeline error")
	}

	duration := time.Since(startTime)
	ledgersPerSecond := float64(processedLedgers) / duration.Seconds()
	log.Printf("Pipeline completed successfully. Processed %d ledgers in %v (%.2f ledgers/sec)",
		processedLedgers, duration, ledgersPerSecond)

	return nil
}

func (adapter *S3BufferedStorageSourceAdapter) processLedger(ctx context.Context, ledger xdr.LedgerCloseMeta) error {
	sequence := ledger.LedgerSequence()
	for _, processor := range adapter.processors {
		if err := processor.Process(ctx, cdpProcessor.Message{Payload: ledger}); err != nil {
			return errors.Wrapf(err, "error processing ledger %d", sequence)
		}
	}
	return nil
}

func (adapter *S3BufferedStorageSourceAdapter) Close() error {
	return nil
}
