package main

import (
	"context"
	"fmt"
	"io"
	"log"
	"strings"
	"time"

	"github.com/pkg/errors"
	"github.com/stellar/go/ingest/cdp"
	"github.com/stellar/go/ingest/ledgerbackend"
	"github.com/stellar/go/support/datastore"
	"github.com/stellar/go/xdr"
	cdpProcessor "github.com/withObsrvr/cdp-pipeline-workflow/processor"
	"github.com/withObsrvr/cdp-pipeline-workflow/utils"
)

// S3BufferedStorageSourceAdapterEnhanced supports time-based configuration
type S3BufferedStorageSourceAdapterEnhanced struct {
	config            S3BufferedStorageConfigEnhanced
	processors        []cdpProcessor.Processor
	schema            datastore.DataStoreSchema
	enhancedConfig    *EnhancedSourceConfig
	continuousProc    *ContinuousLedgerProcessor
	lastProcessedTime time.Time
}

// S3BufferedStorageConfigEnhanced includes both legacy and time-based fields
type S3BufferedStorageConfigEnhanced struct {
	// S3 specific configuration
	BucketName        string
	Region            string
	Endpoint          string
	ForcePathStyle    bool
	BufferSize        uint32
	NumWorkers        uint32
	RetryLimit        uint32
	RetryWait         uint32
	Network           string
	LedgersPerFile    uint32
	FilesPerPartition uint32

	// Enhanced configuration (embedded)
	*EnhancedSourceConfig
}

// NewS3BufferedStorageSourceAdapterEnhanced creates an enhanced S3 adapter with time-based support
func NewS3BufferedStorageSourceAdapterEnhanced(config map[string]interface{}) (SourceAdapter, error) {
	// Parse enhanced configuration first
	enhancedConfig, err := ParseEnhancedConfig(config)
	if err != nil {
		// Fall back to legacy parsing if enhanced parsing fails
		log.Printf("Enhanced config parsing failed, trying legacy mode: %v", err)
		return NewS3BufferedStorageSourceAdapter(config)
	}

	// Parse S3-specific configuration
	bucketName, ok := config["bucket_name"].(string)
	if !ok {
		return nil, errors.New("bucket_name is missing")
	}

	region, ok := config["region"].(string)
	if !ok {
		return nil, errors.New("region must be specified")
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

	ledgersPerFileInt, _ := getIntValue(config["ledgers_per_file"])
	if ledgersPerFileInt == 0 {
		ledgersPerFileInt = 64
	}

	filesPerPartitionInt, _ := getIntValue(config["files_per_partition"])
	if filesPerPartitionInt == 0 {
		filesPerPartitionInt = 10
	}

	schema := datastore.DataStoreSchema{
		LedgersPerFile:    uint32(ledgersPerFileInt),
		FilesPerPartition: uint32(filesPerPartitionInt),
	}

	s3Config := S3BufferedStorageConfigEnhanced{
		BucketName:        bucketName,
		Region:            region,
		Endpoint:          endpoint,
		ForcePathStyle:    forcePathStyle,
		BufferSize:        uint32(bufferSizeInt),
		NumWorkers:        uint32(numWorkersInt),
		RetryLimit:        uint32(retryLimitInt),
		RetryWait:         uint32(retryWaitInt),
		Network:           enhancedConfig.Network,
		LedgersPerFile:    uint32(ledgersPerFileInt),
		FilesPerPartition: uint32(filesPerPartitionInt),
		EnhancedSourceConfig: enhancedConfig,
	}

	adapter := &S3BufferedStorageSourceAdapterEnhanced{
		config:         s3Config,
		schema:         schema,
		enhancedConfig: enhancedConfig,
		processors:     make([]cdpProcessor.Processor, 0),
	}

	// If continuous mode is enabled, create continuous processor
	if enhancedConfig.IsContinuous() {
		continuousProc, err := NewContinuousLedgerProcessor(enhancedConfig)
		if err != nil {
			return nil, fmt.Errorf("failed to create continuous processor: %w", err)
		}
		adapter.continuousProc = continuousProc
	}

	return adapter, nil
}

func (adapter *S3BufferedStorageSourceAdapterEnhanced) Subscribe(processor cdpProcessor.Processor) {
	adapter.processors = append(adapter.processors, processor)
}

func (adapter *S3BufferedStorageSourceAdapterEnhanced) Run(ctx context.Context) error {
	startTime := time.Now()

	// Resolve time-based configuration if needed
	if adapter.enhancedConfig.IsTimeBased() || adapter.enhancedConfig.IsMixedMode() {
		log.Printf("Resolving time-based or mixed mode configuration...")
		if err := adapter.enhancedConfig.Resolve(ctx); err != nil {
			return fmt.Errorf("failed to resolve configuration: %w", err)
		}
	}

	// Get the resolved ledger range
	startLedger, endLedger := adapter.enhancedConfig.GetLedgerRange()

	log.Printf("Starting S3BufferedStorageSourceAdapter from ledger %d", startLedger)
	if adapter.enhancedConfig.IsMixedContinuous() {
		log.Printf("Mixed continuous mode: Will process from ledger %d until ledger %d, then continue with new ledgers", startLedger, endLedger)
	} else if adapter.enhancedConfig.IsMixedMode() {
		log.Printf("Mixed mode: Will process from ledger %d until ledger %d (time-bounded)", startLedger, endLedger)
	} else if endLedger > 0 && !adapter.enhancedConfig.IsContinuous() {
		log.Printf("Will process until ledger %d", endLedger)
	} else if adapter.enhancedConfig.IsContinuous() {
		log.Printf("Will process historical data and continue with new ledgers (continuous mode)")
	} else {
		log.Printf("Will process indefinitely from start ledger")
	}

	// Process initial range
	if err := adapter.processLedgerRange(ctx, startLedger, endLedger); err != nil {
		if err == io.EOF {
			// Expected EOF from rolling window completion, continue to continuous mode
			log.Printf("Rolling window phase completed successfully")
		} else if strings.Contains(err.Error(), "received an error from callback invocation: EOF") {
			// CDP library wrapped EOF, also expected for rolling window completion
			log.Printf("Rolling window phase completed successfully (CDP wrapped EOF)")
		} else {
			return fmt.Errorf("failed to process initial ledger range: %w", err)
		}
	}

	// If continuous mode is enabled, continue processing new ledgers
	if adapter.enhancedConfig.IsContinuous() && adapter.continuousProc != nil {
		if adapter.enhancedConfig.IsMixedContinuous() {
			log.Printf("Entering continuous mode after processing historical range...")
		} else {
			log.Printf("Entering continuous mode...")
		}
		return adapter.processContinuousMode(ctx)
	}

	duration := time.Since(startTime)
	log.Printf("Pipeline completed successfully in %v", duration)
	return nil
}

func (adapter *S3BufferedStorageSourceAdapterEnhanced) processLedgerRange(ctx context.Context, startLedger, endLedger uint32) error {
	var reachedEndLedger bool
	var lastEndLedgerCheck time.Time
	
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

	// Create ledger range
	var ledgerRange ledgerbackend.Range
	if endLedger > 0 && !adapter.enhancedConfig.IsContinuous() {
		// Non-continuous mode with end ledger: use bounded range
		ledgerRange = ledgerbackend.BoundedRange(startLedger, endLedger)
	} else {
		// For continuous mode or rolling windows, use unbounded range
		// and handle stopping in the callback
		ledgerRange = ledgerbackend.UnboundedRange(startLedger)
	}

	log.Printf("Processing ledger range: %v", ledgerRange)

	var processedLedgers uint32
	lastLogTime := time.Now()

	err := cdp.ApplyLedgerMetadata(
		ledgerRange,
		publisherConfig,
		ctx,
		func(lcm xdr.LedgerCloseMeta) error {
			currentTime := time.Now()

			if err := adapter.processLedger(ctx, lcm); err != nil {
				log.Printf("Error processing ledger %d: %v", lcm.LedgerSequence(), err)
				return err
			}

			processedLedgers++
			adapter.lastProcessedTime = currentTime

			if time.Since(lastLogTime) > time.Second*10 {
				rate := float64(processedLedgers) / time.Since(lastLogTime).Seconds()
				log.Printf("Processed %d ledgers (%.2f ledgers/sec)", processedLedgers, rate)
				lastLogTime = time.Now()
			}

			// For rolling window configurations, recalculate end ledger periodically
			var currentEndLedger uint32 = endLedger
			if adapter.enhancedConfig.IsRollingWindow() && (lastEndLedgerCheck.IsZero() || time.Since(lastEndLedgerCheck) > 30*time.Second) {
				if newEndLedger, err := adapter.enhancedConfig.GetCurrentEndLedger(ctx); err == nil {
					currentEndLedger = newEndLedger
					lastEndLedgerCheck = time.Now()
					if newEndLedger != endLedger {
						log.Printf("Rolling window updated: end ledger changed from %d to %d", endLedger, newEndLedger)
					}
				} else {
					log.Printf("Warning: Failed to update rolling window end ledger: %v", err)
				}
			}

			// For mixed mode (with or without continuous), check if we've reached the current end ledger
			if adapter.enhancedConfig.IsMixedMode() && currentEndLedger > 0 && lcm.LedgerSequence() >= currentEndLedger {
				if adapter.enhancedConfig.IsContinuous() {
					log.Printf("Reached end of rolling window at ledger %d (threshold: %d)", lcm.LedgerSequence(), currentEndLedger)
				} else {
					log.Printf("Reached end ledger %d, stopping processing", lcm.LedgerSequence())
				}
				reachedEndLedger = true
				return io.EOF // Return EOF to signal end of this phase
			}

			return nil
		},
	)

	// Handle the end-of-phase EOF specifically for rolling windows
	if err == io.EOF && reachedEndLedger {
		ledgersPerSecond := float64(processedLedgers) / time.Since(lastLogTime).Seconds()
		log.Printf("Processed %d ledgers (%.2f ledgers/sec)", processedLedgers, ledgersPerSecond)
		log.Printf("Successfully completed rolling window phase")
		return io.EOF
	}

	// Handle CDP library's EOF wrapping for rolling window completion
	if err != nil {
		if err == io.EOF {
			return io.EOF
		}
		if strings.Contains(err.Error(), "received an error from callback invocation: EOF") {
			return io.EOF
		}
		return errors.Wrap(err, "pipeline error")
	}

	ledgersPerSecond := float64(processedLedgers) / time.Since(lastLogTime).Seconds()
	log.Printf("Processed %d ledgers (%.2f ledgers/sec)", processedLedgers, ledgersPerSecond)

	return nil
}

// processLedgerRangeForContinuous processes a ledger range in continuous mode without rolling window filtering
func (adapter *S3BufferedStorageSourceAdapterEnhanced) processLedgerRangeForContinuous(ctx context.Context, startLedger, endLedger uint32) error {
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

	// Use bounded range for continuous mode (no rolling window filtering)
	// For single ledger processing, end must be greater than start
	if endLedger == startLedger {
		endLedger = startLedger + 1
	}
	ledgerRange := ledgerbackend.BoundedRange(startLedger, endLedger)
	log.Printf("Processing continuous ledger range: %v", ledgerRange)

	var processedLedgers uint32
	lastLogTime := time.Now()

	err := cdp.ApplyLedgerMetadata(
		ledgerRange,
		publisherConfig,
		ctx,
		func(lcm xdr.LedgerCloseMeta) error {
			currentTime := time.Now()

			if err := adapter.processLedger(ctx, lcm); err != nil {
				log.Printf("Error processing ledger %d: %v", lcm.LedgerSequence(), err)
				return err
			}

			processedLedgers++
			adapter.lastProcessedTime = currentTime

			if time.Since(lastLogTime) > time.Second*10 {
				rate := float64(processedLedgers) / time.Since(lastLogTime).Seconds()
				log.Printf("Processed %d continuous ledgers (%.2f ledgers/sec)", processedLedgers, rate)
				lastLogTime = time.Now()
			}

			// NO rolling window filtering in continuous mode - process all ledgers
			return nil
		},
	)

	if err != nil && err != io.EOF {
		return errors.Wrap(err, "continuous processing error")
	}

	ledgersPerSecond := float64(processedLedgers) / time.Since(lastLogTime).Seconds()
	log.Printf("Processed %d continuous ledgers (%.2f ledgers/sec)", processedLedgers, ledgersPerSecond)
	return nil
}

func (adapter *S3BufferedStorageSourceAdapterEnhanced) processContinuousMode(ctx context.Context) error {
	log.Printf("Starting continuous mode processing...")
	
	// For rolling window configurations, use rolling window continuous mode
	if adapter.enhancedConfig.IsRollingWindow() {
		return adapter.processRollingWindowContinuousMode(ctx)
	}
	
	// For non-rolling window, use standard continuous mode
	for {
		// Check for cancellation
		select {
		case <-ctx.Done():
			log.Printf("Continuous mode cancelled")
			return ctx.Err()
		default:
		}

		// Get next range of ledgers to process
		start, end, hasMore, err := adapter.continuousProc.GetNextLedgerRange(ctx)
		if err != nil {
			log.Printf("Error getting next ledger range: %v", err)
			// Wait before retrying
			if err := adapter.continuousProc.WaitForNewLedgers(ctx); err != nil {
				return err
			}
			continue
		}

		if !hasMore || start > end {
			// No new ledgers, wait
			log.Printf("No new ledgers available, waiting...")
			if err := adapter.continuousProc.WaitForNewLedgers(ctx); err != nil {
				return err
			}
			continue
		}

		// Process the new range (without rolling window filtering in continuous mode)
		log.Printf("Processing new ledgers %d to %d", start, end)
		if err := adapter.processLedgerRangeForContinuous(ctx, start, end); err != nil {
			log.Printf("Error processing ledger range %d-%d: %v", start, end, err)
			// Continue processing despite errors
		}
	}
}

// processRollingWindowContinuousMode handles continuous mode specifically for rolling windows
func (adapter *S3BufferedStorageSourceAdapterEnhanced) processRollingWindowContinuousMode(ctx context.Context) error {
	log.Printf("Starting rolling window continuous mode...")
	
	// Get the current end ledger (last ledger that was processed in the rolling window phase)
	lastProcessedLedger, err := adapter.enhancedConfig.GetCurrentEndLedger(ctx)
	if err != nil {
		log.Printf("Warning: Failed to get current end ledger for rolling window continuous mode: %v", err)
		_, lastProcessedLedger = adapter.enhancedConfig.GetLedgerRange()
	}
	
	log.Printf("Starting rolling window continuous mode from ledger: %d", lastProcessedLedger)

	for {
		// Check for cancellation
		select {
		case <-ctx.Done():
			log.Printf("Rolling window continuous mode cancelled")
			return ctx.Err()
		default:
		}

		// Wait a bit before checking for new ledgers in the rolling window
		time.Sleep(5 * time.Second)
		
		// Calculate what the current "10 days ago" ledger should be
		currentEndLedger, err := adapter.enhancedConfig.GetCurrentEndLedger(ctx)
		if err != nil {
			log.Printf("Error calculating current rolling window end ledger: %v", err)
			continue
		}

		// If the rolling window has moved forward, process the newly available ledger(s)
		if currentEndLedger > lastProcessedLedger {
			nextLedger := lastProcessedLedger + 1
			log.Printf("Rolling window moved: processing newly available ledger %d (new threshold: %d)", nextLedger, currentEndLedger)
			
			// Process just the next ledger that became available
			if err := adapter.processLedgerRangeForContinuous(ctx, nextLedger, nextLedger); err != nil {
				log.Printf("Error processing rolling window ledger %d: %v", nextLedger, err)
				// Continue despite errors
			} else {
				lastProcessedLedger = nextLedger
				log.Printf("Successfully processed rolling window ledger %d", nextLedger)
			}
		} else {
			log.Printf("Rolling window hasn't moved yet (current threshold: %d, last processed: %d)", currentEndLedger, lastProcessedLedger)
		}
	}
}

func (adapter *S3BufferedStorageSourceAdapterEnhanced) processLedger(ctx context.Context, ledger xdr.LedgerCloseMeta) error {
	sequence := ledger.LedgerSequence()
	
	// Create archive metadata for source file provenance
	archiveMetadata := utils.CreateS3ArchiveMetadata(
		adapter.config.BucketName,
		sequence,
		adapter.schema,
	)
	
	// Create message with metadata
	message := utils.CreateMessageWithMetadata(ledger, archiveMetadata)
	
	for _, processor := range adapter.processors {
		if err := processor.Process(ctx, message); err != nil {
			return errors.Wrapf(err, "error processing ledger %d", sequence)
		}
	}
	return nil
}

func (adapter *S3BufferedStorageSourceAdapterEnhanced) Close() error {
	return nil
}