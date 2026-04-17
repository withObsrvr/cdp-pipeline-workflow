package main

import (
	"context"
	"fmt"
	"io"
	"log"
	"os"
	"strings"
	"sync/atomic"
	"time"

	"github.com/pkg/errors"
	"github.com/stellar/go/ingest/cdp"
	"github.com/stellar/go/ingest/ledgerbackend"
	"github.com/stellar/go/support/datastore"
	"github.com/stellar/go/xdr"
	"github.com/withObsrvr/cdp-pipeline-workflow/pkg/checkpoint"
	cdpProcessor "github.com/withObsrvr/cdp-pipeline-workflow/processor"
	"github.com/withObsrvr/cdp-pipeline-workflow/utils"
	"github.com/withobsrvr/flowctl/pkg/console/heartbeat"
)

// BufferedStorageSourceAdapterEnhanced supports time-based configuration for GCS
type BufferedStorageSourceAdapterEnhanced struct {
	config            BufferedStorageConfigEnhanced
	processors        []cdpProcessor.Processor
	schema            datastore.DataStoreSchema
	enhancedConfig    *EnhancedSourceConfig
	continuousProc    *ContinuousLedgerProcessor
	lastProcessedTime time.Time

	// Checkpointing support
	checkpointMgr   *checkpoint.Manager
	currentLedger   atomic.Uint32 // Thread-safe current ledger tracking
	totalProcessed  atomic.Uint64
	totalErrors     atomic.Uint64
	heartbeatClient *heartbeat.Client // Console heartbeat for billing tracking
}

// BufferedStorageConfigEnhanced includes both legacy and time-based fields
type BufferedStorageConfigEnhanced struct {
	// GCS specific configuration
	BucketName        string
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

// NewBufferedStorageSourceAdapterEnhancedWithCheckpoint creates an enhanced buffered storage adapter with checkpointing support
func NewBufferedStorageSourceAdapterEnhancedWithCheckpoint(config map[string]interface{}, checkpointDir string) (SourceAdapter, error) {
	adapter, err := newBufferedStorageSourceAdapterEnhancedInternal(config)
	if err != nil {
		return nil, err
	}

	// Create checkpoint manager if directory provided
	if checkpointDir != "" {
		pipelineID := os.Getenv("PIPELINE_ID")
		if pipelineID == "" {
			pipelineID = "unknown"
		}

		// Extract team slug from checkpoint directory path
		// Expected format: /checkpoints/{team_slug}/{pipeline_id}
		teamSlug := extractTeamSlugFromPath(checkpointDir)
		if teamSlug == "" {
			teamSlug = "default-team"
		}

		pipelineName := fmt.Sprintf("buffered-storage-%s", pipelineID)

		mgr, err := checkpoint.NewManager(checkpointDir, pipelineID, teamSlug, pipelineName, config)
		if err != nil {
			log.Printf("[WARN] Failed to create checkpoint manager: %v (continuing without checkpointing)", err)
		} else {
			adapter.checkpointMgr = mgr
			log.Printf("[INFO] Checkpoint manager enabled for pipeline %s/%s", teamSlug, pipelineID)
		}
	}

	// Initialize console heartbeat client
	adapter.heartbeatClient = initializeHeartbeatClient()

	return adapter, nil
}

// extractTeamSlugFromPath extracts team slug from checkpoint directory path
// Expected format: /checkpoints/{team_slug}/{pipeline_id}
func extractTeamSlugFromPath(path string) string {
	parts := strings.Split(strings.Trim(path, "/"), "/")
	if len(parts) >= 2 {
		// Return second-to-last part (team slug)
		return parts[len(parts)-2]
	}
	return ""
}

// NewBufferedStorageSourceAdapterEnhanced creates an enhanced buffered storage adapter with time-based support (legacy, no checkpointing)
func NewBufferedStorageSourceAdapterEnhanced(config map[string]interface{}) (SourceAdapter, error) {
	return newBufferedStorageSourceAdapterEnhancedInternal(config)
}

// newBufferedStorageSourceAdapterEnhancedInternal is the internal constructor
func newBufferedStorageSourceAdapterEnhancedInternal(config map[string]interface{}) (*BufferedStorageSourceAdapterEnhanced, error) {
	// Parse enhanced configuration first
	enhancedConfig, err := ParseEnhancedConfig(config)
	if err != nil {
		// Fall back to legacy parsing if enhanced parsing fails
		log.Printf("Enhanced config parsing failed, cannot create enhanced adapter: %v", err)
		return nil, fmt.Errorf("enhanced config parsing failed: %w", err)
	}

	// Parse BufferedStorage-specific configuration
	bucketName, ok := config["bucket_name"].(string)
	if !ok {
		return nil, errors.New("bucket_name is missing")
	}

	// Optional configuration values with defaults
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

	bufferConfig := BufferedStorageConfigEnhanced{
		BucketName:        bucketName,
		BufferSize:        uint32(bufferSizeInt),
		NumWorkers:        uint32(numWorkersInt),
		RetryLimit:        uint32(retryLimitInt),
		RetryWait:         uint32(retryWaitInt),
		Network:           enhancedConfig.Network,
		LedgersPerFile:    uint32(ledgersPerFileInt),
		FilesPerPartition: uint32(filesPerPartitionInt),
		EnhancedSourceConfig: enhancedConfig,
	}

	adapter := &BufferedStorageSourceAdapterEnhanced{
		config:         bufferConfig,
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

func (adapter *BufferedStorageSourceAdapterEnhanced) Subscribe(processor cdpProcessor.Processor) {
	adapter.processors = append(adapter.processors, processor)
}

func (adapter *BufferedStorageSourceAdapterEnhanced) Run(ctx context.Context) error {
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

	// CHECKPOINTING: Load checkpoint and override start ledger if found
	if adapter.checkpointMgr != nil {
		if cp, err := adapter.checkpointMgr.LoadCheckpoint(); err == nil {
			log.Printf("[INFO] Found checkpoint from %s", cp.CheckpointTimestamp.Format(time.RFC3339))
			log.Printf("[INFO] Resuming from ledger %d", cp.LastProcessedLedger+1)
			startLedger = cp.LastProcessedLedger + 1
			// Update config to reflect resumed ledger
			adapter.enhancedConfig.ResolvedStartLedger = startLedger
		} else {
			log.Printf("[INFO] No checkpoint found, starting from config start_ledger: %d", startLedger)
		}

		// Start auto-checkpoint in background
		getState := func() uint32 {
			return adapter.currentLedger.Load()
		}
		go adapter.checkpointMgr.StartAutoCheckpoint(ctx, getState)
	}

	log.Printf("Starting BufferedStorageSourceAdapter from ledger %d", startLedger)
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
	if adapter.enhancedConfig.IsContinuous() {
		if adapter.continuousProc == nil {
			log.Printf("ERROR: Continuous mode enabled but continuousProc is nil")
			return fmt.Errorf("continuous processor not initialized")
		}
		
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

func (adapter *BufferedStorageSourceAdapterEnhanced) processLedgerRange(ctx context.Context, startLedger, endLedger uint32) error {
	var reachedEndLedger bool
	var lastEndLedgerCheck time.Time
	
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
	log.Printf("DataStore config: Type=%s, Params=%+v", dataStoreConfig.Type, dataStoreConfig.Params)
	log.Printf("Schema: LedgersPerFile=%d, FilesPerPartition=%d", schema.LedgersPerFile, schema.FilesPerPartition)

	var processedLedgers uint32
	lastLogTime := time.Now()

	log.Printf("Calling ApplyLedgerMetadata...")
	log.Printf("Creating datastore with config: %+v", dataStoreConfig)
	
	// Try to create the datastore first to check for errors
	store, err := datastore.NewDataStore(ctx, dataStoreConfig)
	if err != nil {
		return errors.Wrap(err, "failed to create datastore")
	}
	log.Printf("DataStore created successfully: %T", store)
	
	err = cdp.ApplyLedgerMetadata(
		ledgerRange,
		publisherConfig,
		ctx,
		func(lcm xdr.LedgerCloseMeta) error {
			ledgerSeq := lcm.LedgerSequence()
			log.Printf("Processing ledger %d", ledgerSeq)
			currentTime := time.Now()

			// CHECKPOINTING: Update current ledger
			if adapter.checkpointMgr != nil {
				adapter.currentLedger.Store(ledgerSeq)
				adapter.totalProcessed.Add(1)

				// Update console heartbeat
				if adapter.heartbeatClient != nil {
					adapter.heartbeatClient.SetLedgerCount(int64(adapter.totalProcessed.Load()))
				}
			}

			if err := adapter.processLedger(ctx, lcm); err != nil {
				log.Printf("Error processing ledger %d: %v", ledgerSeq, err)
				// CHECKPOINTING: Track errors
				if adapter.checkpointMgr != nil {
					adapter.totalErrors.Add(1)
					adapter.checkpointMgr.UpdateStats(adapter.totalProcessed.Load(), adapter.totalErrors.Load())
				}
				return err
			}

			processedLedgers++
			adapter.lastProcessedTime = currentTime

			// CHECKPOINTING: Update stats periodically
			if adapter.checkpointMgr != nil && processedLedgers%100 == 0 {
				adapter.checkpointMgr.UpdateStats(adapter.totalProcessed.Load(), adapter.totalErrors.Load())
			}

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
	
	log.Printf("ApplyLedgerMetadata returned: %v", err)

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
func (adapter *BufferedStorageSourceAdapterEnhanced) processLedgerRangeForContinuous(ctx context.Context, startLedger, endLedger uint32) error {
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
			ledgerSeq := lcm.LedgerSequence()
			currentTime := time.Now()

			// CHECKPOINTING: Update current ledger
			if adapter.checkpointMgr != nil {
				adapter.currentLedger.Store(ledgerSeq)
				adapter.totalProcessed.Add(1)

				// Update console heartbeat
				if adapter.heartbeatClient != nil {
					adapter.heartbeatClient.SetLedgerCount(int64(adapter.totalProcessed.Load()))
				}
			}

			if err := adapter.processLedger(ctx, lcm); err != nil {
				log.Printf("Error processing ledger %d: %v", ledgerSeq, err)
				// CHECKPOINTING: Track errors
				if adapter.checkpointMgr != nil {
					adapter.totalErrors.Add(1)
					adapter.checkpointMgr.UpdateStats(adapter.totalProcessed.Load(), adapter.totalErrors.Load())
				}
				return err
			}

			processedLedgers++
			adapter.lastProcessedTime = currentTime

			// CHECKPOINTING: Update stats periodically
			if adapter.checkpointMgr != nil && processedLedgers%100 == 0 {
				adapter.checkpointMgr.UpdateStats(adapter.totalProcessed.Load(), adapter.totalErrors.Load())
			}

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

func (adapter *BufferedStorageSourceAdapterEnhanced) processContinuousMode(ctx context.Context) error {
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
func (adapter *BufferedStorageSourceAdapterEnhanced) processRollingWindowContinuousMode(ctx context.Context) error {
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

func (adapter *BufferedStorageSourceAdapterEnhanced) processLedger(ctx context.Context, ledger xdr.LedgerCloseMeta) error {
	sequence := ledger.LedgerSequence()
	
	// Create archive metadata for source file provenance
	archiveMetadata := utils.CreateGCSArchiveMetadata(
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

func (adapter *BufferedStorageSourceAdapterEnhanced) Close() error {
	return nil
}