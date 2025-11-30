package main

import (
	"context"
	"fmt"
	"log"
	"path/filepath"
	"strings"
	"time"

	"github.com/withObsrvr/cdp-pipeline-workflow/internal/bronze"
	cdpProcessor "github.com/withObsrvr/cdp-pipeline-workflow/processor"
	"github.com/withobsrvr/flowctl/pkg/console/heartbeat"
)

// BronzeSourceAdapter reads from Bronze layer parquet files produced by Bronze Copier.
// It extracts XDR bytes from parquet and emits xdr.LedgerCloseMeta to subscribers.
type BronzeSourceAdapter struct {
	config          BronzeSourceConfig
	reader          *bronze.ParquetReader
	processors      []cdpProcessor.Processor
	heartbeatClient *heartbeat.Client // Console heartbeat for billing tracking
}

// BronzeSourceConfig contains configuration for the Bronze Source Adapter.
type BronzeSourceConfig struct {
	// Storage backend: "local", "gcs", "s3"
	StorageType string

	// Local filesystem
	LocalPath string // Base path to Bronze output

	// GCS settings
	GCSBucket string
	GCSPrefix string

	// GCS HMAC credentials (for S3 compatibility mode)
	GCSAccessKeyID     string
	GCSSecretAccessKey string

	// S3 settings
	S3Bucket           string
	S3Prefix           string
	S3Region           string
	S3Endpoint         string
	AWSAccessKeyID     string
	AWSSecretAccessKey string

	// Bronze data organization
	Network       string // "mainnet" or "testnet"
	BronzeVersion string // "v1"
	EraID         string // "pre_p23", "p23_plus"

	// Ledger range
	StartLedger   uint32
	EndLedger     uint32 // 0 = unbounded (process all available)
	PartitionSize uint32 // Ledgers per partition (default: 10000)

	// Multi-file partition support (new Bronze Copier format)
	// When enabled, reads manifest to discover individual parquet files
	UseManifest bool
}

// NewBronzeSourceAdapter creates a new Bronze Source Adapter.
func NewBronzeSourceAdapter(config map[string]interface{}) (SourceAdapter, error) {
	cfg := BronzeSourceConfig{
		StorageType:        getStringConfigBronze(config, "storage_type", "local"),
		LocalPath:          getStringConfigBronze(config, "local_path", ""),
		GCSBucket:          getStringConfigBronze(config, "gcs_bucket", ""),
		GCSPrefix:          getStringConfigBronze(config, "gcs_prefix", ""),
		GCSAccessKeyID:     getStringConfigBronze(config, "gcs_access_key_id", ""),
		GCSSecretAccessKey: getStringConfigBronze(config, "gcs_secret_access_key", ""),
		S3Bucket:           getStringConfigBronze(config, "s3_bucket", ""),
		S3Prefix:           getStringConfigBronze(config, "s3_prefix", ""),
		S3Region:           getStringConfigBronze(config, "s3_region", "us-east-1"),
		S3Endpoint:         getStringConfigBronze(config, "s3_endpoint", ""),
		AWSAccessKeyID:     getStringConfigBronze(config, "aws_access_key_id", ""),
		AWSSecretAccessKey: getStringConfigBronze(config, "aws_secret_access_key", ""),
		Network:            getStringConfigBronze(config, "network", "mainnet"),
		BronzeVersion:      getStringConfigBronze(config, "bronze_version", "v1"),
		EraID:              getStringConfigBronze(config, "era_id", ""),
		StartLedger:        uint32(getIntConfigBronze(config, "start_ledger", 0)),
		EndLedger:          uint32(getIntConfigBronze(config, "end_ledger", 0)),
		PartitionSize:      uint32(getIntConfigBronze(config, "partition_size", 10000)),
		UseManifest:        getBoolConfigBronze(config, "use_manifest", false),
	}

	// Validate required fields
	if cfg.StartLedger == 0 {
		return nil, fmt.Errorf("start_ledger is required")
	}

	// Validate storage configuration
	switch cfg.StorageType {
	case "local":
		if cfg.LocalPath == "" {
			return nil, fmt.Errorf("local_path is required when storage_type is 'local'")
		}
	case "gcs":
		if cfg.GCSBucket == "" {
			return nil, fmt.Errorf("gcs_bucket is required when storage_type is 'gcs'")
		}
	case "s3":
		if cfg.S3Bucket == "" {
			return nil, fmt.Errorf("s3_bucket is required when storage_type is 's3'")
		}
	default:
		return nil, fmt.Errorf("unsupported storage_type: %s (use 'local', 'gcs', or 's3')", cfg.StorageType)
	}

	// Create parquet reader
	reader, err := bronze.NewParquetReader()
	if err != nil {
		return nil, fmt.Errorf("create parquet reader: %w", err)
	}

	log.Printf("BronzeSourceAdapter: Created with storage_type=%s, network=%s, version=%s, era=%s, range=%d-%d",
		cfg.StorageType, cfg.Network, cfg.BronzeVersion, cfg.EraID, cfg.StartLedger, cfg.EndLedger)

	adapter := &BronzeSourceAdapter{
		config: cfg,
		reader: reader,
	}

	// Initialize console heartbeat client
	adapter.heartbeatClient = initializeHeartbeatClient()

	return adapter, nil
}

// Subscribe adds a processor to receive ledger data.
func (a *BronzeSourceAdapter) Subscribe(processor cdpProcessor.Processor) {
	a.processors = append(a.processors, processor)
}

// Run starts reading from Bronze parquet files and emitting to subscribers.
func (a *BronzeSourceAdapter) Run(ctx context.Context) error {
	log.Printf("BronzeSourceAdapter: Starting, range=%d-%d, use_manifest=%v", a.config.StartLedger, a.config.EndLedger, a.config.UseManifest)

	// Configure storage backend
	if err := a.configureStorage(ctx); err != nil {
		return fmt.Errorf("configure storage: %w", err)
	}

	// Discover partition directories for the range
	partitionDirs, err := a.discoverPartitions()
	if err != nil {
		return fmt.Errorf("discover partitions: %w", err)
	}

	if len(partitionDirs) == 0 {
		return fmt.Errorf("no partitions found for ledger range %d-%d", a.config.StartLedger, a.config.EndLedger)
	}

	log.Printf("BronzeSourceAdapter: Found %d partitions to process", len(partitionDirs))

	processedCount := 0
	startTime := time.Now()

	// Process each partition
	for _, partitionDir := range partitionDirs {
		// Get list of parquet files in this partition
		parquetFiles, err := a.discoverFilesInPartition(ctx, partitionDir)
		if err != nil {
			log.Printf("BronzeSourceAdapter: Failed to discover files in partition %s: %v, skipping", partitionDir, err)
			continue
		}

		if len(parquetFiles) == 0 {
			log.Printf("BronzeSourceAdapter: No files found in partition %s, skipping", partitionDir)
			continue
		}

		log.Printf("BronzeSourceAdapter: Processing partition %s (%d files)", partitionDir, len(parquetFiles))

		// Process each parquet file in the partition
		for _, parquetPath := range parquetFiles {
			ledgerCh, errCh := a.reader.ReadLedgers(ctx, parquetPath, a.config.StartLedger, a.config.EndLedger)

			fileHadData := false
			for lcm := range ledgerCh {
				fileHadData = true
				// Create message with LedgerCloseMeta payload
				msg := cdpProcessor.Message{
					Payload: lcm,
					Metadata: map[string]interface{}{
						"source":         "bronze",
						"bronze_version": a.config.BronzeVersion,
						"network":        a.config.Network,
						"era_id":         a.config.EraID,
					},
				}

				// Emit to all subscribers
				for _, proc := range a.processors {
					if err := proc.Process(ctx, msg); err != nil {
						return fmt.Errorf("process ledger %d: %w", lcm.LedgerSequence(), err)
					}
				}

				processedCount++

				// Update console heartbeat
				if a.heartbeatClient != nil {
					a.heartbeatClient.SetLedgerCount(int64(processedCount))
				}

				if processedCount%1000 == 0 {
					elapsed := time.Since(startTime)
					rate := float64(processedCount) / elapsed.Seconds()
					log.Printf("BronzeSourceAdapter: Processed %d ledgers (%.2f ledgers/sec)",
						processedCount, rate)
				}
			}

			// Check for errors from the reader
			select {
			case err := <-errCh:
				if err != nil {
					// If no data was read and there's an error, the file might not exist
					if !fileHadData {
						log.Printf("BronzeSourceAdapter: File %s not found or empty, skipping", parquetPath)
						continue
					}
					return fmt.Errorf("read file %s: %w", parquetPath, err)
				}
			default:
			}
		}
	}

	elapsed := time.Since(startTime)
	rate := float64(processedCount) / elapsed.Seconds()
	log.Printf("BronzeSourceAdapter: Complete, processed %d ledgers in %v (%.2f ledgers/sec)",
		processedCount, elapsed, rate)

	return nil
}

// discoverFilesInPartition returns the list of parquet file paths in a partition.
// If use_manifest is enabled, it reads the manifest to discover files.
// Otherwise, it returns a single file with the legacy naming convention.
func (a *BronzeSourceAdapter) discoverFilesInPartition(ctx context.Context, partitionDir string) ([]string, error) {
	if a.config.UseManifest {
		// Read manifest to discover files
		manifestPath := partitionDir + "/_manifest.json"
		manifest, err := bronze.ReadManifest(ctx, manifestPath)
		if err != nil {
			return nil, fmt.Errorf("read manifest: %w", err)
		}

		// Check if it's a multi-file manifest
		files := manifest.GetLedgerFiles(a.config.StartLedger, a.config.EndLedger)
		if files != nil && len(files) > 0 {
			// Multi-file partition
			var paths []string
			for _, f := range files {
				paths = append(paths, partitionDir+"/"+f.File)
			}
			log.Printf("BronzeSourceAdapter: Manifest lists %d files for range %d-%d",
				len(paths), a.config.StartLedger, a.config.EndLedger)
			return paths, nil
		}

		// Legacy single-file manifest - fall through to default behavior
		log.Printf("BronzeSourceAdapter: Legacy manifest, using single-file mode")
	}

	// Legacy: single parquet file per partition
	// Extract partition range from directory name (e.g., "range=3-10002")
	// and construct the file path
	partStart, partEnd, err := a.parsePartitionRange(partitionDir)
	if err != nil {
		return nil, fmt.Errorf("parse partition range: %w", err)
	}

	fileName := fmt.Sprintf("part-%d-%d.parquet", partStart, partEnd)
	return []string{partitionDir + "/" + fileName}, nil
}

// parsePartitionRange extracts start and end ledger from partition directory path.
// Expects path ending with "range=X-Y" or "range=X-Y/".
func (a *BronzeSourceAdapter) parsePartitionRange(partitionDir string) (uint32, uint32, error) {
	// Find "range=" in the path
	idx := strings.Index(partitionDir, "range=")
	if idx == -1 {
		return 0, 0, fmt.Errorf("no range= found in partition path: %s", partitionDir)
	}

	rangePart := partitionDir[idx+6:] // Skip "range="
	// Remove trailing slash if present
	rangePart = strings.TrimSuffix(rangePart, "/")

	// Parse "start-end"
	var start, end uint32
	_, err := fmt.Sscanf(rangePart, "%d-%d", &start, &end)
	if err != nil {
		return 0, 0, fmt.Errorf("parse range from %s: %w", rangePart, err)
	}

	return start, end, nil
}

// configureStorage configures the parquet reader for the selected storage backend.
func (a *BronzeSourceAdapter) configureStorage(ctx context.Context) error {
	switch a.config.StorageType {
	case "local":
		// No configuration needed for local filesystem
		return nil
	case "gcs":
		if a.config.GCSAccessKeyID != "" && a.config.GCSSecretAccessKey != "" {
			return a.reader.ConfigureGCSWithHMAC(ctx, a.config.GCSAccessKeyID, a.config.GCSSecretAccessKey)
		}
		return a.reader.ConfigureGCS(ctx)
	case "s3":
		return a.reader.ConfigureS3(ctx, a.config.AWSAccessKeyID, a.config.AWSSecretAccessKey, a.config.S3Region, a.config.S3Endpoint)
	default:
		return fmt.Errorf("unsupported storage type: %s", a.config.StorageType)
	}
}

// discoverPartitions finds all partition directories for the configured ledger range.
// Returns paths to partition directories (not individual files).
//
// Bronze Copier output structure:
//
//	Legacy: {base}/ledgers_lcm_raw/range={start}-{end}/part-{start}-{end}.parquet
//	Multi-file: {base}/ledgers_lcm_raw/range={start}-{end}/_manifest.json + part-X-Y.parquet files
//
// Note: Bronze Copier partitions may not start at ledger 0. For example, testnet
// starts at ledger 3, so the first partition is range=3-10002.
func (a *BronzeSourceAdapter) discoverPartitions() ([]string, error) {
	var paths []string

	partitionSize := a.config.PartitionSize
	if partitionSize == 0 {
		partitionSize = 10000
	}

	endLedger := a.config.EndLedger
	if endLedger == 0 {
		// Unbounded: use a large upper bound to discover all available partitions
		// The code handles missing partitions gracefully by skipping them
		endLedger = 100_000_000
	}

	// Try standard partition alignment (0-based)
	startPartition := (a.config.StartLedger / partitionSize) * partitionSize
	endPartition := ((endLedger / partitionSize) + 1) * partitionSize - 1

	// Also try offset-based alignment (for networks starting at ledger 3)
	// Bronze Copier testnet uses: 3-10002, 10003-20002, etc.
	networkOffset := uint32(3) // Testnet starts at ledger 3
	startPartitionOffset := ((a.config.StartLedger - networkOffset) / partitionSize) * partitionSize + networkOffset
	endPartitionOffset := ((endLedger - networkOffset) / partitionSize + 1) * partitionSize + networkOffset - 1

	// Generate partition starts for both alignments
	partitionStarts := []uint32{}

	// Standard alignment
	for partStart := startPartition; partStart <= endPartition; partStart += partitionSize {
		partitionStarts = append(partitionStarts, partStart)
	}

	// Offset alignment (if different)
	for partStart := startPartitionOffset; partStart <= endPartitionOffset; partStart += partitionSize {
		found := false
		for _, ps := range partitionStarts {
			if ps == partStart {
				found = true
				break
			}
		}
		if !found {
			partitionStarts = append(partitionStarts, partStart)
		}
	}

	// Generate partition directory paths (not file paths)
	for _, partStart := range partitionStarts {
		partEnd := partStart + partitionSize - 1

		var path string
		switch a.config.StorageType {
		case "local":
			basePath := a.config.LocalPath
			if a.config.EraID != "" {
				basePath = filepath.Join(basePath, a.config.Network, a.config.EraID, a.config.BronzeVersion)
			}
			path = filepath.Join(basePath, "ledgers_lcm_raw",
				fmt.Sprintf("range=%d-%d", partStart, partEnd))

		case "gcs":
			prefix := strings.TrimSuffix(a.config.GCSPrefix, "/")
			if prefix == "" {
				prefix = fmt.Sprintf("bronze/%s", a.config.Network)
			}
			// Append era_id and bronze_version if provided
			if a.config.EraID != "" {
				prefix = prefix + "/" + a.config.EraID
			}
			if a.config.BronzeVersion != "" {
				prefix = prefix + "/" + a.config.BronzeVersion
			}
			path = fmt.Sprintf("gs://%s/%s/ledgers_lcm_raw/range=%d-%d",
				a.config.GCSBucket, prefix, partStart, partEnd)

		case "s3":
			prefix := strings.TrimSuffix(a.config.S3Prefix, "/")
			if prefix == "" {
				prefix = fmt.Sprintf("bronze/%s", a.config.Network)
			}
			// Append era_id and bronze_version if provided
			if a.config.EraID != "" {
				prefix = prefix + "/" + a.config.EraID
			}
			if a.config.BronzeVersion != "" {
				prefix = prefix + "/" + a.config.BronzeVersion
			}
			path = fmt.Sprintf("s3://%s/%s/ledgers_lcm_raw/range=%d-%d",
				a.config.S3Bucket, prefix, partStart, partEnd)
		}

		paths = append(paths, path)
	}

	return paths, nil
}

// Close closes the parquet reader.
func (a *BronzeSourceAdapter) Close() error {
	if a.reader != nil {
		return a.reader.Close()
	}
	return nil
}

// Helper functions for config parsing
func getStringConfigBronze(config map[string]interface{}, key, defaultValue string) string {
	if v, ok := config[key].(string); ok {
		return v
	}
	return defaultValue
}

func getIntConfigBronze(config map[string]interface{}, key string, defaultValue int) int {
	switch v := config[key].(type) {
	case int:
		return v
	case int64:
		return int(v)
	case float64:
		return int(v)
	}
	return defaultValue
}

func getBoolConfigBronze(config map[string]interface{}, key string, defaultValue bool) bool {
	if v, ok := config[key].(bool); ok {
		return v
	}
	return defaultValue
}
