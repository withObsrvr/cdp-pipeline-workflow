package utils

import (
	"fmt"
	"time"

	"github.com/stellar/go/support/datastore"
	cdpProcessor "github.com/withObsrvr/cdp-pipeline-workflow/processor"
)

// CalculateArchiveFilePath determines the archive file path for a given ledger sequence
// based on the datastore schema configuration
func CalculateArchiveFilePath(ledgerSeq uint32, ledgersPerFile, filesPerPartition uint32) (string, uint32, uint32) {
	startLedger := (ledgerSeq / ledgersPerFile) * ledgersPerFile
	endLedger := startLedger + ledgersPerFile - 1
	partitionNum := startLedger / (ledgersPerFile * filesPerPartition)
	
	fileName := fmt.Sprintf("ledger-%d-%d.xdr", startLedger, endLedger)
	filePath := fmt.Sprintf("partition-%d/%s", partitionNum, fileName)
	
	return filePath, startLedger, endLedger
}

// CalculateHistoryArchivePath calculates the Stellar History Archive path format using schema parameters
// Example: "FFF82FFF--512000-575999/FFF7640A--564213.xdr.zstd"
func CalculateHistoryArchivePath(ledgerSeq uint32, ledgersPerFile, filesPerPartition uint32) (string, uint32, uint32) {
	// Use the actual schema configuration for archive structure
	// Each "file" contains ledgersPerFile ledgers
	// Each "partition/directory" contains filesPerPartition files
	
	// Calculate which file this ledger belongs to
	fileStartLedger := (ledgerSeq / ledgersPerFile) * ledgersPerFile
	fileEndLedger := fileStartLedger + ledgersPerFile - 1
	
	// Calculate which partition/directory this file belongs to
	partitionSize := ledgersPerFile * filesPerPartition
	partitionStartLedger := (ledgerSeq / partitionSize) * partitionSize
	partitionEndLedger := partitionStartLedger + partitionSize - 1
	
	// Calculate the directory hex value (based on partition start, using bitwise NOT/inversion)
	dirHex := fmt.Sprintf("%08X", ^partitionStartLedger)
	
	// Calculate the file hex value (based on file end ledger, using bitwise NOT/inversion)
	fileHex := fmt.Sprintf("%08X", ^fileEndLedger)
	
	// Format: DIRHEX--partition-start-partition-end/FILEHEX--file-end.xdr.zstd
	dirName := fmt.Sprintf("%s--%d-%d", dirHex, partitionStartLedger, partitionEndLedger)
	fileName := fmt.Sprintf("%s--%d.xdr.zstd", fileHex, fileEndLedger)
	historyPath := fmt.Sprintf("%s/%s", dirName, fileName)
	
	return historyPath, fileStartLedger, fileEndLedger
}

// GenerateCloudStorageURL generates the full cloud storage URL
func GenerateCloudStorageURL(sourceType, bucketName, historyPath string) string {
	switch sourceType {
	case "GCS":
		return fmt.Sprintf("https://storage.googleapis.com/%s/%s", bucketName, historyPath)
	case "S3":
		// For S3, the URL format depends on the region and endpoint configuration
		// This is a basic format - real implementation might need region/endpoint info
		return fmt.Sprintf("https://%s.s3.amazonaws.com/%s", bucketName, historyPath)
	default:
		return ""
	}
}

// CreateArchiveMetadata creates standardized archive metadata for a given ledger
func CreateArchiveMetadata(sourceType, bucketName string, ledgerSeq uint32, schema datastore.DataStoreSchema) *cdpProcessor.ArchiveSourceMetadata {
	// Calculate datastore schema path
	filePath, startLedger, endLedger := CalculateArchiveFilePath(
		ledgerSeq, 
		uint32(schema.LedgersPerFile), 
		uint32(schema.FilesPerPartition))
	
	fileName := fmt.Sprintf("ledger-%d-%d.xdr", startLedger, endLedger)
	partitionNum := startLedger / (uint32(schema.LedgersPerFile) * uint32(schema.FilesPerPartition))
	
	// Calculate history archive path (for actual cloud storage files)
	historyPath, _, _ := CalculateHistoryArchivePath(ledgerSeq, uint32(schema.LedgersPerFile), uint32(schema.FilesPerPartition))
	
	// Generate full cloud storage URL
	fullCloudURL := GenerateCloudStorageURL(sourceType, bucketName, historyPath)
	
	return &cdpProcessor.ArchiveSourceMetadata{
		SourceType:         sourceType,
		BucketName:         bucketName,
		FilePath:           filePath,
		FileName:           fileName,
		StartLedger:        startLedger,
		EndLedger:          endLedger,
		ProcessedAt:        time.Now(),
		Partition:          partitionNum,
		HistoryArchivePath: historyPath,
		FullCloudURL:       fullCloudURL,
	}
}

// CreateS3ArchiveMetadata creates S3-specific archive metadata
func CreateS3ArchiveMetadata(bucketName string, ledgerSeq uint32, schema datastore.DataStoreSchema) *cdpProcessor.ArchiveSourceMetadata {
	return CreateArchiveMetadata("S3", bucketName, ledgerSeq, schema)
}

// CreateGCSArchiveMetadata creates GCS-specific archive metadata
func CreateGCSArchiveMetadata(bucketName string, ledgerSeq uint32, schema datastore.DataStoreSchema) *cdpProcessor.ArchiveSourceMetadata {
	return CreateArchiveMetadata("GCS", bucketName, ledgerSeq, schema)
}

// CreateFSArchiveMetadata creates filesystem-specific archive metadata (deprecated)
func CreateFSArchiveMetadata(basePath string, ledgerSeq uint32, schema datastore.DataStoreSchema) *cdpProcessor.ArchiveSourceMetadata {
	metadata := CreateArchiveMetadata("FS", "", ledgerSeq, schema)
	metadata.BucketName = basePath // Use BucketName field for filesystem base path
	return metadata
}

// InjectArchiveMetadata adds archive metadata to a message
func InjectArchiveMetadata(msg *cdpProcessor.Message, archiveMetadata *cdpProcessor.ArchiveSourceMetadata) {
	if msg.Metadata == nil {
		msg.Metadata = make(map[string]interface{})
	}
	msg.Metadata["archive_source"] = archiveMetadata
}

// CreateMessageWithMetadata creates a new message with archive metadata
func CreateMessageWithMetadata(payload interface{}, archiveMetadata *cdpProcessor.ArchiveSourceMetadata) cdpProcessor.Message {
	msg := cdpProcessor.Message{
		Payload:  payload,
		Metadata: make(map[string]interface{}),
	}
	InjectArchiveMetadata(&msg, archiveMetadata)
	return msg
}