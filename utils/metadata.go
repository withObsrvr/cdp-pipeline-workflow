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

// CreateArchiveMetadata creates standardized archive metadata for a given ledger
func CreateArchiveMetadata(sourceType, bucketName string, ledgerSeq uint32, schema datastore.DataStoreSchema) *cdpProcessor.ArchiveSourceMetadata {
	filePath, startLedger, endLedger := CalculateArchiveFilePath(
		ledgerSeq, 
		uint32(schema.LedgersPerFile), 
		uint32(schema.FilesPerPartition))
	
	fileName := fmt.Sprintf("ledger-%d-%d.xdr", startLedger, endLedger)
	partitionNum := startLedger / (uint32(schema.LedgersPerFile) * uint32(schema.FilesPerPartition))
	
	return &cdpProcessor.ArchiveSourceMetadata{
		SourceType:   sourceType,
		BucketName:   bucketName,
		FilePath:     filePath,
		FileName:     fileName,
		StartLedger:  startLedger,
		EndLedger:    endLedger,
		ProcessedAt:  time.Now(),
		Partition:    partitionNum,
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