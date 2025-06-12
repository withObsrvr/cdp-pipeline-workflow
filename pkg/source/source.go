package source

import (
	"context"

	"github.com/withObsrvr/cdp-pipeline-workflow/pkg/common/types"
	"github.com/withObsrvr/cdp-pipeline-workflow/pkg/source/base"
	stellarrpc "github.com/withObsrvr/cdp-pipeline-workflow/pkg/source/stellar-rpc"
)

// BaseSourceAdapter provides a base implementation of the SourceAdapter interface
type BaseSourceAdapter struct {
	processors []types.Processor
}

// Subscribe adds a processor to this source adapter
func (s *BaseSourceAdapter) Subscribe(p types.Processor) {
	s.processors = append(s.processors, p)
}

// Run starts the source adapter
func (s *BaseSourceAdapter) Run(ctx context.Context) error {
	// Placeholder implementation
	<-ctx.Done()
	return nil
}

// CaptiveCoreInboundAdapter is a source adapter for Captive Core
type CaptiveCoreInboundAdapter struct {
	BaseSourceAdapter
	config map[string]interface{}
}

// NewCaptiveCoreInboundAdapter creates a new CaptiveCoreInboundAdapter
func NewCaptiveCoreInboundAdapter(config map[string]interface{}) (base.SourceAdapter, error) {
	return &CaptiveCoreInboundAdapter{
		config: config,
	}, nil
}

// BufferedStorageSourceAdapter is a source adapter for buffered storage
type BufferedStorageSourceAdapter struct {
	BaseSourceAdapter
	config map[string]interface{}
}

// NewBufferedStorageSourceAdapter creates a new BufferedStorageSourceAdapter
func NewBufferedStorageSourceAdapter(config map[string]interface{}) (base.SourceAdapter, error) {
	return &BufferedStorageSourceAdapter{
		config: config,
	}, nil
}

// SorobanSourceAdapter is a source adapter for Soroban
type SorobanSourceAdapter struct {
	BaseSourceAdapter
	config map[string]interface{}
}

// NewSorobanSourceAdapter creates a new SorobanSourceAdapter
func NewSorobanSourceAdapter(config map[string]interface{}) (base.SourceAdapter, error) {
	return &SorobanSourceAdapter{
		config: config,
	}, nil
}

// GCSBufferedStorageSourceAdapter is a source adapter for GCS buffered storage
type GCSBufferedStorageSourceAdapter struct {
	BaseSourceAdapter
	config map[string]interface{}
}

// NewGCSBufferedStorageSourceAdapter creates a new GCSBufferedStorageSourceAdapter
func NewGCSBufferedStorageSourceAdapter(config map[string]interface{}) (base.SourceAdapter, error) {
	return &GCSBufferedStorageSourceAdapter{
		config: config,
	}, nil
}

// FSBufferedStorageSourceAdapter is a source adapter for FS buffered storage
type FSBufferedStorageSourceAdapter struct {
	BaseSourceAdapter
	config map[string]interface{}
}

// NewFSBufferedStorageSourceAdapter creates a new FSBufferedStorageSourceAdapter
func NewFSBufferedStorageSourceAdapter(config map[string]interface{}) (base.SourceAdapter, error) {
	return &FSBufferedStorageSourceAdapter{
		config: config,
	}, nil
}

// S3BufferedStorageSourceAdapter is a source adapter for S3 buffered storage
type S3BufferedStorageSourceAdapter struct {
	BaseSourceAdapter
	config map[string]interface{}
}

// NewS3BufferedStorageSourceAdapter creates a new S3BufferedStorageSourceAdapter
func NewS3BufferedStorageSourceAdapter(config map[string]interface{}) (base.SourceAdapter, error) {
	return &S3BufferedStorageSourceAdapter{
		config: config,
	}, nil
}

// RPCSourceAdapter is a source adapter for RPC
type RPCSourceAdapter struct {
	BaseSourceAdapter
	config map[string]interface{}
}

// NewRPCSourceAdapter creates a new RPCSourceAdapter
func NewRPCSourceAdapter(config map[string]interface{}) (base.SourceAdapter, error) {
	// Use the stellar-rpc adapter implementation
	return stellarrpc.NewStellarRPCSourceAdapter(config)
}
