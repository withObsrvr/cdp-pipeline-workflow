package base

import (
	"context"

	"github.com/withObsrvr/cdp-pipeline-workflow/pkg/common/types"
)

// SourceAdapter defines the interface for source adapters
type SourceAdapter interface {
	Run(context.Context) error
	Subscribe(types.Processor)
}

// SourceConfig defines configuration for a source adapter
type SourceConfig struct {
	Type   string                 `yaml:"type"`
	Config map[string]interface{} `yaml:"config"`
}
