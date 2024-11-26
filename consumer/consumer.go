package consumer

import (
	"context"

	"github.com/withObsrvr/cdp-pipeline-workflow/processor"
)

// Processor defines the interface for processing messages.
type Consumer interface {
	Process(context.Context, processor.Message) error
	Subscribe(processor.Processor)
}

type ConsumerConfig struct {
	Type   string                 `yaml:"type"`
	Config map[string]interface{} `yaml:"config"`
}
