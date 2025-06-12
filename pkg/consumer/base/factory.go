package base

import (
	"fmt"

	"github.com/withObsrvr/cdp-pipeline-workflow/pkg/common/types"
)

// NewConsumer creates a consumer based on the provided configuration.
func NewConsumer(config types.ConsumerConfig) (types.Consumer, error) {
	// Here we would add cases for different consumer types
	return nil, fmt.Errorf("unknown consumer type: %s", config.Type)
}
