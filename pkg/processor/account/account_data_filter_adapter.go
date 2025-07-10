package account

import (
	"context"

	"github.com/withObsrvr/cdp-pipeline-workflow/pkg/common/types"
	"github.com/withObsrvr/cdp-pipeline-workflow/processor"
)

// AccountDataFilterAdapter wraps the legacy AccountDataFilter to work with the new type system
type AccountDataFilterAdapter struct {
	legacyProcessor *processor.AccountDataFilter
}

// NewAccountDataFilterAdapter creates a new adapter for the AccountDataFilter processor
func NewAccountDataFilterAdapter(config map[string]interface{}) (*AccountDataFilterAdapter, error) {
	// Create the legacy processor
	legacyProc, err := processor.NewAccountDataFilter(config)
	if err != nil {
		return nil, err
	}

	return &AccountDataFilterAdapter{
		legacyProcessor: legacyProc,
	}, nil
}

// Process implements the types.Processor interface by converting between Message types
func (a *AccountDataFilterAdapter) Process(ctx context.Context, msg types.Message) error {
	// Convert types.Message to processor.Message
	legacyMsg := processor.Message{
		Payload: msg.Payload,
	}

	// Call the legacy processor
	return a.legacyProcessor.Process(ctx, legacyMsg)
}

// Subscribe implements the types.Processor interface
func (a *AccountDataFilterAdapter) Subscribe(p types.Processor) {
	// Create an adapter that converts back from types.Processor to processor.Processor
	adapter := &reverseFilterAdapter{processor: p}
	a.legacyProcessor.Subscribe(adapter)
}

// reverseFilterAdapter converts from types.Processor back to processor.Processor
type reverseFilterAdapter struct {
	processor types.Processor
}

func (r *reverseFilterAdapter) Process(ctx context.Context, msg processor.Message) error {
	// Convert processor.Message to types.Message
	typesMsg := types.Message{
		Payload: msg.Payload,
	}
	return r.processor.Process(ctx, typesMsg)
}

func (r *reverseFilterAdapter) Subscribe(p processor.Processor) {
	// This is not used in the adapter pattern
}