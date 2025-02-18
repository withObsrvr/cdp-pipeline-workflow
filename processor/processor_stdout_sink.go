package processor

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
)

// StdoutSink is a processor that writes the incoming payload to stdout.
type StdoutSink struct{}

// NewStdoutSink creates a new StdoutSink instance.
func NewStdoutSink() *StdoutSink {
	return &StdoutSink{}
}

// Process implements the Processor interface by marshaling the payload to JSON and printing it.
func (s *StdoutSink) Process(ctx context.Context, msg Message) error {
	var output []byte
	switch payload := msg.Payload.(type) {
	case []byte:
		output = payload
	default:
		var err error
		output, err = json.Marshal(payload)
		if err != nil {
			return fmt.Errorf("StdoutSink: error marshaling payload: %w", err)
		}
	}

	// Write to stdout followed by a newline.
	_, err := os.Stdout.Write(append(output, '\n'))
	return err
}

// Subscribe implements the Processor interface.
// Since StdoutSink is a sink, this is a no-op.
func (s *StdoutSink) Subscribe(proc Processor) {
	// no-op: StdoutSink is the final stage so it doesn't subscribe to any downstream processor.
}
