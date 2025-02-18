package consumer

import (
	"context"
	"encoding/json"
	"fmt"
	"os"

	"github.com/withObsrvr/cdp-pipeline-workflow/processor"
)

// StdoutConsumer is a consumer that writes the incoming payload to stdout
// in JSON format.
type StdoutConsumer struct{}

// NewStdoutConsumer creates a new StdoutConsumer instance.
func NewStdoutConsumer() *StdoutConsumer {
	return &StdoutConsumer{}
}

// Process implements the processor.Processor interface.
// It marshals the payload to JSON (if necessary) and writes it to stdout.
func (s *StdoutConsumer) Process(ctx context.Context, msg processor.Message) error {
	var output []byte
	switch payload := msg.Payload.(type) {
	case []byte:
		output = payload
	default:
		var err error
		output, err = json.Marshal(payload)
		if err != nil {
			return fmt.Errorf("StdoutConsumer: error marshaling payload: %w", err)
		}
	}

	// Write the output to stdout followed by a newline.
	_, err := os.Stdout.Write(append(output, '\n'))
	return err
}

// Subscribe implements the Processor interface.
// Since StdoutConsumer is a sink, this is a no-op.
func (s *StdoutConsumer) Subscribe(proc processor.Processor) {
	// no-op: StdoutConsumer doesn't subscribe to any downstream processor.
}
